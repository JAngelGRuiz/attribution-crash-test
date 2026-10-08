import type { PoolClient } from 'pg';
import { config } from './config.js';
import { commissionFor, type ClickEvent, type OrderEvent, type PipelineEvent } from './events.js';

export type Outcome = 'applied' | 'unattributed' | 'dup_event' | 'dup_order';

export interface ApplyResult {
  outcome: Outcome;
  /** Orders whose attribution was created or moved because a click arrived late. */
  lateClickReattributions: number;
}

/**
 * Apply one event to Postgres. Safe to call any number of times with the same
 * event, in any order relative to other events, from any number of workers.
 *
 * Everything happens in ONE transaction, and the stream message is only
 * acknowledged after this commits. If the process dies in between, the message
 * is redelivered and the inbox row turns the second run into a no-op.
 */
export async function applyEvent(db: PoolClient, ev: PipelineEvent): Promise<ApplyResult> {
  await db.query('BEGIN');
  try {
    // Serialize all work for one (store, visitor). A click and an order for the
    // same person can then never interleave across workers (no write skew).
    // Released automatically at COMMIT / ROLLBACK.
    await db.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`${ev.store_id}:${ev.visitor_id}`]);

    // Layer 1: inbox. Same event_id seen before => we already did this work.
    const inbox = await db.query('INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING', [ev.event_id]);
    if (inbox.rowCount === 0) {
      await db.query('ROLLBACK');
      return { outcome: 'dup_event', lateClickReattributions: 0 };
    }

    const result = ev.type === 'click' ? await applyClick(db, ev) : await applyOrder(db, ev);
    await db.query('COMMIT');
    return result;
  } catch (e) {
    await db.query('ROLLBACK');
    throw e;
  }
}

async function applyClick(db: PoolClient, ev: ClickEvent): Promise<ApplyResult> {
  await db.query(
    `INSERT INTO clicks (event_id, store_id, visitor_id, creator_id, submission_id, clicked_at)
     VALUES ($1, $2, $3, $4, $5, to_timestamp($6::bigint / 1000.0))
     ON CONFLICT (event_id) DO NOTHING`,
    [ev.event_id, ev.store_id, ev.visitor_id, ev.creator_id, ev.submission_id, ev.ts],
  );

  // A click can arrive AFTER an order it should win (in-app browser retries,
  // offline queues, or simply another worker being faster with the order).
  // Re-evaluate every order of this visitor that this click could be the
  // last click for. This is what makes processing order irrelevant.
  const affected = await db.query<{ order_id: string }>(
    `SELECT order_id FROM orders
     WHERE store_id = $1 AND visitor_id = $2
       AND ordered_at >= to_timestamp($3::bigint / 1000.0)
       AND ordered_at <  to_timestamp($3::bigint / 1000.0) + ($4::int * interval '1 second')`,
    [ev.store_id, ev.visitor_id, ev.ts, config.attributionWindowSec],
  );

  let moved = 0;
  for (const row of affected.rows) {
    const r = await reconcileAttribution(db, row.order_id);
    if (r === 'attributed' || r === 'moved') moved++;
  }
  return { outcome: 'applied', lateClickReattributions: moved };
}

async function applyOrder(db: PoolClient, ev: OrderEvent): Promise<ApplyResult> {
  // Layer 2: the order itself is unique. A redelivery with a NEW event_id
  // (Shopify does this) passes the inbox but stops here.
  const ins = await db.query(
    `INSERT INTO orders (order_id, store_id, visitor_id, amount_cents, ordered_at)
     VALUES ($1, $2, $3, $4, to_timestamp($5::bigint / 1000.0))
     ON CONFLICT (order_id) DO NOTHING`,
    [ev.order_id, ev.store_id, ev.visitor_id, ev.amount_cents, ev.ts],
  );
  if (ins.rowCount === 0) return { outcome: 'dup_order', lateClickReattributions: 0 };

  const r = await reconcileAttribution(db, ev.order_id);
  return { outcome: r === 'none' ? 'unattributed' : 'applied', lateClickReattributions: 0 };
}

/**
 * Make the stored attribution for an order match the correct answer computed
 * from what Postgres knows right now: the latest click of the same visitor at
 * or before the order, inside the attribution window.
 *
 * This is a pure function of the data, so calling it twice is harmless, and
 * it is the ONLY place that writes to `attributions` and `ledger`.
 */
async function reconcileAttribution(db: PoolClient, orderId: string): Promise<'none' | 'unchanged' | 'attributed' | 'moved'> {
  const { rows } = await db.query<{
    amount_cents: string;
    current_click: string | null;
    current_creator: string | null;
    best_click: string | null;
    best_creator: string | null;
    best_submission: string | null;
  }>(
    `SELECT o.amount_cents,
            a.click_event_id AS current_click, a.creator_id AS current_creator,
            c.event_id AS best_click, c.creator_id AS best_creator, c.submission_id AS best_submission
     FROM orders o
     LEFT JOIN attributions a ON a.order_id = o.order_id
     LEFT JOIN LATERAL (
       SELECT event_id, creator_id, submission_id
       FROM clicks
       WHERE store_id = o.store_id AND visitor_id = o.visitor_id
         AND clicked_at <= o.ordered_at
         AND clicked_at >  o.ordered_at - ($2::int * interval '1 second')
       ORDER BY clicked_at DESC, event_id DESC
       LIMIT 1
     ) c ON true
     WHERE o.order_id = $1`,
    [orderId, config.attributionWindowSec],
  );
  const r = rows[0];
  if (!r?.best_click) return 'none';
  if (r.current_click === r.best_click) return 'unchanged';

  const commission = commissionFor(Number(r.amount_cents), config.commissionBps);

  if (r.current_click) {
    // Layer 3: never mutate money. Reverse the old creator's entry, then add the new one.
    await db.query(
      `INSERT INTO ledger (idempotency_key, creator_id, order_id, entry_type, amount_cents)
       VALUES ($1, $2, $3, 'reversal', $4) ON CONFLICT (idempotency_key) DO NOTHING`,
      [`reversal:${orderId}:${r.current_click}`, r.current_creator, orderId, -commission],
    );
    await db.query(
      `UPDATE attributions SET click_event_id = $2, creator_id = $3, submission_id = $4, attributed_at = now()
       WHERE order_id = $1`,
      [orderId, r.best_click, r.best_creator, r.best_submission],
    );
  } else {
    await db.query(
      `INSERT INTO attributions (order_id, click_event_id, creator_id, submission_id)
       VALUES ($1, $2, $3, $4) ON CONFLICT (order_id) DO NOTHING`,
      [orderId, r.best_click, r.best_creator, r.best_submission],
    );
  }

  await db.query(
    `INSERT INTO ledger (idempotency_key, creator_id, order_id, entry_type, amount_cents)
     VALUES ($1, $2, $3, 'commission', $4) ON CONFLICT (idempotency_key) DO NOTHING`,
    [`commission:${orderId}:${r.best_click}`, r.best_creator, orderId, commission],
  );

  return r.current_click ? 'moved' : 'attributed';
}
