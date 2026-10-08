/**
 * Independent check: replay the raw stream with a pure, in-memory reference
 * implementation of last-click attribution, and compare it to what the
 * workers wrote to Postgres. If the pipeline is correct, they agree exactly,
 * no matter how many crashes, retries and reorderings happened on the way.
 */
import type Redis from 'ioredis';
import type pg from 'pg';
import { config } from '../src/config.js';
import { pool } from '../src/db.js';
import { commissionFor, parseEvent, type ClickEvent, type OrderEvent } from '../src/events.js';
import { money, paint } from '../src/log.js';
import { createRedis, pendingCount } from '../src/redis.js';

interface Row { label: string; value: string; ok?: boolean }

export interface CrashFacts { pendingAfterCrash: number; committedAmongPending: number }

export async function verify(redis: Redis, db: pg.Pool, crash?: CrashFacts): Promise<boolean> {
  // ---- 1. Reference: replay the raw log ---------------------------------------
  const raw = (await redis.xrange(config.stream, '-', '+')) as [string, string[]][];
  const seen = new Set<string>();
  const clicks: ClickEvent[] = [];
  const orders = new Map<string, OrderEvent>();
  let poison = 0, exactDup = 0, orderRedeliveries = 0;

  for (const [, fields] of raw) {
    const payload = fields[fields.indexOf('payload') + 1];
    let ev;
    try { ev = parseEvent(payload); } catch { poison++; continue; }
    if (seen.has(ev.event_id)) { exactDup++; continue; }
    seen.add(ev.event_id);
    if (ev.type === 'click') clicks.push(ev);
    else if (orders.has(ev.order_id)) orderRedeliveries++;
    else orders.set(ev.order_id, ev);
  }

  const byVisitor = new Map<string, ClickEvent[]>();
  for (const c of clicks) {
    const k = `${c.store_id}:${c.visitor_id}`;
    (byVisitor.get(k) ?? byVisitor.set(k, []).get(k)!).push(c);
  }
  for (const list of byVisitor.values()) list.sort((a, b) => b.ts - a.ts || (b.event_id < a.event_id ? -1 : 1));

  const windowMs = config.attributionWindowSec * 1000;
  const expected = new Map<string, { click: string; creator: string; commission: number }>();
  const expectedByCreator = new Map<string, number>();
  for (const o of orders.values()) {
    const best = (byVisitor.get(`${o.store_id}:${o.visitor_id}`) ?? []).find((c) => c.ts <= o.ts && c.ts > o.ts - windowMs);
    if (!best) continue;
    const commission = commissionFor(o.amount_cents, config.commissionBps);
    expected.set(o.order_id, { click: best.event_id, creator: best.creator_id, commission });
    expectedByCreator.set(best.creator_id, (expectedByCreator.get(best.creator_id) ?? 0) + commission);
  }

  // ---- 2. Actual: what the workers wrote ---------------------------------------
  const attr = await db.query<{ order_id: string; click_event_id: string; creator_id: string }>('SELECT order_id, click_event_id, creator_id FROM attributions');
  const ledgerByOrder = await db.query<{ order_id: string; net: string }>('SELECT order_id, SUM(amount_cents)::bigint AS net FROM ledger GROUP BY order_id');
  const ledgerByCreator = await db.query<{ creator_id: string; net: string }>('SELECT creator_id, SUM(amount_cents)::bigint AS net FROM ledger GROUP BY creator_id');
  const counts = await db.query<{ events: string; orders: string; clicks: string; ledger_rows: string }>(
    `SELECT (SELECT COUNT(*) FROM processed_events) AS events, (SELECT COUNT(*) FROM orders) AS orders,
            (SELECT COUNT(*) FROM clicks) AS clicks, (SELECT COUNT(*) FROM ledger) AS ledger_rows`,
  );
  const pending = await pendingCount(redis);
  const dlq = await redis.xlen(config.dlqStream);
  const stats = await redis.hgetall('stats');

  // ---- 3. Compare -------------------------------------------------------------
  let attrMismatch = 0;
  const actualAttr = new Map(attr.rows.map((r) => [r.order_id, r]));
  for (const [orderId, exp] of expected) if (actualAttr.get(orderId)?.click_event_id !== exp.click) attrMismatch++;
  for (const orderId of actualAttr.keys()) if (!expected.has(orderId)) attrMismatch++;

  let ledgerDrift = 0;
  const netByOrder = new Map(ledgerByOrder.rows.map((r) => [r.order_id, Number(r.net)]));
  for (const orderId of orders.keys()) {
    const want = expected.get(orderId)?.commission ?? 0;
    if ((netByOrder.get(orderId) ?? 0) !== want) ledgerDrift++;
  }
  for (const orderId of netByOrder.keys()) if (!orders.has(orderId)) ledgerDrift++;

  let creatorMismatch = 0;
  const netByCreator = new Map(ledgerByCreator.rows.map((r) => [r.creator_id, Number(r.net)]));
  const creatorsAll = new Set([...netByCreator.keys(), ...expectedByCreator.keys()]);
  for (const c of creatorsAll) if ((netByCreator.get(c) ?? 0) !== (expectedByCreator.get(c) ?? 0)) creatorMismatch++;

  const totalExpected = [...expectedByCreator.values()].reduce((a, b) => a + b, 0);
  const totalActual = [...netByCreator.values()].reduce((a, b) => a + b, 0);
  const n = (k: string) => Number(stats[k] ?? 0);

  const rows: Row[] = [
    { label: 'Stream messages delivered', value: String(raw.length) },
    { label: '  unique valid events', value: `${seen.size}  (${clicks.length} clicks, ${orders.size} orders)` },
    { label: '  exact retries (same event_id)', value: String(exactDup) },
    { label: '  order redeliveries (new event_id)', value: String(orderRedeliveries) },
    { label: '  poison messages', value: String(poison) },
  ];
  if (crash) {
    rows.push({ label: 'After crash: delivered but never acked', value: String(crash.pendingAfterCrash) });
    rows.push({ label: '  of which already committed in Postgres', value: String(crash.committedAmongPending) });
  }
  rows.push(
    { label: 'Duplicate deliveries rejected by inbox', value: String(n('dup_event')) },
    { label: 'Duplicate orders rejected by order PK', value: String(n('dup_order')) },
    { label: 'Orders fixed up by a late click', value: String(n('late_click')) },
    { label: 'Messages reclaimed via XAUTOCLAIM', value: String(n('reclaimed')) },
    { label: '', value: '' },
    { label: 'Orders attributed   pipeline / replay', value: `${actualAttr.size} / ${expected.size}`, ok: actualAttr.size === expected.size },
    { label: 'Attribution mismatches', value: String(attrMismatch), ok: attrMismatch === 0 },
    { label: 'Orders whose ledger ≠ correct commission', value: String(ledgerDrift), ok: ledgerDrift === 0 },
    { label: 'Creators whose balance ≠ replay', value: String(creatorMismatch), ok: creatorMismatch === 0 },
    { label: 'Total creator earnings   pipeline / replay', value: `${money(totalActual)} / ${money(totalExpected)}`, ok: totalActual === totalExpected },
    { label: 'Ledger rows (append-only, incl. reversals)', value: counts.rows[0].ledger_rows },
    { label: 'processed_events = unique valid events', value: `${counts.rows[0].events} = ${seen.size}`, ok: Number(counts.rows[0].events) === seen.size },
    { label: 'orders rows = unique orders', value: `${counts.rows[0].orders} = ${orders.size}`, ok: Number(counts.rows[0].orders) === orders.size },
    { label: 'Pending in consumer group', value: String(pending), ok: pending === 0 },
    { label: 'Dead-lettered = poison messages', value: `${dlq} = ${poison}`, ok: dlq === poison },
  );

  const ok = rows.every((r) => r.ok !== false);
  console.log();
  for (const r of rows) {
    if (!r.label) { console.log('  ' + '─'.repeat(74)); continue; }
    const mark = r.ok === undefined ? '  ' : r.ok ? paint('green', '✔ ') : paint('red', '✘ ');
    console.log(`  ${mark}${r.label.padEnd(46)} ${r.value}`);
  }
  console.log();
  return ok;
}

// Standalone: `npm run verify` (after a demo run)
if (process.argv[1]?.endsWith('verify.ts')) {
  const redis = createRedis();
  const ok = await verify(redis, pool);
  console.log(ok ? paint('green', 'PASS') : paint('red', 'FAIL'));
  redis.disconnect();
  await pool.end();
  process.exit(ok ? 0 : 1);
}
