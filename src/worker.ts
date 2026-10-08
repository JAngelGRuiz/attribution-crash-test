/**
 * Stream consumer. Run as many of these as you like; they share the work
 * through a Redis consumer group.
 *
 * Env knobs used by the demo:
 *   WORKER_NAME        consumer name inside the group
 *   WORKER_COLOR       log color
 *   CRASH_AFTER_N      hard-kill this process after N messages, BEFORE acking the batch
 *   EXIT_WHEN_IDLE_MS  exit once the stream is drained and nothing is pending for this long
 */
import type Redis from 'ioredis';
import { applyEvent, type Outcome } from './attribution.js';
import { config } from './config.js';
import { pool } from './db.js';
import { parseEvent } from './events.js';
import { makeLogger } from './log.js';
import { createRedis, ensureGroup, pendingCount, toMsg, type StreamMsg } from './redis.js';

const name = process.env.WORKER_NAME ?? `worker-${process.pid}`;
const crashAfter = Number(process.env.CRASH_AFTER_N ?? 0);
const exitWhenIdleMs = Number(process.env.EXIT_WHEN_IDLE_MS ?? 0);
const log = makeLogger(name, process.env.WORKER_COLOR ?? 'green');

type Counts = Record<Outcome | 'late_click' | 'failed', number>;

async function main(): Promise<void> {
  const redis = createRedis();
  await ensureGroup(redis);
  const db = await pool.connect();

  log.info(
    `online  batch=${config.batchSize}  reclaim idle>${config.claimMinIdleMs}ms  max deliveries=${config.maxDeliveries}` +
      (crashAfter ? `  ⚠ will crash after ${crashAfter} messages` : ''),
  );

  let processedTotal = 0;
  let idleSince: number | null = null;

  for (;;) {
    // 1. Take over messages that another consumer started but never acked.
    const claimed = await claimStale(redis);
    // 2. Read new messages ('>' = never delivered to anyone in this group).
    const fresh = await readFresh(redis);
    const batch = [...claimed, ...fresh];

    if (batch.length === 0) {
      if (exitWhenIdleMs && (await pendingCount(redis)) === 0) {
        idleSince ??= Date.now();
        if (Date.now() - idleSince >= exitWhenIdleMs) {
          log.info(`stream drained and nothing pending → exiting (processed ${processedTotal} messages)`);
          break;
        }
      } else {
        idleSince = null;
      }
      continue;
    }
    idleSince = null;

    const counts: Counts = { applied: 0, unattributed: 0, dup_event: 0, dup_order: 0, late_click: 0, failed: 0 };
    const ackIds: string[] = [];

    for (const msg of batch) {
      const ok = await handle(redis, db, msg, counts);
      if (ok) ackIds.push(msg.id);
      processedTotal++;

      if (crashAfter && processedTotal >= crashAfter) {
        log.error(
          `💥 simulated crash after ${processedTotal} messages. ` +
            `${ackIds.length} of this batch are COMMITTED in Postgres but NOT acknowledged in Redis.`,
        );
        process.kill(process.pid, 'SIGKILL'); // no cleanup, no ack, like a real OOM kill
      }
    }

    // 3. Ack only after every message in the batch has been committed (or deliberately left for retry).
    if (ackIds.length > 0) await redis.xack(config.stream, config.group, ...ackIds);

    log.info(
      `batch ${String(fresh.length).padStart(2)} fresh + ${String(claimed.length).padStart(2)} reclaimed → ` +
        `applied=${counts.applied} unattributed=${counts.unattributed} ` +
        `dup_event=${counts.dup_event} dup_order=${counts.dup_order} late_click_fixups=${counts.late_click} ` +
        `failed=${counts.failed} → acked ${ackIds.length}`,
    );
  }

  db.release();
  await pool.end();
  redis.disconnect();
}

async function handle(redis: Redis, db: import('pg').PoolClient, msg: StreamMsg, counts: Counts): Promise<boolean> {
  try {
    const ev = parseEvent(msg.payload);
    const r = await applyEvent(db, ev);
    counts[r.outcome]++;
    counts.late_click += r.lateClickReattributions;
    await redis.hincrby('stats', r.outcome, 1);
    if (r.lateClickReattributions) await redis.hincrby('stats', 'late_click', r.lateClickReattributions);
    return true;
  } catch (e) {
    counts.failed++;
    await redis.hincrby('stats', 'failed', 1);
    log.warn(`message ${msg.id} failed: ${(e as Error).message} → left pending for retry`);
    return false; // not acked: it stays pending and will be reclaimed
  }
}

async function readFresh(redis: Redis): Promise<StreamMsg[]> {
  const res = (await redis.xreadgroup(
    'GROUP', config.group, name,
    'COUNT', config.batchSize,
    'BLOCK', config.blockMs,
    'STREAMS', config.stream, '>',
  )) as [string, [string, string[]][]][] | null;
  return res?.[0]?.[1]?.map(toMsg) ?? [];
}

/**
 * XAUTOCLAIM: atomically transfer ownership of messages that have been pending
 * for longer than claimMinIdleMs, whoever owned them. This is how work from a
 * crashed worker gets picked up without any coordinator.
 */
async function claimStale(redis: Redis): Promise<StreamMsg[]> {
  const res = (await redis.xautoclaim(
    config.stream, config.group, name, config.claimMinIdleMs, '0-0', 'COUNT', config.batchSize,
  )) as unknown as [string, [string, string[]][], string[]];
  const entries = (res[1] ?? []).filter(Boolean);
  if (entries.length === 0) return [];
  const msgs = entries.map(toMsg);

  // XAUTOCLAIM doesn't return delivery counts; XPENDING on the claimed range does.
  const pend = (await redis.xpending(
    config.stream, config.group, msgs[0].id, msgs[msgs.length - 1].id, 1000, name,
  )) as unknown as [string, string, number, number][];
  const deliveries = new Map(pend.map((p) => [p[0], Number(p[3])]));

  const keep: StreamMsg[] = [];
  for (const m of msgs) {
    const n = deliveries.get(m.id) ?? 1;
    if (n > config.maxDeliveries) {
      // Poison message: park it with context, ack it so it stops blocking the group.
      await redis
        .multi()
        .xadd(config.dlqStream, '*', 'original_id', m.id, 'payload', m.payload, 'deliveries', String(n), 'reason', 'max deliveries exceeded')
        .xack(config.stream, config.group, m.id)
        .hincrby('stats', 'dlq', 1)
        .exec();
      log.error(`☠ message ${m.id} failed ${n} deliveries → moved to ${config.dlqStream}`);
    } else {
      keep.push(m);
    }
  }
  if (keep.length > 0) {
    const maxDelivery = Math.max(...keep.map((m) => deliveries.get(m.id) ?? 1));
    log.warn(`reclaimed ${keep.length} stale message(s) idle > ${config.claimMinIdleMs}ms from a dead/stuck consumer (delivery #${maxDelivery})`);
    await redis.hincrby('stats', 'reclaimed', keep.length);
  }
  return keep;
}

main().catch((e) => {
  log.error(`fatal: ${e?.stack ?? e}`);
  process.exit(1);
});
