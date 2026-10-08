/**
 * End-to-end crash test. Exit code 0 = the pipeline produced exactly what a
 * clean replay of the raw log produces, despite a mid-batch crash, retries,
 * out-of-order events and a poison message.
 *
 *   1. Produce ~320 messages into the stream (with retries, redeliveries, late clicks, 1 poison).
 *   2. worker-a processes them and is SIGKILLed mid-batch: rows committed, nothing acked.
 *   3. worker-b and worker-c start, reclaim the orphaned messages (XAUTOCLAIM) and drain the stream.
 *   4. Replay the raw log independently and compare with Postgres.
 */
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { config } from '../src/config.js';
import { migrate, pool, resetTables } from '../src/db.js';
import { parseEvent } from '../src/events.js';
import { paint, sleep } from '../src/log.js';
import { generateScenario, produce } from '../src/producer.js';
import { createRedis, ensureGroup, pendingCount } from '../src/redis.js';
import { verify } from './verify.js';

const WORKER = fileURLToPath(new URL('../src/worker.ts', import.meta.url));

function step(n: number, title: string) {
  console.log('\n' + paint('bold', `━━━ ${n}/4  ${title} `.padEnd(84, '━')) + '\n');
}

function runWorker(name: string, color: string, env: Record<string, string>): Promise<{ code: number | null; signal: string | null }> {
  const child = spawn(process.execPath, ['--import', 'tsx', WORKER], {
    stdio: 'inherit',
    // Reset demo knobs so an exported CRASH_AFTER_N in the shell only ever reaches worker-a.
    env: { ...process.env, CRASH_AFTER_N: '', EXIT_WHEN_IDLE_MS: '', WORKER_NAME: name, WORKER_COLOR: color, ...env },
  });
  return new Promise((resolve) => child.on('exit', (code, signal) => resolve({ code, signal })));
}

async function main() {
  const redis = createRedis();
  try {
    await redis.ping();
    await pool.query('SELECT 1');
  } catch (e) {
    console.error(paint('red', `Redis/Postgres not reachable (${(e as Error).message}). Run \`npm run infra:up\` first.`));
    process.exit(2);
  }

  await migrate();
  await resetTables();
  await redis.del(config.stream, config.dlqStream, 'stats');
  await ensureGroup(redis);

  // ---------------------------------------------------------------------------
  step(1, 'Produce events into the Redis Stream');
  const seed = Number(process.env.SEED);
  const scenario = generateScenario(Number.isFinite(seed) ? seed : 42);
  await produce(redis, scenario);
  const s = scenario.summary;
  console.log(`  ${s.messages} messages → ${s.uniqueEvents} unique events (${s.clicks} clicks incl. ${s.lateClicks} late, ${s.orders} orders)`);
  console.log(`  plus the messy parts: ${s.exactRetries} exact retries, ${s.orderRedeliveries} order redeliveries with a new event_id, ${s.poison} poison message`);

  // ---------------------------------------------------------------------------
  const crashAfter = Number(process.env.CRASH_AFTER_N) || 55;
  step(2, `worker-a starts processing… and is killed after ${crashAfter} messages, mid-batch`);
  const a = await runWorker('worker-a', 'yellow', { CRASH_AFTER_N: String(crashAfter) });
  console.log(`\n  worker-a is gone (${a.signal ?? `exit ${a.code}`}).`);

  const pendingAfterCrash = await pendingCount(redis);
  const pendingEntries = (await redis.xpending(config.stream, config.group, '-', '+', 1000)) as unknown as [string, string, number, number][];
  const pendingIds = new Set(pendingEntries.map((p) => p[0]));
  let committedAmongPending = 0;
  if (pendingIds.size > 0) {
    const ids = [...pendingIds].sort();
    const range = (await redis.xrange(config.stream, ids[0], ids[ids.length - 1])) as [string, string[]][];
    const eventIds: string[] = [];
    for (const [id, fields] of range) {
      if (!pendingIds.has(id)) continue;
      try { eventIds.push(parseEvent(fields[fields.indexOf('payload') + 1]).event_id); } catch { /* poison */ }
    }
    const r = await pool.query<{ event_id: string }>('SELECT event_id FROM processed_events WHERE event_id = ANY($1)', [eventIds]);
    const committed = new Set(r.rows.map((x) => x.event_id));
    committedAmongPending = eventIds.filter((id) => committed.has(id)).length; // per message, retries included
  }
  console.log(`  ${paint('bold', String(pendingAfterCrash))} messages are pending in the group: delivered to worker-a, never acknowledged.`);
  console.log(`  ${paint('bold', String(committedAmongPending))} of them are already committed in Postgres.`);
  console.log(`  A consumer that is not idempotent would apply those ${committedAmongPending} a second time and pay creators twice.`);
  await sleep(1200);

  // ---------------------------------------------------------------------------
  step(3, 'worker-b and worker-c take over: XAUTOCLAIM the orphans, drain the rest, dead-letter the poison');
  await Promise.all([
    runWorker('worker-b', 'green', { EXIT_WHEN_IDLE_MS: '4000' }),
    runWorker('worker-c', 'cyan', { EXIT_WHEN_IDLE_MS: '4000' }),
  ]);

  // ---------------------------------------------------------------------------
  step(4, 'Verify: replay the raw log with a reference implementation and diff against Postgres');
  const ok = await verify(redis, pool, { pendingAfterCrash, committedAmongPending });
  console.log(ok
    ? paint('green', '  PASS  every order attributed exactly once, every cent accounted for, nothing pending.')
    : paint('red', '  FAIL  see ✘ rows above.'));
  console.log();

  redis.disconnect();
  await pool.end();
  process.exit(ok ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
