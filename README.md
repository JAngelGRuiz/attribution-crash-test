# Crash-safe last-click attribution on Redis Streams + Postgres

A small, runnable prototype of the one part of a creator-attribution pipeline where money is decided.

Click and order events flow through a Redis Stream. Workers in a consumer group attribute each order to the visitor's last click and write the creator's commission to an append-only ledger. Workers get SIGKILLed mid-batch, receive the same message twice, receive the same order under a new event id, process events out of order, and hit a poison message. The result is still **exactly** what a clean replay of the raw log produces.

`npm run demo` does all of that end to end in about 15 seconds and exits non-zero if a single cent is off.

## Run it

```bash
npm install
npm run infra:up      # redis:7 + postgres:16 via docker compose (ports 6390 / 5440)
npm run demo          # produce → crash → recover → verify
```

Abridged output:

```
━━━ 2/4  worker-a starts processing… and is killed after 55 messages, mid-batch
[worker-a]  batch 20 fresh +  0 reclaimed → applied=14 unattributed=6 … → acked 20
[worker-a]  batch 20 fresh +  0 reclaimed → applied=13 unattributed=6 … → acked 20
[worker-a]  💥 simulated crash after 55 messages. 15 of this batch are COMMITTED in Postgres but NOT acknowledged in Redis.

  20 messages are pending in the group: delivered to worker-a, never acknowledged.
  15 of them are already committed in Postgres.
  A consumer that is not idempotent would apply those 15 a second time and pay creators twice.

━━━ 3/4  worker-b and worker-c take over: XAUTOCLAIM the orphans, drain the rest, dead-letter the poison
[worker-c]  batch 20 fresh +  0 reclaimed → applied=14 unattributed=3 dup_event=2 dup_order=1 … → acked 20
[worker-b]  message 1791423924381-159 failed: payload is not valid JSON → left pending for retry
…
[worker-c]  reclaimed 20 stale message(s) idle > 1500ms from a dead/stuck consumer (delivery #2)
[worker-c]  batch  0 fresh + 20 reclaimed → applied=3 unattributed=2 dup_event=15 … late_click_fixups=1 → acked 20
[worker-b]  ☠ message 1791423924381-159 failed 4 deliveries → moved to events:dlq

━━━ 4/4  Verify: replay the raw log with a reference implementation and diff against Postgres
    Stream messages delivered                      317
      unique valid events                          307  (194 clicks, 106 orders)
      exact retries (same event_id)                9
      order redeliveries (new event_id)            7
      poison messages                              1
    After crash: delivered but never acked         20
      of which already committed in Postgres       15
    Duplicate deliveries rejected by inbox         24
    Duplicate orders rejected by order PK          7
    Orders fixed up by a late click                7
    Messages reclaimed via XAUTOCLAIM              22
  ──────────────────────────────────────────────────────────────────────────
  ✔ Orders attributed   pipeline / replay          75 / 75
  ✔ Attribution mismatches                         0
  ✔ Orders whose ledger ≠ correct commission       0
  ✔ Creators whose balance ≠ replay                0
  ✔ Total creator earnings   pipeline / replay     $894.65 / $894.65
    Ledger rows (append-only, incl. reversals)     87
  ✔ processed_events = unique valid events         307 = 307
  ✔ Pending in consumer group                      0
  ✔ Dead-lettered = poison messages                1 = 1

  PASS  every order attributed exactly once, every cent accounted for, nothing pending.
```

The verifier is not a rubber stamp: insert one extra commission row by hand and `npm run verify` turns red.

## The problem

Redis Streams with consumer groups gives you **at-least-once** delivery. That is the right choice (nothing gets lost), but it means every consumer must assume it will see the same message more than once, and that the message it is holding may have been half-processed by a worker that no longer exists.

When the consumer's output is "which creator gets paid for this order", the failure modes are not theoretical:

| What goes wrong                                                                                                                        | What a naive consumer does                           |
| -------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| Worker commits to Postgres, then dies before `XACK`                                                                                    | Message is redelivered, commission is paid twice     |
| Pixel retries via `sendBeacon`, same `event_id`                                                                                        | Click stored twice, or order paid twice              |
| Shopify redelivers `orders/create` with a **new** webhook id                                                                           | Passes any event-id dedupe, order paid twice         |
| A click reaches the pipeline after the order it should win (in-app browser, offline retry, or just a faster worker on the other batch) | Order attributed to the wrong creator, or not at all |
| One malformed message                                                                                                                  | Retries forever, blocks the group                    |

## Design

```
 pixel / webhooks                 Redis                              Postgres (source of truth)
 ───────────────     XADD    ┌───────────────┐   XREADGROUP     ┌─────────────────────────────┐
  click ───────────────────▶ │ stream:events │ ───────────────▶ │  BEGIN                       │
  order ───────────────────▶ │  group:       │   XAUTOCLAIM     │   advisory lock (visitor)    │
  retry (same id) ─────────▶ │  attribution  │ ◀── stale ────── │   inbox: processed_events PK │──▶ dup? ROLLBACK
  redelivery (new id) ─────▶ │               │                  │   clicks / orders            │
                             │  events:dlq   │ ◀── poison ───── │   reconcile attribution      │
                             └───────────────┘                  │   ledger (append-only)       │
                                    ▲                           │  COMMIT                      │
                                    └───────── XACK ─────────── └─────────────────────────────┘
                                         (only after COMMIT)
```

### Guarantees and where they come from

| Guarantee                                        | Mechanism                                                                                                                                                                                                                                                    | Code                                |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------- |
| No message is lost when a worker dies            | Ack after commit, never before. Unacked messages stay in the PEL and are reclaimed with `XAUTOCLAIM` once idle > 1.5 s                                                                                                                                       | `src/worker.ts`                     |
| A redelivered message is a no-op                 | **Inbox table**: `INSERT INTO processed_events … ON CONFLICT DO NOTHING` in the same transaction as the business write. 0 rows = already done, roll back                                                                                                     | `src/attribution.ts` → `applyEvent` |
| An order exists once, whatever its event id      | **Order primary key**: `INSERT INTO orders … ON CONFLICT (order_id) DO NOTHING`                                                                                                                                                                              | `applyOrder`                        |
| An order is attributed at most once              | **`attributions.order_id` is the PK**. Twice is a constraint violation, not a bug to find later                                                                                                                                                              | `src/schema.sql`                    |
| Money is never double counted or silently edited | **Append-only ledger** in integer cents with a unique idempotency key per `(order, click)` entry. Moving an order to another creator is a reversal row plus a new row, never an `UPDATE`                                                                     | `reconcileAttribution`              |
| Result does not depend on processing order       | A `pg_advisory_xact_lock` per `(store, visitor)` serializes a visitor's click and order handling across workers. A late click re-evaluates every order it could be the last click for. Attribution becomes a pure function of the data, not of arrival order | `applyClick`                        |
| A poison message cannot block the group          | Delivery count from `XPENDING`; above `MAX_DELIVERIES` it is moved to `events:dlq` with its payload and acked                                                                                                                                                | `claimStale`                        |
| Any of the above can be proven after the fact    | The raw stream is the audit log. `scripts/verify.ts` replays it with a 30-line reference implementation and diffs against Postgres                                                                                                                           | `scripts/verify.ts`                 |

### Why these choices

**Ack after commit, not before.** Acking first gives at-most-once: fast, and silently loses money when the worker dies. Acking after gives at-least-once plus duplicates. Duplicates are a solved problem (see next row); lost orders are not.

**Three dedupe layers instead of one.** The inbox handles retries of the same message. The order PK handles the same business fact under a new id, which the inbox cannot see. The ledger key handles the money independently of both, so a bug in attribution logic can never mint a second payout for the same `(order, click)`.

**Postgres decides the last click, not a Redis cache.** A "latest click per visitor" key in Redis would make the order path an O(1) lookup, and I would add it as a read-through cache at scale. But a cache written after the commit can be missed by a crash, and a cache written before can be ahead of the truth. In this prototype the lookup is an index seek on `(store_id, visitor_id, clicked_at DESC)`, which is cheap enough and always consistent with the transaction it runs in.

**Re-evaluate on late clicks instead of forcing order.** The alternative is strict per-visitor ordering (one partition per visitor key, one consumer per partition). That is what Kafka buys you, and it is the right long-term shape. With Redis Streams and a shared consumer group, order across workers is not guaranteed, so I made the handlers commutative instead: whatever order a click and an order arrive in, the stored result converges to the same answer. The advisory lock removes the one race (write skew between the two handlers) that commutativity alone does not cover.

**The ledger can only grow.** Balances are `SUM(amount_cents)`. Reversals are explicit rows, so a creator's statement shows "earned $12.30 for order 118, reversed, re-attributed to another creator" rather than a number that quietly changed. That is also what you need for refunds and chargebacks: a new negative row keyed by the refund id, never an edit.

### Deliberately out of scope

- Refunds and clawbacks (a negative ledger row keyed by refund event id, plus a hold window before Stripe transfers).
- The transactional outbox for downstream side effects (push notifications, Stripe transfers). Same pattern: write the intent in the commit, relay it separately.
- Identity stitching across devices. This prototype keys on a first-party `visitor_id`.
- Trimming `processed_events` (partition by `processed_at`, drop partitions older than the longest retry horizon).

## What the demo exercises

1. **Produce** ~320 messages from a seeded generator: 60 visitors across 3 stores, 8 creators. Mixed in: exact retries (same `event_id`), order redeliveries (same `order_id`, new `event_id`), late clicks with an event time up to 3 minutes in the past, and one truncated JSON payload.
2. **Crash.** `worker-a` processes batches of 20 and is `SIGKILL`ed after 55 messages, i.e. 15 messages into its third batch. Those 15 are committed in Postgres but never acked. The orchestrator then reads the PEL and shows how many of the pending messages are already committed.
3. **Recover.** `worker-b` and `worker-c` start together. They read fresh messages concurrently (so clicks and orders of the same visitor can be processed out of order across workers), reclaim the 20 orphaned messages with `XAUTOCLAIM`, reject the 15 duplicates via the inbox, apply the 5 that never ran, and dead-letter the poison message after 4 deliveries.
4. **Verify.** Replay the raw stream in memory and compare: attribution per order, net commission per order, net balance per creator, row counts, pending count, DLQ size. Exit code is the test result, so `npm test` and CI run the same thing.

Knobs: `CRASH_AFTER_N`, `SEED`, `BATCH_SIZE`, `CLAIM_MIN_IDLE_MS`, `MAX_DELIVERIES`. To drive it by hand: `npm run produce`, then `npm run worker` in two terminals (kill one whenever you like), then `npm run verify`.

## Tradeoffs and what changes at 10×

| Now (prototype)                    | At 10×                                                                                                                  | Why                                                                                                               |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| One stream, one group              | Shard streams by `hash(store_id)`, or move to Kafka/Redpanda                                                            | A stream lives on one Redis node and in memory. Partitions give per-key ordering and days of replayable retention |
| Advisory lock per visitor          | Keep for correctness, but it mostly stops mattering once partitions give per-key ordering                               | The lock is cheap but it is a cross-worker serialization point                                                    |
| Index seek for last click          | Add a Redis read-through cache (`lastclick:{store}:{visitor}`, TTL = window), Postgres stays the fallback and the truth | Order path becomes O(1); cache misses are correct, just slower                                                    |
| `processed_events` grows forever   | Range-partition by time, drop partitions beyond the retry horizon                                                       | Same idea for `clicks`: partition by month, `BRIN` index on time                                                  |
| Workers write straight to Postgres | Workers also batch raw events to S3/R2 as Parquet                                                                       | Cheap, immutable audit log, and the replay source for recomputing attribution after a bug                         |
| Dashboards would hit `ledger`      | Incremental rollups (creator-day, brand-hour) and a columnar store for analytics                                        | A row store summing billions of rows per page load does not survive the growth curve                              |
| Logs                               | Alert on PEL age (oldest pending message), DLQ size, and "no attributions in N minutes"                                 | Consumer lag is the first thing that goes wrong, and it is silent                                                 |

## Layout

```
src/
  config.ts        stream/group names, batch size, idle threshold, attribution window, commission
  events.ts        event types, strict parser (poison detection), integer-cents commission
  schema.sql       inbox, clicks, orders, attributions (PK order_id), append-only ledger
  attribution.ts   applyEvent → applyClick / applyOrder → reconcileAttribution  (the whole point)
  worker.ts        consumer loop: XAUTOCLAIM → XREADGROUP → process → XACK, DLQ, crash hook
  producer.ts      seeded traffic generator with retries, redeliveries, late clicks, poison
scripts/
  crash-demo.ts    orchestrates produce → crash → recover → verify; exit code = result
  verify.ts        independent replay + diff
.github/workflows/crash-demo.yml   runs the demo against service containers on every push
```

Stack: TypeScript, Node 20+, `ioredis`, `pg` with plain SQL (so every `ON CONFLICT` is visible), Docker Compose for Redis 7 and Postgres 16. No framework, no ORM, under 900 lines including the demo and verifier.

## How it was built

Designed on paper first (the guarantees table above was the spec), then built in a few hours with Claude Code as a pair: it did the scaffolding, the first pass of the SQL and the demo harness, and I reviewed every query and transaction boundary, decided the locking and re-evaluation approach, and tuned the crash point and verifier until the verifier could catch a planted double payment. The AI made it fast; the design decisions are the part I would defend in a review.
