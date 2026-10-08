export const config = {
  redisUrl: process.env.REDIS_URL ?? 'redis://127.0.0.1:6390',
  databaseUrl: process.env.DATABASE_URL ?? 'postgres://trybe:trybe@127.0.0.1:5440/attribution',

  /** Redis Stream that carries click + order events (what a pixel collector would XADD into). */
  stream: 'events',
  /** Messages that failed too many times land here instead of blocking the group forever. */
  dlqStream: 'events:dlq',
  /** Consumer group. Every message is delivered to exactly one consumer in the group. */
  group: 'attribution',

  batchSize: Number(process.env.BATCH_SIZE ?? 20),
  blockMs: 1000,
  /** A pending message idle longer than this is assumed to belong to a dead consumer and gets reclaimed. */
  claimMinIdleMs: Number(process.env.CLAIM_MIN_IDLE_MS ?? 1500),
  /** After this many deliveries a message is considered poison and moved to the DLQ. */
  maxDeliveries: Number(process.env.MAX_DELIVERIES ?? 3),

  /** Last-click attribution window. */
  attributionWindowSec: 7 * 24 * 3600,
  /** Creator commission on attributed sales, in basis points (1000 = 10%). */
  commissionBps: 1000,
};
