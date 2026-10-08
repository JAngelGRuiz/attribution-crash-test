import Redis from 'ioredis';
import { config } from './config.js';

export function createRedis(): Redis {
  return new Redis(config.redisUrl, { maxRetriesPerRequest: null, lazyConnect: false });
}

/** Create the consumer group at id 0 so it reads the stream from the beginning. Idempotent. */
export async function ensureGroup(redis: Redis): Promise<void> {
  try {
    await redis.xgroup('CREATE', config.stream, config.group, '0', 'MKSTREAM');
  } catch (e) {
    if (!String(e).includes('BUSYGROUP')) throw e;
  }
}

export interface StreamMsg {
  id: string;
  payload: string;
}

export function toMsg([id, fields]: [string, string[]]): StreamMsg {
  const i = fields.indexOf('payload');
  return { id, payload: i >= 0 ? fields[i + 1] : '' };
}

/** Count of messages delivered to the group but not yet acknowledged. */
export async function pendingCount(redis: Redis): Promise<number> {
  const summary = (await redis.xpending(config.stream, config.group)) as [number, ...unknown[]];
  return Number(summary[0] ?? 0);
}
