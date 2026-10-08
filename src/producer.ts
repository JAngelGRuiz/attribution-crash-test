/**
 * Deterministic traffic generator standing in for the pixel collector and the
 * Shopify orders webhook. Besides clean traffic it injects the messy things a
 * real pipeline sees:
 *   - exact retries (same event_id sent twice)
 *   - order redeliveries (same order_id, NEW event_id)
 *   - late clicks (event time in the past, arrives now)
 *   - one poison message (truncated JSON)
 */
import type Redis from 'ioredis';
import { config } from './config.js';
import type { PipelineEvent } from './events.js';
import { createRedis } from './redis.js';

export interface Scenario {
  messages: string[];
  summary: {
    messages: number;
    uniqueEvents: number;
    clicks: number;
    lateClicks: number;
    orders: number;
    exactRetries: number;
    orderRedeliveries: number;
    poison: number;
  };
}

function mulberry32(seed: number) {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function generateScenario(seed = 42, count = 300): Scenario {
  const rnd = mulberry32(seed);
  const pick = <T>(arr: T[]): T => arr[Math.floor(rnd() * arr.length)];

  const stores = ['store_haus', 'store_cuts', 'store_calai'];
  const creators = Array.from({ length: 8 }, (_, i) => `creator_${i + 1}`);
  const visitors = Array.from({ length: 60 }, (_, i) => `v_${String(i + 1).padStart(3, '0')}`);
  const visitorStore = new Map(visitors.map((v) => [v, pick(stores)]));

  let seq = 0;
  const nextId = () => `evt_${String(++seq).padStart(4, '0')}`;
  let orderSeq = 0;
  let ts = Date.now() - 6 * 3600 * 1000;

  const messages: string[] = [];
  const s = { messages: 0, uniqueEvents: 0, clicks: 0, lateClicks: 0, orders: 0, exactRetries: 0, orderRedeliveries: 0, poison: 0 };
  const push = (ev: PipelineEvent) => messages.push(JSON.stringify(ev));

  for (let i = 0; i < count; i++) {
    ts += 1000 + Math.floor(rnd() * 30_000);
    const visitor_id = pick(visitors);
    const store_id = visitorStore.get(visitor_id)!;
    const roll = rnd();
    let ev: PipelineEvent;

    if (roll < 0.60) {
      ev = { event_id: nextId(), type: 'click', store_id, visitor_id, creator_id: pick(creators), submission_id: `sub_${1 + Math.floor(rnd() * 40)}`, ts };
      s.clicks++;
    } else if (roll < 0.66) {
      // Late click: happened 10s–3min ago, only reaches us now.
      const late = ts - (10_000 + Math.floor(rnd() * 170_000));
      ev = { event_id: nextId(), type: 'click', store_id, visitor_id, creator_id: pick(creators), submission_id: `sub_${1 + Math.floor(rnd() * 40)}`, ts: late };
      s.clicks++;
      s.lateClicks++;
    } else {
      ev = { event_id: nextId(), type: 'order', store_id, visitor_id, order_id: `ord_${String(++orderSeq).padStart(4, '0')}`, amount_cents: 1500 + Math.floor(rnd() * 20_000), ts };
      s.orders++;
    }
    push(ev);
    s.uniqueEvents++;

    if (rnd() < 0.04) {
      push(ev); // sendBeacon / webhook retry: identical message
      s.exactRetries++;
    }
    if (ev.type === 'order' && rnd() < 0.06) {
      push({ ...ev, event_id: nextId() }); // Shopify redelivery: new webhook id, same order
      s.orderRedeliveries++;
      s.uniqueEvents++;
    }
    if (i === Math.floor(count / 2)) {
      messages.push('{"event_id":"evt_poison","type":"order","store_id":"store_haus","visitor_id":"v_0'); // truncated in transit
      s.poison++;
    }
  }
  s.messages = messages.length;
  return { messages, summary: s };
}

export async function produce(redis: Redis, scenario: Scenario): Promise<void> {
  const pipe = redis.pipeline();
  for (const payload of scenario.messages) pipe.xadd(config.stream, '*', 'payload', payload);
  await pipe.exec();
}

// Standalone: `npm run produce`
if (process.argv[1]?.endsWith('producer.ts')) {
  const redis = createRedis();
  const scenario = generateScenario();
  await produce(redis, scenario);
  console.log(scenario.summary);
  redis.disconnect();
}
