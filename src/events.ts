/**
 * Event shapes as they travel through the stream. In production these come from
 * the first-party pixel (clicks) and the Shopify orders webhook (orders).
 */
export interface ClickEvent {
  event_id: string;
  type: 'click';
  store_id: string;
  visitor_id: string;
  creator_id: string;
  submission_id: string;
  /** Event time in epoch ms (when it happened, not when it arrived). */
  ts: number;
}

export interface OrderEvent {
  event_id: string;
  type: 'order';
  store_id: string;
  visitor_id: string;
  order_id: string;
  amount_cents: number;
  ts: number;
}

export type PipelineEvent = ClickEvent | OrderEvent;

export class InvalidEventError extends Error {}

const str = (o: Record<string, unknown>, k: string): string => {
  const v = o[k];
  if (typeof v !== 'string' || v.length === 0) throw new InvalidEventError(`missing or invalid "${k}"`);
  return v;
};
const num = (o: Record<string, unknown>, k: string): number => {
  const v = o[k];
  if (typeof v !== 'number' || !Number.isFinite(v)) throw new InvalidEventError(`missing or invalid "${k}"`);
  return v;
};

/** Strict parse. Anything malformed throws, which is how a poison message reveals itself. */
export function parseEvent(payload: string): PipelineEvent {
  let o: Record<string, unknown>;
  try {
    o = JSON.parse(payload);
  } catch {
    throw new InvalidEventError('payload is not valid JSON');
  }
  const base = { event_id: str(o, 'event_id'), store_id: str(o, 'store_id'), visitor_id: str(o, 'visitor_id'), ts: num(o, 'ts') };
  const type = str(o, 'type');
  if (type === 'click') return { ...base, type, creator_id: str(o, 'creator_id'), submission_id: str(o, 'submission_id') };
  if (type === 'order') return { ...base, type, order_id: str(o, 'order_id'), amount_cents: num(o, 'amount_cents') };
  throw new InvalidEventError(`unknown event type "${type}"`);
}

/** Integer cents in, integer cents out. Never floats for money. */
export function commissionFor(amountCents: number, bps: number): number {
  return Math.floor((amountCents * bps) / 10_000);
}
