-- Idempotency layer 1: the "inbox". One row per event_id ever applied.
-- Inserted in the same transaction as the business write, so a redelivered
-- message (crash before XACK, pixel retry, webhook retry) is a no-op.
CREATE TABLE IF NOT EXISTS processed_events (
  event_id     TEXT PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS clicks (
  event_id      TEXT PRIMARY KEY,
  store_id      TEXT NOT NULL,
  visitor_id    TEXT NOT NULL,
  creator_id    TEXT NOT NULL,
  submission_id TEXT NOT NULL,
  clicked_at    TIMESTAMPTZ NOT NULL
);
-- Serves "last click for this visitor before time T" as an index seek, not a scan.
CREATE INDEX IF NOT EXISTS clicks_visitor_time_idx ON clicks (store_id, visitor_id, clicked_at DESC);

-- Idempotency layer 2: an order exists once, no matter how many events mention it
-- (Shopify can redeliver orders/create with a brand-new webhook id).
CREATE TABLE IF NOT EXISTS orders (
  order_id     TEXT PRIMARY KEY,
  store_id     TEXT NOT NULL,
  visitor_id   TEXT NOT NULL,
  amount_cents BIGINT NOT NULL,
  ordered_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS orders_visitor_time_idx ON orders (store_id, visitor_id, ordered_at);

-- Current attribution per order. PRIMARY KEY (order_id) makes "attributed twice" impossible.
CREATE TABLE IF NOT EXISTS attributions (
  order_id       TEXT PRIMARY KEY REFERENCES orders (order_id),
  click_event_id TEXT NOT NULL REFERENCES clicks (event_id),
  creator_id     TEXT NOT NULL,
  submission_id  TEXT NOT NULL,
  attributed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency layer 3: money is an append-only ledger in integer cents.
-- Balances are SUM(amount_cents); nothing here is ever UPDATEd or DELETEd.
-- Moving an order to another creator = a reversal row + a new commission row.
CREATE TABLE IF NOT EXISTS ledger (
  id              BIGSERIAL PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  creator_id      TEXT NOT NULL,
  order_id        TEXT NOT NULL,
  entry_type      TEXT NOT NULL CHECK (entry_type IN ('commission', 'reversal')),
  amount_cents    BIGINT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ledger_creator_idx ON ledger (creator_id);
CREATE INDEX IF NOT EXISTS ledger_order_idx ON ledger (order_id);
