BEGIN;

CREATE TABLE IF NOT EXISTS sync_inbox (
  id BIGSERIAL PRIMARY KEY,
  device_id VARCHAR(120),
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  action_type VARCHAR(60) NOT NULL,
  path VARCHAR(255) NOT NULL,
  method VARCHAR(10) NOT NULL,
  idempotency_key VARCHAR(120) NOT NULL,
  action_fingerprint VARCHAR(120),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status VARCHAR(30) NOT NULL DEFAULT 'PENDING'
    CHECK (status IN ('PENDING', 'APPLIED', 'REJECTED', 'CONFLICT', 'DUPLICATE')),
  conflict_code VARCHAR(60),
  result_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ,
  UNIQUE (user_id, idempotency_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_inbox_fingerprint_issue_receipt
  ON sync_inbox(user_id, action_type, action_fingerprint)
  WHERE action_type IN ('STOCK_IN', 'ISSUE_ITEM', 'ISSUE_SCAN', 'RETURN_ITEM', 'RETURN_SCAN')
    AND action_fingerprint IS NOT NULL;

CREATE TABLE IF NOT EXISTS sync_outbox (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  target_device_id VARCHAR(120),
  event_type VARCHAR(80) NOT NULL,
  event_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  acked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sync_outbox_user_id ON sync_outbox(user_id, id);

COMMIT;
