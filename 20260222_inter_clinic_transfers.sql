BEGIN;

/* ==========================================================
   INTER-CLINIC TRANSFERS
   ========================================================== */

CREATE TABLE IF NOT EXISTS transfer_requests (
  id BIGSERIAL PRIMARY KEY,
  org_id BIGINT REFERENCES organizations(id) ON DELETE SET NULL,
  from_clinic_id BIGINT NOT NULL REFERENCES clinics(id) ON DELETE RESTRICT,
  to_clinic_id BIGINT NOT NULL REFERENCES clinics(id) ON DELETE RESTRICT,
  status VARCHAR(30) NOT NULL DEFAULT 'PENDING'
    CHECK (status IN (
      'PENDING',
      'APPROVED',
      'REJECTED',
      'PARTIALLY_PICKED',
      'IN_TRANSIT',
      'PARTIALLY_RECEIVED',
      'RECEIVED',
      'PARTIALLY_CANCELLED',
      'CANCELLED'
    )),
  needed_by TIMESTAMPTZ,
  requested_by BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  approved_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
  approved_at TIMESTAMPTZ,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (from_clinic_id <> to_clinic_id)
);

CREATE INDEX IF NOT EXISTS idx_transfer_requests_status ON transfer_requests(status);
CREATE INDEX IF NOT EXISTS idx_transfer_requests_from_clinic ON transfer_requests(from_clinic_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transfer_requests_to_clinic ON transfer_requests(to_clinic_id, created_at DESC);

CREATE TABLE IF NOT EXISTS transfer_request_lines (
  id BIGSERIAL PRIMARY KEY,
  transfer_request_id BIGINT NOT NULL REFERENCES transfer_requests(id) ON DELETE CASCADE,
  item_id BIGINT NOT NULL REFERENCES items(id) ON DELETE RESTRICT,
  requested_qty NUMERIC(14,3) NOT NULL CHECK (requested_qty > 0),
  approved_qty NUMERIC(14,3) NOT NULL DEFAULT 0 CHECK (approved_qty >= 0),
  picked_qty NUMERIC(14,3) NOT NULL DEFAULT 0 CHECK (picked_qty >= 0),
  received_qty NUMERIC(14,3) NOT NULL DEFAULT 0 CHECK (received_qty >= 0),
  cancelled_qty NUMERIC(14,3) NOT NULL DEFAULT 0 CHECK (cancelled_qty >= 0),
  line_status VARCHAR(30) NOT NULL DEFAULT 'PENDING'
    CHECK (line_status IN (
      'PENDING',
      'APPROVED',
      'PARTIALLY_PICKED',
      'IN_TRANSIT',
      'PARTIALLY_RECEIVED',
      'RECEIVED',
      'PARTIALLY_CANCELLED',
      'CANCELLED',
      'REJECTED'
    )),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (transfer_request_id, item_id),
  CHECK (approved_qty <= requested_qty),
  CHECK (picked_qty <= approved_qty),
  CHECK (received_qty <= picked_qty),
  CHECK (cancelled_qty <= approved_qty)
);

CREATE INDEX IF NOT EXISTS idx_transfer_request_lines_request_id ON transfer_request_lines(transfer_request_id);

CREATE TABLE IF NOT EXISTS transfer_action_idempotency (
  id BIGSERIAL PRIMARY KEY,
  transfer_request_id BIGINT REFERENCES transfer_requests(id) ON DELETE CASCADE,
  action_name VARCHAR(50) NOT NULL,
  actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  idempotency_key VARCHAR(120) NOT NULL,
  response_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (actor_user_id, action_name, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_transfer_action_idempotency_request
  ON transfer_action_idempotency(transfer_request_id, action_name);

COMMIT;
