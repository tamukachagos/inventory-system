-- Canonical migration for barcode inventory app
-- Date: 2026-02-21
-- This file supersedes prior partial migrations.

BEGIN;

/* ---------------------------
   USERS
---------------------------- */

CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  student_card VARCHAR(100) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  role VARCHAR(50) NOT NULL DEFAULT 'STUDENT' CHECK (role IN ('ADMIN', 'STAFF', 'STUDENT')),
  password_hash VARCHAR(255),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE users ADD COLUMN IF NOT EXISTS student_card VARCHAR(100);
ALTER TABLE users ADD COLUMN IF NOT EXISTS name VARCHAR(255);
ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(50);
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash VARCHAR(255);
ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS must_reset_password BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_expires_at TIMESTAMPTZ;

UPDATE users SET role = 'STUDENT' WHERE role IS NULL;
UPDATE users SET is_active = TRUE WHERE is_active IS NULL;
UPDATE users SET must_reset_password = FALSE WHERE must_reset_password IS NULL;
UPDATE users SET password_changed_at = NOW() WHERE password_changed_at IS NULL;
ALTER TABLE users ALTER COLUMN role SET DEFAULT 'STUDENT';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'users_role_check'
  ) THEN
    ALTER TABLE users
      ADD CONSTRAINT users_role_check CHECK (role IN ('ADMIN', 'STAFF', 'STUDENT'));
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_student_card ON users(student_card);
CREATE INDEX IF NOT EXISTS idx_users_is_active ON users(is_active);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

/* ---------------------------
   ITEMS
---------------------------- */

CREATE TABLE IF NOT EXISTS items (
  id BIGSERIAL PRIMARY KEY,
  sku_code VARCHAR(100) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  cost NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (cost >= 0),
  quantity INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0),
  expiry_tracked BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_items_sku_code ON items(sku_code);
CREATE INDEX IF NOT EXISTS idx_items_expiry_tracked ON items(expiry_tracked);

/* ---------------------------
   ENCOUNTERS
---------------------------- */

CREATE TABLE IF NOT EXISTS encounters (
  id BIGSERIAL PRIMARY KEY,
  encounter_code VARCHAR(100) NOT NULL UNIQUE,
  scheduled_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'COMPLETED', 'CANCELLED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_encounters_code ON encounters(encounter_code);
CREATE INDEX IF NOT EXISTS idx_encounters_status ON encounters(status);
ALTER TABLE encounters ADD COLUMN IF NOT EXISTS appointment_id VARCHAR(120);
ALTER TABLE encounters ADD COLUMN IF NOT EXISTS provider_card VARCHAR(100);
ALTER TABLE encounters ADD COLUMN IF NOT EXISTS provider_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_encounters_appointment_id ON encounters(appointment_id);
CREATE INDEX IF NOT EXISTS idx_encounters_provider_user_id ON encounters(provider_user_id);

CREATE TABLE IF NOT EXISTS encounter_participants (
  id BIGSERIAL PRIMARY KEY,
  encounter_id BIGINT NOT NULL REFERENCES encounters(id) ON DELETE CASCADE,
  user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  card_code VARCHAR(100) NOT NULL,
  participant_role VARCHAR(30) NOT NULL CHECK (participant_role IN ('PROVIDER', 'ASSISTANT')),
  created_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_encounter_participants_encounter ON encounter_participants(encounter_id);
CREATE INDEX IF NOT EXISTS idx_encounter_participants_card ON encounter_participants(card_code);
CREATE UNIQUE INDEX IF NOT EXISTS idx_encounter_participants_unique_role_card
  ON encounter_participants(encounter_id, participant_role, card_code);

/* ---------------------------
   INVENTORY TRANSACTIONS
---------------------------- */

CREATE TABLE IF NOT EXISTS inventory_transactions (
  id BIGSERIAL PRIMARY KEY,
  item_id BIGINT NOT NULL REFERENCES items(id) ON DELETE RESTRICT,
  encounter_id BIGINT REFERENCES encounters(id) ON DELETE SET NULL,
  user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  type VARCHAR(20) NOT NULL CHECK (type IN ('ISSUE', 'RETURN', 'STOCK_IN', 'ADJUST_IN', 'ADJUST_OUT')),
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE inventory_transactions
  ADD COLUMN IF NOT EXISTS movement_actor_id BIGINT REFERENCES users(id) ON DELETE RESTRICT,
  ADD COLUMN IF NOT EXISTS reason_code VARCHAR(50),
  ADD COLUMN IF NOT EXISTS movement_source VARCHAR(40),
  ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(120),
  ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(120),
  ADD COLUMN IF NOT EXISTS location_code VARCHAR(60),
  ADD COLUMN IF NOT EXISTS lot_code VARCHAR(120),
  ADD COLUMN IF NOT EXISTS allow_negative BOOLEAN NOT NULL DEFAULT FALSE;

DROP TRIGGER IF EXISTS trg_inventory_transactions_immutable ON inventory_transactions;

UPDATE inventory_transactions it
SET movement_actor_id = COALESCE(
  it.user_id,
  (SELECT id FROM users ORDER BY id LIMIT 1)
)
WHERE it.movement_actor_id IS NULL;

UPDATE inventory_transactions SET reason_code = 'UNSPECIFIED' WHERE reason_code IS NULL;
UPDATE inventory_transactions SET movement_source = 'API' WHERE movement_source IS NULL;
UPDATE inventory_transactions SET correlation_id = CONCAT('legacy-', id) WHERE correlation_id IS NULL;
UPDATE inventory_transactions SET location_code = 'MAIN' WHERE location_code IS NULL;

ALTER TABLE inventory_transactions ALTER COLUMN movement_actor_id SET NOT NULL;
ALTER TABLE inventory_transactions ALTER COLUMN reason_code SET NOT NULL;
ALTER TABLE inventory_transactions ALTER COLUMN movement_source SET NOT NULL;
ALTER TABLE inventory_transactions ALTER COLUMN correlation_id SET NOT NULL;
ALTER TABLE inventory_transactions ALTER COLUMN location_code SET NOT NULL;
ALTER TABLE inventory_transactions ALTER COLUMN allow_negative SET DEFAULT FALSE;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE c.conname = 'inventory_transactions_type_check'
      AND t.relname = 'inventory_transactions'
      AND n.nspname = current_schema()
  ) THEN
    ALTER TABLE inventory_transactions DROP CONSTRAINT inventory_transactions_type_check;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE c.conname = 'inventory_transactions_type_check'
      AND t.relname = 'inventory_transactions'
      AND n.nspname = current_schema()
  ) THEN
    ALTER TABLE inventory_transactions
      ADD CONSTRAINT inventory_transactions_type_check
      CHECK (type IN ('ISSUE', 'RETURN', 'STOCK_IN', 'ADJUST_IN', 'ADJUST_OUT'));
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_transactions_actor_idemp
  ON inventory_transactions (movement_actor_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_transactions_correlation_id
  ON inventory_transactions (correlation_id);
CREATE INDEX IF NOT EXISTS idx_inventory_transactions_item_loc_lot
  ON inventory_transactions(item_id, location_code, lot_code, created_at DESC);

CREATE OR REPLACE FUNCTION prevent_inventory_transaction_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'inventory_transactions is append-only; % is not allowed', TG_OP;
END;
$$;

CREATE TRIGGER trg_inventory_transactions_immutable
BEFORE UPDATE OR DELETE ON inventory_transactions
FOR EACH ROW
EXECUTE FUNCTION prevent_inventory_transaction_mutation();

CREATE OR REPLACE VIEW inventory_balance_audit AS
SELECT
  i.id AS item_id,
  i.sku_code,
  i.name,
  'MAIN'::varchar(60) AS location_code,
  NULL::varchar(120) AS lot_code,
  i.quantity::numeric AS on_hand_qty,
  COALESCE(SUM(CASE
    WHEN it.type IN ('STOCK_IN', 'RETURN', 'ADJUST_IN') THEN it.quantity
    WHEN it.type IN ('ISSUE', 'ADJUST_OUT') THEN -1 * it.quantity
    ELSE 0
  END), 0)::numeric AS derived_qty,
  (i.quantity::numeric - COALESCE(SUM(CASE
    WHEN it.type IN ('STOCK_IN', 'RETURN', 'ADJUST_IN') THEN it.quantity
    WHEN it.type IN ('ISSUE', 'ADJUST_OUT') THEN -1 * it.quantity
    ELSE 0
  END), 0)::numeric) AS variance_qty
FROM items i
LEFT JOIN inventory_transactions it
  ON it.item_id = i.id
GROUP BY i.id, i.sku_code, i.name, i.quantity;

CREATE INDEX IF NOT EXISTS idx_inventory_transactions_item_id ON inventory_transactions(item_id);
CREATE INDEX IF NOT EXISTS idx_inventory_transactions_encounter_id ON inventory_transactions(encounter_id);
CREATE INDEX IF NOT EXISTS idx_inventory_transactions_user_id ON inventory_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_inventory_transactions_type ON inventory_transactions(type);
CREATE INDEX IF NOT EXISTS idx_inventory_transactions_created_at ON inventory_transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_inventory_transactions_filters
  ON inventory_transactions(item_id, encounter_id, user_id, type, created_at DESC);

/* ---------------------------
   MULTI-CLINIC FOUNDATION
---------------------------- */

CREATE TABLE IF NOT EXISTS organizations (
  id BIGSERIAL PRIMARY KEY,
  code VARCHAR(50) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS clinics (
  id BIGSERIAL PRIMARY KEY,
  org_id BIGINT NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  code VARCHAR(50) NOT NULL,
  name VARCHAR(255) NOT NULL,
  timezone VARCHAR(80),
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (org_id, code)
);

CREATE TABLE IF NOT EXISTS locations (
  id BIGSERIAL PRIMARY KEY,
  clinic_id BIGINT NOT NULL REFERENCES clinics(id) ON DELETE RESTRICT,
  code VARCHAR(60) NOT NULL,
  name VARCHAR(255) NOT NULL,
  location_type VARCHAR(40) NOT NULL DEFAULT 'WAREHOUSE'
    CHECK (location_type IN ('WAREHOUSE', 'OPERATORY', 'RECEIVING', 'QUARANTINE', 'VIRTUAL')),
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (clinic_id, code)
);

CREATE INDEX IF NOT EXISTS idx_clinics_org_id ON clinics(org_id);
CREATE INDEX IF NOT EXISTS idx_locations_clinic_id ON locations(clinic_id);

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS org_id BIGINT REFERENCES organizations(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS home_clinic_id BIGINT REFERENCES clinics(id) ON DELETE SET NULL;

ALTER TABLE encounters
  ADD COLUMN IF NOT EXISTS clinic_id BIGINT REFERENCES clinics(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS location_id BIGINT REFERENCES locations(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_encounters_clinic_id ON encounters(clinic_id);
CREATE INDEX IF NOT EXISTS idx_encounters_location_id ON encounters(location_id);

CREATE TABLE IF NOT EXISTS item_master (
  id BIGSERIAL PRIMARY KEY,
  sku_code VARCHAR(120) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stock_ledger (
  id BIGSERIAL PRIMARY KEY,
  org_id BIGINT NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  clinic_id BIGINT NOT NULL REFERENCES clinics(id) ON DELETE RESTRICT,
  location_id BIGINT REFERENCES locations(id) ON DELETE SET NULL,
  item_master_id BIGINT NOT NULL REFERENCES item_master(id) ON DELETE RESTRICT,
  lot_id BIGINT,
  encounter_id BIGINT REFERENCES encounters(id) ON DELETE SET NULL,
  movement_type VARCHAR(40) NOT NULL CHECK (
    movement_type IN ('RECEIPT', 'ISSUE', 'RETURN', 'ADJUST_IN', 'ADJUST_OUT', 'TRANSFER_IN', 'TRANSFER_OUT')
  ),
  qty_delta NUMERIC(14,3) NOT NULL CHECK (qty_delta <> 0),
  reason_code VARCHAR(60) NOT NULL,
  movement_source VARCHAR(60) NOT NULL,
  correlation_id VARCHAR(120) NOT NULL UNIQUE,
  idempotency_key VARCHAR(120),
  actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  allow_negative BOOLEAN NOT NULL DEFAULT FALSE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_ledger_actor_idemp
  ON stock_ledger(actor_user_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stock_ledger_balance_dims
  ON stock_ledger(clinic_id, location_id, item_master_id, lot_id, occurred_at DESC);

INSERT INTO organizations (code, name)
VALUES ('DEFAULT_ORG', 'Default Organization')
ON CONFLICT (code) DO NOTHING;

WITH org_ref AS (
  SELECT id AS org_id FROM organizations WHERE code = 'DEFAULT_ORG' ORDER BY id LIMIT 1
)
INSERT INTO clinics (org_id, code, name)
SELECT org_ref.org_id, 'CLINIC01', 'Clinic 01'
FROM org_ref
WHERE NOT EXISTS (
  SELECT 1
  FROM clinics
  WHERE clinics.org_id = org_ref.org_id
    AND clinics.code = 'CLINIC01'
);

WITH clinic_ref AS (
  SELECT id AS clinic_id FROM clinics WHERE code = 'CLINIC01' ORDER BY id LIMIT 1
)
INSERT INTO locations (clinic_id, code, name, location_type)
SELECT clinic_ref.clinic_id, 'MAIN', 'Main Warehouse', 'WAREHOUSE'
FROM clinic_ref
WHERE NOT EXISTS (
  SELECT 1
  FROM locations
  WHERE locations.clinic_id = clinic_ref.clinic_id
    AND locations.code = 'MAIN'
);

WITH org_ref AS (
  SELECT id AS org_id FROM organizations WHERE code = 'DEFAULT_ORG' ORDER BY id LIMIT 1
),
clinic_ref AS (
  SELECT id AS clinic_id FROM clinics WHERE code = 'CLINIC01' AND org_id = (SELECT org_id FROM org_ref) ORDER BY id LIMIT 1
)
UPDATE users u
SET org_id = COALESCE(u.org_id, (SELECT org_id FROM org_ref)),
    home_clinic_id = COALESCE(u.home_clinic_id, (SELECT clinic_id FROM clinic_ref))
WHERE u.org_id IS NULL OR u.home_clinic_id IS NULL;

INSERT INTO item_master (sku_code, name, active, created_at, updated_at)
SELECT i.sku_code, i.name, TRUE, NOW(), NOW()
FROM items i
ON CONFLICT (sku_code) DO UPDATE SET
  name = EXCLUDED.name,
  updated_at = NOW();

/* ---------------------------
   LOT + EXPIRY MANAGEMENT
---------------------------- */

CREATE TABLE IF NOT EXISTS inventory_lot_balances (
  id BIGSERIAL PRIMARY KEY,
  clinic_id BIGINT REFERENCES clinics(id) ON DELETE SET NULL,
  location_code VARCHAR(60) NOT NULL DEFAULT 'MAIN',
  item_id BIGINT NOT NULL REFERENCES items(id) ON DELETE RESTRICT,
  lot_code VARCHAR(120) NOT NULL,
  expiry_date DATE NOT NULL,
  quantity NUMERIC(14,3) NOT NULL DEFAULT 0 CHECK (quantity >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (clinic_id, location_code, item_id, lot_code)
);

CREATE INDEX IF NOT EXISTS idx_inventory_lot_balances_item_expiry
  ON inventory_lot_balances(item_id, expiry_date, quantity DESC);
CREATE INDEX IF NOT EXISTS idx_inventory_lot_balances_clinic_expiry
  ON inventory_lot_balances(clinic_id, expiry_date, quantity DESC);

CREATE TABLE IF NOT EXISTS clinic_alert_digests (
  id BIGSERIAL PRIMARY KEY,
  clinic_id BIGINT REFERENCES clinics(id) ON DELETE CASCADE,
  digest_date DATE NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (clinic_id, digest_date)
);

CREATE OR REPLACE VIEW inventory_expiry_dashboard AS
SELECT
  ilb.clinic_id,
  ilb.item_id,
  i.sku_code,
  i.name,
  ilb.lot_code,
  ilb.expiry_date,
  ilb.quantity::numeric(14,3) AS on_hand_qty,
  CASE
    WHEN ilb.expiry_date < CURRENT_DATE THEN 'EXPIRED'
    WHEN ilb.expiry_date <= CURRENT_DATE + INTERVAL '30 days' THEN 'EXP_30'
    WHEN ilb.expiry_date <= CURRENT_DATE + INTERVAL '60 days' THEN 'EXP_60'
    WHEN ilb.expiry_date <= CURRENT_DATE + INTERVAL '90 days' THEN 'EXP_90'
    ELSE 'HEALTHY'
  END AS expiry_bucket
FROM inventory_lot_balances ilb
JOIN items i ON i.id = ilb.item_id
WHERE ilb.quantity > 0;

/* ---------------------------
   REORDER RECOMMENDATION ENGINE
---------------------------- */

CREATE TABLE IF NOT EXISTS clinic_item_settings (
  id BIGSERIAL PRIMARY KEY,
  clinic_id BIGINT REFERENCES clinics(id) ON DELETE RESTRICT,
  item_master_id BIGINT NOT NULL REFERENCES item_master(id) ON DELETE RESTRICT,
  location_code VARCHAR(60) NOT NULL DEFAULT 'MAIN',
  par_level NUMERIC(14,3) NOT NULL DEFAULT 0,
  on_order_qty NUMERIC(14,3) NOT NULL DEFAULT 0,
  lead_time_days INTEGER NOT NULL DEFAULT 14,
  expiry_risk_weight NUMERIC(8,3) NOT NULL DEFAULT 1,
  is_stocked BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (clinic_id, item_master_id, location_code)
);

CREATE TABLE IF NOT EXISTS procurement_orders (
  id BIGSERIAL PRIMARY KEY,
  clinic_id BIGINT REFERENCES clinics(id) ON DELETE SET NULL,
  item_master_id BIGINT NOT NULL REFERENCES item_master(id) ON DELETE RESTRICT,
  location_code VARCHAR(60) NOT NULL DEFAULT 'MAIN',
  qty_outstanding NUMERIC(14,3) NOT NULL CHECK (qty_outstanding >= 0),
  expected_date DATE,
  status VARCHAR(30) NOT NULL DEFAULT 'OPEN'
    CHECK (status IN ('OPEN', 'PARTIAL', 'RECEIVED', 'CANCELLED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE VIEW clinic_item_on_hand_v AS
SELECT
  sl.clinic_id,
  sl.item_master_id,
  COALESCE(SUM(sl.qty_delta), 0)::numeric(14,3) AS on_hand_qty
FROM stock_ledger sl
GROUP BY sl.clinic_id, sl.item_master_id;

CREATE OR REPLACE VIEW clinic_item_usage_velocity_v AS
SELECT
  sl.clinic_id,
  sl.item_master_id,
  COALESCE(SUM(CASE WHEN sl.occurred_at >= NOW() - INTERVAL '30 days' AND sl.qty_delta < 0 THEN ABS(sl.qty_delta) ELSE 0 END), 0)::numeric(14,3) AS usage_30d,
  COALESCE(SUM(CASE WHEN sl.occurred_at >= NOW() - INTERVAL '60 days' AND sl.qty_delta < 0 THEN ABS(sl.qty_delta) ELSE 0 END), 0)::numeric(14,3) AS usage_60d,
  COALESCE(SUM(CASE WHEN sl.occurred_at >= NOW() - INTERVAL '90 days' AND sl.qty_delta < 0 THEN ABS(sl.qty_delta) ELSE 0 END), 0)::numeric(14,3) AS usage_90d
FROM stock_ledger sl
GROUP BY sl.clinic_id, sl.item_master_id;

CREATE OR REPLACE VIEW clinic_item_expiry_risk_v AS
SELECT
  ilb.clinic_id,
  i.sku_code,
  COALESCE(SUM(CASE WHEN ilb.expiry_date <= CURRENT_DATE + INTERVAL '30 days' THEN ilb.quantity ELSE 0 END), 0)::numeric(14,3) AS qty_exp_30d,
  COALESCE(SUM(ilb.quantity), 0)::numeric(14,3) AS total_qty,
  CASE
    WHEN COALESCE(SUM(ilb.quantity), 0) = 0 THEN 0::numeric
    ELSE COALESCE(SUM(CASE WHEN ilb.expiry_date <= CURRENT_DATE + INTERVAL '30 days' THEN ilb.quantity ELSE 0 END), 0)::numeric / NULLIF(COALESCE(SUM(ilb.quantity), 0)::numeric, 0)
  END AS expiry_risk_ratio
FROM inventory_lot_balances ilb
JOIN items i ON i.id = ilb.item_id
WHERE ilb.quantity > 0
GROUP BY ilb.clinic_id, i.sku_code;

DROP MATERIALIZED VIEW IF EXISTS reorder_recommendation_basis_mv;
CREATE MATERIALIZED VIEW reorder_recommendation_basis_mv AS
WITH open_orders AS (
  SELECT clinic_id, item_master_id, location_code, COALESCE(SUM(qty_outstanding), 0)::numeric(14,3) AS on_order_qty
  FROM procurement_orders
  WHERE status IN ('OPEN', 'PARTIAL')
  GROUP BY clinic_id, item_master_id, location_code
)
SELECT
  cis.clinic_id,
  c.name AS clinic_name,
  cis.location_code,
  cis.item_master_id,
  im.sku_code,
  im.name AS item_name,
  cis.par_level::numeric(14,3) AS par_level,
  COALESCE(oh.on_hand_qty, 0)::numeric(14,3) AS on_hand_qty,
  (COALESCE(cis.on_order_qty, 0) + COALESCE(oo.on_order_qty, 0))::numeric(14,3) AS on_order_qty,
  COALESCE(uv.usage_30d, 0)::numeric(14,3) AS usage_30d,
  COALESCE(uv.usage_60d, 0)::numeric(14,3) AS usage_60d,
  COALESCE(uv.usage_90d, 0)::numeric(14,3) AS usage_90d,
  GREATEST(1, COALESCE(cis.lead_time_days, 14))::int AS lead_time_days,
  COALESCE(er.expiry_risk_ratio, 0)::numeric(14,4) AS expiry_risk_ratio,
  GREATEST(cis.par_level - (COALESCE(oh.on_hand_qty, 0) + COALESCE(cis.on_order_qty, 0) + COALESCE(oo.on_order_qty, 0)), 0)::numeric(14,3) AS shortage_vs_par,
  GREATEST((COALESCE(oh.on_hand_qty, 0) + COALESCE(cis.on_order_qty, 0) + COALESCE(oo.on_order_qty, 0)) - cis.par_level, 0)::numeric(14,3) AS excess_vs_par
FROM clinic_item_settings cis
JOIN item_master im ON im.id = cis.item_master_id
LEFT JOIN clinics c ON c.id = cis.clinic_id
LEFT JOIN clinic_item_on_hand_v oh ON oh.clinic_id = cis.clinic_id AND oh.item_master_id = cis.item_master_id
LEFT JOIN clinic_item_usage_velocity_v uv ON uv.clinic_id = cis.clinic_id AND uv.item_master_id = cis.item_master_id
LEFT JOIN clinic_item_expiry_risk_v er ON er.clinic_id = cis.clinic_id AND er.sku_code = im.sku_code
LEFT JOIN open_orders oo ON oo.clinic_id = cis.clinic_id AND oo.item_master_id = cis.item_master_id AND oo.location_code = cis.location_code
WHERE cis.is_stocked = TRUE;
CREATE UNIQUE INDEX IF NOT EXISTS idx_reorder_basis_unique
  ON reorder_recommendation_basis_mv(clinic_id, item_master_id, location_code);

/* ---------------------------
   OFFLINE SYNC LAYER
---------------------------- */

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

/* ---------------------------
   ENTERPRISE RELEASE CONTROLS
---------------------------- */

CREATE TABLE IF NOT EXISTS schema_versions (
  id BIGSERIAL PRIMARY KEY,
  version_tag VARCHAR(120) NOT NULL,
  checksum VARCHAR(128) NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  applied_by VARCHAR(120),
  notes TEXT,
  UNIQUE (version_tag, checksum)
);

CREATE TABLE IF NOT EXISTS feature_flags (
  id BIGSERIAL PRIMARY KEY,
  clinic_id BIGINT,
  flag_key VARCHAR(100) NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT FALSE,
  rollout_percentage INTEGER NOT NULL DEFAULT 100 CHECK (rollout_percentage >= 0 AND rollout_percentage <= 100),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
  updated_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (clinic_id, flag_key)
);

CREATE INDEX IF NOT EXISTS idx_feature_flags_key ON feature_flags(flag_key, clinic_id);

/* ---------------------------
   AUDIT LOGS
---------------------------- */

CREATE TABLE IF NOT EXISTS audit_logs (
  id BIGSERIAL PRIMARY KEY,
  action VARCHAR(120) NOT NULL,
  actor_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  actor_role VARCHAR(50),
  severity VARCHAR(20) NOT NULL DEFAULT 'INFO' CHECK (severity IN ('INFO', 'WARN', 'ERROR')),
  request_path VARCHAR(255),
  request_method VARCHAR(10),
  ip_address VARCHAR(100),
  details JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_logs_actor_user_id ON audit_logs(actor_user_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_severity ON audit_logs(severity);

/* ---------------------------
   FRAUD FLAGS
---------------------------- */

CREATE TABLE IF NOT EXISTS fraud_flags (
  id BIGSERIAL PRIMARY KEY,
  flag_type VARCHAR(80) NOT NULL,
  score NUMERIC(8,2) NOT NULL DEFAULT 0,
  status VARCHAR(20) NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'REVIEWING', 'RESOLVED', 'DISMISSED')),
  signature VARCHAR(255) NOT NULL UNIQUE,
  summary TEXT NOT NULL,
  metadata JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_flags_status ON fraud_flags(status);
CREATE INDEX IF NOT EXISTS idx_fraud_flags_score ON fraud_flags(score DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_flags_created_at ON fraud_flags(created_at DESC);

/* ---------------------------
   EVENT LEDGER (IMMUTABLE APPEND-ONLY)
---------------------------- */

CREATE TABLE IF NOT EXISTS event_ledger (
  id BIGSERIAL PRIMARY KEY,
  actor_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  actor_role VARCHAR(50),
  event_type VARCHAR(120) NOT NULL,
  entity_type VARCHAR(80),
  entity_id VARCHAR(120),
  payload JSONB,
  previous_hash VARCHAR(128),
  event_hash VARCHAR(128) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_ledger_created_at ON event_ledger(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_event_ledger_event_type ON event_ledger(event_type);
CREATE INDEX IF NOT EXISTS idx_event_ledger_actor_user_id ON event_ledger(actor_user_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_event_ledger_event_hash ON event_ledger(event_hash);

/* ---------------------------
   CYCLE COUNT WORKFLOW
---------------------------- */

CREATE TABLE IF NOT EXISTS cycle_counts (
  id BIGSERIAL PRIMARY KEY,
  code VARCHAR(80) NOT NULL UNIQUE,
  status VARCHAR(20) NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'SUBMITTED', 'APPROVED', 'REJECTED')),
  created_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
  approved_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  submitted_at TIMESTAMPTZ,
  approved_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS cycle_count_lines (
  id BIGSERIAL PRIMARY KEY,
  cycle_count_id BIGINT NOT NULL REFERENCES cycle_counts(id) ON DELETE CASCADE,
  item_id BIGINT NOT NULL REFERENCES items(id) ON DELETE RESTRICT,
  expected_qty INTEGER NOT NULL,
  counted_qty INTEGER NOT NULL,
  variance INTEGER NOT NULL,
  entered_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (cycle_count_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_cycle_counts_status ON cycle_counts(status);
CREATE INDEX IF NOT EXISTS idx_cycle_count_lines_cycle_count_id ON cycle_count_lines(cycle_count_id);
CREATE INDEX IF NOT EXISTS idx_cycle_count_lines_item_id ON cycle_count_lines(item_id);

/* ---------------------------
   PURCHASE IMPORT + RECONCILIATION
---------------------------- */

CREATE TABLE IF NOT EXISTS purchase_import_batches (
  id BIGSERIAL PRIMARY KEY,
  source_name VARCHAR(200) NOT NULL,
  imported_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
  total_rows INTEGER NOT NULL DEFAULT 0,
  matched_rows INTEGER NOT NULL DEFAULT 0,
  created_rows INTEGER NOT NULL DEFAULT 0,
  unmatched_rows INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS purchase_import_lines (
  id BIGSERIAL PRIMARY KEY,
  batch_id BIGINT NOT NULL REFERENCES purchase_import_batches(id) ON DELETE CASCADE,
  line_no INTEGER NOT NULL,
  sku_code VARCHAR(100),
  item_name VARCHAR(255),
  purchased_qty INTEGER NOT NULL DEFAULT 0,
  purchased_cost NUMERIC(12,2),
  matched_item_id BIGINT REFERENCES items(id) ON DELETE SET NULL,
  match_status VARCHAR(20) NOT NULL CHECK (match_status IN ('MATCHED', 'CREATED', 'UNMATCHED')),
  raw_row JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_purchase_import_batches_created_at ON purchase_import_batches(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_purchase_import_lines_batch_id ON purchase_import_lines(batch_id);
CREATE INDEX IF NOT EXISTS idx_purchase_import_lines_match_status ON purchase_import_lines(match_status);
CREATE INDEX IF NOT EXISTS idx_purchase_import_lines_matched_item_id ON purchase_import_lines(matched_item_id);

/* ---------------------------
   VENDOR IMPORT TEMPLATES
---------------------------- */

CREATE TABLE IF NOT EXISTS import_vendor_templates (
  id BIGSERIAL PRIMARY KEY,
  vendor_name VARCHAR(200) NOT NULL UNIQUE,
  description TEXT,
  required_columns JSONB NOT NULL DEFAULT '[]'::jsonb,
  column_map JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_import_vendor_templates_vendor ON import_vendor_templates(vendor_name);
CREATE INDEX IF NOT EXISTS idx_import_vendor_templates_active ON import_vendor_templates(is_active);

/* ---------------------------
   PRINT JOB TRACKING
---------------------------- */

CREATE TABLE IF NOT EXISTS print_jobs (
  id BIGSERIAL PRIMARY KEY,
  label_type VARCHAR(80) NOT NULL,
  requester_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  printer_ip VARCHAR(100) NOT NULL,
  printer_port INTEGER NOT NULL DEFAULT 9100,
  copies INTEGER NOT NULL DEFAULT 1,
  status VARCHAR(20) NOT NULL DEFAULT 'QUEUED' CHECK (status IN ('QUEUED', 'SUCCESS', 'FAILED')),
  attempts INTEGER NOT NULL DEFAULT 0,
  error_message TEXT,
  zpl TEXT NOT NULL,
  payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  printed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_print_jobs_status ON print_jobs(status);
CREATE INDEX IF NOT EXISTS idx_print_jobs_created_at ON print_jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_print_jobs_requester ON print_jobs(requester_user_id);

COMMIT;
