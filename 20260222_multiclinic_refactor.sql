BEGIN;

/* ==========================================================
   MULTI-CLINIC FOUNDATION
   - org -> clinic -> location hierarchy
   - global item_master
   - append-only stock_ledger (source of truth)
   - backwards-compatible bridge from legacy tables
   ========================================================== */

/* ---------------------------
   HIERARCHY TABLES
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

/* ---------------------------
   GLOBAL ITEM MASTER
---------------------------- */

CREATE TABLE IF NOT EXISTS item_master (
  id BIGSERIAL PRIMARY KEY,
  sku_code VARCHAR(120) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  uom VARCHAR(30) NOT NULL DEFAULT 'EA',
  category VARCHAR(120),
  manufacturer VARCHAR(255),
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS clinic_item_settings (
  id BIGSERIAL PRIMARY KEY,
  clinic_id BIGINT NOT NULL REFERENCES clinics(id) ON DELETE RESTRICT,
  item_master_id BIGINT NOT NULL REFERENCES item_master(id) ON DELETE RESTRICT,
  standard_cost NUMERIC(12,2) NOT NULL DEFAULT 0,
  reorder_point NUMERIC(14,3) NOT NULL DEFAULT 0,
  par_level NUMERIC(14,3) NOT NULL DEFAULT 0,
  is_stocked BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (clinic_id, item_master_id)
);

CREATE INDEX IF NOT EXISTS idx_clinic_item_settings_clinic ON clinic_item_settings(clinic_id);
CREATE INDEX IF NOT EXISTS idx_clinic_item_settings_item ON clinic_item_settings(item_master_id);

CREATE TABLE IF NOT EXISTS inventory_lots (
  id BIGSERIAL PRIMARY KEY,
  clinic_id BIGINT NOT NULL REFERENCES clinics(id) ON DELETE RESTRICT,
  location_id BIGINT REFERENCES locations(id) ON DELETE SET NULL,
  item_master_id BIGINT NOT NULL REFERENCES item_master(id) ON DELETE RESTRICT,
  lot_code VARCHAR(120),
  expiry_date DATE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (clinic_id, item_master_id, lot_code)
);

CREATE INDEX IF NOT EXISTS idx_inventory_lots_item_clinic ON inventory_lots(item_master_id, clinic_id);
CREATE INDEX IF NOT EXISTS idx_inventory_lots_expiry ON inventory_lots(expiry_date);

/* ---------------------------
   USER / RBAC CLINIC SCOPE
---------------------------- */

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS org_id BIGINT REFERENCES organizations(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS home_clinic_id BIGINT REFERENCES clinics(id) ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS user_clinic_roles (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  clinic_id BIGINT NOT NULL REFERENCES clinics(id) ON DELETE CASCADE,
  role VARCHAR(60) NOT NULL
    CHECK (role IN ('ORG_ADMIN', 'CLINIC_MANAGER', 'WAREHOUSE_STAFF', 'AUDITOR', 'READ_ONLY')),
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, clinic_id, role)
);

CREATE INDEX IF NOT EXISTS idx_user_clinic_roles_user ON user_clinic_roles(user_id);
CREATE INDEX IF NOT EXISTS idx_user_clinic_roles_clinic ON user_clinic_roles(clinic_id);

/* ---------------------------
   ENCOUNTER CLINIC SCOPE
---------------------------- */

ALTER TABLE encounters
  ADD COLUMN IF NOT EXISTS clinic_id BIGINT REFERENCES clinics(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS location_id BIGINT REFERENCES locations(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_encounters_clinic_id ON encounters(clinic_id);
CREATE INDEX IF NOT EXISTS idx_encounters_location_id ON encounters(location_id);

/* ---------------------------
   APPEND-ONLY STOCK LEDGER (SOURCE OF TRUTH)
---------------------------- */

CREATE TABLE IF NOT EXISTS stock_ledger (
  id BIGSERIAL PRIMARY KEY,
  org_id BIGINT NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  clinic_id BIGINT NOT NULL REFERENCES clinics(id) ON DELETE RESTRICT,
  location_id BIGINT REFERENCES locations(id) ON DELETE SET NULL,
  item_master_id BIGINT NOT NULL REFERENCES item_master(id) ON DELETE RESTRICT,
  lot_id BIGINT REFERENCES inventory_lots(id) ON DELETE SET NULL,
  encounter_id BIGINT REFERENCES encounters(id) ON DELETE SET NULL,
  movement_type VARCHAR(40) NOT NULL CHECK (
    movement_type IN (
      'RECEIPT',
      'ISSUE',
      'RETURN',
      'ADJUST_IN',
      'ADJUST_OUT',
      'TRANSFER_IN',
      'TRANSFER_OUT'
    )
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
CREATE INDEX IF NOT EXISTS idx_stock_ledger_item
  ON stock_ledger(item_master_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_stock_ledger_org_clinic
  ON stock_ledger(org_id, clinic_id, occurred_at DESC);

CREATE OR REPLACE FUNCTION prevent_stock_ledger_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'stock_ledger is append-only; % is not allowed', TG_OP;
END;
$$;

DROP TRIGGER IF EXISTS trg_stock_ledger_immutable ON stock_ledger;
CREATE TRIGGER trg_stock_ledger_immutable
BEFORE UPDATE OR DELETE ON stock_ledger
FOR EACH ROW
EXECUTE FUNCTION prevent_stock_ledger_mutation();

/* ---------------------------
   COMPUTED ON-HAND VIEWS
---------------------------- */

CREATE OR REPLACE VIEW stock_on_hand AS
SELECT
  sl.org_id,
  sl.clinic_id,
  sl.location_id,
  sl.item_master_id,
  sl.lot_id,
  SUM(sl.qty_delta)::numeric(14,3) AS on_hand_qty
FROM stock_ledger sl
GROUP BY sl.org_id, sl.clinic_id, sl.location_id, sl.item_master_id, sl.lot_id;

CREATE OR REPLACE VIEW stock_on_hand_nonnegative_violations AS
SELECT *
FROM stock_on_hand
WHERE on_hand_qty < 0;

/* ---------------------------
   BACKWARDS-COMPATIBLE SEED/BACKFILL
---------------------------- */

INSERT INTO organizations (code, name)
VALUES ('DEFAULT_ORG', 'Default Organization')
ON CONFLICT (code) DO NOTHING;

WITH org_ref AS (
  SELECT id AS org_id FROM organizations WHERE code = 'DEFAULT_ORG'
)
INSERT INTO clinics (org_id, code, name)
SELECT org_ref.org_id, 'CLINIC01', 'Clinic 01'
FROM org_ref
WHERE NOT EXISTS (SELECT 1 FROM clinics WHERE code = 'CLINIC01' AND org_id = org_ref.org_id);

WITH clinic_ref AS (
  SELECT id AS clinic_id FROM clinics WHERE code = 'CLINIC01' ORDER BY id LIMIT 1
)
INSERT INTO locations (clinic_id, code, name, location_type)
SELECT clinic_ref.clinic_id, 'MAIN', 'Main Warehouse', 'WAREHOUSE'
FROM clinic_ref
WHERE NOT EXISTS (SELECT 1 FROM locations WHERE clinic_id = clinic_ref.clinic_id AND code = 'MAIN');

WITH org_ref AS (
  SELECT id AS org_id FROM organizations WHERE code = 'DEFAULT_ORG'
),
clinic_ref AS (
  SELECT id AS clinic_id FROM clinics WHERE code = 'CLINIC01' ORDER BY id LIMIT 1
)
UPDATE users u
SET org_id = COALESCE(u.org_id, org_ref.org_id),
    home_clinic_id = COALESCE(u.home_clinic_id, clinic_ref.clinic_id)
FROM org_ref, clinic_ref
WHERE u.org_id IS NULL OR u.home_clinic_id IS NULL;

INSERT INTO user_clinic_roles (user_id, clinic_id, role, is_active)
SELECT
  u.id,
  u.home_clinic_id,
  CASE u.role
    WHEN 'ADMIN' THEN 'ORG_ADMIN'
    WHEN 'STAFF' THEN 'WAREHOUSE_STAFF'
    ELSE 'READ_ONLY'
  END AS role,
  TRUE
FROM users u
WHERE u.home_clinic_id IS NOT NULL
ON CONFLICT (user_id, clinic_id, role) DO NOTHING;

INSERT INTO item_master (sku_code, name, active, created_at, updated_at)
SELECT i.sku_code, i.name, COALESCE(i.active, TRUE), COALESCE(i.created_at, NOW()), NOW()
FROM items i
ON CONFLICT (sku_code) DO UPDATE
SET name = EXCLUDED.name,
    updated_at = NOW();

WITH clinic_ref AS (
  SELECT id AS clinic_id FROM clinics WHERE code = 'CLINIC01' ORDER BY id LIMIT 1
)
INSERT INTO clinic_item_settings (clinic_id, item_master_id, standard_cost, reorder_point, par_level, is_stocked)
SELECT clinic_ref.clinic_id, im.id, COALESCE(i.cost, 0), 0, 0, TRUE
FROM items i
JOIN item_master im ON im.sku_code = i.sku_code
CROSS JOIN clinic_ref
ON CONFLICT (clinic_id, item_master_id) DO NOTHING;

/* Backfill stock_ledger from existing immutable movement table */
WITH org_ref AS (
  SELECT id AS org_id FROM organizations WHERE code = 'DEFAULT_ORG'
),
clinic_ref AS (
  SELECT id AS clinic_id FROM clinics WHERE code = 'CLINIC01' ORDER BY id LIMIT 1
),
location_ref AS (
  SELECT id AS location_id
  FROM locations
  WHERE code = 'MAIN'
    AND clinic_id = (SELECT clinic_id FROM clinic_ref)
  ORDER BY id
  LIMIT 1
)
INSERT INTO stock_ledger (
  org_id,
  clinic_id,
  location_id,
  item_master_id,
  encounter_id,
  movement_type,
  qty_delta,
  reason_code,
  movement_source,
  correlation_id,
  idempotency_key,
  actor_user_id,
  allow_negative,
  metadata,
  occurred_at,
  created_at
)
SELECT
  org_ref.org_id,
  clinic_ref.clinic_id,
  location_ref.location_id,
  im.id AS item_master_id,
  it.encounter_id,
  CASE it.type
    WHEN 'STOCK_IN' THEN 'RECEIPT'
    WHEN 'RETURN' THEN 'RETURN'
    WHEN 'ISSUE' THEN 'ISSUE'
    WHEN 'ADJUST_IN' THEN 'ADJUST_IN'
    WHEN 'ADJUST_OUT' THEN 'ADJUST_OUT'
    ELSE 'ADJUST_IN'
  END AS movement_type,
  CASE
    WHEN it.type IN ('STOCK_IN', 'RETURN', 'ADJUST_IN') THEN it.quantity::numeric
    WHEN it.type IN ('ISSUE', 'ADJUST_OUT') THEN -1 * it.quantity::numeric
    ELSE it.quantity::numeric
  END AS qty_delta,
  COALESCE(it.reason_code, 'LEGACY_BACKFILL') AS reason_code,
  COALESCE(it.movement_source, 'LEGACY_BACKFILL') AS movement_source,
  CONCAT('legacy-itx-', it.id) AS correlation_id,
  it.idempotency_key,
  COALESCE(it.movement_actor_id, it.user_id, (SELECT id FROM users ORDER BY id LIMIT 1)) AS actor_user_id,
  COALESCE(it.allow_negative, FALSE),
  jsonb_build_object('legacy_inventory_transaction_id', it.id) AS metadata,
  COALESCE(it.created_at, NOW()),
  NOW()
FROM inventory_transactions it
JOIN items i ON i.id = it.item_id
JOIN item_master im ON im.sku_code = i.sku_code
CROSS JOIN org_ref
CROSS JOIN clinic_ref
CROSS JOIN location_ref
ON CONFLICT (correlation_id) DO NOTHING;

/* Keep legacy inventory_transactions flowing into stock_ledger during transition */
CREATE OR REPLACE FUNCTION mirror_legacy_inventory_transaction()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_org_id BIGINT;
  v_clinic_id BIGINT;
  v_location_id BIGINT;
  v_item_master_id BIGINT;
  v_actor_id BIGINT;
BEGIN
  SELECT id INTO v_org_id FROM organizations WHERE code = 'DEFAULT_ORG' ORDER BY id LIMIT 1;
  SELECT id INTO v_clinic_id FROM clinics WHERE code = 'CLINIC01' ORDER BY id LIMIT 1;
  SELECT id INTO v_location_id FROM locations WHERE clinic_id = v_clinic_id AND code = 'MAIN' ORDER BY id LIMIT 1;
  SELECT im.id
    INTO v_item_master_id
  FROM items i
  JOIN item_master im ON im.sku_code = i.sku_code
  WHERE i.id = NEW.item_id
  LIMIT 1;

  IF v_item_master_id IS NULL THEN
    RETURN NEW;
  END IF;

  v_actor_id := COALESCE(NEW.movement_actor_id, NEW.user_id, (SELECT id FROM users ORDER BY id LIMIT 1));
  IF v_actor_id IS NULL THEN
    RETURN NEW;
  END IF;

  INSERT INTO stock_ledger (
    org_id, clinic_id, location_id, item_master_id, encounter_id,
    movement_type, qty_delta, reason_code, movement_source, correlation_id,
    idempotency_key, actor_user_id, allow_negative, metadata, occurred_at
  )
  VALUES (
    v_org_id,
    v_clinic_id,
    v_location_id,
    v_item_master_id,
    NEW.encounter_id,
    CASE NEW.type
      WHEN 'STOCK_IN' THEN 'RECEIPT'
      WHEN 'RETURN' THEN 'RETURN'
      WHEN 'ISSUE' THEN 'ISSUE'
      WHEN 'ADJUST_IN' THEN 'ADJUST_IN'
      WHEN 'ADJUST_OUT' THEN 'ADJUST_OUT'
      ELSE 'ADJUST_IN'
    END,
    CASE
      WHEN NEW.type IN ('STOCK_IN', 'RETURN', 'ADJUST_IN') THEN NEW.quantity::numeric
      WHEN NEW.type IN ('ISSUE', 'ADJUST_OUT') THEN -1 * NEW.quantity::numeric
      ELSE NEW.quantity::numeric
    END,
    COALESCE(NEW.reason_code, 'LEGACY_MIRROR'),
    COALESCE(NEW.movement_source, 'LEGACY_MIRROR'),
    CONCAT('legacy-itx-', NEW.id),
    NEW.idempotency_key,
    v_actor_id,
    COALESCE(NEW.allow_negative, FALSE),
    jsonb_build_object('mirrored_from_legacy', TRUE, 'legacy_inventory_transaction_id', NEW.id),
    COALESCE(NEW.created_at, NOW())
  )
  ON CONFLICT (correlation_id) DO NOTHING;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_mirror_legacy_inventory_transaction ON inventory_transactions;
CREATE TRIGGER trg_mirror_legacy_inventory_transaction
AFTER INSERT ON inventory_transactions
FOR EACH ROW
EXECUTE FUNCTION mirror_legacy_inventory_transaction();

COMMIT;
