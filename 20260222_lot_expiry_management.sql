BEGIN;

ALTER TABLE items
  ADD COLUMN IF NOT EXISTS expiry_tracked BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_items_expiry_tracked ON items(expiry_tracked);

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

COMMIT;
