BEGIN;

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

COMMIT;
