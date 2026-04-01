BEGIN;

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

COMMIT;
