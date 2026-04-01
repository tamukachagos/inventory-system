const { Pool } = require('pg');
require('dotenv').config();

const required = ['DB_USER', 'DB_HOST', 'DB_NAME', 'DB_PASSWORD', 'DB_PORT'];
for (const key of required) {
  if (!process.env[key]) {
    throw new Error(`Missing required env var: ${key}`);
  }
}

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: Number(process.env.DB_PORT),
  ssl: false,
});

const main = async () => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const org = await client.query(
      `INSERT INTO organizations (code, name)
       VALUES ('PILOT_ORG', 'Pilot Organization')
       ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
       RETURNING id`
    );
    const orgId = Number(org.rows[0].id);

    const clinic = await client.query(
      `INSERT INTO clinics (org_id, code, name)
       VALUES ($1, 'PILOT_CLINIC', 'Pilot Clinic')
       ON CONFLICT (org_id, code) DO UPDATE SET name = EXCLUDED.name
       RETURNING id`,
      [orgId]
    );
    const clinicId = Number(clinic.rows[0].id);

    await client.query(
      `INSERT INTO locations (clinic_id, code, name, location_type)
       VALUES ($1, 'PILOT_MAIN', 'Pilot Main Warehouse', 'WAREHOUSE')
       ON CONFLICT (clinic_id, code) DO UPDATE SET name = EXCLUDED.name`,
      [clinicId]
    );

    await client.query(
      `INSERT INTO feature_flags (clinic_id, flag_key, enabled, rollout_percentage, payload, created_at, updated_at)
       VALUES
         ($1, 'OFFLINE_SYNC_V1', TRUE, 100, '{"mode":"pilot"}'::jsonb, NOW(), NOW()),
         ($1, 'REORDER_ENGINE_V1', TRUE, 100, '{"mode":"pilot"}'::jsonb, NOW(), NOW()),
         ($1, 'LOT_EXPIRY_V1', TRUE, 100, '{"mode":"pilot"}'::jsonb, NOW(), NOW())
       ON CONFLICT (clinic_id, flag_key)
       DO UPDATE SET
         enabled = EXCLUDED.enabled,
         rollout_percentage = EXCLUDED.rollout_percentage,
         payload = EXCLUDED.payload,
         updated_at = NOW()`,
      [clinicId]
    );

    await client.query(
      `UPDATE users
       SET org_id = COALESCE(org_id, $1),
           home_clinic_id = COALESCE(home_clinic_id, $2)
       WHERE role IN ('ADMIN', 'STAFF')`,
      [orgId, clinicId]
    );

    await client.query('COMMIT');
    console.log(`Pilot clinic seeded. org_id=${orgId} clinic_id=${clinicId}`);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw err;
  } finally {
    client.release();
    await pool.end();
  }
};

main().catch((err) => {
  console.error('Pilot seed failed:', err && err.message ? err.message : err);
  process.exitCode = 1;
});
