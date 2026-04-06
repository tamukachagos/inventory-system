const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { Pool } = require('pg');

if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

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

const migrationFile = process.env.MIGRATION_FILE || 'migration.sql';
const sqlPath = path.resolve(__dirname, migrationFile);
const sql = fs.readFileSync(sqlPath, 'utf8');
const sqlChecksum = crypto.createHash('sha256').update(sql).digest('hex');
const versionTag = process.env.SCHEMA_VERSION_TAG || path.basename(sqlPath);
const appliedBy = process.env.USER || process.env.USERNAME || 'unknown';
const migrationLockKey = Number(process.env.MIGRATION_LOCK_KEY || 921337);
const lockTimeout = String(process.env.MIGRATION_LOCK_TIMEOUT || '15s').replace(/'/g, '');
const statementTimeout = String(process.env.MIGRATION_STATEMENT_TIMEOUT || '10min').replace(/'/g, '');

const ensureSchemaVersioning = async (client) => {
  await client.query(
    `CREATE TABLE IF NOT EXISTS schema_versions (
      id BIGSERIAL PRIMARY KEY,
      version_tag VARCHAR(120) NOT NULL,
      checksum VARCHAR(128) NOT NULL,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      applied_by VARCHAR(120),
      notes TEXT,
      UNIQUE (version_tag, checksum)
    )`
  );
};

const enforceSafetyChecks = async (client) => {
  if (process.env.NODE_ENV === 'production' && process.env.ALLOW_PROD_MIGRATION !== 'true') {
    throw new Error('Production migration blocked. Set ALLOW_PROD_MIGRATION=true to proceed.');
  }

  const inRecovery = await client.query('SELECT pg_is_in_recovery() AS in_recovery');
  if (Boolean(inRecovery.rows[0]?.in_recovery)) {
    throw new Error('Migration blocked on read replica (pg_is_in_recovery=true).');
  }

  const hasVersion = await client.query(
    `SELECT id, applied_at
     FROM schema_versions
     WHERE version_tag = $1
       AND checksum = $2
     ORDER BY id DESC
     LIMIT 1`,
    [versionTag, sqlChecksum]
  );
  if (hasVersion.rows.length > 0) {
    return {
      alreadyApplied: true,
      versionRow: hasVersion.rows[0],
    };
  }

  return { alreadyApplied: false };
};

(async () => {
  const client = await pool.connect();
  try {
    await client.query(`SET lock_timeout = '${lockTimeout}'`);
    await client.query(`SET statement_timeout = '${statementTimeout}'`);
    await client.query('SELECT pg_advisory_lock($1)', [migrationLockKey]);

    await ensureSchemaVersioning(client);
    const safety = await enforceSafetyChecks(client);
    if (safety.alreadyApplied) {
      console.log(`Migration skipped: version already applied (${versionTag}, checksum=${sqlChecksum.slice(0, 12)}...)`);
      return;
    }

    await client.query(sql);
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO schema_versions (version_tag, checksum, applied_by, notes)
       VALUES ($1, $2, $3, $4)`,
      [versionTag, sqlChecksum, appliedBy, 'Applied via scripts/run-migration.js']
    );
    await client.query('COMMIT');
    console.log(`Migration applied successfully. version=${versionTag} checksum=${sqlChecksum.slice(0, 12)}...`);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('Migration failed:', err && err.message ? err.message : err);
    process.exitCode = 1;
  } finally {
    try { await client.query('SELECT pg_advisory_unlock($1)', [migrationLockKey]); } catch (_) {}
    client.release();
    await pool.end();
  }
})();
