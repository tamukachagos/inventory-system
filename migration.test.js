const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const { createPool, migrationSql } = require('./test-support');

test('migration is idempotent and provisions required tables/columns', async () => {
  const pool = createPool();
  const schemaName = `mig_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`;
  const sql = migrationSql();

  try {
    await pool.query(`CREATE SCHEMA "${schemaName}"`);
    await pool.query(`SET search_path TO "${schemaName}"`);

    await pool.query(sql);
    await pool.query(sql);

    const tables = await pool.query(
      `SELECT table_name
       FROM information_schema.tables
       WHERE table_schema = $1
         AND table_name IN (
           'users',
           'items',
           'encounters',
           'inventory_transactions',
           'audit_logs',
           'event_ledger',
           'cycle_counts',
           'cycle_count_lines'
         )`,
      [schemaName]
    );
    assert.equal(tables.rows.length, 8);

    const keyColumns = await pool.query(
      `SELECT table_name, column_name
       FROM information_schema.columns
       WHERE table_schema = $1
         AND (table_name, column_name) IN (
           ('users', 'is_active'),
           ('users', 'must_reset_password'),
           ('users', 'password_expires_at'),
           ('items', 'quantity'),
           ('inventory_transactions', 'type'),
           ('audit_logs', 'details'),
           ('event_ledger', 'event_hash')
         )`,
      [schemaName]
    );
    assert.equal(keyColumns.rows.length, 7);

    const uniqueIndexes = await pool.query(
      `SELECT indexname
       FROM pg_indexes
       WHERE schemaname = $1
         AND indexname IN (
           'idx_users_student_card',
           'idx_items_sku_code',
           'idx_encounters_code',
           'idx_event_ledger_event_hash'
         )`,
      [schemaName]
    );
    assert.equal(uniqueIndexes.rows.length, 4);
  } finally {
    await pool.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    await pool.end();
  }
});
