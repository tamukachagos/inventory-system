const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const {
  createPool,
  startServer,
  stopServer,
  request,
  login,
  seedUsers,
} = require('./test-support');

const PORT = Number(process.env.TEST_PORT_REORDER || 5063);
const runReliability = process.env.RUN_RELIABILITY_TESTS === 'true';

let serverProc = null;
let baseUrl = null;
let token = null;
let pool = null;
let orgId = null;
let clinicA = null;
let clinicB = null;
let actorUserId = null;

if (!runReliability) {
  test.skip('Reorder recommendation suite disabled. Set RUN_RELIABILITY_TESTS=true to run.', () => {});
} else {
  test.before(async () => {
    await seedUsers();
    const started = await startServer(PORT);
    serverProc = started.proc;
    baseUrl = started.baseUrl;
    token = (await login(baseUrl, 'ADM-001', 'Admin1234!')).token;
    pool = createPool();

    const meta = await request(baseUrl, 'GET', '/reorder/meta', { token });
    assert.equal(meta.status, 200, meta.text);
    const org = await pool.query(
      `INSERT INTO organizations (code, name)
       VALUES ($1, $2)
       ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
       RETURNING id`,
      [`ORG-REORDER-${Date.now()}`, 'Reorder Org']
    );
    orgId = Number(org.rows[0].id);
    const cA = await pool.query(
      `INSERT INTO clinics (org_id, code, name)
       VALUES ($1, $2, $3)
       RETURNING id`,
      [orgId, `RA-${Date.now()}`, 'Clinic A']
    );
    clinicA = Number(cA.rows[0].id);
    const cB = await pool.query(
      `INSERT INTO clinics (org_id, code, name)
       VALUES ($1, $2, $3)
       RETURNING id`,
      [orgId, `RB-${Date.now()}`, 'Clinic B']
    );
    clinicB = Number(cB.rows[0].id);
    const actor = await pool.query(`SELECT id FROM users ORDER BY id ASC LIMIT 1`);
    actorUserId = Number(actor.rows[0].id);
  });

  test.after(async () => {
    await stopServer(serverProc);
    if (pool) await pool.end();
  });

  const seedReorderCase = async ({
    sku,
    name,
    clinicAPar,
    clinicAOnHand,
    clinicAUsage30,
    clinicALeadDays,
    clinicBPar,
    clinicBOnHand,
    clinicBUsage30,
    clinicBLeadDays,
  }) => {
    const item = await pool.query(
      `INSERT INTO item_master (sku_code, name, active)
       VALUES ($1, $2, TRUE)
       RETURNING id`,
      [sku, name]
    );
    const itemMasterId = Number(item.rows[0].id);

    await pool.query(
      `INSERT INTO clinic_item_settings
       (clinic_id, item_master_id, location_code, par_level, on_order_qty, lead_time_days, is_stocked)
       VALUES
       ($1, $3, 'MAIN', $4, 0, $5, TRUE),
       ($2, $3, 'MAIN', $6, 0, $7, TRUE)`,
      [clinicA, clinicB, itemMasterId, clinicAPar, clinicALeadDays, clinicBPar, clinicBLeadDays]
    );

    await pool.query(
      `INSERT INTO stock_ledger
       (org_id, clinic_id, item_master_id, movement_type, qty_delta, reason_code, movement_source, correlation_id, actor_user_id, allow_negative, metadata, occurred_at)
       VALUES
       ($1, $2, $4, 'RECEIPT', $5, 'TEST', 'TEST', $6, $3, FALSE, '{}'::jsonb, NOW()),
       ($1, $7, $4, 'RECEIPT', $8, 'TEST', 'TEST', $9, $3, FALSE, '{}'::jsonb, NOW())`,
      [
        orgId,
        clinicA,
        actorUserId,
        itemMasterId,
        clinicAOnHand,
        `corr-${crypto.randomUUID()}`,
        clinicB,
        clinicBOnHand,
        `corr-${crypto.randomUUID()}`,
      ]
    );

    const usageInsert = async (clinicId, qtyPerTx, txCount) => {
      for (let i = 0; i < txCount; i += 1) {
        await pool.query(
          `INSERT INTO stock_ledger
           (org_id, clinic_id, item_master_id, movement_type, qty_delta, reason_code, movement_source, correlation_id, actor_user_id, allow_negative, metadata, occurred_at)
           VALUES ($1, $2, $3, 'ISSUE', $4, 'TEST_USAGE', 'TEST', $5, $6, FALSE, '{}'::jsonb, NOW() - INTERVAL '10 days')`,
          [orgId, clinicId, itemMasterId, -1 * qtyPerTx, `corr-${crypto.randomUUID()}`, actorUserId]
        );
      }
    };

    if (clinicAUsage30 > 0) await usageInsert(clinicA, 1, clinicAUsage30);
    if (clinicBUsage30 > 0) await usageInsert(clinicB, 1, clinicBUsage30);

    return itemMasterId;
  };

  test('reorder engine recommends transfer-only and blocks purchase when network covers lead-time demand', async () => {
    const sku = `SKU-REO-TO-${Date.now()}`;
    await seedReorderCase({
      sku,
      name: 'Transfer Only Item',
      clinicAPar: 100,
      clinicAOnHand: 20,
      clinicAUsage30: 60,
      clinicALeadDays: 14,
      clinicBPar: 50,
      clinicBOnHand: 120,
      clinicBUsage30: 10,
      clinicBLeadDays: 14,
    });

    const res = await request(baseUrl, 'GET', `/reorder/recommendations?clinic_id=${clinicA}&location_code=MAIN`, { token });
    assert.equal(res.status, 200, res.text);
    const row = res.json.rows.find((r) => r.sku_code === sku);
    assert.ok(row);
    assert.equal(row.recommendation_type, 'TRANSFER_ONLY');
    assert.ok(Number(row.suggested_transfer_qty) > 0);
    assert.equal(Number(row.suggested_purchase_qty), 0);
    assert.equal(Boolean(row.network_covers_lead_time), true);
  });

  test('reorder engine recommends transfer + purchase when network excess is insufficient', async () => {
    const sku = `SKU-REO-TP-${Date.now()}`;
    await seedReorderCase({
      sku,
      name: 'Transfer Purchase Item',
      clinicAPar: 100,
      clinicAOnHand: 10,
      clinicAUsage30: 150,
      clinicALeadDays: 14,
      clinicBPar: 80,
      clinicBOnHand: 140,
      clinicBUsage30: 30,
      clinicBLeadDays: 14,
    });

    const res = await request(baseUrl, 'GET', `/reorder/recommendations?clinic_id=${clinicA}&location_code=MAIN`, { token });
    assert.equal(res.status, 200, res.text);
    const row = res.json.rows.find((r) => r.sku_code === sku);
    assert.ok(row);
    assert.equal(row.recommendation_type, 'TRANSFER_PLUS_PURCHASE');
    assert.ok(Number(row.suggested_transfer_qty) > 0);
    assert.ok(Number(row.suggested_purchase_qty) > 0);
    assert.equal(Boolean(row.network_covers_lead_time), false);
  });
}
