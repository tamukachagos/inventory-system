const test = require('node:test');
const assert = require('node:assert/strict');
const {
  createPool,
  startServer,
  stopServer,
  request,
  login,
  seedUsers,
} = require('./test-support');

const PORT = Number(process.env.TEST_PORT_TRANSFERS || 5161);
const runReliability = process.env.RUN_RELIABILITY_TESTS === 'true';

let serverProc = null;
let baseUrl = null;
let adminToken = null;
let staffToken = null;
let clinic1Id = null;
let clinic2Id = null;
let clinic3Id = null;

if (!runReliability) {
  test.skip('Transfers suite disabled. Set RUN_RELIABILITY_TESTS=true to run.', () => {});
} else {
  test.before(async () => {
    await seedUsers();
    const started = await startServer(PORT);
    serverProc = started.proc;
    baseUrl = started.baseUrl;
    adminToken = (await login(baseUrl, 'ADM-001', 'Admin1234!')).token;
    staffToken = (await login(baseUrl, 'STU-001', 'Test1234!')).token;

    const meta = await request(baseUrl, 'GET', '/transfers/meta', { token: adminToken });
    assert.equal(meta.status, 200, meta.text);
    assert.ok(Array.isArray(meta.json.clinics));
    assert.ok(meta.json.clinics.length >= 1);
    clinic1Id = Number(meta.json.user_scope?.home_clinic_id || meta.json.clinics[0].id);

    const pool = createPool();
    try {
      const userOrg = await pool.query(
        `SELECT org_id
         FROM users
         WHERE student_card = 'STU-001'
         LIMIT 1`
      );
      const orgId = Number(userOrg.rows[0].org_id);
      const c2 = await pool.query(
        `INSERT INTO clinics (org_id, code, name)
         VALUES ($1, $2, $3)
         ON CONFLICT (org_id, code) DO UPDATE SET name = EXCLUDED.name
         RETURNING id`,
        [orgId, 'CLINIC02', 'Clinic 02']
      );
      clinic2Id = Number(c2.rows[0].id);
      const c3 = await pool.query(
        `INSERT INTO clinics (org_id, code, name)
         VALUES ($1, $2, $3)
         ON CONFLICT (org_id, code) DO UPDATE SET name = EXCLUDED.name
         RETURNING id`,
        [orgId, 'CLINIC03', 'Clinic 03']
      );
      clinic3Id = Number(c3.rows[0].id);
    } finally {
      await pool.end();
    }
  });

  test.after(async () => {
    await stopServer(serverProc);
  });

  test('transfers: create -> approve -> pick/pack -> receive -> cancel with idempotency and audit logs', async () => {
    const sku = `SKU-XFER-${Date.now()}`;
    const createItem = await request(baseUrl, 'POST', '/create-item', {
      token: adminToken,
      body: { sku_code: sku, name: 'Transfer Flow Item', cost: 3.5 },
    });
    assert.equal(createItem.status, 200, createItem.text);
    const itemId = Number(createItem.json.id);

    const stockIn = await request(baseUrl, 'POST', '/stock-in', {
      token: staffToken,
      body: { item_id: itemId, quantity: 40 },
    });
    assert.equal(stockIn.status, 200, stockIn.text);

    const beforeLevels = await request(baseUrl, 'GET', '/stock-levels', { token: staffToken });
    const beforeQty = Number(beforeLevels.json.find((row) => Number(row.id) === itemId).quantity);

    const createKey = `xfer-create-${Date.now()}`;
    const created = await request(baseUrl, 'POST', '/transfers/requests', {
      token: staffToken,
      headers: { 'idempotency-key': createKey },
      body: {
        from_clinic_id: clinic1Id,
        to_clinic_id: clinic2Id,
        needed_by: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString(),
        notes: 'Integration transfer request',
        items: [{ item_id: itemId, requested_qty: 20 }],
      },
    });
    assert.equal(created.status, 200, created.text);
    const transferId = Number(created.json.transfer.id);
    assert.equal(created.json.transfer.status, 'PENDING');

    const createReplay = await request(baseUrl, 'POST', '/transfers/requests', {
      token: staffToken,
      headers: { 'idempotency-key': createKey },
      body: {
        from_clinic_id: clinic1Id,
        to_clinic_id: clinic2Id,
        items: [{ item_id: itemId, requested_qty: 20 }],
      },
    });
    assert.equal(createReplay.status, 200, createReplay.text);
    assert.equal(Boolean(createReplay.json.idempotent_replay), true);

    const approveKey = `xfer-approve-${Date.now()}`;
    const approved = await request(baseUrl, 'POST', `/transfers/${transferId}/approve`, {
      token: adminToken,
      headers: { 'idempotency-key': approveKey },
      body: {
        decision: 'APPROVE',
        lines: [{ item_id: itemId, approved_qty: 20 }],
      },
    });
    assert.equal(approved.status, 200, approved.text);
    assert.equal(approved.json.transfer.status, 'APPROVED');

    const approveReplay = await request(baseUrl, 'POST', `/transfers/${transferId}/approve`, {
      token: adminToken,
      headers: { 'idempotency-key': approveKey },
      body: { decision: 'APPROVE' },
    });
    assert.equal(approveReplay.status, 200, approveReplay.text);
    assert.equal(Boolean(approveReplay.json.idempotent_replay), true);

    const pick = await request(baseUrl, 'POST', `/transfers/${transferId}/pick-pack`, {
      token: staffToken,
      headers: { 'idempotency-key': `xfer-pick-${Date.now()}` },
      body: {
        lines: [{ item_id: itemId, quantity: 12 }],
      },
    });
    assert.equal(pick.status, 200, pick.text);
    assert.ok(['PARTIALLY_PICKED', 'IN_TRANSIT'].includes(pick.json.transfer.status));

    const oversizePick = await request(baseUrl, 'POST', `/transfers/${transferId}/pick-pack`, {
      token: staffToken,
      headers: { 'idempotency-key': `xfer-pick-oversize-${Date.now()}` },
      body: {
        lines: [{ item_id: itemId, quantity: 99999 }],
      },
    });
    assert.equal(oversizePick.status, 400, oversizePick.text);

    const receive = await request(baseUrl, 'POST', `/transfers/${transferId}/receive`, {
      token: staffToken,
      headers: { 'idempotency-key': `xfer-receive-${Date.now()}` },
      body: {
        lines: [{ item_id: itemId, quantity: 7 }],
      },
    });
    assert.equal(receive.status, 200, receive.text);

    const cancel = await request(baseUrl, 'POST', `/transfers/${transferId}/cancel`, {
      token: staffToken,
      headers: { 'idempotency-key': `xfer-cancel-${Date.now()}` },
      body: {
        reason: 'Cancel residual in-transit quantity',
        lines: [{ item_id: itemId, quantity: 5 }],
      },
    });
    assert.equal(cancel.status, 200, cancel.text);

    const detail = await request(baseUrl, 'GET', `/transfers/requests/${transferId}`, { token: staffToken });
    assert.equal(detail.status, 200, detail.text);
    const line = detail.json.lines.find((row) => Number(row.item_id) === itemId);
    assert.ok(line);
    assert.equal(Number(line.approved_qty), 20);
    assert.equal(Number(line.picked_qty), 12);
    assert.equal(Number(line.received_qty), 7);
    assert.equal(Number(line.cancelled_qty), 5);

    const afterLevels = await request(baseUrl, 'GET', '/stock-levels', { token: staffToken });
    assert.equal(afterLevels.status, 200, afterLevels.text);
    const afterQty = Number(afterLevels.json.find((row) => Number(row.id) === itemId).quantity);
    assert.equal(afterQty, beforeQty);

    const audit = await request(baseUrl, 'GET', '/audit-logs?limit=200&offset=0', { token: adminToken });
    assert.equal(audit.status, 200, audit.text);
    const actions = audit.json.rows.map((row) => row.action);
    assert.ok(actions.includes('TRANSFER_REQUEST_CREATED'));
    assert.ok(actions.includes('TRANSFER_REQUEST_APPROVED'));
    assert.ok(actions.includes('TRANSFER_PICK_PACK'));
    assert.ok(actions.includes('TRANSFER_RECEIVE'));
    assert.ok(actions.includes('TRANSFER_CANCEL'));
  });

  test('transfers: staff cannot create transfer outside clinic scope', async () => {
    const sku = `SKU-XFER-SCOPE-${Date.now()}`;
    const createItem = await request(baseUrl, 'POST', '/create-item', {
      token: adminToken,
      body: { sku_code: sku, name: 'Scope Item', cost: 1 },
    });
    assert.equal(createItem.status, 200, createItem.text);
    const itemId = Number(createItem.json.id);

    const scoped = await request(baseUrl, 'POST', '/transfers/requests', {
      token: staffToken,
      headers: { 'idempotency-key': `xfer-scope-${Date.now()}` },
      body: {
        from_clinic_id: clinic2Id,
        to_clinic_id: clinic3Id,
        items: [{ item_id: itemId, requested_qty: 2 }],
      },
    });
    assert.equal(scoped.status, 403, scoped.text);
  });
}
