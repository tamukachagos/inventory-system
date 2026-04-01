const test = require('node:test');
const assert = require('node:assert/strict');
const {
  startServer,
  stopServer,
  request,
  login,
  seedUsers,
} = require('./test-support');

const PORT = Number(process.env.TEST_PORT_CONTRACT || 5056);
const runReliability = process.env.RUN_RELIABILITY_TESTS === 'true';

let serverProc = null;
let baseUrl = null;
let adminToken = null;
let staffToken = null;

if (!runReliability) {
  test.skip('API contract suite disabled. Set RUN_RELIABILITY_TESTS=true to run.', () => {});
} else {
test.before(async () => {
  await seedUsers();
  const started = await startServer(PORT);
  serverProc = started.proc;
  baseUrl = started.baseUrl;
  adminToken = (await login(baseUrl, 'ADM-001', 'Admin1234!')).token;
  staffToken = (await login(baseUrl, 'STU-001', 'Test1234!')).token;
});

test.after(async () => {
  await stopServer(serverProc);
});

test('contract: login returns token + refresh + role context', async () => {
  const result = await request(baseUrl, 'POST', '/login', {
    body: { student_card: 'STU-001', password: 'Test1234!' },
  });
  assert.equal(result.status, 200, result.text);
  assert.equal(typeof result.json.token, 'string');
  assert.equal(typeof result.json.refresh_token, 'string');
  assert.equal(result.json.user.role, 'STAFF');
  assert.ok(Array.isArray(result.json.capabilities));
});

test('contract: create item -> stock receipt -> stock issue -> report surfaces data', async () => {
  const sku = `SKU-CONTRACT-${Date.now()}`;
  const appointment = `APT-CONTRACT-${Date.now()}`;

  const createItem = await request(baseUrl, 'POST', '/create-item', {
    token: adminToken,
    body: { sku_code: sku, name: 'Contract Item', cost: 12.5 },
  });
  assert.equal(createItem.status, 200, createItem.text);
  assert.equal(createItem.json.sku_code, sku);
  const itemId = Number(createItem.json.id);
  assert.ok(itemId > 0);

  const stockIn = await request(baseUrl, 'POST', '/stock-in', {
    token: staffToken,
    body: { item_id: itemId, quantity: 25, cost: 12.5, vendor: 'UnitTestVendor', batch_number: 'B1' },
  });
  assert.equal(stockIn.status, 200, stockIn.text);
  assert.equal(stockIn.json.type, 'STOCK_IN');
  assert.equal(Number(stockIn.json.quantity), 25);

  const checkin = await request(baseUrl, 'POST', '/check-in', {
    token: staffToken,
    body: {
      appointment_id: appointment,
      provider_card: 'STU-001',
      status: 'ACTIVE',
    },
  });
  assert.equal(checkin.status, 200, checkin.text);
  const encounterId = Number(checkin.json.encounter.id);

  const issue = await request(baseUrl, 'POST', '/issue-item', {
    token: staffToken,
    body: {
      item_id: itemId,
      encounter_id: encounterId,
      quantity: 7,
    },
  });
  assert.equal(issue.status, 200, issue.text);
  assert.equal(issue.json.type, 'ISSUE');
  assert.equal(Number(issue.json.quantity), 7);

  const now = new Date().toISOString().slice(0, 10);
  const kpi = await request(baseUrl, 'GET', `/dashboard/kpis?from=${now}&to=${now}`, {
    token: staffToken,
  });
  assert.equal(kpi.status, 200, kpi.text);
  assert.equal(typeof kpi.json.inventory_value, 'number');
  assert.equal(typeof kpi.json.daily_usage, 'number');
  assert.equal(typeof kpi.json.low_stock_alerts, 'number');

  const report = await request(baseUrl, 'GET', `/dashboard/report?from=${now}&to=${now}`, {
    token: staffToken,
  });
  assert.equal(report.status, 200, report.text);
  assert.ok(Array.isArray(report.json.daily_usage));
  assert.ok(Array.isArray(report.json.valuation_trend));
  assert.ok(Array.isArray(report.json.cost_per_encounter));
});

test('contract: cycle-count approval adjustment updates on-hand inventory', async () => {
  const sku = `SKU-ADJ-${Date.now()}`;
  const createItem = await request(baseUrl, 'POST', '/create-item', {
    token: adminToken,
    body: { sku_code: sku, name: 'Adjustment Item', cost: 2.5 },
  });
  assert.equal(createItem.status, 200, createItem.text);
  const itemId = Number(createItem.json.id);

  const stockIn = await request(baseUrl, 'POST', '/stock-in', {
    token: staffToken,
    body: { item_id: itemId, quantity: 20 },
  });
  assert.equal(stockIn.status, 200, stockIn.text);

  const cycleHeader = await request(baseUrl, 'POST', '/cycle-counts', {
    token: staffToken,
    body: { notes: 'Adjustment flow contract test' },
  });
  assert.equal(cycleHeader.status, 200, cycleHeader.text);
  const cycleId = Number(cycleHeader.json.id);

  const upsertLine = await request(baseUrl, 'POST', `/cycle-counts/${cycleId}/lines`, {
    token: staffToken,
    body: { item_id: itemId, counted_qty: 17 },
  });
  assert.equal(upsertLine.status, 200, upsertLine.text);
  assert.equal(Number(upsertLine.json.variance), -3);

  const submitted = await request(baseUrl, 'POST', `/cycle-counts/${cycleId}/submit`, {
    token: staffToken,
    body: {},
  });
  assert.equal(submitted.status, 200, submitted.text);
  assert.equal(submitted.json.status, 'SUBMITTED');

  const approved = await request(baseUrl, 'POST', `/cycle-counts/${cycleId}/approve`, {
    token: adminToken,
    body: { decision: 'APPROVED', apply_adjustments: true, notes: 'Apply adjustment' },
  });
  assert.equal(approved.status, 200, approved.text);
  assert.equal(approved.json.status, 'APPROVED');

  const levels = await request(baseUrl, 'GET', '/stock-levels', { token: staffToken });
  assert.equal(levels.status, 200, levels.text);
  const adjusted = levels.json.find((row) => Number(row.id) === itemId);
  assert.ok(adjusted);
  assert.equal(Number(adjusted.quantity), 17);
});
}
