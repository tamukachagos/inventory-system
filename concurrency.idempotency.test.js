const test = require('node:test');
const assert = require('node:assert/strict');
const {
  startServer,
  stopServer,
  request,
  login,
  seedUsers,
} = require('./test-support');

const PORT = Number(process.env.TEST_PORT_RELIABILITY || 5057);
const runReliability = process.env.RUN_RELIABILITY_TESTS === 'true';

let serverProc = null;
let baseUrl = null;
let staffToken = null;
let itemId = null;
let encounterIds = [];

if (!runReliability) {
  test.skip('Concurrency/idempotency suite disabled. Set RUN_RELIABILITY_TESTS=true to run.', () => {});
} else {
test.before(async () => {
  await seedUsers();
  const started = await startServer(PORT);
  serverProc = started.proc;
  baseUrl = started.baseUrl;
  staffToken = (await login(baseUrl, 'STU-001', 'Test1234!')).token;

  const createItem = await request(baseUrl, 'POST', '/create-item', {
    token: staffToken,
    body: {
      sku_code: `SKU-CONC-${Date.now()}`,
      name: 'Concurrency Item',
      cost: 1,
    },
  });
  assert.equal(createItem.status, 200, createItem.text);
  itemId = Number(createItem.json.id);

  const stockIn = await request(baseUrl, 'POST', '/stock-in', {
    token: staffToken,
    body: { item_id: itemId, quantity: 100 },
  });
  assert.equal(stockIn.status, 200, stockIn.text);

  const encounterPromises = Array.from({ length: 20 }).map((_, idx) =>
    request(baseUrl, 'POST', '/check-in', {
      token: staffToken,
      body: {
        appointment_id: `APT-CONC-${Date.now()}-${idx}`,
        provider_card: 'STU-001',
        status: 'ACTIVE',
      },
    })
  );
  const created = await Promise.all(encounterPromises);
  created.forEach((res) => assert.equal(res.status, 200, res.text));
  encounterIds = created.map((res) => Number(res.json.encounter.id));
});

test.after(async () => {
  await stopServer(serverProc);
});

test('concurrency: simultaneous issues do not corrupt quantity or go negative', async () => {
  const attempts = encounterIds.map((encounterId) =>
    request(baseUrl, 'POST', '/issue-item', {
      token: staffToken,
      body: {
        item_id: itemId,
        encounter_id: encounterId,
        quantity: 6,
      },
    })
  );
  const responses = await Promise.all(attempts);
  const successCount = responses.filter((r) => r.status === 200).length;
  const failCount = responses.filter((r) => r.status !== 200).length;

  assert.ok(successCount > 0);
  assert.ok(failCount > 0);

  const levels = await request(baseUrl, 'GET', '/stock-levels', { token: staffToken });
  assert.equal(levels.status, 200, levels.text);
  const row = levels.json.find((it) => Number(it.id) === itemId);
  assert.ok(row);

  const expected = 100 - (successCount * 6);
  assert.equal(Number(row.quantity), expected);
  assert.ok(Number(row.quantity) >= 0);
});

test('idempotency: duplicate issue-item request is rejected and inventory decremented once', async () => {
  const checkin = await request(baseUrl, 'POST', '/check-in', {
    token: staffToken,
    body: {
      appointment_id: `APT-IDEMP-${Date.now()}`,
      provider_card: 'STU-001',
      status: 'ACTIVE',
    },
  });
  assert.equal(checkin.status, 200, checkin.text);
  const encounterId = Number(checkin.json.encounter.id);

  const before = await request(baseUrl, 'GET', '/stock-levels', { token: staffToken });
  const beforeQty = Number(before.json.find((it) => Number(it.id) === itemId).quantity);

  const payload = {
    item_id: itemId,
    encounter_id: encounterId,
    quantity: 1,
  };
  const first = await request(baseUrl, 'POST', '/issue-item', { token: staffToken, body: payload });
  const second = await request(baseUrl, 'POST', '/issue-item', { token: staffToken, body: payload });

  assert.equal(first.status, 200, first.text);
  assert.equal(second.status, 409, second.text);
  assert.equal(second.json.error, 'Duplicate action rejected');

  const after = await request(baseUrl, 'GET', '/stock-levels', { token: staffToken });
  const afterQty = Number(after.json.find((it) => Number(it.id) === itemId).quantity);
  assert.equal(afterQty, beforeQty - 1);
});
}
