const test = require('node:test');
const assert = require('node:assert/strict');
const {
  startServer,
  stopServer,
  request,
  login,
  seedUsers,
} = require('./test-support');

const PORT = Number(process.env.TEST_PORT_SYNC || 5064);
const runReliability = process.env.RUN_RELIABILITY_TESTS === 'true';

let serverProc = null;
let baseUrl = null;
let token = null;
let deviceId = null;
let itemId = null;
let encounterId = null;

if (!runReliability) {
  test.skip('Sync suite disabled. Set RUN_RELIABILITY_TESTS=true to run.', () => {});
} else {
  test.before(async () => {
    await seedUsers();
    const started = await startServer(PORT);
    serverProc = started.proc;
    baseUrl = started.baseUrl;
    token = (await login(baseUrl, 'STU-001', 'Test1234!')).token;
    deviceId = `tablet-${Date.now()}`;

    const item = await request(baseUrl, 'POST', '/create-item', {
      token,
      body: { sku_code: `SKU-SYNC-${Date.now()}`, name: 'Sync Item', cost: 1 },
    });
    assert.equal(item.status, 200, item.text);
    itemId = Number(item.json.id);

    const receipt = await request(baseUrl, 'POST', '/stock-in', {
      token,
      headers: { 'Idempotency-Key': `seed-sync-rcpt-${Date.now()}` },
      body: { item_id: itemId, quantity: 20 },
    });
    assert.equal(receipt.status, 200, receipt.text);

    const encounter = await request(baseUrl, 'POST', '/check-in', {
      token,
      body: {
        appointment_id: `APT-SYNC-${Date.now()}`,
        provider_card: 'STU-001',
        status: 'ACTIVE',
      },
    });
    assert.equal(encounter.status, 200, encounter.text);
    encounterId = Number(encounter.json.encounter.id);
  });

  test.after(async () => {
    await stopServer(serverProc);
  });

  test('sync push handles applied, idempotent replay, and duplicate-pattern conflict', async () => {
    const idem = `sync-issue-${Date.now()}`;
    const action = {
      client_action_id: 'local-1',
      action_type: 'ISSUE_ITEM',
      path: '/issue-item',
      method: 'POST',
      body: { item_id: itemId, encounter_id: encounterId, quantity: 2 },
      idempotency_key: idem,
      happened_at: new Date().toISOString(),
    };

    const first = await request(baseUrl, 'POST', '/sync/push', {
      token,
      body: { device_id: deviceId, actions: [action] },
    });
    assert.equal(first.status, 200, first.text);
    assert.equal(first.json.results[0].status, 'APPLIED');

    const replay = await request(baseUrl, 'POST', '/sync/push', {
      token,
      body: { device_id: deviceId, actions: [action] },
    });
    assert.equal(replay.status, 200, replay.text);
    assert.equal(replay.json.results[0].idempotent_replay, true);

    const dupePattern = await request(baseUrl, 'POST', '/sync/push', {
      token,
      body: {
        device_id: deviceId,
        actions: [
          {
            ...action,
            client_action_id: 'local-2',
            idempotency_key: `sync-issue-dupe-${Date.now()}`,
          },
        ],
      },
    });
    assert.equal(dupePattern.status, 200, dupePattern.text);
    assert.equal(dupePattern.json.results[0].status, 'CONFLICT');
    assert.equal(dupePattern.json.results[0].conflict_code, 'DUPLICATE_ACTION_PATTERN');
  });

  test('sync pull + ack works after reconnect cycle', async () => {
    const pull = await request(baseUrl, 'GET', `/sync/pull?since_id=0&limit=50&device_id=${encodeURIComponent(deviceId)}`, { token });
    assert.equal(pull.status, 200, pull.text);
    assert.ok(Array.isArray(pull.json.rows));
    assert.ok(Number(pull.json.max_outbox_id) >= 0);

    if (Number(pull.json.max_outbox_id) > 0) {
      const ack = await request(baseUrl, 'POST', '/sync/ack', {
        token,
        body: { max_outbox_id: pull.json.max_outbox_id, device_id: deviceId },
      });
      assert.equal(ack.status, 200, ack.text);
      assert.equal(ack.json.status, 'ok');
    }
  });
}
