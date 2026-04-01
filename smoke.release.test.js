const test = require('node:test');
const assert = require('node:assert/strict');
const {
  startServer,
  stopServer,
  request,
  login,
  seedUsers,
} = require('./test-support');

const PORT = Number(process.env.TEST_PORT_SMOKE || 5065);

let serverProc = null;
let baseUrl = null;
let adminToken = null;
let staffToken = null;

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

test('smoke: health endpoints are reachable', async () => {
  const health = await request(baseUrl, 'GET', '/health');
  assert.equal(health.status, 200, health.text);
  assert.equal(health.json.status, 'ok');

  const db = await request(baseUrl, 'GET', '/health/db');
  assert.equal(db.status, 200, db.text);
  assert.equal(db.json.status, 'ok');
});

test('smoke: release controls endpoints respond', async () => {
  const schemaVersion = await request(baseUrl, 'GET', '/release/schema-version', { token: adminToken });
  assert.equal(schemaVersion.status, 200, schemaVersion.text);
  assert.ok(Object.prototype.hasOwnProperty.call(schemaVersion.json, 'latest'));

  const flagUpsert = await request(baseUrl, 'POST', '/feature-flags', {
    token: adminToken,
    body: {
      clinic_id: 1,
      flag_key: `SMOKE_FLAG_${Date.now()}`,
      enabled: true,
      rollout_percentage: 100,
      payload: { env: 'smoke' },
    },
  });
  assert.equal(flagUpsert.status, 200, flagUpsert.text);

  const evaluate = await request(baseUrl, 'GET', '/feature-flags/evaluate', { token: staffToken });
  assert.equal(evaluate.status, 200, evaluate.text);
  assert.equal(typeof evaluate.json.flags, 'object');
});

test('smoke: sync endpoints baseline flow responds', async () => {
  const pull = await request(baseUrl, 'GET', '/sync/pull?since_id=0&limit=20&device_id=smoke-device', { token: staffToken });
  assert.equal(pull.status, 200, pull.text);
  assert.ok(Array.isArray(pull.json.rows));

  const ack = await request(baseUrl, 'POST', '/sync/ack', {
    token: staffToken,
    body: { max_outbox_id: 1, device_id: 'smoke-device' },
  });
  assert.equal(ack.status, 200, ack.text);
  assert.equal(ack.json.status, 'ok');
});
