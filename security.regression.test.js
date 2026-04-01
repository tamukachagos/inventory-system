const test = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const path = require('node:path');
const bcrypt = require('bcryptjs');
const { Pool } = require('pg');
require('dotenv').config();

const TEST_PORT = Number(process.env.TEST_PORT || 5055);
const BASE = process.env.BASE_URL || `http://127.0.0.1:${TEST_PORT}`;
const ROOT = path.resolve(__dirname, '..');

let serverProc = null;

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const request = async (method, route, { token, body } = {}) => {
  const response = await fetch(`${BASE}${route}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: typeof body === 'undefined' ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  let json = {};
  try {
    json = JSON.parse(text);
  } catch (_) {}
  return { status: response.status, json, text };
};

const ensureServerReady = async () => {
  for (let i = 0; i < 40; i += 1) {
    try {
      const r = await fetch(`${BASE}/health`);
      if (r.ok) return true;
    } catch (_) {}
    await wait(250);
  }
  return false;
};

const login = async (studentCard, password) => {
  const result = await request('POST', '/login', {
    body: { student_card: studentCard, password },
  });
  assert.equal(result.status, 200, `login failed: ${result.text}`);
  return {
    token: result.json.token,
    refreshToken: result.json.refresh_token,
  };
};

const seedUsers = async () => {
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: Number(process.env.DB_PORT),
    ssl: false,
  });

  try {
    const hash1 = await bcrypt.hash('Test1234!', 10);
    const hash2 = await bcrypt.hash('Second123!', 10);
    await pool.query(
      `INSERT INTO users
         (student_card, name, role, password_hash, is_active, must_reset_password, password_changed_at, password_expires_at)
       VALUES
         ('STU-001','Test Student','STAFF',$1,TRUE,FALSE,NOW(),NOW() + INTERVAL '90 days')
       ON CONFLICT (student_card)
       DO UPDATE SET
         name = EXCLUDED.name,
         role = EXCLUDED.role,
         password_hash = EXCLUDED.password_hash,
         is_active = TRUE,
         must_reset_password = FALSE,
         password_changed_at = NOW(),
         password_expires_at = NOW() + INTERVAL '90 days'`,
      [hash1]
    );
    await pool.query(
      `INSERT INTO users
         (student_card, name, role, password_hash, is_active, must_reset_password, password_changed_at, password_expires_at)
       VALUES
         ('STU-999','Second User','STAFF',$1,TRUE,FALSE,NOW(),NOW() + INTERVAL '90 days')
       ON CONFLICT (student_card)
       DO UPDATE SET
         name = EXCLUDED.name,
         role = EXCLUDED.role,
         password_hash = EXCLUDED.password_hash,
         is_active = TRUE,
         must_reset_password = FALSE,
         password_changed_at = NOW(),
         password_expires_at = NOW() + INTERVAL '90 days'`,
      [hash2]
    );
  } finally {
    await pool.end();
  }
};

test.before(async () => {
  serverProc = spawn('node', ['server.js'], {
    cwd: ROOT,
    stdio: 'ignore',
    env: { ...process.env, PORT: String(TEST_PORT) },
  });
  const ready = await ensureServerReady();
  assert.equal(ready, true, 'server did not become ready for integration tests');
  await seedUsers();
});

test.after(async () => {
  if (serverProc) {
    serverProc.kill('SIGTERM');
  }
});

test('mass assignment is blocked on issue-item user_id spoof', async () => {
  const { token } = await login('STU-001', 'Test1234!');

  const checkin = await request('POST', '/check-in', {
    token,
    body: {
      appointment_id: `APT-REG-${Date.now()}`,
      provider_card: 'STU-001',
      status: 'ACTIVE',
    },
  });
  assert.equal(checkin.status, 200, checkin.text);
  const encounterId = checkin.json.encounter.id;

  const issue = await request('POST', '/issue-item', {
    token,
    body: {
      item_id: 1,
      encounter_id: encounterId,
      user_id: 2,
      quantity: 1,
    },
  });
  assert.equal(issue.status, 200, issue.text);
  assert.equal(Number(issue.json.user_id), 1);
});

test('print job retry forbids non-owner', async () => {
  const { token: token1 } = await login('STU-001', 'Test1234!');
  const { token: token2 } = await login('STU-999', 'Second123!');

  const job = await request('POST', '/print/zebra', {
    token: token1,
    body: {
      zpl: '^XA^FO60,60^A0N,30,30^FDTest^FS^XZ',
      printer_ip: '192.168.1.250',
      printer_port: 9100,
    },
  });
  assert.equal(job.status, 200, job.text);

  const retry = await request('POST', `/print-jobs/${job.json.print_job_id}/retry`, {
    token: token2,
    body: {},
  });
  assert.equal(retry.status, 403, retry.text);
});

test('printer target allowlist blocks public IPs', async () => {
  const { token } = await login('STU-001', 'Test1234!');
  const blocked = await request('POST', '/print/zebra', {
    token,
    body: {
      zpl: '^XA^FO60,60^A0N,30,30^FDProbe^FS^XZ',
      printer_ip: '8.8.8.8',
      printer_port: 9100,
    },
  });
  assert.equal(blocked.status, 400, blocked.text);
});

test('logout revokes token replay', async () => {
  const { token } = await login('STU-001', 'Test1234!');
  const before = await request('GET', '/me/capabilities', { token });
  assert.equal(before.status, 200, before.text);

  const out = await request('POST', '/logout', { token, body: {} });
  assert.equal(out.status, 200, out.text);

  const after = await request('GET', '/me/capabilities', { token });
  assert.equal(after.status, 401, after.text);
});

test('refresh token rotates and old refresh token is rejected', async () => {
  const { token, refreshToken } = await login('STU-001', 'Test1234!');
  assert.ok(token);
  assert.ok(refreshToken);

  const refreshed = await request('POST', '/auth/refresh', {
    body: { refresh_token: refreshToken },
  });
  assert.equal(refreshed.status, 200, refreshed.text);
  assert.ok(refreshed.json.token);
  assert.ok(refreshed.json.refresh_token);

  const replay = await request('POST', '/auth/refresh', {
    body: { refresh_token: refreshToken },
  });
  assert.equal(replay.status, 401, replay.text);
});

test('logout invalidates refresh session', async () => {
  const { token, refreshToken } = await login('STU-001', 'Test1234!');
  const out = await request('POST', '/logout', { token, body: {} });
  assert.equal(out.status, 200, out.text);

  const refreshAfterLogout = await request('POST', '/auth/refresh', {
    body: { refresh_token: refreshToken },
  });
  assert.equal(refreshAfterLogout.status, 401, refreshAfterLogout.text);
});
