const path = require('node:path');
const fs = require('node:fs');
const { spawn } = require('node:child_process');
const net = require('node:net');
const bcrypt = require('bcryptjs');
const { Pool } = require('pg');
require('dotenv').config();

const ROOT = __dirname;

const createPool = () =>
  new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: Number(process.env.DB_PORT),
    ssl: false,
  });

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const createOutputBuffer = () => {
  const chunks = [];
  return {
    append(chunk) {
      if (!chunk) return;
      chunks.push(chunk.toString());
      if (chunks.length > 200) chunks.shift();
    },
    dump() {
      return chunks.join('').trim();
    },
  };
};

const waitForHealth = async (baseUrl, retries = 120, delayMs = 250) => {
  for (let i = 0; i < retries; i += 1) {
    try {
      const response = await fetch(`${baseUrl}/health`);
      if (response.ok) return true;
    } catch (_) {}
    await wait(delayMs);
  }
  return false;
};

const waitForPort = async (port, host = '127.0.0.1', retries = 120, delayMs = 250) => {
  for (let i = 0; i < retries; i += 1) {
    const connected = await new Promise((resolve) => {
      const socket = net.createConnection({ port, host });
      const finish = (result) => {
        socket.removeAllListeners();
        socket.destroy();
        resolve(result);
      };
      socket.setTimeout(500);
      socket.once('connect', () => finish(true));
      socket.once('timeout', () => finish(false));
      socket.once('error', () => finish(false));
    });
    if (connected) return true;
    await wait(delayMs);
  }
  return false;
};

const startServer = async (port) => {
  const stdout = createOutputBuffer();
  const stderr = createOutputBuffer();
  let exitInfo = null;
  const proc = spawn('node', ['server.js'], {
    cwd: ROOT,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, PORT: String(port) },
  });
  proc.stdout.on('data', (chunk) => stdout.append(chunk));
  proc.stderr.on('data', (chunk) => stderr.append(chunk));
  proc.once('exit', (code, signal) => {
    exitInfo = { code, signal };
  });
  const baseUrl = `http://127.0.0.1:${port}`;
  const portReady = await waitForPort(port);
  const ready = portReady || await waitForHealth(baseUrl);
  if (!ready) {
    proc.kill('SIGTERM');
    const serverLogs = [stdout.dump(), stderr.dump()].filter(Boolean).join('\n');
    const exitSummary = exitInfo ? ` exit=${JSON.stringify(exitInfo)}` : '';
    throw new Error(
      `Server failed to start on ${baseUrl}.${exitSummary}${serverLogs ? `\n--- server logs ---\n${serverLogs}` : ''}`
    );
  }
  return { proc, baseUrl };
};

const stopServer = async (proc) => {
  if (!proc) return;
  const exited = new Promise((resolve) => {
    proc.once('exit', () => resolve());
  });
  proc.kill('SIGTERM');
  const timeout = setTimeout(() => {
    if (!proc.killed) {
      proc.kill('SIGKILL');
    }
  }, 5_000);
  try {
    await exited;
  } finally {
    clearTimeout(timeout);
  }
};

const request = async (baseUrl, method, route, { token, body, headers: extraHeaders } = {}) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 60_000);
  let response;
  try {
    response = await fetch(`${baseUrl}${route}`, {
      method,
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...(extraHeaders || {}),
      },
      body: typeof body === 'undefined' ? undefined : JSON.stringify(body),
    });
  } catch (err) {
    if (err && err.name === 'AbortError') {
      throw new Error(`Request timed out: ${method} ${route}`);
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
  const text = await response.text();
  let json = {};
  try {
    json = JSON.parse(text);
  } catch (_) {}
  return { status: response.status, json, text };
};

const login = async (baseUrl, studentCard, password) => {
  const response = await request(baseUrl, 'POST', '/login', {
    body: { student_card: studentCard, password },
  });
  if (response.status !== 200) {
    throw new Error(`Login failed for ${studentCard}: ${response.text}`);
  }
  return {
    token: response.json.token,
    refreshToken: response.json.refresh_token,
    user: response.json.user,
  };
};

const seedUsers = async () => {
  const pool = createPool();
  try {
    const adminHash = await bcrypt.hash('Admin1234!', 10);
    const staffHash = await bcrypt.hash('Test1234!', 10);
    const staff2Hash = await bcrypt.hash('Second123!', 10);
    await pool.query(
      `INSERT INTO users
         (student_card, name, role, password_hash, is_active, must_reset_password, password_changed_at, password_expires_at)
       VALUES
         ('ADM-001','Admin User','ADMIN',$1,TRUE,FALSE,NOW(),NOW() + INTERVAL '90 days')
       ON CONFLICT (student_card)
       DO UPDATE SET
         name = EXCLUDED.name,
         role = EXCLUDED.role,
         password_hash = EXCLUDED.password_hash,
         is_active = TRUE,
         must_reset_password = FALSE,
         password_changed_at = NOW(),
         password_expires_at = NOW() + INTERVAL '90 days'`,
      [adminHash]
    );
    await pool.query(
      `INSERT INTO users
         (student_card, name, role, password_hash, is_active, must_reset_password, password_changed_at, password_expires_at)
       VALUES
         ('STU-001','Test Staff','STAFF',$1,TRUE,FALSE,NOW(),NOW() + INTERVAL '90 days')
       ON CONFLICT (student_card)
       DO UPDATE SET
         name = EXCLUDED.name,
         role = EXCLUDED.role,
         password_hash = EXCLUDED.password_hash,
         is_active = TRUE,
         must_reset_password = FALSE,
         password_changed_at = NOW(),
         password_expires_at = NOW() + INTERVAL '90 days'`,
      [staffHash]
    );
    await pool.query(
      `INSERT INTO users
         (student_card, name, role, password_hash, is_active, must_reset_password, password_changed_at, password_expires_at)
       VALUES
         ('STU-999','Second Staff','STAFF',$1,TRUE,FALSE,NOW(),NOW() + INTERVAL '90 days')
       ON CONFLICT (student_card)
       DO UPDATE SET
         name = EXCLUDED.name,
         role = EXCLUDED.role,
         password_hash = EXCLUDED.password_hash,
         is_active = TRUE,
         must_reset_password = FALSE,
         password_changed_at = NOW(),
         password_expires_at = NOW() + INTERVAL '90 days'`,
      [staff2Hash]
    );
  } finally {
    await pool.end();
  }
};

const migrationSql = () => fs.readFileSync(path.join(ROOT, 'migration.sql'), 'utf8');

module.exports = {
  ROOT,
  createPool,
  startServer,
  stopServer,
  request,
  login,
  seedUsers,
  migrationSql,
};
