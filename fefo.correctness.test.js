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

const PORT = Number(process.env.TEST_PORT_FEFO || 5062);
const runReliability = process.env.RUN_RELIABILITY_TESTS === 'true';

let serverProc = null;
let baseUrl = null;
let token = null;
let pool = null;
let fefoCapable = false;

if (!runReliability) {
  test.skip('FEFO suite disabled. Set RUN_RELIABILITY_TESTS=true to run.', () => {});
} else {
  test.before(async () => {
    await seedUsers();
    const started = await startServer(PORT);
    serverProc = started.proc;
    baseUrl = started.baseUrl;
    token = (await login(baseUrl, 'STU-001', 'Test1234!')).token;
    pool = createPool();
    const col = await pool.query(
      `SELECT 1
       FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name = 'items'
         AND column_name = 'expiry_tracked'
       LIMIT 1`
    );
    fefoCapable = col.rows.length > 0;
  });

  test.after(async () => {
    await stopServer(serverProc);
    if (pool) await pool.end();
  });

  test('FEFO issues from earliest non-expired lot first', async (t) => {
    if (!fefoCapable) {
      t.skip('expiry_tracked column unavailable for current DB role');
      return;
    }
    const sku = `SKU-FEFO-${Date.now()}`;
    const item = await request(baseUrl, 'POST', '/create-item', {
      token,
      body: {
        sku_code: sku,
        name: 'FEFO Item',
        cost: 1,
        expiry_tracked: true,
      },
    });
    assert.equal(item.status, 200, item.text);
    const itemId = Number(item.json.id);

    const soonDate = new Date(Date.now() + 15 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const laterDate = new Date(Date.now() + 45 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

    const receiptA = await request(baseUrl, 'POST', '/stock-in', {
      token,
      headers: { 'idempotency-key': `fefo-a-${Date.now()}`, 'x-lot-code': 'LOT-A', 'x-expiry-date': laterDate },
      body: { item_id: itemId, quantity: 8, lot_code: 'LOT-A', expiry_date: laterDate },
    });
    assert.equal(receiptA.status, 200, receiptA.text);
    const receiptB = await request(baseUrl, 'POST', '/stock-in', {
      token,
      headers: { 'idempotency-key': `fefo-b-${Date.now()}`, 'x-lot-code': 'LOT-B', 'x-expiry-date': soonDate },
      body: { item_id: itemId, quantity: 5, lot_code: 'LOT-B', expiry_date: soonDate },
    });
    assert.equal(receiptB.status, 200, receiptB.text);

    const checkin = await request(baseUrl, 'POST', '/check-in', {
      token,
      body: {
        appointment_id: `APT-FEFO-${Date.now()}`,
        provider_card: 'STU-001',
        status: 'ACTIVE',
      },
    });
    assert.equal(checkin.status, 200, checkin.text);
    const encounterId = Number(checkin.json.encounter.id);

    const issue = await request(baseUrl, 'POST', '/issue-item', {
      token,
      headers: { 'idempotency-key': `fefo-issue-${Date.now()}`, 'x-location-code': 'MAIN' },
      body: { item_id: itemId, encounter_id: encounterId, quantity: 6 },
    });
    assert.equal(issue.status, 200, issue.text);
    assert.equal(issue.json.fefo, true);
    assert.ok(Array.isArray(issue.json.issued_lots));
    assert.equal(issue.json.issued_lots.length, 2);
    assert.equal(issue.json.issued_lots[0].lot_code, 'LOT-B');
    assert.equal(Number(issue.json.issued_lots[0].quantity), 5);
    assert.equal(issue.json.issued_lots[1].lot_code, 'LOT-A');
    assert.equal(Number(issue.json.issued_lots[1].quantity), 1);

    const lotBalances = await pool.query(
      `SELECT lot_code, quantity
       FROM inventory_lot_balances
       WHERE item_id = $1
       ORDER BY lot_code ASC`,
      [itemId]
    );
    const lotA = lotBalances.rows.find((row) => row.lot_code === 'LOT-A');
    const lotB = lotBalances.rows.find((row) => row.lot_code === 'LOT-B');
    assert.ok(lotA);
    assert.ok(lotB);
    assert.equal(Number(lotA.quantity), 7);
    assert.equal(Number(lotB.quantity), 0);
  });

  test('FEFO rejects issue when only expired lot stock exists', async (t) => {
    if (!fefoCapable) {
      t.skip('expiry_tracked column unavailable for current DB role');
      return;
    }
    const sku = `SKU-FEFO-EXP-${Date.now()}`;
    const item = await request(baseUrl, 'POST', '/create-item', {
      token,
      body: {
        sku_code: sku,
        name: 'Expired FEFO Item',
        cost: 1,
        expiry_tracked: true,
      },
    });
    assert.equal(item.status, 200, item.text);
    const itemId = Number(item.json.id);

    const expiredDate = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const receipt = await request(baseUrl, 'POST', '/stock-in', {
      token,
      headers: { 'idempotency-key': `fefo-exp-rcpt-${Date.now()}`, 'x-lot-code': 'LOT-EXP', 'x-expiry-date': expiredDate },
      body: { item_id: itemId, quantity: 4, lot_code: 'LOT-EXP', expiry_date: expiredDate },
    });
    assert.equal(receipt.status, 200, receipt.text);

    const checkin = await request(baseUrl, 'POST', '/check-in', {
      token,
      body: {
        appointment_id: `APT-FEFO-EXP-${Date.now()}`,
        provider_card: 'STU-001',
        status: 'ACTIVE',
      },
    });
    assert.equal(checkin.status, 200, checkin.text);
    const encounterId = Number(checkin.json.encounter.id);

    const issue = await request(baseUrl, 'POST', '/issue-item', {
      token,
      headers: { 'idempotency-key': `fefo-exp-issue-${Date.now()}`, 'x-location-code': 'MAIN' },
      body: { item_id: itemId, encounter_id: encounterId, quantity: 1 },
    });
    assert.equal(issue.status, 400, issue.text);
    assert.match(issue.text, /FEFO/i);
  });
}
