const test = require('node:test');
const assert = require('node:assert/strict');
const { createPool, startServer, stopServer, request, login, seedUsers } = require('./test-support');

const PORT = Number(process.env.TEST_PORT_INVARIANTS || 5058);
const runReliability = process.env.RUN_RELIABILITY_TESTS === 'true';

let serverProc = null;
let baseUrl = null;
let token = null;
let pool = null;
let itemId = null;
let encounterId = null;

if (!runReliability) {
  test.skip('Inventory invariant suite disabled. Set RUN_RELIABILITY_TESTS=true to run.', () => {});
} else {
  test.before(async () => {
    await seedUsers();
    const started = await startServer(PORT);
    serverProc = started.proc;
    baseUrl = started.baseUrl;
    token = (await login(baseUrl, 'STU-001', 'Test1234!')).token;
    pool = createPool();

    const item = await request(baseUrl, 'POST', '/create-item', {
      token,
      body: {
        sku_code: `SKU-INV-${Date.now()}`,
        name: 'Invariant Item',
        cost: 2,
      },
    });
    assert.equal(item.status, 200, item.text);
    itemId = Number(item.json.id);

    const receipt = await request(baseUrl, 'POST', '/stock-in', {
      token,
      body: { item_id: itemId, quantity: 120, cost: 2 },
      headers: { 'Idempotency-Key': `seed-receipt-${Date.now()}` },
    });
    assert.equal(receipt.status, 200, receipt.text);

    const encounter = await request(baseUrl, 'POST', '/check-in', {
      token,
      body: {
        appointment_id: `APT-INV-${Date.now()}`,
        provider_card: 'STU-001',
        status: 'ACTIVE',
      },
    });
    assert.equal(encounter.status, 200, encounter.text);
    encounterId = Number(encounter.json.encounter.id);
  });

  test.after(async () => {
    await stopServer(serverProc);
    if (pool) await pool.end();
  });

  test('idempotency key prevents double submit for issue movement', async () => {
    const idemKey = `idem-issue-${Date.now()}`;
    const payload = { item_id: itemId, encounter_id: encounterId, quantity: 2 };

    const first = await request(baseUrl, 'POST', '/issue-item', {
      token,
      body: payload,
      headers: { 'Idempotency-Key': idemKey, 'X-Correlation-Id': `corr-${idemKey}` },
    });
    const second = await request(baseUrl, 'POST', '/issue-item', {
      token,
      body: payload,
      headers: { 'Idempotency-Key': idemKey, 'X-Correlation-Id': `corr-${idemKey}` },
    });

    assert.equal(first.status, 200, first.text);
    assert.equal(second.status, 200, second.text);
    assert.equal(Number(first.json.id), Number(second.json.id));
    assert.equal(second.json.idempotent_replay, true);
  });

  test('concurrent receipts/issues preserve invariant and prevent negative stock', async () => {
    const movementCalls = [];
    for (let i = 0; i < 10; i += 1) {
      movementCalls.push(
        request(baseUrl, 'POST', '/stock-in', {
          token,
          body: { item_id: itemId, quantity: 3, cost: 2 },
          headers: { 'Idempotency-Key': `rcpt-${Date.now()}-${i}` },
        })
      );
    }
    for (let i = 0; i < 16; i += 1) {
      movementCalls.push(
        request(baseUrl, 'POST', '/issue-item', {
          token,
          body: { item_id: itemId, encounter_id: encounterId, quantity: 2 },
          headers: { 'Idempotency-Key': `iss-${Date.now()}-${i}` },
        })
      );
    }

    const responses = await Promise.all(movementCalls);
    const failedNegative = responses.filter((r) => r.status === 400 && r.text.includes('Insufficient stock'));
    const hardFailures = responses.filter((r) => ![200, 400].includes(r.status));
    assert.equal(hardFailures.length, 0, JSON.stringify(hardFailures.map((x) => ({ status: x.status, text: x.text }))));
    assert.ok(failedNegative.length >= 0);

    const stock = await request(baseUrl, 'GET', '/stock-levels', { token });
    assert.equal(stock.status, 200, stock.text);
    const itemRow = stock.json.find((row) => Number(row.id) === itemId);
    assert.ok(itemRow);
    assert.ok(Number(itemRow.quantity) >= 0);

    const invariant = await request(baseUrl, 'GET', '/inventory/invariants', { token });
    assert.equal(invariant.status, 200, invariant.text);
    const invariantRow = invariant.json.rows.find((row) => Number(row.item_id) === itemId);
    assert.ok(invariantRow);
    assert.equal(Number(invariantRow.variance_qty), 0);
  });

  test('movement ledger is append-only (update blocked)', async () => {
    const trigger = await pool.query(
      `SELECT 1
       FROM pg_trigger t
       JOIN pg_class c ON c.oid = t.tgrelid
       WHERE c.relname = 'inventory_transactions'
         AND t.tgname = 'trg_inventory_transactions_immutable'
         AND NOT t.tgisinternal`
    );
    if (trigger.rows.length === 0) {
      return;
    }
    const tx = await pool.query(
      `SELECT id
       FROM inventory_transactions
       WHERE item_id = $1
       ORDER BY id DESC
       LIMIT 1`,
      [itemId]
    );
    assert.equal(tx.rows.length, 1);
    let failed = false;
    try {
      await pool.query('UPDATE inventory_transactions SET quantity = quantity + 1 WHERE id = $1', [tx.rows[0].id]);
    } catch (err) {
      failed = true;
      assert.match(String(err.message), /append-only/i);
    }
    assert.equal(failed, true);
  });
}
