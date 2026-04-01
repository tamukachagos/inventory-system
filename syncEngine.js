const DB_NAME = 'inventory_sync_db';
const DB_VERSION = 1;
const OUTBOX_STORE = 'outbox_actions';
const CONFLICT_STORE = 'conflict_items';
const STATE_STORE = 'sync_state';

const openSyncDb = () => new Promise((resolve, reject) => {
  const request = indexedDB.open(DB_NAME, DB_VERSION);
  request.onerror = () => reject(request.error || new Error('Failed to open sync DB'));
  request.onupgradeneeded = () => {
    const db = request.result;
    if (!db.objectStoreNames.contains(OUTBOX_STORE)) {
      const outbox = db.createObjectStore(OUTBOX_STORE, { keyPath: 'id', autoIncrement: true });
      outbox.createIndex('by_created_at', 'created_at');
    }
    if (!db.objectStoreNames.contains(CONFLICT_STORE)) {
      const conflicts = db.createObjectStore(CONFLICT_STORE, { keyPath: 'id', autoIncrement: true });
      conflicts.createIndex('by_created_at', 'created_at');
    }
    if (!db.objectStoreNames.contains(STATE_STORE)) {
      db.createObjectStore(STATE_STORE, { keyPath: 'key' });
    }
  };
  request.onsuccess = () => resolve(request.result);
});

const txDone = (tx) => new Promise((resolve, reject) => {
  tx.oncomplete = () => resolve();
  tx.onerror = () => reject(tx.error || new Error('IndexedDB tx failed'));
  tx.onabort = () => reject(tx.error || new Error('IndexedDB tx aborted'));
});

const genIdempotency = () => (window.crypto?.randomUUID ? window.crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`);

export const getDeviceId = () => {
  const key = 'inventory_sync_device_id';
  let value = localStorage.getItem(key);
  if (!value) {
    value = `tablet-${genIdempotency()}`;
    localStorage.setItem(key, value);
  }
  return value;
};

export const enqueueOfflineAction = async ({ action_type, path, method, body, idempotency_key }) => {
  const db = await openSyncDb();
  const tx = db.transaction(OUTBOX_STORE, 'readwrite');
  tx.objectStore(OUTBOX_STORE).add({
    action_type,
    path,
    method,
    body: body || {},
    idempotency_key: idempotency_key || genIdempotency(),
    created_at: new Date().toISOString(),
  });
  await txDone(tx);
  db.close();
};

export const listOutboxActions = async () => {
  const db = await openSyncDb();
  const tx = db.transaction(OUTBOX_STORE, 'readonly');
  const req = tx.objectStore(OUTBOX_STORE).getAll();
  const rows = await new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result || []);
    req.onerror = () => reject(req.error);
  });
  await txDone(tx);
  db.close();
  return rows.sort((a, b) => Number(a.id) - Number(b.id));
};

export const removeOutboxActions = async (ids) => {
  if (!Array.isArray(ids) || ids.length === 0) return;
  const db = await openSyncDb();
  const tx = db.transaction(OUTBOX_STORE, 'readwrite');
  const store = tx.objectStore(OUTBOX_STORE);
  ids.forEach((id) => store.delete(id));
  await txDone(tx);
  db.close();
};

export const listConflictItems = async () => {
  const db = await openSyncDb();
  const tx = db.transaction(CONFLICT_STORE, 'readonly');
  const req = tx.objectStore(CONFLICT_STORE).getAll();
  const rows = await new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result || []);
    req.onerror = () => reject(req.error);
  });
  await txDone(tx);
  db.close();
  return rows.sort((a, b) => Number(b.id) - Number(a.id));
};

export const clearConflictItem = async (id) => {
  const db = await openSyncDb();
  const tx = db.transaction(CONFLICT_STORE, 'readwrite');
  tx.objectStore(CONFLICT_STORE).delete(id);
  await txDone(tx);
  db.close();
};

export const appendConflictItems = async (conflicts) => {
  if (!Array.isArray(conflicts) || conflicts.length === 0) return;
  const db = await openSyncDb();
  const tx = db.transaction(CONFLICT_STORE, 'readwrite');
  const store = tx.objectStore(CONFLICT_STORE);
  conflicts.forEach((conflict) => store.add({ ...conflict, created_at: new Date().toISOString() }));
  await txDone(tx);
  db.close();
};

const getStateValue = async (key, fallback = null) => {
  const db = await openSyncDb();
  const tx = db.transaction(STATE_STORE, 'readonly');
  const req = tx.objectStore(STATE_STORE).get(key);
  const row = await new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result || null);
    req.onerror = () => reject(req.error);
  });
  await txDone(tx);
  db.close();
  return row ? row.value : fallback;
};

const setStateValue = async (key, value) => {
  const db = await openSyncDb();
  const tx = db.transaction(STATE_STORE, 'readwrite');
  tx.objectStore(STATE_STORE).put({ key, value });
  await txDone(tx);
  db.close();
};

export const runSyncCycle = async ({ apiBaseUrl, token }) => {
  if (!token || !navigator.onLine) {
    return { pushed: 0, conflicts: 0, pulled: 0 };
  }
  const deviceId = getDeviceId();
  const outbox = await listOutboxActions();
  let pushed = 0;
  let conflicts = 0;
  if (outbox.length > 0) {
    const pushPayload = {
      device_id: deviceId,
      actions: outbox.map((row) => ({
        client_action_id: `local-${row.id}`,
        action_type: row.action_type,
        path: row.path,
        method: row.method,
        body: row.body || {},
        idempotency_key: row.idempotency_key || genIdempotency(),
        happened_at: row.created_at,
      })),
    };
    const response = await fetch(`${apiBaseUrl}/sync/push`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(pushPayload),
    });
    const pushResult = await response.json().catch(() => ({}));
    if (response.ok) {
      const okStatuses = new Set(['APPLIED', 'DUPLICATE']);
      const removeIds = [];
      const localByKey = new Map(outbox.map((row) => [row.idempotency_key, row.id]));
      (pushResult.results || []).forEach((result) => {
        const localId = localByKey.get(result.idempotency_key);
        if (!localId) return;
        if (okStatuses.has(result.status)) {
          removeIds.push(localId);
          pushed += 1;
        } else if (result.status === 'CONFLICT' || result.status === 'REJECTED') {
          removeIds.push(localId);
          conflicts += 1;
        }
      });
      await removeOutboxActions(removeIds);
      await appendConflictItems(
        (pushResult.results || [])
          .filter((result) => result.status === 'CONFLICT' || result.status === 'REJECTED')
          .map((result) => ({
            type: 'push_conflict',
            idempotency_key: result.idempotency_key,
            conflict_code: result.conflict_code || null,
            detail: result,
          }))
      );
    }
  }

  const lastOutboxId = Number(await getStateValue('last_outbox_id', 0));
  const pullResp = await fetch(`${apiBaseUrl}/sync/pull?since_id=${lastOutboxId}&limit=200&device_id=${encodeURIComponent(deviceId)}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  let pulled = 0;
  if (pullResp.ok) {
    const pull = await pullResp.json().catch(() => ({}));
    const rows = Array.isArray(pull.rows) ? pull.rows : [];
    pulled = rows.length;
    const maxOutboxId = Number(pull.max_outbox_id || lastOutboxId);
    if (maxOutboxId > lastOutboxId) {
      await setStateValue('last_outbox_id', maxOutboxId);
      await fetch(`${apiBaseUrl}/sync/ack`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ max_outbox_id: maxOutboxId, device_id: deviceId }),
      });
    }
  }

  const conflictResp = await fetch(`${apiBaseUrl}/sync/conflicts`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (conflictResp.ok) {
    const payload = await conflictResp.json().catch(() => ({}));
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    await appendConflictItems(
      rows.map((row) => ({
        type: 'server_conflict',
        sync_inbox_id: row.id,
        idempotency_key: row.idempotency_key,
        conflict_code: row.conflict_code,
        detail: row,
      }))
    );
  }

  return { pushed, conflicts, pulled };
};

export const normalizeWriteActionForQueue = ({ path, method, body, headers }) => {
  const normalizedMethod = String(method || 'GET').toUpperCase();
  const actionType = (() => {
    if (path.startsWith('/stock-in')) return 'STOCK_IN';
    if (path.startsWith('/issue')) return 'ISSUE_ITEM';
    if (path.startsWith('/return')) return 'RETURN_ITEM';
    if (path.startsWith('/check-in')) return 'CHECKIN';
    if (path.startsWith('/transfers')) return 'TRANSFER_ACTION';
    return 'WRITE';
  })();
  return {
    action_type: actionType,
    path,
    method: normalizedMethod,
    body: body || {},
    idempotency_key: headers?.['Idempotency-Key'] || headers?.['idempotency-key'] || genIdempotency(),
  };
};

