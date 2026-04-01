# Offline Sync Protocol (Clinic Tablets)

## Overview
- Model: `offline-first`, client outbox + server reconciliation.
- Transport: HTTPS JSON.
- Write safety: every write must include `idempotency_key`.
- Source of truth: append-only ledgers (`inventory_transactions`, `stock_ledger`).

## Client Storage
- IndexedDB database: `inventory_sync_db`
- Stores:
  - `outbox_actions`: pending offline writes
  - `conflict_items`: server conflicts/rejections for user resolution
  - `sync_state`: last pulled outbox cursor (`last_outbox_id`)

## Server Tables
- `sync_inbox`: incoming client actions and reconciliation result
- `sync_outbox`: server events to deliver to clients

## API

### `POST /sync/push`
- Auth required (`ADMIN`/`STAFF`)
- Body:
```json
{
  "device_id": "tablet-ward-a-01",
  "actions": [
    {
      "client_action_id": "local-123",
      "action_type": "ISSUE_ITEM",
      "path": "/issue-item",
      "method": "POST",
      "body": { "item_id": 10, "encounter_id": 22, "quantity": 2 },
      "idempotency_key": "a5d6...uuid",
      "happened_at": "2026-02-22T17:00:00.000Z"
    }
  ]
}
```
- Response includes per-action status:
  - `APPLIED`
  - `DUPLICATE` (idempotent replay)
  - `CONFLICT` (duplicate pattern or write rejected)
  - `REJECTED` (policy violations like direct on-hand edit)

### `GET /sync/pull?since_id=<n>&limit=<n>&device_id=<id>`
- Returns server outbox events for user/device since cursor.

### `POST /sync/ack`
- Marks pulled outbox rows as acknowledged.

### `GET /sync/conflicts`
- Returns unresolved conflict/rejected inbox rows for conflict UI.

## Reconciliation Rules
- Reject direct on-hand edits (`PATCH/PUT` on inventory totals).
- Reject duplicate by idempotency key (`sync_inbox(user_id,idempotency_key)` unique).
- Conflict-detect duplicate issue/receipt pattern via action fingerprint.
- Execute valid actions through normal API handlers (same auth/validation/business rules).
- Never mutate ledgers directly; only append via existing write endpoints.

## Conflict Handling UX
- Client stores conflicts locally and exposes a queue.
- User may:
  - retry with corrected payload/new idempotency key
  - discard local action
  - inspect server rejection reason

## Recommended Sync Loop
1. On reconnect or interval:
   - push pending outbox actions (`/sync/push`)
2. Pull server updates (`/sync/pull`)
3. Ack pulled rows (`/sync/ack`)
4. Refresh local UI snapshots

