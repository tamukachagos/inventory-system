# Multi-Clinic Refactor Plan (Principal Architecture)

## 1) Updated ERD (Text)

```text
organizations (1) ────< clinics (N) ────< locations (N)
      │                      │
      │                      └───< clinic_item_settings (N) >───(1) item_master
      │
      └───< users (N) ────< user_clinic_roles (N) >───(1) clinics

item_master (1) ────< inventory_lots (N)
      │
      └───< stock_ledger (N) >───(1) clinics
                             ├───(0..1) locations
                             ├───(0..1) inventory_lots
                             ├───(0..1) encounters
                             └───(1) users (actor)

encounters (N) now include clinic_id + location_id

Legacy compatibility:
items -> mapped into item_master by sku_code
inventory_transactions -> mirrored/backfilled into stock_ledger
```

## 2) SQL Migrations

Implemented in:
- `migrations/20260222_multiclinic_refactor.sql`

Key additions:
- Hierarchy: `organizations`, `clinics`, `locations`
- Global catalog: `item_master`, `clinic_item_settings`, `inventory_lots`
- Clinic RBAC tables: `user_clinic_roles`
- Encounter scoping: `encounters.clinic_id`, `encounters.location_id`
- Source-of-truth ledger: `stock_ledger` (append-only)
- Computed on-hand:
  - `stock_on_hand`
  - `stock_on_hand_nonnegative_violations`
- Transition bridge:
  - Backfill from legacy `inventory_transactions` to `stock_ledger`
  - Trigger `trg_mirror_legacy_inventory_transaction` to dual-write during cutover

## 3) API Changes (v2 Model)

### Organization / Clinic / Location
- `GET /v2/orgs/:orgId/clinics`
- `GET /v2/clinics/:clinicId/locations`
- `POST /v2/clinics/:clinicId/locations`

### Item Master + Clinic Settings
- `GET /v2/items/master`
- `POST /v2/items/master`
- `PATCH /v2/items/master/:itemMasterId`
- `GET /v2/clinics/:clinicId/items`
- `PUT /v2/clinics/:clinicId/items/:itemMasterId/settings`

### Stock Ledger (authoritative)
- `POST /v2/stock/movements`
  - Required: `clinic_id`, `location_id`, `item_master_id`, `qty_delta`, `movement_type`,
    `reason_code`, `movement_source`, `correlation_id`, `idempotency_key`, `actor_user_id`
- `GET /v2/stock/on-hand?clinic_id=&location_id=&item_master_id=&lot_id=`
- `GET /v2/stock/ledger?clinic_id=&from=&to=&item_master_id=`

### Dashboards
- `GET /v2/dashboard/clinic/:clinicId`
- `GET /v2/dashboard/network` (cross-clinic rollup for org roles)

### Backwards-compatibility
- Keep existing `/stock-in`, `/issue-item`, `/return-item` alive.
- During transition, they continue writing legacy table and auto-mirror to `stock_ledger`.
- New reads should progressively switch to `/v2/stock/on-hand` or `stock_on_hand`.

## 4) RBAC Changes

### Roles
- `ORG_ADMIN`:
  - full access across all clinics in org
- `CLINIC_MANAGER`:
  - full access within assigned clinic(s)
- `WAREHOUSE_STAFF`:
  - stock operations within assigned clinic(s)/locations
- `AUDITOR`:
  - read ledger/invariants/network dashboards
- `READ_ONLY`:
  - limited operational visibility

### Enforcement model
- JWT claims include:
  - `org_id`
  - `clinic_access: number[]`
  - `active_clinic_id`
  - role grants
- Middleware checks:
  - org-level routes: role in `ORG_ADMIN|AUDITOR`
  - clinic routes: `clinic_id` must be in `clinic_access` unless org-global role
  - movement write routes: actor must have write permissions on target clinic/location

## 5) Backwards-Compatible Data Migration Plan

### Phase 0 (Preparation)
1. Deploy migration creating new tables/views/triggers without disabling legacy APIs.
2. Seed default org/clinic/location for single-site legacy data.

### Phase 1 (Backfill)
1. Backfill `item_master` from `items`.
2. Backfill `stock_ledger` from `inventory_transactions` with deterministic `correlation_id`:
   - `legacy-itx-<id>`
3. Validate reconciliation:
   - compare `items.quantity` vs `stock_on_hand` for default clinic/location.

### Phase 2 (Dual-write)
1. Keep legacy APIs active.
2. Trigger mirrors new legacy inserts into `stock_ledger`.
3. Monitor `stock_on_hand_nonnegative_violations` and reconciliation drift.

### Phase 3 (Read-cutover)
1. Move all dashboards/stock reads to `stock_on_hand` / `stock_ledger`.
2. Keep legacy writes temporarily for rollback safety.

### Phase 4 (Write-cutover)
1. Move write APIs to direct `stock_ledger` writes (`/v2/stock/movements`).
2. Decommission legacy write path after stabilization window.

### Phase 5 (Cleanup)
1. Freeze legacy `inventory_transactions` or retain as compatibility view.
2. Remove mirror trigger only after all writes use v2.

## 6) Test Plan

### A. Data Integrity / Invariants
- Per clinic/location/lot:
  - `on_hand = sum(qty_delta)` from `stock_ledger`
- No negatives unless explicitly allowed with reason
- Correlation id uniqueness
- Idempotency `(actor_user_id, idempotency_key)` uniqueness

### B. Concurrency
- 100+ simultaneous issues on same SKU/location:
  - no negative on-hand
  - no lost updates
- mixed receipts/issues with serializable isolation:
  - deterministic final balance

### C. RBAC
- clinic-scoped user cannot read/write outside assigned clinics
- org admin can view network rollups
- auditor can read ledger but cannot write movement

### D. Compatibility
- legacy endpoint write -> mirrored `stock_ledger` row appears
- backfilled correlation IDs remain unique and stable
- dashboards match pre-cutover totals

### E. Performance / Scale
- 10 clinics x 100k movements:
  - on-hand query latency under agreed SLO
  - dashboard rollup indexes effective

## 7) Scalability Notes (10+ Clinics, No Rewrite)

Design avoids clinic-specific schema branches:
- All clinic separation is data-driven (`clinic_id`, `location_id`).
- Global catalog (`item_master`) centralizes SKU governance.
- Ledger model supports additional dimensions (lot/vendor/source) via columns + `metadata`.
- Indexing strategy supports horizontal growth without schema redesign.
