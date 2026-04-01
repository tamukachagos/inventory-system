# Reliability Checklist

## 1) API Contract Reliability
- [ ] `POST /login` returns token, refresh token, user role context, capabilities.
- [ ] `POST /create-item` succeeds for authorized role and returns canonical item object.
- [ ] `POST /stock-in` increases stock and emits `STOCK_IN` transaction.
- [ ] `POST /issue-item` decrements stock safely and records `ISSUE`.
- [ ] Cycle count adjustment flow (`/cycle-counts` -> `/lines` -> `/submit` -> `/approve`) updates on-hand quantity exactly.
- [ ] Reporting endpoints (`/dashboard/kpis`, `/dashboard/report`) return stable schemas for dashboard rendering.

## 2) Database Migration Reliability
- [ ] Migration is idempotent (can run multiple times safely).
- [ ] Required tables exist (`users`, `items`, `encounters`, `inventory_transactions`, `audit_logs`, `event_ledger`, cycle count tables).
- [ ] Required columns exist (password policy columns, inventory quantities, ledger hash fields).
- [ ] Critical indexes exist (`student_card`, `sku_code`, `encounter_code`, `event_hash` uniqueness path).

## 3) Concurrency and Consistency
- [ ] Simultaneous stock issues never produce negative quantity.
- [ ] Final stock equals initial stock minus successful issues.
- [ ] At least one failing request occurs when demand exceeds stock (no silent over-issue).

## 4) Idempotency / Double-submit Protection
- [ ] Duplicate stock movement (same user/action/payload inside dedup window) is rejected with HTTP `409`.
- [ ] Inventory mutation occurs only once for duplicate-submitted payload.

## 5) UI E2E Reliability (Playwright)
- [ ] User can login from branded login screen.
- [ ] Dashboard renders KPI cards successfully.
- [ ] Check-in workflow creates encounter and shows success feedback.
- [ ] Stock-in scan workflow records stock movement and shows success feedback.

## 6) CI Reliability Gates
- [ ] Backend lint.
- [ ] Migration + seed scripts run in CI database service.
- [ ] Backend tests (security + contract + concurrency + migration).
- [ ] Frontend lint + build.
- [ ] Playwright Chromium E2E.
- [ ] Dependency audit for backend and frontend.

## Coverage Goal
- Backend reliability/security test coverage target: **75%+ statements** and **90%+ for critical stock/auth flows**.
- Frontend E2E smoke target: **100% of critical operational journeys** (login, check-in, stock-in, issue path smoke).
