# Enterprise Migration Strategy

## Objectives
- Prevent unsafe production schema changes.
- Make migrations idempotent and traceable.
- Preserve tablet compatibility during rolling upgrades.

## Controls Implemented
- `schema_versions` table tracks:
  - `version_tag`
  - SQL `checksum`
  - `applied_at`, `applied_by`, `notes`
- Migration runner safety checks in `scripts/run-migration.js`:
  - advisory transaction lock (`pg_advisory_xact_lock`)
  - lock timeout + statement timeout
  - production guard (`ALLOW_PROD_MIGRATION=true` required in prod)
  - read-replica guard (`pg_is_in_recovery` must be false)
  - idempotent skip when same `version_tag + checksum` already recorded

## Rollout Sequence
1. Run migration in staging.
2. Run smoke suite:
   - health
   - release controls
   - sync baseline
3. Run integration tests.
4. Promote to pilot clinic only.
5. Expand by feature flag rollout percentage.

## Failure / Rollback
- Schema rollback should be handled by forward-fix migrations.
- Feature deactivation is done immediately using `feature_flags`.
- Tablet clients continue using stable API paths and idempotent retries.

## Operational Commands
```powershell
npm run db:migrate
npm run test:smoke
npm run test:integration
```
