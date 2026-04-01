BEGIN;

-- Ensure base transfer schema objects exist (run migration first in real environments).

WITH org_ref AS (
  SELECT id AS org_id
  FROM organizations
  WHERE code = 'DEFAULT_ORG'
  ORDER BY id
  LIMIT 1
)
INSERT INTO clinics (org_id, code, name)
SELECT org_ref.org_id, 'CLINIC02', 'Clinic 02'
FROM org_ref
WHERE NOT EXISTS (
  SELECT 1
  FROM clinics
  WHERE clinics.org_id = org_ref.org_id
    AND clinics.code = 'CLINIC02'
);

WITH org_ref AS (
  SELECT id AS org_id
  FROM organizations
  WHERE code = 'DEFAULT_ORG'
  ORDER BY id
  LIMIT 1
)
INSERT INTO clinics (org_id, code, name)
SELECT org_ref.org_id, 'CLINIC03', 'Clinic 03'
FROM org_ref
WHERE NOT EXISTS (
  SELECT 1
  FROM clinics
  WHERE clinics.org_id = org_ref.org_id
    AND clinics.code = 'CLINIC03'
);

WITH from_clinic AS (
  SELECT id AS clinic_id, org_id
  FROM clinics
  WHERE code = 'CLINIC01'
  ORDER BY id
  LIMIT 1
),
to_clinic AS (
  SELECT id AS clinic_id
  FROM clinics
  WHERE code = 'CLINIC02'
    AND org_id = (SELECT org_id FROM from_clinic)
  ORDER BY id
  LIMIT 1
),
actor AS (
  SELECT id AS user_id
  FROM users
  WHERE role IN ('ADMIN', 'STAFF')
  ORDER BY CASE role WHEN 'ADMIN' THEN 0 ELSE 1 END, id
  LIMIT 1
),
item_ref AS (
  SELECT id AS item_id
  FROM items
  ORDER BY id
  LIMIT 1
)
INSERT INTO transfer_requests
  (org_id, from_clinic_id, to_clinic_id, status, needed_by, requested_by, notes)
SELECT
  from_clinic.org_id,
  from_clinic.clinic_id,
  to_clinic.clinic_id,
  'APPROVED',
  NOW() + INTERVAL '2 days',
  actor.user_id,
  'Sample approved transfer request'
FROM from_clinic, to_clinic, actor
WHERE EXISTS (SELECT 1 FROM item_ref);

WITH transfer_ref AS (
  SELECT id AS transfer_request_id
  FROM transfer_requests
  WHERE notes = 'Sample approved transfer request'
  ORDER BY id DESC
  LIMIT 1
),
item_ref AS (
  SELECT id AS item_id
  FROM items
  ORDER BY id
  LIMIT 1
)
INSERT INTO transfer_request_lines
  (transfer_request_id, item_id, requested_qty, approved_qty, picked_qty, received_qty, cancelled_qty, line_status)
SELECT
  transfer_ref.transfer_request_id,
  item_ref.item_id,
  10,
  10,
  4,
  2,
  0,
  'PARTIALLY_RECEIVED'
FROM transfer_ref, item_ref
WHERE NOT EXISTS (
  SELECT 1
  FROM transfer_request_lines l
  WHERE l.transfer_request_id = transfer_ref.transfer_request_id
    AND l.item_id = item_ref.item_id
);

COMMIT;
