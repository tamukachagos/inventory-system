const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const { z } = require('zod');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const net = require('net');
const crypto = require('crypto');
const ExcelJS = require('exceljs');
const bcrypt = require('bcryptjs');
const argon2 = require('argon2');
const jwt = require('jsonwebtoken');
if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

const app = express();
const isProduction = process.env.NODE_ENV === 'production';

const requireEnv = (name) => {
  const value = process.env[name];
  if (typeof value === 'undefined' || value === '') {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
};

const configuredOrigins = (process.env.CORS_ORIGIN || '')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);
const ALLOW_LAN_CORS = process.env.ALLOW_LAN_CORS === 'true';

const localDevOrigins = [
  'http://localhost:3000',
  'http://localhost:5173',
  'http://127.0.0.1:3000',
  'http://127.0.0.1:5173',
];

const allowedOrigins = new Set(isProduction ? configuredOrigins : [...localDevOrigins, ...configuredOrigins]);

const isPrivateIPv4 = (hostname) => {
  const parts = hostname.split('.').map((part) => Number(part));
  if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) {
    return false;
  }

  if (parts[0] === 10) return true;
  if (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) return true;
  if (parts[0] === 192 && parts[1] === 168) return true;
  return false;
};

const isLocalNetworkOrigin = (origin) => {
  try {
    const { hostname } = new URL(origin);
    if (hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '::1') {
      return true;
    }
    return isPrivateIPv4(hostname);
  } catch (_) {
    return false;
  }
};

const isSameHostOrigin = (origin, req) => {
  try {
    return new URL(origin).host === req.get('host');
  } catch (_) {
    return false;
  }
};

app.use(
  cors((req, callback) => {
    const origin = req.header('Origin');
    let allowed = false;
    if (!origin) {
      allowed = true;
    } else if (isSameHostOrigin(origin, req)) {
      allowed = true;
    } else if (allowedOrigins.has(origin)) {
      allowed = true;
    } else if (ALLOW_LAN_CORS && isLocalNetworkOrigin(origin)) {
      allowed = true;
    }

    if (!allowed) {
      return callback(new Error('CORS origin not allowed'));
    }

    return callback(null, {
      origin: true,
      credentials: true,
    });
  })
);
app.use(helmet({
  contentSecurityPolicy: false,
  crossOriginEmbedderPolicy: false,
}));
app.use(express.json({ limit: process.env.JSON_BODY_LIMIT || '1mb' }));
app.use(express.urlencoded({ extended: false, limit: process.env.URLENCODED_BODY_LIMIT || '1mb' }));

const ipRateLimit = rateLimit({
  windowMs: Number(process.env.RATE_LIMIT_WINDOW_MS || 60_000),
  max: Number(process.env.RATE_LIMIT_MAX || 180),
  standardHeaders: true,
  legacyHeaders: false,
});
const loginRateLimit = rateLimit({
  windowMs: Number(process.env.LOGIN_RATE_LIMIT_WINDOW_MS || 15 * 60_000),
  max: Number(process.env.LOGIN_RATE_LIMIT_MAX || 12),
  standardHeaders: true,
  legacyHeaders: false,
});
app.use(ipRateLimit);

/* ---------------------------
   DATABASE CONNECTION
---------------------------- */

const pool = new Pool({
  user: requireEnv('DB_USER'),
  host: requireEnv('DB_HOST'),
  database: requireEnv('DB_NAME'),
  password: requireEnv('DB_PASSWORD'),
  port: Number(requireEnv('DB_PORT')),
  ssl: isProduction ? { rejectUnauthorized: false } : false,
});

/* ---------------------------
   JWT AUTHENTICATION & RBAC
---------------------------- */

const JWT_SECRET = requireEnv('JWT_SECRET');
const JWT_ACCESS_EXPIRES_IN = process.env.JWT_ACCESS_EXPIRES_IN || '15m';
const JWT_REFRESH_EXPIRES_DAYS = Number(process.env.JWT_REFRESH_EXPIRES_DAYS || 7);
const PASSWORD_MAX_AGE_DAYS = Number(process.env.PASSWORD_MAX_AGE_DAYS || 90);
const APP_ALLOWED_LOGIN_ROLES = new Set(['ADMIN', 'STAFF']);
const PRINT_WORKER_ENABLED = process.env.PRINT_WORKER_ENABLED !== 'false';
const PRINT_WORKER_INTERVAL_MS = Number(process.env.PRINT_WORKER_INTERVAL_MS || 3000);
const PRINT_WORKER_BATCH_SIZE = Number(process.env.PRINT_WORKER_BATCH_SIZE || 5);
const EXPIRY_ALERT_WORKER_ENABLED = process.env.EXPIRY_ALERT_WORKER_ENABLED !== 'false';
const EXPIRY_ALERT_WORKER_INTERVAL_MS = Number(process.env.EXPIRY_ALERT_WORKER_INTERVAL_MS || 24 * 60 * 60 * 1000);
const ACTION_DEDUP_WINDOW_MS = Number(process.env.ACTION_DEDUP_WINDOW_MS || 4000);
const ROLE_CAPABILITIES = {
  ADMIN: [
    'CHECKIN',
    'STOCK_IN',
    'ISSUE_ITEM',
    'RETURN_ITEM',
    'CYCLE_COUNT',
    'PURCHASE_IMPORT',
    'PRINT_QUEUE',
    'AI_ASSISTANT',
    'VIEW_AUDIT_LOGS',
    'VIEW_FRAUD_FLAGS',
    'RUN_FRAUD_ANALYSIS',
    'VIEW_EVENT_LEDGER',
    'MANAGE_USERS',
    'MANAGE_IMPORT_TEMPLATES',
    'MANAGE_PRINT_WORKER',
    'TRANSFER',
    'REORDER_PLANNER',
  ],
  STAFF: [
    'CHECKIN',
    'STOCK_IN',
    'ISSUE_ITEM',
    'RETURN_ITEM',
    'CYCLE_COUNT',
    'PURCHASE_IMPORT',
    'PRINT_QUEUE',
    'AI_ASSISTANT',
    'TRANSFER',
    'REORDER_PLANNER',
  ],
};
const printWorkerState = {
  timer: null,
  running: false,
  lastRunAt: null,
  lastProcessed: 0,
  lastError: null,
};
const expiryAlertWorkerState = {
  timer: null,
  running: false,
  lastRunAt: null,
  lastProcessed: 0,
  lastError: null,
};
const recentActionFingerprints = new Map();
const revokedTokenDigests = new Map();
const recentUserRequestCounters = new Map();
const USER_RATE_LIMIT_WINDOW_MS = Number(process.env.USER_RATE_LIMIT_WINDOW_MS || 60_000);
const USER_RATE_LIMIT_MAX = Number(process.env.USER_RATE_LIMIT_MAX || 240);
let refreshTableReady = false;
let movementSchemaReady = false;
let movementSchemaExtended = false;
let transferSchemaReady = false;
let lotExpirySchemaReady = false;
let expiryTrackedColumnAvailable = false;
let reorderSchemaReady = false;
let syncSchemaReady = false;
let releaseControlSchemaReady = false;

// Validation schemas
const loginSchema = z.object({
  student_card: z.string().min(1, 'student_card is required'),
  password: z.string().min(1, 'password is required'),
});

const transactionSchema = z.object({
  item_id: z.coerce.number().int().positive(),
  encounter_id: z.coerce.number().int().positive(),
  quantity: z.coerce.number().int().positive(),
});
const scanTransactionSchema = z.object({
  encounter_code: z.string().min(1, 'encounter_code is required'),
  item_barcode: z.string().min(1, 'item_barcode is required'),
  operator_card: z.string().min(1, 'operator_card is required'),
  quantity: z.coerce.number().int().positive().optional().default(1),
});

const stockInSchema = z.object({
  item_id: z.coerce.number().int().positive(),
  quantity: z.coerce.number().int().positive(),
  cost: z.coerce.number().positive().optional(),
  vendor: z.string().optional(),
  batch_number: z.string().optional(),
  lot_code: z.string().optional(),
  expiry_date: z.string().optional(),
});
const stockInScanSchema = z.object({
  item_barcode: z.string().min(1, 'item_barcode is required'),
  quantity: z.coerce.number().int().positive(),
  item_name: z.string().optional(),
  cost: z.coerce.number().nonnegative().optional(),
  lot_code: z.string().optional(),
  expiry_date: z.string().optional(),
  print_new_label: z.boolean().optional().default(false),
  printer_ip: z.string().optional(),
  printer_port: z.coerce.number().int().positive().optional().default(9100),
});

const createUserSchema = z.object({
  student_card: z.string().min(1),
  name: z.string().min(1),
  password: z.string().min(6, 'Password must be at least 6 characters'),
  role: z.enum(['ADMIN', 'STAFF', 'STUDENT']).default('STUDENT'),
});

const changePasswordSchema = z.object({
  current_password: z.string().min(1, 'current_password is required'),
  new_password: z.string().min(8, 'new_password must be at least 8 characters'),
});

const updateUserAccessSchema = z.object({
  role: z.enum(['ADMIN', 'STAFF', 'STUDENT']).optional(),
  is_active: z.boolean().optional(),
  force_password_reset: z.boolean().optional(),
});

const createItemSchema = z.object({
  sku_code: z.string().min(1, 'sku_code is required'),
  name: z.string().min(1, 'name is required'),
  cost: z.coerce.number().nonnegative().optional(),
  expiry_tracked: z.boolean().optional().default(false),
});

const createEncounterSchema = z.object({
  encounter_code: z.string().min(1, 'encounter_code is required'),
  scheduled_time: z.coerce.date().optional(),
  status: z.enum(['ACTIVE', 'COMPLETED', 'CANCELLED']).optional(),
});

const encounterCheckInSchema = z.object({
  appointment_id: z.string().min(1, 'appointment_id is required'),
  provider_card: z.string().min(1, 'provider_card is required'),
  scheduled_time: z.coerce.date().optional(),
  status: z.enum(['ACTIVE', 'COMPLETED', 'CANCELLED']).optional().default('ACTIVE'),
  print_label: z.boolean().optional().default(false),
  printer_ip: z.string().min(1).optional(),
  printer_port: z.coerce.number().int().positive().optional().default(9100),
  copies: z.coerce.number().int().min(1).max(5).optional().default(1),
});

const zebraPrintSchema = z.object({
  zpl: z.string().min(1, 'zpl is required'),
  printer_ip: z.string().min(1, 'printer_ip is required'),
  printer_port: z.coerce.number().int().positive().optional().default(9100),
  dry_run: z.boolean().optional().default(false),
});

const encounterLabelSchema = z.object({
  encounter_code: z.string().min(1, 'encounter_code is required'),
  appointment_id: z.string().optional(),
  provider_card: z.string().optional(),
  provider_name: z.string().optional(),
  copies: z.coerce.number().int().min(1).max(5).optional().default(1),
});

const auditLogQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(500).optional().default(100),
  offset: z.coerce.number().int().min(0).optional().default(0),
  action: z.string().optional(),
  severity: z.enum(['INFO', 'WARN', 'ERROR']).optional(),
  actor_user_id: z.coerce.number().int().positive().optional(),
});

const fraudFlagQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(500).optional().default(100),
  offset: z.coerce.number().int().min(0).optional().default(0),
  status: z.enum(['OPEN', 'REVIEWING', 'RESOLVED', 'DISMISSED']).optional(),
  min_score: z.coerce.number().nonnegative().optional(),
});

const cycleCountCreateSchema = z.object({
  notes: z.string().optional(),
});

const cycleCountLineSchema = z.object({
  item_id: z.coerce.number().int().positive(),
  counted_qty: z.coerce.number().int().min(0),
});

const cycleCountApproveSchema = z.object({
  decision: z.enum(['APPROVED', 'REJECTED']),
  notes: z.string().optional(),
  apply_adjustments: z.boolean().optional().default(false),
});
const cycleCountRandomSchema = z.object({
  item_count: z.coerce.number().int().min(1).max(500),
  notes: z.string().optional(),
});
const encounterAssistantScanSchema = z.object({
  assistant_card: z.string().min(1, 'assistant_card is required'),
});

const fraudFlagUpdateSchema = z.object({
  status: z.enum(['OPEN', 'REVIEWING', 'RESOLVED', 'DISMISSED']),
});

const purchaseImportCsvSchema = z.object({
  source_name: z.string().min(1, 'source_name is required'),
  csv_text: z.string().min(1, 'csv_text is required'),
  auto_create_items: z.boolean().optional().default(false),
  template_id: z.coerce.number().int().positive().optional(),
});

const purchaseImportXlsxSchema = z.object({
  source_name: z.string().min(1, 'source_name is required'),
  file_base64: z.string().min(1, 'file_base64 is required'),
  sheet_name: z.string().optional(),
  auto_create_items: z.boolean().optional().default(false),
  template_id: z.coerce.number().int().positive().optional(),
});

const printJobQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(500).optional().default(100),
  offset: z.coerce.number().int().min(0).optional().default(0),
  status: z.enum(['QUEUED', 'SUCCESS', 'FAILED']).optional(),
});

const importTemplateSchema = z.object({
  vendor_name: z.string().min(1, 'vendor_name is required'),
  description: z.string().optional(),
  required_columns: z.array(z.string().min(1)).optional().default([]),
  column_map: z.record(z.array(z.string().min(1))).optional().default({}),
  is_active: z.boolean().optional().default(true),
});
const importTemplateUpdateSchema = z.object({
  description: z.string().optional(),
  required_columns: z.array(z.string().min(1)).optional(),
  column_map: z.record(z.array(z.string().min(1))).optional(),
  is_active: z.boolean().optional(),
}).refine((value) => Object.keys(value).length > 0, {
  message: 'At least one field is required',
});

const aiAssistantSchema = z.object({
  message: z.string().min(1, 'message is required'),
});

const refreshTokenSchema = z.object({
  refresh_token: z.string().min(20, 'refresh_token is required'),
});
const emptyBodySchema = z.object({}).strict();
const positiveIdParamSchema = z.object({
  id: z.coerce.number().int().positive(),
});
const itemIdParamSchema = z.object({
  item_id: z.coerce.number().int().positive(),
});
const encounterIdParamSchema = z.object({
  encounter_id: z.coerce.number().int().positive(),
});
const providerScopedQuerySchema = z.object({
  provider_user_id: z.coerce.number().int().positive().optional(),
});
const dashboardRangeQuerySchema = z.object({
  from: z.string().optional(),
  to: z.string().optional(),
  provider_user_id: z.coerce.number().int().positive().optional(),
});
const eventLedgerQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(500).optional().default(100),
  offset: z.coerce.number().int().min(0).optional().default(0),
});
const intelligenceQuerySchema = z.object({
  lookback_days: z.coerce.number().int().min(14).max(120).optional(),
  target_days: z.coerce.number().int().min(7).max(60).optional(),
  provider_user_id: z.coerce.number().int().positive().optional(),
});
const transactionsQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(200).optional().default(50),
  offset: z.coerce.number().int().min(0).optional().default(0),
  item_id: z.coerce.number().int().positive().optional(),
  encounter_id: z.coerce.number().int().positive().optional(),
  user_id: z.coerce.number().int().positive().optional(),
  type: z.enum(['ISSUE', 'RETURN', 'STOCK_IN', 'ADJUST_IN', 'ADJUST_OUT']).optional(),
  from: z.string().optional(),
  to: z.string().optional(),
});
const encounterCostQuerySchema = z.object({
  from: z.string().optional(),
  to: z.string().optional(),
});
const userAccessParamSchema = z.object({
  id: z.coerce.number().int().positive(),
});
const importTemplateParamSchema = z.object({
  id: z.coerce.number().int().positive(),
});
const fraudFlagParamSchema = z.object({
  id: z.coerce.number().int().positive(),
});
const transferRequestCreateSchema = z.object({
  from_clinic_id: z.coerce.number().int().positive(),
  to_clinic_id: z.coerce.number().int().positive(),
  needed_by: z.string().optional(),
  notes: z.string().optional(),
  items: z.array(
    z.object({
      item_id: z.coerce.number().int().positive(),
      requested_qty: z.coerce.number().positive(),
    })
  ).min(1),
});
const transferListQuerySchema = z.object({
  status: z.string().optional(),
  limit: z.coerce.number().int().min(1).max(200).optional().default(50),
  offset: z.coerce.number().int().min(0).optional().default(0),
});
const transferActionParamSchema = z.object({
  id: z.coerce.number().int().positive(),
});
const transferApproveSchema = z.object({
  decision: z.enum(['APPROVE', 'REJECT']),
  notes: z.string().optional(),
  lines: z.array(
    z.object({
      item_id: z.coerce.number().int().positive(),
      approved_qty: z.coerce.number().nonnegative(),
    })
  ).optional(),
});
const transferPickPackSchema = z.object({
  notes: z.string().optional(),
  lines: z.array(
    z.object({
      item_id: z.coerce.number().int().positive(),
      quantity: z.coerce.number().positive(),
    })
  ).min(1),
});
const transferReceiveSchema = z.object({
  notes: z.string().optional(),
  lines: z.array(
    z.object({
      item_id: z.coerce.number().int().positive(),
      quantity: z.coerce.number().positive(),
    })
  ).min(1),
});
const transferCancelSchema = z.object({
  reason: z.string().optional(),
  lines: z.array(
    z.object({
      item_id: z.coerce.number().int().positive(),
      quantity: z.coerce.number().positive(),
    })
  ).optional(),
});
const expirySuggestionQuerySchema = z.object({
  days: z.coerce.number().int().min(7).max(120).optional().default(30),
});
const reorderRecommendationQuerySchema = z.object({
  clinic_id: z.coerce.number().int().positive().optional(),
  location_code: z.string().optional(),
  include_zero_shortage: z.coerce.boolean().optional().default(false),
});
const syncPullQuerySchema = z.object({
  since_id: z.coerce.number().int().min(0).optional().default(0),
  limit: z.coerce.number().int().min(1).max(500).optional().default(100),
  device_id: z.string().min(1).max(120).optional(),
});
const syncAckSchema = z.object({
  max_outbox_id: z.coerce.number().int().positive(),
  device_id: z.string().min(1).max(120).optional(),
});
const syncPushActionSchema = z.object({
  client_action_id: z.string().min(1).max(120).optional(),
  action_type: z.string().min(1).max(60),
  path: z.string().min(1).max(255),
  method: z.enum(['POST', 'PUT', 'PATCH', 'DELETE']),
  body: z.record(z.any()).optional().default({}),
  idempotency_key: z.string().min(1).max(120),
  happened_at: z.string().optional(),
});
const syncPushSchema = z.object({
  device_id: z.string().min(1).max(120),
  actions: z.array(syncPushActionSchema).min(1).max(200),
});
const featureFlagUpsertSchema = z.object({
  clinic_id: z.coerce.number().int().positive(),
  flag_key: z.string().min(1).max(100),
  enabled: z.boolean(),
  rollout_percentage: z.coerce.number().int().min(0).max(100).optional().default(100),
  payload: z.record(z.any()).optional().default({}),
});
const featureFlagQuerySchema = z.object({
  clinic_id: z.coerce.number().int().positive().optional(),
  flag_key: z.string().optional(),
});

const formatEncounterCode = () => {
  const now = new Date();
  const datePart = now.toISOString().slice(0, 10).replace(/-/g, '');
  const randomPart = Math.floor(100000 + Math.random() * 900000);
  return `ENC-${datePart}-${randomPart}`;
};

const escapeZpl = (value) => String(value || '').replace(/\^/g, '').replace(/~/g, '');

const buildEncounterLabelZpl = ({
  encounterCode,
  appointmentId,
  providerCard,
  providerName,
  copies = 1,
}) => {
  const safeEncounter = escapeZpl(encounterCode);
  const safeAppointment = escapeZpl(appointmentId || 'N/A');
  const safeCard = escapeZpl(providerCard || 'N/A');
  const safeName = escapeZpl(providerName || 'N/A');

  return [
    '^XA',
    '^PW812',
    '^LL406',
    '^FO40,30^A0N,42,42^FDIndiana University Dental School^FS',
    '^FO40,85^A0N,34,34^FDEncounter Check-In Label^FS',
    `^FO40,140^A0N,30,30^FDEncounter: ${safeEncounter}^FS`,
    `^FO40,185^A0N,28,28^FDAppointment: ${safeAppointment}^FS`,
    `^FO40,225^A0N,28,28^FDProvider Card: ${safeCard}^FS`,
    `^FO40,260^A0N,24,24^FDProvider: ${safeName}^FS`,
    `^FO40,270^BY3,3,120^BCN,120,Y,N,N^FD${safeEncounter}^FS`,
    `^PQ${copies}`,
    '^XZ',
  ].join('\n');
};

const buildItemLabelZpl = ({ skuCode, itemName, copies = 1 }) => {
  const safeSku = escapeZpl(skuCode);
  const safeName = escapeZpl(itemName || skuCode);
  return [
    '^XA',
    '^PW812',
    '^LL300',
    '^FO40,30^A0N,36,36^FDIU Dental Warehouse Item^FS',
    `^FO40,80^A0N,30,30^FDPart: ${safeSku}^FS`,
    `^FO40,120^A0N,24,24^FD${safeName}^FS`,
    `^FO40,150^BY3,3,110^BCN,110,Y,N,N^FD${safeSku}^FS`,
    `^PQ${copies}`,
    '^XZ',
  ].join('\n');
};

const sendZplToPrinter = ({ printerIp, printerPort = 9100, zpl }) =>
  new Promise((resolve, reject) => {
    const socket = new net.Socket();
    let settled = false;

    const finish = (err) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      if (err) reject(err);
      else resolve();
    };

    socket.setTimeout(5000);
    socket.on('timeout', () => finish(new Error('Printer connection timed out')));
    socket.on('error', (err) => finish(err));

    socket.connect(printerPort, printerIp, () => {
      socket.write(zpl, (err) => {
        if (err) return finish(err);
        finish();
      });
    });
  });

const digestToken = (token) => crypto.createHash('sha256').update(token).digest('hex');

const isAllowedPrinterTarget = (ip) => {
  if (!ip) return false;
  if (ip === '127.0.0.1' || ip === 'localhost') return true;
  return isPrivateIPv4(ip);
};

const hashPassword = async (plain) =>
  argon2.hash(plain, {
    type: argon2.argon2id,
    memoryCost: Number(process.env.ARGON2_MEMORY_COST || 19456),
    timeCost: Number(process.env.ARGON2_TIME_COST || 2),
    parallelism: Number(process.env.ARGON2_PARALLELISM || 1),
  });

const verifyPassword = async (plain, hash) => {
  if (!hash) return false;
  if (hash.startsWith('$argon2')) return argon2.verify(hash, plain);
  return bcrypt.compare(plain, hash);
};

const maybeUpgradeHashToArgon2 = async ({ userId, plain, currentHash }) => {
  if (!currentHash || currentHash.startsWith('$argon2')) return;
  const newHash = await hashPassword(plain);
  await pool.query(
    `UPDATE users
     SET password_hash = $1,
         updated_at = NOW()
     WHERE id = $2`,
    [newHash, userId]
  );
};

const enforceUserRateLimit = (req, res) => {
  if (!req.user || !req.user.id) return false;
  const now = Date.now();
  const key = String(req.user.id);
  const existing = recentUserRequestCounters.get(key);
  if (!existing || existing.resetAt <= now) {
    recentUserRequestCounters.set(key, { count: 1, resetAt: now + USER_RATE_LIMIT_WINDOW_MS });
    return false;
  }

  existing.count += 1;
  if (existing.count > USER_RATE_LIMIT_MAX) {
    res.setHeader('Retry-After', Math.ceil((existing.resetAt - now) / 1000));
    res.status(429).json({ error: 'Too many requests for user. Please retry later.' });
    return true;
  }
  return false;
};

const validateRequest = ({ body, query, params } = {}) =>
  (req, res, next) => {
    if (body) {
      const parsedBody = body.safeParse(req.body ?? {});
      if (!parsedBody.success) {
        return res.status(400).json({ error: 'Invalid request body', issues: parsedBody.error.issues });
      }
      req.body = parsedBody.data;
    }
    if (query) {
      const parsedQuery = query.safeParse(req.query ?? {});
      if (!parsedQuery.success) {
        return res.status(400).json({ error: 'Invalid request query', issues: parsedQuery.error.issues });
      }
      req.query = parsedQuery.data;
    }
    if (params) {
      const parsedParams = params.safeParse(req.params ?? {});
      if (!parsedParams.success) {
        return res.status(400).json({ error: 'Invalid route parameters', issues: parsedParams.error.issues });
      }
      req.params = parsedParams.data;
    }
    return next();
  };

const ensureRefreshTable = async () => {
  if (refreshTableReady) return;
  await pool.query(
    `CREATE TABLE IF NOT EXISTS refresh_sessions (
       id BIGSERIAL PRIMARY KEY,
       user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
       token_hash VARCHAR(128) NOT NULL UNIQUE,
       expires_at TIMESTAMPTZ NOT NULL,
       revoked_at TIMESTAMPTZ,
       rotated_from BIGINT REFERENCES refresh_sessions(id) ON DELETE SET NULL,
       created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
     )`
  );
  refreshTableReady = true;
};

const ensureMovementSchema = async () => {
  if (movementSchemaReady) return;
  try {
    await pool.query(
      `ALTER TABLE inventory_transactions
        ADD COLUMN IF NOT EXISTS movement_actor_id BIGINT REFERENCES users(id) ON DELETE RESTRICT,
        ADD COLUMN IF NOT EXISTS reason_code VARCHAR(50) NOT NULL DEFAULT 'UNSPECIFIED',
        ADD COLUMN IF NOT EXISTS movement_source VARCHAR(40) NOT NULL DEFAULT 'API',
        ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(120),
        ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(120),
        ADD COLUMN IF NOT EXISTS location_code VARCHAR(60) NOT NULL DEFAULT 'MAIN',
        ADD COLUMN IF NOT EXISTS lot_code VARCHAR(120),
        ADD COLUMN IF NOT EXISTS allow_negative BOOLEAN NOT NULL DEFAULT FALSE`
    );
    await pool.query(`DROP TRIGGER IF EXISTS trg_inventory_transactions_immutable ON inventory_transactions`);
    await pool.query(
      `UPDATE inventory_transactions it
       SET movement_actor_id = COALESCE(
         it.movement_actor_id,
         it.user_id,
         (SELECT id FROM users ORDER BY id LIMIT 1)
       )
       WHERE it.movement_actor_id IS NULL`
    );
    await pool.query(
      `UPDATE inventory_transactions
       SET correlation_id = CONCAT('legacy-', id)
       WHERE correlation_id IS NULL`
    );
    await pool.query(`ALTER TABLE inventory_transactions ALTER COLUMN movement_actor_id SET NOT NULL`);
    await pool.query(`ALTER TABLE inventory_transactions ALTER COLUMN correlation_id SET NOT NULL`);
    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_transactions_actor_idemp
       ON inventory_transactions (movement_actor_id, idempotency_key)
       WHERE idempotency_key IS NOT NULL`
    );
    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_transactions_correlation_id
       ON inventory_transactions (correlation_id)`
    );
    await pool.query(
      `CREATE OR REPLACE VIEW inventory_balance_audit AS
       SELECT
         i.id AS item_id,
         i.sku_code,
         i.name,
         'MAIN'::varchar(60) AS location_code,
         NULL::varchar(120) AS lot_code,
         i.quantity::numeric AS on_hand_qty,
         COALESCE(SUM(CASE
           WHEN it.type IN ('STOCK_IN', 'RETURN', 'ADJUST_IN') THEN it.quantity
           WHEN it.type IN ('ISSUE', 'ADJUST_OUT') THEN -1 * it.quantity
           ELSE 0
         END), 0)::numeric AS derived_qty,
         (i.quantity::numeric - COALESCE(SUM(CASE
           WHEN it.type IN ('STOCK_IN', 'RETURN', 'ADJUST_IN') THEN it.quantity
           WHEN it.type IN ('ISSUE', 'ADJUST_OUT') THEN -1 * it.quantity
           ELSE 0
         END), 0)::numeric) AS variance_qty
       FROM items i
       LEFT JOIN inventory_transactions it ON it.item_id = i.id
       GROUP BY i.id, i.sku_code, i.name, i.quantity`
    );
    await pool.query(
      `DO $$
       BEGIN
         IF EXISTS (
           SELECT 1
           FROM pg_constraint c
           JOIN pg_class t ON t.oid = c.conrelid
           JOIN pg_namespace n ON n.oid = t.relnamespace
           WHERE c.conname = 'inventory_transactions_type_check'
             AND t.relname = 'inventory_transactions'
             AND n.nspname = current_schema()
         ) THEN
           ALTER TABLE inventory_transactions DROP CONSTRAINT inventory_transactions_type_check;
         END IF;
         ALTER TABLE inventory_transactions
           ADD CONSTRAINT inventory_transactions_type_check
           CHECK (type IN ('ISSUE', 'RETURN', 'STOCK_IN', 'ADJUST_IN', 'ADJUST_OUT'));
       EXCEPTION
         WHEN duplicate_object THEN NULL;
       END $$;`
    );
    await pool.query(
      `CREATE OR REPLACE FUNCTION prevent_inventory_transaction_mutation()
       RETURNS trigger
       LANGUAGE plpgsql
       AS $$
       BEGIN
         RAISE EXCEPTION 'inventory_transactions is append-only; % is not allowed', TG_OP;
       END;
       $$;`
    );
    await pool.query(`DROP TRIGGER IF EXISTS trg_inventory_transactions_immutable ON inventory_transactions`);
    await pool.query(
      `CREATE TRIGGER trg_inventory_transactions_immutable
       BEFORE UPDATE OR DELETE ON inventory_transactions
       FOR EACH ROW
       EXECUTE FUNCTION prevent_inventory_transaction_mutation()`
    );
  } catch (err) {
    console.warn('Movement schema hardening skipped:', err && err.message ? err.message : err);
  }
  const columns = await pool.query(
    `SELECT COUNT(*)::int AS count
     FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'inventory_transactions'
       AND column_name IN ('movement_actor_id','reason_code','movement_source','correlation_id','idempotency_key','location_code','lot_code','allow_negative')`
  );
  movementSchemaExtended = Number(columns.rows[0].count || 0) === 8;
  movementSchemaReady = true;
};

const transferActionIdempotencyKeyFromRequest = (req) =>
  sanitizeTokenLikeValue(req.headers['idempotency-key'] || req.headers['x-idempotency-key'], null);

const parseNumeric = (value) => Number.parseFloat(value || 0) || 0;

const deriveTransferLineStatus = (line) => {
  const approved = parseNumeric(line.approved_qty);
  const picked = parseNumeric(line.picked_qty);
  const received = parseNumeric(line.received_qty);
  const cancelled = parseNumeric(line.cancelled_qty);

  if (approved <= 0 && picked <= 0 && received <= 0 && cancelled <= 0) return 'PENDING';
  if (approved <= 0) return cancelled > 0 ? 'CANCELLED' : 'REJECTED';
  if (cancelled >= approved) return 'CANCELLED';
  const effectiveApproved = Math.max(approved - cancelled, 0);
  if (effectiveApproved > 0 && received >= effectiveApproved) return 'RECEIVED';
  if (received > 0) return 'PARTIALLY_RECEIVED';
  if (effectiveApproved > 0 && picked >= effectiveApproved) return 'IN_TRANSIT';
  if (picked > 0) return 'PARTIALLY_PICKED';
  if (cancelled > 0) return 'PARTIALLY_CANCELLED';
  return 'APPROVED';
};

const deriveTransferHeaderStatus = (lines) => {
  if (!Array.isArray(lines) || lines.length === 0) return 'PENDING';
  const statuses = lines.map((line) => deriveTransferLineStatus(line));

  if (statuses.every((status) => status === 'REJECTED')) return 'REJECTED';
  if (statuses.every((status) => status === 'CANCELLED' || status === 'REJECTED')) return 'CANCELLED';
  if (statuses.every((status) => ['RECEIVED', 'CANCELLED', 'REJECTED'].includes(status))) return 'RECEIVED';
  if (statuses.some((status) => status === 'PARTIALLY_RECEIVED' || status === 'RECEIVED')) return 'PARTIALLY_RECEIVED';
  if (statuses.some((status) => status === 'IN_TRANSIT')) {
    const allPicked = lines.every((line) => {
      const approved = parseNumeric(line.approved_qty);
      const picked = parseNumeric(line.picked_qty);
      const cancelled = parseNumeric(line.cancelled_qty);
      return approved <= 0 || picked >= Math.max(approved - cancelled, 0);
    });
    return allPicked ? 'IN_TRANSIT' : 'PARTIALLY_PICKED';
  }
  if (statuses.some((status) => status === 'PARTIALLY_PICKED')) return 'PARTIALLY_PICKED';
  if (statuses.some((status) => status === 'PARTIALLY_CANCELLED')) return 'PARTIALLY_CANCELLED';
  if (statuses.some((status) => status === 'APPROVED')) return 'APPROVED';
  return 'PENDING';
};

const requireTransferActionIdempotencyKey = (req, res) => {
  const key = transferActionIdempotencyKeyFromRequest(req);
  if (!key) {
    res.status(400).json({ error: 'idempotency-key header is required for transfer actions' });
    return null;
  }
  return key;
};

const ensureTransferSchema = async () => {
  if (transferSchemaReady) return;

  await pool.query(
    `CREATE TABLE IF NOT EXISTS organizations (
      id BIGSERIAL PRIMARY KEY,
      code VARCHAR(50) NOT NULL UNIQUE,
      name VARCHAR(255) NOT NULL,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS clinics (
      id BIGSERIAL PRIMARY KEY,
      org_id BIGINT NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
      code VARCHAR(50) NOT NULL,
      name VARCHAR(255) NOT NULL,
      timezone VARCHAR(80),
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (org_id, code)
    )`
  );
  await pool.query(
    `ALTER TABLE users
      ADD COLUMN IF NOT EXISTS org_id BIGINT REFERENCES organizations(id) ON DELETE SET NULL,
      ADD COLUMN IF NOT EXISTS home_clinic_id BIGINT REFERENCES clinics(id) ON DELETE SET NULL`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS item_master (
      id BIGSERIAL PRIMARY KEY,
      sku_code VARCHAR(120) NOT NULL UNIQUE,
      name VARCHAR(255) NOT NULL,
      active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS stock_ledger (
      id BIGSERIAL PRIMARY KEY,
      org_id BIGINT REFERENCES organizations(id) ON DELETE RESTRICT,
      clinic_id BIGINT REFERENCES clinics(id) ON DELETE RESTRICT,
      location_id BIGINT,
      item_master_id BIGINT NOT NULL REFERENCES item_master(id) ON DELETE RESTRICT,
      lot_id BIGINT,
      encounter_id BIGINT,
      movement_type VARCHAR(40) NOT NULL CHECK (
        movement_type IN ('RECEIPT', 'ISSUE', 'RETURN', 'ADJUST_IN', 'ADJUST_OUT', 'TRANSFER_IN', 'TRANSFER_OUT')
      ),
      qty_delta NUMERIC(14,3) NOT NULL CHECK (qty_delta <> 0),
      reason_code VARCHAR(60) NOT NULL,
      movement_source VARCHAR(60) NOT NULL,
      correlation_id VARCHAR(120) NOT NULL UNIQUE,
      idempotency_key VARCHAR(120),
      actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
      allow_negative BOOLEAN NOT NULL DEFAULT FALSE,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS stock_ledger (
      id BIGSERIAL PRIMARY KEY,
      org_id BIGINT REFERENCES organizations(id) ON DELETE RESTRICT,
      clinic_id BIGINT REFERENCES clinics(id) ON DELETE RESTRICT,
      location_id BIGINT,
      item_master_id BIGINT NOT NULL REFERENCES item_master(id) ON DELETE RESTRICT,
      lot_id BIGINT,
      encounter_id BIGINT,
      movement_type VARCHAR(40) NOT NULL CHECK (
        movement_type IN ('RECEIPT', 'ISSUE', 'RETURN', 'ADJUST_IN', 'ADJUST_OUT', 'TRANSFER_IN', 'TRANSFER_OUT')
      ),
      qty_delta NUMERIC(14,3) NOT NULL CHECK (qty_delta <> 0),
      reason_code VARCHAR(60) NOT NULL,
      movement_source VARCHAR(60) NOT NULL,
      correlation_id VARCHAR(120) NOT NULL UNIQUE,
      idempotency_key VARCHAR(120),
      actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
      allow_negative BOOLEAN NOT NULL DEFAULT FALSE,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS stock_ledger (
      id BIGSERIAL PRIMARY KEY,
      org_id BIGINT NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
      clinic_id BIGINT NOT NULL REFERENCES clinics(id) ON DELETE RESTRICT,
      location_id BIGINT,
      item_master_id BIGINT NOT NULL REFERENCES item_master(id) ON DELETE RESTRICT,
      lot_id BIGINT,
      encounter_id BIGINT REFERENCES encounters(id) ON DELETE SET NULL,
      movement_type VARCHAR(40) NOT NULL CHECK (
        movement_type IN ('RECEIPT', 'ISSUE', 'RETURN', 'ADJUST_IN', 'ADJUST_OUT', 'TRANSFER_IN', 'TRANSFER_OUT')
      ),
      qty_delta NUMERIC(14,3) NOT NULL CHECK (qty_delta <> 0),
      reason_code VARCHAR(60) NOT NULL,
      movement_source VARCHAR(60) NOT NULL,
      correlation_id VARCHAR(120) NOT NULL UNIQUE,
      idempotency_key VARCHAR(120),
      actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
      allow_negative BOOLEAN NOT NULL DEFAULT FALSE,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await pool.query(
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_ledger_actor_idemp
     ON stock_ledger(actor_user_id, idempotency_key)
     WHERE idempotency_key IS NOT NULL`
  );
  await pool.query(
    `INSERT INTO organizations (code, name)
     VALUES ('DEFAULT_ORG', 'Default Organization')
     ON CONFLICT (code) DO NOTHING`
  );
  await pool.query(
    `WITH org_ref AS (
       SELECT id AS org_id FROM organizations WHERE code = 'DEFAULT_ORG' ORDER BY id LIMIT 1
     )
     INSERT INTO clinics (org_id, code, name)
     SELECT org_ref.org_id, 'CLINIC01', 'Clinic 01'
     FROM org_ref
     WHERE NOT EXISTS (
       SELECT 1
       FROM clinics
       WHERE clinics.org_id = org_ref.org_id
         AND clinics.code = 'CLINIC01'
     )`
  );
  await pool.query(
    `WITH org_ref AS (
       SELECT id AS org_id FROM organizations WHERE code = 'DEFAULT_ORG' ORDER BY id LIMIT 1
     ),
     clinic_ref AS (
       SELECT id AS clinic_id FROM clinics WHERE code = 'CLINIC01' AND org_id = (SELECT org_id FROM org_ref) ORDER BY id LIMIT 1
     )
     UPDATE users u
     SET org_id = COALESCE(u.org_id, (SELECT org_id FROM org_ref)),
         home_clinic_id = COALESCE(u.home_clinic_id, (SELECT clinic_id FROM clinic_ref))
     WHERE u.org_id IS NULL OR u.home_clinic_id IS NULL`
  );
  await pool.query(
    `INSERT INTO item_master (sku_code, name, active, created_at, updated_at)
     SELECT i.sku_code, i.name, COALESCE(i.active, TRUE), COALESCE(i.created_at, NOW()), NOW()
     FROM items i
     ON CONFLICT (sku_code) DO UPDATE
     SET name = EXCLUDED.name,
         updated_at = NOW()`
  );
  await pool.query(
    `WITH org_ref AS (
       SELECT id AS org_id FROM organizations WHERE code = 'DEFAULT_ORG' ORDER BY id LIMIT 1
     ),
     clinic_ref AS (
       SELECT id AS clinic_id FROM clinics WHERE code = 'CLINIC01' ORDER BY id LIMIT 1
     ),
     actor_ref AS (
       SELECT id AS actor_user_id FROM users ORDER BY id LIMIT 1
     )
     INSERT INTO stock_ledger (
       org_id, clinic_id, item_master_id, movement_type, qty_delta, reason_code,
       movement_source, correlation_id, actor_user_id, allow_negative, metadata, occurred_at
     )
     SELECT
       (SELECT org_id FROM org_ref),
       (SELECT clinic_id FROM clinic_ref),
       im.id,
       'RECEIPT',
       i.quantity::numeric,
       'BASELINE_SEED',
       'SYSTEM',
       CONCAT('baseline-item-', i.id),
       (SELECT actor_user_id FROM actor_ref),
       FALSE,
       jsonb_build_object('seed', TRUE, 'legacy_item_id', i.id),
       NOW()
     FROM items i
     JOIN item_master im ON im.sku_code = i.sku_code
     CROSS JOIN actor_ref
     WHERE i.quantity > 0
       AND NOT EXISTS (
         SELECT 1
         FROM stock_ledger sl
         WHERE sl.correlation_id = CONCAT('baseline-item-', i.id)
       )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS transfer_requests (
      id BIGSERIAL PRIMARY KEY,
      org_id BIGINT REFERENCES organizations(id) ON DELETE SET NULL,
      from_clinic_id BIGINT NOT NULL REFERENCES clinics(id) ON DELETE RESTRICT,
      to_clinic_id BIGINT NOT NULL REFERENCES clinics(id) ON DELETE RESTRICT,
      status VARCHAR(30) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'PARTIALLY_PICKED', 'IN_TRANSIT', 'PARTIALLY_RECEIVED', 'RECEIVED', 'PARTIALLY_CANCELLED', 'CANCELLED')),
      needed_by TIMESTAMPTZ,
      requested_by BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
      approved_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
      approved_at TIMESTAMPTZ,
      notes TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (from_clinic_id <> to_clinic_id)
    )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS transfer_request_lines (
      id BIGSERIAL PRIMARY KEY,
      transfer_request_id BIGINT NOT NULL REFERENCES transfer_requests(id) ON DELETE CASCADE,
      item_id BIGINT NOT NULL REFERENCES items(id) ON DELETE RESTRICT,
      requested_qty NUMERIC(14,3) NOT NULL CHECK (requested_qty > 0),
      approved_qty NUMERIC(14,3) NOT NULL DEFAULT 0 CHECK (approved_qty >= 0),
      picked_qty NUMERIC(14,3) NOT NULL DEFAULT 0 CHECK (picked_qty >= 0),
      received_qty NUMERIC(14,3) NOT NULL DEFAULT 0 CHECK (received_qty >= 0),
      cancelled_qty NUMERIC(14,3) NOT NULL DEFAULT 0 CHECK (cancelled_qty >= 0),
      line_status VARCHAR(30) NOT NULL DEFAULT 'PENDING'
        CHECK (line_status IN ('PENDING', 'APPROVED', 'PARTIALLY_PICKED', 'IN_TRANSIT', 'PARTIALLY_RECEIVED', 'RECEIVED', 'PARTIALLY_CANCELLED', 'CANCELLED', 'REJECTED')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (transfer_request_id, item_id),
      CHECK (approved_qty <= requested_qty),
      CHECK (picked_qty <= approved_qty),
      CHECK (received_qty <= picked_qty),
      CHECK (cancelled_qty <= approved_qty)
    )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS transfer_action_idempotency (
      id BIGSERIAL PRIMARY KEY,
      transfer_request_id BIGINT REFERENCES transfer_requests(id) ON DELETE CASCADE,
      action_name VARCHAR(50) NOT NULL,
      actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
      idempotency_key VARCHAR(120) NOT NULL,
      response_json JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (actor_user_id, action_name, idempotency_key)
    )`
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_transfer_action_idempotency_request
     ON transfer_action_idempotency(transfer_request_id, action_name)`
  );
  transferSchemaReady = true;
};

const ensureLotExpirySchema = async () => {
  if (lotExpirySchemaReady) return;
  try {
    await pool.query(`ALTER TABLE items ADD COLUMN IF NOT EXISTS expiry_tracked BOOLEAN NOT NULL DEFAULT FALSE`);
  } catch (err) {
    console.warn('Skipping items.expiry_tracked alter:', err && err.message ? err.message : err);
  }
  try {
    const check = await pool.query(
      `SELECT 1
       FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name = 'items'
         AND column_name = 'expiry_tracked'
       LIMIT 1`
    );
    expiryTrackedColumnAvailable = check.rows.length > 0;
  } catch (_) {
    expiryTrackedColumnAvailable = false;
  }
  await pool.query(
    `CREATE TABLE IF NOT EXISTS inventory_lot_balances (
      id BIGSERIAL PRIMARY KEY,
      clinic_id BIGINT,
      location_code VARCHAR(60) NOT NULL DEFAULT 'MAIN',
      item_id BIGINT NOT NULL REFERENCES items(id) ON DELETE RESTRICT,
      lot_code VARCHAR(120) NOT NULL,
      expiry_date DATE NOT NULL,
      quantity NUMERIC(14,3) NOT NULL DEFAULT 0 CHECK (quantity >= 0),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (clinic_id, location_code, item_id, lot_code)
    )`
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_inventory_lot_balances_item_expiry
     ON inventory_lot_balances(item_id, expiry_date, quantity DESC)`
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_inventory_lot_balances_clinic_expiry
     ON inventory_lot_balances(clinic_id, expiry_date, quantity DESC)`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS clinic_alert_digests (
      id BIGSERIAL PRIMARY KEY,
      clinic_id BIGINT,
      digest_date DATE NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (clinic_id, digest_date)
    )`
  );
  await pool.query(
    `CREATE OR REPLACE VIEW inventory_expiry_dashboard AS
     SELECT
       ilb.clinic_id,
       ilb.item_id,
       i.sku_code,
       i.name,
       ilb.lot_code,
       ilb.expiry_date,
       ilb.quantity::numeric(14,3) AS on_hand_qty,
       CASE
         WHEN ilb.expiry_date < CURRENT_DATE THEN 'EXPIRED'
         WHEN ilb.expiry_date <= CURRENT_DATE + INTERVAL '30 days' THEN 'EXP_30'
         WHEN ilb.expiry_date <= CURRENT_DATE + INTERVAL '60 days' THEN 'EXP_60'
         WHEN ilb.expiry_date <= CURRENT_DATE + INTERVAL '90 days' THEN 'EXP_90'
         ELSE 'HEALTHY'
       END AS expiry_bucket
     FROM inventory_lot_balances ilb
     JOIN items i ON i.id = ilb.item_id
     WHERE ilb.quantity > 0`
  );
  lotExpirySchemaReady = true;
};

const parseExpiryDate = (value) => {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
};

const clinicIdFromRequest = (req) => {
  const headerClinic = Number(req.headers['x-clinic-id'] || 0);
  if (Number.isInteger(headerClinic) && headerClinic > 0) return headerClinic;
  if (req.user && Number.isInteger(Number(req.user.home_clinic_id)) && Number(req.user.home_clinic_id) > 0) {
    return Number(req.user.home_clinic_id);
  }
  return null;
};

const upsertLotBalance = async ({ client, clinicId, locationCode, itemId, lotCode, expiryDate, qtyDelta }) => {
  if (!lotCode || !expiryDate || Number(qtyDelta) === 0) return null;
  await client.query(
    `INSERT INTO inventory_lot_balances
     (clinic_id, location_code, item_id, lot_code, expiry_date, quantity)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (clinic_id, location_code, item_id, lot_code)
     DO UPDATE SET
       quantity = inventory_lot_balances.quantity + EXCLUDED.quantity,
       expiry_date = EXCLUDED.expiry_date,
       updated_at = NOW()`,
    [clinicId, locationCode || 'MAIN', itemId, lotCode, expiryDate, qtyDelta]
  );
  await client.query(
    `UPDATE inventory_lot_balances
     SET quantity = 0, updated_at = NOW()
     WHERE clinic_id IS NOT DISTINCT FROM $1
       AND location_code = $2
       AND item_id = $3
       AND lot_code = $4
       AND quantity < 0`,
    [clinicId, locationCode || 'MAIN', itemId, lotCode]
  );
  const result = await client.query(
    `SELECT *
     FROM inventory_lot_balances
     WHERE clinic_id IS NOT DISTINCT FROM $1
       AND location_code = $2
       AND item_id = $3
       AND lot_code = $4`,
    [clinicId, locationCode || 'MAIN', itemId, lotCode]
  );
  return result.rows[0] || null;
};

const selectFefoLots = async ({ client, clinicId, locationCode, itemId, quantity, allowExpired = false }) => {
  const rows = await client.query(
    `SELECT id, lot_code, expiry_date, quantity
     FROM inventory_lot_balances
     WHERE clinic_id IS NOT DISTINCT FROM $1
       AND location_code = $2
       AND item_id = $3
       AND quantity > 0
       AND ($4::boolean OR expiry_date >= CURRENT_DATE)
     ORDER BY expiry_date ASC, lot_code ASC
     FOR UPDATE`,
    [clinicId, locationCode || 'MAIN', itemId, allowExpired]
  );
  let remaining = Number(quantity);
  const picks = [];
  for (const lot of rows.rows) {
    if (remaining <= 0) break;
    const available = parseNumeric(lot.quantity);
    if (available <= 0) continue;
    const take = Math.min(available, remaining);
    picks.push({
      lot_balance_id: lot.id,
      lot_code: lot.lot_code,
      expiry_date: lot.expiry_date,
      quantity: take,
    });
    remaining -= take;
  }
  return { picks, remaining };
};

const buildClinicDigestPayload = async ({ client, clinicId }) => {
  const windowCounts = await client.query(
    `SELECT
       COALESCE(SUM(CASE WHEN expiry_date <= CURRENT_DATE + INTERVAL '30 days' AND expiry_date >= CURRENT_DATE THEN quantity ELSE 0 END), 0)::numeric AS exp_30_qty,
       COALESCE(SUM(CASE WHEN expiry_date <= CURRENT_DATE + INTERVAL '60 days' AND expiry_date > CURRENT_DATE + INTERVAL '30 days' THEN quantity ELSE 0 END), 0)::numeric AS exp_60_qty,
       COALESCE(SUM(CASE WHEN expiry_date <= CURRENT_DATE + INTERVAL '90 days' AND expiry_date > CURRENT_DATE + INTERVAL '60 days' THEN quantity ELSE 0 END), 0)::numeric AS exp_90_qty,
       COALESCE(SUM(CASE WHEN expiry_date < CURRENT_DATE THEN quantity ELSE 0 END), 0)::numeric AS expired_qty
     FROM inventory_lot_balances
     WHERE clinic_id IS NOT DISTINCT FROM $1
       AND quantity > 0`,
    [clinicId]
  );
  const slowMoving = await client.query(
    `SELECT ilb.item_id, i.sku_code, i.name, ilb.lot_code, ilb.expiry_date, ilb.quantity,
            COALESCE(usage.issue_qty_30d, 0)::numeric AS issue_qty_30d
     FROM inventory_lot_balances ilb
     JOIN items i ON i.id = ilb.item_id
     LEFT JOIN (
       SELECT item_id, COALESCE(SUM(quantity), 0)::numeric AS issue_qty_30d
       FROM inventory_transactions
       WHERE type = 'ISSUE'
         AND created_at >= NOW() - INTERVAL '30 days'
       GROUP BY item_id
     ) usage ON usage.item_id = ilb.item_id
     WHERE ilb.clinic_id IS NOT DISTINCT FROM $1
       AND ilb.quantity > 0
       AND ilb.expiry_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '45 days'
       AND COALESCE(usage.issue_qty_30d, 0) <= 5
     ORDER BY ilb.expiry_date ASC, ilb.quantity DESC
     LIMIT 20`,
    [clinicId]
  );
  return {
    generated_at: new Date().toISOString(),
    clinic_id: clinicId,
    expiring_30_qty: parseNumeric(windowCounts.rows[0].exp_30_qty),
    expiring_60_qty: parseNumeric(windowCounts.rows[0].exp_60_qty),
    expiring_90_qty: parseNumeric(windowCounts.rows[0].exp_90_qty),
    expired_on_hand_qty: parseNumeric(windowCounts.rows[0].expired_qty),
    slow_moving_near_expiry: slowMoving.rows,
  };
};

const processDailyExpiryDigests = async () => {
  await ensureLotExpirySchema();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const clinics = await client.query(
      `SELECT DISTINCT clinic_id AS id
       FROM inventory_lot_balances
       ORDER BY clinic_id NULLS LAST`
    );
    let processed = 0;
    for (const clinic of clinics.rows) {
      const payload = await buildClinicDigestPayload({ client, clinicId: clinic.id });
      const inserted = await client.query(
        `INSERT INTO clinic_alert_digests (clinic_id, digest_date, payload)
         VALUES ($1, CURRENT_DATE, $2)
         ON CONFLICT (clinic_id, digest_date) DO NOTHING
         RETURNING id`,
        [clinic.id, payload]
      );
      if (inserted.rows.length > 0) processed += 1;
    }
    await client.query('COMMIT');
    return processed;
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw err;
  } finally {
    client.release();
  }
};

const ensureReorderSchema = async () => {
  if (reorderSchemaReady) return;
  await ensureLotExpirySchema();

  await pool.query(
    `CREATE TABLE IF NOT EXISTS organizations (
      id BIGSERIAL PRIMARY KEY,
      code VARCHAR(50) NOT NULL UNIQUE,
      name VARCHAR(255) NOT NULL,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS clinics (
      id BIGSERIAL PRIMARY KEY,
      org_id BIGINT REFERENCES organizations(id) ON DELETE RESTRICT,
      code VARCHAR(50) NOT NULL,
      name VARCHAR(255) NOT NULL,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (org_id, code)
    )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS item_master (
      id BIGSERIAL PRIMARY KEY,
      sku_code VARCHAR(120) NOT NULL UNIQUE,
      name VARCHAR(255) NOT NULL,
      active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS stock_ledger (
      id BIGSERIAL PRIMARY KEY,
      org_id BIGINT REFERENCES organizations(id) ON DELETE RESTRICT,
      clinic_id BIGINT REFERENCES clinics(id) ON DELETE RESTRICT,
      location_id BIGINT,
      item_master_id BIGINT NOT NULL REFERENCES item_master(id) ON DELETE RESTRICT,
      lot_id BIGINT,
      encounter_id BIGINT,
      movement_type VARCHAR(40) NOT NULL CHECK (
        movement_type IN ('RECEIPT', 'ISSUE', 'RETURN', 'ADJUST_IN', 'ADJUST_OUT', 'TRANSFER_IN', 'TRANSFER_OUT')
      ),
      qty_delta NUMERIC(14,3) NOT NULL CHECK (qty_delta <> 0),
      reason_code VARCHAR(60) NOT NULL,
      movement_source VARCHAR(60) NOT NULL,
      correlation_id VARCHAR(120) NOT NULL UNIQUE,
      idempotency_key VARCHAR(120),
      actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
      allow_negative BOOLEAN NOT NULL DEFAULT FALSE,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS clinic_item_settings (
      id BIGSERIAL PRIMARY KEY,
      clinic_id BIGINT REFERENCES clinics(id) ON DELETE RESTRICT,
      item_master_id BIGINT NOT NULL REFERENCES item_master(id) ON DELETE RESTRICT,
      location_code VARCHAR(60) NOT NULL DEFAULT 'MAIN',
      par_level NUMERIC(14,3) NOT NULL DEFAULT 0,
      on_order_qty NUMERIC(14,3) NOT NULL DEFAULT 0,
      lead_time_days INTEGER NOT NULL DEFAULT 14,
      expiry_risk_weight NUMERIC(8,3) NOT NULL DEFAULT 1,
      is_stocked BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (clinic_id, item_master_id, location_code)
    )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS procurement_orders (
      id BIGSERIAL PRIMARY KEY,
      clinic_id BIGINT REFERENCES clinics(id) ON DELETE SET NULL,
      item_master_id BIGINT NOT NULL REFERENCES item_master(id) ON DELETE RESTRICT,
      location_code VARCHAR(60) NOT NULL DEFAULT 'MAIN',
      qty_outstanding NUMERIC(14,3) NOT NULL CHECK (qty_outstanding >= 0),
      expected_date DATE,
      status VARCHAR(30) NOT NULL DEFAULT 'OPEN'
        CHECK (status IN ('OPEN', 'PARTIAL', 'RECEIVED', 'CANCELLED')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await pool.query(
    `INSERT INTO organizations (code, name)
     VALUES ('DEFAULT_ORG', 'Default Organization')
     ON CONFLICT (code) DO NOTHING`
  );
  await pool.query(
    `WITH org_ref AS (
       SELECT id AS org_id FROM organizations WHERE code = 'DEFAULT_ORG' ORDER BY id LIMIT 1
     )
     INSERT INTO clinics (org_id, code, name)
     SELECT org_ref.org_id, 'CLINIC01', 'Clinic 01'
     FROM org_ref
     WHERE NOT EXISTS (
       SELECT 1 FROM clinics WHERE clinics.org_id = org_ref.org_id AND clinics.code = 'CLINIC01'
     )`
  );
  await pool.query(
    `INSERT INTO item_master (sku_code, name, active, created_at, updated_at)
     SELECT i.sku_code, i.name, TRUE, NOW(), NOW()
     FROM items i
     ON CONFLICT (sku_code) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()`
  );
  await pool.query(
    `WITH clinic_ref AS (
       SELECT id AS clinic_id
       FROM clinics
       WHERE code = 'CLINIC01'
       ORDER BY id
       LIMIT 1
     )
     INSERT INTO clinic_item_settings (clinic_id, item_master_id, location_code, par_level, on_order_qty, lead_time_days, expiry_risk_weight, is_stocked)
     SELECT clinic_ref.clinic_id, im.id, 'MAIN', 0, 0, 14, 1, TRUE
     FROM item_master im
     CROSS JOIN clinic_ref
     ON CONFLICT (clinic_id, item_master_id, location_code) DO NOTHING`
  );
  await pool.query(
    `CREATE OR REPLACE VIEW clinic_item_on_hand_v AS
     SELECT
       sl.clinic_id,
       sl.item_master_id,
       COALESCE(SUM(sl.qty_delta), 0)::numeric(14,3) AS on_hand_qty
     FROM stock_ledger sl
     GROUP BY sl.clinic_id, sl.item_master_id`
  );
  await pool.query(
    `CREATE OR REPLACE VIEW clinic_item_usage_velocity_v AS
     SELECT
       sl.clinic_id,
       sl.item_master_id,
       COALESCE(SUM(CASE WHEN sl.occurred_at >= NOW() - INTERVAL '30 days' AND sl.qty_delta < 0 THEN ABS(sl.qty_delta) ELSE 0 END), 0)::numeric(14,3) AS usage_30d,
       COALESCE(SUM(CASE WHEN sl.occurred_at >= NOW() - INTERVAL '60 days' AND sl.qty_delta < 0 THEN ABS(sl.qty_delta) ELSE 0 END), 0)::numeric(14,3) AS usage_60d,
       COALESCE(SUM(CASE WHEN sl.occurred_at >= NOW() - INTERVAL '90 days' AND sl.qty_delta < 0 THEN ABS(sl.qty_delta) ELSE 0 END), 0)::numeric(14,3) AS usage_90d
     FROM stock_ledger sl
     GROUP BY sl.clinic_id, sl.item_master_id`
  );
  await pool.query(
    `CREATE OR REPLACE VIEW clinic_item_expiry_risk_v AS
     SELECT
       ilb.clinic_id,
       i.sku_code,
       COALESCE(SUM(CASE WHEN ilb.expiry_date <= CURRENT_DATE + INTERVAL '30 days' THEN ilb.quantity ELSE 0 END), 0)::numeric(14,3) AS qty_exp_30d,
       COALESCE(SUM(ilb.quantity), 0)::numeric(14,3) AS total_qty,
       CASE
         WHEN COALESCE(SUM(ilb.quantity), 0) = 0 THEN 0::numeric
         ELSE COALESCE(SUM(CASE WHEN ilb.expiry_date <= CURRENT_DATE + INTERVAL '30 days' THEN ilb.quantity ELSE 0 END), 0)::numeric / NULLIF(COALESCE(SUM(ilb.quantity), 0)::numeric, 0)
       END AS expiry_risk_ratio
     FROM inventory_lot_balances ilb
     JOIN items i ON i.id = ilb.item_id
     WHERE ilb.quantity > 0
     GROUP BY ilb.clinic_id, i.sku_code`
  );
  await pool.query(
    `DROP MATERIALIZED VIEW IF EXISTS reorder_recommendation_basis_mv`
  );
  await pool.query(
    `CREATE MATERIALIZED VIEW reorder_recommendation_basis_mv AS
     WITH open_orders AS (
       SELECT clinic_id, item_master_id, location_code, COALESCE(SUM(qty_outstanding), 0)::numeric(14,3) AS on_order_qty
       FROM procurement_orders
       WHERE status IN ('OPEN', 'PARTIAL')
       GROUP BY clinic_id, item_master_id, location_code
     )
     SELECT
       cis.clinic_id,
       c.name AS clinic_name,
       cis.location_code,
       cis.item_master_id,
       im.sku_code,
       im.name AS item_name,
       cis.par_level::numeric(14,3) AS par_level,
       COALESCE(oh.on_hand_qty, 0)::numeric(14,3) AS on_hand_qty,
       (COALESCE(cis.on_order_qty, 0) + COALESCE(oo.on_order_qty, 0))::numeric(14,3) AS on_order_qty,
       COALESCE(uv.usage_30d, 0)::numeric(14,3) AS usage_30d,
       COALESCE(uv.usage_60d, 0)::numeric(14,3) AS usage_60d,
       COALESCE(uv.usage_90d, 0)::numeric(14,3) AS usage_90d,
       GREATEST(1, COALESCE(cis.lead_time_days, 14))::int AS lead_time_days,
       COALESCE(er.expiry_risk_ratio, 0)::numeric(14,4) AS expiry_risk_ratio,
       GREATEST(cis.par_level - (COALESCE(oh.on_hand_qty, 0) + COALESCE(cis.on_order_qty, 0) + COALESCE(oo.on_order_qty, 0)), 0)::numeric(14,3) AS shortage_vs_par,
       GREATEST((COALESCE(oh.on_hand_qty, 0) + COALESCE(cis.on_order_qty, 0) + COALESCE(oo.on_order_qty, 0)) - cis.par_level, 0)::numeric(14,3) AS excess_vs_par
     FROM clinic_item_settings cis
     JOIN item_master im ON im.id = cis.item_master_id
     LEFT JOIN clinics c ON c.id = cis.clinic_id
     LEFT JOIN clinic_item_on_hand_v oh ON oh.clinic_id = cis.clinic_id AND oh.item_master_id = cis.item_master_id
     LEFT JOIN clinic_item_usage_velocity_v uv ON uv.clinic_id = cis.clinic_id AND uv.item_master_id = cis.item_master_id
     LEFT JOIN clinic_item_expiry_risk_v er ON er.clinic_id = cis.clinic_id AND er.sku_code = im.sku_code
     LEFT JOIN open_orders oo ON oo.clinic_id = cis.clinic_id AND oo.item_master_id = cis.item_master_id AND oo.location_code = cis.location_code
     WHERE cis.is_stocked = TRUE`
  );
  await pool.query(
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_reorder_basis_unique
     ON reorder_recommendation_basis_mv(clinic_id, item_master_id, location_code)`
  );

  reorderSchemaReady = true;
};

const refreshReorderBasis = async () => {
  await ensureReorderSchema();
  await pool.query('REFRESH MATERIALIZED VIEW reorder_recommendation_basis_mv');
};

const computeReorderRecommendations = async ({ clinicId = null, locationCode = null, includeZeroShortage = false }) => {
  await refreshReorderBasis();
  const where = [];
  const params = [];
  let idx = 1;
  if (clinicId) {
    where.push(`clinic_id = $${idx++}`);
    params.push(clinicId);
  }
  if (locationCode) {
    where.push(`location_code = $${idx++}`);
    params.push(String(locationCode));
  }
  if (!includeZeroShortage) {
    where.push('shortage_vs_par > 0');
  }
  const whereSql = where.length > 0 ? `WHERE ${where.join(' AND ')}` : '';
  const rows = await pool.query(
    `SELECT *
     FROM reorder_recommendation_basis_mv
     ${whereSql}
     ORDER BY shortage_vs_par DESC, clinic_id ASC, sku_code ASC`,
    params
  );

  const allRows = await pool.query('SELECT * FROM reorder_recommendation_basis_mv');
  const byItem = new Map();
  for (const row of allRows.rows) {
    const key = `${row.item_master_id}|${row.location_code}`;
    if (!byItem.has(key)) byItem.set(key, []);
    byItem.get(key).push(row);
  }

  const recommendations = rows.rows.map((row) => {
    const dailyUsageWeighted = (
      (parseNumeric(row.usage_30d) / 30) * 0.6
      + (parseNumeric(row.usage_60d) / 60) * 0.3
      + (parseNumeric(row.usage_90d) / 90) * 0.1
    );
    const leadTimeDemand = Math.max(dailyUsageWeighted * Number(row.lead_time_days || 14), 0);
    const donors = (byItem.get(`${row.item_master_id}|${row.location_code}`) || [])
      .filter((candidate) => Number(candidate.clinic_id) !== Number(row.clinic_id))
      .filter((candidate) => parseNumeric(candidate.excess_vs_par) > 0)
      .sort((a, b) => parseNumeric(b.excess_vs_par) - parseNumeric(a.excess_vs_par))
      .map((candidate) => ({
        clinic_id: candidate.clinic_id,
        clinic_name: candidate.clinic_name,
        available_excess_qty: parseNumeric(candidate.excess_vs_par),
      }));

    const networkExcess = donors.reduce((acc, donor) => acc + parseNumeric(donor.available_excess_qty), 0);
    const shortage = parseNumeric(row.shortage_vs_par);
    const transferQty = Math.min(shortage, networkExcess);
    const networkCoversLeadTime = networkExcess >= leadTimeDemand;
    const purchaseQty = networkCoversLeadTime ? 0 : Math.max(shortage - transferQty, 0);
    const recommendationType = transferQty > 0 && purchaseQty <= 0
      ? 'TRANSFER_ONLY'
      : (transferQty > 0 && purchaseQty > 0 ? 'TRANSFER_PLUS_PURCHASE' : (purchaseQty > 0 ? 'PURCHASE' : 'NONE'));

    return {
      clinic_id: row.clinic_id,
      clinic_name: row.clinic_name,
      location_code: row.location_code,
      item_master_id: row.item_master_id,
      sku_code: row.sku_code,
      item_name: row.item_name,
      par_level: parseNumeric(row.par_level),
      on_hand_qty: parseNumeric(row.on_hand_qty),
      on_order_qty: parseNumeric(row.on_order_qty),
      shortage_vs_par: shortage,
      lead_time_days: Number(row.lead_time_days || 14),
      lead_time_demand: Number(leadTimeDemand.toFixed(3)),
      usage_30d: parseNumeric(row.usage_30d),
      usage_60d: parseNumeric(row.usage_60d),
      usage_90d: parseNumeric(row.usage_90d),
      expiry_risk_ratio: parseNumeric(row.expiry_risk_ratio),
      network_excess_qty: Number(networkExcess.toFixed(3)),
      suggested_transfer_qty: Number(transferQty.toFixed(3)),
      suggested_purchase_qty: Number(purchaseQty.toFixed(3)),
      network_covers_lead_time: networkCoversLeadTime,
      recommendation_type: recommendationType,
      transfer_donors: donors,
      rationale: networkCoversLeadTime
        ? 'Network excess can cover projected lead-time demand; purchase blocked.'
        : (transferQty > 0
          ? 'Transfer suggested first from excess clinics; purchase only for residual shortage.'
          : 'No network excess available; purchase required to recover shortage.'),
    };
  });

  return {
    generated_at: new Date().toISOString(),
    filters: { clinic_id: clinicId, location_code: locationCode || null, include_zero_shortage: includeZeroShortage },
    rows: recommendations,
  };
};

const ensureSyncSchema = async () => {
  if (syncSchemaReady) return;
  await pool.query(
    `CREATE TABLE IF NOT EXISTS sync_inbox (
      id BIGSERIAL PRIMARY KEY,
      device_id VARCHAR(120),
      user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      action_type VARCHAR(60) NOT NULL,
      path VARCHAR(255) NOT NULL,
      method VARCHAR(10) NOT NULL,
      idempotency_key VARCHAR(120) NOT NULL,
      action_fingerprint VARCHAR(120),
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      status VARCHAR(30) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'APPLIED', 'REJECTED', 'CONFLICT', 'DUPLICATE')),
      conflict_code VARCHAR(60),
      result_json JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      processed_at TIMESTAMPTZ,
      UNIQUE (user_id, idempotency_key)
    )`
  );
  await pool.query(
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_inbox_fingerprint_issue_receipt
     ON sync_inbox(user_id, action_type, action_fingerprint)
     WHERE action_type IN ('STOCK_IN', 'ISSUE_ITEM', 'ISSUE_SCAN', 'RETURN_ITEM', 'RETURN_SCAN')
       AND action_fingerprint IS NOT NULL`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS sync_outbox (
      id BIGSERIAL PRIMARY KEY,
      user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
      target_device_id VARCHAR(120),
      event_type VARCHAR(80) NOT NULL,
      event_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      acked_at TIMESTAMPTZ
    )`
  );
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_sync_outbox_user_id ON sync_outbox(user_id, id)`);
  syncSchemaReady = true;
};

const ensureReleaseControlSchema = async () => {
  if (releaseControlSchemaReady) return;
  await pool.query(
    `CREATE TABLE IF NOT EXISTS schema_versions (
      id BIGSERIAL PRIMARY KEY,
      version_tag VARCHAR(120) NOT NULL,
      checksum VARCHAR(128) NOT NULL,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      applied_by VARCHAR(120),
      notes TEXT,
      UNIQUE (version_tag, checksum)
    )`
  );
  await pool.query(
    `CREATE TABLE IF NOT EXISTS feature_flags (
      id BIGSERIAL PRIMARY KEY,
      clinic_id BIGINT,
      flag_key VARCHAR(100) NOT NULL,
      enabled BOOLEAN NOT NULL DEFAULT FALSE,
      rollout_percentage INTEGER NOT NULL DEFAULT 100 CHECK (rollout_percentage >= 0 AND rollout_percentage <= 100),
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
      updated_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (clinic_id, flag_key)
    )`
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_feature_flags_key ON feature_flags(flag_key, clinic_id)`
  );
  releaseControlSchemaReady = true;
};

const getUserClinicId = (req) => {
  const headerClinic = Number(req.headers['x-clinic-id'] || 0);
  if (headerClinic > 0) return headerClinic;
  const home = Number(req.user?.home_clinic_id || 0);
  return home > 0 ? home : null;
};

const evaluateFeatureFlags = async ({ userId = null, clinicId = null }) => {
  await ensureReleaseControlSchema();
  const result = await pool.query(
    `SELECT clinic_id, flag_key, enabled, rollout_percentage, payload
     FROM feature_flags
     WHERE clinic_id IS NULL
        OR clinic_id = $1`,
    [clinicId]
  );
  const userHash = userId
    ? Number.parseInt(crypto.createHash('sha256').update(String(userId)).digest('hex').slice(0, 8), 16)
    : 0;
  const flags = {};
  for (const row of result.rows) {
    if (!row.enabled) {
      flags[row.flag_key] = { enabled: false, payload: row.payload || {}, source: row.clinic_id ? 'clinic' : 'global' };
      continue;
    }
    const bucket = row.rollout_percentage >= 100 ? 0 : (userHash % 100);
    const inRollout = row.rollout_percentage >= 100 || bucket < Number(row.rollout_percentage);
    if (!Object.prototype.hasOwnProperty.call(flags, row.flag_key) || row.clinic_id) {
      flags[row.flag_key] = {
        enabled: inRollout,
        payload: row.payload || {},
        rollout_percentage: Number(row.rollout_percentage),
        source: row.clinic_id ? 'clinic' : 'global',
      };
    }
  }
  return flags;
};

const buildSyncActionFingerprint = (action) => {
  const body = action && action.body && typeof action.body === 'object' ? action.body : {};
  const keyPayload = {
    action_type: String(action.action_type || ''),
    path: String(action.path || ''),
    method: String(action.method || ''),
    item_id: body.item_id || null,
    item_barcode: body.item_barcode || null,
    encounter_id: body.encounter_id || null,
    encounter_code: body.encounter_code || null,
    quantity: body.quantity || null,
    lot_code: body.lot_code || null,
    expiry_date: body.expiry_date || null,
  };
  const raw = JSON.stringify(keyPayload);
  return crypto.createHash('sha256').update(raw).digest('hex');
};

const publishSyncEvent = async ({ eventType, payload, actorUserId = null }) => {
  try {
    await ensureSyncSchema();
    await pool.query(
      `INSERT INTO sync_outbox (user_id, target_device_id, event_type, event_payload)
       VALUES ($1, NULL, $2, $3)`,
      [actorUserId || null, eventType, payload || {}]
    );
  } catch (_) {
    // best effort, never break business path
  }
};

const isBlockedDirectOnHandEdit = (action) => {
  const path = String(action.path || '').toLowerCase();
  const method = String(action.method || '').toUpperCase();
  if (path.startsWith('/sync/')) return true;
  if (method === 'PATCH' || method === 'PUT') {
    if (path.startsWith('/items') || path.includes('/stock-levels')) return true;
  }
  if (String(action.action_type || '').toUpperCase().includes('ON_HAND_EDIT')) return true;
  return false;
};

const executeSyncActionViaApi = async ({ req, action }) => {
  const targetUrl = `${req.protocol}://${req.get('host')}${action.path}`;
  const response = await fetch(targetUrl, {
    method: action.method,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${req.token}`,
      'Idempotency-Key': action.idempotency_key,
      'X-Sync-Source': 'offline-client',
    },
    body: JSON.stringify(action.body || {}),
  });
  const text = await response.text();
  let json = {};
  try {
    json = JSON.parse(text);
  } catch (_) {
    json = {};
  }
  return { status: response.status, ok: response.ok, json, text };
};

const getUserClinicScope = async ({ client, userId }) => {
  const result = await client.query(
    `SELECT id, role, org_id, home_clinic_id
     FROM users
     WHERE id = $1`,
    [userId]
  );
  if (result.rows.length === 0) throw new Error('User not found');
  return result.rows[0];
};

const ensureItemMasterForItem = async ({ client, itemId }) => {
  const item = await client.query(
    `SELECT id, sku_code, name
     FROM items
     WHERE id = $1`,
    [itemId]
  );
  if (item.rows.length === 0) return null;
  const row = item.rows[0];
  await client.query(
    `INSERT INTO item_master (sku_code, name, active, created_at, updated_at)
     VALUES ($1, $2, TRUE, NOW(), NOW())
     ON CONFLICT (sku_code) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()`,
    [row.sku_code, row.name]
  );
  const mapped = await client.query(
    `SELECT id
     FROM item_master
     WHERE sku_code = $1`,
    [row.sku_code]
  );
  return mapped.rows.length > 0 ? mapped.rows[0].id : null;
};

const findTransferActionReplay = async ({ client, transferRequestId, actorUserId, actionName, idempotencyKey }) => {
  const result = await client.query(
    `SELECT response_json
     FROM transfer_action_idempotency
     WHERE transfer_request_id IS NOT DISTINCT FROM $1
       AND actor_user_id = $2
       AND action_name = $3
       AND idempotency_key = $4
     LIMIT 1`,
    [transferRequestId || null, actorUserId, actionName, idempotencyKey]
  );
  return result.rows.length > 0 ? result.rows[0].response_json : null;
};

const saveTransferActionReplay = async ({
  client,
  transferRequestId,
  actorUserId,
  actionName,
  idempotencyKey,
  responseJson,
}) => {
  await client.query(
    `INSERT INTO transfer_action_idempotency
     (transfer_request_id, action_name, actor_user_id, idempotency_key, response_json)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (actor_user_id, action_name, idempotency_key)
     DO UPDATE SET response_json = EXCLUDED.response_json`,
    [transferRequestId || null, actionName, actorUserId, idempotencyKey, responseJson]
  );
};

const loadTransferWithLines = async ({ client, transferId }) => {
  const header = await client.query(
    `SELECT tr.*,
            fc.name AS from_clinic_name,
            tc.name AS to_clinic_name,
            requester.name AS requested_by_name,
            approver.name AS approved_by_name
     FROM transfer_requests tr
     JOIN clinics fc ON fc.id = tr.from_clinic_id
     JOIN clinics tc ON tc.id = tr.to_clinic_id
     LEFT JOIN users requester ON requester.id = tr.requested_by
     LEFT JOIN users approver ON approver.id = tr.approved_by
     WHERE tr.id = $1`,
    [transferId]
  );
  if (header.rows.length === 0) return null;
  const lines = await client.query(
    `SELECT l.*, i.sku_code, i.name
     FROM transfer_request_lines l
     JOIN items i ON i.id = l.item_id
     WHERE l.transfer_request_id = $1
     ORDER BY l.id ASC`,
    [transferId]
  );
  return { transfer: header.rows[0], lines: lines.rows };
};

const enforceTransferScope = ({ req, scope, transfer }) => {
  if (req.user.role === 'ADMIN') return true;
  const homeClinicId = Number(scope.home_clinic_id || 0);
  return Number(transfer.from_clinic_id) === homeClinicId || Number(transfer.to_clinic_id) === homeClinicId;
};

const recomputeTransferStatuses = async ({ client, transferId }) => {
  const linesResult = await client.query(
    `SELECT id, approved_qty, picked_qty, received_qty, cancelled_qty
     FROM transfer_request_lines
     WHERE transfer_request_id = $1
     ORDER BY id ASC`,
    [transferId]
  );
  for (const line of linesResult.rows) {
    const lineStatus = deriveTransferLineStatus(line);
    await client.query(
      `UPDATE transfer_request_lines
       SET line_status = $1, updated_at = NOW()
       WHERE id = $2`,
      [lineStatus, line.id]
    );
  }

  const refreshed = await client.query(
    `SELECT approved_qty, picked_qty, received_qty, cancelled_qty, line_status
     FROM transfer_request_lines
     WHERE transfer_request_id = $1
     ORDER BY id ASC`,
    [transferId]
  );
  const status = deriveTransferHeaderStatus(refreshed.rows);
  await client.query(
    `UPDATE transfer_requests
     SET status = $1, updated_at = NOW()
     WHERE id = $2`,
    [status, transferId]
  );
  return status;
};

const getClinicOnHandQty = async ({ client, clinicId, itemMasterId }) => {
  const result = await client.query(
    `SELECT COALESCE(SUM(qty_delta), 0)::numeric AS qty
     FROM stock_ledger
     WHERE clinic_id = $1
       AND item_master_id = $2`,
    [clinicId, itemMasterId]
  );
  return parseNumeric(result.rows[0] ? result.rows[0].qty : 0);
};

const issueSessionTokens = async ({ user }) => {
  await ensureRefreshTable();
  const passwordChangedAt = user.password_changed_at ? new Date(user.password_changed_at) : new Date();
  const refreshToken = crypto.randomBytes(48).toString('hex');
  const refreshHash = digestToken(refreshToken);
  const expiresAt = new Date(Date.now() + JWT_REFRESH_EXPIRES_DAYS * 24 * 60 * 60 * 1000);

  const session = await pool.query(
    `INSERT INTO refresh_sessions (user_id, token_hash, expires_at)
     VALUES ($1, $2, $3)
     RETURNING id`,
    [user.id, refreshHash, expiresAt]
  );

  const accessToken = jwt.sign(
    {
      id: user.id,
      student_card: user.student_card,
      name: user.name,
      role: user.role,
      pwd_ts: Math.floor(passwordChangedAt.getTime() / 1000),
      sid: Number(session.rows[0].id),
      must_reset_password: Boolean(user.must_reset_password) || isPasswordExpired(user.password_expires_at),
    },
    JWT_SECRET,
    { expiresIn: JWT_ACCESS_EXPIRES_IN }
  );

  return {
    accessToken,
    refreshToken,
    refreshExpiresAt: expiresAt.toISOString(),
    sessionId: Number(session.rows[0].id),
  };
};

const writeAuditLog = async (req, { action, severity = 'INFO', details = {}, actorUserId = null }) => {
  try {
    await pool.query(
      `INSERT INTO audit_logs
       (action, actor_user_id, actor_role, severity, request_path, request_method, ip_address, details)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        action,
        actorUserId || (req.user ? req.user.id : null),
        req.user ? req.user.role : null,
        severity,
        req.originalUrl || req.path || null,
        req.method || null,
        req.ip || null,
        details,
      ]
    );
  } catch (err) {
    console.error('AUDIT LOG WRITE FAILED:', err && err.message ? err.message : err);
  }
};

const getRoleCapabilities = (role) => ROLE_CAPABILITIES[role] || [];

const buildActionFingerprint = ({ userId, action, payload }) => {
  const normalizedPayload = payload && typeof payload === 'object' ? payload : {};
  const raw = JSON.stringify({ userId, action, payload: normalizedPayload });
  return crypto.createHash('sha256').update(raw).digest('hex');
};

const rejectDuplicateAction = (req, res, action, payload) => {
  const hasIdempotencyKey = String(req.headers['idempotency-key'] || req.headers['x-idempotency-key'] || '')
    .trim()
    .length > 0;
  if (hasIdempotencyKey) {
    return false;
  }
  const userId = req.user ? req.user.id : null;
  if (!userId) return false;

  const now = Date.now();
  for (const [key, expiresAt] of recentActionFingerprints.entries()) {
    if (expiresAt <= now) recentActionFingerprints.delete(key);
  }

  const fingerprint = buildActionFingerprint({ userId, action, payload });
  const existing = recentActionFingerprints.get(fingerprint);
  if (existing && existing > now) {
    writeAuditLog(req, {
      action: 'ACTION_DUPLICATE_REJECTED',
      severity: 'WARN',
      details: { dedup_action: action },
    }).catch(() => {});
    res.status(409).json({
      error: 'Duplicate action rejected',
      action,
      retry_after_ms: Math.max(1, existing - now),
    });
    return true;
  }

  recentActionFingerprints.set(fingerprint, now + ACTION_DEDUP_WINDOW_MS);
  return false;
};

const sanitizeTokenLikeValue = (value, fallback = null) => {
  if (!value) return fallback;
  const normalized = String(value).trim();
  if (!normalized) return fallback;
  return normalized.slice(0, 120);
};

const movementReasonFromRequest = (req, defaultReason) =>
  sanitizeTokenLikeValue(req.headers['x-reason-code'], defaultReason || 'UNSPECIFIED');

const movementSourceFromRequest = (req, defaultSource) =>
  sanitizeTokenLikeValue(req.headers['x-movement-source'], defaultSource || 'API');

const movementLocationFromRequest = (req, defaultLocation = 'MAIN') =>
  sanitizeTokenLikeValue(req.headers['x-location-code'], defaultLocation || 'MAIN');

const movementLotFromRequest = (req) =>
  sanitizeTokenLikeValue(req.headers['x-lot-code'], null);

const movementCorrelationFromRequest = (req) =>
  sanitizeTokenLikeValue(req.headers['x-correlation-id'], crypto.randomUUID());

const idempotencyKeyFromRequest = (req) =>
  sanitizeTokenLikeValue(req.headers['idempotency-key'] || req.headers['x-idempotency-key'], null);

const beginInventoryTxn = async (client) => {
  await client.query('BEGIN');
  await client.query('SET LOCAL TRANSACTION ISOLATION LEVEL SERIALIZABLE');
};

const findIdempotentMovement = async ({ client, actorUserId, idempotencyKey }) => {
  if (!movementSchemaExtended || !idempotencyKey) return null;
  const existing = await client.query(
    `SELECT *
     FROM inventory_transactions
     WHERE movement_actor_id = $1
       AND idempotency_key = $2
     ORDER BY id DESC
     LIMIT 1`,
    [actorUserId, idempotencyKey]
  );
  return existing.rows[0] || null;
};

const insertInventoryMovement = async ({
  client,
  itemId,
  encounterId = null,
  userId,
  actorUserId,
  type,
  quantity,
  reasonCode,
  movementSource,
  correlationId,
  idempotencyKey,
  locationCode,
  lotCode,
  allowNegative = false,
}) => {
  const legacyType = type === 'ADJUST_IN'
    ? 'STOCK_IN'
    : (type === 'ADJUST_OUT' ? 'ISSUE' : type);
  if (movementSchemaExtended) {
    const inserted = await client.query(
      `INSERT INTO inventory_transactions
       (item_id, encounter_id, user_id, movement_actor_id, type, quantity, reason_code, movement_source, correlation_id, idempotency_key, location_code, lot_code, allow_negative)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
       RETURNING *`,
      [
        itemId,
        encounterId,
        userId,
        actorUserId,
        type,
        quantity,
        reasonCode || 'UNSPECIFIED',
        movementSource || 'API',
        correlationId || crypto.randomUUID(),
        idempotencyKey || null,
        locationCode || 'MAIN',
        lotCode || null,
        allowNegative,
      ]
    );
    return inserted.rows[0];
  }

  let legacyEncounterId = encounterId;
  if (!legacyEncounterId) {
    const existingEncounter = await client.query(
      `SELECT id
       FROM encounters
       ORDER BY id ASC
       LIMIT 1`
    );
    if (existingEncounter.rows.length > 0) {
      legacyEncounterId = existingEncounter.rows[0].id;
    } else {
      const createdEncounter = await client.query(
        `INSERT INTO encounters (encounter_code, scheduled_time, status)
         VALUES ($1, NOW(), 'ACTIVE')
         RETURNING id`,
        [`SYS-STOCK-${Date.now()}`]
      );
      legacyEncounterId = createdEncounter.rows[0].id;
    }
  }

  const inserted = await client.query(
    `INSERT INTO inventory_transactions
     (item_id, encounter_id, user_id, type, quantity)
     VALUES ($1, $2, $3, $4, $5)
     RETURNING *`,
    [itemId, legacyEncounterId, userId || actorUserId, legacyType, quantity]
  );
  return inserted.rows[0];
};

const buildNextPasswordExpiry = () => {
  const value = new Date();
  value.setDate(value.getDate() + PASSWORD_MAX_AGE_DAYS);
  return value;
};

const isPasswordExpired = (passwordExpiresAt) =>
  Boolean(passwordExpiresAt) && new Date(passwordExpiresAt).getTime() <= Date.now();

const writeLedgerEvent = async (req, { eventType, entityType = null, entityId = null, payload = {} }) => {
  try {
    const prev = await pool.query('SELECT event_hash FROM event_ledger ORDER BY id DESC LIMIT 1');
    const previousHash = prev.rows.length > 0 ? prev.rows[0].event_hash : null;
    const raw = JSON.stringify({
      previousHash,
      eventType,
      entityType,
      entityId,
      actorUserId: req.user ? req.user.id : null,
      actorRole: req.user ? req.user.role : null,
      payload,
      ts: new Date().toISOString(),
    });
    const eventHash = crypto.createHash('sha256').update(raw).digest('hex');

    await pool.query(
      `INSERT INTO event_ledger
       (actor_user_id, actor_role, event_type, entity_type, entity_id, payload, previous_hash, event_hash)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        req.user ? req.user.id : null,
        req.user ? req.user.role : null,
        eventType,
        entityType,
        entityId ? String(entityId) : null,
        payload,
        previousHash,
        eventHash,
      ]
    );
    await publishSyncEvent({
      eventType,
      payload: {
        entity_type: entityType || null,
        entity_id: entityId ? String(entityId) : null,
        payload,
        actor_user_id: req.user ? req.user.id : null,
        created_at: new Date().toISOString(),
      },
      actorUserId: req.user ? req.user.id : null,
    });
  } catch (err) {
    console.error('LEDGER WRITE FAILED:', err && err.message ? err.message : err);
  }
};

const parseCsvLine = (line) => {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += ch;
    }
  }
  result.push(current.trim());
  return result;
};

const parseCsvRows = (csvText) => {
  const lines = csvText
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (lines.length < 2) {
    throw new Error('CSV must include header + at least one data row');
  }

  const headers = parseCsvLine(lines[0]).map((h) => h.toLowerCase());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const values = parseCsvLine(lines[i]);
    const row = {};
    headers.forEach((header, idx) => {
      row[header] = typeof values[idx] === 'string' ? values[idx] : '';
    });
    rows.push(row);
  }
  return rows;
};

const normalizeExcelCellValue = (value) => {
  if (value === null || typeof value === 'undefined') return '';
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) {
    return value.map((part) => normalizeExcelCellValue(part)).join('');
  }
  if (typeof value === 'object') {
    if (Object.prototype.hasOwnProperty.call(value, 'result')) {
      return normalizeExcelCellValue(value.result);
    }
    if (Object.prototype.hasOwnProperty.call(value, 'text')) {
      return normalizeExcelCellValue(value.text);
    }
    if (Array.isArray(value.richText)) {
      return value.richText.map((part) => part.text || '').join('');
    }
    if (Object.prototype.hasOwnProperty.call(value, 'hyperlink') && Object.prototype.hasOwnProperty.call(value, 'text')) {
      return normalizeExcelCellValue(value.text);
    }
    return String(value);
  }
  return value;
};

const parseWorksheetRows = (worksheet) => {
  const columnCount = worksheet.columnCount || worksheet.actualColumnCount || 0;
  if (columnCount === 0 || worksheet.actualRowCount < 2) {
    return [];
  }

  const headers = [];
  for (let col = 1; col <= columnCount; col += 1) {
    const headerValue = normalizeExcelCellValue(worksheet.getRow(1).getCell(col).value);
    headers.push(String(headerValue || '').trim().toLowerCase());
  }

  if (!headers.some(Boolean)) {
    return [];
  }

  const rows = [];
  worksheet.eachRow({ includeEmpty: false }, (row, rowNumber) => {
    if (rowNumber === 1) return;

    const out = {};
    let hasData = false;
    for (let col = 1; col <= columnCount; col += 1) {
      const header = headers[col - 1];
      if (!header) continue;
      const rawValue = normalizeExcelCellValue(row.getCell(col).value);
      const value = rawValue === null || typeof rawValue === 'undefined' ? '' : rawValue;
      if (value !== '') {
        hasData = true;
      }
      out[header] = value;
    }

    if (hasData) {
      rows.push(out);
    }
  });

  return rows;
};

const lowerKeyObject = (obj) => {
  const out = {};
  Object.entries(obj || {}).forEach(([k, v]) => {
    out[String(k).trim().toLowerCase()] = v;
  });
  return out;
};

const applyTemplateToRow = (row, template) => {
  const lower = lowerKeyObject(row);
  if (!template) return lower;

  const mapped = { ...lower };
  const columnMap = template.column_map || {};
  Object.entries(columnMap).forEach(([canonical, aliases]) => {
    if (typeof mapped[canonical] !== 'undefined' && String(mapped[canonical]).trim() !== '') return;
    const foundAlias = (aliases || []).find((alias) => {
      const key = String(alias).trim().toLowerCase();
      return typeof mapped[key] !== 'undefined' && String(mapped[key]).trim() !== '';
    });
    if (foundAlias) {
      mapped[canonical] = mapped[String(foundAlias).trim().toLowerCase()];
    }
  });
  return mapped;
};

const normalizeImportRows = (rows, template = null) =>
  rows.map((rawRow) => {
    const row = applyTemplateToRow(rawRow, template);
    return {
      sku_code: String(row.sku_code || '').trim(),
      item_name: String(row.name || row.item_name || '').trim(),
      purchased_qty: Number.parseInt(row.quantity || row.purchased_qty || '0', 10) || 0,
      purchased_cost: Number.parseFloat(row.cost || row.purchased_cost || '0') || 0,
      raw_row: rawRow,
    };
  });

const validateImportRowsAgainstTemplate = (normalizedRows, template) => {
  if (!template) return [];
  const required = Array.isArray(template.required_columns) ? template.required_columns : [];
  const errors = [];
  normalizedRows.forEach((row, idx) => {
    required.forEach((field) => {
      if (String(row[field] || '').trim() === '') {
        errors.push({ line_no: idx + 1, field, message: `Missing required field '${field}'` });
      }
    });
  });
  return errors;
};

const importPurchaseRows = async ({
  req,
  sourceName,
  rows,
  autoCreateItems,
  template = null,
}) => {
  const normalizedRows = normalizeImportRows(rows, template);
  const validationErrors = validateImportRowsAgainstTemplate(normalizedRows, template);
  if (validationErrors.length > 0) {
    const error = new Error('Import rows failed template validation');
    error.validationErrors = validationErrors;
    throw error;
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const batch = await client.query(
      `INSERT INTO purchase_import_batches (source_name, imported_by, total_rows)
       VALUES ($1, $2, $3)
       RETURNING *`,
      [sourceName, req.user.id, normalizedRows.length]
    );
    const batchId = batch.rows[0].id;

    let matchedRows = 0;
    let createdRows = 0;
    let unmatchedRows = 0;

    for (let i = 0; i < normalizedRows.length; i++) {
      const row = normalizedRows[i];
      let matchedItemId = null;
      let matchStatus = 'UNMATCHED';

      if (row.sku_code) {
        const existing = await client.query('SELECT id FROM items WHERE sku_code = $1', [row.sku_code]);
        if (existing.rows.length > 0) {
          matchedItemId = existing.rows[0].id;
          matchStatus = 'MATCHED';
          matchedRows += 1;
        } else if (autoCreateItems) {
          const created = await client.query(
            `INSERT INTO items (sku_code, name, cost, quantity)
             VALUES ($1, $2, $3, 0)
             RETURNING id`,
            [row.sku_code, row.item_name || row.sku_code, row.purchased_cost]
          );
          matchedItemId = created.rows[0].id;
          matchStatus = 'CREATED';
          createdRows += 1;
        } else {
          unmatchedRows += 1;
        }
      } else {
        unmatchedRows += 1;
      }

      await client.query(
        `INSERT INTO purchase_import_lines
         (batch_id, line_no, sku_code, item_name, purchased_qty, purchased_cost, matched_item_id, match_status, raw_row)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [
          batchId,
          i + 1,
          row.sku_code || null,
          row.item_name || null,
          row.purchased_qty,
          row.purchased_cost,
          matchedItemId,
          matchStatus,
          row.raw_row,
        ]
      );
    }

    const updatedBatch = await client.query(
      `UPDATE purchase_import_batches
       SET matched_rows = $1, created_rows = $2, unmatched_rows = $3
       WHERE id = $4
       RETURNING *`,
      [matchedRows, createdRows, unmatchedRows, batchId]
    );

    await client.query('COMMIT');

    await writeAuditLog(req, {
      action: 'PURCHASE_IMPORT',
      details: {
        batch_id: batchId,
        source_name: sourceName,
        total_rows: normalizedRows.length,
        matched_rows: matchedRows,
        created_rows: createdRows,
        unmatched_rows: unmatchedRows,
      },
    });
    await writeLedgerEvent(req, {
      eventType: 'PURCHASE_IMPORT',
      entityType: 'PURCHASE_IMPORT_BATCH',
      entityId: batchId,
      payload: {
        source_name: sourceName,
        total_rows: normalizedRows.length,
        matched_rows: matchedRows,
        created_rows: createdRows,
        unmatched_rows: unmatchedRows,
      },
    });

    return {
      batch: updatedBatch.rows[0],
      summary: {
        total_rows: normalizedRows.length,
        matched_rows: matchedRows,
        created_rows: createdRows,
        unmatched_rows: unmatchedRows,
      },
    };
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw err;
  } finally {
    client.release();
  }
};

const createPrintJobRecord = async ({
  req,
  labelType,
  printerIp,
  printerPort,
  copies = 1,
  zpl,
  payload = {},
}) => {
  const result = await pool.query(
    `INSERT INTO print_jobs
     (label_type, requester_user_id, printer_ip, printer_port, copies, status, attempts, zpl, payload)
     VALUES ($1, $2, $3, $4, $5, 'QUEUED', 0, $6, $7)
     RETURNING *`,
    [labelType, req.user ? req.user.id : null, printerIp, printerPort, copies, zpl, payload]
  );
  return result.rows[0];
};

const processPrintJob = async ({ jobId, printerIp, printerPort, zpl }) => {
  try {
    await sendZplToPrinter({ printerIp, printerPort, zpl });
    const success = await pool.query(
      `UPDATE print_jobs
       SET status = 'SUCCESS', attempts = attempts + 1, updated_at = NOW(), printed_at = NOW(), error_message = NULL
       WHERE id = $1
       RETURNING *`,
      [jobId]
    );
    return success.rows[0];
  } catch (err) {
    const failed = await pool.query(
      `UPDATE print_jobs
       SET status = 'FAILED', attempts = attempts + 1, updated_at = NOW(), error_message = $2
       WHERE id = $1
       RETURNING *`,
      [jobId, err && err.message ? err.message : 'Print failed']
    );
    throw Object.assign(new Error(failed.rows[0].error_message), { job: failed.rows[0] });
  }
};

const processQueuedPrintJobs = async (batchSize = PRINT_WORKER_BATCH_SIZE) => {
  const client = await pool.connect();
  let processed = 0;
  try {
    while (processed < batchSize) {
      await client.query('BEGIN');
      const picked = await client.query(
        `SELECT id, printer_ip, printer_port, zpl
         FROM print_jobs
         WHERE status = 'QUEUED'
         ORDER BY id
         FOR UPDATE SKIP LOCKED
         LIMIT 1`
      );

      if (picked.rows.length === 0) {
        await client.query('ROLLBACK');
        break;
      }

      const job = picked.rows[0];
      try {
        await sendZplToPrinter({
          printerIp: job.printer_ip,
          printerPort: job.printer_port,
          zpl: job.zpl,
        });
        await client.query(
          `UPDATE print_jobs
           SET status = 'SUCCESS', attempts = attempts + 1, updated_at = NOW(), error_message = NULL, printed_at = NOW()
           WHERE id = $1`,
          [job.id]
        );
      } catch (err) {
        await client.query(
          `UPDATE print_jobs
           SET status = 'FAILED', attempts = attempts + 1, updated_at = NOW(), error_message = $2
           WHERE id = $1`,
          [job.id, err && err.message ? err.message : 'Print failed']
        );
      }

      await client.query('COMMIT');
      processed += 1;
    }
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw err;
  } finally {
    client.release();
  }
  return processed;
};

const buildAssistantResponse = (message, role) => {
  const text = message.toLowerCase();
  const suggestions = [];
  let response = 'Try the Dashboard cards: Check-In, Stock In, Issue, Return, and Cycle Counts.';

  if (text.includes('check in') || text.includes('encounter')) {
    response = 'Use "Check-In + Label": enter appointment ID + provider card, then press Check In.';
    suggestions.push('Then scan the encounter label in Issue mode.');
  } else if (text.includes('cycle') || text.includes('count')) {
    response = 'Use "Cycle Count Workflow": create count, add lines, submit, then admin approves.';
    suggestions.push('Enable "Apply adjustments" during approval to sync stock.');
  } else if (text.includes('fraud') || text.includes('audit')) {
    response = 'Use Fraud Flags and Audit Logs cards. Run analysis, then move flags to REVIEWING/RESOLVED.';
  } else if (text.includes('password') || text.includes('reset')) {
    response = 'Password resets are enforced by policy. Use the reset form or admin access panel.';
  } else if (text.includes('import') || text.includes('excel') || text.includes('csv')) {
    response = 'Paste CSV into Purchase Import card. Required columns: sku_code,name,quantity,cost.';
    suggestions.push('Enable auto-create for unknown SKUs when needed.');
  } else if (text.includes('role') || text.includes('access') || text.includes('user')) {
    response = 'Admins can update role/active/reset settings in Admin User Access.';
  }

  if (role !== 'ADMIN') {
    suggestions.push('Admin-only pages: user access, fraud flags, audit logs, event ledger.');
  }

  return { response, suggestions };
};

const getImportTemplateById = async (templateId) => {
  if (!templateId) return null;
  const result = await pool.query(
    `SELECT id, vendor_name, description, required_columns, column_map, is_active, created_by, created_at, updated_at
     FROM import_vendor_templates
     WHERE id = $1`,
    [templateId]
  );
  if (result.rows.length === 0) return null;
  return result.rows[0];
};

const normalizeTemplatePayload = (payload) => {
  const requiredColumns = Array.from(new Set((payload.required_columns || [])
    .map((value) => String(value || '').trim().toLowerCase())
    .filter(Boolean)));

  const normalizedMap = {};
  Object.entries(payload.column_map || {}).forEach(([canonical, aliases]) => {
    const canonicalKey = String(canonical || '').trim().toLowerCase();
    if (!canonicalKey) return;
    const mappedAliases = Array.from(new Set((Array.isArray(aliases) ? aliases : [])
      .map((alias) => String(alias || '').trim().toLowerCase())
      .filter(Boolean)));
    normalizedMap[canonicalKey] = mappedAliases;
  });

  return {
    ...payload,
    required_columns: requiredColumns,
    column_map: normalizedMap,
  };
};

const resolveEncounterByCode = async (encounterCode) => {
  const result = await pool.query(
    'SELECT * FROM encounters WHERE encounter_code = $1',
    [encounterCode]
  );
  return result.rows.length > 0 ? result.rows[0] : null;
};

const isCardLinkedToEncounter = async (encounterId, cardCode) => {
  const direct = await pool.query(
    `SELECT 1
     FROM encounter_participants
     WHERE encounter_id = $1 AND card_code = $2
     LIMIT 1`,
    [encounterId, cardCode]
  );
  if (direct.rows.length > 0) return true;

  const provider = await pool.query(
    `SELECT 1
     FROM encounters
     WHERE id = $1 AND provider_card = $2
     LIMIT 1`,
    [encounterId, cardCode]
  );
  return provider.rows.length > 0;
};

const runFraudAnalysis = async () => {
  const inserted = [];

  // Heuristic 1: unusually large quantity on a single ISSUE transaction.
  const highQtyResult = await pool.query(
    `SELECT id, item_id, encounter_id, user_id, quantity, created_at
     FROM inventory_transactions
     WHERE type = 'ISSUE' AND quantity >= 20
     ORDER BY created_at DESC
     LIMIT 50`
  );

  for (const row of highQtyResult.rows) {
    const signature = `HIGH_QTY_ISSUE:${row.id}`;
    const summary = `High quantity ISSUE detected (tx ${row.id}, qty ${row.quantity}).`;
    const metadata = {
      transaction_id: row.id,
      item_id: row.item_id,
      encounter_id: row.encounter_id,
      user_id: row.user_id,
      quantity: row.quantity,
      created_at: row.created_at,
    };

    const result = await pool.query(
      `INSERT INTO fraud_flags (flag_type, score, status, signature, summary, metadata)
       VALUES ('HIGH_QTY_ISSUE', $1, 'OPEN', $2, $3, $4)
       ON CONFLICT (signature) DO NOTHING
       RETURNING id, flag_type, score, status, signature, summary, created_at`,
      [Math.min(100, row.quantity * 2), signature, summary, metadata]
    );

    if (result.rows.length > 0) inserted.push(result.rows[0]);
  }

  // Heuristic 2: repeated ISSUE transactions by same user for same item in last 24h.
  const repeatedIssueResult = await pool.query(
    `SELECT user_id, item_id, COUNT(*) AS issue_count, SUM(quantity) AS total_qty
     FROM inventory_transactions
     WHERE type = 'ISSUE'
       AND user_id IS NOT NULL
       AND created_at >= NOW() - INTERVAL '24 HOURS'
     GROUP BY user_id, item_id
     HAVING COUNT(*) >= 5
     ORDER BY COUNT(*) DESC, SUM(quantity) DESC
     LIMIT 50`
  );

  for (const row of repeatedIssueResult.rows) {
    const signature = `REPEATED_ISSUE_24H:${row.user_id}:${row.item_id}`;
    const summary = `Repeated ISSUE pattern in 24h (user ${row.user_id}, item ${row.item_id}, count ${row.issue_count}).`;
    const metadata = {
      user_id: row.user_id,
      item_id: row.item_id,
      issue_count: Number(row.issue_count),
      total_qty: Number(row.total_qty),
      window: '24h',
    };

    const score = Math.min(100, Number(row.issue_count) * 10 + Number(row.total_qty));
    const result = await pool.query(
      `INSERT INTO fraud_flags (flag_type, score, status, signature, summary, metadata)
       VALUES ('REPEATED_ISSUE_24H', $1, 'OPEN', $2, $3, $4)
       ON CONFLICT (signature) DO NOTHING
       RETURNING id, flag_type, score, status, signature, summary, created_at`,
      [score, signature, summary, metadata]
    );

    if (result.rows.length > 0) inserted.push(result.rows[0]);
  }

  return inserted;
};

// Middleware: Verify JWT, confirm live user access state, and enforce password policy
const verifyJWT = async (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Missing authorization token' });
  }

  try {
    const digest = digestToken(token);
    const revokedUntil = revokedTokenDigests.get(digest);
    if (revokedUntil && revokedUntil > Date.now()) {
      return res.status(401).json({ error: 'Token revoked' });
    }

    const decoded = jwt.verify(token, JWT_SECRET);
    if (!decoded.sid) {
      return res.status(401).json({ error: 'Invalid session token' });
    }

    const userResult = await pool.query(
      `SELECT id, student_card, name, role, is_active, must_reset_password, password_changed_at, password_expires_at
       FROM users
       WHERE id = $1`,
      [decoded.id]
    );

    if (userResult.rows.length === 0) {
      return res.status(401).json({ error: 'User no longer exists' });
    }

    const user = userResult.rows[0];
    if (!user.is_active) {
      return res.status(403).json({ error: 'User account is inactive' });
    }

    const tokenPwdTs = Number(decoded.pwd_ts || 0);
    const dbPwdTs = Math.floor(new Date(user.password_changed_at).getTime() / 1000);
    if (tokenPwdTs !== dbPwdTs) {
      return res.status(401).json({ error: 'Session invalidated after password change. Please login again.' });
    }

    await ensureRefreshTable();
    const sessionResult = await pool.query(
      `SELECT id
       FROM refresh_sessions
       WHERE id = $1
         AND user_id = $2
         AND revoked_at IS NULL
         AND expires_at > NOW()`,
      [decoded.sid, decoded.id]
    );
    if (sessionResult.rows.length === 0) {
      return res.status(401).json({ error: 'Session revoked or expired' });
    }

    req.user = {
      id: user.id,
      student_card: user.student_card,
      name: user.name,
      role: user.role,
      org_id: null,
      home_clinic_id: null,
      must_reset_password: Boolean(user.must_reset_password) || isPasswordExpired(user.password_expires_at),
      password_expires_at: user.password_expires_at,
    };
    req.token = token;
    next();
  } catch (err) {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
};

app.post('/logout', verifyJWT, validateRequest({ body: emptyBodySchema }), async (req, res) => {
  try {
    const decoded = jwt.decode(req.token);
    const expMs = decoded && decoded.exp
      ? Number(decoded.exp) * 1000
      : Date.now() + (8 * 60 * 60 * 1000);
    revokedTokenDigests.set(digestToken(req.token), expMs);
    if (decoded && decoded.sid) {
      await ensureRefreshTable();
      await pool.query(
        `UPDATE refresh_sessions
         SET revoked_at = NOW()
         WHERE id = $1 AND user_id = $2`,
        [decoded.sid, req.user.id]
      );
    }

    await writeAuditLog(req, {
      action: 'LOGOUT',
      details: { user_id: req.user.id },
    });
    return res.json({ status: 'ok' });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to logout' });
  }
});

// Middleware: Check if user has required role and password state
const requireRole = (...allowedRoles) => {
  return async (req, res, next) => {
    await verifyJWT(req, res, () => {
      if (!req.user || !allowedRoles.includes(req.user.role)) {
        return res.status(403).json({ error: 'Insufficient permissions for this role' });
      }
      if (enforceUserRateLimit(req, res)) {
        return;
      }

      if (req.user.must_reset_password && req.path !== '/change-password') {
        return res.status(403).json({
          error: 'Password reset required before accessing operational routes',
          code: 'PASSWORD_RESET_REQUIRED',
        });
      }

      next();
    });
  };
};

/* ---------------------------
   HEALTH CHECK
---------------------------- */

app.get('/', (req, res) => {
  res.send('Inventory system running');
});

app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
  });
});

app.get('/health/db', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    return res.status(200).json({
      status: 'ok',
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error('DB health check failed:', err && err.message ? err.message : err);
    return res.status(500).json({
      status: 'error',
      timestamp: new Date().toISOString(),
    });
  }
});

app.get('/test-db', requireRole('ADMIN'), async (req, res, next) => {
  try {
    const result = await pool.query('SELECT NOW()');
    res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

/* ---------------------------
   LOGIN (PASSWORD-BASED AUTH)
---------------------------- */

app.post('/login', loginRateLimit, async (req, res, next) => {
  const validation = loginSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }

  const { student_card, password } = validation.data;

  try {
    const result = await pool.query(
      `SELECT id, student_card, name, role, password_hash, is_active, must_reset_password, password_changed_at, password_expires_at
       FROM users
       WHERE student_card = $1`,
      [student_card]
    );

    if (result.rows.length === 0) {
      await writeAuditLog(req, {
        action: 'LOGIN_FAILED_USER_NOT_FOUND',
        severity: 'WARN',
        details: { student_card },
      });
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const user = result.rows[0];

    if (!user.is_active) {
      await writeAuditLog(req, {
        action: 'LOGIN_BLOCKED_INACTIVE_USER',
        severity: 'WARN',
        details: { student_card },
        actorUserId: user.id,
      });
      return res.status(403).json({ error: 'Account is inactive' });
    }

    if (!APP_ALLOWED_LOGIN_ROLES.has(user.role)) {
      await writeAuditLog(req, {
        action: 'LOGIN_BLOCKED_ROLE_NOT_ALLOWED',
        severity: 'WARN',
        details: { student_card, role: user.role },
        actorUserId: user.id,
      });
      return res.status(403).json({ error: 'Role is not allowed for warehouse tablet access' });
    }

    const passwordMatch = await verifyPassword(password, user.password_hash);
    if (!passwordMatch) {
      await writeAuditLog(req, {
        action: 'LOGIN_FAILED_BAD_PASSWORD',
        severity: 'WARN',
        details: { student_card },
        actorUserId: user.id,
      });
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    await maybeUpgradeHashToArgon2({
      userId: user.id,
      plain: password,
      currentHash: user.password_hash,
    });

    const mustResetPassword = Boolean(user.must_reset_password) || isPasswordExpired(user.password_expires_at);
    const sessionTokens = await issueSessionTokens({ user });

    await writeAuditLog(req, {
      action: 'LOGIN_SUCCESS',
      details: {
        student_card: user.student_card,
        role: user.role,
        must_reset_password: mustResetPassword,
        password_expires_at: user.password_expires_at,
      },
      actorUserId: user.id,
    });

    const capabilities = getRoleCapabilities(user.role);

    res.json({
      token: sessionTokens.accessToken,
      refresh_token: sessionTokens.refreshToken,
      refresh_expires_at: sessionTokens.refreshExpiresAt,
      user: {
        id: user.id,
        student_card: user.student_card,
        name: user.name,
        role: user.role,
        must_reset_password: mustResetPassword,
        password_expires_at: user.password_expires_at,
      },
      capabilities,
    });
  } catch (err) {
    next(err);
  }
});

app.post('/auth/refresh', validateRequest({ body: refreshTokenSchema }), async (req, res, next) => {
  const { refresh_token } = req.body;
  try {
    await ensureRefreshTable();
    const refreshHash = digestToken(refresh_token);
    const sessionResult = await pool.query(
      `SELECT rs.id, rs.user_id, rs.expires_at, u.student_card, u.name, u.role, u.is_active, u.must_reset_password, u.password_changed_at, u.password_expires_at
       FROM refresh_sessions rs
       JOIN users u ON u.id = rs.user_id
       WHERE rs.token_hash = $1
         AND rs.revoked_at IS NULL
         AND rs.expires_at > NOW()`,
      [refreshHash]
    );
    if (sessionResult.rows.length === 0) {
      return res.status(401).json({ error: 'Invalid or expired refresh token' });
    }
    const row = sessionResult.rows[0];
    if (!row.is_active) {
      return res.status(403).json({ error: 'Account is inactive' });
    }

    await pool.query(
      `UPDATE refresh_sessions
       SET revoked_at = NOW()
       WHERE id = $1`,
      [row.id]
    );

    const refreshed = await issueSessionTokens({
      user: {
        id: row.user_id,
        student_card: row.student_card,
        name: row.name,
        role: row.role,
        must_reset_password: row.must_reset_password,
        password_changed_at: row.password_changed_at,
        password_expires_at: row.password_expires_at,
      },
    });

    await pool.query(
      `UPDATE refresh_sessions
       SET rotated_from = $1
       WHERE token_hash = $2`,
      [row.id, digestToken(refreshed.refreshToken)]
    );

    await writeAuditLog(req, {
      action: 'TOKEN_REFRESH_ROTATED',
      details: { user_id: row.user_id, previous_session_id: row.id },
      actorUserId: row.user_id,
    });

    return res.json({
      token: refreshed.accessToken,
      refresh_token: refreshed.refreshToken,
      refresh_expires_at: refreshed.refreshExpiresAt,
    });
  } catch (err) {
    next(err);
  }
});

app.get('/me/capabilities', verifyJWT, async (req, res, next) => {
  try {
    const clinicId = getUserClinicId(req);
    const flags = await evaluateFeatureFlags({ userId: req.user.id, clinicId });
    return res.json({
      user: {
        id: req.user.id,
        student_card: req.user.student_card,
        name: req.user.name,
        role: req.user.role,
      },
      capabilities: getRoleCapabilities(req.user.role),
      feature_flags: flags,
      clinic_id: clinicId,
    });
  } catch (err) {
    next(err);
  }
});

/* ---------------------------
   CREATE ITEM
---------------------------- */

app.post('/create-item', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const validation = createItemSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }

  const { sku_code, name, cost, expiry_tracked } = validation.data;

  try {
    await ensureLotExpirySchema();
    const result = expiryTrackedColumnAvailable
      ? await pool.query(
        `INSERT INTO items (sku_code, name, cost, expiry_tracked)
         VALUES ($1, $2, $3, $4)
         RETURNING *`,
        [sku_code, name, cost, expiry_tracked]
      )
      : await pool.query(
        `INSERT INTO items (sku_code, name, cost)
         VALUES ($1, $2, $3)
         RETURNING *`,
        [sku_code, name, cost]
      );

    await writeAuditLog(req, {
      action: 'CREATE_ITEM',
      details: { item_id: result.rows[0].id, sku_code: result.rows[0].sku_code, name: result.rows[0].name },
    });
    await writeLedgerEvent(req, {
      eventType: 'CREATE_ITEM',
      entityType: 'ITEM',
      entityId: result.rows[0].id,
      payload: { sku_code: result.rows[0].sku_code, name: result.rows[0].name, cost: result.rows[0].cost, expiry_tracked: result.rows[0].expiry_tracked },
    });

    res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

/* ---------------------------
   CREATE USER
---------------------------- */

app.post('/create-user', requireRole('ADMIN'), async (req, res, next) => {
  const validation = createUserSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }

  const { student_card, name, password, role } = validation.data;

  try {
    const passwordHash = await hashPassword(password);
    const passwordExpiresAt = buildNextPasswordExpiry();

    const result = await pool.query(
      `INSERT INTO users (student_card, name, role, password_hash, is_active, must_reset_password, password_changed_at, password_expires_at)
       VALUES ($1, $2, $3, $4, TRUE, TRUE, NOW(), $5)
       RETURNING id, student_card, name, role, is_active, must_reset_password, password_expires_at`,
      [student_card, name, role, passwordHash, passwordExpiresAt]
    );

    await writeAuditLog(req, {
      action: 'CREATE_USER',
      details: { created_user_id: result.rows[0].id, role: result.rows[0].role },
    });
    await writeLedgerEvent(req, {
      eventType: 'CREATE_USER',
      entityType: 'USER',
      entityId: result.rows[0].id,
      payload: { student_card: result.rows[0].student_card, role: result.rows[0].role },
    });

    res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

app.post('/change-password', verifyJWT, async (req, res, next) => {
  const validation = changePasswordSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }

  const { current_password, new_password } = validation.data;

  try {
    const userResult = await pool.query(
      'SELECT id, password_hash FROM users WHERE id = $1',
      [req.user.id]
    );
    if (userResult.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    const passwordMatch = await verifyPassword(current_password, userResult.rows[0].password_hash);
    if (!passwordMatch) {
      await writeAuditLog(req, {
        action: 'PASSWORD_CHANGE_FAILED_BAD_CURRENT_PASSWORD',
        severity: 'WARN',
      });
      return res.status(400).json({ error: 'Current password is incorrect' });
    }

    const passwordHash = await hashPassword(new_password);
    const passwordExpiresAt = buildNextPasswordExpiry();

    await pool.query(
      `UPDATE users
       SET password_hash = $1,
           must_reset_password = FALSE,
           password_changed_at = NOW(),
           password_expires_at = $2,
           updated_at = NOW()
       WHERE id = $3`,
      [passwordHash, passwordExpiresAt, req.user.id]
    );

    await writeAuditLog(req, {
      action: 'PASSWORD_CHANGED',
      details: { password_expires_at: passwordExpiresAt.toISOString() },
    });
    await writeLedgerEvent(req, {
      eventType: 'PASSWORD_CHANGED',
      entityType: 'USER',
      entityId: req.user.id,
      payload: { password_expires_at: passwordExpiresAt.toISOString() },
    });

    return res.json({ status: 'ok', password_expires_at: passwordExpiresAt.toISOString() });
  } catch (err) {
    next(err);
  }
});

app.get('/admin/users', requireRole('ADMIN'), async (req, res, next) => {
  try {
    const result = await pool.query(
      `SELECT id, student_card, name, role, is_active, must_reset_password, password_changed_at, password_expires_at, created_at
       FROM users
       ORDER BY id`
    );
    res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

app.patch('/admin/users/:id/access', requireRole('ADMIN'), validateRequest({ params: userAccessParamSchema }), async (req, res, next) => {
  const userId = req.params.id;
  const validation = updateUserAccessSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }

  const { role, is_active, force_password_reset } = validation.data;
  if (typeof role === 'undefined' && typeof is_active === 'undefined' && typeof force_password_reset === 'undefined') {
    return res.status(400).json({ error: 'No changes provided' });
  }

  try {
    const fields = [];
    const values = [];
    let idx = 1;

    if (typeof role !== 'undefined') {
      fields.push(`role = $${idx++}`);
      values.push(role);
    }
    if (typeof is_active !== 'undefined') {
      fields.push(`is_active = $${idx++}`);
      values.push(is_active);
    }
    if (typeof force_password_reset !== 'undefined') {
      fields.push(`must_reset_password = $${idx++}`);
      values.push(force_password_reset);
    }
    fields.push('updated_at = NOW()');

    values.push(userId);
    const result = await pool.query(
      `UPDATE users
       SET ${fields.join(', ')}
       WHERE id = $${idx}
       RETURNING id, student_card, name, role, is_active, must_reset_password, password_changed_at, password_expires_at`,
      values
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    await writeAuditLog(req, {
      action: 'ADMIN_UPDATED_USER_ACCESS',
      details: { target_user_id: userId, role, is_active, force_password_reset },
    });
    await writeLedgerEvent(req, {
      eventType: 'ADMIN_UPDATED_USER_ACCESS',
      entityType: 'USER',
      entityId: userId,
      payload: { role, is_active, force_password_reset },
    });

    return res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

/* ---------------------------
   CREATE ENCOUNTER
---------------------------- */

app.post('/create-encounter', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const validation = createEncounterSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }

  const { encounter_code, scheduled_time, status } = validation.data;

  try {
    const result = await pool.query(
      `INSERT INTO encounters (encounter_code, scheduled_time, status)
       VALUES ($1, $2, $3)
       RETURNING *`,
      [encounter_code, scheduled_time || new Date(), status || 'ACTIVE']
    );

    await writeAuditLog(req, {
      action: 'CREATE_ENCOUNTER',
      details: { encounter_id: result.rows[0].id, encounter_code: result.rows[0].encounter_code },
    });
    await writeLedgerEvent(req, {
      eventType: 'CREATE_ENCOUNTER',
      entityType: 'ENCOUNTER',
      entityId: result.rows[0].id,
      payload: { encounter_code: result.rows[0].encounter_code, status: result.rows[0].status },
    });

    res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

/* ---------------------------
   CHECK-IN + ENCOUNTER LABEL
---------------------------- */

app.post('/check-in', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const validation = encounterCheckInSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }

  const {
    appointment_id,
    provider_card,
    scheduled_time,
    status,
    print_label,
    printer_ip,
    printer_port,
    copies,
  } = validation.data;

  if (rejectDuplicateAction(req, res, 'CHECKIN_CREATE', {
    appointment_id,
    provider_card,
    status,
    print_label,
    printer_ip: printer_ip || null,
    printer_port,
    copies,
  })) {
    return;
  }

  try {
    const providerResult = await pool.query(
      'SELECT id, student_card, name, role FROM users WHERE student_card = $1 AND is_active = TRUE',
      [provider_card]
    );

    if (providerResult.rows.length === 0) {
      await writeAuditLog(req, {
        action: 'CHECKIN_FAILED_PROVIDER_NOT_FOUND',
        severity: 'WARN',
        details: { appointment_id, provider_card },
      });
      return res.status(404).json({ error: 'Provider card not found in users table' });
    }

    const provider = providerResult.rows[0];
    const encounterCode = formatEncounterCode();

    const encounterResult = await pool.query(
      `INSERT INTO encounters (encounter_code, scheduled_time, status, appointment_id, provider_card, provider_user_id)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING *`,
      [encounterCode, scheduled_time || new Date(), status || 'ACTIVE', appointment_id, provider_card, provider.id]
    );

    await pool.query(
      `INSERT INTO encounter_participants
       (encounter_id, user_id, card_code, participant_role, created_by)
       VALUES ($1, $2, $3, 'PROVIDER', $4)
       ON CONFLICT DO NOTHING`,
      [encounterResult.rows[0].id, provider.id, provider_card, req.user.id]
    );

    const zpl = buildEncounterLabelZpl({
      encounterCode,
      appointmentId: appointment_id,
      providerCard: provider_card,
      providerName: provider.name,
      copies,
    });

    let printed = false;
    let printError = null;
    let printJobId = null;

    if (print_label) {
      if (!printer_ip) {
        return res.status(400).json({ error: 'printer_ip is required when print_label is true' });
      }

      try {
        const job = await createPrintJobRecord({
          req,
          labelType: 'ENCOUNTER',
          printerIp: printer_ip,
          printerPort: printer_port,
          copies,
          zpl,
          payload: { encounter_code: encounterCode, appointment_id, provider_card },
        });
        printJobId = job.id;
        printed = false;
      } catch (err) {
        printError = err && err.message ? err.message : 'Printer error';
      }
    }

    await writeAuditLog(req, {
      action: 'CHECKIN_CREATED_ENCOUNTER',
      details: {
        encounter_id: encounterResult.rows[0].id,
        encounter_code: encounterCode,
        appointment_id,
        provider_card,
        provider_user_id: provider.id,
        label_printed: printed,
        print_error: printError,
        print_job_id: printJobId,
        print_queued: Boolean(printJobId),
      },
    });
    await writeLedgerEvent(req, {
      eventType: 'CHECKIN_CREATED_ENCOUNTER',
      entityType: 'ENCOUNTER',
      entityId: encounterResult.rows[0].id,
      payload: {
        encounter_code: encounterCode,
        appointment_id,
        provider_card,
        printed,
        print_error: printError,
      },
    });

    return res.json({
      encounter: encounterResult.rows[0],
      provider: {
        id: provider.id,
        provider_card: provider.student_card,
        name: provider.name,
        role: provider.role,
      },
      label: {
        encounter_code: encounterCode,
        appointment_id,
        provider_card,
        zpl,
        printed,
        print_error: printError,
        print_job_id: printJobId,
        print_queued: Boolean(printJobId),
      },
    });
  } catch (err) {
    next(err);
  }
});

app.post('/encounters/:encounter_code/assistant-scan', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const encounterCode = String(req.params.encounter_code || '').trim();
  const parsed = encounterAssistantScanSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }

  const { assistant_card } = parsed.data;
  try {
    const encounter = await resolveEncounterByCode(encounterCode);
    if (!encounter) {
      return res.status(404).json({ error: 'Encounter not found' });
    }

    const userResult = await pool.query(
      'SELECT id, student_card, name, role FROM users WHERE student_card = $1 AND is_active = TRUE',
      [assistant_card]
    );
    if (userResult.rows.length === 0) {
      return res.status(404).json({ error: 'Assistant card not found' });
    }
    const assistant = userResult.rows[0];

    const insert = await pool.query(
      `INSERT INTO encounter_participants
       (encounter_id, user_id, card_code, participant_role, created_by)
       VALUES ($1, $2, $3, 'ASSISTANT', $4)
       ON CONFLICT (encounter_id, participant_role, card_code)
       DO UPDATE SET user_id = EXCLUDED.user_id
       RETURNING *`,
      [encounter.id, assistant.id, assistant_card, req.user.id]
    );

    await writeAuditLog(req, {
      action: 'ENCOUNTER_ASSISTANT_SCANNED',
      details: { encounter_code: encounterCode, encounter_id: encounter.id, assistant_card, assistant_user_id: assistant.id },
    });
    await writeLedgerEvent(req, {
      eventType: 'ENCOUNTER_ASSISTANT_SCANNED',
      entityType: 'ENCOUNTER',
      entityId: encounter.id,
      payload: { assistant_card, assistant_user_id: assistant.id },
    });

    return res.json({
      status: 'ok',
      encounter_code: encounterCode,
      participant: insert.rows[0],
      assistant: { id: assistant.id, card: assistant.student_card, name: assistant.name, role: assistant.role },
    });
  } catch (err) {
    next(err);
  }
});

app.post('/labels/encounter-zpl', requireRole('ADMIN', 'STAFF'), (req, res) => {
  const validation = encounterLabelSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }

  const { encounter_code, appointment_id, provider_card, provider_name, copies } = validation.data;
  const zpl = buildEncounterLabelZpl({
    encounterCode: encounter_code,
    appointmentId: appointment_id,
    providerCard: provider_card,
    providerName: provider_name,
    copies,
  });

  return res.json({ zpl });
});

app.post('/print/zebra', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const validation = zebraPrintSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }

  const { zpl, printer_ip, printer_port, dry_run } = validation.data;
  if (!isAllowedPrinterTarget(printer_ip)) {
    return res.status(400).json({ error: 'printer_ip must be local/private network address' });
  }
  if (Number(printer_port) !== 9100) {
    return res.status(400).json({ error: 'printer_port must be 9100' });
  }

  if (dry_run) {
    await writeAuditLog(req, {
      action: 'ZEBRA_PRINT_DRY_RUN',
      details: { printer_ip, printer_port, bytes: Buffer.byteLength(zpl, 'utf8') },
    });
    return res.json({ status: 'ok', dry_run: true, bytes: Buffer.byteLength(zpl, 'utf8') });
  }

  try {
    const job = await createPrintJobRecord({
      req,
      labelType: 'RAW_ZPL',
      printerIp: printer_ip,
      printerPort: printer_port,
      copies: 1,
      zpl,
      payload: { source: 'print-zebra-endpoint' },
    });
    await writeAuditLog(req, {
      action: 'ZEBRA_PRINT_QUEUED',
      details: { printer_ip, printer_port, bytes: Buffer.byteLength(zpl, 'utf8'), print_job_id: job.id },
    });
    await writeLedgerEvent(req, {
      eventType: 'ZEBRA_PRINT_QUEUED',
      entityType: 'LABEL',
      entityId: job.id,
      payload: { printer_ip, printer_port, bytes: Buffer.byteLength(zpl, 'utf8') },
    });
    return res.json({ status: 'ok', queued: true, print_job_id: job.id });
  } catch (err) {
    await writeAuditLog(req, {
      action: 'ZEBRA_PRINT_FAILURE',
      severity: 'ERROR',
      details: {
        printer_ip,
        printer_port,
        error: err && err.message ? err.message : 'Failed to print label',
      },
    });
    return res.status(500).json({
      status: 'error',
      printed: false,
      error: err && err.message ? err.message : 'Failed to print label',
    });
  }
});

/* ---------------------------
   ISSUE ITEM (STOCK CONTROL)
---------------------------- */
app.post('/issue-item', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const validation = transactionSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }
  const { item_id, encounter_id, quantity } = validation.data;
  const user_id = req.user.id;
  const reasonCode = movementReasonFromRequest(req, 'ISSUE_MANUAL');
  const movementSource = movementSourceFromRequest(req, 'API');
  const correlationId = movementCorrelationFromRequest(req);
  const idempotencyKey = idempotencyKeyFromRequest(req);
  const locationCode = movementLocationFromRequest(req, 'MAIN');
  const lotCode = movementLotFromRequest(req);
  const clinicId = clinicIdFromRequest(req);
  if (rejectDuplicateAction(req, res, 'ISSUE_ITEM', { item_id, encounter_id, user_id, quantity })) {
    return;
  }

  const client = await pool.connect();
  try {
    await ensureMovementSchema();
    await ensureLotExpirySchema();
    await beginInventoryTxn(client);
    const replayed = await findIdempotentMovement({ client, actorUserId: user_id, idempotencyKey });
    if (replayed) {
      await client.query('ROLLBACK');
      return res.json({ ...replayed, idempotent_replay: true });
    }

    const itemMeta = await client.query(
      expiryTrackedColumnAvailable
        ? `SELECT id, expiry_tracked
           FROM items
           WHERE id = $1
           FOR UPDATE`
        : `SELECT id, FALSE::boolean AS expiry_tracked
           FROM items
           WHERE id = $1
           FOR UPDATE`,
      [item_id]
    );
    if (itemMeta.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).send('Item not found');
    }
    const isExpiryTracked = Boolean(itemMeta.rows[0].expiry_tracked);
    let issuedLots = [];
    if (isExpiryTracked) {
      const fefo = await selectFefoLots({
        client,
        clinicId,
        locationCode,
        itemId: item_id,
        quantity,
        allowExpired: false,
      });
      if (fefo.remaining > 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          error: 'Insufficient non-expired lot stock for FEFO issue',
          remaining_unfulfilled_qty: fefo.remaining,
        });
      }
      issuedLots = fefo.picks;
    }

    const decremented = await client.query(
      `UPDATE items
       SET quantity = quantity - $1
       WHERE id = $2
         AND quantity >= $1
       RETURNING id, quantity`,
      [quantity, item_id]
    );
    if (decremented.rows.length === 0) {
      const itemExists = await client.query('SELECT id FROM items WHERE id = $1', [item_id]);
      await client.query('ROLLBACK');
      if (itemExists.rows.length === 0) return res.status(404).send('Item not found');
      return res.status(400).send('Insufficient stock');
    }

    const movementRows = [];
    if (isExpiryTracked) {
      for (let i = 0; i < issuedLots.length; i += 1) {
        const pick = issuedLots[i];
        await client.query(
          `UPDATE inventory_lot_balances
           SET quantity = quantity - $1, updated_at = NOW()
           WHERE id = $2
             AND quantity >= $1`,
          [pick.quantity, pick.lot_balance_id]
        );
        const movement = await insertInventoryMovement({
          client,
          itemId: item_id,
          encounterId: encounter_id,
          userId: user_id,
          actorUserId: user_id,
          type: 'ISSUE',
          quantity: pick.quantity,
          reasonCode: `${reasonCode}_FEFO`,
          movementSource,
          correlationId: `${correlationId}-lot-${i + 1}`,
          idempotencyKey: i === 0 ? idempotencyKey : null,
          locationCode,
          lotCode: pick.lot_code,
          allowNegative: false,
        });
        movementRows.push(movement);
      }
    } else {
      const movement = await insertInventoryMovement({
        client,
        itemId: item_id,
        encounterId: encounter_id,
        userId: user_id,
        actorUserId: user_id,
        type: 'ISSUE',
        quantity,
        reasonCode,
        movementSource,
        correlationId,
        idempotencyKey,
        locationCode,
        lotCode,
        allowNegative: false,
      });
      movementRows.push(movement);
    }

    await client.query('COMMIT');

    await writeAuditLog(req, {
      action: 'ISSUE_ITEM',
      details: {
        item_id,
        encounter_id,
        user_id,
        quantity,
        transaction_id: movementRows[0].id,
        fefo_lot_count: issuedLots.length,
      },
    });
    await writeLedgerEvent(req, {
      eventType: 'ISSUE_ITEM',
      entityType: 'INVENTORY_TRANSACTION',
      entityId: movementRows[0].id,
      payload: { item_id, encounter_id, user_id, quantity, fefo: isExpiryTracked, issued_lots: issuedLots },
    });

    res.json({
      ...movementRows[0],
      fefo: isExpiryTracked,
      issued_lots: issuedLots,
      transactions: movementRows,
    });

  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (e) { console.error('ROLLBACK ERROR:', e); }
    next(err);
  } finally {
    client.release();
  }
});

app.post('/issue-scan', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const validation = scanTransactionSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }
  const { encounter_code, item_barcode, operator_card, quantity } = validation.data;
  const reasonCode = movementReasonFromRequest(req, 'ISSUE_SCAN');
  const movementSource = movementSourceFromRequest(req, 'SCANNER');
  const correlationId = movementCorrelationFromRequest(req);
  const idempotencyKey = idempotencyKeyFromRequest(req);
  const locationCode = movementLocationFromRequest(req, 'MAIN');
  const lotCode = movementLotFromRequest(req);
  const clinicId = clinicIdFromRequest(req);
  if (rejectDuplicateAction(req, res, 'ISSUE_SCAN', { encounter_code, item_barcode, operator_card, quantity })) {
    return;
  }

  const client = await pool.connect();
  try {
    await ensureMovementSchema();
    await ensureLotExpirySchema();
    await beginInventoryTxn(client);

    const encounterResult = await client.query(
      'SELECT * FROM encounters WHERE encounter_code = $1 FOR UPDATE',
      [encounter_code]
    );
    if (encounterResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Encounter not found' });
    }
    const encounter = encounterResult.rows[0];

    const itemResult = await client.query(
      expiryTrackedColumnAvailable
        ? 'SELECT id, sku_code, name, quantity, expiry_tracked FROM items WHERE sku_code = $1 FOR UPDATE'
        : 'SELECT id, sku_code, name, quantity, FALSE::boolean AS expiry_tracked FROM items WHERE sku_code = $1 FOR UPDATE',
      [item_barcode]
    );
    if (itemResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Item barcode not found' });
    }
    const item = itemResult.rows[0];

    const operatorResult = await client.query(
      'SELECT id, student_card, name FROM users WHERE student_card = $1 AND is_active = TRUE',
      [operator_card]
    );
    if (operatorResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Operator card not found' });
    }
    const operator = operatorResult.rows[0];
    const replayed = await findIdempotentMovement({ client, actorUserId: operator.id, idempotencyKey });
    if (replayed) {
      await client.query('ROLLBACK');
      return res.json({ status: 'ok', idempotent_replay: true, transaction: replayed, encounter, item, operator });
    }

    const linked = await isCardLinkedToEncounter(encounter.id, operator_card);
    if (!linked) {
      await client.query('ROLLBACK');
      return res.status(403).json({ error: 'Operator card is not linked to this encounter' });
    }

    let issuedLots = [];
    if (item.expiry_tracked) {
      const fefo = await selectFefoLots({
        client,
        clinicId,
        locationCode,
        itemId: item.id,
        quantity,
        allowExpired: false,
      });
      if (fefo.remaining > 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          error: 'Insufficient non-expired lot stock for FEFO issue',
          remaining_unfulfilled_qty: fefo.remaining,
        });
      }
      issuedLots = fefo.picks;
    }

    const decrement = await client.query(
      `UPDATE items
       SET quantity = quantity - $1
       WHERE id = $2
         AND quantity >= $1
       RETURNING quantity`,
      [quantity, item.id]
    );
    if (decrement.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Insufficient stock' });
    }

    const transactions = [];
    if (item.expiry_tracked) {
      for (let i = 0; i < issuedLots.length; i += 1) {
        const pick = issuedLots[i];
        await client.query(
          `UPDATE inventory_lot_balances
           SET quantity = quantity - $1, updated_at = NOW()
           WHERE id = $2
             AND quantity >= $1`,
          [pick.quantity, pick.lot_balance_id]
        );
        const tx = await insertInventoryMovement({
          client,
          itemId: item.id,
          encounterId: encounter.id,
          userId: operator.id,
          actorUserId: operator.id,
          type: 'ISSUE',
          quantity: pick.quantity,
          reasonCode: `${reasonCode}_FEFO`,
          movementSource,
          correlationId: `${correlationId}-lot-${i + 1}`,
          idempotencyKey: i === 0 ? idempotencyKey : null,
          locationCode,
          lotCode: pick.lot_code,
          allowNegative: false,
        });
        transactions.push(tx);
      }
    } else {
      const tx = await insertInventoryMovement({
        client,
        itemId: item.id,
        encounterId: encounter.id,
        userId: operator.id,
        actorUserId: operator.id,
        type: 'ISSUE',
        quantity,
        reasonCode,
        movementSource,
        correlationId,
        idempotencyKey,
        locationCode,
        lotCode,
        allowNegative: false,
      });
      transactions.push(tx);
    }
    await client.query('COMMIT');

    await writeAuditLog(req, {
      action: 'ISSUE_SCAN',
      details: {
        encounter_code,
        item_barcode,
        operator_card,
        quantity,
        transaction_id: transactions[0].id,
        fefo_lot_count: issuedLots.length,
      },
    });
    await writeLedgerEvent(req, {
      eventType: 'ISSUE_SCAN',
      entityType: 'INVENTORY_TRANSACTION',
      entityId: transactions[0].id,
      payload: {
        encounter_code,
        item_barcode,
        operator_card,
        quantity,
        fefo: item.expiry_tracked,
        issued_lots: issuedLots,
      },
    });
    return res.json({
      status: 'ok',
      transaction: transactions[0],
      transactions,
      fefo: item.expiry_tracked,
      issued_lots: issuedLots,
      encounter,
      item,
      operator,
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally {
    client.release();
  }
});

app.post('/return-item', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const validation = transactionSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }
  const { item_id, encounter_id, quantity } = validation.data;
  const user_id = req.user.id;
  const reasonCode = movementReasonFromRequest(req, 'RETURN_MANUAL');
  const movementSource = movementSourceFromRequest(req, 'API');
  const correlationId = movementCorrelationFromRequest(req);
  const idempotencyKey = idempotencyKeyFromRequest(req);
  const locationCode = movementLocationFromRequest(req, 'MAIN');
  const lotCode = movementLotFromRequest(req);
  if (rejectDuplicateAction(req, res, 'RETURN_ITEM', { item_id, encounter_id, user_id, quantity })) {
    return;
  }

  const client = await pool.connect();
  try {
    await ensureMovementSchema();
    await ensureLotExpirySchema();
    await beginInventoryTxn(client);
    const replayed = await findIdempotentMovement({ client, actorUserId: user_id, idempotencyKey });
    if (replayed) {
      await client.query('ROLLBACK');
      return res.json({ ...replayed, idempotent_replay: true });
    }

    const updated = await client.query(
      expiryTrackedColumnAvailable
        ? 'UPDATE items SET quantity = quantity + $1 WHERE id = $2 RETURNING id, expiry_tracked'
        : 'UPDATE items SET quantity = quantity + $1 WHERE id = $2 RETURNING id, FALSE::boolean AS expiry_tracked',
      [quantity, item_id]
    );
    if (updated.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).send('Item not found');
    }
    if (updated.rows[0].expiry_tracked && !lotCode) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'lot_code header is required for expiry-tracked return' });
    }

    const movement = await insertInventoryMovement({
      client,
      itemId: item_id,
      encounterId: encounter_id,
      userId: user_id,
      actorUserId: user_id,
      type: 'RETURN',
      quantity,
      reasonCode,
      movementSource,
      correlationId,
      idempotencyKey,
      locationCode,
      lotCode,
      allowNegative: false,
    });
    if (updated.rows[0].expiry_tracked) {
      await upsertLotBalance({
        client,
        clinicId,
        locationCode,
        itemId: item_id,
        lotCode,
        expiryDate: expiryDate || new Date(Date.now() + 365 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10),
        qtyDelta: quantity,
      });
    }

    await client.query('COMMIT');

    await writeAuditLog(req, {
      action: 'RETURN_ITEM',
      details: { item_id, encounter_id, user_id, quantity, transaction_id: movement.id },
    });
    await writeLedgerEvent(req, {
      eventType: 'RETURN_ITEM',
      entityType: 'INVENTORY_TRANSACTION',
      entityId: movement.id,
      payload: { item_id, encounter_id, user_id, quantity },
    });

    res.json(movement);

  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (e) { console.error('ROLLBACK ERROR:', e); }
    next(err);
  } finally {
    client.release();
  }
});

/* ---------------------------
   STOCK IN (ADD INVENTORY)
---------------------------- */

app.post('/stock-in', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const validation = stockInSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }

  const { item_id, quantity, cost, vendor, batch_number, lot_code, expiry_date } = validation.data;
  const reasonCode = movementReasonFromRequest(req, 'RECEIPT_MANUAL');
  const movementSource = movementSourceFromRequest(req, 'API');
  const correlationId = movementCorrelationFromRequest(req);
  const idempotencyKey = idempotencyKeyFromRequest(req);
  const locationCode = movementLocationFromRequest(req, 'MAIN');
  const lotCode = movementLotFromRequest(req) || sanitizeTokenLikeValue(lot_code || batch_number, null);
  const expiryDate = parseExpiryDate(expiry_date || req.headers['x-expiry-date']);
  const clinicId = clinicIdFromRequest(req);
  if (rejectDuplicateAction(req, res, 'STOCK_IN', { item_id, quantity, cost, vendor, batch_number })) {
    return;
  }

  const client = await pool.connect();
  try {
    await ensureMovementSchema();
    await ensureLotExpirySchema();
    await beginInventoryTxn(client);
    const replayed = await findIdempotentMovement({ client, actorUserId: req.user.id, idempotencyKey });
    if (replayed) {
      await client.query('ROLLBACK');
      return res.json({ ...replayed, idempotent_replay: true });
    }

    const stockCheck = await client.query(
      expiryTrackedColumnAvailable
        ? 'SELECT quantity, expiry_tracked FROM items WHERE id = $1 FOR UPDATE'
        : 'SELECT quantity, FALSE::boolean AS expiry_tracked FROM items WHERE id = $1 FOR UPDATE',
      [item_id]
    );

    if (stockCheck.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).send('Item not found');
    }
    if (stockCheck.rows[0].expiry_tracked && (!lotCode || !expiryDate)) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'lot_code and expiry_date are required for expiry-tracked items' });
    }

    if (typeof cost === 'number') {
      await client.query(
        'UPDATE items SET quantity = quantity + $1, cost = $2 WHERE id = $3',
        [quantity, cost, item_id]
      );
    } else {
      await client.query(
        'UPDATE items SET quantity = quantity + $1 WHERE id = $2',
        [quantity, item_id]
      );
    }

    const result = await insertInventoryMovement({
      client,
      itemId: item_id,
      encounterId: null,
      userId: req.user.id,
      actorUserId: req.user.id,
      type: 'STOCK_IN',
      quantity,
      reasonCode,
      movementSource,
      correlationId,
      idempotencyKey,
      locationCode,
      lotCode,
      allowNegative: false,
    });

    if (stockCheck.rows[0].expiry_tracked) {
      await upsertLotBalance({
        client,
        clinicId,
        locationCode,
        itemId: item_id,
        lotCode,
        expiryDate,
        qtyDelta: quantity,
      });
    }

    await client.query('COMMIT');

    // Optionally log vendor/batch_number as application metadata
    if (vendor || batch_number) {
      console.log('STOCK_IN metadata:', { item_id, vendor, batch_number });
    }

    await writeAuditLog(req, {
      action: 'STOCK_IN',
      details: {
        item_id,
        quantity,
        cost: typeof cost === 'number' ? cost : null,
        vendor: vendor || null,
        batch_number: batch_number || null,
        transaction_id: result.id,
      },
    });
    await writeLedgerEvent(req, {
      eventType: 'STOCK_IN',
      entityType: 'INVENTORY_TRANSACTION',
      entityId: result.id,
      payload: {
        item_id,
        quantity,
        cost: typeof cost === 'number' ? cost : null,
        vendor: vendor || null,
        batch_number: batch_number || null,
      },
    });

    res.json(result);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (e) { console.error('ROLLBACK ERROR:', e); }
    next(err);
  } finally {
    client.release();
  }
});

/* ---------------------------
   STOCK LEVELS (READ-ONLY)
---------------------------- */

app.get('/stock-levels', requireRole('ADMIN', 'STAFF', 'STUDENT'), async (req, res, next) => {
  try {
    await ensureLotExpirySchema();
    const result = await pool.query(
      expiryTrackedColumnAvailable
        ? 'SELECT id, sku_code, name, quantity, cost, expiry_tracked FROM items ORDER BY id'
        : 'SELECT id, sku_code, name, quantity, cost, FALSE::boolean AS expiry_tracked FROM items ORDER BY id'
    );

    res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

app.get('/inventory/invariants', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    await ensureMovementSchema();
    let rows;
    try {
      rows = await pool.query(
        `SELECT item_id, sku_code, name, location_code, lot_code, on_hand_qty, derived_qty, variance_qty
         FROM inventory_balance_audit
         ORDER BY ABS(variance_qty) DESC, item_id ASC`
      );
    } catch (_) {
      rows = await pool.query(
        `SELECT
           i.id AS item_id,
           i.sku_code,
           i.name,
           'MAIN'::varchar(60) AS location_code,
           NULL::varchar(120) AS lot_code,
           i.quantity::numeric AS on_hand_qty,
           COALESCE(SUM(CASE
             WHEN it.type IN ('STOCK_IN', 'RETURN') THEN it.quantity
             WHEN it.type = 'ISSUE' THEN -1 * it.quantity
             ELSE 0
           END), 0)::numeric AS derived_qty,
           (i.quantity::numeric - COALESCE(SUM(CASE
             WHEN it.type IN ('STOCK_IN', 'RETURN') THEN it.quantity
             WHEN it.type = 'ISSUE' THEN -1 * it.quantity
             ELSE 0
           END), 0)::numeric) AS variance_qty
         FROM items i
         LEFT JOIN inventory_transactions it ON it.item_id = i.id
         GROUP BY i.id, i.sku_code, i.name, i.quantity
         ORDER BY ABS((i.quantity::numeric - COALESCE(SUM(CASE
             WHEN it.type IN ('STOCK_IN', 'RETURN') THEN it.quantity
             WHEN it.type = 'ISSUE' THEN -1 * it.quantity
             ELSE 0
           END), 0)::numeric)) DESC, i.id ASC`
      );
    }
    const mismatches = rows.rows.filter((row) => Number(row.variance_qty) !== 0);
    return res.json({
      generated_at: new Date().toISOString(),
      total_items: rows.rows.length,
      mismatches: mismatches.length,
      rows: rows.rows,
    });
  } catch (err) {
    next(err);
  }
});

const parseDashboardRange = (query) => {
  const to = query.to ? new Date(query.to) : new Date();
  const from = query.from ? new Date(query.from) : new Date(to.getTime() - 29 * 24 * 60 * 60 * 1000);
  if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) {
    throw new Error('Invalid date range');
  }
  if (from > to) {
    throw new Error('from cannot be greater than to');
  }
  return { from, to };
};

const INTELLIGENCE_DEFAULT_WINDOW_DAYS = Number(process.env.INTELLIGENCE_DEFAULT_WINDOW_DAYS || 30);
const INTELLIGENCE_DEFAULT_TARGET_COVERAGE_DAYS = Number(process.env.INTELLIGENCE_DEFAULT_TARGET_COVERAGE_DAYS || 21);

const clampInteger = (value, fallback, min, max) => {
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) return fallback;
  if (parsed < min) return min;
  if (parsed > max) return max;
  return parsed;
};

const classifyStockoutRisk = (daysRemaining, projectedDailyUsage, onHandQty) => {
  if (projectedDailyUsage <= 0) return 'LOW';
  if (onHandQty <= 0) return 'HIGH';
  if (daysRemaining <= 7) return 'HIGH';
  if (daysRemaining <= 21) return 'MEDIUM';
  return 'LOW';
};

const buildInventoryIntelligenceRow = (rawRow, targetCoverageDays, lookbackDays) => {
  const onHandQty = Number(rawRow.on_hand_qty || 0);
  const netUsage7d = Number(rawRow.net_usage_7d || 0);
  const netUsageWindow = Number(rawRow.net_usage_window || 0);

  const movingAverageDailyUsage = Math.max(0, netUsageWindow / lookbackDays);
  const shortMovingAverageDailyUsage = Math.max(0, netUsage7d / 7);
  const baseUsage = movingAverageDailyUsage > 0 ? movingAverageDailyUsage : shortMovingAverageDailyUsage;
  const trendRatio = movingAverageDailyUsage > 0 ? (shortMovingAverageDailyUsage / movingAverageDailyUsage) : (shortMovingAverageDailyUsage > 0 ? 1.2 : 1);
  const trendMultiplier = Math.min(Math.max(trendRatio, 0.85), 1.5);
  const projectedDailyUsage = Number((baseUsage * trendMultiplier).toFixed(3));
  const predictedDaysRemaining = projectedDailyUsage > 0
    ? Number((Math.max(onHandQty, 0) / projectedDailyUsage).toFixed(1))
    : null;

  const recommendedReorderQty = projectedDailyUsage > 0
    ? Math.max(Math.ceil(projectedDailyUsage * targetCoverageDays - onHandQty), 0)
    : 0;

  const riskLevel = classifyStockoutRisk(predictedDaysRemaining === null ? Number.POSITIVE_INFINITY : predictedDaysRemaining, projectedDailyUsage, onHandQty);
  const explanation = projectedDailyUsage <= 0
    ? 'No sustained consumption trend detected; item is currently low risk.'
    : `Projected daily usage ${projectedDailyUsage.toFixed(2)} units/day with ${onHandQty} on hand gives ~${predictedDaysRemaining} days remaining.`;

  return {
    item_id: rawRow.item_id,
    sku_code: rawRow.sku_code,
    name: rawRow.name,
    on_hand_qty: onHandQty,
    moving_average_daily_usage: Number(movingAverageDailyUsage.toFixed(3)),
    short_moving_average_daily_usage: Number(shortMovingAverageDailyUsage.toFixed(3)),
    projected_daily_usage: projectedDailyUsage,
    predicted_days_remaining: predictedDaysRemaining,
    recommended_reorder_qty: recommendedReorderQty,
    risk_level: riskLevel,
    ai_explanation: explanation,
  };
};

app.get(
  '/inventory-intelligence',
  requireRole('ADMIN', 'STAFF'),
  validateRequest({ query: intelligenceQuerySchema }),
  async (req, res, next) => {
  try {
    const lookbackDays = clampInteger(req.query.lookback_days, INTELLIGENCE_DEFAULT_WINDOW_DAYS, 14, 120);
    const targetCoverageDays = clampInteger(req.query.target_days, INTELLIGENCE_DEFAULT_TARGET_COVERAGE_DAYS, 7, 60);
    const providerUserId = req.query.provider_user_id ?? null;

    const result = await pool.query(
      `SELECT
         i.id AS item_id,
         i.sku_code,
         i.name,
         i.quantity::int AS on_hand_qty,
         COALESCE(SUM(CASE
           WHEN it.type = 'ISSUE'
             AND it.created_at >= NOW() - INTERVAL '6 days'
             AND ($1::int IS NULL OR e.provider_user_id = $1) THEN it.quantity
           WHEN it.type = 'RETURN'
             AND it.created_at >= NOW() - INTERVAL '6 days'
             AND ($1::int IS NULL OR e.provider_user_id = $1) THEN -1 * it.quantity
           ELSE 0
         END), 0)::numeric(14,3) AS net_usage_7d,
         COALESCE(SUM(CASE
           WHEN it.type = 'ISSUE'
             AND it.created_at >= NOW() - (($2::text || ' days')::interval)
             AND ($1::int IS NULL OR e.provider_user_id = $1) THEN it.quantity
           WHEN it.type = 'RETURN'
             AND it.created_at >= NOW() - (($2::text || ' days')::interval)
             AND ($1::int IS NULL OR e.provider_user_id = $1) THEN -1 * it.quantity
           ELSE 0
         END), 0)::numeric(14,3) AS net_usage_window
       FROM items i
       LEFT JOIN inventory_transactions it
         ON it.item_id = i.id
        AND it.type IN ('ISSUE', 'RETURN')
       LEFT JOIN encounters e
         ON e.id = it.encounter_id
       GROUP BY i.id, i.sku_code, i.name, i.quantity
       ORDER BY i.sku_code ASC, i.id ASC`,
      [providerUserId, lookbackDays]
    );

    const rows = result.rows
      .map((row) => buildInventoryIntelligenceRow(row, targetCoverageDays, lookbackDays))
      .sort((a, b) => {
        const weight = { HIGH: 3, MEDIUM: 2, LOW: 1 };
        const scoreDiff = (weight[b.risk_level] || 0) - (weight[a.risk_level] || 0);
        if (scoreDiff !== 0) return scoreDiff;
        const aDays = a.predicted_days_remaining === null ? Number.POSITIVE_INFINITY : a.predicted_days_remaining;
        const bDays = b.predicted_days_remaining === null ? Number.POSITIVE_INFINITY : b.predicted_days_remaining;
        return aDays - bDays;
      });

    const summary = rows.reduce((acc, row) => {
      if (row.risk_level === 'HIGH') acc.high_risk_count += 1;
      if (row.risk_level === 'MEDIUM') acc.medium_risk_count += 1;
      if (row.risk_level === 'LOW') acc.low_risk_count += 1;
      return acc;
    }, { high_risk_count: 0, medium_risk_count: 0, low_risk_count: 0 });

    return res.json({
      generated_at: new Date().toISOString(),
      parameters: {
        lookback_days: lookbackDays,
        target_coverage_days: targetCoverageDays,
        provider_user_id: providerUserId,
      },
      summary,
      rows,
    });
  } catch (err) {
    next(err);
  }
});

app.get('/dashboard/providers', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    const result = await pool.query(
      `SELECT id, student_card, name, role
       FROM users
       WHERE is_active = TRUE AND role IN ('ADMIN', 'STAFF')
       ORDER BY name`
    );
    return res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

app.get('/dashboard/kpis', requireRole('ADMIN', 'STAFF'), validateRequest({ query: dashboardRangeQuerySchema }), async (req, res, next) => {
  try {
    const { from, to } = parseDashboardRange(req.query);
    const providerUserId = req.query.provider_user_id ?? null;

    const dayCount = Math.max(1, Math.round((to.getTime() - from.getTime()) / (24 * 60 * 60 * 1000)) + 1);
    const prevTo = new Date(from.getTime() - 24 * 60 * 60 * 1000);
    const prevFrom = new Date(prevTo.getTime() - (dayCount - 1) * 24 * 60 * 60 * 1000);

    const providerClause = providerUserId ? ' AND e.provider_user_id = $3 ' : '';
    const providerParams = providerUserId ? [from, to, providerUserId] : [from, to];
    const prevProviderParams = providerUserId ? [prevFrom, prevTo, providerUserId] : [prevFrom, prevTo];

    const [valueResult, lowStockResult, activeResult, currUsageResult, prevUsageResult, currCostResult, prevCostResult] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(quantity * cost), 0)::numeric(14,2) AS inventory_value FROM items`),
      pool.query(`SELECT COUNT(*)::int AS low_stock_alerts FROM items WHERE quantity <= 5`),
      providerUserId
        ? pool.query(
          `SELECT COUNT(*)::int AS active_encounters
           FROM encounters
           WHERE status = 'ACTIVE' AND provider_user_id = $1`,
          [providerUserId]
        )
        : pool.query(`SELECT COUNT(*)::int AS active_encounters FROM encounters WHERE status = 'ACTIVE'`),
      pool.query(
        `SELECT COALESCE(SUM(it.quantity), 0)::int AS daily_usage
         FROM inventory_transactions it
         LEFT JOIN encounters e ON e.id = it.encounter_id
         WHERE it.type = 'ISSUE'
           AND it.created_at::date BETWEEN $1::date AND $2::date
           ${providerClause}`,
        providerParams
      ),
      pool.query(
        `SELECT COALESCE(SUM(it.quantity), 0)::int AS daily_usage
         FROM inventory_transactions it
         LEFT JOIN encounters e ON e.id = it.encounter_id
         WHERE it.type = 'ISSUE'
           AND it.created_at::date BETWEEN $1::date AND $2::date
           ${providerClause}`,
        prevProviderParams
      ),
      pool.query(
        `SELECT COALESCE(SUM(it.quantity * i.cost), 0)::numeric(14,2) AS encounter_cost
         FROM inventory_transactions it
         JOIN items i ON i.id = it.item_id
         LEFT JOIN encounters e ON e.id = it.encounter_id
         WHERE it.type = 'ISSUE'
           AND it.created_at::date BETWEEN $1::date AND $2::date
           ${providerClause}`,
        providerParams
      ),
      pool.query(
        `SELECT COALESCE(SUM(it.quantity * i.cost), 0)::numeric(14,2) AS encounter_cost
         FROM inventory_transactions it
         JOIN items i ON i.id = it.item_id
         LEFT JOIN encounters e ON e.id = it.encounter_id
         WHERE it.type = 'ISSUE'
           AND it.created_at::date BETWEEN $1::date AND $2::date
           ${providerClause}`,
        prevProviderParams
      ),
    ]);

    const deltaPct = (current, previous) => {
      const curr = Number(current || 0);
      const prev = Number(previous || 0);
      if (prev === 0) return curr === 0 ? 0 : 100;
      return Number((((curr - prev) / prev) * 100).toFixed(2));
    };

    return res.json({
      filters: {
        from: from.toISOString().slice(0, 10),
        to: to.toISOString().slice(0, 10),
        provider_user_id: providerUserId,
      },
      inventory_value: Number(valueResult.rows[0].inventory_value || 0),
      active_encounters: Number(activeResult.rows[0].active_encounters || 0),
      daily_usage: Number(currUsageResult.rows[0].daily_usage || 0),
      low_stock_alerts: Number(lowStockResult.rows[0].low_stock_alerts || 0),
      encounter_cost_total: Number(currCostResult.rows[0].encounter_cost || 0),
      deltas: {
        daily_usage_pct_vs_prev_window: deltaPct(currUsageResult.rows[0].daily_usage, prevUsageResult.rows[0].daily_usage),
        encounter_cost_pct_vs_prev_window: deltaPct(currCostResult.rows[0].encounter_cost, prevCostResult.rows[0].encounter_cost),
      },
    });
  } catch (err) {
    next(err);
  }
});

app.get('/dashboard/usage-7d', requireRole('ADMIN', 'STAFF'), validateRequest({ query: providerScopedQuerySchema }), async (req, res, next) => {
  try {
    const providerUserId = req.query.provider_user_id ?? null;
    const result = await pool.query(
      `SELECT to_char(day::date, 'YYYY-MM-DD') AS day, COALESCE(issue_qty, 0)::int AS issue_qty
       FROM (
         SELECT generate_series(CURRENT_DATE - INTERVAL '6 days', CURRENT_DATE, INTERVAL '1 day') AS day
       ) d
       LEFT JOIN (
         SELECT it.created_at::date AS issue_day, SUM(it.quantity) AS issue_qty
         FROM inventory_transactions it
         LEFT JOIN encounters e ON e.id = it.encounter_id
         WHERE it.type = 'ISSUE'
           AND it.created_at::date >= CURRENT_DATE - INTERVAL '6 days'
           ${providerUserId ? 'AND e.provider_user_id = $1' : ''}
         GROUP BY it.created_at::date
       ) q ON q.issue_day = d.day::date
       ORDER BY day`,
      providerUserId ? [providerUserId] : []
    );
    return res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

app.get('/dashboard/report', requireRole('ADMIN', 'STAFF'), validateRequest({ query: dashboardRangeQuerySchema }), async (req, res, next) => {
  try {
    const { from, to } = parseDashboardRange(req.query);
    const providerUserId = req.query.provider_user_id ?? null;

    const providerClause = providerUserId ? ' AND e.provider_user_id = $3 ' : '';
    const params = providerUserId ? [from, to, providerUserId] : [from, to];

    const [valuationTrend, dailyUsage, costPerEncounter, stockAging, lowRiskHeatmap, velocity] = await Promise.all([
      pool.query(
        `WITH days AS (
           SELECT generate_series($1::date, $2::date, INTERVAL '1 day') AS day
         ),
         tx AS (
           SELECT it.created_at::date AS day,
                 SUM(CASE
                        WHEN it.type = 'STOCK_IN' THEN it.quantity * i.cost
                        WHEN it.type = 'RETURN' THEN it.quantity * i.cost
                        WHEN it.type = 'ADJUST_IN' THEN it.quantity * i.cost
                        WHEN it.type = 'ISSUE' THEN -1 * it.quantity * i.cost
                        WHEN it.type = 'ADJUST_OUT' THEN -1 * it.quantity * i.cost
                        ELSE 0
                      END) AS delta_value
           FROM inventory_transactions it
           JOIN items i ON i.id = it.item_id
           LEFT JOIN encounters e ON e.id = it.encounter_id
           WHERE it.created_at::date BETWEEN $1::date AND $2::date
             ${providerClause}
           GROUP BY it.created_at::date
         )
         SELECT to_char(d.day::date, 'YYYY-MM-DD') AS day,
                COALESCE(t.delta_value, 0)::numeric(14,2) AS valuation_delta
         FROM days d
         LEFT JOIN tx t ON t.day = d.day::date
         ORDER BY day`,
        params
      ),
      pool.query(
        `SELECT to_char(day::date, 'YYYY-MM-DD') AS day, COALESCE(issue_qty, 0)::int AS issue_qty
         FROM (
           SELECT generate_series($1::date, $2::date, INTERVAL '1 day') AS day
         ) d
         LEFT JOIN (
           SELECT it.created_at::date AS issue_day, SUM(it.quantity) AS issue_qty
           FROM inventory_transactions it
           LEFT JOIN encounters e ON e.id = it.encounter_id
           WHERE it.type = 'ISSUE'
             AND it.created_at::date BETWEEN $1::date AND $2::date
             ${providerClause}
           GROUP BY it.created_at::date
         ) q ON q.issue_day = d.day::date
         ORDER BY day`,
        params
      ),
      pool.query(
        `SELECT it.encounter_id,
                to_char(MAX(it.created_at)::date, 'YYYY-MM-DD') AS last_tx_day,
                COALESCE(SUM(it.quantity * i.cost), 0)::numeric(14,2) AS total_cost
         FROM inventory_transactions it
         JOIN items i ON i.id = it.item_id
         LEFT JOIN encounters e ON e.id = it.encounter_id
         WHERE it.type = 'ISSUE'
           AND it.created_at::date BETWEEN $1::date AND $2::date
           ${providerClause}
         GROUP BY it.encounter_id
         ORDER BY total_cost DESC
         LIMIT 40`,
        params
      ),
      pool.query(
        `WITH item_age AS (
           SELECT i.id, i.sku_code, i.name,
                  COALESCE(EXTRACT(DAY FROM NOW() - MAX(CASE WHEN it.type='STOCK_IN' THEN it.created_at END)), 9999) AS age_days
           FROM items i
           LEFT JOIN inventory_transactions it ON it.item_id = i.id
           GROUP BY i.id, i.sku_code, i.name
         )
         SELECT CASE
                  WHEN age_days <= 30 THEN '0-30'
                  WHEN age_days <= 60 THEN '31-60'
                  WHEN age_days <= 90 THEN '61-90'
                  ELSE '90+'
                END AS bucket,
                COUNT(*)::int AS item_count
         FROM item_age
         GROUP BY bucket
         ORDER BY bucket`
      ),
      pool.query(
        `WITH usage30 AS (
           SELECT it.item_id, COUNT(*)::int AS tx_count_30d
           FROM inventory_transactions it
           WHERE it.created_at >= NOW() - INTERVAL '30 days'
           GROUP BY it.item_id
         )
         SELECT i.id,
                i.sku_code,
                i.name,
                i.quantity,
                COALESCE(u.tx_count_30d, 0)::int AS tx_count_30d,
                (GREATEST(0, 10 - i.quantity) * (1 + COALESCE(u.tx_count_30d, 0)))::int AS risk_score
         FROM items i
         LEFT JOIN usage30 u ON u.item_id = i.id
         WHERE i.quantity <= 10
         ORDER BY risk_score DESC, i.quantity ASC
         LIMIT 40`
      ),
      pool.query(
        `SELECT to_char(day::date, 'YYYY-MM-DD') AS day,
                COALESCE(issue_count, 0)::int AS issue_count,
                COALESCE(return_count, 0)::int AS return_count,
                COALESCE(stock_in_count, 0)::int AS stock_in_count
         FROM (
           SELECT generate_series($1::date, $2::date, INTERVAL '1 day') AS day
         ) d
         LEFT JOIN (
           SELECT it.created_at::date AS tx_day,
                  COUNT(*) FILTER (WHERE it.type='ISSUE') AS issue_count,
                  COUNT(*) FILTER (WHERE it.type='RETURN') AS return_count,
                  COUNT(*) FILTER (WHERE it.type='STOCK_IN') AS stock_in_count
           FROM inventory_transactions it
           LEFT JOIN encounters e ON e.id = it.encounter_id
           WHERE it.created_at::date BETWEEN $1::date AND $2::date
             ${providerClause}
           GROUP BY it.created_at::date
         ) t ON t.tx_day = d.day::date
         ORDER BY day`,
        params
      ),
    ]);

    return res.json({
      filters: {
        from: from.toISOString().slice(0, 10),
        to: to.toISOString().slice(0, 10),
        provider_user_id: providerUserId,
      },
      valuation_trend: valuationTrend.rows,
      daily_usage: dailyUsage.rows,
      cost_per_encounter: costPerEncounter.rows,
      stock_aging: stockAging.rows,
      low_stock_heatmap: lowRiskHeatmap.rows,
      transaction_velocity: velocity.rows,
    });
  } catch (err) {
    next(err);
  }
});

app.get('/dashboard/expiry', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    await ensureLotExpirySchema();
    const clinicId = clinicIdFromRequest(req);
    const clinicWhere = clinicId ? 'AND ilb.clinic_id = $1' : '';
    const params = clinicId ? [clinicId] : [];

    const [summary, details, slowMoving] = await Promise.all([
      pool.query(
        `SELECT
           COALESCE(SUM(CASE WHEN ilb.expiry_date < CURRENT_DATE THEN ilb.quantity ELSE 0 END), 0)::numeric AS expired_on_hand,
           COALESCE(SUM(CASE WHEN ilb.expiry_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days' THEN ilb.quantity ELSE 0 END), 0)::numeric AS expiring_30,
           COALESCE(SUM(CASE WHEN ilb.expiry_date > CURRENT_DATE + INTERVAL '30 days' AND ilb.expiry_date <= CURRENT_DATE + INTERVAL '60 days' THEN ilb.quantity ELSE 0 END), 0)::numeric AS expiring_60,
           COALESCE(SUM(CASE WHEN ilb.expiry_date > CURRENT_DATE + INTERVAL '60 days' AND ilb.expiry_date <= CURRENT_DATE + INTERVAL '90 days' THEN ilb.quantity ELSE 0 END), 0)::numeric AS expiring_90
         FROM inventory_lot_balances ilb
         WHERE ilb.quantity > 0
           ${clinicWhere}`,
        params
      ),
      pool.query(
        `SELECT ilb.clinic_id, ilb.item_id, i.sku_code, i.name, ilb.lot_code, ilb.expiry_date, ilb.quantity
         FROM inventory_lot_balances ilb
         JOIN items i ON i.id = ilb.item_id
         WHERE ilb.quantity > 0
           AND ilb.expiry_date <= CURRENT_DATE + INTERVAL '90 days'
           ${clinicWhere}
         ORDER BY ilb.expiry_date ASC, ilb.quantity DESC
         LIMIT 200`,
        params
      ),
      pool.query(
        `SELECT ilb.clinic_id, ilb.item_id, i.sku_code, i.name, ilb.lot_code, ilb.expiry_date, ilb.quantity,
                COALESCE(usage.issue_qty_30d, 0)::numeric AS issue_qty_30d
         FROM inventory_lot_balances ilb
         JOIN items i ON i.id = ilb.item_id
         LEFT JOIN (
           SELECT item_id, COALESCE(SUM(quantity), 0)::numeric AS issue_qty_30d
           FROM inventory_transactions
           WHERE type = 'ISSUE'
             AND created_at >= NOW() - INTERVAL '30 days'
           GROUP BY item_id
         ) usage ON usage.item_id = ilb.item_id
         WHERE ilb.quantity > 0
           AND ilb.expiry_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '45 days'
           AND COALESCE(usage.issue_qty_30d, 0) <= 5
           ${clinicWhere}
         ORDER BY ilb.expiry_date ASC, ilb.quantity DESC
         LIMIT 100`,
        params
      ),
    ]);

    return res.json({
      clinic_id: clinicId,
      summary: summary.rows[0] || {
        expired_on_hand: 0,
        expiring_30: 0,
        expiring_60: 0,
        expiring_90: 0,
      },
      expiring_rows: details.rows,
      slow_moving_near_expiry: slowMoving.rows,
    });
  } catch (err) {
    next(err);
  }
});

app.get('/inventory/lots', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    await ensureLotExpirySchema();
    const clinicId = clinicIdFromRequest(req);
    const result = await pool.query(
      `SELECT ilb.*, i.sku_code, i.name, ${expiryTrackedColumnAvailable ? 'i.expiry_tracked' : 'FALSE::boolean AS expiry_tracked'}
       FROM inventory_lot_balances ilb
       JOIN items i ON i.id = ilb.item_id
       WHERE ilb.quantity > 0
         AND ($1::bigint IS NULL OR ilb.clinic_id = $1)
       ORDER BY ilb.expiry_date ASC, i.sku_code ASC`,
      [clinicId || null]
    );
    return res.json({ clinic_id: clinicId, rows: result.rows });
  } catch (err) {
    next(err);
  }
});

app.get('/alerts/digests', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    await ensureLotExpirySchema();
    const clinicId = clinicIdFromRequest(req);
    const result = await pool.query(
      `SELECT d.id, d.clinic_id, CAST(d.clinic_id AS text) AS clinic_name, d.digest_date, d.payload, d.created_at
       FROM clinic_alert_digests d
       WHERE ($1::bigint IS NULL OR d.clinic_id = $1)
       ORDER BY d.digest_date DESC, d.id DESC
       LIMIT 60`,
      [clinicId || null]
    );
    return res.json({ rows: result.rows });
  } catch (err) {
    next(err);
  }
});

app.post('/alerts/digests/run', requireRole('ADMIN'), async (req, res, next) => {
  try {
    const processed = await processDailyExpiryDigests();
    await writeAuditLog(req, {
      action: 'EXPIRY_ALERT_DIGEST_RUN',
      details: { processed },
    });
    return res.json({ status: 'ok', processed });
  } catch (err) {
    next(err);
  }
});

app.get('/expiry-transfer-suggestions', requireRole('ADMIN', 'STAFF'), validateRequest({ query: expirySuggestionQuerySchema }), async (req, res, next) => {
  try {
    await ensureLotExpirySchema();
    const clinicId = clinicIdFromRequest(req);
    const days = req.query.days;
    const suggestions = await pool.query(
      `WITH expiring AS (
         SELECT ilb.clinic_id AS from_clinic_id, ilb.item_id, ilb.lot_code, ilb.expiry_date, ilb.quantity
         FROM inventory_lot_balances ilb
         WHERE ilb.quantity > 0
           AND ilb.expiry_date BETWEEN CURRENT_DATE AND CURRENT_DATE + ($1::int * INTERVAL '1 day')
           AND ($2::bigint IS NULL OR ilb.clinic_id = $2)
       ),
       usage AS (
         SELECT sl.clinic_id AS to_clinic_id, im.sku_code, COALESCE(SUM(ABS(sl.qty_delta)), 0)::numeric AS issue_qty_30d
         FROM stock_ledger sl
         JOIN item_master im ON im.id = sl.item_master_id
         WHERE sl.movement_type IN ('ISSUE', 'TRANSFER_OUT')
           AND sl.occurred_at >= NOW() - INTERVAL '30 days'
         GROUP BY sl.clinic_id, im.sku_code
       ),
       mapped AS (
         SELECT e.from_clinic_id, e.item_id, e.lot_code, e.expiry_date, e.quantity,
                u.to_clinic_id, u.issue_qty_30d,
                ROW_NUMBER() OVER (PARTITION BY e.from_clinic_id, e.item_id, e.lot_code ORDER BY u.issue_qty_30d DESC NULLS LAST) AS rn
         FROM expiring e
         JOIN items i ON i.id = e.item_id
         JOIN usage u ON u.sku_code = i.sku_code
         WHERE u.to_clinic_id <> e.from_clinic_id
       )
       SELECT m.from_clinic_id,
              CAST(m.from_clinic_id AS text) AS from_clinic_name,
              m.to_clinic_id,
              CAST(m.to_clinic_id AS text) AS to_clinic_name,
              m.item_id,
              i.sku_code,
              i.name,
              m.lot_code,
              m.expiry_date,
              m.quantity AS available_lot_qty,
              m.issue_qty_30d AS target_issue_qty_30d,
              LEAST(m.quantity, GREATEST(m.issue_qty_30d, 1))::numeric(14,3) AS suggested_transfer_qty
       FROM mapped m
       JOIN items i ON i.id = m.item_id
       WHERE m.rn = 1
       ORDER BY m.expiry_date ASC, m.issue_qty_30d DESC
       LIMIT 100`,
      [days, clinicId || null]
    );
    return res.json({ days, clinic_id: clinicId, rows: suggestions.rows });
  } catch (err) {
    next(err);
  }
});

app.get('/reorder/meta', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    await ensureReorderSchema();
    const clinics = await pool.query(
      `SELECT id, code, name
       FROM clinics
       ORDER BY name ASC`
    );
    return res.json({ clinics: clinics.rows });
  } catch (err) {
    next(err);
  }
});

app.get('/reorder/recommendations', requireRole('ADMIN', 'STAFF'), validateRequest({ query: reorderRecommendationQuerySchema }), async (req, res, next) => {
  try {
    const payload = await computeReorderRecommendations({
      clinicId: req.query.clinic_id || null,
      locationCode: req.query.location_code || null,
      includeZeroShortage: req.query.include_zero_shortage,
    });
    await writeAuditLog(req, {
      action: 'REORDER_RECOMMENDATIONS_GENERATED',
      details: {
        clinic_id: req.query.clinic_id || null,
        location_code: req.query.location_code || null,
        include_zero_shortage: req.query.include_zero_shortage,
        row_count: payload.rows.length,
      },
    });
    return res.json(payload);
  } catch (err) {
    next(err);
  }
});

app.get('/sync/pull', requireRole('ADMIN', 'STAFF'), validateRequest({ query: syncPullQuerySchema }), async (req, res, next) => {
  try {
    await ensureSyncSchema();
    const { since_id, limit, device_id } = req.query;
    const rows = await pool.query(
      `SELECT id, user_id, target_device_id, event_type, event_payload, created_at
       FROM sync_outbox
       WHERE id > $1
         AND (user_id IS NULL OR user_id = $2)
         AND (target_device_id IS NULL OR target_device_id = $3)
         AND acked_at IS NULL
       ORDER BY id ASC
       LIMIT $4`,
      [since_id, req.user.id, device_id || null, limit]
    );
    return res.json({
      since_id,
      max_outbox_id: rows.rows.length > 0 ? Number(rows.rows[rows.rows.length - 1].id) : since_id,
      rows: rows.rows,
    });
  } catch (err) {
    next(err);
  }
});

app.post('/sync/ack', requireRole('ADMIN', 'STAFF'), validateRequest({ body: syncAckSchema }), async (req, res, next) => {
  try {
    await ensureSyncSchema();
    const { max_outbox_id, device_id } = req.body;
    const updated = await pool.query(
      `UPDATE sync_outbox
       SET acked_at = NOW()
       WHERE id <= $1
         AND (user_id IS NULL OR user_id = $2)
         AND (target_device_id IS NULL OR target_device_id = $3)
         AND acked_at IS NULL
       RETURNING id`,
      [max_outbox_id, req.user.id, device_id || null]
    );
    return res.json({ status: 'ok', acked: updated.rows.length, max_outbox_id });
  } catch (err) {
    next(err);
  }
});

app.post('/sync/push', requireRole('ADMIN', 'STAFF'), validateRequest({ body: syncPushSchema }), async (req, res, next) => {
  const { device_id, actions } = req.body;
  const client = await pool.connect();
  try {
    await ensureSyncSchema();
    const results = [];
    for (const action of actions) {
      if (isBlockedDirectOnHandEdit(action)) {
        results.push({
          idempotency_key: action.idempotency_key,
          status: 'REJECTED',
          conflict_code: 'DIRECT_ON_HAND_EDIT_BLOCKED',
          message: 'Direct on-hand edits are not permitted',
        });
        continue;
      }

      await client.query('BEGIN');
      const existing = await client.query(
        `SELECT status, result_json, conflict_code
         FROM sync_inbox
         WHERE user_id = $1
           AND idempotency_key = $2
         LIMIT 1`,
        [req.user.id, action.idempotency_key]
      );
      if (existing.rows.length > 0) {
        await client.query('COMMIT');
        results.push({
          idempotency_key: action.idempotency_key,
          status: existing.rows[0].status,
          conflict_code: existing.rows[0].conflict_code || null,
          result: existing.rows[0].result_json || {},
          idempotent_replay: true,
        });
        continue;
      }

      const actionFingerprint = buildSyncActionFingerprint(action);
      const dupPattern = await client.query(
        `SELECT id
         FROM sync_inbox
         WHERE user_id = $1
           AND action_type = $2
           AND action_fingerprint = $3
         LIMIT 1`,
        [req.user.id, action.action_type, actionFingerprint]
      );
      if (dupPattern.rows.length > 0) {
        await client.query(
          `INSERT INTO sync_inbox
           (device_id, user_id, action_type, path, method, idempotency_key, action_fingerprint, payload, status, conflict_code, result_json, processed_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'CONFLICT', 'DUPLICATE_ACTION_PATTERN', $9, NOW())`,
          [
            device_id,
            req.user.id,
            action.action_type,
            action.path,
            action.method,
            action.idempotency_key,
            null,
            action.body || {},
            { error: 'Duplicate receipt/issue pattern detected' },
          ]
        );
        await client.query('COMMIT');
        results.push({
          idempotency_key: action.idempotency_key,
          status: 'CONFLICT',
          conflict_code: 'DUPLICATE_ACTION_PATTERN',
          message: 'Duplicate receipt/issue pattern detected',
        });
        continue;
      }

      const inserted = await client.query(
        `INSERT INTO sync_inbox
         (device_id, user_id, action_type, path, method, idempotency_key, action_fingerprint, payload, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'PENDING')
         RETURNING id`,
        [
          device_id,
          req.user.id,
          action.action_type,
          action.path,
          action.method,
          action.idempotency_key,
          actionFingerprint,
          action.body || {},
        ]
      );
      await client.query('COMMIT');

      const exec = await executeSyncActionViaApi({ req, action });
      const syncStatus = exec.ok
        ? 'APPLIED'
        : ((exec.status === 409 || String(exec.json?.error || '').toLowerCase().includes('duplicate')) ? 'DUPLICATE' : 'CONFLICT');
      const conflictCode = syncStatus === 'DUPLICATE'
        ? 'IDEMPOTENCY_DUPLICATE'
        : (syncStatus === 'CONFLICT' ? 'WRITE_REJECTED' : null);

      await client.query(
        `UPDATE sync_inbox
         SET status = $1,
             conflict_code = $2,
             result_json = $3,
             processed_at = NOW()
         WHERE id = $4`,
        [syncStatus, conflictCode, exec.json || { text: exec.text }, inserted.rows[0].id]
      );

      results.push({
        idempotency_key: action.idempotency_key,
        status: syncStatus,
        conflict_code: conflictCode,
        response_status: exec.status,
        result: exec.json,
      });
    }

    await writeAuditLog(req, {
      action: 'SYNC_PUSH_BATCH',
      details: { device_id, action_count: actions.length, result_count: results.length },
    });
    return res.json({
      status: 'ok',
      device_id,
      results,
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally {
    client.release();
  }
});

app.get('/sync/conflicts', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    await ensureSyncSchema();
    const rows = await pool.query(
      `SELECT id, device_id, action_type, path, method, idempotency_key, conflict_code, payload, result_json, created_at, processed_at
       FROM sync_inbox
       WHERE user_id = $1
         AND status IN ('CONFLICT', 'REJECTED')
       ORDER BY id DESC
       LIMIT 300`,
      [req.user.id]
    );
    return res.json({ rows: rows.rows });
  } catch (err) {
    next(err);
  }
});

app.get('/release/schema-version', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    await ensureReleaseControlSchema();
    const latest = await pool.query(
      `SELECT id, version_tag, checksum, applied_at, applied_by, notes
       FROM schema_versions
       ORDER BY id DESC
       LIMIT 1`
    );
    return res.json({
      latest: latest.rows[0] || null,
      server_time: new Date().toISOString(),
    });
  } catch (err) {
    next(err);
  }
});

app.get('/feature-flags', requireRole('ADMIN', 'STAFF'), validateRequest({ query: featureFlagQuerySchema }), async (req, res, next) => {
  try {
    await ensureReleaseControlSchema();
    const clinicId = req.query.clinic_id ?? getUserClinicId(req);
    const flagKey = req.query.flag_key || null;
    const where = [];
    const params = [];
    let idx = 1;
    if (clinicId) {
      where.push(`(clinic_id = $${idx} OR clinic_id IS NULL)`);
      params.push(clinicId);
      idx += 1;
    }
    if (flagKey) {
      where.push(`flag_key = $${idx}`);
      params.push(flagKey);
      idx += 1;
    }
    const whereSql = where.length > 0 ? `WHERE ${where.join(' AND ')}` : '';
    const rows = await pool.query(
      `SELECT id, clinic_id, flag_key, enabled, rollout_percentage, payload, created_at, updated_at
       FROM feature_flags
       ${whereSql}
       ORDER BY clinic_id NULLS FIRST, flag_key ASC`,
      params
    );
    return res.json({ rows: rows.rows });
  } catch (err) {
    next(err);
  }
});

app.post('/feature-flags', requireRole('ADMIN'), async (req, res, next) => {
  const parsed = featureFlagUpsertSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }
  try {
    await ensureReleaseControlSchema();
    const { clinic_id, flag_key, enabled, rollout_percentage, payload } = parsed.data;
    const result = await pool.query(
      `INSERT INTO feature_flags
       (clinic_id, flag_key, enabled, rollout_percentage, payload, created_by, updated_by)
       VALUES ($1, $2, $3, $4, $5, $6, $6)
       ON CONFLICT (clinic_id, flag_key)
       DO UPDATE SET
         enabled = EXCLUDED.enabled,
         rollout_percentage = EXCLUDED.rollout_percentage,
         payload = EXCLUDED.payload,
         updated_by = EXCLUDED.updated_by,
         updated_at = NOW()
       RETURNING *`,
      [clinic_id, flag_key, enabled, rollout_percentage, payload || {}, req.user.id]
    );
    await writeAuditLog(req, {
      action: 'FEATURE_FLAG_UPSERT',
      details: { clinic_id, flag_key, enabled, rollout_percentage },
    });
    return res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

app.get('/feature-flags/evaluate', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    const clinicId = getUserClinicId(req);
    const flags = await evaluateFeatureFlags({ userId: req.user.id, clinicId });
    return res.json({
      clinic_id: clinicId,
      user_id: req.user.id,
      flags,
    });
  } catch (err) {
    next(err);
  }
});

/* ---------------------------
   INVENTORY HISTORY (READ-ONLY)
---------------------------- */

app.get('/inventory-history/:item_id', requireRole('ADMIN', 'STAFF'), validateRequest({ params: itemIdParamSchema }), async (req, res, next) => {
  const item_id = req.params.item_id;

  try {
    const result = await pool.query(
      'SELECT * FROM inventory_transactions WHERE item_id = $1 ORDER BY id DESC',
      [item_id]
    );

    res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

/* ---------------------------
   ENCOUNTER USAGE (READ-ONLY)
---------------------------- */

app.get('/encounter-usage/:encounter_id', requireRole('ADMIN', 'STAFF'), validateRequest({ params: encounterIdParamSchema }), async (req, res, next) => {
  const encounter_id = req.params.encounter_id;

  try {
    const result = await pool.query(
      `SELECT i.name, SUM(it.quantity) as total_used
       FROM inventory_transactions it
       JOIN items i ON it.item_id = i.id
       WHERE it.encounter_id = $1 AND it.type = 'ISSUE'
       GROUP BY i.name`,
      [encounter_id]
    );

    res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

/* ---------------------------
   TRANSACTIONS (PAGINATED/FILTERED)
---------------------------- */

app.get('/transactions', requireRole('ADMIN', 'STAFF'), validateRequest({ query: transactionsQuerySchema }), async (req, res, next) => {
  try {
    const { limit, offset } = req.query;

    // Build dynamic WHERE clause and parameters array
    const conditions = [];
    const params = [];
    let paramIndex = 1;

    // Optional filters
    if (req.query.item_id) {
      conditions.push(`item_id = $${paramIndex}`);
      params.push(req.query.item_id);
      paramIndex++;
    }

    if (req.query.encounter_id) {
      conditions.push(`encounter_id = $${paramIndex}`);
      params.push(req.query.encounter_id);
      paramIndex++;
    }

    if (req.query.user_id) {
      conditions.push(`user_id = $${paramIndex}`);
      params.push(req.query.user_id);
      paramIndex++;
    }

    if (req.query.type) {
      conditions.push(`type = $${paramIndex}`);
      params.push(req.query.type);
      paramIndex++;
    }

    if (req.query.from) {
      conditions.push(`created_at >= $${paramIndex}`);
      params.push(req.query.from);
      paramIndex++;
    }

    if (req.query.to) {
      conditions.push(`created_at <= $${paramIndex}`);
      params.push(req.query.to);
      paramIndex++;
    }

    // Build base query
    let whereClause = '';
    if (conditions.length > 0) {
      whereClause = 'WHERE ' + conditions.join(' AND ');
    }

    // Add limit and offset parameters
    params.push(limit);
    params.push(offset);

    const query = `
      SELECT * FROM inventory_transactions
      ${whereClause}
      ORDER BY id DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;

    const result = await pool.query(query, params);

    res.json({
      limit,
      offset,
      rows: result.rows
    });

  } catch (err) {
    next(err);
  }
});

/* ---------------------------
   ENCOUNTER COST (COST SUMMARY)
---------------------------- */

app.get('/encounter-cost', requireRole('ADMIN', 'STAFF'), validateRequest({ query: encounterCostQuerySchema }), async (req, res, next) => {
  try {
    // Build dynamic WHERE clause and parameters array
    const conditions = ['it.type = \'ISSUE\''];
    const params = [];
    let paramIndex = 1;

    // Optional date filters
    if (req.query.from) {
      conditions.push(`it.created_at >= $${paramIndex}`);
      params.push(req.query.from);
      paramIndex++;
    }

    if (req.query.to) {
      conditions.push(`it.created_at <= $${paramIndex}`);
      params.push(req.query.to);
      paramIndex++;
    }

    // Build WHERE clause
    const whereClause = conditions.join(' AND ');

    const query = `
      SELECT 
        it.encounter_id,
        SUM(it.quantity * i.cost) as total_cost
      FROM inventory_transactions it
      JOIN items i ON it.item_id = i.id
      WHERE ${whereClause}
      GROUP BY it.encounter_id
      ORDER BY it.encounter_id
    `;

    const result = await pool.query(query, params);

    res.json(result.rows);

  } catch (err) {
    next(err);
  }
});

/* ---------------------------
   CYCLE COUNTS + DISCREPANCY APPROVAL
---------------------------- */

app.post('/cycle-counts', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const parsed = cycleCountCreateSchema.safeParse(req.body || {});
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }

  const code = `CC-${new Date().toISOString().slice(0, 10).replace(/-/g, '')}-${Math.floor(100000 + Math.random() * 900000)}`;
  try {
    const result = await pool.query(
      `INSERT INTO cycle_counts (code, status, created_by, notes)
       VALUES ($1, 'OPEN', $2, $3)
       RETURNING *`,
      [code, req.user.id, parsed.data.notes || null]
    );

    await writeAuditLog(req, {
      action: 'CYCLE_COUNT_CREATED',
      details: { cycle_count_id: result.rows[0].id, code: result.rows[0].code },
    });
    await writeLedgerEvent(req, {
      eventType: 'CYCLE_COUNT_CREATED',
      entityType: 'CYCLE_COUNT',
      entityId: result.rows[0].id,
      payload: { code: result.rows[0].code, notes: result.rows[0].notes },
    });

    return res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

app.post('/cycle-counts/:id/lines', requireRole('ADMIN', 'STAFF'), validateRequest({ params: positiveIdParamSchema }), async (req, res, next) => {
  const cycleCountId = req.params.id;

  const parsed = cycleCountLineSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }

  const { item_id, counted_qty } = parsed.data;

  try {
    const cc = await pool.query('SELECT id, status FROM cycle_counts WHERE id = $1', [cycleCountId]);
    if (cc.rows.length === 0) return res.status(404).json({ error: 'Cycle count not found' });
    if (cc.rows[0].status !== 'OPEN') return res.status(400).json({ error: 'Cycle count is not OPEN' });

    const item = await pool.query('SELECT id, quantity FROM items WHERE id = $1', [item_id]);
    if (item.rows.length === 0) return res.status(404).json({ error: 'Item not found' });

    const expectedQty = Number(item.rows[0].quantity);
    const variance = counted_qty - expectedQty;

    const result = await pool.query(
      `INSERT INTO cycle_count_lines (cycle_count_id, item_id, expected_qty, counted_qty, variance, entered_by)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (cycle_count_id, item_id)
       DO UPDATE SET expected_qty = EXCLUDED.expected_qty,
                     counted_qty = EXCLUDED.counted_qty,
                     variance = EXCLUDED.variance,
                     entered_by = EXCLUDED.entered_by
       RETURNING *`,
      [cycleCountId, item_id, expectedQty, counted_qty, variance, req.user.id]
    );

    await writeAuditLog(req, {
      action: 'CYCLE_COUNT_LINE_UPSERT',
      details: { cycle_count_id: cycleCountId, item_id, expected_qty: expectedQty, counted_qty, variance },
    });
    await writeLedgerEvent(req, {
      eventType: 'CYCLE_COUNT_LINE_UPSERT',
      entityType: 'CYCLE_COUNT',
      entityId: cycleCountId,
      payload: { item_id, expected_qty: expectedQty, counted_qty, variance },
    });

    return res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

app.get('/cycle-counts', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    const result = await pool.query(
      `SELECT cc.*,
              COUNT(ccl.id)::int AS line_count,
              COALESCE(SUM(ABS(ccl.variance)), 0)::int AS total_variance_units
       FROM cycle_counts cc
       LEFT JOIN cycle_count_lines ccl ON ccl.cycle_count_id = cc.id
       GROUP BY cc.id
       ORDER BY cc.id DESC
       LIMIT 200`
    );
    return res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

app.get('/cycle-counts/:id', requireRole('ADMIN', 'STAFF'), validateRequest({ params: positiveIdParamSchema }), async (req, res, next) => {
  const cycleCountId = req.params.id;
  try {
    const header = await pool.query('SELECT * FROM cycle_counts WHERE id = $1', [cycleCountId]);
    if (header.rows.length === 0) return res.status(404).json({ error: 'Cycle count not found' });

    const lines = await pool.query(
      `SELECT ccl.*, i.name AS item_name, i.sku_code
       FROM cycle_count_lines ccl
       JOIN items i ON i.id = ccl.item_id
       WHERE ccl.cycle_count_id = $1
       ORDER BY ccl.id`,
      [cycleCountId]
    );

    return res.json({ header: header.rows[0], lines: lines.rows });
  } catch (err) {
    next(err);
  }
});

app.post('/cycle-counts/random-pick', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const parsed = cycleCountRandomSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }

  const { item_count, notes } = parsed.data;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const picked = await client.query(
      `SELECT id, sku_code, name, quantity
       FROM items
       ORDER BY random()
       LIMIT $1`,
      [item_count]
    );
    if (picked.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'No items available for cycle count' });
    }

    const countCode = `CC-${new Date().toISOString().slice(0, 10).replace(/-/g, '')}-${Math.floor(100000 + Math.random() * 900000)}`;
    const header = await client.query(
      `INSERT INTO cycle_counts (code, status, created_by, notes)
       VALUES ($1, 'OPEN', $2, $3)
       RETURNING *`,
      [countCode, req.user.id, notes || `Random pick: ${picked.rows.length} items`]
    );

    for (const item of picked.rows) {
      await client.query(
        `INSERT INTO cycle_count_lines (cycle_count_id, item_id, expected_qty, counted_qty, variance, entered_by)
         VALUES ($1, $2, $3, $3, 0, $4)`,
        [header.rows[0].id, item.id, item.quantity, req.user.id]
      );
    }

    await client.query('COMMIT');

    await writeAuditLog(req, {
      action: 'CYCLE_COUNT_RANDOM_PICK',
      details: { cycle_count_id: header.rows[0].id, item_count: picked.rows.length },
    });
    await writeLedgerEvent(req, {
      eventType: 'CYCLE_COUNT_RANDOM_PICK',
      entityType: 'CYCLE_COUNT',
      entityId: header.rows[0].id,
      payload: { item_count: picked.rows.length },
    });

    return res.json({
      cycle_count: header.rows[0],
      picked_items: picked.rows,
      printable_rows: picked.rows.map((row, idx) => ({
        line_no: idx + 1,
        sku_code: row.sku_code,
        item_name: row.name,
        expected_qty: row.quantity,
      })),
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally {
    client.release();
  }
});

app.get('/cycle-counts/:id/printable', requireRole('ADMIN', 'STAFF'), validateRequest({ params: positiveIdParamSchema }), async (req, res, next) => {
  const cycleCountId = req.params.id;
  try {
    const header = await pool.query('SELECT * FROM cycle_counts WHERE id = $1', [cycleCountId]);
    if (header.rows.length === 0) return res.status(404).json({ error: 'Cycle count not found' });
    const lines = await pool.query(
      `SELECT ccl.id, i.sku_code, i.name AS item_name, ccl.expected_qty, ccl.counted_qty, ccl.variance
       FROM cycle_count_lines ccl
       JOIN items i ON i.id = ccl.item_id
       WHERE ccl.cycle_count_id = $1
       ORDER BY ccl.id`,
      [cycleCountId]
    );
    return res.json({ header: header.rows[0], lines: lines.rows });
  } catch (err) {
    next(err);
  }
});

app.post('/cycle-counts/:id/submit', requireRole('ADMIN', 'STAFF'), validateRequest({ params: positiveIdParamSchema }), async (req, res, next) => {
  const cycleCountId = req.params.id;
  try {
    const result = await pool.query(
      `UPDATE cycle_counts
       SET status = 'SUBMITTED', submitted_at = NOW()
       WHERE id = $1 AND status = 'OPEN'
       RETURNING *`,
      [cycleCountId]
    );
    if (result.rows.length === 0) {
      return res.status(400).json({ error: 'Cycle count must be OPEN to submit' });
    }

    await writeAuditLog(req, {
      action: 'CYCLE_COUNT_SUBMITTED',
      details: { cycle_count_id: cycleCountId },
    });
    await writeLedgerEvent(req, {
      eventType: 'CYCLE_COUNT_SUBMITTED',
      entityType: 'CYCLE_COUNT',
      entityId: cycleCountId,
      payload: {},
    });

    return res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

app.post('/cycle-counts/:id/approve', requireRole('ADMIN'), validateRequest({ params: positiveIdParamSchema }), async (req, res, next) => {
  const cycleCountId = req.params.id;

  const parsed = cycleCountApproveSchema.safeParse(req.body || {});
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }

  const { decision, notes, apply_adjustments } = parsed.data;
  const client = await pool.connect();
  try {
    await ensureMovementSchema();
    await beginInventoryTxn(client);

    const header = await client.query(
      `SELECT * FROM cycle_counts WHERE id = $1 FOR UPDATE`,
      [cycleCountId]
    );
    if (header.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Cycle count not found' });
    }
    if (header.rows[0].status !== 'SUBMITTED') {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Cycle count must be SUBMITTED for approval' });
    }

    const lines = await client.query(
      'SELECT * FROM cycle_count_lines WHERE cycle_count_id = $1',
      [cycleCountId]
    );

    if (decision === 'APPROVED' && apply_adjustments) {
      const correlationBase = movementCorrelationFromRequest(req);
      for (const line of lines.rows) {
        const variance = Number(line.variance || 0);
        if (variance === 0) continue;
        if (variance < 0) {
          const decremented = await client.query(
            `UPDATE items
             SET quantity = quantity - $1
             WHERE id = $2
               AND quantity >= $1
             RETURNING quantity`,
            [Math.abs(variance), line.item_id]
          );
          if (decremented.rows.length === 0) {
            await client.query('ROLLBACK');
            return res.status(409).json({ error: `Cycle adjustment would create negative stock for item ${line.item_id}` });
          }
        } else {
          await client.query(
            'UPDATE items SET quantity = quantity + $1 WHERE id = $2',
            [variance, line.item_id]
          );
        }

        await insertInventoryMovement({
          client,
          itemId: line.item_id,
          encounterId: null,
          userId: req.user.id,
          actorUserId: req.user.id,
          type: variance > 0 ? 'ADJUST_IN' : 'ADJUST_OUT',
          quantity: Math.abs(variance),
          reasonCode: 'CYCLE_COUNT_ADJUSTMENT',
          movementSource: 'CYCLE_COUNT_APPROVAL',
          correlationId: `${correlationBase}-item-${line.item_id}`,
          idempotencyKey: null,
          locationCode: 'MAIN',
          lotCode: null,
          allowNegative: false,
        });
      }
    }

    const status = decision === 'APPROVED' ? 'APPROVED' : 'REJECTED';
    const updated = await client.query(
      `UPDATE cycle_counts
       SET status = $1,
           approved_by = $2,
           approved_at = NOW(),
           notes = COALESCE($3, notes)
       WHERE id = $4
       RETURNING *`,
      [status, req.user.id, notes || null, cycleCountId]
    );

    await client.query('COMMIT');

    await writeAuditLog(req, {
      action: 'CYCLE_COUNT_APPROVAL_DECISION',
      details: { cycle_count_id: cycleCountId, decision, apply_adjustments, line_count: lines.rows.length },
    });
    await writeLedgerEvent(req, {
      eventType: 'CYCLE_COUNT_APPROVAL_DECISION',
      entityType: 'CYCLE_COUNT',
      entityId: cycleCountId,
      payload: { decision, apply_adjustments, notes: notes || null, line_count: lines.rows.length },
    });

    return res.json({ cycle_count: updated.rows[0], lines: lines.rows, adjustments_applied: decision === 'APPROVED' && apply_adjustments });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally {
    client.release();
  }
});

app.post('/stock-in-scan', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const validation = stockInScanSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }

  const { item_barcode, quantity, item_name, cost, print_new_label, printer_ip, printer_port, lot_code, expiry_date } = validation.data;
  const reasonCode = movementReasonFromRequest(req, 'RECEIPT_SCAN');
  const movementSource = movementSourceFromRequest(req, 'SCANNER');
  const correlationId = movementCorrelationFromRequest(req);
  const idempotencyKey = idempotencyKeyFromRequest(req);
  const locationCode = movementLocationFromRequest(req, 'MAIN');
  const lotCode = movementLotFromRequest(req) || sanitizeTokenLikeValue(lot_code, null);
  const expiryDate = parseExpiryDate(expiry_date || req.headers['x-expiry-date']);
  const clinicId = clinicIdFromRequest(req);
  if (rejectDuplicateAction(req, res, 'STOCK_IN_SCAN', { item_barcode, quantity, item_name, cost })) {
    return;
  }

  const client = await pool.connect();
  try {
    await ensureMovementSchema();
    await ensureLotExpirySchema();
    await beginInventoryTxn(client);
    const replayed = await findIdempotentMovement({ client, actorUserId: req.user.id, idempotencyKey });
    if (replayed) {
      await client.query('ROLLBACK');
      return res.json({ status: 'ok', idempotent_replay: true, transaction: replayed });
    }
    let item;
    const existing = await client.query(
      'SELECT * FROM items WHERE sku_code = $1 FOR UPDATE',
      [item_barcode]
    );
    if (existing.rows.length > 0) {
      item = existing.rows[0];
      if (typeof cost === 'number') {
        await client.query(
          'UPDATE items SET quantity = quantity + $1, cost = $2 WHERE id = $3',
          [quantity, cost, item.id]
        );
      } else {
        await client.query(
          'UPDATE items SET quantity = quantity + $1 WHERE id = $2',
          [quantity, item.id]
        );
      }
    } else {
      const created = expiryTrackedColumnAvailable
        ? await client.query(
          `INSERT INTO items (sku_code, name, cost, quantity, expiry_tracked)
           VALUES ($1, $2, $3, $4, FALSE)
           RETURNING *`,
          [item_barcode, item_name || `Part ${item_barcode}`, typeof cost === 'number' ? cost : 0, quantity]
        )
        : await client.query(
          `INSERT INTO items (sku_code, name, cost, quantity)
           VALUES ($1, $2, $3, $4)
           RETURNING *`,
          [item_barcode, item_name || `Part ${item_barcode}`, typeof cost === 'number' ? cost : 0, quantity]
        );
      item = created.rows[0];
    }

    if (item.expiry_tracked && (!lotCode || !expiryDate)) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'lot_code and expiry_date are required for expiry-tracked items' });
    }

    const tx = await insertInventoryMovement({
      client,
      itemId: item.id,
      encounterId: null,
      userId: req.user.id,
      actorUserId: req.user.id,
      type: 'STOCK_IN',
      quantity,
      reasonCode,
      movementSource,
      correlationId,
      idempotencyKey,
      locationCode,
      lotCode,
      allowNegative: false,
    });

    if (item.expiry_tracked) {
      await upsertLotBalance({
        client,
        clinicId,
        locationCode,
        itemId: item.id,
        lotCode,
        expiryDate,
        qtyDelta: quantity,
      });
    }

    await client.query('COMMIT');

    let label = null;
    if (print_new_label || existing.rows.length === 0) {
      const zpl = buildItemLabelZpl({ skuCode: item.sku_code, itemName: item.name, copies: 1 });
      if (printer_ip) {
        const job = await createPrintJobRecord({
          req,
          labelType: 'ITEM',
          printerIp: printer_ip,
          printerPort: printer_port,
          copies: 1,
          zpl,
          payload: { item_id: item.id, sku_code: item.sku_code },
        });
        label = { zpl, print_job_id: job.id, queued: true };
      } else {
        label = { zpl, queued: false };
      }
    }

    await writeAuditLog(req, {
      action: 'STOCK_IN_SCAN',
      details: { item_id: item.id, item_barcode, quantity, created_new_item: existing.rows.length === 0, transaction_id: tx.id },
    });
    await writeLedgerEvent(req, {
      eventType: 'STOCK_IN_SCAN',
      entityType: 'INVENTORY_TRANSACTION',
      entityId: tx.id,
      payload: { item_id: item.id, item_barcode, quantity },
    });

    return res.json({
      status: 'ok',
      created_new_item: existing.rows.length === 0,
      item,
      transaction: tx,
      label,
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally {
    client.release();
  }
});

app.post('/return-scan', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const validation = scanTransactionSchema.safeParse(req.body);
  if (!validation.success) {
    return res.status(400).json({ error: 'Invalid request', issues: validation.error.issues });
  }
  const { encounter_code, item_barcode, operator_card, quantity } = validation.data;
  const reasonCode = movementReasonFromRequest(req, 'RETURN_SCAN');
  const movementSource = movementSourceFromRequest(req, 'SCANNER');
  const correlationId = movementCorrelationFromRequest(req);
  const idempotencyKey = idempotencyKeyFromRequest(req);
  const locationCode = movementLocationFromRequest(req, 'MAIN');
  const lotCode = movementLotFromRequest(req);
  if (rejectDuplicateAction(req, res, 'RETURN_SCAN', { encounter_code, item_barcode, operator_card, quantity })) {
    return;
  }

  const client = await pool.connect();
  try {
    await ensureMovementSchema();
    await beginInventoryTxn(client);
    const encounterResult = await client.query(
      'SELECT * FROM encounters WHERE encounter_code = $1 FOR UPDATE',
      [encounter_code]
    );
    if (encounterResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Encounter not found' });
    }
    const encounter = encounterResult.rows[0];

    const itemResult = await client.query(
      expiryTrackedColumnAvailable
        ? 'SELECT id, sku_code, name, expiry_tracked FROM items WHERE sku_code = $1 FOR UPDATE'
        : 'SELECT id, sku_code, name, FALSE::boolean AS expiry_tracked FROM items WHERE sku_code = $1 FOR UPDATE',
      [item_barcode]
    );
    if (itemResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Item barcode not found' });
    }
    const item = itemResult.rows[0];

    const operatorResult = await client.query(
      'SELECT id, student_card, name FROM users WHERE student_card = $1 AND is_active = TRUE',
      [operator_card]
    );
    if (operatorResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Operator card not found' });
    }
    const operator = operatorResult.rows[0];
    const replayed = await findIdempotentMovement({ client, actorUserId: operator.id, idempotencyKey });
    if (replayed) {
      await client.query('ROLLBACK');
      return res.json({ status: 'ok', idempotent_replay: true, transaction: replayed, encounter, item, operator });
    }

    const linked = await isCardLinkedToEncounter(encounter.id, operator_card);
    if (!linked) {
      await client.query('ROLLBACK');
      return res.status(403).json({ error: 'Operator card is not linked to this encounter' });
    }

    await client.query('UPDATE items SET quantity = quantity + $1 WHERE id = $2', [quantity, item.id]);
    if (item.expiry_tracked && !lotCode) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'lot_code header is required for expiry-tracked return' });
    }
    if (item.expiry_tracked) {
      await upsertLotBalance({
        client,
        clinicId,
        locationCode,
        itemId: item.id,
        lotCode,
        expiryDate: expiryDate || new Date(Date.now() + 365 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10),
        qtyDelta: quantity,
      });
    }
    const tx = await insertInventoryMovement({
      client,
      itemId: item.id,
      encounterId: encounter.id,
      userId: operator.id,
      actorUserId: operator.id,
      type: 'RETURN',
      quantity,
      reasonCode,
      movementSource,
      correlationId,
      idempotencyKey,
      locationCode,
      lotCode,
      allowNegative: false,
    });
    await client.query('COMMIT');

    await writeAuditLog(req, {
      action: 'RETURN_SCAN',
      details: { encounter_code, item_barcode, operator_card, quantity, transaction_id: tx.id },
    });
    await writeLedgerEvent(req, {
      eventType: 'RETURN_SCAN',
      entityType: 'INVENTORY_TRANSACTION',
      entityId: tx.id,
      payload: { encounter_code, item_barcode, operator_card, quantity },
    });
    return res.json({ status: 'ok', transaction: tx, encounter, item, operator });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally {
    client.release();
  }
});

/* ---------------------------
   PURCHASE IMPORT + RECONCILIATION
---------------------------- */

app.post('/imports/purchases/csv', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const parsed = purchaseImportCsvSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }

  const { source_name, csv_text, auto_create_items, template_id } = parsed.data;
  try {
    const template = template_id ? await getImportTemplateById(template_id) : null;
    if (template_id && !template) {
      return res.status(404).json({ error: 'Import template not found' });
    }
    if (template && !template.is_active) {
      return res.status(400).json({ error: 'Import template is inactive' });
    }
    const rows = parseCsvRows(csv_text);
    const result = await importPurchaseRows({
      req,
      sourceName: source_name,
      rows,
      autoCreateItems: auto_create_items,
      template,
    });
    return res.json({ ...result, template_id: template ? template.id : null });
  } catch (err) {
    if (err && Array.isArray(err.validationErrors)) {
      return res.status(400).json({
        error: 'Import failed template validation',
        validation_errors: err.validationErrors,
      });
    }
    next(err);
  }
});

app.post('/imports/purchases/xlsx', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const parsed = purchaseImportXlsxSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }

  const { source_name, file_base64, sheet_name, auto_create_items, template_id } = parsed.data;

  try {
    const template = template_id ? await getImportTemplateById(template_id) : null;
    if (template_id && !template) {
      return res.status(404).json({ error: 'Import template not found' });
    }
    if (template && !template.is_active) {
      return res.status(400).json({ error: 'Import template is inactive' });
    }

    const binary = Buffer.from(file_base64, 'base64');
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.load(binary);
    const worksheet = sheet_name
      ? workbook.getWorksheet(sheet_name)
      : workbook.worksheets[0];

    if (!worksheet) {
      return res.status(400).json({ error: 'Excel file has no sheets' });
    }

    const rows = parseWorksheetRows(worksheet);
    if (rows.length === 0) {
      return res.status(400).json({ error: 'Selected sheet has no data rows' });
    }

    const result = await importPurchaseRows({
      req,
      sourceName: source_name,
      rows,
      autoCreateItems: auto_create_items,
      template,
    });
    return res.json({ ...result, sheet_name: worksheet.name, template_id: template ? template.id : null });
  } catch (err) {
    if (err && Array.isArray(err.validationErrors)) {
      return res.status(400).json({
        error: 'Import failed template validation',
        validation_errors: err.validationErrors,
      });
    }
    next(err);
  }
});

app.get('/import-templates', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    const result = await pool.query(
      `SELECT id, vendor_name, description, required_columns, column_map, is_active, created_by, created_at, updated_at
       FROM import_vendor_templates
       ORDER BY vendor_name ASC`
    );
    return res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

app.post('/import-templates', requireRole('ADMIN'), async (req, res, next) => {
  const parsed = importTemplateSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }

  const payload = normalizeTemplatePayload(parsed.data);
  try {
    const result = await pool.query(
      `INSERT INTO import_vendor_templates
       (vendor_name, description, required_columns, column_map, is_active, created_by, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, NOW())
       RETURNING id, vendor_name, description, required_columns, column_map, is_active, created_by, created_at, updated_at`,
      [
        payload.vendor_name.trim(),
        payload.description || null,
        payload.required_columns,
        payload.column_map,
        payload.is_active,
        req.user.id,
      ]
    );

    await writeAuditLog(req, {
      action: 'IMPORT_TEMPLATE_CREATED',
      details: { template_id: result.rows[0].id, vendor_name: result.rows[0].vendor_name },
    });
    await writeLedgerEvent(req, {
      eventType: 'IMPORT_TEMPLATE_CREATED',
      entityType: 'IMPORT_TEMPLATE',
      entityId: result.rows[0].id,
      payload: { vendor_name: result.rows[0].vendor_name },
    });

    return res.status(201).json(result.rows[0]);
  } catch (err) {
    if (err && err.code === '23505') {
      return res.status(409).json({ error: 'vendor_name already exists' });
    }
    next(err);
  }
});

app.patch('/import-templates/:id', requireRole('ADMIN'), validateRequest({ params: importTemplateParamSchema }), async (req, res, next) => {
  const templateId = req.params.id;
  const parsed = importTemplateUpdateSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }

  const payload = normalizeTemplatePayload(parsed.data);
  const fields = [];
  const values = [];
  let idx = 1;

  if (Object.prototype.hasOwnProperty.call(payload, 'description')) {
    fields.push(`description = $${idx++}`);
    values.push(payload.description || null);
  }
  if (Object.prototype.hasOwnProperty.call(payload, 'required_columns')) {
    fields.push(`required_columns = $${idx++}`);
    values.push(payload.required_columns);
  }
  if (Object.prototype.hasOwnProperty.call(payload, 'column_map')) {
    fields.push(`column_map = $${idx++}`);
    values.push(payload.column_map);
  }
  if (Object.prototype.hasOwnProperty.call(payload, 'is_active')) {
    fields.push(`is_active = $${idx++}`);
    values.push(payload.is_active);
  }

  if (fields.length === 0) {
    return res.status(400).json({ error: 'No updatable fields provided' });
  }

  values.push(templateId);
  try {
    const result = await pool.query(
      `UPDATE import_vendor_templates
       SET ${fields.join(', ')}, updated_at = NOW()
       WHERE id = $${idx}
       RETURNING id, vendor_name, description, required_columns, column_map, is_active, created_by, created_at, updated_at`,
      values
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Import template not found' });
    }

    await writeAuditLog(req, {
      action: 'IMPORT_TEMPLATE_UPDATED',
      details: { template_id: templateId },
    });
    await writeLedgerEvent(req, {
      eventType: 'IMPORT_TEMPLATE_UPDATED',
      entityType: 'IMPORT_TEMPLATE',
      entityId: templateId,
      payload,
    });

    return res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

app.get('/imports/purchases', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    const result = await pool.query(
      `SELECT *
       FROM purchase_import_batches
       ORDER BY id DESC
       LIMIT 200`
    );
    return res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

app.get('/imports/purchases/:id/reconciliation', requireRole('ADMIN', 'STAFF'), validateRequest({ params: positiveIdParamSchema }), async (req, res, next) => {
  const batchId = req.params.id;

  try {
    const batch = await pool.query('SELECT * FROM purchase_import_batches WHERE id = $1', [batchId]);
    if (batch.rows.length === 0) {
      return res.status(404).json({ error: 'Batch not found' });
    }

    const lines = await pool.query(
      `SELECT pil.*,
              i.name AS matched_item_name,
              i.sku_code AS matched_sku_code,
              COALESCE((
                SELECT SUM(it.quantity)
                FROM inventory_transactions it
                WHERE it.item_id = pil.matched_item_id
                  AND it.type = 'STOCK_IN'
              ), 0) AS stocked_qty
       FROM purchase_import_lines pil
       LEFT JOIN items i ON i.id = pil.matched_item_id
       WHERE pil.batch_id = $1
       ORDER BY pil.line_no`,
      [batchId]
    );

    const rows = lines.rows.map((row) => {
      let coverage_status = 'UNMATCHED';
      if (row.match_status === 'MATCHED' || row.match_status === 'CREATED') {
        const purchased = Number(row.purchased_qty || 0);
        const stocked = Number(row.stocked_qty || 0);
        if (stocked === 0) coverage_status = 'NOT_STOCKED';
        else if (stocked < purchased) coverage_status = 'PARTIAL_STOCKED';
        else coverage_status = 'STOCKED';
      }
      return { ...row, coverage_status };
    });

    const summary = {
      total_rows: rows.length,
      unmatched_rows: rows.filter((r) => r.match_status === 'UNMATCHED').length,
      stocked_rows: rows.filter((r) => r.coverage_status === 'STOCKED').length,
      partial_rows: rows.filter((r) => r.coverage_status === 'PARTIAL_STOCKED').length,
      not_stocked_rows: rows.filter((r) => r.coverage_status === 'NOT_STOCKED').length,
    };

    return res.json({ batch: batch.rows[0], summary, rows });
  } catch (err) {
    next(err);
  }
});

app.get('/print-jobs', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const parsed = printJobQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid query', issues: parsed.error.issues });
  }

  const { limit, offset, status } = parsed.data;
  try {
    const params = [];
    let idx = 1;
    let where = '';
    if (status) {
      where = `WHERE status = $${idx++}`;
      params.push(status);
    }
    if (req.user.role !== 'ADMIN') {
      where = where
        ? `${where} AND requester_user_id = $${idx++}`
        : `WHERE requester_user_id = $${idx++}`;
      params.push(req.user.id);
    }
    params.push(limit);
    params.push(offset);

    const result = await pool.query(
      `SELECT id, label_type, requester_user_id, printer_ip, printer_port, copies, status, attempts, error_message, created_at, updated_at, printed_at
       FROM print_jobs
       ${where}
       ORDER BY id DESC
       LIMIT $${idx} OFFSET $${idx + 1}`,
      params
    );
    return res.json({ limit, offset, rows: result.rows });
  } catch (err) {
    next(err);
  }
});

app.post('/print-jobs/:id/retry', requireRole('ADMIN', 'STAFF'), validateRequest({ params: positiveIdParamSchema }), async (req, res, next) => {
  const jobId = req.params.id;
  try {
    const result = await pool.query('SELECT * FROM print_jobs WHERE id = $1', [jobId]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Print job not found' });
    }
    if (req.user.role !== 'ADMIN' && Number(result.rows[0].requester_user_id) !== Number(req.user.id)) {
      return res.status(403).json({ error: 'Forbidden: cannot retry another user job' });
    }
    const updated = await pool.query(
      `UPDATE print_jobs
       SET status = 'QUEUED', error_message = NULL, updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [jobId]
    );
    await writeAuditLog(req, { action: 'PRINT_JOB_RETRY_QUEUED', details: { print_job_id: jobId } });
    await writeLedgerEvent(req, {
      eventType: 'PRINT_JOB_RETRY_QUEUED',
      entityType: 'PRINT_JOB',
      entityId: jobId,
      payload: {},
    });
    return res.json({ status: 'ok', queued: true, print_job: updated.rows[0] });
  } catch (err) {
    next(err);
  }
});

app.get('/print-worker/status', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    const counts = await pool.query(
      `SELECT status, COUNT(*)::int AS count
       FROM print_jobs
       GROUP BY status`
    );
    const byStatus = { QUEUED: 0, SUCCESS: 0, FAILED: 0 };
    counts.rows.forEach((row) => {
      byStatus[row.status] = row.count;
    });
    return res.json({
      enabled: PRINT_WORKER_ENABLED,
      interval_ms: PRINT_WORKER_INTERVAL_MS,
      batch_size: PRINT_WORKER_BATCH_SIZE,
      running: printWorkerState.running,
      last_run_at: printWorkerState.lastRunAt,
      last_processed: printWorkerState.lastProcessed,
      last_error: printWorkerState.lastError,
      queue_counts: byStatus,
    });
  } catch (err) {
    next(err);
  }
});

app.post('/print-worker/run-once', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  if (printWorkerState.running) {
    return res.status(409).json({ error: 'Print worker is already running' });
  }
  printWorkerState.running = true;
  printWorkerState.lastError = null;
  try {
    const processed = await processQueuedPrintJobs(PRINT_WORKER_BATCH_SIZE);
    printWorkerState.lastRunAt = new Date().toISOString();
    printWorkerState.lastProcessed = processed;
    await writeAuditLog(req, {
      action: 'PRINT_WORKER_RUN_ONCE',
      details: { processed },
    });
    return res.json({ status: 'ok', processed });
  } catch (err) {
    printWorkerState.lastRunAt = new Date().toISOString();
    printWorkerState.lastProcessed = 0;
    printWorkerState.lastError = err && err.message ? err.message : 'Worker run failed';
    await writeAuditLog(req, {
      action: 'PRINT_WORKER_RUN_ONCE_FAILURE',
      severity: 'ERROR',
      details: { error: printWorkerState.lastError },
    });
    return res.status(500).json({ status: 'error', error: printWorkerState.lastError });
  } finally {
    printWorkerState.running = false;
  }
});

/* ---------------------------
   INTER-CLINIC TRANSFERS
---------------------------- */

app.get('/transfers/meta', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  try {
    await ensureTransferSchema();
    const clinics = await pool.query(
      `SELECT id, org_id, code, name
       FROM clinics
       WHERE is_active = TRUE
       ORDER BY name ASC`
    );
    const items = await pool.query(
      `SELECT id, sku_code, name
       FROM items
       ORDER BY id ASC
       LIMIT 1000`
    );
    const scope = await pool.query(
      `SELECT id, role, org_id, home_clinic_id
       FROM users
       WHERE id = $1`,
      [req.user.id]
    );
    return res.json({
      clinics: clinics.rows,
      items: items.rows,
      user_scope: scope.rows[0] || null,
    });
  } catch (err) {
    next(err);
  }
});

app.post('/transfers/requests', requireRole('ADMIN', 'STAFF'), async (req, res, next) => {
  const parsed = transferRequestCreateSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }
  const idempotencyKey = requireTransferActionIdempotencyKey(req, res);
  if (!idempotencyKey) return;

  const { from_clinic_id, to_clinic_id, needed_by, notes, items } = parsed.data;
  const client = await pool.connect();
  try {
    await ensureTransferSchema();
    await client.query('BEGIN');
    await client.query('SET LOCAL TRANSACTION ISOLATION LEVEL SERIALIZABLE');

    const scope = await getUserClinicScope({ client, userId: req.user.id });
    if (req.user.role !== 'ADMIN') {
      const homeClinicId = Number(scope.home_clinic_id || 0);
      if (homeClinicId <= 0 || (homeClinicId !== Number(from_clinic_id) && homeClinicId !== Number(to_clinic_id))) {
        await client.query('ROLLBACK');
        return res.status(403).json({ error: 'Transfer must involve your clinic scope' });
      }
    }

    const replay = await findTransferActionReplay({
      client,
      transferRequestId: null,
      actorUserId: req.user.id,
      actionName: 'CREATE_REQUEST',
      idempotencyKey,
    });
    if (replay) {
      await client.query('ROLLBACK');
      return res.json({ ...replay, idempotent_replay: true });
    }

    const clinics = await client.query(
      `SELECT id, org_id
       FROM clinics
       WHERE id = ANY($1::bigint[])`,
      [[from_clinic_id, to_clinic_id]]
    );
    if (clinics.rows.length !== 2) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'One or more clinics not found' });
    }
    const fromClinic = clinics.rows.find((row) => Number(row.id) === Number(from_clinic_id));
    const toClinic = clinics.rows.find((row) => Number(row.id) === Number(to_clinic_id));
    if (!fromClinic || !toClinic || Number(fromClinic.org_id) !== Number(toClinic.org_id)) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Transfer must be within the same organization' });
    }

    const inserted = await client.query(
      `INSERT INTO transfer_requests
       (org_id, from_clinic_id, to_clinic_id, status, needed_by, requested_by, notes)
       VALUES ($1, $2, $3, 'PENDING', $4, $5, $6)
       RETURNING id`,
      [fromClinic.org_id, from_clinic_id, to_clinic_id, needed_by ? new Date(needed_by) : null, req.user.id, notes || null]
    );
    const transferId = inserted.rows[0].id;

    for (const line of items) {
      await client.query(
        `INSERT INTO transfer_request_lines
         (transfer_request_id, item_id, requested_qty, approved_qty, picked_qty, received_qty, cancelled_qty, line_status)
         VALUES ($1, $2, $3, 0, 0, 0, 0, 'PENDING')`,
        [transferId, line.item_id, line.requested_qty]
      );
    }

    await recomputeTransferStatuses({ client, transferId });
    const payload = await loadTransferWithLines({ client, transferId });
    await saveTransferActionReplay({
      client,
      transferRequestId: transferId,
      actorUserId: req.user.id,
      actionName: 'CREATE_REQUEST',
      idempotencyKey,
      responseJson: payload,
    });

    await client.query('COMMIT');
    await writeAuditLog(req, {
      action: 'TRANSFER_REQUEST_CREATED',
      details: { transfer_request_id: transferId, from_clinic_id, to_clinic_id, line_count: items.length },
    });
    await writeLedgerEvent(req, {
      eventType: 'TRANSFER_REQUEST_CREATED',
      entityType: 'TRANSFER_REQUEST',
      entityId: transferId,
      payload: { from_clinic_id, to_clinic_id, line_count: items.length },
    });
    return res.json(payload);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally {
    client.release();
  }
});

app.get('/transfers/requests', requireRole('ADMIN', 'STAFF'), validateRequest({ query: transferListQuerySchema }), async (req, res, next) => {
  try {
    await ensureTransferSchema();
    const { status, limit, offset } = req.query;
    const scope = await pool.query(
      `SELECT role, home_clinic_id
       FROM users
       WHERE id = $1`,
      [req.user.id]
    );
    if (scope.rows.length === 0) return res.status(404).json({ error: 'User not found' });

    const where = [];
    const params = [];
    let idx = 1;
    if (status) {
      where.push(`tr.status = $${idx++}`);
      params.push(status);
    }
    if (req.user.role !== 'ADMIN') {
      where.push(`(tr.from_clinic_id = $${idx} OR tr.to_clinic_id = $${idx})`);
      params.push(scope.rows[0].home_clinic_id || 0);
      idx += 1;
    }
    const whereSql = where.length > 0 ? `WHERE ${where.join(' AND ')}` : '';
    params.push(limit);
    params.push(offset);

    const rows = await pool.query(
      `SELECT tr.id, tr.status, tr.from_clinic_id, tr.to_clinic_id, tr.needed_by, tr.requested_by, tr.approved_by, tr.created_at, tr.updated_at,
              fc.name AS from_clinic_name,
              tc.name AS to_clinic_name
       FROM transfer_requests tr
       JOIN clinics fc ON fc.id = tr.from_clinic_id
       JOIN clinics tc ON tc.id = tr.to_clinic_id
       ${whereSql}
       ORDER BY tr.id DESC
       LIMIT $${idx} OFFSET $${idx + 1}`,
      params
    );
    return res.json({ rows: rows.rows, limit, offset });
  } catch (err) {
    next(err);
  }
});

app.get('/transfers/requests/:id', requireRole('ADMIN', 'STAFF'), validateRequest({ params: transferActionParamSchema }), async (req, res, next) => {
  const client = await pool.connect();
  try {
    await ensureTransferSchema();
    const scope = await getUserClinicScope({ client, userId: req.user.id });
    const payload = await loadTransferWithLines({ client, transferId: req.params.id });
    if (!payload) return res.status(404).json({ error: 'Transfer request not found' });
    if (!enforceTransferScope({ req, scope, transfer: payload.transfer })) {
      return res.status(403).json({ error: 'Transfer request is outside your clinic scope' });
    }
    return res.json(payload);
  } catch (err) {
    next(err);
  } finally {
    client.release();
  }
});

app.post('/transfers/:id/approve', requireRole('ADMIN'), validateRequest({ params: transferActionParamSchema }), async (req, res, next) => {
  const parsed = transferApproveSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }
  const idempotencyKey = requireTransferActionIdempotencyKey(req, res);
  if (!idempotencyKey) return;

  const transferId = req.params.id;
  const { decision, lines, notes } = parsed.data;
  const client = await pool.connect();
  try {
    await ensureTransferSchema();
    await client.query('BEGIN');
    await client.query('SET LOCAL TRANSACTION ISOLATION LEVEL SERIALIZABLE');
    const payload = await loadTransferWithLines({ client, transferId });
    if (!payload) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Transfer request not found' });
    }
    if (payload.transfer.status !== 'PENDING') {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: `Transfer cannot be approved from status ${payload.transfer.status}` });
    }

    const replay = await findTransferActionReplay({
      client,
      transferRequestId: transferId,
      actorUserId: req.user.id,
      actionName: 'APPROVE',
      idempotencyKey,
    });
    if (replay) {
      await client.query('ROLLBACK');
      return res.json({ ...replay, idempotent_replay: true });
    }

    if (decision === 'REJECT') {
      await client.query(
        `UPDATE transfer_request_lines
         SET approved_qty = 0, line_status = 'REJECTED', updated_at = NOW()
         WHERE transfer_request_id = $1`,
        [transferId]
      );
      await client.query(
        `UPDATE transfer_requests
         SET status = 'REJECTED',
             approved_by = $1,
             approved_at = NOW(),
             notes = COALESCE($2, notes),
             updated_at = NOW()
         WHERE id = $3`,
        [req.user.id, notes || null, transferId]
      );
    } else {
      const map = new Map((lines || []).map((line) => [Number(line.item_id), parseNumeric(line.approved_qty)]));
      for (const line of payload.lines) {
        const requested = parseNumeric(line.requested_qty);
        const approved = map.has(Number(line.item_id))
          ? Math.min(requested, Math.max(0, map.get(Number(line.item_id))))
          : requested;
        const status = approved > 0 ? 'APPROVED' : 'REJECTED';
        await client.query(
          `UPDATE transfer_request_lines
           SET approved_qty = $1, line_status = $2, updated_at = NOW()
           WHERE id = $3`,
          [approved, status, line.id]
        );
      }
      await client.query(
        `UPDATE transfer_requests
         SET status = 'APPROVED',
             approved_by = $1,
             approved_at = NOW(),
             notes = COALESCE($2, notes),
             updated_at = NOW()
         WHERE id = $3`,
        [req.user.id, notes || null, transferId]
      );
      await recomputeTransferStatuses({ client, transferId });
    }

    const updated = await loadTransferWithLines({ client, transferId });
    await saveTransferActionReplay({
      client,
      transferRequestId: transferId,
      actorUserId: req.user.id,
      actionName: 'APPROVE',
      idempotencyKey,
      responseJson: updated,
    });
    await client.query('COMMIT');

    await writeAuditLog(req, {
      action: 'TRANSFER_REQUEST_APPROVED',
      details: { transfer_request_id: Number(transferId), decision, line_count: updated.lines.length },
    });
    await writeLedgerEvent(req, {
      eventType: 'TRANSFER_REQUEST_APPROVED',
      entityType: 'TRANSFER_REQUEST',
      entityId: transferId,
      payload: { decision, notes: notes || null },
    });
    return res.json(updated);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally {
    client.release();
  }
});

app.post('/transfers/:id/pick-pack', requireRole('ADMIN', 'STAFF'), validateRequest({ params: transferActionParamSchema }), async (req, res, next) => {
  const parsed = transferPickPackSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }
  const idempotencyKey = requireTransferActionIdempotencyKey(req, res);
  if (!idempotencyKey) return;

  const transferId = req.params.id;
  const { lines, notes } = parsed.data;
  const client = await pool.connect();
  try {
    await ensureTransferSchema();
    await ensureMovementSchema();
    await beginInventoryTxn(client);
    const scope = await getUserClinicScope({ client, userId: req.user.id });
    const payload = await loadTransferWithLines({ client, transferId });
    if (!payload) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Transfer request not found' });
    }
    if (!enforceTransferScope({ req, scope, transfer: payload.transfer })) {
      await client.query('ROLLBACK');
      return res.status(403).json({ error: 'Transfer request is outside your clinic scope' });
    }
    if (!['APPROVED', 'PARTIALLY_PICKED', 'IN_TRANSIT', 'PARTIALLY_CANCELLED'].includes(payload.transfer.status)) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: `Transfer cannot be picked from status ${payload.transfer.status}` });
    }

    const replay = await findTransferActionReplay({
      client,
      transferRequestId: transferId,
      actorUserId: req.user.id,
      actionName: 'PICK_PACK',
      idempotencyKey,
    });
    if (replay) {
      await client.query('ROLLBACK');
      return res.json({ ...replay, idempotent_replay: true });
    }

    const lineMap = new Map(payload.lines.map((line) => [Number(line.item_id), line]));
    for (const actionLine of lines) {
      const line = lineMap.get(Number(actionLine.item_id));
      if (!line) {
        await client.query('ROLLBACK');
        return res.status(404).json({ error: `Transfer line not found for item ${actionLine.item_id}` });
      }

      const approved = parseNumeric(line.approved_qty);
      const picked = parseNumeric(line.picked_qty);
      const cancelled = parseNumeric(line.cancelled_qty);
      const remainingApproved = Math.max(approved - picked - cancelled, 0);
      const pickQty = parseNumeric(actionLine.quantity);
      if (pickQty <= 0 || pickQty > remainingApproved) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          error: `Pick quantity exceeds remaining approved quantity for item ${actionLine.item_id}`,
          remaining_approved_qty: remainingApproved,
        });
      }

      const itemMasterId = await ensureItemMasterForItem({ client, itemId: line.item_id });
      if (!itemMasterId) {
        await client.query('ROLLBACK');
        return res.status(404).json({ error: `Item ${line.item_id} not found` });
      }

      const onHandClinicQty = await getClinicOnHandQty({
        client,
        clinicId: payload.transfer.from_clinic_id,
        itemMasterId,
      });
      if (onHandClinicQty < pickQty) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          error: `Insufficient stock in source clinic for item ${actionLine.item_id}`,
          on_hand_qty: onHandClinicQty,
          requested_pick_qty: pickQty,
        });
      }

      const decremented = await client.query(
        `UPDATE items
         SET quantity = quantity - $1
         WHERE id = $2
           AND quantity >= $1
         RETURNING id`,
        [pickQty, line.item_id]
      );
      if (decremented.rows.length === 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({ error: `Insufficient global stock for item ${actionLine.item_id}` });
      }

      const movementCorrelation = `transfer-${transferId}-pick-${line.id}-${crypto.randomUUID()}`;
      await insertInventoryMovement({
        client,
        itemId: line.item_id,
        encounterId: null,
        userId: req.user.id,
        actorUserId: req.user.id,
        type: 'ISSUE',
        quantity: pickQty,
        reasonCode: 'TRANSFER_PICK_PACK',
        movementSource: 'TRANSFER',
        correlationId: movementCorrelation,
        idempotencyKey: null,
        locationCode: `CLINIC-${payload.transfer.from_clinic_id}`,
        lotCode: null,
        allowNegative: false,
      });

      await client.query(
        `INSERT INTO stock_ledger
         (org_id, clinic_id, item_master_id, movement_type, qty_delta, reason_code, movement_source, correlation_id, idempotency_key, actor_user_id, allow_negative, metadata, occurred_at)
         VALUES ($1, $2, $3, 'TRANSFER_OUT', $4, 'TRANSFER_PICK_PACK', 'TRANSFER', $5, $6, $7, FALSE, $8, NOW())`,
        [
          payload.transfer.org_id,
          payload.transfer.from_clinic_id,
          itemMasterId,
          -1 * pickQty,
          movementCorrelation,
          `${idempotencyKey}-${line.id}`,
          req.user.id,
          {
            transfer_request_id: Number(transferId),
            transfer_line_id: line.id,
            to_clinic_id: payload.transfer.to_clinic_id,
            in_transit: true,
            notes: notes || null,
          },
        ]
      );

      await client.query(
        `UPDATE transfer_request_lines
         SET picked_qty = picked_qty + $1, updated_at = NOW()
         WHERE id = $2`,
        [pickQty, line.id]
      );
    }

    await recomputeTransferStatuses({ client, transferId });
    await client.query(
      `UPDATE transfer_requests
       SET notes = COALESCE($1, notes), updated_at = NOW()
       WHERE id = $2`,
      [notes || null, transferId]
    );

    const updated = await loadTransferWithLines({ client, transferId });
    await saveTransferActionReplay({
      client,
      transferRequestId: transferId,
      actorUserId: req.user.id,
      actionName: 'PICK_PACK',
      idempotencyKey,
      responseJson: updated,
    });
    await client.query('COMMIT');

    await writeAuditLog(req, {
      action: 'TRANSFER_PICK_PACK',
      details: { transfer_request_id: Number(transferId), line_count: lines.length },
    });
    await writeLedgerEvent(req, {
      eventType: 'TRANSFER_PICK_PACK',
      entityType: 'TRANSFER_REQUEST',
      entityId: transferId,
      payload: { line_count: lines.length },
    });
    return res.json(updated);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally {
    client.release();
  }
});

app.post('/transfers/:id/receive', requireRole('ADMIN', 'STAFF'), validateRequest({ params: transferActionParamSchema }), async (req, res, next) => {
  const parsed = transferReceiveSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }
  const idempotencyKey = requireTransferActionIdempotencyKey(req, res);
  if (!idempotencyKey) return;

  const transferId = req.params.id;
  const { lines, notes } = parsed.data;
  const client = await pool.connect();
  try {
    await ensureTransferSchema();
    await ensureMovementSchema();
    await beginInventoryTxn(client);
    const scope = await getUserClinicScope({ client, userId: req.user.id });
    const payload = await loadTransferWithLines({ client, transferId });
    if (!payload) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Transfer request not found' });
    }
    if (!enforceTransferScope({ req, scope, transfer: payload.transfer })) {
      await client.query('ROLLBACK');
      return res.status(403).json({ error: 'Transfer request is outside your clinic scope' });
    }
    if (!['PARTIALLY_PICKED', 'IN_TRANSIT', 'PARTIALLY_RECEIVED', 'PARTIALLY_CANCELLED'].includes(payload.transfer.status)) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: `Transfer cannot be received from status ${payload.transfer.status}` });
    }

    const replay = await findTransferActionReplay({
      client,
      transferRequestId: transferId,
      actorUserId: req.user.id,
      actionName: 'RECEIVE',
      idempotencyKey,
    });
    if (replay) {
      await client.query('ROLLBACK');
      return res.json({ ...replay, idempotent_replay: true });
    }

    const lineMap = new Map(payload.lines.map((line) => [Number(line.item_id), line]));
    for (const actionLine of lines) {
      const line = lineMap.get(Number(actionLine.item_id));
      if (!line) {
        await client.query('ROLLBACK');
        return res.status(404).json({ error: `Transfer line not found for item ${actionLine.item_id}` });
      }
      const picked = parseNumeric(line.picked_qty);
      const received = parseNumeric(line.received_qty);
      const cancelled = parseNumeric(line.cancelled_qty);
      const remainingReceivable = Math.max(picked - received - cancelled, 0);
      const receiveQty = parseNumeric(actionLine.quantity);
      if (receiveQty <= 0 || receiveQty > remainingReceivable) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          error: `Receive quantity exceeds in-transit quantity for item ${actionLine.item_id}`,
          remaining_receivable_qty: remainingReceivable,
        });
      }

      const itemMasterId = await ensureItemMasterForItem({ client, itemId: line.item_id });
      if (!itemMasterId) {
        await client.query('ROLLBACK');
        return res.status(404).json({ error: `Item ${line.item_id} not found` });
      }

      await client.query(
        `UPDATE items
         SET quantity = quantity + $1
         WHERE id = $2`,
        [receiveQty, line.item_id]
      );
      const movementCorrelation = `transfer-${transferId}-receive-${line.id}-${crypto.randomUUID()}`;
      await insertInventoryMovement({
        client,
        itemId: line.item_id,
        encounterId: null,
        userId: req.user.id,
        actorUserId: req.user.id,
        type: 'STOCK_IN',
        quantity: receiveQty,
        reasonCode: 'TRANSFER_RECEIVE',
        movementSource: 'TRANSFER',
        correlationId: movementCorrelation,
        idempotencyKey: null,
        locationCode: `CLINIC-${payload.transfer.to_clinic_id}`,
        lotCode: null,
        allowNegative: false,
      });
      await client.query(
        `INSERT INTO stock_ledger
         (org_id, clinic_id, item_master_id, movement_type, qty_delta, reason_code, movement_source, correlation_id, idempotency_key, actor_user_id, allow_negative, metadata, occurred_at)
         VALUES ($1, $2, $3, 'TRANSFER_IN', $4, 'TRANSFER_RECEIVE', 'TRANSFER', $5, $6, $7, FALSE, $8, NOW())`,
        [
          payload.transfer.org_id,
          payload.transfer.to_clinic_id,
          itemMasterId,
          receiveQty,
          movementCorrelation,
          `${idempotencyKey}-${line.id}`,
          req.user.id,
          {
            transfer_request_id: Number(transferId),
            transfer_line_id: line.id,
            from_clinic_id: payload.transfer.from_clinic_id,
            notes: notes || null,
          },
        ]
      );
      await client.query(
        `UPDATE transfer_request_lines
         SET received_qty = received_qty + $1, updated_at = NOW()
         WHERE id = $2`,
        [receiveQty, line.id]
      );
    }

    await recomputeTransferStatuses({ client, transferId });
    await client.query(
      `UPDATE transfer_requests
       SET notes = COALESCE($1, notes), updated_at = NOW()
       WHERE id = $2`,
      [notes || null, transferId]
    );
    const updated = await loadTransferWithLines({ client, transferId });
    await saveTransferActionReplay({
      client,
      transferRequestId: transferId,
      actorUserId: req.user.id,
      actionName: 'RECEIVE',
      idempotencyKey,
      responseJson: updated,
    });
    await client.query('COMMIT');

    await writeAuditLog(req, {
      action: 'TRANSFER_RECEIVE',
      details: { transfer_request_id: Number(transferId), line_count: lines.length },
    });
    await writeLedgerEvent(req, {
      eventType: 'TRANSFER_RECEIVE',
      entityType: 'TRANSFER_REQUEST',
      entityId: transferId,
      payload: { line_count: lines.length },
    });
    return res.json(updated);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally {
    client.release();
  }
});

app.post('/transfers/:id/cancel', requireRole('ADMIN', 'STAFF'), validateRequest({ params: transferActionParamSchema }), async (req, res, next) => {
  const parsed = transferCancelSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }
  const idempotencyKey = requireTransferActionIdempotencyKey(req, res);
  if (!idempotencyKey) return;

  const transferId = req.params.id;
  const { lines, reason } = parsed.data;
  const client = await pool.connect();
  try {
    await ensureTransferSchema();
    await ensureMovementSchema();
    await beginInventoryTxn(client);
    const scope = await getUserClinicScope({ client, userId: req.user.id });
    const payload = await loadTransferWithLines({ client, transferId });
    if (!payload) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Transfer request not found' });
    }
    if (!enforceTransferScope({ req, scope, transfer: payload.transfer })) {
      await client.query('ROLLBACK');
      return res.status(403).json({ error: 'Transfer request is outside your clinic scope' });
    }
    if (['CANCELLED', 'REJECTED', 'RECEIVED'].includes(payload.transfer.status)) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: `Transfer cannot be cancelled from status ${payload.transfer.status}` });
    }

    const replay = await findTransferActionReplay({
      client,
      transferRequestId: transferId,
      actorUserId: req.user.id,
      actionName: 'CANCEL',
      idempotencyKey,
    });
    if (replay) {
      await client.query('ROLLBACK');
      return res.json({ ...replay, idempotent_replay: true });
    }

    const requestMap = new Map((lines || []).map((line) => [Number(line.item_id), parseNumeric(line.quantity)]));
    const targetLines = lines && lines.length > 0
      ? payload.lines.filter((line) => requestMap.has(Number(line.item_id)))
      : payload.lines;
    if (targetLines.length === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'No transfer lines selected for cancellation' });
    }

    for (const line of targetLines) {
      const approved = parseNumeric(line.approved_qty);
      const picked = parseNumeric(line.picked_qty);
      const received = parseNumeric(line.received_qty);
      const cancelled = parseNumeric(line.cancelled_qty);
      const unpickedCancelable = Math.max(approved - picked - cancelled, 0);
      const inTransitCancelable = Math.max(picked - received - cancelled, 0);
      const maxCancelable = unpickedCancelable + inTransitCancelable;
      const requestedCancel = lines && lines.length > 0 ? requestMap.get(Number(line.item_id)) : maxCancelable;
      const cancelQty = Math.min(maxCancelable, Math.max(0, parseNumeric(requestedCancel)));
      if (cancelQty <= 0) continue;

      const fromInTransitQty = Math.min(inTransitCancelable, cancelQty);
      if (fromInTransitQty > 0) {
        const itemMasterId = await ensureItemMasterForItem({ client, itemId: line.item_id });
        if (!itemMasterId) {
          await client.query('ROLLBACK');
          return res.status(404).json({ error: `Item ${line.item_id} not found` });
        }

        await client.query(
          `UPDATE items
           SET quantity = quantity + $1
           WHERE id = $2`,
          [fromInTransitQty, line.item_id]
        );
        const movementCorrelation = `transfer-${transferId}-cancel-${line.id}-${crypto.randomUUID()}`;
        await insertInventoryMovement({
          client,
          itemId: line.item_id,
          encounterId: null,
          userId: req.user.id,
          actorUserId: req.user.id,
          type: 'STOCK_IN',
          quantity: fromInTransitQty,
          reasonCode: 'TRANSFER_CANCEL_REVERSAL',
          movementSource: 'TRANSFER',
          correlationId: movementCorrelation,
          idempotencyKey: null,
          locationCode: `CLINIC-${payload.transfer.from_clinic_id}`,
          lotCode: null,
          allowNegative: false,
        });
        await client.query(
          `INSERT INTO stock_ledger
           (org_id, clinic_id, item_master_id, movement_type, qty_delta, reason_code, movement_source, correlation_id, idempotency_key, actor_user_id, allow_negative, metadata, occurred_at)
           VALUES ($1, $2, $3, 'TRANSFER_IN', $4, 'TRANSFER_CANCEL_REVERSAL', 'TRANSFER', $5, $6, $7, FALSE, $8, NOW())`,
          [
            payload.transfer.org_id,
            payload.transfer.from_clinic_id,
            itemMasterId,
            fromInTransitQty,
            movementCorrelation,
            `${idempotencyKey}-${line.id}`,
            req.user.id,
            {
              transfer_request_id: Number(transferId),
              transfer_line_id: line.id,
              cancelled_from_in_transit: true,
              reason: reason || null,
            },
          ]
        );
      }

      await client.query(
        `UPDATE transfer_request_lines
         SET cancelled_qty = cancelled_qty + $1, updated_at = NOW()
         WHERE id = $2`,
        [cancelQty, line.id]
      );
    }

    await recomputeTransferStatuses({ client, transferId });
    await client.query(
      `UPDATE transfer_requests
       SET notes = COALESCE($1, notes), updated_at = NOW()
       WHERE id = $2`,
      [reason || null, transferId]
    );
    const updated = await loadTransferWithLines({ client, transferId });
    await saveTransferActionReplay({
      client,
      transferRequestId: transferId,
      actorUserId: req.user.id,
      actionName: 'CANCEL',
      idempotencyKey,
      responseJson: updated,
    });
    await client.query('COMMIT');

    await writeAuditLog(req, {
      action: 'TRANSFER_CANCEL',
      details: { transfer_request_id: Number(transferId), line_count: targetLines.length, reason: reason || null },
    });
    await writeLedgerEvent(req, {
      eventType: 'TRANSFER_CANCEL',
      entityType: 'TRANSFER_REQUEST',
      entityId: transferId,
      payload: { line_count: targetLines.length, reason: reason || null },
    });
    return res.json(updated);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally {
    client.release();
  }
});

app.post('/ai/assistant', requireRole('ADMIN', 'STAFF'), async (req, res) => {
  const parsed = aiAssistantSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }

  const answer = buildAssistantResponse(parsed.data.message, req.user.role);
  await writeAuditLog(req, {
    action: 'AI_ASSISTANT_QUERY',
    details: { message_preview: parsed.data.message.slice(0, 120) },
  });

  return res.json({ role: req.user.role, ...answer });
});

/* ---------------------------
   SUPERVISOR AUDIT + FRAUD
---------------------------- */

app.get('/audit-logs', requireRole('ADMIN'), async (req, res, next) => {
  const parsed = auditLogQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid query', issues: parsed.error.issues });
  }

  const { limit, offset, action, severity, actor_user_id } = parsed.data;

  try {
    const conditions = [];
    const params = [];
    let idx = 1;

    if (action) {
      conditions.push(`action = $${idx++}`);
      params.push(action);
    }

    if (severity) {
      conditions.push(`severity = $${idx++}`);
      params.push(severity);
    }

    if (actor_user_id) {
      conditions.push(`actor_user_id = $${idx++}`);
      params.push(actor_user_id);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    params.push(limit);
    params.push(offset);

    const result = await pool.query(
      `SELECT *
       FROM audit_logs
       ${where}
       ORDER BY id DESC
       LIMIT $${idx} OFFSET $${idx + 1}`,
      params
    );

    return res.json({ limit, offset, rows: result.rows });
  } catch (err) {
    next(err);
  }
});

app.get('/fraud-flags', requireRole('ADMIN'), async (req, res, next) => {
  const parsed = fraudFlagQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid query', issues: parsed.error.issues });
  }

  const { limit, offset, status, min_score } = parsed.data;

  try {
    const conditions = [];
    const params = [];
    let idx = 1;

    if (status) {
      conditions.push(`status = $${idx++}`);
      params.push(status);
    }

    if (typeof min_score === 'number') {
      conditions.push(`score >= $${idx++}`);
      params.push(min_score);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    params.push(limit);
    params.push(offset);

    const result = await pool.query(
      `SELECT *
       FROM fraud_flags
       ${where}
       ORDER BY id DESC
       LIMIT $${idx} OFFSET $${idx + 1}`,
      params
    );

    return res.json({ limit, offset, rows: result.rows });
  } catch (err) {
    next(err);
  }
});

app.patch('/fraud-flags/:id/status', requireRole('ADMIN'), validateRequest({ params: fraudFlagParamSchema }), async (req, res, next) => {
  const flagId = req.params.id;
  const parsed = fraudFlagUpdateSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', issues: parsed.error.issues });
  }
  try {
    const result = await pool.query(
      `UPDATE fraud_flags
       SET status = $1, updated_at = NOW()
       WHERE id = $2
       RETURNING *`,
      [parsed.data.status, flagId]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'Fraud flag not found' });

    await writeAuditLog(req, {
      action: 'FRAUD_FLAG_STATUS_UPDATED',
      details: { fraud_flag_id: flagId, status: parsed.data.status },
    });
    await writeLedgerEvent(req, {
      eventType: 'FRAUD_FLAG_STATUS_UPDATED',
      entityType: 'FRAUD_FLAG',
      entityId: flagId,
      payload: { status: parsed.data.status },
    });

    return res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

app.post('/fraud-flags/run-analysis', requireRole('ADMIN'), async (req, res, next) => {
  try {
    const insertedFlags = await runFraudAnalysis();

    await writeAuditLog(req, {
      action: 'RUN_FRAUD_ANALYSIS',
      details: { inserted_count: insertedFlags.length },
    });

    return res.json({
      status: 'ok',
      inserted_count: insertedFlags.length,
      inserted_flags: insertedFlags,
    });
  } catch (err) {
    next(err);
  }
});

app.get('/event-ledger', requireRole('ADMIN'), validateRequest({ query: eventLedgerQuerySchema }), async (req, res, next) => {
  try {
    const { limit, offset } = req.query;

    const result = await pool.query(
      `SELECT id, actor_user_id, actor_role, event_type, entity_type, entity_id, payload, previous_hash, event_hash, created_at
       FROM event_ledger
       ORDER BY id DESC
       LIMIT $1 OFFSET $2`,
      [limit, offset]
    );
    return res.json({ limit, offset, rows: result.rows });
  } catch (err) {
    next(err);
  }
});

/* ---------------------------
   SERVER START
---------------------------- */

// Centralized error handler
app.use((err, req, res, next) => {
  const logPayload = isProduction
    ? (err && err.message ? err.message : 'Internal error')
    : (err && err.stack ? err.stack : err);
  console.error(logPayload);
  writeAuditLog(req, {
    action: 'UNHANDLED_ERROR',
    severity: 'ERROR',
    details: { message: err && err.message ? err.message : 'Internal error' },
  }).catch(() => {});
  res.status(500).json({ error: 'Internal Server Error' });
});

const PORT = Number(process.env.PORT || 5000);

const startBackgroundWorkers = () => {
  if (PRINT_WORKER_ENABLED) {
    printWorkerState.timer = setInterval(async () => {
      if (printWorkerState.running) return;
      printWorkerState.running = true;
      printWorkerState.lastError = null;
      try {
        const processed = await processQueuedPrintJobs(PRINT_WORKER_BATCH_SIZE);
        printWorkerState.lastRunAt = new Date().toISOString();
        printWorkerState.lastProcessed = processed;
      } catch (err) {
        printWorkerState.lastRunAt = new Date().toISOString();
        printWorkerState.lastProcessed = 0;
        printWorkerState.lastError = err && err.message ? err.message : 'Worker run failed';
        console.error('PRINT WORKER ERROR:', printWorkerState.lastError);
      } finally {
        printWorkerState.running = false;
      }
    }, PRINT_WORKER_INTERVAL_MS);
    if (typeof printWorkerState.timer.unref === 'function') {
      printWorkerState.timer.unref();
    }
    console.log(
      `Print worker enabled (interval=${PRINT_WORKER_INTERVAL_MS}ms, batch=${PRINT_WORKER_BATCH_SIZE})`
    );
  } else {
    console.log('Print worker disabled via PRINT_WORKER_ENABLED=false');
  }

  if (EXPIRY_ALERT_WORKER_ENABLED) {
    expiryAlertWorkerState.timer = setInterval(async () => {
      if (expiryAlertWorkerState.running) return;
      expiryAlertWorkerState.running = true;
      expiryAlertWorkerState.lastError = null;
      try {
        const processed = await processDailyExpiryDigests();
        expiryAlertWorkerState.lastRunAt = new Date().toISOString();
        expiryAlertWorkerState.lastProcessed = processed;
      } catch (err) {
        expiryAlertWorkerState.lastRunAt = new Date().toISOString();
        expiryAlertWorkerState.lastProcessed = 0;
        expiryAlertWorkerState.lastError = err && err.message ? err.message : 'Expiry alert worker failed';
        console.error('EXPIRY ALERT WORKER ERROR:', expiryAlertWorkerState.lastError);
      } finally {
        expiryAlertWorkerState.running = false;
      }
    }, EXPIRY_ALERT_WORKER_INTERVAL_MS);
    if (typeof expiryAlertWorkerState.timer.unref === 'function') {
      expiryAlertWorkerState.timer.unref();
    }
    console.log(`Expiry alert worker enabled (interval=${EXPIRY_ALERT_WORKER_INTERVAL_MS}ms)`);
  } else {
    console.log('Expiry alert worker disabled via EXPIRY_ALERT_WORKER_ENABLED=false');
  }
};

if (require.main === module) {
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Server running on http://0.0.0.0:${PORT}`);
    startBackgroundWorkers();
  });
}

module.exports = app;

