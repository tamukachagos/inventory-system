import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  CssBaseline,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  Drawer,
  Fab,
  Grid,
  LinearProgress,
  MenuItem,
  Select,
  Skeleton,
  Snackbar,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
  useMediaQuery,
  useTheme,
} from '@mui/material';
import { AnimatePresence, motion } from 'framer-motion';
import { Activity, AlertTriangle, ArrowDownRight, ArrowRightLeft, ArrowUpRight, Bot, ClipboardCheck, Download, FileText, Home, PackagePlus, Printer, ScanLine, ShoppingCart, Warehouse } from 'lucide-react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip as ChartTooltip,
  XAxis,
  YAxis,
} from 'recharts';
import './App.css';
import AppLayout from './AppLayout';
import BarcodeInput from './BarcodeInput';
import {
  clearConflictItem,
  enqueueOfflineAction,
  getDeviceId,
  listConflictItems,
  listOutboxActions,
  normalizeWriteActionForQueue,
  runSyncCycle,
} from './syncEngine';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || `${window.location.protocol}//${window.location.hostname}:5000`;
const DRAWER_WIDTH = 280;
const MotionCard = motion(Card);
const APP_NAME = 'CrimsonSupply Nexus';
const APP_TAGLINE = 'Clinical Inventory Intelligence Platform';

const initialForms = {
  login: { student_card: '', password: '' },
  password: { current_password: '', new_password: '' },
  checkIn: {
    appointment_id: '',
    provider_card: '',
    status: 'ACTIVE',
    print_label: false,
    printer_ip: '',
    printer_port: '9100',
    copies: '1',
  },
  assistantScan: { encounter_code: '', assistant_card: '' },
  stockInScan: { item_barcode: '', item_name: '', quantity: '1', cost: '', lot_code: '', expiry_date: '' },
  issueScan: { encounter_code: '', item_barcode: '', operator_card: '', quantity: '1' },
  returnScan: { encounter_code: '', item_barcode: '', operator_card: '', quantity: '1' },
  cycleRandom: { item_count: '20', notes: '' },
  assistant: { message: '' },
  transferRequest: {
    from_clinic_id: '',
    to_clinic_id: '',
    item_id: '',
    requested_qty: '1',
    needed_by: '',
    notes: '',
  },
  transferApprove: { decision: 'APPROVE', item_id: '', approved_qty: '' },
  transferPick: { item_id: '', quantity: '1', notes: '' },
  transferReceive: { item_id: '', quantity: '1', notes: '' },
  transferCancel: { item_id: '', quantity: '1', reason: '' },
};

const roleCapabilityFallback = {
  ADMIN: ['CHECKIN', 'STOCK_IN', 'ISSUE_ITEM', 'RETURN_ITEM', 'CYCLE_COUNT', 'PRINT_QUEUE', 'AI_ASSISTANT', 'TRANSFER', 'REORDER_PLANNER'],
  STAFF: ['CHECKIN', 'STOCK_IN', 'ISSUE_ITEM', 'RETURN_ITEM', 'CYCLE_COUNT', 'PRINT_QUEUE', 'AI_ASSISTANT', 'TRANSFER', 'REORDER_PLANNER'],
};

const EmptyState = ({ title, subtitle }) => (
  <Card variant="outlined" sx={{ borderStyle: 'dashed' }}>
    <CardContent>
      <Typography variant="h6">{title}</Typography>
      <Typography color="text.secondary">{subtitle}</Typography>
    </CardContent>
  </Card>
);

const AI_DISCLAIMER = 'AI suggestions are advisory. Any data-changing action requires explicit confirmation.';
const TypingIndicator = () => (
  <span className="typing-indicator" aria-label="AI is typing" role="status">
    <span />
    <span />
    <span />
  </span>
);

function App({ colorMode = 'light', onToggleColorMode = () => {} }) {
  const [token, setToken] = useState(localStorage.getItem('inventory_token') || '');
  const [refreshToken, setRefreshToken] = useState(localStorage.getItem('inventory_refresh_token') || '');
  const [user, setUser] = useState(() => {
    const raw = localStorage.getItem('inventory_user');
    return raw ? JSON.parse(raw) : null;
  });
  const [forms, setForms] = useState(initialForms);
  const [capabilities, setCapabilities] = useState(() => user?.role ? roleCapabilityFallback[user.role] || [] : []);
  const [view, setView] = useState('dashboard');
  const [mobileOpen, setMobileOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState('');
  const [error, setError] = useState('');
  const [toast, setToast] = useState({ open: false, message: '', severity: 'success' });
  const [fieldErrors, setFieldErrors] = useState({});
  const [logoutConfirmOpen, setLogoutConfirmOpen] = useState(false);

  const [kpis, setKpis] = useState({ inventory_value: 0, active_encounters: 0, daily_usage: 0, low_stock_alerts: 0 });
  const [stockLevels, setStockLevels] = useState([]);
  const [providers, setProviders] = useState([]);
  const [reportData, setReportData] = useState({
    valuation_trend: [],
    daily_usage: [],
    cost_per_encounter: [],
    stock_aging: [],
    low_stock_heatmap: [],
    transaction_velocity: [],
  });
  const [expiryDashboard, setExpiryDashboard] = useState({
    summary: { expired_on_hand: 0, expiring_30: 0, expiring_60: 0, expiring_90: 0 },
    expiring_rows: [],
    slow_moving_near_expiry: [],
  });
  const [expirySuggestions, setExpirySuggestions] = useState([]);
  const [alertDigests, setAlertDigests] = useState([]);
  const [reorderMeta, setReorderMeta] = useState({ clinics: [] });
  const [reorderFilters, setReorderFilters] = useState({ clinic_id: '', location_code: 'MAIN' });
  const [reorderRows, setReorderRows] = useState([]);
  const [syncStatus, setSyncStatus] = useState({ online: typeof navigator !== 'undefined' ? navigator.onLine : true, lastRun: null, message: '' });
  const [syncOutboxRows, setSyncOutboxRows] = useState([]);
  const [syncConflictRows, setSyncConflictRows] = useState([]);
  const [intelligenceRows, setIntelligenceRows] = useState([]);
  const [reportFilters, setReportFilters] = useState(() => {
    const to = new Date();
    const from = new Date(to.getTime() - 29 * 24 * 60 * 60 * 1000);
    return {
      from: from.toISOString().slice(0, 10),
      to: to.toISOString().slice(0, 10),
      provider_user_id: '',
    };
  });
  const [cyclePickRows, setCyclePickRows] = useState([]);
  const [transferMeta, setTransferMeta] = useState({ clinics: [], items: [], user_scope: null });
  const [transferRows, setTransferRows] = useState([]);
  const [selectedTransferId, setSelectedTransferId] = useState('');
  const [transferDetail, setTransferDetail] = useState(null);
  const [assistantMessages, setAssistantMessages] = useState([]);
  const [printJobs, setPrintJobs] = useState([]);
  const [lastLabel, setLastLabel] = useState(null);
  const [copilotOpen, setCopilotOpen] = useState(false);
  const [copilotInput, setCopilotInput] = useState('');
  const [copilotMessages, setCopilotMessages] = useState([
    { role: 'assistant', content: 'Copilot online. Ask for stock insights, encounter costs, restock suggestions, or workflow navigation.', streaming: false, action: null },
  ]);
  const [copilotStreaming, setCopilotStreaming] = useState(false);
  const [aiUsageLog, setAiUsageLog] = useState([]);
  const [pendingAction, setPendingAction] = useState(null);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const skipNextScrollRestoreRef = useRef(false);

  const theme = useTheme();
  const isMobile = useMediaQuery('(max-width:1024px)');
  const prefersReducedMotion = useMediaQuery('(prefers-reduced-motion: reduce)');
  const isAuthenticated = Boolean(token);
  const mustResetPassword = Boolean(user?.must_reset_password);
  const authHeaders = useMemo(() => (token ? { Authorization: `Bearer ${token}` } : {}), [token]);
  const clearSession = useCallback(() => {
    setToken('');
    setRefreshToken('');
    setUser(null);
    localStorage.removeItem('inventory_token');
    localStorage.removeItem('inventory_refresh_token');
    localStorage.removeItem('inventory_user');
  }, []);

  const hasCapability = (name) => capabilities.includes(name);
  const hasInventoryOps = ['STOCK_IN', 'ISSUE_ITEM', 'RETURN_ITEM', 'CYCLE_COUNT'].some((c) => hasCapability(c));
  const checkInOnly = hasCapability('CHECKIN') && !hasInventoryOps;

  const nav = [
    { id: 'dashboard', label: 'Dashboard', icon: Home, cap: 'CHECKIN' },
    { id: 'checkin', label: 'Check-In', icon: ScanLine, cap: 'CHECKIN' },
    { id: 'stock', label: 'Stock In', icon: PackagePlus, cap: 'STOCK_IN' },
    { id: 'issue', label: 'Issue', icon: Warehouse, cap: 'ISSUE_ITEM' },
    { id: 'return', label: 'Return', icon: Activity, cap: 'RETURN_ITEM' },
    { id: 'cycle', label: 'Cycle Count', icon: ClipboardCheck, cap: 'CYCLE_COUNT' },
    { id: 'transfers', label: 'Transfers', icon: ArrowRightLeft, cap: 'TRANSFER' },
    { id: 'reorder', label: 'Reorder Planner', icon: ShoppingCart, cap: 'REORDER_PLANNER' },
    { id: 'sync', label: 'Sync Queue', icon: Activity, cap: 'CHECKIN' },
    { id: 'print', label: 'Print Queue', icon: Printer, cap: 'PRINT_QUEUE' },
    { id: 'assistant', label: 'AI Assistant', icon: Bot, cap: 'AI_ASSISTANT' },
  ].filter((item) => hasCapability(item.cap));
  const navIds = useMemo(() => nav.map((item) => item.id), [nav]);

  const setField = (section, field, value) => {
    setForms((prev) => ({ ...prev, [section]: { ...prev[section], [field]: value } }));
    setFieldErrors((prev) => {
      if (!prev[`${section}.${field}`]) return prev;
      const next = { ...prev };
      delete next[`${section}.${field}`];
      return next;
    });
  };
  const toNumber = (value) => (value === '' ? undefined : Number(value));
  const getFieldError = (path) => fieldErrors[path] || '';

  const addUsageLog = (entry) => {
    setAiUsageLog((prev) => [{ ...entry, ts: new Date().toISOString() }, ...prev].slice(0, 100));
  };

  useEffect(() => {
    if (status) {
      setToast({ open: true, message: status, severity: 'success' });
    }
  }, [status]);

  useEffect(() => {
    if (error) {
      setToast({ open: true, message: error, severity: 'error' });
    }
  }, [error]);

  const refreshSession = useCallback(async () => {
    if (!refreshToken) {
      throw new Error('Session expired. Please login again.');
    }
    const response = await fetch(`${API_BASE_URL}/auth/refresh`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refresh_token: refreshToken }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload?.token || !payload?.refresh_token) {
      clearSession();
      throw new Error(payload?.error || 'Session expired. Please login again.');
    }
    setToken(payload.token);
    setRefreshToken(payload.refresh_token);
    localStorage.setItem('inventory_token', payload.token);
    localStorage.setItem('inventory_refresh_token', payload.refresh_token);
    return payload.token;
  }, [clearSession, refreshToken]);

  const apiRequest = useCallback(async (path, options = {}, retrying = false) => {
    const { headers: optionHeaders, ...rest } = options;
    const method = String(rest.method || 'GET').toUpperCase();
    const writeMethods = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);
    const headers = { 'Content-Type': 'application/json', ...(optionHeaders || {}) };
    if (writeMethods.has(method) && !headers['Idempotency-Key'] && !headers['idempotency-key']) {
      headers['Idempotency-Key'] = window.crypto?.randomUUID ? window.crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }

    if (
      writeMethods.has(method)
      && !retrying
      && !navigator.onLine
      && path !== '/login'
      && path !== '/auth/refresh'
      && !path.startsWith('/sync/')
    ) {
      let normalizedBody = rest.body || {};
      if (typeof rest.body === 'string') {
        try {
          normalizedBody = JSON.parse(rest.body || '{}');
        } catch {
          normalizedBody = {};
        }
      }
      const queued = normalizeWriteActionForQueue({ path, method, body: normalizedBody, headers });
      await enqueueOfflineAction(queued);
      setSyncStatus({ online: false, lastRun: new Date().toISOString(), message: 'Action queued offline' });
      const outbox = await listOutboxActions();
      setSyncOutboxRows(outbox);
      return { status: 'queued_offline', queued: true };
    }

    const response = await fetch(`${API_BASE_URL}${path}`, {
      ...rest,
      method,
      headers,
    });
    const payload = await response.json().catch(() => ({}));

    if (response.status === 401 && !retrying && path !== '/login' && path !== '/auth/refresh') {
      const newAccessToken = await refreshSession();
      return apiRequest(
        path,
        {
          ...options,
          headers: {
            ...(optionHeaders || {}),
            Authorization: `Bearer ${newAccessToken}`,
          },
        },
        true
      );
    }

    if (!response.ok) throw new Error(payload?.error || `HTTP ${response.status}`);
    return payload;
  }, [refreshSession]);

  const withLoading = async (fn) => {
    setLoading(true);
    setError('');
    try {
      await fn();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const refreshSyncQueues = useCallback(async () => {
    const [outbox, conflicts] = await Promise.all([listOutboxActions(), listConflictItems()]);
    setSyncOutboxRows(outbox);
    setSyncConflictRows(conflicts);
  }, []);

  const runSyncNow = useCallback(async () => {
    if (!token) return;
    try {
      const result = await runSyncCycle({ apiBaseUrl: API_BASE_URL, token });
      setSyncStatus({
        online: navigator.onLine,
        lastRun: new Date().toISOString(),
        message: `Sync complete. pushed=${result.pushed}, conflicts=${result.conflicts}, pulled=${result.pulled}`,
      });
      await refreshSyncQueues();
    } catch (err) {
      setSyncStatus({
        online: navigator.onLine,
        lastRun: new Date().toISOString(),
        message: `Sync failed: ${err.message}`,
      });
    }
  }, [refreshSyncQueues, token]);

  const loadDashboard = useCallback(async () => withLoading(async () => {
    const params = new URLSearchParams({
      from: reportFilters.from,
      to: reportFilters.to,
    });
    if (reportFilters.provider_user_id) params.set('provider_user_id', reportFilters.provider_user_id);

    const [kpiRes, reportRes, stockRes, providerRes, intelligenceRes, expiryRes, suggestionRes, digestRes] = await Promise.all([
      apiRequest(`/dashboard/kpis?${params.toString()}`, { headers: authHeaders }).catch(() => ({
        inventory_value: 0,
        active_encounters: 0,
        daily_usage: 0,
        low_stock_alerts: 0,
        deltas: { daily_usage_pct_vs_prev_window: 0, encounter_cost_pct_vs_prev_window: 0 },
      })),
      apiRequest(`/dashboard/report?${params.toString()}`, { headers: authHeaders }).catch(() => ({
        valuation_trend: [],
        daily_usage: [],
        cost_per_encounter: [],
        stock_aging: [],
        low_stock_heatmap: [],
        transaction_velocity: [],
      })),
      apiRequest('/stock-levels', { headers: authHeaders }).catch(() => []),
      apiRequest('/dashboard/providers', { headers: authHeaders }).catch(() => []),
      apiRequest(`/inventory-intelligence?provider_user_id=${encodeURIComponent(reportFilters.provider_user_id || '')}`, { headers: authHeaders }).catch(() => ({ rows: [] })),
      apiRequest('/dashboard/expiry', { headers: authHeaders }).catch(() => ({
        summary: { expired_on_hand: 0, expiring_30: 0, expiring_60: 0, expiring_90: 0 },
        expiring_rows: [],
        slow_moving_near_expiry: [],
      })),
      apiRequest('/expiry-transfer-suggestions?days=30', { headers: authHeaders }).catch(() => ({ rows: [] })),
      apiRequest('/alerts/digests', { headers: authHeaders }).catch(() => ({ rows: [] })),
    ]);

    setKpis(kpiRes);
    setReportData(reportRes);
    setStockLevels(Array.isArray(stockRes) ? stockRes : []);
    setProviders(Array.isArray(providerRes) ? providerRes : []);
    setIntelligenceRows(Array.isArray(intelligenceRes?.rows) ? intelligenceRes.rows : []);
    setExpiryDashboard({
      summary: expiryRes?.summary || { expired_on_hand: 0, expiring_30: 0, expiring_60: 0, expiring_90: 0 },
      expiring_rows: Array.isArray(expiryRes?.expiring_rows) ? expiryRes.expiring_rows : [],
      slow_moving_near_expiry: Array.isArray(expiryRes?.slow_moving_near_expiry) ? expiryRes.slow_moving_near_expiry : [],
    });
    setExpirySuggestions(Array.isArray(suggestionRes?.rows) ? suggestionRes.rows : []);
    setAlertDigests(Array.isArray(digestRes?.rows) ? digestRes.rows : []);
  }), [apiRequest, authHeaders, reportFilters.from, reportFilters.provider_user_id, reportFilters.to]);

  useEffect(() => {
    if (!token) return;
    const run = async () => {
      try {
        const payload = await apiRequest('/me/capabilities', { headers: authHeaders });
        setCapabilities(payload.capabilities || []);
      } catch {
        setCapabilities(user?.role ? roleCapabilityFallback[user.role] || [] : []);
      }
    };
    run();
  }, [token, authHeaders, user?.role, apiRequest]);

  useEffect(() => {
    if (!isAuthenticated) return;
    refreshSyncQueues();
    const onlineHandler = () => setSyncStatus((prev) => ({ ...prev, online: true }));
    const offlineHandler = () => setSyncStatus((prev) => ({ ...prev, online: false }));
    window.addEventListener('online', onlineHandler);
    window.addEventListener('offline', offlineHandler);
    return () => {
      window.removeEventListener('online', onlineHandler);
      window.removeEventListener('offline', offlineHandler);
    };
  }, [isAuthenticated, refreshSyncQueues]);

  useEffect(() => {
    if (!isAuthenticated) return undefined;
    if (!navigator.onLine) return undefined;
    runSyncNow();
    const timer = window.setInterval(() => {
      if (!navigator.onLine) return;
      runSyncNow();
    }, 15000);
    return () => window.clearInterval(timer);
  }, [isAuthenticated, runSyncNow]);

  useEffect(() => {
    if (isAuthenticated && !mustResetPassword) loadDashboard();
  }, [isAuthenticated, mustResetPassword, loadDashboard]);

  useLayoutEffect(() => {
    if (!isAuthenticated || mustResetPassword) return;
    if (skipNextScrollRestoreRef.current) {
      skipNextScrollRestoreRef.current = false;
      return;
    }
    window.scrollTo({ top: 0, left: 0, behavior: 'auto' });
    document.documentElement.scrollTop = 0;
    document.body.scrollTop = 0;
  }, [isAuthenticated, mustResetPassword, view]);

  useEffect(() => {
    if (!isAuthenticated || mustResetPassword) return;
    const hashView = (window.location.hash || '').replace('#', '').trim();
    if (hashView && navIds.includes(hashView) && hashView !== view) {
      setView(hashView);
      return;
    }
    window.history.replaceState({ view }, '', `#${view}`);
  }, [isAuthenticated, mustResetPassword, navIds, view]);

  useEffect(() => {
    if (!isAuthenticated || mustResetPassword) return undefined;
    const handlePopState = (event) => {
      const next = event?.state?.view;
      if (!next || !navIds.includes(next)) return;
      skipNextScrollRestoreRef.current = true;
      setView(next);
    };
    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, [isAuthenticated, mustResetPassword, navIds]);

  const navigateView = useCallback((nextView) => {
    if (!navIds.includes(nextView) || nextView === view) {
      if (isMobile) setMobileOpen(false);
      return;
    }
    window.history.pushState({ view: nextView }, '', `#${nextView}`);
    setView(nextView);
    if (isMobile) setMobileOpen(false);
  }, [isMobile, navIds, view]);


  const validateLoginForm = () => {
    const nextErrors = {};
    if (!forms.login.student_card.trim()) nextErrors['login.student_card'] = 'ID card is required.';
    if (!forms.login.password.trim()) nextErrors['login.password'] = 'Password is required.';
    setFieldErrors((prev) => ({ ...prev, ...nextErrors }));
    return Object.keys(nextErrors).length === 0;
  };

  const validatePasswordForm = () => {
    const nextErrors = {};
    if (!forms.password.current_password.trim()) nextErrors['password.current_password'] = 'Current password is required.';
    if (!forms.password.new_password.trim()) nextErrors['password.new_password'] = 'New password is required.';
    if (forms.password.new_password && forms.password.new_password.length < 8) {
      nextErrors['password.new_password'] = 'Use at least 8 characters.';
    }
    setFieldErrors((prev) => ({ ...prev, ...nextErrors }));
    return Object.keys(nextErrors).length === 0;
  };

  const validateCheckInForm = () => {
    const nextErrors = {};
    if (!forms.checkIn.appointment_id.trim()) nextErrors['checkIn.appointment_id'] = 'Appointment ID is required.';
    if (!forms.checkIn.provider_card.trim()) nextErrors['checkIn.provider_card'] = 'Provider ID card is required.';
    setFieldErrors((prev) => ({ ...prev, ...nextErrors }));
    return Object.keys(nextErrors).length === 0;
  };

  const login = async (event) => {
    event.preventDefault();
    if (!validateLoginForm()) return;
    await withLoading(async () => {
      const payload = await apiRequest('/login', { method: 'POST', body: JSON.stringify(forms.login) });
      setToken(payload.token);
      setRefreshToken(payload.refresh_token || '');
      setUser(payload.user);
      setCapabilities(payload.capabilities || roleCapabilityFallback[payload.user.role] || []);
      localStorage.setItem('inventory_token', payload.token);
      if (payload.refresh_token) localStorage.setItem('inventory_refresh_token', payload.refresh_token);
      localStorage.setItem('inventory_user', JSON.stringify(payload.user));
      setStatus(`Logged in as ${payload.user.name}`);
    });
  };

  const logout = async () => {
    try {
      if (token) {
        await apiRequest('/logout', { method: 'POST', headers: authHeaders });
      }
    } catch {
      // best-effort logout; clear client session regardless
    }
    clearSession();
    setStatus('Signed out.');
  };

  const changePassword = async (event) => {
    event.preventDefault();
    if (!validatePasswordForm()) return;
    await withLoading(async () => {
      await apiRequest('/change-password', { method: 'POST', headers: authHeaders, body: JSON.stringify(forms.password) });
      setStatus('Password updated.');
    });
  };

  const checkIn = async (event) => {
    event.preventDefault();
    if (!validateCheckInForm()) return;
    await withLoading(async () => {
      const payload = await apiRequest('/check-in', {
        method: 'POST',
        headers: authHeaders,
        body: JSON.stringify({
          appointment_id: forms.checkIn.appointment_id,
          provider_card: forms.checkIn.provider_card,
          status: forms.checkIn.status,
          print_label: forms.checkIn.print_label,
          printer_ip: forms.checkIn.printer_ip || undefined,
          printer_port: toNumber(forms.checkIn.printer_port) || 9100,
          copies: toNumber(forms.checkIn.copies) || 1,
        }),
      });
      setLastLabel(payload.label);
      setField('assistantScan', 'encounter_code', payload.label.encounter_code);
      setStatus(`Encounter ${payload.label.encounter_code} created.`);
      await loadDashboard();
    });
  };

  const attachAssistant = async (event) => {
    event.preventDefault();
    await withLoading(async () => {
      await apiRequest(`/encounters/${encodeURIComponent(forms.assistantScan.encounter_code)}/assistant-scan`, {
        method: 'POST',
        headers: authHeaders,
        body: JSON.stringify({ assistant_card: forms.assistantScan.assistant_card }),
      });
      setStatus('Assistant attached to encounter.');
    });
  };

  const stockInScan = async (event) => {
    event.preventDefault();
    await withLoading(async () => {
      await apiRequest('/stock-in-scan', { method: 'POST', headers: authHeaders, body: JSON.stringify({ ...forms.stockInScan, quantity: toNumber(forms.stockInScan.quantity), cost: toNumber(forms.stockInScan.cost) }) });
      setStatus('Stock-in scan recorded.');
      await loadDashboard();
    });
  };

  const issueScan = async (event) => {
    event.preventDefault();
    await withLoading(async () => {
      await apiRequest('/issue-scan', { method: 'POST', headers: authHeaders, body: JSON.stringify({ ...forms.issueScan, quantity: toNumber(forms.issueScan.quantity) || 1 }) });
      setStatus('Issue scan recorded.');
      await loadDashboard();
    });
  };

  const returnScan = async (event) => {
    event.preventDefault();
    await withLoading(async () => {
      await apiRequest('/return-scan', { method: 'POST', headers: authHeaders, body: JSON.stringify({ ...forms.returnScan, quantity: toNumber(forms.returnScan.quantity) || 1 }) });
      setStatus('Return scan recorded.');
      await loadDashboard();
    });
  };

  const runScanAction = async (action) => {
    setLoading(true);
    setError('');
    try {
      await action();
      return true;
    } catch (err) {
      setError(err?.message || 'Scan failed.');
      return false;
    } finally {
      setLoading(false);
    }
  };

  const submitCheckInFromScan = async () => {
    if (!validateCheckInForm()) {
      setError('Appointment ID is required before scanning provider card.');
      return false;
    }
    return runScanAction(async () => {
      const payload = await apiRequest('/check-in', {
        method: 'POST',
        headers: authHeaders,
        body: JSON.stringify({
          appointment_id: forms.checkIn.appointment_id,
          provider_card: forms.checkIn.provider_card,
          status: forms.checkIn.status,
          print_label: forms.checkIn.print_label,
          printer_ip: forms.checkIn.printer_ip || undefined,
          printer_port: toNumber(forms.checkIn.printer_port) || 9100,
          copies: toNumber(forms.checkIn.copies) || 1,
        }),
      });
      setLastLabel(payload.label);
      setField('assistantScan', 'encounter_code', payload.label.encounter_code);
      setStatus(`Encounter ${payload.label.encounter_code} created.`);
      await loadDashboard();
    });
  };

  const submitAssistantFromScan = async () => {
    if (!forms.assistantScan.encounter_code.trim()) {
      setError('Encounter Code is required before scanning assistant card.');
      return false;
    }
    return runScanAction(async () => {
      await apiRequest(`/encounters/${encodeURIComponent(forms.assistantScan.encounter_code)}/assistant-scan`, {
        method: 'POST',
        headers: authHeaders,
        body: JSON.stringify({ assistant_card: forms.assistantScan.assistant_card }),
      });
      setStatus('Assistant attached to encounter.');
    });
  };

  const submitStockInFromScan = async () => {
    if (!forms.stockInScan.item_barcode.trim()) {
      setError('Item barcode is required.');
      return false;
    }
    if (!forms.stockInScan.item_name.trim()) {
      setError('Item Name is required for stock-in scan.');
      return false;
    }
    return runScanAction(async () => {
      await apiRequest('/stock-in-scan', {
        method: 'POST',
        headers: authHeaders,
        body: JSON.stringify({ ...forms.stockInScan, quantity: toNumber(forms.stockInScan.quantity), cost: toNumber(forms.stockInScan.cost) }),
      });
      setStatus('Stock-in scan recorded.');
      await loadDashboard();
    });
  };

  const submitIssueFromScan = async () => {
    if (!forms.issueScan.encounter_code.trim() || !forms.issueScan.operator_card.trim() || !forms.issueScan.item_barcode.trim()) {
      setError('Encounter Code and Operator Card are required before scanning item barcode.');
      return false;
    }
    return runScanAction(async () => {
      await apiRequest('/issue-scan', {
        method: 'POST',
        headers: authHeaders,
        body: JSON.stringify({ ...forms.issueScan, quantity: toNumber(forms.issueScan.quantity) || 1 }),
      });
      setStatus('Issue scan recorded.');
      await loadDashboard();
    });
  };

  const submitReturnFromScan = async () => {
    if (!forms.returnScan.encounter_code.trim() || !forms.returnScan.operator_card.trim() || !forms.returnScan.item_barcode.trim()) {
      setError('Encounter Code and Operator Card are required before scanning item barcode.');
      return false;
    }
    return runScanAction(async () => {
      await apiRequest('/return-scan', {
        method: 'POST',
        headers: authHeaders,
        body: JSON.stringify({ ...forms.returnScan, quantity: toNumber(forms.returnScan.quantity) || 1 }),
      });
      setStatus('Return scan recorded.');
      await loadDashboard();
    });
  };

  const randomCycle = async (event) => {
    event.preventDefault();
    await withLoading(async () => {
      const payload = await apiRequest('/cycle-counts/random-pick', { method: 'POST', headers: authHeaders, body: JSON.stringify({ item_count: toNumber(forms.cycleRandom.item_count), notes: forms.cycleRandom.notes || undefined }) });
      setCyclePickRows(payload.printable_rows || []);
      setStatus(`Cycle sheet ${payload.cycle_count.code} generated.`);
    });
  };

  const nextIdempotencyKey = () => (window.crypto && window.crypto.randomUUID
    ? window.crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(16).slice(2)}`);

  const loadTransferMeta = useCallback(async () => {
    const payload = await apiRequest('/transfers/meta', { headers: authHeaders });
    setTransferMeta({
      clinics: Array.isArray(payload.clinics) ? payload.clinics : [],
      items: Array.isArray(payload.items) ? payload.items : [],
      user_scope: payload.user_scope || null,
    });
  }, [apiRequest, authHeaders]);

  const loadTransferRows = useCallback(async () => {
    const payload = await apiRequest('/transfers/requests?limit=100&offset=0', { headers: authHeaders });
    setTransferRows(Array.isArray(payload.rows) ? payload.rows : []);
  }, [apiRequest, authHeaders]);

  const loadTransferDetail = useCallback(async (transferId) => {
    if (!transferId) {
      setTransferDetail(null);
      return;
    }
    const payload = await apiRequest(`/transfers/requests/${transferId}`, { headers: authHeaders });
    setTransferDetail(payload);
  }, [apiRequest, authHeaders]);

  useEffect(() => {
    if (!isAuthenticated || !capabilities.includes('TRANSFER')) return;
    if (view !== 'transfers') return;
    withLoading(async () => {
      await Promise.all([loadTransferMeta(), loadTransferRows()]);
    });
  }, [capabilities, isAuthenticated, loadTransferMeta, loadTransferRows, view]);

  useEffect(() => {
    if (!isAuthenticated || !capabilities.includes('TRANSFER')) return;
    if (view !== 'transfers') return;
    if (!selectedTransferId) {
      setTransferDetail(null);
      return;
    }
    withLoading(async () => {
      await loadTransferDetail(selectedTransferId);
    });
  }, [capabilities, isAuthenticated, loadTransferDetail, selectedTransferId, view]);

  const createTransferRequest = async (event) => {
    event.preventDefault();
    await withLoading(async () => {
      const payload = await apiRequest('/transfers/requests', {
        method: 'POST',
        headers: { ...authHeaders, 'idempotency-key': nextIdempotencyKey() },
        body: JSON.stringify({
          from_clinic_id: toNumber(forms.transferRequest.from_clinic_id),
          to_clinic_id: toNumber(forms.transferRequest.to_clinic_id),
          needed_by: forms.transferRequest.needed_by || undefined,
          notes: forms.transferRequest.notes || undefined,
          items: [{
            item_id: toNumber(forms.transferRequest.item_id),
            requested_qty: Number(forms.transferRequest.requested_qty),
          }],
        }),
      });
      setStatus(`Transfer request #${payload.transfer?.id || ''} created.`);
      setSelectedTransferId(String(payload.transfer.id));
      await loadTransferRows();
      await loadTransferDetail(payload.transfer.id);
    });
  };

  const approveTransfer = async (event) => {
    event.preventDefault();
    if (!selectedTransferId) return;
    await withLoading(async () => {
      const body = {
        decision: forms.transferApprove.decision,
      };
      if (forms.transferApprove.item_id && forms.transferApprove.approved_qty !== '') {
        body.lines = [{
          item_id: toNumber(forms.transferApprove.item_id),
          approved_qty: Number(forms.transferApprove.approved_qty),
        }];
      }
      const payload = await apiRequest(`/transfers/${selectedTransferId}/approve`, {
        method: 'POST',
        headers: { ...authHeaders, 'idempotency-key': nextIdempotencyKey() },
        body: JSON.stringify(body),
      });
      setStatus(`Transfer #${payload.transfer.id} ${forms.transferApprove.decision === 'APPROVE' ? 'approved' : 'rejected'}.`);
      await loadTransferRows();
      await loadTransferDetail(selectedTransferId);
    });
  };

  const pickPackTransfer = async (event) => {
    event.preventDefault();
    if (!selectedTransferId) return;
    await withLoading(async () => {
      const payload = await apiRequest(`/transfers/${selectedTransferId}/pick-pack`, {
        method: 'POST',
        headers: { ...authHeaders, 'idempotency-key': nextIdempotencyKey() },
        body: JSON.stringify({
          notes: forms.transferPick.notes || undefined,
          lines: [{
            item_id: toNumber(forms.transferPick.item_id),
            quantity: Number(forms.transferPick.quantity),
          }],
        }),
      });
      setStatus(`Transfer #${payload.transfer.id} pick/pack updated.`);
      await loadTransferRows();
      await loadTransferDetail(selectedTransferId);
    });
  };

  const receiveTransfer = async (event) => {
    event.preventDefault();
    if (!selectedTransferId) return;
    await withLoading(async () => {
      const payload = await apiRequest(`/transfers/${selectedTransferId}/receive`, {
        method: 'POST',
        headers: { ...authHeaders, 'idempotency-key': nextIdempotencyKey() },
        body: JSON.stringify({
          notes: forms.transferReceive.notes || undefined,
          lines: [{
            item_id: toNumber(forms.transferReceive.item_id),
            quantity: Number(forms.transferReceive.quantity),
          }],
        }),
      });
      setStatus(`Transfer #${payload.transfer.id} receive updated.`);
      await loadTransferRows();
      await loadTransferDetail(selectedTransferId);
    });
  };

  const cancelTransfer = async (event) => {
    event.preventDefault();
    if (!selectedTransferId) return;
    await withLoading(async () => {
      const payload = await apiRequest(`/transfers/${selectedTransferId}/cancel`, {
        method: 'POST',
        headers: { ...authHeaders, 'idempotency-key': nextIdempotencyKey() },
        body: JSON.stringify({
          reason: forms.transferCancel.reason || undefined,
          lines: [{
            item_id: toNumber(forms.transferCancel.item_id),
            quantity: Number(forms.transferCancel.quantity),
          }],
        }),
      });
      setStatus(`Transfer #${payload.transfer.id} cancelled/updated.`);
      await loadTransferRows();
      await loadTransferDetail(selectedTransferId);
    });
  };

  const loadReorderMeta = useCallback(async () => {
    const payload = await apiRequest('/reorder/meta', { headers: authHeaders });
    setReorderMeta({ clinics: Array.isArray(payload.clinics) ? payload.clinics : [] });
  }, [apiRequest, authHeaders]);

  const loadReorderRecommendations = useCallback(async () => {
    const params = new URLSearchParams();
    if (reorderFilters.clinic_id) params.set('clinic_id', reorderFilters.clinic_id);
    if (reorderFilters.location_code) params.set('location_code', reorderFilters.location_code);
    const payload = await apiRequest(`/reorder/recommendations?${params.toString()}`, { headers: authHeaders });
    setReorderRows(Array.isArray(payload.rows) ? payload.rows : []);
  }, [apiRequest, authHeaders, reorderFilters.clinic_id, reorderFilters.location_code]);

  useEffect(() => {
    if (!isAuthenticated || !capabilities.includes('REORDER_PLANNER')) return;
    if (view !== 'reorder') return;
    withLoading(async () => {
      await Promise.all([loadReorderMeta(), loadReorderRecommendations()]);
    });
  }, [capabilities, isAuthenticated, loadReorderMeta, loadReorderRecommendations, view]);

  const loadPrintJobs = async () => withLoading(async () => {
    const payload = await apiRequest('/print-jobs?limit=100&offset=0', { headers: authHeaders });
    setPrintJobs(payload.rows || []);
  });

  const askAssistant = async (event) => {
    event.preventDefault();
    await withLoading(async () => {
      const payload = await apiRequest('/ai/assistant', { method: 'POST', headers: authHeaders, body: JSON.stringify({ message: forms.assistant.message }) });
      setAssistantMessages((prev) => [...prev, { role: 'user', content: forms.assistant.message }, { role: 'assistant', content: payload.response }]);
      setField('assistant', 'message', '');
    });
  };

  const updateReportFilter = (field, value) => {
    setReportFilters((prev) => ({ ...prev, [field]: value }));
  };

  const exportDashboardCsv = () => {
    const lines = [];
    lines.push('section,key,value');
    lines.push(`kpi,inventory_value,${Number(kpis.inventory_value || 0)}`);
    lines.push(`kpi,active_encounters,${Number(kpis.active_encounters || 0)}`);
    lines.push(`kpi,daily_usage,${Number(kpis.daily_usage || 0)}`);
    lines.push(`kpi,low_stock_alerts,${Number(kpis.low_stock_alerts || 0)}`);
    (reportData.daily_usage || []).forEach((row) => lines.push(`daily_usage,${row.day},${row.issue_qty}`));
    (reportData.cost_per_encounter || []).forEach((row) => lines.push(`cost_per_encounter,${row.encounter_id},${row.total_cost}`));
    (reportData.stock_aging || []).forEach((row) => lines.push(`stock_aging,${row.bucket},${row.item_count}`));
    (reportData.low_stock_heatmap || []).forEach((row) => lines.push(`low_stock_risk,${row.sku_code},${row.risk_score}`));
    (reportData.transaction_velocity || []).forEach((row) => lines.push(`velocity,${row.day},${row.issue_count}|${row.return_count}|${row.stock_in_count}`));
    (intelligenceRows || []).forEach((row) => lines.push(`predictive,${row.sku_code},${row.risk_level}|${row.predicted_days_remaining}|${row.recommended_reorder_qty}`));

    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `executive-dashboard-${reportFilters.from}-to-${reportFilters.to}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const exportDashboardPdf = async () => {
    const [{ default: jsPDF }, { default: autoTable }] = await Promise.all([
      import('jspdf'),
      import('jspdf-autotable'),
    ]);

    const doc = new jsPDF({ unit: 'pt', format: 'a4' });
    doc.setFontSize(14);
    doc.text('Executive Inventory Dashboard', 40, 40);
    doc.setFontSize(10);
    doc.text(`Range: ${reportFilters.from} to ${reportFilters.to}`, 40, 58);
    doc.text(`Provider Filter: ${reportFilters.provider_user_id || 'All'}`, 40, 72);

    autoTable(doc, {
      startY: 90,
      head: [['KPI', 'Value', 'Delta']],
      body: [
        ['Inventory Value', '$' + Number(kpis.inventory_value || 0).toLocaleString(), '-'],
        ['Active Encounters', String(Number(kpis.active_encounters || 0)), '-'],
        ['Daily Usage', String(Number(kpis.daily_usage || 0)), String(Number(kpis?.deltas?.daily_usage_pct_vs_prev_window || 0)) + '%'],
        ['Low Stock Alerts', String(Number(kpis.low_stock_alerts || 0)), '-'],
      ],
    });

    autoTable(doc, {
      startY: doc.lastAutoTable.finalY + 18,
      head: [['Encounter', 'Total Cost']],
      body: (reportData.cost_per_encounter || []).slice(0, 20).map((r) => [String(r.encounter_id), '$' + Number(r.total_cost || 0).toFixed(2)]),
    });

    doc.save(`executive-dashboard-${reportFilters.from}-to-${reportFilters.to}.pdf`);
  };

  const deltaChip = (value) => (
    <Stack direction="row" spacing={0.4} alignItems="center">
      {Number(value || 0) >= 0 ? <ArrowUpRight size={14} color="#159E66" /> : <ArrowDownRight size={14} color="#D43D51" />}
      <Typography variant="caption" color={Number(value || 0) >= 0 ? 'success.main' : 'error.main'}>
        {Number(value || 0).toFixed(2)}%
      </Typography>
    </Stack>
  );

  const classifyCopilotIntent = (message) => {
    const text = message.toLowerCase();
    if (text.includes('summarize stock') || text.includes('stock levels')) return 'STOCK_SUMMARY';
    if (text.includes('low inventory') || text.includes('low stock')) return 'LOW_INVENTORY';
    if (text.includes('encounter cost')) return 'ENCOUNTER_COST';
    if (text.includes('restock')) return 'RESTOCK_SUGGEST';
    if (text.includes('go to') || text.includes('navigate')) return 'NAVIGATE';
    if (text.includes('generate cycle') || text.includes('create cycle')) return 'GENERATE_CYCLE';
    return 'GENERAL';
  };

  const buildCopilotAction = (intent, message, insight) => {
    const text = message.toLowerCase();
    if (intent === 'NAVIGATE') {
      if (text.includes('check')) return { type: 'NAVIGATE', label: 'Open Check-In', payload: { view: 'checkin' }, mutates: false };
      if (text.includes('stock')) return { type: 'NAVIGATE', label: 'Open Stock In', payload: { view: 'stock' }, mutates: false };
      if (text.includes('issue')) return { type: 'NAVIGATE', label: 'Open Issue', payload: { view: 'issue' }, mutates: false };
      if (text.includes('return')) return { type: 'NAVIGATE', label: 'Open Return', payload: { view: 'return' }, mutates: false };
      if (text.includes('cycle')) return { type: 'NAVIGATE', label: 'Open Cycle Count', payload: { view: 'cycle' }, mutates: false };
    }
    if (intent === 'LOW_INVENTORY' && insight?.lowCount > 0) {
      return { type: 'NAVIGATE', label: 'Open Stock In For Replenishment', payload: { view: 'stock' }, mutates: false };
    }
    if (intent === 'GENERATE_CYCLE') {
      return { type: 'GENERATE_CYCLE', label: 'Generate Random Cycle Count (20)', payload: { item_count: 20 }, mutates: true, capability: 'CYCLE_COUNT' };
    }
    return null;
  };

  const streamAssistantMessage = async (fullText, action = null) => {
    const placeholderId = Date.now();
    setCopilotMessages((prev) => [...prev, { id: placeholderId, role: 'assistant', content: '', streaming: true, action }]);
    setCopilotStreaming(true);
    let idx = 0;
    await new Promise((resolve) => {
      const timer = setInterval(() => {
        idx += 3;
        const chunk = fullText.slice(0, idx);
        setCopilotMessages((prev) => prev.map((m) => (m.id === placeholderId ? { ...m, content: chunk } : m)));
        if (idx >= fullText.length) {
          clearInterval(timer);
          resolve();
        }
      }, prefersReducedMotion ? 6 : 14);
    });
    setCopilotMessages((prev) => prev.map((m) => (m.id === placeholderId ? { ...m, streaming: false } : m)));
    setCopilotStreaming(false);
  };

  const executeSafeAction = async (action) => {
    if (!action) return;
    if (action.capability && !hasCapability(action.capability)) {
      setError(`Action blocked: requires capability ${action.capability}`);
      return;
    }
    if (action.mutates) {
      setPendingAction(action);
      setConfirmOpen(true);
      return;
    }
    if (action.type === 'NAVIGATE') {
      navigateView(action.payload.view);
      setCopilotOpen(false);
      setStatus(`Navigated to ${action.payload.view}.`);
    }
  };

  const runConfirmedAction = async () => {
    if (!pendingAction) return;
    setConfirmOpen(false);
    const action = pendingAction;
    setPendingAction(null);
    await withLoading(async () => {
      if (action.type === 'GENERATE_CYCLE') {
        const payload = await apiRequest('/cycle-counts/random-pick', {
          method: 'POST',
          headers: authHeaders,
          body: JSON.stringify({ item_count: action.payload.item_count }),
        });
        setCyclePickRows(payload.printable_rows || []);
        navigateView('cycle');
        setStatus(`Cycle sheet ${payload.cycle_count.code} generated.`);
      }
    });
  };

  const getCopilotInsight = async (intent, message) => {
    if (intent === 'STOCK_SUMMARY' || intent === 'LOW_INVENTORY' || intent === 'RESTOCK_SUGGEST') {
      const rows = stockLevels.length > 0 ? stockLevels : await apiRequest('/stock-levels', { headers: authHeaders });
      const totalItems = rows.length;
      const lowItems = rows.filter((r) => Number(r.quantity || 0) <= 5);
      const totalValue = rows.reduce((acc, r) => acc + Number(r.quantity || 0) * Number(r.cost || 0), 0);
      if (intent === 'STOCK_SUMMARY') {
        return {
          text: `Stock summary: ${totalItems} parts tracked, ${lowItems.length} low-stock alerts, estimated inventory value $${totalValue.toFixed(2)}.`,
          lowCount: lowItems.length,
        };
      }
      if (intent === 'LOW_INVENTORY') {
        const top = lowItems.slice(0, 6).map((x) => `${x.sku_code || x.id} (${x.quantity})`).join(', ');
        return {
          text: lowItems.length === 0 ? 'No low inventory detected today.' : `Low inventory detected for ${lowItems.length} parts. Priority list: ${top}.`,
          lowCount: lowItems.length,
        };
      }
      const suggest = lowItems.slice(0, 8).map((x) => `${x.sku_code || x.id}: reorder ${Math.max(10 - Number(x.quantity || 0), 5)}`).join('; ');
      return {
        text: lowItems.length === 0 ? 'No restock recommendation needed right now.' : `Suggested restock quantities based on threshold policy: ${suggest}.`,
        lowCount: lowItems.length,
      };
    }

    if (intent === 'ENCOUNTER_COST') {
      const match = message.match(/(\d{1,8})/);
      const encounterId = match ? Number(match[1]) : null;
      const costRows = await apiRequest('/encounter-cost', { headers: authHeaders });
      if (!encounterId) {
        const top = (costRows || []).slice(0, 5).map((r) => `Encounter ${r.encounter_id}: $${Number(r.total_cost || 0).toFixed(2)}`).join(', ');
        return { text: top ? `Recent encounter costs: ${top}.` : 'No encounter cost data available yet.' };
      }
      const target = (costRows || []).find((r) => Number(r.encounter_id) === encounterId);
      return { text: target ? `Encounter ${encounterId} estimated issued-supply cost: $${Number(target.total_cost || 0).toFixed(2)}.` : `No cost row found for encounter ${encounterId}.` };
    }

    return { text: '' };
  };

  const sendCopilot = async (rawMessage, source = 'manual') => {
    const message = String(rawMessage || '').trim();
    if (!message) return;
    const intent = classifyCopilotIntent(message);
    const contextBlock = `context: page=${view}; role=${user?.role || 'unknown'}; capabilities=${capabilities.join(',')};`;
    setCopilotMessages((prev) => [...prev, { role: 'user', content: message, streaming: false, action: null }]);

    await withLoading(async () => {
      const [aiPayload, insight] = await Promise.all([
        apiRequest('/ai/assistant', {
          method: 'POST',
          headers: authHeaders,
          body: JSON.stringify({
            message: [
              'You are Inventory Copilot.',
              'Return concise, operational guidance.',
              'Do not execute mutations directly.',
              contextBlock,
              `intent=${intent}`,
              `user_message=${message}`,
            ].join('\n'),
          }),
        }),
        getCopilotInsight(intent, message),
      ]);

      const action = buildCopilotAction(intent, message, insight);
      const response = [aiPayload?.response, insight?.text, action ? `Suggested action: ${action.label}` : null]
        .filter(Boolean)
        .join('\n\n');

      await streamAssistantMessage(response || 'I could not generate a response for that request.', action);
      addUsageLog({
        source,
        page: view,
        intent,
        prompt: message,
        action: action ? action.type : null,
        status: 'completed',
      });
    });
  };

  const renderDashboard = () => {
    const kpiCards = [
      { label: 'Total Inventory Value', value: `$${Number(kpis.inventory_value || 0).toLocaleString()}`, delta: null },
      { label: 'Active Encounters', value: Number(kpis.active_encounters || 0), delta: null },
      { label: 'Daily Usage', value: Number(kpis.daily_usage || 0), delta: Number(kpis?.deltas?.daily_usage_pct_vs_prev_window || 0) },
      { label: 'Low Stock Alerts', value: Number(kpis.low_stock_alerts || 0), delta: null },
      { label: 'Encounter Cost (Range)', value: `$${Number(kpis.encounter_cost_total || 0).toLocaleString()}`, delta: Number(kpis?.deltas?.encounter_cost_pct_vs_prev_window || 0) },
    ];

    const heatmapRows = (reportData.low_stock_heatmap || []).slice(0, 16);
    const intelligenceTop = (intelligenceRows || []).slice(0, 12);
    const highRiskCount = intelligenceRows.filter((row) => row.risk_level === 'HIGH').length;
    const mediumRiskCount = intelligenceRows.filter((row) => row.risk_level === 'MEDIUM').length;
    const lowRiskCount = intelligenceRows.filter((row) => row.risk_level === 'LOW').length;

    return (
      <Stack spacing={2}>
        <Card>
          <CardContent>
            <Stack direction={{ xs: 'column', lg: 'row' }} spacing={1.2} alignItems={{ xs: 'stretch', lg: 'center' }} justifyContent="space-between">
              <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.2} sx={{ flexWrap: 'wrap' }}>
                <TextField
                  size="small"
                  type="date"
                  label="From"
                  InputLabelProps={{ shrink: true }}
                  value={reportFilters.from}
                  onChange={(e) => updateReportFilter('from', e.target.value)}
                />
                <TextField
                  size="small"
                  type="date"
                  label="To"
                  InputLabelProps={{ shrink: true }}
                  value={reportFilters.to}
                  onChange={(e) => updateReportFilter('to', e.target.value)}
                />
                <TextField
                  select
                  size="small"
                  label="Provider"
                  value={reportFilters.provider_user_id}
                  onChange={(e) => updateReportFilter('provider_user_id', e.target.value)}
                  sx={{ minWidth: 220 }}
                >
                  <MenuItem value="">All Providers</MenuItem>
                  {providers.map((p) => (
                    <MenuItem key={p.id} value={String(p.id)}>
                      {p.name} ({p.student_card})
                    </MenuItem>
                  ))}
                </TextField>
                <Button variant="contained" onClick={loadDashboard}>Apply</Button>
              </Stack>
              <Stack direction="row" spacing={1}>
                <Button variant="outlined" startIcon={<Download size={16} />} onClick={exportDashboardCsv}>Export CSV</Button>
                <Button variant="outlined" startIcon={<FileText size={16} />} onClick={exportDashboardPdf}>Export PDF</Button>
              </Stack>
            </Stack>
          </CardContent>
        </Card>

        <Grid container spacing={2}>
          {kpiCards.map((kpi, idx) => (
            <Grid item xs={12} sm={6} lg={idx === 4 ? 4 : 2} key={kpi.label}>
              <MotionCard initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: idx * 0.03, duration: 0.25 }}>
                <CardContent>
                  <Typography variant="body2" color="text.secondary">{kpi.label}</Typography>
                  <Typography variant="h4" sx={{ fontWeight: 700 }}>{kpi.value}</Typography>
                  {typeof kpi.delta === 'number' ? (
                    <Stack direction="row" spacing={0.7} alignItems="center">
                      {deltaChip(kpi.delta)}
                      <Typography variant="caption" color="text.secondary">vs last window</Typography>
                    </Stack>
                  ) : (
                    <Typography variant="caption" color="text.secondary">Stable baseline</Typography>
                  )}
                </CardContent>
              </MotionCard>
            </Grid>
          ))}
        </Grid>

        <Grid container spacing={2}>
          <Grid item xs={12} lg={6}>
            <Card>
              <CardContent>
                <Typography variant="h6">Inventory Valuation Trend</Typography>
                <Typography variant="caption" color="text.secondary">Daily valuation movement by transaction mix.</Typography>
                <Box sx={{ height: 260, mt: 1 }}>
                  {loading ? (
                    <Skeleton variant="rounded" height={240} />
                  ) : (
                    <ResponsiveContainer>
                      <LineChart data={reportData.valuation_trend || []}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="day" />
                        <YAxis />
                        <ChartTooltip />
                        <Line type="monotone" dataKey="valuation_delta" stroke="#0F5FFF" strokeWidth={2.2} dot={false} />
                      </LineChart>
                    </ResponsiveContainer>
                  )}
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12} lg={6}>
            <Card>
              <CardContent>
                <Typography variant="h6">Daily Usage</Typography>
                <Typography variant="caption" color="text.secondary">Issued unit volume by day.</Typography>
                <Box sx={{ height: 260, mt: 1 }}>
                  {loading ? (
                    <Skeleton variant="rounded" height={240} />
                  ) : (
                    <ResponsiveContainer>
                      <BarChart data={reportData.daily_usage || []}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="day" />
                        <YAxis />
                        <ChartTooltip />
                        <Bar dataKey="issue_qty" fill="#159E66" radius={[6, 6, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  )}
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12} lg={7}>
            <Card>
              <CardContent>
                <Typography variant="h6">Cost Per Encounter</Typography>
                <Typography variant="caption" color="text.secondary">Highest supply-cost encounters in selected range.</Typography>
                <Box sx={{ height: 280, mt: 1 }}>
                  {loading ? (
                    <Skeleton variant="rounded" height={260} />
                  ) : (reportData.cost_per_encounter || []).length === 0 ? (
                    <EmptyState title="No encounter cost data" subtitle="Issue transactions are required to compute encounter cost." />
                  ) : (
                    <ResponsiveContainer>
                      <BarChart data={(reportData.cost_per_encounter || []).slice(0, 12).map((r) => ({ ...r, encounter: `E-${r.encounter_id}` }))}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="encounter" />
                        <YAxis />
                        <ChartTooltip />
                        <Bar dataKey="total_cost" fill="#D43D51" radius={[6, 6, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  )}
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12} lg={5}>
            <Card>
              <CardContent>
                <Typography variant="h6">Stock Aging Analysis</Typography>
                <Typography variant="caption" color="text.secondary">On-hand part count by aging bucket.</Typography>
                <Box sx={{ height: 280, mt: 1 }}>
                  {loading ? (
                    <Skeleton variant="rounded" height={260} />
                  ) : (
                    <ResponsiveContainer>
                      <BarChart data={reportData.stock_aging || []}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="bucket" />
                        <YAxis />
                        <ChartTooltip />
                        <Bar dataKey="item_count" fill="#6D7B92" radius={[6, 6, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  )}
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12} lg={6}>
            <Card>
              <CardContent>
                <Typography variant="h6">Low-Stock Risk Heatmap</Typography>
                <Typography variant="caption" color="text.secondary">Risk score combines low quantity and recent velocity.</Typography>
                <Box sx={{ maxHeight: 292, overflowY: 'auto', mt: 1 }}>
                  {loading ? (
                    <Skeleton variant="rounded" height={250} />
                  ) : heatmapRows.length === 0 ? (
                    <EmptyState title="No low-stock risk rows" subtitle="All tracked stock is above threshold." />
                  ) : (
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>SKU</TableCell>
                          <TableCell>Qty</TableCell>
                          <TableCell>30d TX</TableCell>
                          <TableCell align="right">Risk</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {heatmapRows.map((row) => (
                          <TableRow key={row.id}>
                            <TableCell>{row.sku_code}</TableCell>
                            <TableCell>{row.quantity}</TableCell>
                            <TableCell>{row.tx_count_30d}</TableCell>
                            <TableCell align="right">
                              <Chip
                                size="small"
                                label={row.risk_score}
                                color={Number(row.risk_score) > 30 ? 'error' : Number(row.risk_score) > 15 ? 'warning' : 'success'}
                              />
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12} lg={6}>
            <Card>
              <CardContent>
                <Typography variant="h6">Transaction Velocity</Typography>
                <Typography variant="caption" color="text.secondary">Daily count of issue, return, and stock-in actions.</Typography>
                <Box sx={{ height: 290, mt: 1 }}>
                  {loading ? (
                    <Skeleton variant="rounded" height={260} />
                  ) : (
                    <ResponsiveContainer>
                      <LineChart data={reportData.transaction_velocity || []}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="day" />
                        <YAxis />
                        <ChartTooltip />
                        <Legend />
                        <Line type="monotone" dataKey="issue_count" stroke="#D43D51" dot={false} />
                        <Line type="monotone" dataKey="return_count" stroke="#159E66" dot={false} />
                        <Line type="monotone" dataKey="stock_in_count" stroke="#0F5FFF" dot={false} />
                      </LineChart>
                    </ResponsiveContainer>
                  )}
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.2} alignItems={{ xs: 'flex-start', md: 'center' }} justifyContent="space-between">
                  <Box>
                    <Typography variant="h6">Predictive Inventory Intelligence</Typography>
                    <Typography variant="caption" color="text.secondary">
                      Moving-average forecast for stockout risk and reorder planning.
                    </Typography>
                  </Box>
                  <Stack direction="row" spacing={1}>
                    <Chip size="small" color="error" label={`High ${highRiskCount}`} />
                    <Chip size="small" color="warning" label={`Medium ${mediumRiskCount}`} />
                    <Chip size="small" color="success" label={`Low ${lowRiskCount}`} />
                  </Stack>
                </Stack>
                <Box sx={{ mt: 1.5, maxHeight: 330, overflowY: 'auto' }}>
                  {loading ? (
                    <Skeleton variant="rounded" height={180} />
                  ) : intelligenceTop.length === 0 ? (
                    <EmptyState title="No predictive rows" subtitle="No transaction history yet to forecast demand." />
                  ) : (
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>SKU</TableCell>
                          <TableCell>On Hand</TableCell>
                          <TableCell>Days Remaining</TableCell>
                          <TableCell>Reorder Qty</TableCell>
                          <TableCell>Risk</TableCell>
                          <TableCell align="right">AI Explanation</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {intelligenceTop.map((row) => (
                          <TableRow key={row.item_id}>
                            <TableCell>{row.sku_code}</TableCell>
                            <TableCell>{row.on_hand_qty}</TableCell>
                            <TableCell>{row.predicted_days_remaining === null ? 'N/A' : row.predicted_days_remaining}</TableCell>
                            <TableCell>{row.recommended_reorder_qty}</TableCell>
                            <TableCell>
                              <Chip
                                size="small"
                                color={row.risk_level === 'HIGH' ? 'error' : row.risk_level === 'MEDIUM' ? 'warning' : 'success'}
                                label={row.risk_level}
                              />
                            </TableCell>
                            <TableCell align="right">
                              <Tooltip title={row.ai_explanation || 'No explanation available'} arrow placement="left">
                                <Typography variant="caption" sx={{ cursor: 'help', color: 'primary.main' }}>
                                  Why this risk
                                </Typography>
                              </Tooltip>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Typography variant="h6">Lot Expiry Risk Dashboard</Typography>
                <Grid container spacing={1.2} sx={{ mt: 0.4 }}>
                  <Grid item xs={6} md={3}><Chip color="error" label={`Expired: ${Number(expiryDashboard.summary?.expired_on_hand || 0)}`} /></Grid>
                  <Grid item xs={6} md={3}><Chip color="warning" label={`30d: ${Number(expiryDashboard.summary?.expiring_30 || 0)}`} /></Grid>
                  <Grid item xs={6} md={3}><Chip color="warning" label={`60d: ${Number(expiryDashboard.summary?.expiring_60 || 0)}`} /></Grid>
                  <Grid item xs={6} md={3}><Chip color="info" label={`90d: ${Number(expiryDashboard.summary?.expiring_90 || 0)}`} /></Grid>
                </Grid>
                <Box sx={{ mt: 1.5, maxHeight: 220, overflowY: 'auto' }}>
                  {(expiryDashboard.expiring_rows || []).length === 0 ? (
                    <EmptyState title="No expiring lots" subtitle="No lot inventory within the next 90 days." />
                  ) : (
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>SKU</TableCell>
                          <TableCell>Lot</TableCell>
                          <TableCell>Expiry</TableCell>
                          <TableCell align="right">Qty</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {expiryDashboard.expiring_rows.slice(0, 20).map((row, idx) => (
                          <TableRow key={`${row.item_id}-${row.lot_code}-${idx}`}>
                            <TableCell>{row.sku_code}</TableCell>
                            <TableCell>{row.lot_code}</TableCell>
                            <TableCell>{String(row.expiry_date).slice(0, 10)}</TableCell>
                            <TableCell align="right">{row.quantity}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12} lg={6}>
            <Card>
              <CardContent>
                <Typography variant="h6">Slow-Moving Near Expiry</Typography>
                <Box sx={{ mt: 1, maxHeight: 220, overflowY: 'auto' }}>
                  {(expiryDashboard.slow_moving_near_expiry || []).length === 0 ? (
                    <EmptyState title="No slow-moving near expiry lots" subtitle="Usage velocity is healthy for near-expiry inventory." />
                  ) : (
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>SKU</TableCell>
                          <TableCell>Lot</TableCell>
                          <TableCell>Expiry</TableCell>
                          <TableCell>30d Usage</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {expiryDashboard.slow_moving_near_expiry.slice(0, 15).map((row, idx) => (
                          <TableRow key={`${row.item_id}-${row.lot_code}-${idx}`}>
                            <TableCell>{row.sku_code}</TableCell>
                            <TableCell>{row.lot_code}</TableCell>
                            <TableCell>{String(row.expiry_date).slice(0, 10)}</TableCell>
                            <TableCell>{row.issue_qty_30d}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12} lg={6}>
            <Card>
              <CardContent>
                <Typography variant="h6">Transfer Suggestions For Expiring Lots</Typography>
                <Box sx={{ mt: 1, maxHeight: 220, overflowY: 'auto' }}>
                  {expirySuggestions.length === 0 ? (
                    <EmptyState title="No suggestions" subtitle="No high-usage target clinics detected for expiring lots." />
                  ) : (
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>From</TableCell>
                          <TableCell>To</TableCell>
                          <TableCell>SKU</TableCell>
                          <TableCell>Lot</TableCell>
                          <TableCell align="right">Suggested Qty</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {expirySuggestions.slice(0, 15).map((row, idx) => (
                          <TableRow key={`${row.item_id}-${row.lot_code}-${idx}`}>
                            <TableCell>{row.from_clinic_name}</TableCell>
                            <TableCell>{row.to_clinic_name}</TableCell>
                            <TableCell>{row.sku_code}</TableCell>
                            <TableCell>{row.lot_code}</TableCell>
                            <TableCell align="right">{row.suggested_transfer_qty}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Stack direction="row" justifyContent="space-between" alignItems="center">
                  <Typography variant="h6">Daily Expiry Alert Digests</Typography>
                  {user?.role === 'ADMIN' && (
                    <Button
                      size="small"
                      variant="outlined"
                      onClick={() => withLoading(async () => {
                        await apiRequest('/alerts/digests/run', { method: 'POST', headers: authHeaders, body: JSON.stringify({}) });
                        await loadDashboard();
                      })}
                    >
                      Run Digest Now
                    </Button>
                  )}
                </Stack>
                <Box sx={{ mt: 1, maxHeight: 200, overflowY: 'auto' }}>
                  {alertDigests.length === 0 ? (
                    <EmptyState title="No digests yet" subtitle="Run digest job or wait for scheduler interval." />
                  ) : (
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>Date</TableCell>
                          <TableCell>Clinic</TableCell>
                          <TableCell>Expired</TableCell>
                          <TableCell>30d</TableCell>
                          <TableCell>60d</TableCell>
                          <TableCell>90d</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {alertDigests.slice(0, 20).map((row) => (
                          <TableRow key={row.id}>
                            <TableCell>{String(row.digest_date).slice(0, 10)}</TableCell>
                            <TableCell>{row.clinic_name || row.clinic_id}</TableCell>
                            <TableCell>{row.payload?.expired_on_hand_qty ?? 0}</TableCell>
                            <TableCell>{row.payload?.expiring_30_qty ?? 0}</TableCell>
                            <TableCell>{row.payload?.expiring_60_qty ?? 0}</TableCell>
                            <TableCell>{row.payload?.expiring_90_qty ?? 0}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </Box>
              </CardContent>
            </Card>
          </Grid>
        </Grid>

        <Card>
          <CardContent>
            <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}>
              <Button variant="outlined" onClick={() => sendCopilot('Summarize stock levels', 'quick_button')}>Ask AI: Stock Summary</Button>
              <Button variant="outlined" onClick={() => sendCopilot('Detect low inventory and prioritize replenishment', 'quick_button')}>Ask AI: Low Inventory</Button>
              <Button variant="outlined" onClick={() => sendCopilot('Explain encounter costs for this date range', 'quick_button')}>Ask AI: Encounter Cost</Button>
              <Button variant="outlined" onClick={() => sendCopilot('Suggest restock quantities based on current stock', 'quick_button')}>Ask AI: Restock Plan</Button>
            </Stack>
          </CardContent>
        </Card>
      </Stack>
    );
  };

  const breadcrumb = nav.find((n) => n.id === view)?.label || 'Dashboard';

  if (!isAuthenticated) {
    return (
      <Box className={`login-shell mode-${colorMode}`}>
        <CssBaseline />
        <Card sx={{ maxWidth: 420, width: '100%' }}>
          <CardContent>
            <div className="auth-hero">
              <img className="auth-logo" src="/brand-logo.svg" alt={`${APP_NAME} logo`} />
            </div>
            <Typography variant="h4">{APP_NAME}</Typography>
            <Typography color="text.secondary" sx={{ mb: 2 }}>{APP_TAGLINE}</Typography>
            <Stack component="form" spacing={2} onSubmit={login}>
              <TextField
                label="ID Card"
                value={forms.login.student_card}
                error={Boolean(getFieldError('login.student_card'))}
                helperText={getFieldError('login.student_card')}
                onChange={(e) => setField('login', 'student_card', e.target.value)}
              />
              <TextField
                label="Password"
                type="password"
                value={forms.login.password}
                error={Boolean(getFieldError('login.password'))}
                helperText={getFieldError('login.password')}
                onChange={(e) => setField('login', 'password', e.target.value)}
              />
              <Button type="submit" variant="contained">Sign In</Button>
            </Stack>
          </CardContent>
        </Card>
      </Box>
    );
  }

  if (mustResetPassword) {
    return (
      <Box className={`login-shell mode-${colorMode}`}>
        <CssBaseline />
        <Card sx={{ maxWidth: 420, width: '100%' }}>
          <CardContent>
            <Typography variant="h5">Password Reset Required</Typography>
            <Stack component="form" spacing={2} onSubmit={changePassword} sx={{ mt: 2 }}>
              <TextField
                label="Current Password"
                type="password"
                value={forms.password.current_password}
                error={Boolean(getFieldError('password.current_password'))}
                helperText={getFieldError('password.current_password')}
                onChange={(e) => setField('password', 'current_password', e.target.value)}
              />
              <TextField
                label="New Password"
                type="password"
                value={forms.password.new_password}
                error={Boolean(getFieldError('password.new_password'))}
                helperText={getFieldError('password.new_password') || 'Minimum 8 characters.'}
                onChange={(e) => setField('password', 'new_password', e.target.value)}
              />
              <Button type="submit" variant="contained">Update Password</Button>
            </Stack>
          </CardContent>
        </Card>
      </Box>
    );
  }

  return (
    <AppLayout
      appName={APP_NAME}
      appTagline={APP_TAGLINE}
      drawerWidth={DRAWER_WIDTH}
      isMobile={isMobile}
      mobileOpen={mobileOpen}
      onOpenMobileNav={() => setMobileOpen(true)}
      onCloseMobileNav={() => setMobileOpen(false)}
      colorMode={colorMode}
      onToggleColorMode={onToggleColorMode}
      breadcrumb={breadcrumb}
      user={user}
      checkInOnly={checkInOnly}
      nav={nav}
      activeView={view}
      onNavigate={navigateView}
      onRequestLogout={() => setLogoutConfirmOpen(true)}
    >
        {loading && <LinearProgress />}
        <Stack spacing={2}>
          <Typography variant="h5" data-testid="page-primary-heading">{breadcrumb}</Typography>
          {checkInOnly && <Alert severity="info">This account can access check-in workflows only.</Alert>}
          <AnimatePresence mode="wait">
            <motion.div
              key={view}
              initial={prefersReducedMotion ? false : { opacity: 0, y: 8 }}
              animate={prefersReducedMotion ? { opacity: 1 } : { opacity: 1, y: 0 }}
              exit={prefersReducedMotion ? { opacity: 0 } : { opacity: 0, y: -8 }}
              transition={{ duration: prefersReducedMotion ? 0 : 0.2, ease: 'easeOut' }}
            >
              {view === 'dashboard' && renderDashboard()}
          {view === 'checkin' && (
            <Grid container spacing={2}>
              <Grid item xs={12} md={7}>
                <Card><CardContent>
                  <Typography variant="h6" gutterBottom>Encounter Check-In</Typography>
                  <Stack component="form" spacing={2} onSubmit={checkIn}>
                    <TextField
                      label="Appointment ID"
                      value={forms.checkIn.appointment_id}
                      error={Boolean(getFieldError('checkIn.appointment_id'))}
                      helperText={getFieldError('checkIn.appointment_id')}
                      onChange={(e) => setField('checkIn', 'appointment_id', e.target.value)}
                      required
                    />
                    <BarcodeInput
                      label="Provider ID Card"
                      value={forms.checkIn.provider_card}
                      onChange={(value) => setField('checkIn', 'provider_card', value)}
                      onScan={submitCheckInFromScan}
                      autoFocus
                      autoFocusKey={view}
                      error={Boolean(getFieldError('checkIn.provider_card'))}
                      helperText={getFieldError('checkIn.provider_card')}
                    />
                    <Select value={forms.checkIn.status} onChange={(e) => setField('checkIn', 'status', e.target.value)}><MenuItem value="ACTIVE">ACTIVE</MenuItem><MenuItem value="COMPLETED">COMPLETED</MenuItem><MenuItem value="CANCELLED">CANCELLED</MenuItem></Select>
                    <Button type="submit" variant="contained">Check In</Button>
                  </Stack>
                </CardContent></Card>
              </Grid>
              <Grid item xs={12} md={5}>
                <Card><CardContent>
                  <Typography variant="h6" gutterBottom>Assistant Link</Typography>
                  <Stack component="form" spacing={2} onSubmit={attachAssistant}>
                    <BarcodeInput
                      label="Encounter Code"
                      value={forms.assistantScan.encounter_code}
                      onChange={(value) => setField('assistantScan', 'encounter_code', value)}
                    />
                    <BarcodeInput
                      label="Assistant ID Card"
                      value={forms.assistantScan.assistant_card}
                      onChange={(value) => setField('assistantScan', 'assistant_card', value)}
                      onScan={submitAssistantFromScan}
                    />
                    <Button type="submit" variant="outlined">Attach Assistant</Button>
                  </Stack>
                  <Divider sx={{ my: 2 }} />
                  {!lastLabel ? <EmptyState title="No label yet" subtitle="Create a check-in to generate barcode label." /> : <TextField multiline minRows={7} value={lastLabel.zpl || ''} />}
                </CardContent></Card>
              </Grid>
            </Grid>
          )}
          {view === 'stock' && (
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>Stock In by Scan</Typography>
                <Stack component="form" spacing={2} onSubmit={stockInScan}>
                  <BarcodeInput
                    label="Item Barcode"
                    value={forms.stockInScan.item_barcode}
                    onChange={(value) => setField('stockInScan', 'item_barcode', value)}
                    onScan={submitStockInFromScan}
                    autoFocus
                    autoFocusKey={view}
                  />
                  <TextField label="Item Name" value={forms.stockInScan.item_name} onChange={(e) => setField('stockInScan', 'item_name', e.target.value)} />
                  <TextField label="Quantity" value={forms.stockInScan.quantity} onChange={(e) => setField('stockInScan', 'quantity', e.target.value)} />
                  <TextField label="Cost" value={forms.stockInScan.cost} onChange={(e) => setField('stockInScan', 'cost', e.target.value)} />
                  <TextField label="Lot Code (required for expiry-tracked)" value={forms.stockInScan.lot_code} onChange={(e) => setField('stockInScan', 'lot_code', e.target.value)} />
                  <TextField type="date" label="Expiry Date (required for expiry-tracked)" InputLabelProps={{ shrink: true }} value={forms.stockInScan.expiry_date} onChange={(e) => setField('stockInScan', 'expiry_date', e.target.value)} />
                  <Button type="submit">Stock In</Button>
                </Stack>
              </CardContent>
            </Card>
          )}
          {view === 'issue' && (
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>Issue by Scan</Typography>
                <Stack component="form" spacing={2} onSubmit={issueScan}>
                  <BarcodeInput
                    label="Encounter Code"
                    value={forms.issueScan.encounter_code}
                    onChange={(value) => setField('issueScan', 'encounter_code', value)}
                    autoFocus
                    autoFocusKey={view}
                  />
                  <BarcodeInput
                    label="Item Barcode"
                    value={forms.issueScan.item_barcode}
                    onChange={(value) => setField('issueScan', 'item_barcode', value)}
                    onScan={submitIssueFromScan}
                  />
                  <BarcodeInput
                    label="Operator Card"
                    value={forms.issueScan.operator_card}
                    onChange={(value) => setField('issueScan', 'operator_card', value)}
                  />
                  <TextField label="Quantity" value={forms.issueScan.quantity} onChange={(e) => setField('issueScan', 'quantity', e.target.value)} />
                  <Button type="submit">Issue</Button>
                </Stack>
              </CardContent>
            </Card>
          )}
          {view === 'return' && (
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>Return by Scan</Typography>
                <Stack component="form" spacing={2} onSubmit={returnScan}>
                  <BarcodeInput
                    label="Encounter Code"
                    value={forms.returnScan.encounter_code}
                    onChange={(value) => setField('returnScan', 'encounter_code', value)}
                    autoFocus
                    autoFocusKey={view}
                  />
                  <BarcodeInput
                    label="Item Barcode"
                    value={forms.returnScan.item_barcode}
                    onChange={(value) => setField('returnScan', 'item_barcode', value)}
                    onScan={submitReturnFromScan}
                  />
                  <BarcodeInput
                    label="Operator Card"
                    value={forms.returnScan.operator_card}
                    onChange={(value) => setField('returnScan', 'operator_card', value)}
                  />
                  <TextField label="Quantity" value={forms.returnScan.quantity} onChange={(e) => setField('returnScan', 'quantity', e.target.value)} />
                  <Button type="submit">Return</Button>
                </Stack>
              </CardContent>
            </Card>
          )}
          {view === 'cycle' && <Card><CardContent><Typography variant="h6" gutterBottom>Random Cycle Count Sheet</Typography><Stack component="form" spacing={2} onSubmit={randomCycle}><TextField label="Item Count" value={forms.cycleRandom.item_count} onChange={(e) => setField('cycleRandom', 'item_count', e.target.value)} /><TextField label="Notes" value={forms.cycleRandom.notes} onChange={(e) => setField('cycleRandom', 'notes', e.target.value)} /><Stack direction="row" spacing={1}><Button type="submit">Generate</Button><Button variant="outlined" onClick={() => window.print()}>Print</Button></Stack></Stack><Divider sx={{ my: 2 }} />{cyclePickRows.length === 0 ? <EmptyState title="No pick sheet" subtitle="Generate a random sheet to begin cycle count." /> : <Table size="small"><TableHead><TableRow><TableCell>Line</TableCell><TableCell>SKU</TableCell><TableCell>Name</TableCell><TableCell>Expected</TableCell></TableRow></TableHead><TableBody>{cyclePickRows.map((r) => <TableRow key={`${r.line_no}-${r.sku_code}`}><TableCell>{r.line_no}</TableCell><TableCell>{r.sku_code}</TableCell><TableCell>{r.item_name}</TableCell><TableCell>{r.expected_qty}</TableCell></TableRow>)}</TableBody></Table>}</CardContent></Card>}
          {view === 'transfers' && (
            <Grid container spacing={2}>
              <Grid item xs={12} md={5}>
                <Card>
                  <CardContent>
                    <Typography variant="h6" gutterBottom>Create Transfer Request</Typography>
                    <Stack component="form" spacing={1.5} onSubmit={createTransferRequest}>
                      <TextField
                        select
                        label="From Clinic"
                        value={forms.transferRequest.from_clinic_id}
                        onChange={(e) => setField('transferRequest', 'from_clinic_id', e.target.value)}
                      >
                        {transferMeta.clinics.map((clinic) => <MenuItem key={clinic.id} value={String(clinic.id)}>{clinic.name}</MenuItem>)}
                      </TextField>
                      <TextField
                        select
                        label="To Clinic"
                        value={forms.transferRequest.to_clinic_id}
                        onChange={(e) => setField('transferRequest', 'to_clinic_id', e.target.value)}
                      >
                        {transferMeta.clinics.map((clinic) => <MenuItem key={clinic.id} value={String(clinic.id)}>{clinic.name}</MenuItem>)}
                      </TextField>
                      <TextField
                        select
                        label="Item"
                        value={forms.transferRequest.item_id}
                        onChange={(e) => setField('transferRequest', 'item_id', e.target.value)}
                      >
                        {transferMeta.items.map((item) => <MenuItem key={item.id} value={String(item.id)}>{item.sku_code} - {item.name}</MenuItem>)}
                      </TextField>
                      <TextField label="Requested Qty" value={forms.transferRequest.requested_qty} onChange={(e) => setField('transferRequest', 'requested_qty', e.target.value)} />
                      <TextField type="datetime-local" label="Needed By" InputLabelProps={{ shrink: true }} value={forms.transferRequest.needed_by} onChange={(e) => setField('transferRequest', 'needed_by', e.target.value)} />
                      <TextField label="Notes" value={forms.transferRequest.notes} onChange={(e) => setField('transferRequest', 'notes', e.target.value)} />
                      <Button type="submit" variant="contained">Create Request</Button>
                    </Stack>
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={7}>
                <Card>
                  <CardContent>
                    <Stack direction="row" spacing={1} sx={{ mb: 1 }}>
                      <Button variant="outlined" onClick={loadTransferRows}>Refresh Requests</Button>
                    </Stack>
                    {transferRows.length === 0 ? (
                      <EmptyState title="No transfer requests" subtitle="Create a request to start inter-clinic transfer." />
                    ) : (
                      <Table size="small">
                        <TableHead>
                          <TableRow>
                            <TableCell>ID</TableCell>
                            <TableCell>Status</TableCell>
                            <TableCell>From</TableCell>
                            <TableCell>To</TableCell>
                            <TableCell align="right">Action</TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {transferRows.map((row) => (
                            <TableRow key={row.id} selected={String(row.id) === String(selectedTransferId)}>
                              <TableCell>{row.id}</TableCell>
                              <TableCell><Chip size="small" label={row.status} /></TableCell>
                              <TableCell>{row.from_clinic_name}</TableCell>
                              <TableCell>{row.to_clinic_name}</TableCell>
                              <TableCell align="right">
                                <Button size="small" onClick={() => setSelectedTransferId(String(row.id))}>Open</Button>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    )}
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12}>
                <Card>
                  <CardContent>
                    {!transferDetail ? (
                      <EmptyState title="Select a request" subtitle="Open a transfer request to process approvals, pick/pack, receive, or cancel." />
                    ) : (
                      <Stack spacing={2}>
                        <Typography variant="h6">
                          Transfer #{transferDetail.transfer.id} | {transferDetail.transfer.from_clinic_name} to {transferDetail.transfer.to_clinic_name}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">Status: {transferDetail.transfer.status}</Typography>
                        <Table size="small">
                          <TableHead>
                            <TableRow>
                              <TableCell>Item</TableCell>
                              <TableCell>Requested</TableCell>
                              <TableCell>Approved</TableCell>
                              <TableCell>Picked</TableCell>
                              <TableCell>Received</TableCell>
                              <TableCell>Cancelled</TableCell>
                              <TableCell>Status</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {transferDetail.lines.map((line) => (
                              <TableRow key={line.id}>
                                <TableCell>{line.sku_code} - {line.name}</TableCell>
                                <TableCell>{line.requested_qty}</TableCell>
                                <TableCell>{line.approved_qty}</TableCell>
                                <TableCell>{line.picked_qty}</TableCell>
                                <TableCell>{line.received_qty}</TableCell>
                                <TableCell>{line.cancelled_qty}</TableCell>
                                <TableCell>{line.line_status}</TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                        <Grid container spacing={1.5}>
                          <Grid item xs={12} md={3}>
                            <Stack component="form" spacing={1} onSubmit={approveTransfer}>
                              <Typography variant="subtitle2">Approve (Admin)</Typography>
                              <Select value={forms.transferApprove.decision} onChange={(e) => setField('transferApprove', 'decision', e.target.value)}>
                                <MenuItem value="APPROVE">APPROVE</MenuItem>
                                <MenuItem value="REJECT">REJECT</MenuItem>
                              </Select>
                              <TextField label="Item ID (optional)" value={forms.transferApprove.item_id} onChange={(e) => setField('transferApprove', 'item_id', e.target.value)} />
                              <TextField label="Approved Qty (optional)" value={forms.transferApprove.approved_qty} onChange={(e) => setField('transferApprove', 'approved_qty', e.target.value)} />
                              <Button type="submit" variant="outlined">Submit</Button>
                            </Stack>
                          </Grid>
                          <Grid item xs={12} md={3}>
                            <Stack component="form" spacing={1} onSubmit={pickPackTransfer}>
                              <Typography variant="subtitle2">Pick / Pack</Typography>
                              <TextField label="Item ID" value={forms.transferPick.item_id} onChange={(e) => setField('transferPick', 'item_id', e.target.value)} />
                              <TextField label="Qty" value={forms.transferPick.quantity} onChange={(e) => setField('transferPick', 'quantity', e.target.value)} />
                              <TextField label="Notes" value={forms.transferPick.notes} onChange={(e) => setField('transferPick', 'notes', e.target.value)} />
                              <Button type="submit" variant="outlined">Submit</Button>
                            </Stack>
                          </Grid>
                          <Grid item xs={12} md={3}>
                            <Stack component="form" spacing={1} onSubmit={receiveTransfer}>
                              <Typography variant="subtitle2">Receive</Typography>
                              <TextField label="Item ID" value={forms.transferReceive.item_id} onChange={(e) => setField('transferReceive', 'item_id', e.target.value)} />
                              <TextField label="Qty" value={forms.transferReceive.quantity} onChange={(e) => setField('transferReceive', 'quantity', e.target.value)} />
                              <TextField label="Notes" value={forms.transferReceive.notes} onChange={(e) => setField('transferReceive', 'notes', e.target.value)} />
                              <Button type="submit" variant="outlined">Submit</Button>
                            </Stack>
                          </Grid>
                          <Grid item xs={12} md={3}>
                            <Stack component="form" spacing={1} onSubmit={cancelTransfer}>
                              <Typography variant="subtitle2">Cancel</Typography>
                              <TextField label="Item ID" value={forms.transferCancel.item_id} onChange={(e) => setField('transferCancel', 'item_id', e.target.value)} />
                              <TextField label="Qty" value={forms.transferCancel.quantity} onChange={(e) => setField('transferCancel', 'quantity', e.target.value)} />
                              <TextField label="Reason" value={forms.transferCancel.reason} onChange={(e) => setField('transferCancel', 'reason', e.target.value)} />
                              <Button type="submit" variant="outlined" color="error">Submit</Button>
                            </Stack>
                          </Grid>
                        </Grid>
                      </Stack>
                    )}
                  </CardContent>
                </Card>
              </Grid>
            </Grid>
          )}
          {view === 'reorder' && (
            <Stack spacing={2}>
              <Card>
                <CardContent>
                  <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.2} alignItems={{ xs: 'stretch', md: 'center' }}>
                    <TextField
                      select
                      label="Clinic"
                      value={reorderFilters.clinic_id}
                      onChange={(e) => setReorderFilters((prev) => ({ ...prev, clinic_id: e.target.value }))}
                      sx={{ minWidth: 220 }}
                    >
                      <MenuItem value="">All Clinics</MenuItem>
                      {reorderMeta.clinics.map((clinic) => (
                        <MenuItem key={clinic.id} value={String(clinic.id)}>{clinic.name}</MenuItem>
                      ))}
                    </TextField>
                    <TextField
                      label="Location Code"
                      value={reorderFilters.location_code}
                      onChange={(e) => setReorderFilters((prev) => ({ ...prev, location_code: e.target.value }))}
                      sx={{ minWidth: 180 }}
                    />
                    <Button variant="contained" onClick={() => withLoading(loadReorderRecommendations)}>Generate Recommendations</Button>
                  </Stack>
                </CardContent>
              </Card>

              <Card>
                <CardContent>
                  <Typography variant="h6" gutterBottom>Network Reorder Recommendations</Typography>
                  {reorderRows.length === 0 ? (
                    <EmptyState title="No recommendations" subtitle="No shortages detected for current filters." />
                  ) : (
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>Clinic</TableCell>
                          <TableCell>SKU</TableCell>
                          <TableCell align="right">Shortage</TableCell>
                          <TableCell align="right">Lead-Time Demand</TableCell>
                          <TableCell align="right">Network Excess</TableCell>
                          <TableCell align="right">Transfer</TableCell>
                          <TableCell align="right">Purchase</TableCell>
                          <TableCell>Type</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {reorderRows.map((row) => (
                          <TableRow key={`${row.clinic_id}-${row.item_master_id}-${row.location_code}`}>
                            <TableCell>{row.clinic_name || row.clinic_id}</TableCell>
                            <TableCell>{row.sku_code}</TableCell>
                            <TableCell align="right">{row.shortage_vs_par}</TableCell>
                            <TableCell align="right">{row.lead_time_demand}</TableCell>
                            <TableCell align="right">{row.network_excess_qty}</TableCell>
                            <TableCell align="right">{row.suggested_transfer_qty}</TableCell>
                            <TableCell align="right">{row.suggested_purchase_qty}</TableCell>
                            <TableCell>
                              <Tooltip title={row.rationale || ''}>
                                <Chip
                                  size="small"
                                  color={row.recommendation_type === 'PURCHASE' ? 'warning' : row.recommendation_type === 'TRANSFER_ONLY' ? 'success' : 'info'}
                                  label={row.recommendation_type}
                                />
                              </Tooltip>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </CardContent>
              </Card>
            </Stack>
          )}
          {view === 'sync' && (
            <Stack spacing={2}>
              <Card>
                <CardContent>
                  <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.2} alignItems={{ xs: 'stretch', md: 'center' }} justifyContent="space-between">
                    <Typography variant="h6">Offline Sync Engine</Typography>
                    <Stack direction="row" spacing={1}>
                      <Chip size="small" color={syncStatus.online ? 'success' : 'warning'} label={syncStatus.online ? 'Online' : 'Offline'} />
                      <Button variant="contained" onClick={() => withLoading(runSyncNow)}>Run Sync Now</Button>
                    </Stack>
                  </Stack>
                  <Typography variant="caption" color="text.secondary">
                    Device: {getDeviceId()} | Last run: {syncStatus.lastRun ? new Date(syncStatus.lastRun).toLocaleString() : 'never'}
                  </Typography>
                  {syncStatus.message && <Alert sx={{ mt: 1 }} severity="info">{syncStatus.message}</Alert>}
                </CardContent>
              </Card>

              <Grid container spacing={2}>
                <Grid item xs={12} lg={6}>
                  <Card>
                    <CardContent>
                      <Typography variant="h6">Outbox Queue</Typography>
                      {syncOutboxRows.length === 0 ? (
                        <EmptyState title="Outbox empty" subtitle="No pending offline actions." />
                      ) : (
                        <Table size="small">
                          <TableHead>
                            <TableRow>
                              <TableCell>ID</TableCell>
                              <TableCell>Action</TableCell>
                              <TableCell>Path</TableCell>
                              <TableCell>Created</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {syncOutboxRows.map((row) => (
                              <TableRow key={row.id}>
                                <TableCell>{row.id}</TableCell>
                                <TableCell>{row.action_type}</TableCell>
                                <TableCell>{row.path}</TableCell>
                                <TableCell>{new Date(row.created_at).toLocaleString()}</TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      )}
                    </CardContent>
                  </Card>
                </Grid>
                <Grid item xs={12} lg={6}>
                  <Card>
                    <CardContent>
                      <Typography variant="h6">Conflict Queue</Typography>
                      {syncConflictRows.length === 0 ? (
                        <EmptyState title="No conflicts" subtitle="Sync conflicts and rejections will appear here." />
                      ) : (
                        <Stack spacing={1}>
                          {syncConflictRows.slice(0, 40).map((row) => (
                            <Alert key={row.id} severity="warning" action={<Button size="small" onClick={async () => { await clearConflictItem(row.id); await refreshSyncQueues(); }}>Dismiss</Button>}>
                              {row.conflict_code || 'CONFLICT'} | {row.idempotency_key || row.sync_inbox_id}
                            </Alert>
                          ))}
                        </Stack>
                      )}
                    </CardContent>
                  </Card>
                </Grid>
              </Grid>
            </Stack>
          )}
          {view === 'print' && <Card><CardContent><Stack direction="row" spacing={1} sx={{ mb: 2 }}><Button onClick={loadPrintJobs}>Load Print Jobs</Button></Stack>{loading ? <Skeleton variant="rounded" height={120} /> : printJobs.length === 0 ? <EmptyState title="No print jobs" subtitle="Load queue entries to inspect status." /> : <Table size="small"><TableHead><TableRow><TableCell>ID</TableCell><TableCell>Type</TableCell><TableCell>Status</TableCell></TableRow></TableHead><TableBody>{printJobs.map((j) => <TableRow key={j.id}><TableCell>{j.id}</TableCell><TableCell>{j.label_type}</TableCell><TableCell>{j.status}</TableCell></TableRow>)}</TableBody></Table>}</CardContent></Card>}
          {view === 'assistant' && <Card><CardContent><Typography variant="h6" gutterBottom>AI Assistant</Typography><Stack component="form" spacing={2} onSubmit={askAssistant}><TextField label="Question" value={forms.assistant.message} onChange={(e) => setField('assistant', 'message', e.target.value)} /><Button type="submit">Ask</Button></Stack><Divider sx={{ my: 2 }} />{assistantMessages.length === 0 ? <EmptyState title="No conversation" subtitle="Ask navigation and workflow questions." /> : <Stack spacing={1}>{assistantMessages.map((m, idx) => <Alert key={idx} severity={m.role === 'assistant' ? 'info' : 'success'}>{m.content}</Alert>)}</Stack>}</CardContent></Card>}
            </motion.div>
          </AnimatePresence>
        </Stack>
      

      {hasCapability('AI_ASSISTANT') && (
        <>
          <Fab
            color="primary"
            onClick={() => setCopilotOpen(true)}
            aria-label="Open AI copilot panel"
            sx={{
              position: 'fixed',
              right: 24,
              bottom: 24,
              boxShadow: '0 0 22px rgba(15,95,255,0.45)',
            }}
          >
            <Bot size={19} />
          </Fab>

          <Drawer
            anchor="right"
            open={copilotOpen}
            onClose={() => setCopilotOpen(false)}
            sx={{ '& .MuiDrawer-paper': { width: { xs: '100%', sm: 430 }, p: 2, borderLeft: '1px solid', borderColor: 'divider' } }}
          >
            <Stack spacing={1.5} sx={{ height: '100%' }}>
              <Typography variant="h6">AI Copilot</Typography>
              <Typography variant="body2" color="text.secondary">
                Context: {view} | Role: {user?.role}
              </Typography>
              <Alert severity="info" icon={<AlertTriangle size={16} />}>{AI_DISCLAIMER}</Alert>

              <Box sx={{ flex: 1, overflowY: 'auto', pr: 0.5 }}>
                <Stack spacing={1.2}>
                  {copilotMessages.map((m, idx) => (
                    <Card key={`${idx}-${m.role}`} sx={{ background: m.role === 'assistant' ? 'rgba(15,95,255,0.06)' : theme.palette.background.paper }}>
                      <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
                        <Typography variant="caption" color="text.secondary">{m.role === 'assistant' ? 'Copilot' : 'You'}</Typography>
                        <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
                          {m.content}
                          {m.streaming && <TypingIndicator />}
                        </Typography>
                        {m.action && (
                          <Button size="small" sx={{ mt: 1 }} onClick={() => executeSafeAction(m.action)}>
                            {m.action.mutates ? 'Review Action' : 'Run Action'}
                          </Button>
                        )}
                      </CardContent>
                    </Card>
                  ))}
                </Stack>
              </Box>

              <Stack direction="row" spacing={1}>
                <TextField
                  fullWidth
                  size="small"
                  value={copilotInput}
                  onChange={(e) => setCopilotInput(e.target.value)}
                  placeholder="Ask Copilot..."
                />
                <Button
                  variant="contained"
                  disabled={copilotStreaming}
                  onClick={() => {
                    sendCopilot(copilotInput, 'panel');
                    setCopilotInput('');
                  }}
                >
                  Send
                </Button>
              </Stack>

              <Divider />
              <Typography variant="subtitle2">AI Usage Log</Typography>
              <Box sx={{ maxHeight: 130, overflowY: 'auto' }}>
                {aiUsageLog.length === 0 ? (
                  <Typography variant="caption" color="text.secondary">No AI interactions logged yet.</Typography>
                ) : (
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>Intent</TableCell>
                        <TableCell>Page</TableCell>
                        <TableCell>Status</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {aiUsageLog.slice(0, 10).map((row, idx) => (
                        <TableRow key={idx}>
                          <TableCell>{row.intent}</TableCell>
                          <TableCell>{row.page}</TableCell>
                          <TableCell>{row.status}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </Box>
            </Stack>
          </Drawer>

          <Dialog open={confirmOpen} onClose={() => setConfirmOpen(false)} fullWidth maxWidth="xs">
            <DialogTitle>Confirm AI Action</DialogTitle>
            <DialogContent>
              <Typography variant="body2">
                The AI requested a data-changing action: <strong>{pendingAction?.label}</strong>.
              </Typography>
              <Typography variant="caption" color="text.secondary">
                This will mutate inventory/cycle data. Proceed only if intended.
              </Typography>
            </DialogContent>
            <DialogActions>
              <Button onClick={() => setConfirmOpen(false)}>Cancel</Button>
              <Button variant="contained" color="error" onClick={runConfirmedAction}>Confirm</Button>
            </DialogActions>
          </Dialog>

          <Dialog open={logoutConfirmOpen} onClose={() => setLogoutConfirmOpen(false)} fullWidth maxWidth="xs">
            <DialogTitle>Confirm Logout</DialogTitle>
            <DialogContent>
              <Typography variant="body2">
                End this session on this device?
              </Typography>
            </DialogContent>
            <DialogActions>
              <Button onClick={() => setLogoutConfirmOpen(false)}>Cancel</Button>
              <Button
                variant="contained"
                color="error"
                onClick={() => {
                  setLogoutConfirmOpen(false);
                  logout();
                }}
              >
                Logout
              </Button>
            </DialogActions>
          </Dialog>
        </>
      )}

      <Snackbar
        open={toast.open}
        autoHideDuration={toast.severity === 'error' ? 5000 : 3000}
        onClose={() => {
          setToast((prev) => ({ ...prev, open: false }));
          if (status) setStatus('');
          if (error) setError('');
        }}
      >
        <Alert
          variant="filled"
          onClose={() => {
            setToast((prev) => ({ ...prev, open: false }));
            if (status) setStatus('');
            if (error) setError('');
          }}
          severity={toast.severity}
          sx={{ minWidth: 280 }}
        >
          {toast.message}
        </Alert>
      </Snackbar>
    </AppLayout>
  );
}

export default App;
