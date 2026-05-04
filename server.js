// Load .env for local development (requires 'dotenv' to be installed: npm install dotenv)
try { require('dotenv').config(); } catch (_) {}

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const http = require('http'); // Built-in HTTP for createServer
const https = require('https');
const crypto = require('crypto'); // Built-in node crypto for password hashing
const dns = require('dns').promises; // Built-in DNS for MX checks
const net = require('net'); // Built-in Net for SMTP handshake
const nodemailer = require('nodemailer'); // Added for sending emails

// ── Resolve rate_limits.json path ────────────────────────────────────────────
// Priority: RATE_LIMITS_PATH env var > same directory as server.js > one level up > two levels up.
// Two-level-up fallback supports installs where server.js lives in
// <root>/Candidate Analyser/backend/ while rate_limits.json is at <root>/
// (e.g. F:\Recruiting Tools\Autosourcing\rate_limits.json with server.js two
// subdirectories below it).
const RATE_LIMITS_PATH = (() => {
  if (process.env.RATE_LIMITS_PATH) return process.env.RATE_LIMITS_PATH;
  const local = path.join(__dirname, 'rate_limits.json');
  try { fs.accessSync(local, fs.constants.R_OK); return local; } catch (_) {}
  const oneUp = path.join(__dirname, '..', 'rate_limits.json');
  try { fs.accessSync(oneUp, fs.constants.R_OK); return oneUp; } catch (_) {}
  return path.join(__dirname, '..', '..', 'rate_limits.json');
})();

// ── ICS URL store path: ICS_.json (same directory resolution as rate_limits.json) ─
const ICS_URLS_PATH = (() => {
  if (process.env.ICS_URLS_PATH) return process.env.ICS_URLS_PATH;
  const local = path.join(__dirname, 'ICS_.json');
  try { fs.accessSync(local, fs.constants.R_OK); return local; } catch (_) {}
  const oneUp = path.join(__dirname, '..', 'ICS_.json');
  try { fs.accessSync(oneUp, fs.constants.R_OK); return oneUp; } catch (_) {}
  return local; // default to __dirname when file does not yet exist
})();

// ── System config: read once at startup from rate_limits.json (system section) ──
// Priority: process.env override > rate_limits.json system section > hardcoded default.
// This lets operators tune behaviour via admin_rate_limits.html without editing source.
const _RL_PATH_STARTUP = RATE_LIMITS_PATH;
const _SYS = (() => {
  try { return JSON.parse(fs.readFileSync(_RL_PATH_STARTUP, 'utf8')).system || {}; }
  catch (_) { return {}; }
})();

// ── Token credit/deduction config: read once at startup from rate_limits.json (tokens section) ──
const _TOKENS = (() => {
  try { return JSON.parse(fs.readFileSync(_RL_PATH_STARTUP, 'utf8')).tokens || {}; }
  catch (_) { return {}; }
})();

// Configurable token credit/deduction constants (env var > rate_limits.json tokens > hardcoded default)
const _APPEAL_APPROVE_CREDIT     = parseInt(process.env.APPEAL_APPROVE_CREDIT, 10)     || _TOKENS.appeal_approve_credit     || 1;
const _VERIFIED_SELECTION_DEDUCT = parseInt(process.env.VERIFIED_SELECTION_DEDUCT, 10) || _TOKENS.verified_selection_deduct || 2;
const _CONTACT_GEN_DEDUCT        = parseInt(process.env.CONTACT_GEN_DEDUCT, 10)        || _TOKENS.contact_gen_deduct        || 2;
const _TOKEN_COST_SGD            = parseFloat(process.env.TOKEN_COST_SGD)               || _TOKENS.token_cost_sgd            || 0.10;

// Configurable server parameters (env var takes highest priority, then rate_limits.json system, then default)
const _BACKOFF_MAX_RETRIES       = parseInt(process.env.BACKOFF_MAX_RETRIES, 10)        || _SYS.backoff_max_retries       || 3;
const _BACKOFF_BASE_DELAY_MS     = parseInt(process.env.BACKOFF_BASE_DELAY_MS, 10)      || _SYS.backoff_base_delay_ms     || 500;
const _SSE_HEARTBEAT_MS          = parseInt(process.env.SSE_HEARTBEAT_MS, 10)           || _SYS.sse_heartbeat_ms          || 30000;
const _SSE_COALESCE_DELAY_MS     = parseInt(process.env.SSE_COALESCE_DELAY_MS, 10)      || _SYS.sse_coalesce_delay_ms     || 150;
const _SMTP_MAX_CONNECTIONS      = parseInt(process.env.SMTP_MAX_CONNECTIONS, 10)       || _SYS.smtp_max_connections      || 3;
const _SESSION_COOKIE_MAX_AGE_MS = parseInt(process.env.SESSION_COOKIE_MAX_AGE_MS, 10) || _SYS.session_cookie_max_age_ms || 2592000000;
const _SCHEDULER_DEFAULT_DURATION  = parseInt(process.env.SCHEDULER_DEFAULT_DURATION, 10)  || _SYS.scheduler_default_duration  || 30;
const _SCHEDULER_DEFAULT_MAX_SLOTS = parseInt(process.env.SCHEDULER_DEFAULT_MAX_SLOTS, 10) || _SYS.scheduler_default_max_slots || 1000;
const _PORTING_UPLOAD_MAX_BYTES  = parseInt(process.env.PORTING_UPLOAD_MAX_BYTES, 10)   || _SYS.porting_upload_max_bytes  || 1024 * 1024;
const _DASHBOARD_DEFAULT_REQUESTS       = parseInt(process.env.DEFAULT_DASHBOARD_REQUESTS, 10)        || _SYS.dashboard_default_requests       || 50;
const _DASHBOARD_DEFAULT_WINDOW_SECONDS = parseInt(process.env.DEFAULT_DASHBOARD_WINDOW_SECONDS, 10)  || _SYS.dashboard_default_window_seconds || 60;

// ── AI Autofix pipeline modules (lazy-loaded so server starts even if optional) ──
let _aiAutofix = null;
let _gitops    = null;
let _applyPatch = null;
try { _aiAutofix  = require('./server/ai_autofix');         } catch (_) {}
try { _gitops     = require('./server/gitops');             } catch (_) {}
try { _applyPatch = require('./server/apply_patch_endpoint'); } catch (_) {}

// ── Structured error logger (writes JSONL to shared log dir) ─────────────────
const _LOG_DIR = process.env.AUTOSOURCING_LOG_DIR || String.raw`F:\Recruiting Tools\Autosourcing\log`;
// ── Appeal archive directory (pending appeals saved here on DB Dock Out) ─────
const APPEAL_ARCHIVE_DIR = process.env.APPEAL_ARCHIVE_DIR || String.raw`F:\Recruiting Tools\Autosourcing\Appeal`;
// All timestamps use Singapore Standard Time (UTC+8) per organisational logging policy.
function _sgtISO() {
  const now = new Date();
  const sgt = new Date(now.getTime() + 8 * 60 * 60 * 1000);
  return sgt.toISOString().replace('Z', '+08:00');
}
function _writeLogEntry(filePrefix, entry) {
  try {
    fs.mkdirSync(_LOG_DIR, { recursive: true });
    const ts = _sgtISO();
    const date = ts.slice(0, 10);
    const logFile = path.join(_LOG_DIR, `${filePrefix}_${date}.txt`);
    const line = JSON.stringify({ timestamp: ts, ...entry });
    // Non-blocking append — never stall the event loop for a log write.
    fs.appendFile(logFile, line + '\n', 'utf8', err => {
      if (err) console.error('[_writeLogEntry] append error:', err.message);
    });
  } catch (_) { /* never crash the server over a log write */ }
}
function _writeErrorLog(entry)    { _writeLogEntry('error_capture', entry); }
function _writeApprovalLog(entry) { _writeLogEntry('human_approval', entry); }
function _writeInfraLog(entry)    { _writeLogEntry('infrastructure_byok', entry); }
function _writeFinancialLog(entry) { _writeLogEntry('financial_credits', entry); }

// Lazy-load Gemini SDK so the server still boots if it isn't installed
let GoogleGenerativeAIClass = null;
try {
  ({ GoogleGenerativeAI: GoogleGenerativeAIClass } = require('@google/generative-ai'));
} catch (e) {
  console.warn("[WARN] '@google/generative-ai' not installed. /verify-data will return an informative error until it's installed.");
}

// Lazy-load OpenAI SDK
let OpenAIClass = null;
try {
  ({ OpenAI: OpenAIClass } = require('openai'));
} catch (_) {}

// Lazy-load Anthropic SDK
let AnthropicClass = null;
try {
  AnthropicClass = require('@anthropic-ai/sdk').default || require('@anthropic-ai/sdk');
} catch (_) {}

// Lazy-load Google APIs for Looker/Sheets integration
let google = null;
try {
  ({ google } = require('googleapis'));
} catch (e) {
  console.warn("[WARN] 'googleapis' not installed. Port to Looker Studio features will fail.");
}

const app = express();
const port = 4000;
const PBKDF2_ITERATIONS = 260000; // Iteration count for pbkdf2:sha256 employee password hashing

// ── Exponential back-off helper ───────────────────────────────────────────────
// Retries `fn` up to `maxRetries` times when the error looks transient
// (HTTP 429 / 503 from Google APIs or Gemini rate-limit responses).
async function withExponentialBackoff(fn, { maxRetries = _BACKOFF_MAX_RETRIES, baseDelayMs = _BACKOFF_BASE_DELAY_MS, label = 'op' } = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const status = (err.response && err.response.status) || (err.status) || err.code;
      const isRetryable = status === 429 || status === 503 || status === 'ECONNRESET' || status === 'ETIMEDOUT';
      if (!isRetryable || attempt === maxRetries) throw err;
      const delay = baseDelayMs * Math.pow(2, attempt) + Math.random() * 200;
      console.warn(`[${label}] transient error (${status}), retry ${attempt + 1}/${maxRetries} in ${Math.round(delay)} ms`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  throw lastErr;
}

// ── Gemini model instance cache ───────────────────────────────────────────────
// Avoids creating a new GoogleGenerativeAI client + model object on every request.
const _geminiModelCache = new Map(); // apiKey → model
function getGeminiModel(apiKey, modelName = 'gemini-2.5-flash-lite') {
  const cacheKey = `${apiKey}:${modelName}`;
  if (_geminiModelCache.has(cacheKey)) return _geminiModelCache.get(cacheKey);
  const genAI = new GoogleGenerativeAIClass(apiKey);
  const model = genAI.getGenerativeModel({ model: modelName });
  _geminiModelCache.set(cacheKey, model);
  return model;
}

// ── OpenAI / Anthropic client instance caches ────────────────────────────────
// Avoid recreating SDK client objects on every LLM call; cache by API key.
const _openaiClientCache    = new Map(); // apiKey → OpenAIClass instance
const _anthropicClientCache = new Map(); // apiKey → AnthropicClass instance

function _getOpenAIClient(apiKey) {
  if (!_openaiClientCache.has(apiKey)) _openaiClientCache.set(apiKey, new OpenAIClass({ apiKey }));
  return _openaiClientCache.get(apiKey);
}
function _getAnthropicClient(apiKey) {
  if (!_anthropicClientCache.has(apiKey)) _anthropicClientCache.set(apiKey, new AnthropicClass({ apiKey }));
  return _anthropicClientCache.get(apiKey);
}

// ── LLM concurrency semaphore ─────────────────────────────────────────────────
// Prevents unbounded parallelism of expensive LLM calls (Gemini / OpenAI / Anthropic).
// Cap via LLM_MAX_CONCURRENT env var (default 5).
const _LLM_MAX_CONCURRENT = parseInt(process.env.LLM_MAX_CONCURRENT, 10) || 5;
let _llmInFlight = 0;
const _llmWaitQueue = [];
function _llmAcquire() {
  if (_llmInFlight < _LLM_MAX_CONCURRENT) { _llmInFlight++; return Promise.resolve(); }
  // Waiter inherits the slot: _llmInFlight is NOT decremented in _llmRelease when
  // there are waiters, so the counter stays consistent without a race window.
  return new Promise(resolve => _llmWaitQueue.push(resolve));
}
function _llmRelease() {
  if (_llmWaitQueue.length > 0) {
    // Pass the slot directly to the next waiter — no decrement/re-increment needed.
    _llmWaitQueue.shift()();
  } else {
    _llmInFlight--;
  }
}

// Returns the Gemini model name for a user.
// For BYOK users, uses their saved preference; all others use the global LLM config default.
const ALLOWED_GEMINI_MODELS = [
  'gemini-3.1-pro', 'gemini-3-flash', 'gemini-3.1-flash-lite',
  'gemini-2.5-pro', 'gemini-2.5-flash', 'gemini-2.5-flash-lite',
  'gemini-2.0-flash', 'gemini-2.0-flash-lite',
];

/** Read the active Gemini model from llm_provider_config.json (sync, cached for 60s). */
let _llmCfgCache = null, _llmCfgCacheTs = 0;
function _readLlmProviderGeminiModel() {
  const now = Date.now();
  if (_llmCfgCache && now - _llmCfgCacheTs < 60_000) return _llmCfgCache;
  try {
    const cfgPath = path.join(__dirname, 'llm_provider_config.json');
    const cfg = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
    const model = (cfg.gemini && cfg.gemini.model) || 'gemini-2.5-flash-lite';
    _llmCfgCache = ALLOWED_GEMINI_MODELS.includes(model) ? model : 'gemini-2.5-flash-lite';
  } catch (_) {
    _llmCfgCache = 'gemini-2.5-flash-lite';
  }
  _llmCfgCacheTs = now;
  return _llmCfgCache;
}

// Per-username TTL cache for resolveGeminiModel — avoids a DB roundtrip on every LLM call.
// Keyed by username; each entry is { model: string, ts: number }.
// Short TTL (30 s) so model changes propagate quickly.
const _resolveGeminiModelCache = new Map();
const _RESOLVE_GEMINI_MODEL_TTL = parseInt(process.env.RESOLVE_GEMINI_MODEL_TTL, 10) || 30_000;

async function resolveGeminiModel(username) {
  if (!username) return _readLlmProviderGeminiModel();
  const now = Date.now();
  const cached = _resolveGeminiModelCache.get(username);
  if (cached && now - cached.ts < _RESOLVE_GEMINI_MODEL_TTL) return cached.model;
  let model = _readLlmProviderGeminiModel();
  try {
    const r = await pool.query(
      'SELECT gemini_model, useraccess FROM login WHERE username = $1 LIMIT 1', [username]
    );
    if (r.rows.length > 0 && (r.rows[0].useraccess || '').toLowerCase() === 'byok') {
      const m = r.rows[0].gemini_model;
      if (ALLOWED_GEMINI_MODELS.includes(m)) model = m;
    }
  } catch (_) {}
  _resolveGeminiModelCache.set(username, { model, ts: now });
  return model;
}

// ── Provider-agnostic LLM text generation ────────────────────────────────────
// Reads llm_provider_config.json to determine the active provider, then routes
// to OpenAI, Anthropic, or Gemini accordingly. Falls back to Gemini if no other
// provider is configured or available.
let _fullLlmCfgCache = null, _fullLlmCfgCacheTs = 0;
function _readFullLlmConfig() {
  const now = Date.now();
  if (_fullLlmCfgCache && now - _fullLlmCfgCacheTs < 60_000) return _fullLlmCfgCache;
  try {
    const cfgPath = path.join(__dirname, 'llm_provider_config.json');
    _fullLlmCfgCache = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
  } catch (_) {
    _fullLlmCfgCache = {};
  }
  _fullLlmCfgCacheTs = now;
  return _fullLlmCfgCache;
}

/**
 * Generate text using the active LLM provider (Gemini / OpenAI / Anthropic).
 * Provider priority: openai → anthropic → gemini, based on llm_provider_config.json.
 * @param {string} prompt
 * @param {{ username?: string, label?: string }} opts
 * @returns {Promise<string>}
 */
async function llmGenerateText(prompt, opts = {}) {
  const { username, label = 'llm' } = opts;
  const cfg = _readFullLlmConfig();

  // Find the first enabled provider with an API key AND an installed SDK
  let activeProvider = 'gemini'; // default
  for (const p of ['openai', 'anthropic', 'gemini']) {
    const pcfg = cfg[p] || {};
    if (pcfg.enabled && pcfg.api_key) {
      if (p === 'openai' && !OpenAIClass) continue;
      if (p === 'anthropic' && !AnthropicClass) continue;
      activeProvider = p;
      break;
    }
  }

  // Acquire concurrency slot before making the (potentially slow) LLM API call.
  await _llmAcquire();
  try {
    if (activeProvider === 'openai') {
      if (!OpenAIClass) throw new Error('OpenAI SDK not installed');
      const apiKey = (cfg.openai || {}).api_key || '';
      if (!apiKey) throw new Error('OpenAI API key not configured');
      const model = (cfg.openai || {}).model || 'gpt-4.1';
      const maxTokens = parseInt((cfg.openai || {}).max_tokens, 10) || 2048;
      const client = _getOpenAIClient(apiKey);
      const resp = await withExponentialBackoff(
        () => client.chat.completions.create({ model, messages: [{ role: 'user', content: prompt }], temperature: 0, max_tokens: maxTokens }),
        { label }
      );
      return (resp.choices[0]?.message?.content || '').trim();
    }

    if (activeProvider === 'anthropic') {
      if (!AnthropicClass) throw new Error('Anthropic SDK not installed');
      const apiKey = (cfg.anthropic || {}).api_key || '';
      if (!apiKey) throw new Error('Anthropic API key not configured');
      const model = (cfg.anthropic || {}).model || 'claude-sonnet-4-5';
      const client = _getAnthropicClient(apiKey);
      const resp = await withExponentialBackoff(
        () => client.messages.create({ model, max_tokens: 4096, messages: [{ role: 'user', content: prompt }] }),
        { label }
      );
      const block = resp.content && resp.content[0];
      return (block && block.type === 'text' ? block.text : '').trim();
    }

    // Fallback: Gemini
    const geminiApiKey = (cfg.gemini || {}).api_key || process.env.GOOGLE_API_KEY || '';
    if (!geminiApiKey) throw new Error('No LLM API key configured');
    if (!GoogleGenerativeAIClass) throw new Error('Gemini SDK not installed');
    const modelName = await resolveGeminiModel(username);
    const model = getGeminiModel(geminiApiKey, modelName);
    const geminiMaxTokens = parseInt((cfg.gemini || {}).max_tokens, 10) || 2048;
    // Object-based GenerateContentRequest (supported since @google/generative-ai v0.1.1+)
    // passes generationConfig alongside contents so the SDK honours maxOutputTokens.
    const result = await withExponentialBackoff(() => model.generateContent({
      contents: [{ role: 'user', parts: [{ text: prompt }] }],
      generationConfig: { maxOutputTokens: geminiMaxTokens }
    }), { label });
    return result.response.text().trim();
  } finally {
    _llmRelease();
  }
}

// ── AI Comp in-memory result cache (24h TTL) ──────────────────────────────────
// Avoids redundant LLM API calls when the same candidate profile is re-estimated.
// Key = "company|jobtitle|seniority|country|sector" (all lower-cased / trimmed).
const _aiCompCache = new Map(); // cacheKey → { compensation, ts }
const AI_COMP_CACHE_TTL_MS = 24 * 3600 * 1000; // 24 hours
function _aiCompCacheKey(r) {
  return [r.company, r.jobtitle, r.seniority, r.country, r.sector]
    .map(v => String(v || '').toLowerCase().trim())
    .join('|');
}
function _aiCompCacheGet(r) {
  const k = _aiCompCacheKey(r);
  const entry = _aiCompCache.get(k);
  if (!entry) return undefined;
  if (Date.now() - entry.ts > AI_COMP_CACHE_TTL_MS) { _aiCompCache.delete(k); return undefined; }
  return entry.compensation;
}
function _aiCompCacheSet(r, compensation) {
  _aiCompCache.set(_aiCompCacheKey(r), { compensation, ts: Date.now() });
}

// ── Nodemailer transporter pool ───────────────────────────────────────────────
// Reuses SMTP connections for the same host/port/user combination instead of
// creating a new transporter per email request.
const _smtpTransporterCache = new Map(); // configKey → transporter
function getOrCreateTransporter(transporterConfig) {
  const key = [
    transporterConfig.host || '',
    String(transporterConfig.port || ''),
    (transporterConfig.auth && transporterConfig.auth.user) || ''
  ].join('|');
  if (_smtpTransporterCache.has(key)) return _smtpTransporterCache.get(key);
  const t = nodemailer.createTransport({ ...transporterConfig, pool: true, maxConnections: _SMTP_MAX_CONNECTIONS });
  _smtpTransporterCache.set(key, t);
  return t;
}

// Enable parsing cookies
const cookieParser = require('cookie-parser');
app.use(cookieParser());

app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

// ── HTTP error capture middleware ─────────────────────────────────────────────
// Intercepts every response after it is sent. Responses with status >= 400 are
// written to the Error Capture log (4xx → warning, 5xx → critical).
const _HTTP_ERROR_SKIP = new Set(['/favicon.ico', '/admin/client-error', '/admin/logs']);
app.use((req, res, next) => {
  res.on('finish', () => {
    const sc = res.statusCode;
    if (sc >= 400 && req.method !== 'OPTIONS' && !_HTTP_ERROR_SKIP.has(req.path)) {
      const sev = sc >= 500 ? 'critical' : 'warning';
      const username = (req.cookies && req.cookies.username) || '';
      const ip = (req.headers['x-forwarded-for'] || req.ip || '').split(',')[0].trim();
      _writeErrorLog({
        source: 'server.js',
        severity: sev,
        endpoint: req.path,
        message: `${req.method} ${req.path} → HTTP ${sc}`,
        http_status: sc,
        username,
        ip_address: ip,
      });
    }
  });
  next();
});

// NEW: Serve images from 'image' directory
app.use('/image', express.static(path.join(__dirname, 'image')));
// Serve client-side UI modules (admin_ai_fix_snippet.js etc.)
app.use('/ui', express.static(path.join(__dirname, 'ui')));
// Serve jsPDF from local node_modules so the PDF export works without a CDN connection.
app.get('/vendor/jspdf.umd.min.js', dashboardRateLimit, (req, res) => {
  const jspdfPath = path.join(__dirname, 'node_modules', 'jspdf', 'dist', 'jspdf.umd.min.js');
  res.sendFile(jspdfPath, err => {
    if (err) res.status(404).end();
  });
});

// Serve LookerDashboard.html directly so it is same-origin as the API (avoids cross-origin cookie issues).
// When backend and frontend live in separate directories, set LOOKER_DASHBOARD_PATH in .env to the
// path of LookerDashboard.html relative to this file (e.g. ../frontend/src/LookerDashboard.html).
const lookerDashboardFile = process.env.LOOKER_DASHBOARD_PATH
  ? path.resolve(__dirname, process.env.LOOKER_DASHBOARD_PATH)
  : path.join(__dirname, '../frontend/src/LookerDashboard.html');

// In-memory rate-limiter for static-file routes. Limits are read from
// rate_limits.json (dashboard key) so they can be updated without a server restart.
const _dashboardHits = new Map();
function dashboardRateLimit(req, res, next) {
  const cfg = loadRateLimits();
  const feat = (cfg.defaults || {}).dashboard || {};
  const maxHits = parseInt(feat.requests, 10) || _DASHBOARD_DEFAULT_REQUESTS;
  const windowMs = (parseInt(feat.window_seconds, 10) || _DASHBOARD_DEFAULT_WINDOW_SECONDS) * 1000;

  const ip = req.ip || req.socket.remoteAddress || 'unknown';
  const now = Date.now();
  const entry = _dashboardHits.get(ip) || { count: 0, resetAt: now + windowMs };
  if (now > entry.resetAt) { entry.count = 0; entry.resetAt = now + windowMs; }
  entry.count++;
  _dashboardHits.set(ip, entry);
  if (entry.count > maxHits) {
    return res.status(429).json({ error: 'Too Many Requests' });
  }
  next();
}

app.get('/LookerDashboard.html', dashboardRateLimit, (req, res) => {
  res.sendFile(lookerDashboardFile);
});
app.get('/LookerDashboard', dashboardRateLimit, (req, res) => {
  res.sendFile(lookerDashboardFile);
});

// Serve porting HTML pages from this directory
app.get('/upload.html', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, 'upload.html'));
});
app.get('/api_porting.html', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, 'api_porting.html'));
});
app.get('/admin_rate_limits.html', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, 'admin_rate_limits.html'));
});
app.get('/sales_rep_register.html', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, 'sales_rep_register.html'));
});
app.get('/sales_rep_dashboard.html', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, 'sales_rep_dashboard.html'));
});
app.get('/community.html', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, '../frontend/src/community.html'));
});
// Serve shared nav assets used by community.html (and other pages) when accessed via localhost:4000
app.get('/nav-sidebar.css', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, '../frontend/src/nav-sidebar.css'));
});
app.get('/nav-sidebar.js', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, '../frontend/src/nav-sidebar.js'));
});
// Serve the FIOE brand logo used in the nav sidebar
app.get('/fioe-logo.svg', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, '../frontend/src/fioe-logo.svg'));
});
// Public self-scheduler booking page (no auth required).
// scheduler.html lives alongside LookerDashboard.html in the frontend/src directory.
const _schedulerHtmlPathMain = path.join(path.dirname(lookerDashboardFile), 'scheduler.html');
app.get('/scheduler.html', dashboardRateLimit, (req, res) => {
  res.sendFile(_schedulerHtmlPathMain);
});

// ── Per-user rate limiter ─────────────────────────────────────────────────────
// Reads per-user overrides from rate_limits.json (resolved via RATE_LIMITS_PATH above).
// Shared with webbridge.py — both servers read the same file.
const NO_LIMIT= parseInt(process.env.NO_LIMIT_SENTINEL, 10) || _SYS.no_limit_sentinel || 999999; // sentinel: effectively no limit when feature has no config entry
let _rateLimitsCache = null;
let _rateLimitsCacheTime = 0;
const RATE_LIMITS_CACHE_MS = parseInt(process.env.RATE_LIMITS_CACHE_MS, 10) || _SYS.rate_limits_cache_ms || 10000; // re-read at most every 10 s

function loadRateLimits() {
  const now = Date.now();
  if (_rateLimitsCache && now - _rateLimitsCacheTime < RATE_LIMITS_CACHE_MS) {
    return _rateLimitsCache;
  }
  try {
    const raw = fs.readFileSync(RATE_LIMITS_PATH, 'utf8');
    _rateLimitsCache = JSON.parse(raw);
    _rateLimitsCacheTime = now;
  } catch (_) {
    _rateLimitsCache = { defaults: {}, users: {} };
  }
  return _rateLimitsCache;
}

function saveRateLimits(config) {
  const tmp = RATE_LIMITS_PATH + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(config, null, 2), 'utf8');
  fs.renameSync(tmp, RATE_LIMITS_PATH);
  _rateLimitsCache = config;
  _rateLimitsCacheTime = Date.now();
}

// ── Email Verification Service Config ────────────────────────────────────────
// Two-level-up fallback: server.js may live in <root>/Candidate Analyser/backend/
// while email_verif_config.json sits at <root>/ (same dir as webbridge.py / admin_rate_limits.html).
// _EMAIL_VERIF_CONFIG_PATHS is searched in order on every read/write so that a
// file created after startup (e.g. by webbridge.py) is picked up immediately.
const _EMAIL_VERIF_CONFIG_PATHS = [
  process.env.EMAIL_VERIF_CONFIG_PATH,
  path.join(__dirname, 'email_verif_config.json'),
  path.join(__dirname, '..', 'email_verif_config.json'),
  path.join(__dirname, '..', '..', 'email_verif_config.json'),
].filter(Boolean);

const EMAIL_VERIF_SERVICES = ['neverbounce', 'zerobounce', 'bouncer'];
// ContactOut (contact generation) key is stored alongside email verif services in email_verif_config.json
// so that the same path-resolution logic (multi-path search) is used for all provider configs.
// It is intentionally NOT in EMAIL_VERIF_SERVICES so it does not affect hasCustomEmailVerif / token deduction.
const CONTACT_GEN_IN_EMAIL_VERIF = ['contactout', 'apollo', 'rocketreach'];

function _resolveEmailVerifConfigPath() {
  // Use env override if set.
  if (process.env.EMAIL_VERIF_CONFIG_PATH) return process.env.EMAIL_VERIF_CONFIG_PATH;
  // Return the first path that already has the file so we always read from
  // wherever webbridge.py (or the admin POST endpoint) last wrote it.
  for (const p of _EMAIL_VERIF_CONFIG_PATHS) {
    try { fs.accessSync(p, fs.constants.R_OK); return p; } catch (_) {}
  }
  // Default to two-levels-up (matches webbridge.py location) when not yet created.
  return _EMAIL_VERIF_CONFIG_PATHS[_EMAIL_VERIF_CONFIG_PATHS.length - 1];
}

const EXTERNAL_API_TIMEOUT_MS = parseInt(process.env.EXTERNAL_API_TIMEOUT_MS, 10) || _SYS.external_api_timeout_ms || 10000; // 10 s timeout for external email verification API calls

// ── Email Verif config in-memory TTL cache ──────────────────────────────────
// Re-read at most every EMAIL_VERIF_CFG_CACHE_MS (default 10 s) to avoid disk I/O on every request.
let _emailVerifCfgCache = null, _emailVerifCfgCacheTs = 0;
const EMAIL_VERIF_CFG_CACHE_MS = parseInt(process.env.EMAIL_VERIF_CFG_CACHE_MS, 10) || _SYS.email_verif_cfg_cache_ms || 10_000;

function loadEmailVerifConfig() {
  const now = Date.now();
  if (_emailVerifCfgCache && now - _emailVerifCfgCacheTs < EMAIL_VERIF_CFG_CACHE_MS) return _emailVerifCfgCache;
  const configPath = _resolveEmailVerifConfigPath();
  try {
    const raw = fs.readFileSync(configPath, 'utf8');
    _emailVerifCfgCache = JSON.parse(raw);
  } catch (_) {
    _emailVerifCfgCache = {
      neverbounce: { api_key: '', enabled: 'disabled' },
      zerobounce:  { api_key: '', enabled: 'disabled' },
      bouncer:     { api_key: '', enabled: 'disabled' },
    };
  }
  _emailVerifCfgCacheTs = now;
  return _emailVerifCfgCache;
}

function saveEmailVerifConfig(config) {
  const configPath = _resolveEmailVerifConfigPath();
  const tmp = configPath + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(config, null, 2), 'utf8');
  fs.renameSync(tmp, configPath);
  // Invalidate cache immediately so the next read sees the new config.
  _emailVerifCfgCache = config;
  _emailVerifCfgCacheTs = Date.now();
}

// Per-(username, feature) sliding-window state
const _userRateState = new Map(); // key: "username::feature" -> [ timestamp, ... ]

function isUserAllowed(username, feature) {
  if (!username) return true;
  const config = loadRateLimits();
  // When rate limits are globally disabled, allow all requests.
  if (config.rates_enabled === false) return true;
  const userLimits = (config.users || {})[username] || {};
  const defaultLimits = config.defaults || {};
  const limitCfg = userLimits[feature] || defaultLimits[feature];
  if (!limitCfg) return true;
  const maxReq = parseInt(limitCfg.requests, 10) || NO_LIMIT;
  const window  = (parseInt(limitCfg.window_seconds, 10) || 60) * 1000;
  const now = Date.now();
  const key = `${username}::${feature}`;
  let history = (_userRateState.get(key) || []).filter(t => now - t < window);
  if (history.length >= maxReq) {
    _userRateState.set(key, history);
    return false;
  }
  history.push(now);
  _userRateState.set(key, history);
  return true;
}

/** Express middleware factory for per-user rate limiting. */
function userRateLimit(feature) {
  return (req, res, next) => {
    const username = (req.cookies && req.cookies.username) || '';
    if (username && !isUserAllowed(username.trim(), feature)) {
      const config = loadRateLimits();
      const userLimits = (config.users || {})[username.trim()] || {};
      const defaultLimits = config.defaults || {};
      const cfg = (feature in userLimits) ? userLimits[feature] : defaultLimits[feature];
      return res.status(429).json({
        error: `Rate limit exceeded for '${feature}'`,
        feature,
        requests: cfg ? cfg.requests : undefined,
        window_seconds: cfg ? cfg.window_seconds : undefined,
      });
    }
    next();
  };
}

// ── Admin: require admin role ─────────────────────────────────────────────────
async function requireAdmin(req, res, next) {
  const username = (req.cookies && req.cookies.username) || '';
  if (!username) return res.status(401).json({ error: 'Authentication required' });
  try {
    const r = await pool.query('SELECT useraccess FROM login WHERE username = $1 LIMIT 1', [username]);
    if (!r.rows.length || (r.rows[0].useraccess || '').toLowerCase() !== 'admin') {
      return res.status(403).json({ error: 'Admin access required' });
    }
    next();
  } catch (err) {
    res.status(500).json({ error: 'Auth check failed: ' + err.message });
  }
}

// ── Admin: rate-limits CRUD ───────────────────────────────────────────────────
async function ensureAdminColumns() {
  const ddls = [
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS target_limit INTEGER DEFAULT 10`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS last_result_count INTEGER`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS last_deducted_role_tag TEXT`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS session TIMESTAMPTZ`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS google_refresh_token TEXT`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS google_token_expires TIMESTAMP`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS ms_refresh_token TEXT`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS ms_token_expires TIMESTAMP`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS corporation TEXT`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS useraccess TEXT`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS cse_query_count INTEGER DEFAULT 0`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS price_per_query NUMERIC(10,4) DEFAULT 0`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS gemini_query_count INTEGER DEFAULT 0`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS price_per_gemini_query NUMERIC(10,4) DEFAULT 0`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS gemini_model TEXT DEFAULT 'gemini-2.5-flash-lite'`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS bd TEXT`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS session_id TEXT`,
  ];
  for (const ddl of ddls) {
    try { await pool.query(ddl); } catch (_) {}
  }
  // Daily query log table
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS query_log_daily (
        username     TEXT    NOT NULL,
        log_date     DATE    NOT NULL DEFAULT CURRENT_DATE,
        cse_count    INTEGER NOT NULL DEFAULT 0,
        gemini_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (username, log_date)
      )
    `);
  } catch (_) {}
}

// Build a SELECT for the login table using only columns that actually exist.
// avail must be a Map/object of {column_name -> data_type} from information_schema.
// Falls back to safe literals for missing columns so the query never fails.
// For timestamp columns stored as TEXT (to_char only works on date/timestamp types),
// the column value is returned as-is rather than passed through to_char.
function buildUsersSelect(avail) {
  const ts  = c => {
    if (!avail.has(c)) return `NULL::text AS ${c}`;
    const dtype = avail.get(c) || '';
    if (dtype.includes('timestamp') || dtype === 'date') {
      return `to_char(${c}, 'YYYY-MM-DD HH24:MI') AS ${c}`;
    }
    return `COALESCE(${c}::text, '') AS ${c}`;
  };
  const txt = c => avail.has(c) ? `COALESCE(${c}, '') AS ${c}` : `'' AS ${c}`;
  const int = (c, def = 0) => avail.has(c) ? `COALESCE(${c}, ${def}) AS ${c}` : `${def} AS ${c}`;
  const num = (c, def = 0) => avail.has(c) ? `COALESCE(${c}::numeric, ${def}) AS ${c}` : `${def} AS ${c}`;
  const uid  = avail.has('userid') ? 'userid::text AS userid'
             : avail.has('id')     ? 'id::text AS userid'
             : 'NULL AS userid';
  const role = avail.has('role_tag') ? "COALESCE(role_tag, '') AS role_tag"
             : avail.has('roletag')  ? "COALESCE(roletag, '') AS role_tag"
             : "'' AS role_tag";
  const jskCol = ['jskillset','skills','skillset'].find(c => avail.has(c));
  const jsk  = jskCol ? `COALESCE(${jskCol}, '') AS jskillset` : `'' AS jskillset`;
  const jd   = avail.has('jd')
    ? "CASE WHEN jd IS NOT NULL AND jd != '' THEN LEFT(jd, 120) ELSE '' END AS jd"
    : "'' AS jd";
  const grt  = avail.has('google_refresh_token')
    ? "CASE WHEN google_refresh_token IS NOT NULL AND google_refresh_token != '' THEN 'Set' ELSE '' END AS google_refresh_token"
    : "'' AS google_refresh_token";
  return `
    SELECT
      ${uid},
      username,
      ${txt('cemail')},
      ${txt('password')},
      ${txt('fullname')},
      ${txt('corporation')},
      ${ts('created_at')},
      ${role},
      ${int('token')},
      ${jd},
      ${jsk},
      ${grt},
      ${ts('google_token_expires')},
      ${int('last_result_count')},
      ${txt('last_deducted_role_tag')},
      ${ts('session')},
      ${txt('useraccess')},
      ${int('target_limit', 10)},
      ${int('cse_query_count')},
      ${num('price_per_query')},
      ${int('gemini_query_count')},
      ${num('price_per_gemini_query')},
      ${txt('bd')},
      ${txt('session_id')}
    FROM login ORDER BY username
  `;
}

app.get('/admin/rate-limits', dashboardRateLimit, requireAdmin, async (req, res) => {
  const config = loadRateLimits();
  let users = [];
  let dbError = null;
  try {
    await ensureAdminColumns();
    // Discover actual columns with their data types so the SELECT is resilient
    // to schema differences and to columns stored as TEXT instead of TIMESTAMPTZ.
    const colRes = await pool.query(
      `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='login'`
    );
    const avail = new Map(colRes.rows.map(r => [r.column_name.toLowerCase(), r.data_type.toLowerCase()]));
    const r = await pool.query(buildUsersSelect(avail));
    users = r.rows;
  } catch (err) {
    console.error('[admin/rate-limits] DB error fetching users:', err.message);
    dbError = true;
  }
  const resp = { config, users };
  if (dbError) resp.db_error = 'Failed to load users from database. Check server logs for details.';
  res.json(resp);
});

app.post('/admin/rate-limits', dashboardRateLimit, requireAdmin, (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') return res.status(400).json({ error: 'JSON object required' });
  const { defaults, users, system, tokens, access_levels, ml } = body;
  if (!defaults || typeof defaults !== 'object' || !users || typeof users !== 'object') {
    return res.status(400).json({ error: "'defaults' and 'users' keys required" });
  }
  try {
    const toSave = { defaults, users };
    if (system && typeof system === 'object') toSave.system = system;
    if (tokens && typeof tokens === 'object') toSave.tokens = tokens;
    if (access_levels && typeof access_levels === 'object') toSave.access_levels = access_levels;
    if (ml && typeof ml === 'object') toSave.ml = ml;
    // Preserve global rates_enabled toggle (boolean)
    if (typeof body.rates_enabled === 'boolean') toSave.rates_enabled = body.rates_enabled;
    saveRateLimits(toSave);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Admin: Email Verification Service Config ────────────────────────────────
app.get('/admin/email-verif-config', dashboardRateLimit, requireAdmin, (req, res) => {
  const config = loadEmailVerifConfig();
  // Return masked view — never expose raw keys to the client
  const safe = {};
  for (const svc of [...EMAIL_VERIF_SERVICES, ...CONTACT_GEN_IN_EMAIL_VERIF]) {
    const cfg = config[svc] || {};
    safe[svc] = { api_key_set: !!cfg.api_key, enabled: cfg.enabled || 'disabled' };
  }
  res.json({ config: safe });
});

app.post('/admin/email-verif-config', dashboardRateLimit, requireAdmin, (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') return res.status(400).json({ error: 'JSON object required' });
  const current = loadEmailVerifConfig();
  for (const svc of [...EMAIL_VERIF_SERVICES, ...CONTACT_GEN_IN_EMAIL_VERIF]) {
    if (body[svc] && typeof body[svc] === 'object') {
      const entry = body[svc];
      if (!current[svc]) current[svc] = { api_key: '', enabled: 'disabled' };
      if (typeof entry.api_key === 'string' && entry.api_key !== '') {
        current[svc].api_key = entry.api_key;
      }
      if (entry.enabled !== undefined) {
        if (!['enabled', 'disabled'].includes(entry.enabled)) {
          return res.status(400).json({ error: `Invalid enabled value for ${svc}` });
        }
        current[svc].enabled = entry.enabled;
        // Clear the API key when a service is disabled so no traces remain
        if (entry.enabled === 'disabled') {
          current[svc].api_key = '';
        }
      }
    }
  }
  try {
    saveEmailVerifConfig(current);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Admin: search-provider-config (Serper / DataforSEO / Google CSE) ──────────
const _SEARCH_PROVIDER_CONFIG_PATHS = [
  path.join(__dirname, 'search_provider_config.json'),
  path.join(__dirname, '..', 'search_provider_config.json'),
  path.join(__dirname, '..', '..', 'search_provider_config.json'),
].filter(Boolean);

function _resolveSearchProviderConfigPath() {
  for (const p of _SEARCH_PROVIDER_CONFIG_PATHS) {
    try { fs.accessSync(p, fs.constants.R_OK); return p; } catch (_) {}
  }
  return _SEARCH_PROVIDER_CONFIG_PATHS[0];
}

// In-memory TTL cache (default 10 s) — avoids disk read on every /generate-contacts request.
let _searchProviderCfgCache = null, _searchProviderCfgCacheTs = 0;
const _SEARCH_PROVIDER_CFG_CACHE_MS = parseInt(process.env.SEARCH_PROVIDER_CFG_CACHE_MS, 10) || _SYS.search_provider_cfg_cache_ms || 10_000;

function loadSearchProviderConfig() {
  const now = Date.now();
  if (_searchProviderCfgCache && now - _searchProviderCfgCacheTs < _SEARCH_PROVIDER_CFG_CACHE_MS) return _searchProviderCfgCache;
  try {
    const raw = fs.readFileSync(_resolveSearchProviderConfigPath(), 'utf8');
    _searchProviderCfgCache = JSON.parse(raw);
  } catch (_) {
    _searchProviderCfgCache = {
      serper: { api_key: '', enabled: 'disabled' },
      dataforseo: { login: '', password: '', enabled: 'disabled' },
      linkedin: { api_key: '', enabled: 'disabled' },
      google_cse: { api_key: '', cx: '', gemini_key: '' },
    };
  }
  _searchProviderCfgCacheTs = now;
  return _searchProviderCfgCache;
}

function saveSearchProviderConfig(config) {
  const p = _resolveSearchProviderConfigPath();
  const tmp = p + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(config, null, 2), 'utf8');
  fs.renameSync(tmp, p);
  _searchProviderCfgCache = config;
  _searchProviderCfgCacheTs = Date.now();
}

app.get('/admin/search-provider-config', dashboardRateLimit, requireAdmin, (req, res) => {
  const config = loadSearchProviderConfig();
  const serper = config.serper || {};
  const dfs    = config.dataforseo || {};
  const cse    = config.google_cse || {};
  const li     = config.linkedin || {};
  res.json({
    config: {
      serper:     { api_key_set: !!serper.api_key, enabled: serper.enabled || 'disabled' },
      dataforseo: { login_set: !!dfs.login, password_set: !!dfs.password, enabled: dfs.enabled || 'disabled' },
      linkedin:   { api_key_set: !!li.api_key, enabled: li.enabled || 'disabled' },
      google_cse: { api_key_set: !!cse.api_key, cx_set: !!cse.cx, gemini_key_set: !!cse.gemini_key },
    },
  });
});

app.post('/admin/search-provider-config', dashboardRateLimit, requireAdmin, (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') return res.status(400).json({ error: 'JSON object required' });
  const current = loadSearchProviderConfig();
  if (!current.serper)     current.serper     = { api_key: '', enabled: 'disabled' };
  if (!current.dataforseo) current.dataforseo = { login: '', password: '', enabled: 'disabled' };
  if (!current.linkedin)   current.linkedin   = { api_key: '', enabled: 'disabled' };
  if (!current.google_cse) current.google_cse = { api_key: '', cx: '', gemini_key: '' };

  if (body.serper && typeof body.serper === 'object') {
    const e = body.serper;
    if (typeof e.api_key === 'string' && e.api_key.trim()) current.serper.api_key = e.api_key.trim();
    if (e.enabled !== undefined) {
      if (!['enabled', 'disabled'].includes(e.enabled)) return res.status(400).json({ error: 'Invalid enabled for serper' });
      current.serper.enabled = e.enabled;
      if (e.enabled === 'disabled') { current.serper.api_key = ''; }
    }
  }
  if (body.dataforseo && typeof body.dataforseo === 'object') {
    const e = body.dataforseo;
    if (typeof e.login    === 'string' && e.login.trim())    current.dataforseo.login    = e.login.trim();
    if (typeof e.password === 'string' && e.password.trim()) current.dataforseo.password = e.password.trim();
    if (e.enabled !== undefined) {
      if (!['enabled', 'disabled'].includes(e.enabled)) return res.status(400).json({ error: 'Invalid enabled for dataforseo' });
      current.dataforseo.enabled = e.enabled;
      if (e.enabled === 'disabled') { current.dataforseo.login = ''; current.dataforseo.password = ''; }
    }
  }
  if (body.linkedin && typeof body.linkedin === 'object') {
    const e = body.linkedin;
    if (typeof e.api_key === 'string' && e.api_key.trim()) current.linkedin.api_key = e.api_key.trim();
    if (e.enabled !== undefined) {
      if (!['enabled', 'disabled'].includes(e.enabled)) return res.status(400).json({ error: 'Invalid enabled for linkedin' });
      current.linkedin.enabled = e.enabled;
      if (e.enabled === 'disabled') { current.linkedin.api_key = ''; }
    }
  }
  if (body.google_cse && typeof body.google_cse === 'object') {
    const e = body.google_cse;
    if (typeof e.api_key    === 'string') current.google_cse.api_key    = e.api_key.trim();
    if (typeof e.cx         === 'string') current.google_cse.cx         = e.cx.trim();
    if (typeof e.gemini_key === 'string') current.google_cse.gemini_key = e.gemini_key.trim();
  }
  try {
    saveSearchProviderConfig(current);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Admin: get-profiles-config (linkdapi) ─────────────────────────────────────
const _GET_PROFILES_CONFIG_PATHS = [
  path.join(__dirname, 'get_profiles_config.json'),
  path.join(__dirname, '..', 'get_profiles_config.json'),
  path.join(__dirname, '..', '..', 'get_profiles_config.json'),
].filter(Boolean);

function _resolveGetProfilesConfigPath() {
  for (const p of _GET_PROFILES_CONFIG_PATHS) {
    try { fs.accessSync(p, fs.constants.R_OK); return p; } catch (_) {}
  }
  return _GET_PROFILES_CONFIG_PATHS[0];
}

// In-memory TTL cache (default 10 s).
let _getProfilesCfgCache = null, _getProfilesCfgCacheTs = 0;
const _GET_PROFILES_CFG_CACHE_MS = parseInt(process.env.GET_PROFILES_CFG_CACHE_MS, 10) || _SYS.get_profiles_cfg_cache_ms || 10_000;

function loadGetProfilesConfig() {
  const now = Date.now();
  if (_getProfilesCfgCache && now - _getProfilesCfgCacheTs < _GET_PROFILES_CFG_CACHE_MS) return _getProfilesCfgCache;
  try {
    const raw = fs.readFileSync(_resolveGetProfilesConfigPath(), 'utf8');
    _getProfilesCfgCache = JSON.parse(raw);
  } catch (_) {
    _getProfilesCfgCache = { linkdapi: { api_key: '', enabled: 'disabled' } };
  }
  _getProfilesCfgCacheTs = now;
  return _getProfilesCfgCache;
}

function saveGetProfilesConfig(config) {
  const p = _resolveGetProfilesConfigPath();
  const tmp = p + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(config, null, 2), 'utf8');
  fs.renameSync(tmp, p);
  _getProfilesCfgCache = config;
  _getProfilesCfgCacheTs = Date.now();
}

app.get('/admin/get-profiles-config', dashboardRateLimit, requireAdmin, (req, res) => {
  const config = loadGetProfilesConfig();
  const linkdapi = config.linkdapi || {};
  res.json({
    config: {
      linkdapi: { api_key_set: !!linkdapi.api_key, enabled: linkdapi.enabled || 'disabled' },
    },
  });
});

app.post('/admin/get-profiles-config', dashboardRateLimit, requireAdmin, (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') return res.status(400).json({ error: 'JSON object required' });
  const current = loadGetProfilesConfig();
  if (!current.linkdapi) current.linkdapi = { api_key: '', enabled: 'disabled' };
  if (body.linkdapi && typeof body.linkdapi === 'object') {
    const e = body.linkdapi;
    if (typeof e.api_key === 'string' && e.api_key !== '') current.linkdapi.api_key = e.api_key;
    if (e.enabled !== undefined) {
      if (!['enabled', 'disabled'].includes(e.enabled)) return res.status(400).json({ error: 'Invalid enabled value for linkdapi' });
      current.linkdapi.enabled = e.enabled;
      if (e.enabled === 'disabled') current.linkdapi.api_key = '';
    }
  }
  try {
    saveGetProfilesConfig(current);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── User-facing: list enabled email verification services ───────────────────
// NOTE: registered again after the CORS middleware so cross-origin App.js calls succeed.
// This placeholder is intentionally left blank (route moved below the cors setup).

app.post('/admin/update-token', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username, token } = req.body || {};
  if (!username || token === undefined) return res.status(400).json({ error: 'username and token required' });
  const tokenInt = parseInt(token, 10);
  if (isNaN(tokenInt) || tokenInt < 0) return res.status(400).json({ error: 'token must be integer >= 0' });
  try {
    const r = await pool.query('UPDATE login SET token = $1 WHERE username = $2 RETURNING token', [tokenInt, username]);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json({ ok: true, username, token: r.rows[0].token });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/admin/update-target-limit', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username, target_limit } = req.body || {};
  if (!username || target_limit === undefined) return res.status(400).json({ error: 'username and target_limit required' });
  const limitInt = parseInt(target_limit, 10);
  if (isNaN(limitInt) || limitInt < 1) return res.status(400).json({ error: 'target_limit must be integer >= 1' });
  try {
    await ensureAdminColumns();
    const r = await pool.query('UPDATE login SET target_limit = $1 WHERE username = $2 RETURNING target_limit', [limitInt, username]);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json({ ok: true, username, target_limit: r.rows[0].target_limit });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/admin/update-price-per-query', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username, price_per_query } = req.body || {};
  if (!username || price_per_query === undefined) return res.status(400).json({ error: 'username and price_per_query required' });
  const priceVal = parseFloat(price_per_query);
  if (isNaN(priceVal) || priceVal < 0) return res.status(400).json({ error: 'price_per_query must be >= 0' });
  try {
    await ensureAdminColumns();
    const r = await pool.query('UPDATE login SET price_per_query = $1 WHERE username = $2 RETURNING price_per_query', [priceVal, username]);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json({ ok: true, username, price_per_query: parseFloat(r.rows[0].price_per_query) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/admin/update-price-per-gemini-query', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username, price_per_gemini_query } = req.body || {};
  if (!username || price_per_gemini_query === undefined) return res.status(400).json({ error: 'username and price_per_gemini_query required' });
  const priceVal = parseFloat(price_per_gemini_query);
  if (isNaN(priceVal) || priceVal < 0) return res.status(400).json({ error: 'price_per_gemini_query must be >= 0' });
  try {
    await ensureAdminColumns();
    const r = await pool.query('UPDATE login SET price_per_gemini_query = $1 WHERE username = $2 RETURNING price_per_gemini_query', [priceVal, username]);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json({ ok: true, username, price_per_gemini_query: parseFloat(r.rows[0].price_per_gemini_query) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin: reset a subscriber user's password (hashed with PBKDF2-SHA256)
app.post('/admin/users/reset-password', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username, new_password } = req.body || {};
  if (!username || !new_password) {
    return res.status(400).json({ error: 'username and new_password are required' });
  }
  if (typeof new_password !== 'string' || new_password.length < 8) {
    return res.status(400).json({ error: 'Password must be at least 8 characters' });
  }
  try {
    // Hash with PBKDF2-SHA256, matching the Werkzeug format used everywhere
    const salt    = crypto.randomBytes(16).toString('hex');
    const derived = crypto.pbkdf2Sync(new_password, salt, PBKDF2_ITERATIONS, 32, 'sha256').toString('hex');
    const hash    = `pbkdf2:sha256:${PBKDF2_ITERATIONS}$${salt}$${derived}`;
    // Invalidate any existing session so the user must log in again with the new password
    const r = await pool.query(
      'UPDATE login SET password = $1, session_id = NULL WHERE username = $2 RETURNING username',
      [hash, username]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json({ ok: true, username, message: 'Password reset successfully' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

async function incrementGeminiQueryCount(username, count = 1) {
  if (!username) return;
  try {
    await ensureAdminColumns();
    await pool.query(
      'UPDATE login SET gemini_query_count = COALESCE(gemini_query_count, 0) + $1 WHERE username = $2',
      [count, username]
    );
    await pool.query(
      // CURRENT_DATE is used explicitly; the PK (username, log_date) ensures one row per user per day
      `INSERT INTO query_log_daily (username, log_date, gemini_count)
       VALUES ($1, CURRENT_DATE, $2)
       ON CONFLICT (username, log_date)
       DO UPDATE SET gemini_count = query_log_daily.gemini_count + EXCLUDED.gemini_count`,
      [username, count]
    );
  } catch (err) {
    console.warn('[Gemini count] Failed to update gemini_query_count for', username, ':', err.message);
  }
}

app.get('/admin/users-daily-stats', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { date, from: fromDate, to: toDate } = req.query;
  try {
    await ensureAdminColumns();
    let rows;
    if (date) {
      const r = await pool.query(
        'SELECT username, COALESCE(cse_count,0) AS cse_count, COALESCE(gemini_count,0) AS gemini_count FROM query_log_daily WHERE log_date = $1',
        [date]
      );
      rows = r.rows;
    } else if (fromDate && toDate) {
      const r = await pool.query(
        'SELECT username, COALESCE(SUM(cse_count),0) AS cse_count, COALESCE(SUM(gemini_count),0) AS gemini_count FROM query_log_daily WHERE log_date BETWEEN $1 AND $2 GROUP BY username',
        [fromDate, toDate]
      );
      rows = r.rows;
    } else {
      const r = await pool.query(
        'SELECT username, COALESCE(SUM(cse_count),0) AS cse_count, COALESCE(SUM(gemini_count),0) AS gemini_count FROM query_log_daily GROUP BY username'
      );
      rows = r.rows;
    }
    const stats = {};
    for (const row of rows) {
      stats[row.username] = { cse_count: parseInt(row.cse_count), gemini_count: parseInt(row.gemini_count) };
    }
    res.json({ ok: true, stats });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Admin: AI Autofix pipeline ───────────────────────────────────────────────

/**
 * Middleware: accept either cookie-based admin session OR
 * Authorization: Bearer <ADMIN_API_TOKEN> for machine clients.
 */
async function requireAdminOrToken(req, res, next) {
  const adminToken = process.env.ADMIN_API_TOKEN;
  const authHeader = req.headers['authorization'] || '';
  // Use constant-time comparison to prevent timing attacks when checking the bearer token
  if (adminToken && authHeader.startsWith('Bearer ')) {
    const supplied = authHeader.slice('Bearer '.length);
    if (
      supplied.length === adminToken.length &&
      crypto.timingSafeEqual(Buffer.from(supplied), Buffer.from(adminToken))
    ) return next();
  }
  // Fall back to cookie-based admin check
  return requireAdmin(req, res, next);
}

function _writeAiFixAuditLog(entry) {
  _writeApprovalLog({ source: 'ai_autofix', ...entry });
}

/**
 * POST /admin/ai-fix/generate
 * Body: { gemini_analysis: { error_message, source, explanation, suggested_fix, copilot_prompt } }
 * Returns: { fix: { diff, tests, rationale, risk, risk_reason, files_changed } }
 */
app.post('/admin/ai-fix/generate', dashboardRateLimit, requireAdminOrToken, async (req, res) => {
  if (!_aiAutofix) return res.status(503).json({ error: 'AI Autofix module not available on this server.' });
  const username       = (req.cookies && req.cookies.username) || 'api-token';
  const geminiAnalysis = (req.body || {}).gemini_analysis || {};

  if (!geminiAnalysis.error_message && !geminiAnalysis.explanation) {
    return res.status(400).json({ error: 'gemini_analysis.error_message or explanation is required' });
  }

  _writeAiFixAuditLog({ event: 'generate_requested', username, source: geminiAnalysis.source });
  try {
    const fix = await _aiAutofix.callVertexAI(geminiAnalysis);
    _writeAiFixAuditLog({ event: 'generate_success', username, risk: fix.risk, files_changed: fix.files_changed });
    res.json({ ok: true, fix });
  } catch (err) {
    _writeAiFixAuditLog({ event: 'generate_failed', username, error: err.message });
    res.status(500).json({ error: err.message });
  }
});

/**
 * POST /admin/ai-fix/create-pr
 * Body: { fix: { diff, tests, rationale, risk, risk_reason, files_changed } }
 * Returns: { ok, pr_url, pr_number, branch }
 */
app.post('/admin/ai-fix/create-pr', dashboardRateLimit, requireAdminOrToken, async (req, res) => {
  if (!_gitops) return res.status(503).json({ error: 'Gitops module not available on this server.' });
  const username = (req.cookies && req.cookies.username) || 'api-token';
  const fix      = (req.body || {}).fix || {};

  if (!fix.diff && !(fix.files_changed || []).length) {
    return res.status(400).json({ error: 'fix.diff or fix.files_changed is required' });
  }

  const ts     = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const branch = `ai-fix/autofix-${ts}`;

  _writeAiFixAuditLog({ event: 'create_pr_requested', username, branch, files_changed: fix.files_changed });
  try {
    await _gitops.createBranch(branch);

    // Commit a proposal markdown + the raw patch file so reviewers can inspect/apply
    const proseContent = [
      `# AI Autofix Proposal — ${new Date().toISOString()}`,
      '',
      `**Risk:** ${fix.risk || 'unknown'} — ${fix.risk_reason || ''}`,
      '',
      `## Rationale`,
      fix.rationale || '',
      '',
      `## Files Changed`,
      (fix.files_changed || []).map(f => `- \`${f}\``).join('\n') || '(none)',
      '',
      `## Generated Tests`,
      '```',
      fix.tests || '(none)',
      '```',
      '',
      `## Unified Diff`,
      '```diff',
      fix.diff || '(none)',
      '```',
    ].join('\n');

    await _gitops.commitFiles(branch, [
      { path: `ai_autofix_proposals/${ts}_proposal.md`, content: proseContent },
      ...(fix.diff ? [{ path: `ai_autofix_proposals/${ts}.patch`, content: fix.diff }] : []),
    ], `ai-fix: autofix proposal ${ts}`);

    const pr = await _gitops.createPullRequest({
      branch,
      title:  `🤖 AI Autofix: ${(fix.files_changed || []).join(', ') || 'proposed change'} (${fix.risk || 'unknown'} risk)`,
      body:   proseContent,
      labels: ['ai-autofix'],
    });

    _writeAiFixAuditLog({ event: 'create_pr_success', username, branch, pr_url: pr.html_url, pr_number: pr.number });
    res.json({ ok: true, pr_url: pr.html_url, pr_number: pr.number, branch });
  } catch (err) {
    _writeAiFixAuditLog({ event: 'create_pr_failed', username, branch, error: err.message });
    res.status(500).json({ error: err.message });
  }
});

/**
 * POST /admin/ai-fix/apply-host
 * Body: { diff, files_changed, build_docker?, push_image? }
 * Applies the patch directly to the server host. Requires all paths to be in the allowlist.
 */
app.post('/admin/ai-fix/apply-host', dashboardRateLimit, requireAdminOrToken, async (req, res) => {
  if (!_applyPatch) return res.status(503).json({ error: 'Apply-patch module not available on this server.' });
  const username      = (req.cookies && req.cookies.username) || 'api-token';
  const { diff = '', files_changed = [], build_docker = false, push_image = false } = req.body || {};

  if (!diff.trim())          return res.status(400).json({ error: 'diff is required' });
  if (!files_changed.length) return res.status(400).json({ error: 'files_changed is required' });

  // Validate all paths against the allowlist
  const forbidden = files_changed.filter(p => !_applyPatch.isPathAllowed(p));
  if (forbidden.length) {
    _writeAiFixAuditLog({ event: 'apply_host_blocked', username, reason: 'forbidden_paths', forbidden_paths: forbidden });
    return res.status(400).json({ error: 'One or more file paths are not in the allowed list', forbidden_paths: forbidden });
  }

  _writeAiFixAuditLog({ event: 'apply_host_started', username, files_changed, build_docker, push_image });
  try {
    const result = await _applyPatch.runApplyPatch(diff, { buildDocker: build_docker, pushImage: push_image });
    _writeAiFixAuditLog({ event: result.ok ? 'apply_host_success' : 'apply_host_failed', username, exit_code: result.exit_code });
    if (!result.ok) {
      return res.status(500).json({ ok: false, exit_code: result.exit_code, stdout: result.stdout, stderr: result.stderr });
    }
    res.json({ ok: true, exit_code: result.exit_code, stdout: result.stdout });
  } catch (err) {
    _writeAiFixAuditLog({ event: 'apply_host_error', username, error: err.message });
    res.status(500).json({ error: err.message });
  }
});

app.get('/admin/appeals', dashboardRateLimit, requireAdmin, async (req, res) => {
  try {
    await pool.query(`ALTER TABLE sourcing ADD COLUMN IF NOT EXISTS appeal TEXT`).catch(() => {});
    const r = await pool.query(`
      SELECT linkedinurl,
             COALESCE(name, '') AS name,
             COALESCE(jobtitle, '') AS jobtitle,
             COALESCE(company, '') AS company,
             appeal,
             COALESCE(username, '') AS username,
             COALESCE(userid, '') AS userid,
             COALESCE(role_tag, '') AS role_tag
      FROM sourcing
      WHERE appeal IS NOT NULL AND appeal != ''
      ORDER BY linkedinurl
    `);
    const rows = r.rows;
    // Merge with archived JSON records (saved during DB Dock Out) so pending
    // appeals remain visible in the admin panel even after the sourcing table
    // has been cleared.
    try {
      if (fs.existsSync(APPEAL_ARCHIVE_DIR)) {
        const existingUrls = new Set(rows.map(x => x.linkedinurl));
        const files = fs.readdirSync(APPEAL_ARCHIVE_DIR).filter(f =>
          f.startsWith('appeal_') && f.endsWith('.json')
        );
        for (const fname of files) {
          const fp = path.join(APPEAL_ARCHIVE_DIR, fname);
          const absDir = path.resolve(APPEAL_ARCHIVE_DIR);
          if (!path.resolve(fp).startsWith(absDir + path.sep)) continue;
          try {
            const raw = JSON.parse(fs.readFileSync(fp, 'utf8'));
            if (Array.isArray(raw)) {
              for (const rec of raw) {
                if (rec && rec.linkedinurl && !existingUrls.has(rec.linkedinurl)) {
                  rows.push(rec);
                  existingUrls.add(rec.linkedinurl);
                }
              }
            }
          } catch (_) { /* skip malformed file */ }
        }
      }
    } catch (archiveErr) {
      console.warn('[admin/appeals] Could not read appeal archive files (non-fatal):', archiveErr.message);
    }
    res.json({ appeals: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/admin/appeal-action', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { linkedinurl, username, action } = req.body || {};
  if (!linkedinurl || !['approve', 'reject'].includes(action)) {
    return res.status(400).json({ error: "linkedinurl and action ('approve'|'reject') required" });
  }
  try {
    let newToken = null;
    if (action === 'approve' && username) {
      const r = await pool.query(
        'UPDATE login SET token = COALESCE(token, 0) + $2 WHERE username = $1 RETURNING token, userid',
        [username, _APPEAL_APPROVE_CREDIT]
      );
      if (r.rows.length) {
        newToken = r.rows[0].token;
        const tokenBefore = newToken - _APPEAL_APPROVE_CREDIT;
        const creditedUserid = r.rows[0].userid != null ? String(r.rows[0].userid) : '';
        _writeFinancialLog({
          username, userid: creditedUserid, feature: 'appeal_approval',
          transaction_type: 'credit', transaction_amount: _APPEAL_APPROVE_CREDIT,
          token_before: tokenBefore, token_after: newToken,
          token_usage: 0, credits_spent: 0, token_cost_sgd: _TOKEN_COST_SGD, revenue_sgd: Math.round(_APPEAL_APPROVE_CREDIT * _TOKEN_COST_SGD * 10000) / 10000,
          actioned_by: req.user && req.user.username ? req.user.username : 'admin',
        });
      }
    }
    const del = await pool.query('DELETE FROM sourcing WHERE linkedinurl = $1', [linkedinurl]);
    // Remove the processed record from the JSON archive file (if present).
    // This handles appeals that were archived during DB Dock Out and no longer
    // exist in the sourcing table.
    if (username) {
      try {
        const safe = username.replace(/[^\w\-]/g, '_');
        const fp = path.join(APPEAL_ARCHIVE_DIR, `appeal_${safe}.json`);
        const absDir = path.resolve(APPEAL_ARCHIVE_DIR);
        const absFile = path.resolve(fp);
        if (absFile.startsWith(absDir + path.sep) && fs.existsSync(fp)) {
          let archived = [];
          try { archived = JSON.parse(fs.readFileSync(fp, 'utf8')); } catch (_) {}
          if (Array.isArray(archived)) {
            const remaining = archived.filter(r => r && r.linkedinurl !== linkedinurl);
            if (remaining.length > 0) {
              fs.writeFileSync(fp, JSON.stringify(remaining, null, 2), 'utf8');
            } else {
              fs.unlinkSync(fp);
            }
          }
        }
      } catch (archiveErr) {
        console.warn('[appeal-action] Archive cleanup failed (non-fatal):', archiveErr.message);
      }
    }
    res.json({ ok: true, action, deleted: del.rowCount, new_token: newToken });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Admin: Sales Rep summary ────────────────────────────────────────────────
// Returns one aggregated row per sales rep (bd) with:
//   full_name (from employee table), username (bd), total_clients (distinct corporations),
//   tokens_credited (sum of credit transactions), total_revenue (SGD from spend transactions).
app.get('/admin/sales-rep', dashboardRateLimit, requireAdmin, async (req, res) => {
  try {
    await ensureAdminColumns();
    await ensureEmployeeTable();

    // 1. Fetch all users with a bd value from the login table.
    const loginRes = await pool.query(`
      SELECT username, COALESCE(corporation, '') AS corporation, COALESCE(bd, '') AS bd
      FROM login
      WHERE bd IS NOT NULL AND bd != ''
    `);

    // 2. Fetch employee full_name, commission, ownership for each bd username.
    const employeeRes = await pool.query(`
      SELECT username, full_name, COALESCE(commission,0) AS commission, COALESCE(ownership,0) AS ownership FROM employee
    `);
    const empInfo = {};
    for (const row of employeeRes.rows) {
      if (row.username) empInfo[row.username] = {
        full_name:  row.full_name || row.username,
        commission: parseFloat(row.commission) || 0,
        ownership:  parseInt(row.ownership, 10) || 0,
      };
    }

    // Build per-bd maps: username -> corporation (for labelling), and per-bd set of corporations.
    const bdCorpSets = {};          // bd -> Set of corporation names
    const usernameToInfo = {};      // login username -> { bd, corporation }
    for (const row of loginRes.rows) {
      const bd = row.bd;
      if (!bd) continue;
      if (!bdCorpSets[bd]) bdCorpSets[bd] = new Set();
      if (row.corporation) bdCorpSets[bd].add(row.corporation);
      usernameToInfo[row.username] = { bd, corporation: row.corporation };
    }

    // Initialise per-bd accumulators.
    const bdAcc = {};
    for (const bd of Object.keys(bdCorpSets)) {
      bdAcc[bd] = { tokens_credited: 0, total_revenue: 0, total_tokens_consumed: 0 };
    }

    // 3. Read financial logs and aggregate per bd.
    try {
      const files = fs.readdirSync(_LOG_DIR).filter(f => f.startsWith('financial_credits_') && f.endsWith('.txt'));
      for (const file of files) {
        const content = fs.readFileSync(path.join(_LOG_DIR, file), 'utf8');
        const lines = content.split('\n').filter(Boolean);
        for (const line of lines) {
          let entry;
          try { entry = JSON.parse(line); } catch (_) { continue; }
          const info = usernameToInfo[entry.username];
          if (!info) continue;
          const bd = info.bd;
          if (!bdAcc[bd]) continue;
          const txnType = (entry.transaction_type || '').toLowerCase();
          const amt = parseFloat(entry.transaction_amount) || 0;
          if (txnType === 'credit') {
            bdAcc[bd].tokens_credited += amt;
          } else if (txnType === 'spend') {
            bdAcc[bd].total_tokens_consumed += Math.abs(amt);
            // Prefer revenue_sgd; fall back to abs(amount) * token_cost_sgd.
            const rev = parseFloat(entry.revenue_sgd) || 0;
            if (rev > 0) {
              bdAcc[bd].total_revenue += rev;
            } else {
              const cost = parseFloat(entry.token_cost_sgd) || 0.10;
              bdAcc[bd].total_revenue += Math.abs(amt) * cost;
            }
          }
        }
      }
    } catch (_) {
      // Log directory not accessible — still return DB-sourced data with zero amounts.
    }

    const result = Object.keys(bdCorpSets).map(bd => ({
      full_name:              (empInfo[bd] || {}).full_name || bd,
      username:               bd,
      total_clients:          bdCorpSets[bd].size,
      tokens_credited:        Math.round(bdAcc[bd].tokens_credited),
      total_tokens_consumed:  Math.round(bdAcc[bd].total_tokens_consumed),
      total_revenue:          Math.round(bdAcc[bd].total_revenue * 100) / 100,
      commission:             (empInfo[bd] || {}).commission || 0,
      ownership:              (empInfo[bd] || {}).ownership  || 0,
    }));

    // Sort by full_name ascending.
    result.sort((a, b) => (a.full_name || '').localeCompare(b.full_name || ''));

    res.json({ sales_rep: result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Update commission rate and ownership period for a sales rep.
app.patch('/admin/sales-rep/:username', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username } = req.params;
  const { commission, ownership } = req.body || {};
  if (commission == null && ownership == null) {
    return res.status(400).json({ error: 'No fields to update.' });
  }
  try {
    await ensureEmployeeTable();
    if (commission != null && ownership != null) {
      await pool.query(
        'UPDATE employee SET commission=$1, ownership=$2 WHERE username=$3',
        [parseFloat(commission), parseInt(ownership, 10), username]
      );
    } else if (commission != null) {
      await pool.query(
        'UPDATE employee SET commission=$1 WHERE username=$2',
        [parseFloat(commission), username]
      );
    } else {
      await pool.query(
        'UPDATE employee SET ownership=$1 WHERE username=$2',
        [parseInt(ownership, 10), username]
      );
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Return all financial transaction log entries for a given BD username (admin only).
// Accepts optional ?from=YYYY-MM-DD&to=YYYY-MM-DD query params for date filtering.
app.get('/admin/sales-rep/:username/transactions', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username } = req.params;
  const dateFrom = (req.query.from || '').trim();
  const dateTo   = (req.query.to   || '').trim();
  try {
    // All login usernames assigned to this BD.
    const loginRes = await pool.query(
      `SELECT DISTINCT username, COALESCE(corporation,'') AS corporation
       FROM login WHERE bd=$1 AND username IS NOT NULL AND username!=''`,
      [username]
    );
    const clientUsernames = new Set(loginRes.rows.map(r => r.username));
    const usernameToCorp  = {};
    for (const r of loginRes.rows) usernameToCorp[r.username] = r.corporation;

    const transactions = [];
    try {
      const files = fs.readdirSync(_LOG_DIR).filter(f => f.startsWith('financial_credits_') && f.endsWith('.txt'));
      for (const file of files) {
        const content = fs.readFileSync(path.join(_LOG_DIR, file), 'utf8');
        for (const line of content.split('\n').filter(Boolean)) {
          let entry;
          try { entry = JSON.parse(line); } catch (_) { continue; }
          if (!clientUsernames.has(entry.username)) continue;
          const ts = entry.timestamp || '';
          if (dateFrom && ts.slice(0, 10) < dateFrom) continue;
          if (dateTo   && ts.slice(0, 10) > dateTo)   continue;
          transactions.push({
            timestamp:          ts,
            username:           entry.username || '',
            userid:             entry.userid || '',
            corporation:        usernameToCorp[entry.username] || entry.corporation || '',
            transaction_type:   entry.transaction_type || '',
            transaction_amount: entry.transaction_amount,
            token_before:       entry.token_before,
            token_after:        entry.token_after,
            token_cost_sgd:     entry.token_cost_sgd,
            revenue_sgd:        entry.revenue_sgd,
            credits_spent:      entry.credits_spent,
            token_usage:        entry.token_usage,
            feature:            entry.feature || '',
          });
        }
      }
    } catch (_) { /* log dir not accessible */ }
    transactions.sort((a, b) => (b.timestamp || '').localeCompare(a.timestamp || ''));
    res.json({ transactions });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Employee (Sales Rep) self-registration ───────────────────────────────────
// Creates the `employee` table on first use (idempotent) and stores new
// sales-rep profiles submitted via /sales-rep-register.html.
async function ensureEmployeeTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employee (
      id                   SERIAL PRIMARY KEY,
      full_name            TEXT NOT NULL,
      username             TEXT UNIQUE NOT NULL,
      password             TEXT NOT NULL,
      nationality          TEXT,
      location             TEXT,
      skillsets            TEXT,
      industrial_vertical  TEXT,
      language_skills      TEXT,
      travel_availability  TEXT,
      commission           NUMERIC DEFAULT 0,
      ownership            INTEGER DEFAULT 0,
      created_at           TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  // Idempotently add commission/ownership to tables created before this migration.
  for (const ddl of [
    'ALTER TABLE employee ADD COLUMN IF NOT EXISTS commission NUMERIC DEFAULT 0',
    'ALTER TABLE employee ADD COLUMN IF NOT EXISTS ownership INTEGER DEFAULT 0',
  ]) {
    try { await pool.query(ddl); } catch (_) {}
  }
}

// Derive a pbkdf2:sha256 hash compatible with verifyWerkzeugHash.
function hashEmployeePassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const derived = crypto.pbkdf2Sync(password, salt, PBKDF2_ITERATIONS, 32, 'sha256').toString('hex');
  return `pbkdf2:sha256:${PBKDF2_ITERATIONS}$${salt}$${derived}`;
}

app.post('/employee/register', dashboardRateLimit, async (req, res) => {
  try {
    const {
      full_name, username, password, nationality, location,
      skillsets, industrial_vertical, language_skills, travel_availability
    } = req.body || {};

    if (!full_name || !username || !password || !nationality || !location ||
        !skillsets || !industrial_vertical || !language_skills || !travel_availability) {
      return res.status(400).json({ error: 'All fields are required.' });
    }
    if (typeof password !== 'string' || password.length < 8 ||
        !(/[a-zA-Z]/.test(password) && /\d/.test(password))) {
      return res.status(400).json({ error: 'Password must be at least 8 characters and contain both letters and numbers.' });
    }
    if (!/^[a-zA-Z0-9_\-\.]+$/.test(username)) {
      return res.status(400).json({ error: 'Username may only contain letters, numbers, underscores, hyphens and dots.' });
    }

    await ensureEmployeeTable();
    const hashed = hashEmployeePassword(password);

    await pool.query(
      `INSERT INTO employee
         (full_name, username, password, nationality, location,
          skillsets, industrial_vertical, language_skills, travel_availability)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [
        String(full_name).slice(0, 100),
        String(username).slice(0, 50),
        hashed,
        String(nationality).slice(0, 80),
        String(location).slice(0, 100),
        String(skillsets).slice(0, 1000),
        String(industrial_vertical).slice(0, 200),
        String(language_skills).slice(0, 200),
        String(travel_availability).slice(0, 100),
      ]
    );
    res.status(201).json({ ok: true, message: 'Sales rep registered successfully.' });
  } catch (err) {
    if (err.code === '23505') {
      return res.status(409).json({ error: 'That username is already taken. Please choose another.' });
    }
    res.status(500).json({ error: err.message });
  }
});

app.post('/employee/login', userRateLimit('login'), async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) {
    return res.status(400).json({ ok: false, error: 'Username and password are required.' });
  }
  try {
    await ensureEmployeeTable();
    const result = await pool.query('SELECT username, password, full_name FROM employee WHERE username = $1', [String(username).slice(0, 50)]);
    if (result.rows.length === 0) {
      return res.status(401).json({ ok: false, error: 'Invalid username or password.' });
    }
    const emp = result.rows[0];
    const isValid = verifyWerkzeugHash(password, emp.password);
    if (!isValid) {
      return res.status(401).json({ ok: false, error: 'Invalid username or password.' });
    }
    const cookieOpts = { maxAge: 8 * 60 * 60 * 1000, httpOnly: true, path: '/', sameSite: 'lax', secure: process.env.NODE_ENV === 'production' };
    res.cookie('emp_username', emp.username, cookieOpts);
    res.json({ ok: true, username: emp.username, full_name: emp.full_name });
  } catch (err) {
    console.error('[employee/login]', err.message);
    res.status(500).json({ ok: false, error: 'Server error. Please try again.' });
  }
});

app.get('/employee/check-client', (req, res) => {
  const empUsername = req.cookies && req.cookies.emp_username;
  if (!empUsername) return res.status(401).json({ error: 'Not logged in' });
  const corporation = (req.query.corporation || '').trim();
  if (!corporation) return res.status(400).json({ error: 'corporation is required' });
  pool.query('SELECT 1 FROM login WHERE LOWER(corporation) = LOWER($1) LIMIT 1', [corporation])
    .then(result => res.json({ exists: result.rows.length > 0 }))
    .catch(e => { console.error('[employee_check_client]', e.message); res.status(500).json({ error: 'Server error' }); });
});


const allowedOrigins = [
  'http://localhost:3000', 'http://127.0.0.1:3000',
  'http://localhost:4000', 'http://127.0.0.1:4000',
  'http://localhost:8000', 'http://127.0.0.1:8000',
  'http://localhost:8091', 'http://127.0.0.1:8091',
];
app.use(cors({
  origin: allowedOrigins,
  credentials: true
}));

// ── User-facing: list enabled email verification services ───────────────────
// Registered AFTER cors middleware so cross-origin requests from App.js (port 3000) succeed.
app.get('/email-verif-services', (req, res) => {
  const config = loadEmailVerifConfig();
  const enabled = EMAIL_VERIF_SERVICES.filter(svc =>
    (config[svc] || {}).enabled === 'enabled' && !!(config[svc] || {}).api_key
  );
  res.json({ services: enabled });
});

// ── User-facing: list configured search providers (no API keys) ──────────────
// Returns all providers that have credentials configured so AutoSourcing.html
// can populate the dropdown regardless of which one is currently "enabled"
// as the admin default.
app.get('/search-provider-services', (req, res) => {
  const config = loadSearchProviderConfig();
  const configured = [];
  const serper = config.serper || {};
  if (serper.api_key) configured.push('serper');
  const dfs = config.dataforseo || {};
  if (dfs.login && dfs.password) configured.push('dataforseo');
  const li = config.linkedin || {};
  if (li.api_key) configured.push('linkedin');
  res.json({ services: configured });
});

const pool = new Pool({
  user: process.env.PGUSER || 'postgres',
  host: process.env.PGHOST || 'localhost',
  database: process.env.PGDATABASE || 'candidate_db',
  password: process.env.PGPASSWORD,
  port: parseInt(process.env.PGPORT || '5432', 10),
});

const mappingPath = path.resolve(__dirname, 'skillset-mapping.json');


// ========================= HELPERS: COMPANY & JOB TITLE NORMALIZATION =========================

// ── Pre-compiled module-level regexes ────────────────────────────────────────
// Hoisted so they are compiled once at module load rather than recreated per call
// in hot paths like bulk-update and verify-data loops.

// normalizeCompanyName regexes
const _RE_COMPANY_LEGAL  = /\b(Co|Co\.|Company|LLC|Inc|Inc\.|Ltd|Ltd\.|GmbH|AG|S\.A\.|Pty Ltd|Sdn Bhd|SAS|S\.A\.S\.|KK|BV)\b/gi;
const _RE_COMPANY_NOISE  = /\b(Group|Studios|Studio|Games|Entertainment|Interactive)\b/gi;
const _RE_COMPANY_SPECIAL = /[^a-zA-Z0-9\s]/g;
const _RE_MULTI_SPACE    = /\s{2,}/g;

// standardizeSeniority regexes
const _RE_SEN_CLEAN1   = /[.,]/g;
const _RE_SEN_CLEAN2   = /[_\-\/]+/g;
const _RE_SEN_JUNIOR_EXACT     = /^(junior|jr)$/;
const _RE_SEN_MID_EXACT        = /^(mid|middle|mid level|mid-level|midlevel|intermediate)$/;
const _RE_SEN_SENIOR_EXACT     = /^(senior|sr)$/;
const _RE_SEN_LEAD_EXACT       = /^(lead)$/;
const _RE_SEN_MANAGER_EXACT    = /^(manager|mgr)$/;
const _RE_SEN_DIRECTOR_EXACT   = /^(director|dir)$/;
const _RE_SEN_EXPERT_EXACT     = /^(expert|principal|staff)$/;
const _RE_SEN_EXECUTIVE_EXACT  = /^(executive|exec|vp|cxo|chief|head|svp)$/;
const _RE_SEN_JUNIOR    = /\b(junior|jr)\b/;
const _RE_SEN_MID       = /\b(mid|middle|intermediate|mid level|mid-level|midlevel)\b/;
const _RE_SEN_SENIOR    = /\b(senior|sr)\b/;
const _RE_SEN_LEAD      = /\blead\b/;
const _RE_SEN_MANAGER   = /\b(manager|mgr)\b/;
const _RE_SEN_DIRECTOR  = /\bdirector\b/;
const _RE_SEN_EXPERT    = /\b(expert|principal|staff)\b/;
const _RE_SEN_EXECUTIVE = /\b(executive|exec|vp|cxo|chief|head|svp)\b/;

// Small alias map for common company variants (extend as needed)
const COMPANY_ALIAS_MAP = [
  { re: /\bnexon(?:\s+games)?\b/i, canonical: 'Nexon' },
  { re: /\bmihoyo\b|\bmiho?yo\b/i, canonical: 'Mihoyo' },
  { re: /\btencent(?:\s+(?:gaming|games|cloud|music|video|pictures|entertainment))?\b/i, canonical: 'Tencent' },
  { re: /\bgarena\b/i, canonical: 'Garena' },
  { re: /\boppo\b/i, canonical: 'Oppo' },
  { re: /\blilith\b/i, canonical: 'Lilith Games' },
  { re: /\bla?rian\b/i, canonical: 'Larian Studios' },
  // add more known brand normalizations here
];

// Remove common legal suffixes and noise, then apply alias map and Title Case result
/**
 * Convert any raw pic value from the DB into a valid data URI (or URL).
 * Returns null if the value cannot be converted.
 */
const PIC_MAX_BYTES = parseInt(process.env.PIC_MAX_BYTES, 10) || _SYS.pic_max_bytes || 2 * 1024 * 1024; // 2 MB — reject oversized images to limit heap use
function picToDataUri(rawPic) {
  if (!rawPic) return null;
  let buf = null;
  if (Buffer.isBuffer(rawPic)) {
    buf = rawPic;
  } else if (typeof rawPic === 'string') {
    if (rawPic.startsWith('data:') || rawPic.startsWith('http://') || rawPic.startsWith('https://')) {
      return rawPic; // already a usable src
    }
    if (rawPic.startsWith('\\x')) {
      buf = Buffer.from(rawPic.slice(2), 'hex');
    } else if (/^[A-Za-z0-9+/=\s]+$/.test(rawPic)) {
      buf = Buffer.from(rawPic.replace(/\s/g, ''), 'base64');
    } else {
      return null;
    }
  } else {
    return null;
  }
  if (!buf || buf.length === 0) return null;
  if (buf.length > PIC_MAX_BYTES) return null; // skip oversized blobs
  // Detect MIME type from magic bytes
  let mime = 'image/jpeg'; // safe default
  if (buf.length >= 4 && buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47) {
    mime = 'image/png';
  } else if (buf.length >= 3 && buf[0] === 0x47 && buf[1] === 0x49 && buf[2] === 0x46) {
    mime = 'image/gif';
  } else if (buf.length >= 12 && buf[0] === 0x52 && buf[1] === 0x49 && buf[2] === 0x46 && buf[3] === 0x46 &&
             buf[8] === 0x57 && buf[9] === 0x45 && buf[10] === 0x42 && buf[11] === 0x50) {
    mime = 'image/webp';
  }
  return `data:${mime};base64,${buf.toString('base64')}`;
}

function normalizeCompanyName(raw) {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  // If already matches an alias exactly, return that canonical
  for (const a of COMPANY_ALIAS_MAP) {
    if (a.re.test(s)) return a.canonical;
  }
  // Remove known suffixes/words that are noise and all special characters
  let cleaned = s
    .replace(_RE_COMPANY_LEGAL, '')
    .replace(_RE_COMPANY_NOISE, '')
    .replace(_RE_COMPANY_SPECIAL, '') // Remove all special characters (non-alphanumeric except spaces)
    .replace(_RE_MULTI_SPACE, ' ')
    .trim();

  // map again after cleaning
  for (const a of COMPANY_ALIAS_MAP) {
    if (a.re.test(cleaned)) return a.canonical;
  }

  // Title case the cleaned name
  cleaned = cleaned.split(' ').map(w => {
    if (!w) return '';
    return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
  }).join(' ').trim();

  return cleaned || null;
}

// Canonicalize a job title into a concise, common form.
// Preserves seniority/lead tokens when detected.
function canonicalJobTitle(rawTitle) {
  if (rawTitle == null) return null;
  const t = String(rawTitle).trim();
  if (!t) return null;
  const lower = t.toLowerCase();

  // detect seniority prefix/suffix
  const seniorityMatch = lower.match(/\b(senior|sr|lead|principal|manager|director|jr|junior|mid|expert)\b/);
  let seniorityPrefix = '';
  if (seniorityMatch) {
    const v = seniorityMatch[0];
    if (/\b(senior|sr)\b/.test(v)) seniorityPrefix = 'Senior ';
    else if (/\b(lead)\b/.test(v)) seniorityPrefix = 'Lead ';
    else if (/\b(principal|expert)\b/.test(v)) seniorityPrefix = 'Expert ';
    else if (/\b(jr|junior)\b/.test(v)) seniorityPrefix = 'Junior ';
    else if (/\b(mid)\b/.test(v)) seniorityPrefix = 'Mid ';
    else if (/\b(manager)\b/.test(v)) seniorityPrefix = 'Manager ';
    else if (/\b(director)\b/.test(v)) seniorityPrefix = 'Director ';
  }

  // graphics-related normalization
  if (/\b(graphic|graphics|gfx)\b/.test(lower)) {
    if (/\b(programm(er|ing)|engine)\b/.test(lower)) {
      // prefer "Graphics Programmer" for programmer-like titles
      return (seniorityPrefix + 'Graphics Programmer').trim();
    }
    if (/\b(engineer|engineering)\b/.test(lower) && !/\b(programm(er|ing))\b/.test(lower)) {
      return (seniorityPrefix + 'Graphics Engineer').trim();
    }
    // fallback
    return (seniorityPrefix + 'Graphics Engineer').trim();
  }

  // cloud-related normalization (Cloud Specialist, Cloud Developer → Cloud Engineer)
  // Exception: Cloud Architect remains separate due to distinct expertise level
  if (/\b(cloud)\b/.test(lower)) {
    if (/\b(architect)\b/.test(lower)) {
      return (seniorityPrefix + 'Cloud Architect').trim();
    }
    if (/\b(specialist|developer|engineer|consultant|analyst)\b/.test(lower)) {
      return (seniorityPrefix + 'Cloud Engineer').trim();
    }
  }

  // engine programmer / game engine
  if (/\b(engine)\b/.test(lower) && /\b(programm(er|ing))\b/.test(lower)) {
    return (seniorityPrefix + 'Engine Programmer').trim();
  }
  if (/\b(game engine)\b/.test(lower)) {
    return (seniorityPrefix + 'Engine Programmer').trim();
  }

  // general programmer vs engineer detection
  if (/\b(programm(er|ing))\b/.test(lower)) {
    return (seniorityPrefix + 'Programmer').trim();
  }
  if (/\b(engineer|software eng|swe|eng)\b/.test(lower)) {
    return (seniorityPrefix + 'Engineer').trim();
  }

  if (/\b(technical artist|tech artist)\b/.test(lower)) {
    return (seniorityPrefix + 'Technical Artist').trim();
  }

  // manager/director
  if (/\b(manager|mgr)\b/.test(lower)) return (seniorityPrefix + 'Manager').trim();
  if (/\b(director|dir)\b/.test(lower)) return (seniorityPrefix + 'Director').trim();

  // default: compact and title-case the original, but prefer some token normalization
  const cleaned = t.replace(/\s{2,}/g, ' ').split(' ')
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
  return (seniorityPrefix + cleaned).trim();
}

// 'Junior', 'Mid', 'Senior', 'Lead', 'Manager', 'Director', 'Expert', 'Executive' or null
function standardizeSeniority(raw) {
  if (!raw) return null;
  // Normalize: lowercase, remove punctuation that separates tokens, convert hyphens/underscores to spaces
  let s = String(raw).trim().toLowerCase();
  s = s.replace(_RE_SEN_CLEAN1, '');           // remove commas/dots
  s = s.replace(_RE_SEN_CLEAN2, ' ');          // convert hyphen/underscore/slash to space
  s = s.replace(_RE_MULTI_SPACE, ' ').trim();  // collapse multiple spaces

  // Exact/Strong matches (tokenized)
  if (_RE_SEN_JUNIOR_EXACT.test(s))    return 'Junior';
  if (_RE_SEN_MID_EXACT.test(s))       return 'Mid';
  if (_RE_SEN_SENIOR_EXACT.test(s))    return 'Senior';
  if (_RE_SEN_LEAD_EXACT.test(s))      return 'Lead';
  if (_RE_SEN_MANAGER_EXACT.test(s))   return 'Manager';
  if (_RE_SEN_DIRECTOR_EXACT.test(s))  return 'Director';
  if (_RE_SEN_EXPERT_EXACT.test(s))    return 'Expert';
  if (_RE_SEN_EXECUTIVE_EXACT.test(s)) return 'Executive';

  // Fuzzy / contains checks for multi-word or noisy strings
  if (_RE_SEN_JUNIOR.test(s))    return 'Junior';
  if (_RE_SEN_MID.test(s))       return 'Mid';
  if (_RE_SEN_SENIOR.test(s))    return 'Senior';
  if (_RE_SEN_LEAD.test(s))      return 'Lead';
  if (_RE_SEN_MANAGER.test(s))   return 'Manager';
  if (_RE_SEN_DIRECTOR.test(s))  return 'Director';
  if (_RE_SEN_EXPERT.test(s))    return 'Expert';
  if (_RE_SEN_EXECUTIVE.test(s)) return 'Executive';

  return null;
}

// Remove special characters (non-alphanumeric) from a string, keeping only letters, numbers, and spaces
function removeSpecialCharacters(text) {
  if (text == null) return null;
  const s = String(text).trim();
  if (!s) return null;
  // Keep only alphanumeric characters and spaces
  return s.replace(/[^a-zA-Z0-9\s]/g, '').replace(/\s{2,}/g, ' ').trim();
}

// Load and cache country code mapping
let countryCodeMap = null;
function loadCountryCodeMap() {
  if (countryCodeMap) return countryCodeMap;
  try {
    const fs = require('fs');
    const countryCodePath = path.resolve(__dirname, 'countrycode.JSON');
    const data = fs.readFileSync(countryCodePath, 'utf8');
    countryCodeMap = JSON.parse(data);
    return countryCodeMap;
  } catch (err) {
    console.warn('[COUNTRY] Failed to load countrycode.JSON:', err.message);
    return {};
  }
}

// Normalize country name using countrycode.JSON mapping
function normalizeCountry(raw) {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  
  const countryMap = loadCountryCodeMap();
  const lower = s.toLowerCase();
  
  // Check for exact match in values (case-insensitive)
  for (const [code, name] of Object.entries(countryMap)) {
    const nameLower = name.toLowerCase();
    if (nameLower === lower) {
      return name;
    }
  }
  
  // Check for common aliases
  const aliases = {
    'south korea': 'Korea',
    'republic of korea': 'Korea',
    'rok': 'Korea',
    'united states of america': 'United States',
    'usa': 'United States',
    'us': 'United States',
    'uk': 'United Kingdom',
    'great britain': 'United Kingdom',
    'uae': 'United Arab Emirates',
    'emirates': 'United Arab Emirates'
  };
  
  if (aliases[lower]) {
    return aliases[lower];
  }
  
  // Check for partial matches (e.g., "South Korea" contains "Korea")
  for (const [code, name] of Object.entries(countryMap)) {
    const nameLower = name.toLowerCase();
    if (lower.includes(nameLower) || nameLower.includes(lower)) {
      return name;
    }
  }
  
  // Return original if no match found, but title-cased
  return s.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
}

// Utility: update process row's company field if canonicalization suggests change.
// Returns an object { company } with canonical value (may be null or same as input).
async function ensureCanonicalFieldsForId(id, currentCompany, currentJobTitle, currentPersonal) {
  const canonicalCompany = normalizeCompanyName(currentCompany || '');

  // Build SET clauses only if a meaningful change is required.
  const sets = [];
  const values = [];
  let idx = 1;
  if (canonicalCompany != null && String(canonicalCompany).trim() !== String(currentCompany || '').trim()) {
    sets.push(`company = $${idx}`); values.push(canonicalCompany); idx++;
  }

  if (sets.length) {
    values.push(id);
    const sql = `UPDATE "process" SET ${sets.join(', ')} WHERE id = $${idx}`;
    try {
      await pool.query(sql, values);
    } catch (err) {
      console.warn('[CANON] failed to persist canonical fields for id', id, err && err.message);
    }
  }

  return { company: canonicalCompany };
}

// Safe JSON parsing helper and persistence for vskillset

const SAFE_PARSE_MAX_LEN = parseInt(process.env.SAFE_PARSE_MAX_LEN, 10) || _SYS.safe_parse_max_len || 512 * 1024; // 512 KB – skip expensive heuristics on oversized payloads

function safeParseJSONField(raw) {
  if (raw == null) return null;
  if (typeof raw === 'object') return raw;           // already parsed (json/jsonb from pg)
  if (typeof raw !== 'string') return raw;

  // Guard: skip heuristic parsing on excessively large strings to protect the event loop
  if (raw.length > SAFE_PARSE_MAX_LEN) {
    console.warn('[safeParseJSONField] input too large (' + raw.length + ' chars), returning raw');
    return raw;
  }

  // strip control chars and trim
  const s = raw.replace(/[\x00-\x1F\x7F-\x9F]/g, '').trim();
  if (!s) return null;

  // Case A: Starts with normal JSON object/array -> parse directly
  if (/^[\{\[]/.test(s)) {
    try {
      return JSON.parse(s);
    } catch (e) {
      // parsing failed — continue to heuristics
      // console.debug('[safeParseJSONField] direct JSON.parse failed:', e.message);
    }
  }

  // Case B: PostgreSQL array literal of JSON strings:
  // Example: {"{\"skill\":\"Site Activation\",\"probability\":95}", "{\"skill\":\"...\"}", ...}
  if (/^\{\s*\"/.test(s) && s.endsWith('}')) {
    try {
      const inner = s.slice(1, -1);
      const parts = [];
      let cur = '';
      let inQuotes = false;
      for (let i = 0; i < inner.length; i++) {
        const ch = inner[i];
        cur += ch;
        if (ch === '"') {
          // check if escaped
          let backslashCount = 0;
          for (let j = i - 1; j >= 0 && inner[j] === '\\'; j--) backslashCount++;
          if (backslashCount % 2 === 0) inQuotes = !inQuotes;
        }
        if (!inQuotes && ch === ',') {
          parts.push(cur.slice(0, -1));
          cur = '';
        }
      }
      if (cur.length) parts.push(cur);

      const parsedElems = parts.map(el => {
        let sEl = el.trim();
        if (sEl.startsWith('"') && sEl.endsWith('"')) sEl = sEl.slice(1, -1);
        // unescape common sequences
        sEl = sEl.replace(/\\(["\\])/g, '$1');
        try {
          return JSON.parse(sEl);
        } catch (e) {
          // return cleaned string if element is not JSON
          return sEl;
        }
      });

      return parsedElems;
    } catch (e) {
      // fallthrough to tokenization fallback
      // console.debug('[safeParseJSONField] pg-array heuristic failed:', e.message);
    }
  }

  // Case C: tokenization fallback — split on common delimiters and return array if multi-token
  try {
    // Replace a few special separators with a common delimiter, then split.
    const normalized = s
      .replace(/\r\n/g, '\n')
      .replace(/[••·]/g, '\n')
      .replace(/[;|\/]/g, ',')
      .replace(/\band\b/gi, ',')
      .replace(/\s*→\s*/g, ',')
      .trim();

    // Try splitting on commas/newlines and trim tokens
    const tokens = normalized.split(/[\n,]+/)
      .map(t => t.trim())
      .filter(Boolean);

    // If we got multiple sensible tokens, return them as an array
    if (tokens.length > 1) return tokens;

    // If single token that looks like "Skill: X" or "Skill - X", try to extract right-hand part
    const kvMatch = tokens[0] && tokens[0].match(/^[^:\-–—]+[:\-–—]\s*(.+)$/);
    if (kvMatch && kvMatch[1]) {
      return [kvMatch[1].trim()];
    }
  } catch (e) {
    // ignore tokenization errors
  }

  // Final fallback: return original string (no parse)
  return raw;
}

// Try to parse vskillset and persist normalized JSON (stringified) when parse succeeds.
// Returns the parsed object (or the original string/null).
async function parseAndPersistVskillset(id, raw) {
  const parsed = safeParseJSONField(raw);

  // If parsed is an object/array and original was a string, persist the normalized JSON string back to the DB
  if (parsed && (typeof parsed === 'object') && typeof raw === 'string') {
    try {
      await pool.query('UPDATE "process" SET vskillset = $1 WHERE id = $2', [JSON.stringify(parsed), id]);
    } catch (err) {
      console.warn('[parseAndPersistVskillset] failed to persist normalized vskillset for id', id, err && err.message);
    }
  }

  return parsed;
}

// Helper to determine region from country name for validation
function getRegionFromCountry(country) {
  if (!country) return null;
  const c = String(country).trim().toLowerCase();
  // common mappings (extend as needed)
  const asia = ['singapore','china','japan','india','south korea','korea','hong kong','taiwan','thailand','philippines','vietnam','malaysia','indonesia'];
  const northAmerica = ['united states','usa','us','canada','mexico'];
  const westernEurope = ['united kingdom','uk','england','france','germany','spain','italy','netherlands','belgium','sweden','norway','finland','denmark','switzerland','austria','ireland','portugal'];
  const easternEurope = ['russia','poland','ukraine','czech','hungary','slovakia','romania','bulgaria','serbia','croatia','latvia','lithuania','estonia'];
  const middleEast = ['saudi arabia','uae','qatar','israel','iran','iraq','oman','kuwait','jordan','lebanon','bahrain','syria','yemen'];
  const southAmerica = ['brazil','argentina','colombia','chile','peru','venezuela','uruguay','paraguay','bolivia','ecuador'];
  const africa = ['south africa','nigeria','egypt','kenya','ghana','morocco','algeria','tunisia'];
  const oceania = ['australia','new zealand'];

  const groups = [
    { region: 'Asia', list: asia },
    { region: 'North America', list: northAmerica },
    { region: 'Western Europe', list: westernEurope },
    { region: 'Eastern Europe', list: easternEurope },
    { region: 'Middle East', list: middleEast },
    { region: 'South America', list: southAmerica },
    { region: 'Africa', list: africa },
    { region: 'Australia/Oceania', list: oceania }
  ];

  for (const g of groups) {
    for (const name of g.list) {
      if (c.includes(name)) return g.region;
    }
  }
  return null;
}

// Helper: ensure the current req.user owns the given process row id
async function ensureOwnershipOrFail(res, id, userId) {
  try {
    const q = await pool.query('SELECT userid FROM "process" WHERE id = $1', [id]);
    if (q.rows.length === 0) {
      res.status(404).json({ error: 'Not found' });
      return false;
    }
    const owner = q.rows[0].userid;
    if (String(owner) !== String(userId)) {
      res.status(403).json({ error: 'Forbidden: not owner' });
      return false;
    }
    return true;
  } catch (err) {
    console.error('[AUTHZ] ownership check failed', err);
    res.status(500).json({ error: 'Ownership check failed' });
    return false;
  }
}

// ========================= END HELPERS =========================


// ========== NEW: Ensure process table has necessary columns (idempotent) ==========
async function ensureProcessTable() {
  try {
    // Create if missing with a superset of columns we expect.
    // Note: column names are chosen to match the mapping you provided.
    // ADDED linkedinurl to creation script for completeness, though ADD COLUMN below handles existing
    await pool.query(`
      CREATE TABLE IF NOT EXISTS "process" (
        id SERIAL PRIMARY KEY,
        name TEXT,
        jobtitle TEXT,
        company TEXT,
        sector TEXT,
        jobfamily TEXT,
        role_tag TEXT,
        skillset TEXT,
        geographic TEXT,
        country TEXT,
        email TEXT,
        mobile TEXT,
        office TEXT,
        compensation NUMERIC,
        seniority TEXT,
        sourcingstatus TEXT,
        product TEXT,
        userid TEXT,
        username TEXT,
        cv BYTEA,
        lskillset TEXT,
        linkedinurl TEXT,
        jskillset TEXT,
	rating TEXT,
        pic BYTEA,
        education TEXT,
        comment TEXT
      )
    `);

    // Add columns if missing (idempotent)
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS name TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS jobtitle TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS company TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS sector TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS jobfamily TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS role_tag TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS skillset TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS geographic TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS country TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS email TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS mobile TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS office TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS compensation NUMERIC`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS seniority TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS sourcingstatus TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS product TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS userid TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS username TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS cv BYTEA`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS lskillset TEXT`);
    // Ensure linkedinurl column exists for lookups
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS linkedinurl TEXT`);
    // Ensure jskillset column exists
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS jskillset TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS rating TEXT`);
    // Migrate rating column from INTEGER to TEXT if needed (for complex JSON rating objects)
    try {
      await pool.query(`ALTER TABLE "process" ALTER COLUMN rating TYPE TEXT USING rating::TEXT`);
    } catch (_) { /* Column may already be TEXT — safe to ignore */ }
    // Ensure pic column exists for candidate images
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS pic BYTEA`);
    // Ensure sourcing table has pic column for LinkedIn profile images
    await pool.query(`ALTER TABLE sourcing ADD COLUMN IF NOT EXISTS pic BYTEA`);
    // Ensure education column exists
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS education TEXT`);
    // Ensure comment column exists
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS comment TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS vskillset TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS experience TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS tenure TEXT`);
    // Additional DB-only rating/scoring fields
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS exp TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS rating_level TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS rating_updated_at TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS rating_version TEXT`);
  } catch (err) {
    console.error('[INIT] Failed to ensure process table/columns exist:', err);
  }
}

// ========== DB indexes: add after columns exist to avoid missing-column errors ==========
async function ensureProcessIndexes() {
  const idxDefs = [
    // Primary lookup: all candidate queries filter by userid
    { name: 'idx_process_userid',        sql: `CREATE INDEX IF NOT EXISTS idx_process_userid        ON "process" (userid)` },
    // LinkedIn dedup check (dock-in, CV lookup)
    { name: 'idx_process_linkedinurl',   sql: `CREATE INDEX IF NOT EXISTS idx_process_linkedinurl   ON "process" (linkedinurl)` },
    // ML / compensation analytics filter
    { name: 'idx_process_role_tag',      sql: `CREATE INDEX IF NOT EXISTS idx_process_role_tag      ON "process" (role_tag)` },
    // assess-unmatched ORDER BY id DESC paging
    { name: 'idx_process_userid_id',     sql: `CREATE INDEX IF NOT EXISTS idx_process_userid_id     ON "process" (userid, id DESC)` },
    // bulk-update / ownership check id=ANY(...)
    { name: 'idx_process_id',            sql: `CREATE INDEX IF NOT EXISTS idx_process_id            ON "process" (id)` },
    // login table: session auth lookup
    { name: 'idx_login_username',        sql: `CREATE INDEX IF NOT EXISTS idx_login_username        ON "login" (username)` },
    // login table: gemini_query_count admin queries
    { name: 'idx_login_gemini_query_count', sql: `CREATE INDEX IF NOT EXISTS idx_login_gemini_query_count ON "login" (gemini_query_count)` },
    // sourcing dedup
    { name: 'idx_sourcing_linkedinurl',  sql: `CREATE INDEX IF NOT EXISTS idx_sourcing_linkedinurl  ON sourcing (linkedinurl)` },
    { name: 'idx_sourcing_userid',       sql: `CREATE INDEX IF NOT EXISTS idx_sourcing_userid       ON sourcing (userid)` },
  ];
  for (const { name, sql } of idxDefs) {
    try {
      await pool.query(sql);
    } catch (err) {
      console.error(`[INIT] Index ${name} skipped:`, err.message);
    }
  }
}
// Run after ensureProcessTable so columns exist before we index them
ensureProcessTable().then(() => ensureProcessIndexes()).catch(() => {});
// ========== END NEW ==========


// ========== NEW: Ensure login table has columns for Google OAuth (idempotent) ==========
async function ensureLoginColumns() {
  try {
    // Add columns to hold Google OAuth refresh token and optional expiry
    await pool.query(`ALTER TABLE "login" ADD COLUMN IF NOT EXISTS google_refresh_token TEXT`);
    await pool.query(`ALTER TABLE "login" ADD COLUMN IF NOT EXISTS google_token_expires TIMESTAMP`);
    await pool.query(`ALTER TABLE "login" ADD COLUMN IF NOT EXISTS ms_refresh_token TEXT`);
    await pool.query(`ALTER TABLE "login" ADD COLUMN IF NOT EXISTS ms_token_expires TIMESTAMP`);
    // Add corporation column for email template tag [Your Company Name]
    await pool.query(`ALTER TABLE "login" ADD COLUMN IF NOT EXISTS corporation TEXT`);
    // Add per-user target result limit (default 10)
    await pool.query(`ALTER TABLE "login" ADD COLUMN IF NOT EXISTS target_limit INTEGER DEFAULT 10`);
  } catch (err) {
    console.error('[INIT] Failed to ensure login table columns exist:', err);
  }
}
ensureLoginColumns();
// ========== END NEW ==========

// ========================= AUTHENTICATION HELPERS =========================

// Python's Werkzeug generate_password_hash often uses 'pbkdf2:sha256:iterations$salt$hash'
// This helper attempts to verify such a hash using Node built-ins.
function verifyWerkzeugHash(password, hash) {
  if (!hash) return false;
  if (!password) return false;

  const parts = hash.split('$');
  if (parts.length === 3 && parts[0].startsWith('pbkdf2:sha256')) {
    const methodParts = parts[0].split(':');
    const iterations = parseInt(methodParts[2], 10) || 260000; // default default for recent werkzeug
    const salt = parts[1];
    const originalHash = parts[2];

    const derivedKey = crypto.pbkdf2Sync(password, salt, iterations, 32, 'sha256');
    const derivedHex = derivedKey.toString('hex');
    return derivedHex === originalHash;
  }
  
  // Fallback: simple comparison or bcrypt if your DB uses bcrypt (standard $2b$ prefix)
  // If your DB has plain text (unsafe), this covers it too.
  if (hash === password) return true;
  
  return false;
}

// Authentication Middleware
const requireLogin = async (req, res, next) => {
  // Allow OPTIONS preflight
  if (req.method === 'OPTIONS') return next();

  // Check cookies
  const userid     = req.cookies.userid;
  const username   = req.cookies.username;
  const session_id = req.cookies.session_id;

  if (!userid || !username) {
    return res.status(401).json({ error: 'Unauthorized', message: 'Authentication required' });
  }

  // Validate session_id against DB — prevents forged cookies from bypassing auth.
  // If no session_id cookie exists (legacy session before this hardening), fall back
  // to a lightweight DB existence check so existing active sessions are not broken.
  try {
    if (session_id) {
      const r = await pool.query(
        'SELECT userid FROM login WHERE username = $1 AND session_id = $2 LIMIT 1',
        [username, session_id]
      );
      if (!r.rows.length) {
        return res.status(401).json({ error: 'Unauthorized', message: 'Session expired or invalid' });
      }
    } else {
      // Legacy fallback: just confirm the (userid, username) pair exists in DB
      const r = await pool.query(
        'SELECT userid FROM login WHERE username = $1 LIMIT 1',
        [username]
      );
      if (!r.rows.length) {
        return res.status(401).json({ error: 'Unauthorized', message: 'Authentication required' });
      }
    }
  } catch (err) {
    // DB unavailable — fail closed: cannot verify session, return 503
    console.error('[requireLogin] DB session check failed:', err.message);
    return res.status(503).json({ error: 'Service unavailable', message: 'Authentication service temporarily unavailable' });
  }

  req.user = { id: userid, username: username };
  next();
};

// CSRF mitigation: reject state-changing requests without X-Requested-With or X-CSRF-Token.
// Browsers cannot set these custom headers in cross-site form submissions.
// GET and OPTIONS requests are exempt; only POST/PUT/PATCH/DELETE are checked.
const requireCsrfHeader = (req, res, next) => {
  if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) {
    if (!req.headers['x-requested-with'] && !req.headers['x-csrf-token']) {
      return res.status(403).json({ error: 'Missing required header (X-Requested-With or X-CSRF-Token)' });
    }
  }
  next();
};

// Apply CSRF header check globally for all mutation requests
app.use(requireCsrfHeader);

// ========================= AUTH ROUTES =========================

// GET /api/platform-provider-status — admin-level custom provider flags.
// Returns { email_verif_custom, llm_custom } based on admin configs.
// No API keys are exposed — only boolean flags.
app.get('/api/platform-provider-status', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    // Email verif: check if any non-default provider is enabled with a key
    const evCfg = loadEmailVerifConfig();
    const emailVerifCustom = EMAIL_VERIF_SERVICES.some(svc => {
      const c = evCfg[svc] || {};
      return c.enabled === 'enabled' && !!c.api_key;
    });
    // LLM: check if a non-default (non-gemini) provider is enabled with a key
    const llmCfg = _readFullLlmConfig();
    const llmCustom = ['openai', 'anthropic'].some(p => {
      const c = llmCfg[p] || {};
      return c.enabled === 'enabled' && !!c.api_key;
    });
    res.json({ email_verif_custom: emailVerifCustom, llm_custom: llmCustom });
  } catch (err) {
    console.error('[platform-provider-status]', err);
    res.status(500).json({ error: 'Could not read platform provider config' });
  }
});

// ── User-facing: linkdapi enabled status (no key, just boolean) ────────────
app.get('/api/linkdapi-status', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const cfg = loadGetProfilesConfig();
    const ld = cfg.linkdapi || {};
    res.json({ enabled: ld.enabled === 'enabled' && !!ld.api_key });
  } catch (err) {
    res.json({ enabled: false });
  }
});

app.post('/login', userRateLimit('login'), async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ ok: false, error: "Missing credentials" });
  }

  try {
    const result = await pool.query('SELECT * FROM login WHERE username = $1', [username]);
    if (result.rows.length === 0) {
      return res.status(401).json({ ok: false, error: "Invalid username or password" });
    }

    const user = result.rows[0];
    const storedHash = user.password; // Assumes column name is 'password'

    const isValid = verifyWerkzeugHash(password, storedHash);
    
    if (!isValid) {
      return res.status(401).json({ ok: false, error: "Invalid username or password" });
    }

    // Success
    const uid = user.id || user.userid || user.username;

    // Generate a server-side session ID — stored in DB and sent as an httpOnly cookie.
    // requireLogin validates this value against the DB so forged cookies are rejected.
    const newSessionId = crypto.randomBytes(32).toString('hex');
    try {
      await pool.query('UPDATE login SET session_id = $1 WHERE username = $2', [newSessionId, user.username]);
    } catch (err) {
      // Column missing on first boot (migration pending) is acceptable; log anything else
      if (!err.message.includes('column') && !err.message.includes('session_id')) {
        console.error('[login] Failed to store session_id:', err.message);
      }
    }
    
    // Set cookies — httpOnly prevents JS access; Secure is enabled when running behind HTTPS (NODE_ENV=production)
    const cookieOpts = { maxAge: _SESSION_COOKIE_MAX_AGE_MS, httpOnly: true, path: '/', sameSite: 'lax', secure: process.env.NODE_ENV === 'production' };
    res.cookie('username', user.username, cookieOpts);
    res.cookie('userid', String(uid), cookieOpts);
    res.cookie('session_id', newSessionId, cookieOpts);

    // Load the user's SMTP config from their per-user JSON file so the
    // frontend can reflect the saved settings immediately without a separate
    // round-trip.  The password is intentionally excluded here — it stays
    // on the server and is injected by the /send-email handler as needed.
    const smtpCfgFull = await loadSmtpConfig(user.username);
    const smtpCfgPublic = smtpCfgFull
      ? { host: smtpCfgFull.host, port: smtpCfgFull.port, user: smtpCfgFull.user, secure: smtpCfgFull.secure }
      : null;

    res.json({
      ok: true,
      userid: uid,
      username: user.username,
      full_name: user.fullname || user.username,
      corporation: user.corporation || '',
      smtpConfig: smtpCfgPublic
    });

  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({ ok: false, error: "Internal login error" });
  }
});

app.post('/logout', async (req, res) => {
  const username = req.cookies.username;
  // Invalidate the server-side session so the session_id cookie can no longer be reused
  if (username) {
    try {
      await pool.query('UPDATE login SET session_id = NULL WHERE username = $1', [username]);
    } catch (err) {
      console.error('[logout] Failed to clear session_id for', username, ':', err.message);
    }
  }
  res.clearCookie('username',   { path: '/' });
  res.clearCookie('userid',     { path: '/' });
  res.clearCookie('session_id', { path: '/' });
  res.json({ ok: true, message: "Logged out" });
});

app.get('/user/resolve', async (req, res) => {
  const userid = req.cookies.userid;
  const username = req.cookies.username;
  
  if (userid && username) {
    // UPDATED: query full_name from DB instead of just returning cookies
    try {
      const r = await pool.query('SELECT fullname AS full_name, corporation, useraccess, cemail, COALESCE(token, 0) AS token FROM login WHERE username = $1', [username]);
      const full_name = (r.rows.length > 0 && r.rows[0].full_name) ? r.rows[0].full_name : "";
      const corporation = (r.rows.length > 0 && r.rows[0].corporation) ? r.rows[0].corporation : "";
      const useraccess = (r.rows.length > 0 && r.rows[0].useraccess) ? r.rows[0].useraccess : "";
      const cemail = (r.rows.length > 0 && r.rows[0].cemail) ? r.rows[0].cemail : "";
      const token = (r.rows.length > 0) ? Number(r.rows[0].token) : 0;
      return res.json({ ok: true, userid, username, full_name, corporation, useraccess, cemail, token });
    } catch(e) {
      // Fallback if DB fails
      return res.json({ ok: true, userid, username });
    }
  }
  
  // Fallback for query param check if needed similar to Flask
  const qName = req.query.username;
  if (qName) {
     try {
       const result = await pool.query('SELECT id, username, fullname AS full_name, corporation, useraccess, COALESCE(token, 0) AS token FROM login WHERE username = $1', [qName]);
       if (result.rows.length > 0) {
         const u = result.rows[0];
         return res.json({ ok: true, userid: u.id, username: u.username, full_name: u.full_name, corporation: u.corporation || '', useraccess: u.useraccess || '', token: Number(u.token) });
       }
     } catch(e) {}
  }

  res.status(401).json({ ok: false });
});

// GET /auth/check — lightweight session validity check used by login.html.
// requireLogin validates the session_id cookie against the DB. Returns 200 if
// the session is valid; 401 if not (stale cookie, logged-out, or no cookie).
app.get('/auth/check', dashboardRateLimit, requireLogin, (req, res) => {
  res.json({ ok: true, username: req.user.username });
});

// POST /auth/extend-session — re-issues the session cookie with a fresh maxAge so the
// user stays logged in after the session-timeout warning dialog "Stay Logged In" is clicked.
app.post('/auth/extend-session', dashboardRateLimit, requireLogin, async (req, res) => {
  const username = req.user.username;
  try {
    const cookieOpts = { maxAge: _SESSION_COOKIE_MAX_AGE_MS, httpOnly: true, path: '/', sameSite: 'lax', secure: process.env.NODE_ENV === 'production' };
    // Re-issue all three session cookies with a fresh maxAge.
    res.cookie('username', username, cookieOpts);
    res.cookie('userid', String(req.user.userid || ''), cookieOpts);
    res.cookie('session_id', req.cookies.session_id, cookieOpts);
    res.json({ ok: true });
  } catch (err) {
    console.error('[extend-session] error:', err.message);
    res.status(500).json({ error: 'Failed to extend session' });
  }
});

// GET /user/rate-limits - Return the effective rate limits for the calling user.
// Per-user overrides (if any) take precedence over global defaults.
app.get('/user/rate-limits', requireLogin, (req, res) => {
  const username = req.user.username;
  const config = loadRateLimits();
  const defaults = config.defaults || {};
  const userOverrides = (config.users || {})[username] || {};
  // Merge: per-user override wins, then default, otherwise no limit recorded
  const effective = {};
  const allFeatures = new Set([...Object.keys(defaults), ...Object.keys(userOverrides)]);
  for (const feature of allFeatures) {
    effective[feature] = (feature in userOverrides) ? userOverrides[feature] : defaults[feature];
  }
  res.json({ ok: true, limits: effective, has_overrides: Object.keys(userOverrides).length > 0 });
});

// GET /user/gemini-model — Returns the BYOK user's saved Gemini model preference.
app.get('/user/gemini-model', requireLogin, userRateLimit('gemini_model'), async (req, res) => {
  try {
    const r = await pool.query('SELECT gemini_model, useraccess FROM login WHERE username = $1 LIMIT 1', [req.user.username]);
    if (!r.rows.length || (r.rows[0].useraccess || '').toLowerCase() !== 'byok') {
      return res.status(403).json({ error: 'Model preference is only available for BYOK accounts.' });
    }
    const model = r.rows[0].gemini_model || 'gemini-2.5-flash-lite';
    res.json({ ok: true, model });
  } catch (e) {
    res.json({ ok: true, model: 'gemini-2.5-flash-lite' });
  }
});

// PUT /user/gemini-model — Saves the BYOK user's Gemini model preference.
app.put('/user/gemini-model', requireLogin, userRateLimit('gemini_model'), async (req, res) => {
  const model = (req.body && req.body.model) || '';
  if (!ALLOWED_GEMINI_MODELS.includes(model)) {
    return res.status(400).json({ error: 'Invalid model selection.' });
  }
  try {
    const uaRes = await pool.query('SELECT useraccess FROM login WHERE username = $1 LIMIT 1', [req.user.username]);
    if (!uaRes.rows.length || (uaRes.rows[0].useraccess || '').toLowerCase() !== 'byok') {
      return res.status(403).json({ error: 'Model preference is only available for BYOK accounts.' });
    }
    await pool.query('UPDATE login SET gemini_model = $1 WHERE username = $2', [model, req.user.username]);
    res.json({ ok: true, model });
  } catch (e) {
    res.status(500).json({ error: 'Failed to save model preference.', detail: e.message });
  }
});

// GET /user-tokens - Fetch user token information from login table
// NOTE: Consider adding rate limiting for this endpoint in production
app.get('/user-tokens', requireLogin, async (req, res) => {
  try {
    const username = req.user.username;
    const result = await pool.query('SELECT token FROM login WHERE username = $1', [username]);
    
    if (result.rows.length > 0) {
      const accountTokens = result.rows[0].token || 0;
      // For now, tokensLeft is the same as accountTokens
      // You can add separate logic if needed
      return res.json({ 
        accountTokens: accountTokens,
        tokensLeft: accountTokens 
      });
    }
    
    res.json({ accountTokens: 0, tokensLeft: 0 });
  } catch (err) {
    console.error('Error fetching user tokens:', err);
    res.status(500).json({ error: 'Failed to fetch tokens' });
  }
});

// GET /token-config - Return the token credit/deduction configuration from rate_limits.json
// Used by SourcingVerify and AutoSourcing to read dynamic token rates and credit amounts.
// Always reads from disk (bypasses the shared rate-limits cache) so admin changes saved via
// Flask are visible immediately without waiting for the cache TTL to expire.
app.get('/token-config', requireLogin, (req, res) => {
  try {
    let cfg;
    try {
      cfg = JSON.parse(fs.readFileSync(RATE_LIMITS_PATH, 'utf8'));
    } catch (_) {
      cfg = {};
    }
    const t = cfg.tokens || {};
    res.json({
      appeal_approve_credit:     t.appeal_approve_credit     ?? 1,
      verified_selection_deduct: t.verified_selection_deduct ?? 2,
      contact_gen_deduct:        t.contact_gen_deduct        ?? 2,
      rebate_credit_per_profile: t.rebate_credit_per_profile ?? 1,
      analytic_token_cost:       t.analytic_token_cost       ?? 1,
      initial_token_display:     t.initial_token_display     ?? 5000,
      sourcing_rate_base:        t.sourcing_rate_base        ?? 1,
      sourcing_rate_best_mode:   t.sourcing_rate_best_mode   ?? 1.5,
      sourcing_rate_over50:      t.sourcing_rate_over50      ?? 2,
      sourcing_rate_best_over50: t.sourcing_rate_best_over50 ?? 2.5,
      token_cost_sgd:            t.token_cost_sgd            ?? 0.10,
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed to load token config' });
  }
});

// GET /developing-countries – return the list of developing countries (used by the
// Compensation Calculator to suppress the low-salary warning for developing regions)
app.get('/developing-countries', requireLogin, (req, res) => {
  try {
    const filePath = path.join(__dirname, 'developing_countries.json');
    const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    res.json(data);
  } catch (_) {
    res.json([]);
  }
});

// ── SMTP config persistence ──────────────────────────────────────────────────
// Each user's SMTP config is stored in its own file inside SMTP_CONFIG_DIR.
// Set the SMTP_CONFIG_DIR environment variable to override the default location.
// Default: <server directory>/smtp_config
const SMTP_CONFIG_DIR = process.env.SMTP_CONFIG_DIR || path.join(__dirname, 'smtp_config');
// Ensure the directory exists at startup (log its location so operators can verify)
console.log('[SMTP] Config directory:', SMTP_CONFIG_DIR);
fs.mkdirSync(SMTP_CONFIG_DIR, { recursive: true });

function smtpConfigPath(username) {
  // Sanitise username: keep only alphanumeric and underscores to prevent path traversal
  const safe = username.replace(/[^a-zA-Z0-9_]/g, '_');
  return path.join(SMTP_CONFIG_DIR, `smtp-config-${safe}.json`);
}

async function loadSmtpConfig(username) {
  try {
    const data = await fs.promises.readFile(smtpConfigPath(username), 'utf8');
    return JSON.parse(data);
  } catch (err) {
    if (err.code !== 'ENOENT') console.error('loadSmtpConfig parse error:', err.message);
    return null;
  }
}

async function saveSmtpConfig(username, config) {
  const p = smtpConfigPath(username);
  const tmp = p + '.tmp';
  // NOTE: password is stored as plaintext — ensure this directory is outside the web root and not committed.
  await fs.promises.writeFile(tmp, JSON.stringify(config, null, 2), 'utf8');
  await fs.promises.rename(tmp, p);
}

// GET /smtp-config – return the current user's saved SMTP configuration
app.get('/smtp-config', requireLogin, async (req, res) => {
  try {
    const entry = await loadSmtpConfig(req.user.username);
    if (!entry) return res.json({ ok: true, config: null });
    const { userid, username, host, port, user, secure } = entry;
    // Return config without exposing the password
    res.json({ ok: true, config: { userid, username, host, port, user, secure } });
  } catch (err) {
    console.error('GET /smtp-config error:', err);
    res.status(500).json({ error: 'Failed to load SMTP config' });
  }
});

// POST /smtp-config – save the current user's SMTP configuration
app.post('/smtp-config', requireLogin, async (req, res) => {
  try {
    const { host, port, user, pass, secure } = req.body || {};
    if (!host || !user) return res.status(400).json({ error: 'host and user are required' });
    await saveSmtpConfig(req.user.username, {
      userid: String(req.user.id),
      username: req.user.username,
      host,
      port: port || '587',
      user,
      pass: pass || '',
      secure: !!secure,
    });
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /smtp-config error:', err);
    res.status(500).json({ error: 'Failed to save SMTP config' });
  }
});

// ── Server-side custom-provider check ──────────────────────────────────────────
// Reads the per-user service config (same source as /api/user-service-config/status)
// and returns { emailVerif: bool, llm: bool } indicating whether the user has
// their own API keys for email verification or LLM.  When active, the server
// skips token deduction — this is the authoritative guard (the frontend check is
// a UX optimisation only; the server MUST enforce the rule).
function _userHasCustomProviders(username) {
  try {
    const cfg = readUserServiceConfig(username);
    if (!cfg) return { emailVerif: false, llm: false, contactGen: false, emailVerifProvider: '', llmProvider: '', contactGenProvider: '' };
    const ep = ((cfg.email_verif && cfg.email_verif.provider) || '').toLowerCase();
    const lp = ((cfg.llm && cfg.llm.provider) || '').toLowerCase();
    const cp = ((cfg.contact_gen && cfg.contact_gen.provider) || '').toLowerCase();
    return {
      emailVerif: ep === 'neverbounce' || ep === 'zerobounce' || ep === 'bouncer',
      emailVerifProvider: ep,
      llm:        lp === 'openai' || lp === 'anthropic',
      llmProvider: lp,
      contactGen: cp === 'contactout' || cp === 'apollo' || cp === 'rocketreach',
      contactGenProvider: cp,
    };
  } catch (err) {
    console.error('[_userHasCustomProviders]', err.message);
    return { emailVerif: false, llm: false, contactGen: false, emailVerifProvider: '', llmProvider: '', contactGenProvider: '' };
  }
}

// POST /deduct-tokens - Deduct tokens from the authenticated user (called on Verified Selection)
app.post('/deduct-tokens', requireLogin, userRateLimit('upload_multiple_cvs'), async (req, res) => {
  try {
    const username = req.user.username;
    const userid   = String(req.user.id || '');
    const selectedService = ((req.body && req.body.service) || '').toLowerCase();

    // Skip deduction only when the user has custom email verification keys AND
    // the selected service matches their own provider (server-side guard).
    const customProviders = _userHasCustomProviders(username);
    if (customProviders.emailVerif && selectedService && customProviders.emailVerifProvider === selectedService) {
      const curRes = await pool.query('SELECT COALESCE(token, 0) AS t FROM login WHERE username = $1', [username]);
      const current = curRes.rows.length ? parseInt(curRes.rows[0].t, 10) : 0;
      return res.json({ tokensLeft: current, accountTokens: current, skipped: true });
    }

    // Single CTE: lock the row, compute new token, and return both values in one roundtrip.
    // Uses a multi-step CTE to avoid a repeated scalar subquery in RETURNING.
    const deductRes = await pool.query(
      `WITH prev AS (
         SELECT COALESCE(token, 0) AS old_token FROM login WHERE username = $1 FOR UPDATE
       ),
       upd AS (
         UPDATE login SET token = GREATEST(0, (SELECT old_token FROM prev) - $2)
         WHERE username = $1
         RETURNING login.token AS new_token
       )
       SELECT prev.old_token AS token_before, upd.new_token AS token_after FROM prev, upd`,
      [username, _VERIFIED_SELECTION_DEDUCT]
    );
    if (deductRes.rows.length === 0) return res.status(404).json({ error: 'User not found' });
    const tokenBefore = parseInt(deductRes.rows[0].token_before, 10);
    const remaining   = deductRes.rows[0].token_after;
    _writeFinancialLog({
      username, userid, feature: 'verified_selection',
      transaction_type: 'spend', transaction_amount: _VERIFIED_SELECTION_DEDUCT,
      token_before: tokenBefore, token_after: remaining,
      token_usage: _VERIFIED_SELECTION_DEDUCT, credits_spent: 0,
      token_cost_sgd: _TOKEN_COST_SGD, revenue_sgd: Math.round(_VERIFIED_SELECTION_DEDUCT * _TOKEN_COST_SGD * 10000) / 10000,
    });
    res.json({ tokensLeft: remaining, accountTokens: remaining });
  } catch (err) {
    console.error('Error deducting tokens:', err);
    res.status(500).json({ error: 'Failed to deduct tokens' });
  }
});

// POST /deduct-tokens-contact-gen - Deduct tokens for Generate Email/Contact actions (ContactOut/Apollo/RocketReach)
app.post('/deduct-tokens-contact-gen', requireLogin, userRateLimit('upload_multiple_cvs'), async (req, res) => {
  try {
    const username = req.user.username;
    const userid   = String(req.user.id || '');
    const selectedService = ((req.body && req.body.service) || '').toLowerCase();

    // Skip deduction only when the user has their own contact generation keys AND
    // the selected service matches their own provider (server-side guard).
    const customProviders = _userHasCustomProviders(username);
    if (customProviders.contactGen && selectedService && customProviders.contactGenProvider === selectedService) {
      const curRes = await pool.query('SELECT COALESCE(token, 0) AS t FROM login WHERE username = $1', [username]);
      const current = curRes.rows.length ? parseInt(curRes.rows[0].t, 10) : 0;
      return res.json({ tokensLeft: current, accountTokens: current, skipped: true });
    }

    // Read current deduct amount from rate_limits config (uses short TTL cache)
    let deductAmt = _CONTACT_GEN_DEDUCT;
    try {
      const cfg = loadRateLimits();
      const v = (cfg.tokens || {}).contact_gen_deduct;
      if (typeof v === 'number') deductAmt = v;
    } catch (_) {}

    // Single CTE: lock the row, compute new token, and return both values in one roundtrip.
    // Uses a multi-step CTE to avoid a repeated scalar subquery in RETURNING.
    const deductRes = await pool.query(
      `WITH prev AS (
         SELECT COALESCE(token, 0) AS old_token FROM login WHERE username = $1 FOR UPDATE
       ),
       upd AS (
         UPDATE login SET token = GREATEST(0, (SELECT old_token FROM prev) - $2)
         WHERE username = $1
         RETURNING login.token AS new_token
       )
       SELECT prev.old_token AS token_before, upd.new_token AS token_after FROM prev, upd`,
      [username, deductAmt]
    );
    if (deductRes.rows.length === 0) return res.status(404).json({ error: 'User not found' });
    const tokenBefore = parseInt(deductRes.rows[0].token_before, 10);
    const remaining   = deductRes.rows[0].token_after;
    _writeFinancialLog({
      username, userid, feature: 'contact_gen',
      transaction_type: 'spend', transaction_amount: deductAmt,
      token_before: tokenBefore, token_after: remaining,
      token_usage: deductAmt, credits_spent: 0,
      token_cost_sgd: _TOKEN_COST_SGD, revenue_sgd: Math.round(deductAmt * _TOKEN_COST_SGD * 10000) / 10000,
    });
    res.json({ tokensLeft: remaining, accountTokens: remaining });
  } catch (err) {
    console.error('Error deducting contact gen tokens:', err);
    res.status(500).json({ error: 'Failed to deduct tokens' });
  }
});

// POST /candidates/token-deduct - Deduct N tokens after Analytic DB Dock In (1 token per new record)
app.post('/candidates/token-deduct', requireLogin, userRateLimit('upload_multiple_cvs'), async (req, res) => {
  try {
    const count = parseInt((req.body && req.body.count) || 0, 10);
    if (!count || count <= 0) return res.json({ tokensLeft: 0, accountTokens: 0 });
    const username = req.user.username;
    const userid   = String(req.user.id || '');
    // Skip token deduction for BYOK users
    const accessRes = await pool.query('SELECT useraccess, COALESCE(token, 0) AS t FROM login WHERE username = $1', [username]);
    if (accessRes.rows.length > 0 && (accessRes.rows[0].useraccess || '').toLowerCase() === 'byok') {
      const current = parseInt(accessRes.rows[0].t, 10);
      return res.json({ tokensLeft: current, accountTokens: current });
    }
    // Skip deduction when custom LLM or email verif keys are active (server-side guard)
    const customProviders = _userHasCustomProviders(username);
    if (customProviders.llm || customProviders.emailVerif) {
      const current = accessRes.rows.length ? parseInt(accessRes.rows[0].t, 10) : 0;
      return res.json({ tokensLeft: current, accountTokens: current, skipped: true });
    }
    // `accessRes` already holds the token value; no second SELECT needed
    const tokenBefore = accessRes.rows.length ? parseInt(accessRes.rows[0].t, 10) : 0;
    const result = await pool.query(
      'UPDATE login SET token = GREATEST(0, COALESCE(token, 0) - $2) WHERE username = $1 RETURNING token',
      [username, count]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'User not found' });
    const remaining = result.rows[0].token;
    _writeApprovalLog({ action: 'token_deduct_dock_in', username, userid, detail: `Deducted ${count} token(s) for Analytic DB Dock In. Remaining: ${remaining}`, source: 'server.js' });
    _writeFinancialLog({
      username, userid, feature: 'db_analytics',
      transaction_type: 'spend', transaction_amount: count,
      token_before: tokenBefore, token_after: remaining,
      token_usage: count, credits_spent: 0,
      token_cost_sgd: _TOKEN_COST_SGD, revenue_sgd: Math.round(count * _TOKEN_COST_SGD * 10000) / 10000,
    });
    res.json({ tokensLeft: remaining, accountTokens: remaining });
  } catch (err) {
    console.error('Error deducting dock-in tokens:', err);
    res.status(500).json({ error: 'Failed to deduct tokens' });
  }
});

// ========================= END AUTH ROUTES =========================

app.get('/', (req, res) => {
  res.send('Backend API is running!');
});

app.get('/skillset-mapping', (req, res) => {
  try {
    if (!fs.existsSync(mappingPath)) {
      return res.status(404).json({ error: 'skillset-mapping.json not found.' });
    }
    const raw = fs.readFileSync(mappingPath, 'utf8');
    const json = JSON.parse(raw);
    res.json(json);
  } catch (err) {
    console.error('Read skillset-mapping error:', err);
    res.status(500).json({ error: 'Failed to read skillset mapping.' });
  }
});

// === Helpers for ingestion normalization (Project_Title/Project_Date restoration) ===
function firstVal(obj, keys = []) {
  for (const k of keys) {
    if (Object.prototype.hasOwnProperty.call(obj, k) && obj[k] != null && String(obj[k]).trim() !== '') {
      return obj[k];
    }
  }
  return undefined;
}

// Parse to YYYY-MM-DD; supports SG DD/MM/YYYY and Excel serials
function toISODate(value) {
  if (value == null || value === '') return null;

  // Numeric Excel serial
  if (typeof value === 'number' && Number.isFinite(value)) {
    const epoch = new Date(Date.UTC(1899, 11, 30));
    const dt = new Date(epoch.getTime() + value * 86400000);
    if (!isNaN(dt.getTime())) {
      const yyyy = dt.getUTCFullYear();
      const mm = String(dt.getUTCMonth() + 1).padStart(2, '0');
      const dd = String(dt.getUTCDate()).padStart(2, '0');
      return `${yyyy}-${mm}-${dd}`;
    }
  }

  if (value instanceof Date && !isNaN(value.getTime())) {
    const yyyy = value.getUTCFullYear();
    const mm = String(value.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(value.getUTCDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
  }

  if (typeof value === 'string') {
    const v = value.trim();

    // ISO or starts with ISO
    const iso = v.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;

    // DD/MM/YYYY or DD-MM-YYYY
    const sg = v.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    if (sg) {
      const dd = sg[1].padStart(2, '0');
      const mm = sg[2].padStart(2, '0');
      const yyyy = sg[3];
      return `${yyyy}-${mm}-${dd}`;
    }

    const dt = new Date(v);
    if (!isNaN(dt.getTime())) {
      const yyyy = dt.getUTCFullYear();
      const mm = String(dt.getUTCMonth() + 1).padStart(2, '0');
      const dd = String(dt.getUTCDate()).padStart(2, '0');
      return `${yyyy}-${mm}-${dd}`;
    }
  }
  return null;
}

function normalizeIncomingRow(c) {
  return {
    id: (c.id != null && !isNaN(Number(c.id)) && Number(c.id) > 0) ? Number(c.id) : null,
    name: firstVal(c, ['name', 'Name']) || '',
    role: firstVal(c, ['jobtitle', 'job_title', 'Job Title', 'role', 'Role']) || '',
    // Accept exact process table column name 'company' as well as legacy 'organisation'
    organisation: firstVal(c, ['company', 'organisation', 'Organisation']) || '',
    sector: firstVal(c, ['sector', 'Sector']) || '',
    // Accept exact process table column name 'jobfamily' as well as legacy 'job_family'
    job_family: firstVal(c, ['jobfamily', 'job_family', 'Job Family']) || '',
    role_tag: firstVal(c, ['role_tag', 'Role Tag']) || '',
    skillset: firstVal(c, ['skillset', 'Skillset']) || '',
    geographic: firstVal(c, ['geographic', 'Geographic']) || '',
    country: firstVal(c, ['country', 'Country']) || '',
    email: firstVal(c, ['email', 'Email']) || '',
    mobile: firstVal(c, ['mobile', 'Mobile']) || '',
    office: firstVal(c, ['office', 'Office']) || '',
    compensation: (() => {
      const v = firstVal(c, ['compensation', 'Compensation', 'personal', 'Personal']);
      if (v === '' || v == null) return null;
      const n = Number(v);
      return isNaN(n) ? null : n;
    })(),
    seniority: firstVal(c, ['seniority', 'Seniority']) || '',
    // Accept exact process table column name 'sourcingstatus' as well as legacy 'sourcing_status'
    sourcing_status: firstVal(c, ['sourcingstatus', 'sourcing_status', 'Sourcing Status']) || '',
    product: firstVal(c, ['product', 'Product', 'type']) || null,
    linkedinurl: firstVal(c, ['linkedinurl', 'linkedin', 'LinkedIn', 'URL']) || '', // Added for capture
    cv: firstVal(c, ['cv', 'CV', 'resume', 'Resume']) || '',
    // Additional process-table columns preserved from DB Copy when not overridden by Sheet 1
    comment: firstVal(c, ['comment', 'Comment']) || null,
    lskillset: firstVal(c, ['lskillset']) || null,
    vskillset: (() => {
      const v = firstVal(c, ['vskillset']);
      if (v == null) return null;
      if (typeof v === 'object') return JSON.stringify(v);
      const s = String(v).trim();
      return s || null;
    })(),
    education: firstVal(c, ['education', 'Education']) || null,
    experience: firstVal(c, ['experience', 'Experience']) || null,
    tenure: firstVal(c, ['tenure', 'Tenure']) || null,
    rating: (() => {
      const v = firstVal(c, ['rating', 'Rating']);
      if (v == null) return null;
      // Serialize complex rating objects (e.g. assessment_level objects) to JSON string for TEXT column
      if (typeof v === 'object') return JSON.stringify(v);
      const s = String(v).trim();
      return s || null;
    })(),
    jskillset: firstVal(c, ['jskillset']) || null,
    // Additional DB-only fields from DB Copy JSON
    exp: firstVal(c, ['exp']) || null,
    rating_level: firstVal(c, ['rating_level']) || null,
    rating_updated_at: firstVal(c, ['rating_updated_at']) || null,
    rating_version: firstVal(c, ['rating_version']) || null,
  };
}

// Mapping from normalized candidate-style keys to process table columns
const processColumnMap = {
  id: 'id',
  name: 'name',
  role: 'jobtitle',
  organisation: 'company',
  sector: 'sector',
  job_family: 'jobfamily',
  role_tag: 'role_tag',
  skillset: 'skillset',
  geographic: 'geographic',
  country: 'country',
  email: 'email',
  mobile: 'mobile',
  office: 'office',
  compensation: 'compensation',
  seniority: 'seniority',
  sourcing_status: 'sourcingstatus',
  product: 'product',
  linkedinurl: 'linkedinurl',
  // DB Copy passthrough fields
  comment: 'comment',
  lskillset: 'lskillset',
  vskillset: 'vskillset',
  education: 'education',
  experience: 'experience',
  tenure: 'tenure',
  rating: 'rating',
  jskillset: 'jskillset',
  // Additional DB-only fields
  exp: 'exp',
  rating_level: 'rating_level',
  rating_updated_at: 'rating_updated_at',
  rating_version: 'rating_version',
};

// ========== UPDATED: BULK INGESTIONsupports Project_Title and Project_Date and writes to process table ==========
app.post('/candidates/bulk', requireLogin, userRateLimit('upload_multiple_cvs'), async (req, res) => {
  let candidates = req.body.candidates;
  console.log('==== DB Dock & Deploy ====');
  console.log('Received candidates:', JSON.stringify(candidates, null, 2));
  if (!Array.isArray(candidates) || candidates.length === 0) {
    console.log('No candidates data provided!');
    return res.status(400).json({ error: 'No candidates data provided.' });
  }

  candidates = candidates.filter(
    c => Object.values(c).some(val => val && String(val).trim() !== '')
  );

  if (candidates.length === 0) {
    console.log('No valid candidates found!');
    return res.status(400).json({ error: 'No valid candidates found.' });
  }

  // Normalize each row to include canonical and legacy fields
  const normalized = candidates.map(normalizeIncomingRow);

  // Canonical + legacy insertion keys (normalized) — 'role' maps to jobtitle; no duplicate
  const normKeys = [
    'id', 'name', 'role', 'organisation', 'sector', 'job_family',
    'role_tag', 'skillset', 'geographic', 'country',
    'email', 'mobile', 'office', 'compensation',
    'seniority', 'sourcing_status', 'product', 'linkedinurl',
    // DB Copy passthrough: preserved from export JSON, overridden by Sheet 1 where applicable
    'comment', 'lskillset', 'vskillset', 'education', 'experience', 'tenure',
    'rating', 'jskillset',
    // Additional DB-only rating/scoring fields from DB Copy
    'exp', 'rating_level', 'rating_updated_at', 'rating_version',
  ];

  try {
    // Fetch user's JD skill from login table using USERNAME (more reliable)
    let userJskillset = null;
    try {
      const ures = await pool.query('SELECT jskillset FROM login WHERE username = $1', [req.user.username]);
      if (ures.rows.length > 0) userJskillset = ures.rows[0].jskillset || null;
    } catch (e) {
      console.warn('[BULK] unable to fetch user jskillset via username', e && e.message);
      userJskillset = null;
    }

    // Editable column keys (exclude 'id' — never update the primary key via SET)
    const updateNormKeys = normKeys.filter(k => k !== 'id');

    // ── Identity matching ─────────────────────────────────────────────────────
    // Primary:   (userid, LOWER(RTRIM('/',linkedinurl))) → UPDATE existing record
    // Secondary: (userid, id from DB Copy)               → UPDATE (handles changed/missing linkedinurl)
    // Fallback:  INSERT preserving original id from DB Copy when valid
    // Normalize a LinkedIn URL to lowercase without trailing slash for robust matching
    const normalizeLinkedInUrl = u => u ? u.trim().toLowerCase().replace(/\/+$/, '') : '';

    const incomingLinkedInUrls = normalized
      .map(r => normalizeLinkedInUrl(r.linkedinurl))
      .filter(u => u !== '');

    const existingByLinkedin = {};   // normalised linkedinurl → existing DB row id
    if (incomingLinkedInUrls.length > 0) {
      const existingRes = await pool.query(
        `SELECT id, linkedinurl FROM "process" WHERE userid = $1 AND LOWER(RTRIM(linkedinurl, '/')) = ANY($2::text[])`,
        [req.user.id, incomingLinkedInUrls]
      );
      existingRes.rows.forEach(row => {
        if (row.linkedinurl) existingByLinkedin[normalizeLinkedInUrl(row.linkedinurl)] = row.id;
      });
    }

    // Secondary match by id from DB Copy (catches records with changed/missing linkedinurl)
    const incomingDbIds = normalized
      .filter(r => r.id != null && Number.isFinite(r.id) && r.id > 0)
      .map(r => r.id);
    const existingDbIds = new Set();
    if (incomingDbIds.length > 0) {
      const idRes = await pool.query(
        `SELECT id FROM "process" WHERE userid = $1 AND id = ANY($2::int[])`,
        [req.user.id, incomingDbIds]
      );
      idRes.rows.forEach(row => existingDbIds.add(row.id));
    }

    // Split: matched → UPDATE; unmatched → INSERT
    const updateRows = [];
    const insertRows = [];
    normalized.forEach(row => {
      const key = normalizeLinkedInUrl(row.linkedinurl);
      if (key && existingByLinkedin[key] !== undefined) {
        // Primary match by linkedinurl (trailing-slash normalised, case-insensitive)
        updateRows.push({ ...row, matchedId: existingByLinkedin[key] });
      } else if (row.id && existingDbIds.has(row.id)) {
        // Secondary match by id from DB Copy
        updateRows.push({ ...row, matchedId: row.id });
      } else {
        insertRows.push(row);
      }
    });

    let totalAffected = 0;
    const canonicalUpdates = []; // collected for best-effort post-commit canonical normalization

    // ── Wrap UPDATE + INSERT + sequence advance in a single transaction ────────
    const dbClient = await pool.connect();
    try {
      await dbClient.query('BEGIN');

      // ── UPDATE existing records (authoritative: file values override DB) ──────
      // Assessment-derived fields (vskillset, rating, lskillset, exp, rating_level,
      // rating_updated_at, rating_version) use COALESCE so that a null incoming
      // value preserves the existing DB value rather than wiping it.  A non-null
      // incoming value still wins and overwrites the DB.  This protects assessment
      // data for unaffected records during Analytic Dock In.
      const ASSESSMENT_PRESERVE_KEYS = new Set([
        'vskillset', 'rating', 'lskillset', 'exp',
        'rating_level', 'rating_updated_at', 'rating_version',
      ]);
      // In analytic Dock In mode, records whose IDs are in analyticSkipUpdateIds have
      // no matching uploaded CV and must not be modified.  Skip their UPDATE entirely
      // so all existing DB data (including vskillset) is preserved untouched.
      // These records are still INSERTed when they don't exist in the DB yet (e.g.
      // after a Dock Out cleared the DB).
      const analyticSkipUpdateSet = new Set(
        (req.body.analyticSkipUpdateIds || []).map(Number).filter(n => Number.isFinite(n))
      );
      for (const row of updateRows) {
        if (analyticSkipUpdateSet.size > 0 && analyticSkipUpdateSet.has(row.matchedId)) {
          // Unmatched record in analytic mode: preserve all existing DB data unchanged.
          continue;
        }
        const setClauses = [];
        const vals = [];
        let pi = 1;
        updateNormKeys.forEach(k => {
          let v = Object.prototype.hasOwnProperty.call(row, k) ? row[k] : null;
          if (v === '') v = null;
          if (k === 'seniority' && v != null && String(v).trim() !== '') {
            v = standardizeSeniority(v) || null;
          }
          const col = processColumnMap[k] || k;
          if (ASSESSMENT_PRESERVE_KEYS.has(k)) {
            setClauses.push(`${col} = COALESCE($${pi++}, ${col})`);
          } else {
            setClauses.push(`${col} = $${pi++}`);
          }
          vals.push(v);
        });
        vals.push(req.user.id);       // WHERE userid
        vals.push(row.matchedId);     // WHERE id
        await dbClient.query(
          `UPDATE "process" SET ${setClauses.join(', ')} WHERE userid = $${pi} AND id = $${pi + 1}`,
          vals
        );
        totalAffected++;
        canonicalUpdates.push({ id: row.matchedId, organisation: row.organisation || null, role: row.role || '' });
      }

      // ── INSERT new records ────────────────────────────────────────────────────
      // Rows with a valid id from DB Copy get inserted with that id preserved
      // (enables backup/restore round-trips). Rows without an id get an
      // auto-generated id. After any id-specific inserts the sequence is
      // advanced past MAX(id) to avoid future conflicts.
      if (insertRows.length > 0) {
        const rowsWithId    = insertRows.filter(r => r.id != null);
        const rowsWithoutId = insertRows.filter(r => r.id == null);

        const runInsert = async (rows, includeId) => {
          if (!rows.length) return 0;
          const iKeys      = includeId ? normKeys : normKeys.filter(k => k !== 'id');
          const iProcCols  = iKeys.map(k => processColumnMap[k] || k);
          iProcCols.push('userid', 'username');

          const iValues = [];
          const iPlaceholders = rows.map((row, i) => {
            const start = i * iProcCols.length + 1;
            iKeys.forEach(k => {
              let v = Object.prototype.hasOwnProperty.call(row, k) ? row[k] : null;
              if (v === '') v = null;
              if (k === 'seniority' && v != null && String(v).trim() !== '') {
                v = standardizeSeniority(v) || null;
              }
              if (k === 'jskillset' && v == null) v = userJskillset;
              iValues.push(v);
            });
            iValues.push(req.user.id);
            iValues.push(req.user.username);
            return `(${Array.from({ length: iProcCols.length }, (_, j) => `$${start + j}`).join(',')})`;
          }).join(',');

          const iSql = `INSERT INTO "process" (${iProcCols.join(', ')}) VALUES ${iPlaceholders} RETURNING id`;
          const iRes = await dbClient.query(iSql, iValues);
          for (let i = 0; i < iRes.rows.length; i++) {
            canonicalUpdates.push({ id: iRes.rows[i].id, organisation: rows[i].organisation || null, role: rows[i].role || '' });
          }
          return iRes.rowCount;
        };

        const n1 = await runInsert(rowsWithId, true);
        const n2 = await runInsert(rowsWithoutId, false);
        totalAffected += n1 + n2;

        // Advance the sequence past any explicitly-inserted ids to prevent
        // future auto-generated ids from colliding with restored originals.
        if (rowsWithId.length > 0) {
          await dbClient.query(
            `SELECT setval(pg_get_serial_sequence('"process"', 'id'),
                           (SELECT MAX(id) FROM "process"))
             WHERE EXISTS (SELECT 1 FROM "process")`
          );
        }
      }

      await dbClient.query('COMMIT');
    } catch (txErr) {
      await dbClient.query('ROLLBACK');
      throw txErr;
    } finally {
      dbClient.release();
    }

    // Best-effort canonical field normalization — runs after the transaction commits.
    // Use Promise.all so all rows are processed in parallel rather than sequentially.
    await Promise.all(canonicalUpdates.map(({ id, organisation, role }) =>
      ensureCanonicalFieldsForId(id, organisation, role, null).catch(
        e => console.warn('[BULK_CANON] row', id, e && e.message)
      )
    ));

    console.log('Upserted/inserted rows into process:', totalAffected);

    // Notify clients that candidates were changed (clients can choose to refetch)
    try {
      broadcastSSE('candidates_changed', { action: 'bulk_upsert', count: totalAffected });
    } catch (_) { /* ignore emit errors */ }

    res.json({ rowsInserted: totalAffected });
    _writeApprovalLog({ action: 'bulk_candidates_upsert', username: req.user.username, userid: req.user.id, detail: `DB Dock & Deploy upserted/inserted ${totalAffected} candidates`, source: 'server.js' });

    // Background ML profile refresh — recompute and persist ML_{username}.json so confidence
    // scores reflect the latest candidate data (non-blocking; failures are non-fatal).
    _buildMLProfileData(String(req.user.id), req.user.username)
      .then(data => _persistMLUserFile(req.user.username, data))
      .catch(err => console.warn('[bulk] ML profile background refresh failed (non-fatal):', err.message));
  } catch (err) {
    console.error('Bulk insert error:', err);
    res.status(500).json({ error: err.message || 'Bulk insert failed.' });
  }
});

// GET /candidates: return process rows but include candidate-style fallback keys
// UPDATED: Filter by userid to ensure user only sees their own records
app.get('/candidates', requireLogin, userRateLimit('candidates'), async (req, res) => {
  try {
    // Always restrict to the authenticated user's records
    const result = await pool.query('SELECT * FROM "process" WHERE userid = $1 ORDER BY id DESC', [String(req.user.id)]);
    const processedRows = [];

    for (const r of result.rows) {
      // Parse/normalize vskillset (and persist normalized JSON back to DB when parse succeeds)
      const parsedVskillset = await parseAndPersistVskillset(r.id, r.vskillset);

      // Convert pic to a data URI (or URL) that the frontend can use directly
      const picBase64 = picToDataUri(r.pic);

      // compensation sourced directly from the process table's compensation column
      const companyCanonical = normalizeCompanyName(r.company || r.organisation || '');

      // Parse rating if it's a JSON string
      let parsedRating = r.rating;
      if (r.rating && typeof r.rating === 'string') {
        try {
          // Clean the string before parsing
          const cleanedRating = r.rating
            .replace(/[\x00-\x1F\x7F-\x9F]/g, '') // Remove control characters
            .trim();
          
          // Check if it looks like JSON before trying to parse
          if (cleanedRating && (cleanedRating.startsWith('{') || cleanedRating.startsWith('['))) {
            parsedRating = JSON.parse(cleanedRating);
          } else {
            // Keep as string if it doesn't look like JSON
            parsedRating = r.rating;
          }
        } catch (e) {
          // Silently handle parse failures - keep as string if parse fails
          parsedRating = r.rating;
        }
      }

      const mapped = {
        ...r,
        jobtitle: r.jobtitle ?? null,
        company: companyCanonical ?? (r.company ?? null),
        jobfamily: r.jobfamily ?? null,
        sourcingstatus: r.sourcingstatus ?? null,
        product: r.product ?? null,
        lskillset: r.lskillset ?? null,
        vskillset: parsedVskillset ?? null, // use parsed object (or null)
        rating: parsedRating ?? null,
        linkedinurl: r.linkedinurl ?? null,
        jskillset: r.jskillset ?? null,
        pic: picBase64,

        role: r.role ?? r.jobtitle ?? null,
        organisation: companyCanonical ?? (r.organisation ?? r.company ?? null),
        job_family: r.job_family ?? r.jobfamily ?? null,
        sourcing_status: r.sourcing_status ?? r.sourcingstatus ?? null,
        type: r.product ?? null,
        compensation: r.compensation ?? null
      };

      processedRows.push(mapped);
    }

    res.json(processedRows);
  } catch (err) {
    console.error('Fetch process rows error:', err);
    res.status(500).json({ error: 'Failed to fetch candidates/process rows.' });
  }
});

// GET /candidates/:id/cv - Secure CV Fetch by ID (Keep existing)
app.get('/candidates/:id/cv', requireLogin, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) return res.status(400).send('Invalid ID');

  try {
    // Ownership guard
    const q = await pool.query('SELECT userid, cv FROM "process" WHERE id = $1', [id]);
    if (q.rows.length === 0) return res.status(404).send('No CV found');
    if (String(q.rows[0].userid) !== String(req.user.id)) return res.status(403).send('Forbidden');

    const cv = q.rows[0].cv;

    if (!cv) {
      return res.status(404).send('No CV found');
    }

    // Handle Buffer (Postgres BYTEA)
    if (Buffer.isBuffer(cv)) {
        res.setHeader('Content-Type', 'application/pdf');
        // Optional: Check magic bytes for PDF to be sure, otherwise default to pdf
        return res.send(cv);
    }

    // Handle String (Base64 or File Path)
    if (typeof cv === 'string') {
        // If it's a data URI
        if (cv.startsWith('data:')) {
            const matches = cv.match(/^data:([A-Za-z-+\/]+);base64,(.+)$/);
            if (matches && matches.length === 3) {
                const type = matches[1];
                const buf = Buffer.from(matches[2], 'base64');
                res.setHeader('Content-Type', type);
                return res.send(buf);
            }
        }
        try {
           const buf = Buffer.from(cv, 'base64');
           res.setHeader('Content-Type', 'application/pdf');
           return res.send(buf);
        } catch (e) {
           // Not base64
        }
    }

    // Fallback
    res.status(500).send('Unknown CV format');

  } catch (err) {
    console.error('CV fetch error:', err);
    res.status(500).send('Server Error');
  }
});

// ========== NEW: GET /process/download_cv - Secure CV Fetch by LinkedIn URL ==========
app.get('/process/download_cv', requireLogin, async (req, res) => {
  const linkedinUrl = req.query.linkedin;
  if (!linkedinUrl) {
    return res.status(400).send('Missing linkedin parameter');
  }

  try {
    // Fetch process row and ensure ownership
    const result = await pool.query('SELECT cv, userid FROM "process" WHERE linkedinurl = $1', [linkedinUrl]);
    
    // If exact match fails, try relaxed match (without query params or trailing slash)
    if (result.rows.length === 0) {
        const relaxed = linkedinUrl.split('?')[0].replace(/\/+$/, '');
        const retry = await pool.query('SELECT cv, userid FROM "process" WHERE linkedinurl LIKE $1', [relaxed + '%']);
        if (retry.rows.length > 0) {
             if (String(retry.rows[0].userid) !== String(req.user.id)) return res.status(403).send('Forbidden');
             if (!retry.rows[0].cv) return res.status(404).send('No CV found');
             return serveCV(res, retry.rows[0].cv);
        }
        return res.status(404).send('No CV found for this profile');
    }

    if (String(result.rows[0].userid) !== String(req.user.id)) {
      return res.status(403).send('Forbidden');
    }

    if (!result.rows[0].cv) {
      return res.status(404).send('No CV found');
    }

    serveCV(res, result.rows[0].cv);

  } catch (err) {
    console.error('/process/download_cv error:', err);
    res.status(500).send('Server Error');
  }
});

function serveCV(res, cv) {
    // Handle Buffer (Postgres BYTEA)
    if (Buffer.isBuffer(cv)) {
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Length', cv.length);
        return res.send(cv);
    }

    // Handle String (Base64)
    if (typeof cv === 'string') {
        if (cv.startsWith('data:')) {
            const matches = cv.match(/^data:([A-Za-z-+\/]+);base64,(.+)$/);
            if (matches && matches.length === 3) {
                const type = matches[1];
                const buf = Buffer.from(matches[2], 'base64');
                res.setHeader('Content-Type', type);
                res.setHeader('Content-Length', buf.length);
                return res.send(buf);
            }
        }
        try {
           const buf = Buffer.from(cv, 'base64');
           res.setHeader('Content-Type', 'application/pdf');
           res.setHeader('Content-Length', buf.length);
           return res.send(buf);
        } catch (e) { }
    }
    res.status(500).send('Unknown CV format');
}

// ── Path constants shared by multiple endpoints ───────────────────────────────
const CRITERIA_DIR = process.env.CRITERIA_DIR
  || path.join('F:\\', 'Recruiting Tools', 'Autosourcing', 'output', 'Criteras');

// Root output directory written by webbridge.py (job_*.json, bulk_*_results*.json, assessment files).
// Override via AUTOSOURCING_OUTPUT_DIR env var for non-Windows or custom installs.
const AUTOSOURCING_OUTPUT_DIR = process.env.AUTOSOURCING_OUTPUT_DIR
  || path.join('F:\\', 'Recruiting Tools', 'Autosourcing', 'output');

// ML output directory: Gemini-generated JSON files (ML_<username>.json) written here on bulletin
// draft and on DB Dock Out.  Kept in its own subdirectory so the clear-user cleanup scan
// (which targets AUTOSOURCING_OUTPUT_DIR root only) does not delete them.
// Override via ML_OUTPUT_DIR env var for non-Windows or custom installs.
const ML_OUTPUT_DIR = process.env.ML_OUTPUT_DIR
  || path.join(AUTOSOURCING_OUTPUT_DIR, 'ML');

// On Windows the canonical path is: F:\Recruiting Tools\Autosourcing\Candidate Analyser\backend\save state
// Override for any deployment via the SAVE_STATE_DIR environment variable:
//   SAVE_STATE_DIR=C:\your\custom\path   (Windows)
//   SAVE_STATE_DIR=/your/custom/path     (Linux/Mac)
// If the F: drive does not exist on a Windows host, set SAVE_STATE_DIR explicitly.
const SAVE_STATE_DIR = process.env.SAVE_STATE_DIR
    ? path.resolve(process.env.SAVE_STATE_DIR)
    : (process.platform === 'win32'
        ? path.resolve('F:\\Recruiting Tools\\Autosourcing\\Candidate Analyser\\backend\\save state')
        : path.join(__dirname, 'save state'));

function getSaveStatePath(username) {
    // Sanitise: only allow alphanumeric, dash and underscore to prevent path traversal
    const safe = String(username).replace(/[^a-zA-Z0-9_\-]/g, '_');
    return path.join(SAVE_STATE_DIR, `dashboard_${safe}.json`);
}

// POST /candidates/archive-appeals — save pending appeal records to disk before DB Dock Out.
// Called by executeDockOut (App.js) before clear-user so that any sourcing rows with a
// non-empty appeal field are persisted to APPEAL_ARCHIVE_DIR/appeal_<username>.json as an
// array of records.  The admin appeals panel (admin_rate_limits.html) reads both the live
// DB and these JSON files, so appeals remain visible after the sourcing table is cleared.
// Must be defined BEFORE the /:id route so Express matches the literal path first.
app.post('/candidates/archive-appeals', requireLogin, userRateLimit('bulk_delete'), async (req, res) => {
  const username = req.user.username;
  const userid   = String(req.user.id);
  try {
    await pool.query(`ALTER TABLE sourcing ADD COLUMN IF NOT EXISTS appeal TEXT`).catch(() => {});
    const r = await pool.query(`
      SELECT linkedinurl,
             COALESCE(name, '') AS name,
             COALESCE(jobtitle, '') AS jobtitle,
             COALESCE(company, '') AS company,
             COALESCE(appeal, '') AS appeal,
             COALESCE(username, '') AS username,
             COALESCE(userid, '') AS userid,
             COALESCE(role_tag, '') AS role_tag
      FROM sourcing
      WHERE (userid = $1 OR username = $2)
        AND appeal IS NOT NULL AND appeal != ''
    `, [userid, username]);
    if (r.rows.length > 0) {
      try {
        const safe = username.replace(/[^\w\-]/g, '_');
        fs.mkdirSync(APPEAL_ARCHIVE_DIR, { recursive: true });
        const fp = path.join(APPEAL_ARCHIVE_DIR, `appeal_${safe}.json`);
        const absDir = path.resolve(APPEAL_ARCHIVE_DIR);
        const absFile = path.resolve(fp);
        if (!absFile.startsWith(absDir + path.sep)) throw new Error('Path traversal detected');
        // Merge with any previously archived records so no records are lost if
        // archive-appeals is called more than once before clear-user.
        let existing = [];
        try {
          if (fs.existsSync(fp)) {
            const raw = JSON.parse(fs.readFileSync(fp, 'utf8'));
            if (Array.isArray(raw)) existing = raw;
          }
        } catch (_) {}
        const mergedMap = new Map(existing.map(x => [x.linkedinurl, x]));
        r.rows.forEach(row => mergedMap.set(row.linkedinurl, row));
        fs.writeFileSync(fp, JSON.stringify([...mergedMap.values()], null, 2), 'utf8');
      } catch (writeErr) {
        console.warn('[archive-appeals] File write failed (non-fatal):', writeErr.message);
      }
    }
    res.json({ ok: true, count: r.rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /candidates/clear-user — remove all process + sourcing rows for the logged-in user,
// delete their save-state and criteria JSON files.
// Used by DB Dock Out after export to clear the user's data.
// Must be defined BEFORE the /:id route so Express matches the literal path first.
app.delete('/candidates/clear-user', requireLogin, userRateLimit('bulk_delete'), async (req, res) => {
  const username = req.user.username;
  const userid   = String(req.user.id);
  try {
    // 1. Delete all records from the process table
    const result = await pool.query(
      'DELETE FROM "process" WHERE userid = $1 RETURNING id',
      [userid]
    );

    // 2. Delete all records from the sourcing table for this user
    await pool.query(
      'DELETE FROM sourcing WHERE userid = $1 OR username = $2',
      [userid, username]
    ).catch(e => console.warn('[clear-user] sourcing delete failed (non-fatal):', e.message));

    // 3. Delete save-state files (orgchart_<username>.json + dashboard_<username>.json)
    const safe = String(username).replace(/[^a-zA-Z0-9_\-]/g, '_');
    for (const prefix of ['orgchart', 'dashboard']) {
      const fp = path.join(SAVE_STATE_DIR, `${prefix}_${safe}.json`);
      try { if (fs.existsSync(fp)) fs.unlinkSync(fp); } catch (e) {
        console.warn(`[clear-user] Could not delete ${fp}:`, e.message);
      }
    }

    // 4. Delete criteria JSON files for this user from CRITERIA_DIR.
    //    Criteria files are named: "<role_tag> <username>.json" (username at end, space-separated).
    try {
      if (fs.existsSync(CRITERIA_DIR)) {
        const suffix = ` ${username}.json`;
        const entries = fs.readdirSync(CRITERIA_DIR).filter(f =>
          f.toLowerCase().endsWith('.json') &&
          f.slice(-suffix.length).toLowerCase() === suffix.toLowerCase()
        );
        for (const f of entries) {
          try { fs.unlinkSync(path.join(CRITERIA_DIR, f)); } catch (e) {
            console.warn(`[clear-user] Could not delete criteria file ${f}:`, e.message);
          }
        }
      }
    } catch (e) {
      console.warn('[clear-user] Criteria dir cleanup failed (non-fatal):', e.message);
    }

    // 5. Delete output JSON files associated with this user from AUTOSOURCING_OUTPUT_DIR.
    //    Files are named with a _<username> suffix, e.g.:
    //      job_<id>_<username>.json
    //      bulk_<id>_results_<username>.json
    //      assessments/assessment_<hash>_<username>.json
    const _safeUsername = String(username).replace(/[^a-zA-Z0-9_\-]/g, '');
    if (_safeUsername) {
      const _outputDirs = [
        AUTOSOURCING_OUTPUT_DIR,
        path.join(AUTOSOURCING_OUTPUT_DIR, 'assessments'),
      ];
      for (const dir of _outputDirs) {
        try {
          if (fs.existsSync(dir)) {
            const _suffix = `_${_safeUsername}.json`;
            const _files = fs.readdirSync(dir).filter(f =>
              f.endsWith('.json') &&
              f.length >= _suffix.length &&
              f.slice(-_suffix.length).toLowerCase() === _suffix.toLowerCase()
            );
            for (const f of _files) {
              try { fs.unlinkSync(path.join(dir, f)); } catch (e) {
                console.warn(`[clear-user] Could not delete output file ${f}:`, e.message);
              }
            }
          }
        } catch (e) {
          console.warn(`[clear-user] Output dir cleanup failed for ${dir} (non-fatal):`, e.message);
        }
      }
    }

    try {
      broadcastSSE('candidates_changed', { action: 'clear_user', userid: req.user.id });
    } catch (_) { /* ignore */ }

    // 6. Wipe BYOK keys file — all BYOK data must be cleared on DB Dock Out
    try {
      const bPath = byokFilePath(username);
      if (fs.existsSync(bPath)) {
        fs.unlinkSync(bPath);
        _writeInfraLog({ event_type: 'byok_wiped', username, userid, detail: 'BYOK keys wiped during DB Dock Out', status: 'success', source: 'server.js' });
      }
    } catch (e) {
      console.warn('[clear-user] Could not wipe BYOK keys (non-fatal):', e.message);
    }

    // 7. Delete Autosourcing search XLS/CSV output files belonging to this user.
    //    Files are named {username}_{job_id}_results.xlsx / .csv (safe chars only).
    try {
      if (fs.existsSync(SEARCH_XLS_DIR)) {
        const _xlsPrefix = `${safe}_`;
        const _xlsBase   = path.resolve(SEARCH_XLS_DIR);
        const _xlsEntries = fs.readdirSync(SEARCH_XLS_DIR).filter(f => {
          // Filename must start with safe_username prefix and end with _results.xlsx/.csv.
          // Also reject any entry whose name contains a path separator to prevent traversal.
          if (f.includes('/') || f.includes('\\')) return false;
          const lower = f.toLowerCase();
          return f.startsWith(_xlsPrefix) && (lower.endsWith('_results.xlsx') || lower.endsWith('_results.csv'));
        });
        for (const f of _xlsEntries) {
          try {
            const resolved = path.resolve(path.join(SEARCH_XLS_DIR, f));
            // Verify the resolved path is still inside SEARCH_XLS_DIR
            if (!resolved.startsWith(_xlsBase + path.sep) && resolved !== _xlsBase) {
              console.warn(`[clear-user] Skipping searchxls file outside base dir: ${f}`);
              continue;
            }
            fs.unlinkSync(resolved);
          } catch (e) {
            console.warn(`[clear-user] Could not delete searchxls file ${f}:`, e.message);
          }
        }
      }
    } catch (e) {
      console.warn('[clear-user] searchxls cleanup failed (non-fatal):', e.message);
    }

    res.json({ deleted: result.rowCount });
  } catch (err) {
    console.error('Clear-user delete error:', err);
    res.status(500).json({ error: 'Failed to clear user data.' });
  }
});

// GET /candidates/bulletin-preview — return process-table data grouped for the bulletin modal
app.get('/candidates/bulletin-preview', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT role_tag, seniority, skillset, country, jobfamily, sector, rating, sourcingstatus
       FROM "process" WHERE userid = $1`,
      [String(req.user.id)]
    );
    const rows = result.rows;
    const roleTags = [...new Set(rows.map(r => r.role_tag).filter(Boolean))];
    const skillsetCounts = {};
    rows.forEach(r => {
      if (r.skillset) {
        r.skillset.split(',').map(s => s.trim()).filter(Boolean).forEach(s => {
          skillsetCounts[s] = (skillsetCounts[s] || 0) + 1;
        });
      }
    });
    const skillsets = Object.entries(skillsetCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([s]) => s);
    const jobfamilies = [...new Set(rows.map(r => r.jobfamily).filter(Boolean))];
    const sectors = [...new Set(rows.map(r => r.sector).filter(Boolean))];
    const countries = [...new Set(rows.map(r => r.country).filter(Boolean))];
    res.json({ rows, roleTags, skillsets, jobfamilies, sectors, countries });
  } catch (err) {
    console.error('[Bulletin Preview] Error:', err);
    res.status(500).json({ error: 'Failed to fetch bulletin preview data.' });
  }
});

// POST /candidates/bulletin-draft — AI-assisted headline + description generation for bulletin export
app.post('/candidates/bulletin-draft', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { prompt: userPrompt, context } = req.body;
    const roleTag = context?.role_tag || '';
    const sector = context?.sector || '';
    const seniority = context?.seniority || '';
    const skillsets = Array.isArray(context?.skillsets) ? context.skillsets.join(', ') : '';
    const instruction = `
You are a talent acquisition specialist writing concise, professional copy for a talent marketplace bulletin card.
Based on the following talent pool details:
- Role Tag: ${roleTag}
- Sector: ${sector}
- Seniority: ${seniority}
- Key Skills: ${skillsets}
- Additional context: "${userPrompt}"

Write a short, compelling card entry. Return strictly a JSON object with two fields:
{
  "headline": "A short title (max 60 chars) that combines the role and key specialisation",
  "description": "A compelling one-liner (max 80 chars) summarising the talent pool based on the user's context above — do NOT default to just listing sector and seniority unless the user prompt calls for it"
}
Do not wrap in markdown code blocks.
    `.trim();
    const text = await llmGenerateText(instruction, { username: req.user && req.user.username, label: 'llm/bulletin-draft' });
    incrementGeminiQueryCount(req.user.username).catch(() => {});
    const jsonStr = text.replace(/```json|```/g, '').trim();
    let data;
    try {
      data = JSON.parse(jsonStr);
    } catch (parseErr) {
      console.warn('[bulletin-draft] JSON parse failed, attempting regex fallback:', parseErr.message);
      const match = text.match(/\{[\s\S]*\}/);
      if (match) data = JSON.parse(match[0]);
      else throw new Error('Failed to parse AI bulletin draft response');
    }
    res.json(data);
  } catch (err) {
    console.error('/candidates/bulletin-draft error:', err);
    res.status(500).json({ error: 'Bulletin draft failed.' });
  }
});

// Helper: build a proportion map from an array of string values (blanks excluded).
// Returns null when no non-blank values are present.
function _buildDistribution(values) {
  const counts = {};
  let total = 0;
  for (const v of values) {
    const k = (v || '').trim();
    if (!k) continue;
    counts[k] = (counts[k] || 0) + 1;
    total++;
  }
  if (total === 0) return null;
  const dist = {};
  let sumRounded = 0;
  const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]);
  for (const [k, c] of entries) {
    const p = Math.round((c / total) * 100) / 100;
    dist[k] = p;
    sumRounded += p;
  }
  const drift = Math.round((1.0 - sumRounded) * 100) / 100;
  if (drift !== 0 && entries.length > 0) dist[entries[0][0]] = Math.round((dist[entries[0][0]] + drift) * 100) / 100;
  return dist;
}

// _buildMLProfileData — compute the ML profile data object for a given user's candidate pool.
// Used by both the ml-summary endpoint (Dock Out) and the background refresh after bulk save.
async function _buildMLProfileData(userid, username, useraccess) {
  // If useraccess was not explicitly provided (or was passed as null), fetch it directly from
  // login.useraccess in Postgres using the reliable username lookup.
  // This ensures all call sites (bulk upsert, on-the-fly compute, Google Sheets export, ml-summary)
  // always embed the correct access level from the DB rather than defaulting to null.
  if (useraccess == null) {
    try {
      const uaRow = await pool.query(
        'SELECT useraccess FROM login WHERE username = $1 LIMIT 1',
        [username]
      );
      useraccess = (uaRow.rows.length > 0 && uaRow.rows[0].useraccess)
        ? String(uaRow.rows[0].useraccess).toLowerCase()
        : null;
    } catch (_) {
      useraccess = null;
    }
  }

  const today = new Date().toISOString().split('T')[0];
  const candidateResult = await pool.query(
    `SELECT jobtitle, role_tag, sector, seniority, jobfamily,
            skillset, lskillset, vskillset,
            country, compensation, sourcingstatus, company, exp
     FROM "process" WHERE userid = $1`,
    [userid]
  );
  if (candidateResult.rows.length === 0) {
    const ua = useraccess || null;
    const emptyCompensation = { last_updated: today, username, useraccess: ua, compensation_by_job_title: {} };
    const emptyData = {
      Job_Families: [],
      company: { last_updated: today, username, useraccess: ua, sector: {} },
    };
    emptyData._sections = {
      job_title: { last_updated: today, username, useraccess: ua, job_title: {} },
      company: emptyData.company,
      compensation: emptyCompensation,
    };
    return emptyData;
  }

  const allSkillTokens = [];
  const seniorityVals = [], jobfamilyVals = [], countryVals = [], statusVals = [];
  const jobtitles = [];
  const compensationNums = [];

  // Track company → sector record counts (count how many candidates have each company+sector combo).
  // Confidence = count(company, sector) / count(company, all sectors) — record-count based.
  const perCompanySectorCounts = {};  // { companyName: { sectorName: count } }
  const COMP_NO_COUNTRY = '__no_country__';

  // Track per-job-title compensation numbers grouped by country to build per-country arrays.
  const perJobTitleCompByCountry = {};  // { jobTitle: { country: [num, ...] } }

  // Track per-job-title seniority, job family, skills, and experience for per-title profiles.
  const perJobTitleSeniority   = {};  // { jobTitle: [seniority, ...] }
  const perJobTitleJobFamily   = {};  // { jobTitle: [jobFamily, ...] }
  const perJobTitleSkillTokens = {};  // { jobTitle: [skillToken, ...] }
  const perJobTitleExp         = {};  // { jobTitle: [exp, ...] }

  for (const r of candidateResult.rows) {
    const jtRaw = (r.jobtitle || r.role_tag || '').trim();
    jobtitles.push(jtRaw);
    seniorityVals.push(r.seniority || '');
    jobfamilyVals.push(r.jobfamily || '');
    countryVals.push(r.country || '');
    statusVals.push(r.sourcingstatus || '');
    const companyName = (r.company || '').trim();
    const sectorName  = (r.sector  || '').trim();
    if (companyName) {
      if (!perCompanySectorCounts[companyName]) perCompanySectorCounts[companyName] = {};
      if (sectorName) perCompanySectorCounts[companyName][sectorName] = (perCompanySectorCounts[companyName][sectorName] || 0) + 1;
    }
    const skillParts = [];
    if (r.skillset) skillParts.push(r.skillset);
    if (r.lskillset) skillParts.push(r.lskillset);
    if (r.vskillset) {
      try {
        const vs = typeof r.vskillset === 'string' ? JSON.parse(r.vskillset) : r.vskillset;
        if (vs && typeof vs === 'object') {
          const vsSkills = vs.skills || vs.skillset || vs.tags || null;
          if (Array.isArray(vsSkills)) skillParts.push(vsSkills.join(', '));
          else if (typeof vsSkills === 'string') skillParts.push(vsSkills);
        }
      } catch (_) {}
    }
    const skillStr = skillParts.join(', ');
    const skillTokens = skillStr
      ? skillStr.split(/[,;|\/\n]+/).map(s => s.trim()).filter(s => s.length > 1)
      : [];
    allSkillTokens.push(...skillTokens);
    // Collect per-job-title seniority, job family, skill tokens, and experience
    if (jtRaw) {
      if (!perJobTitleSeniority[jtRaw])   perJobTitleSeniority[jtRaw]   = [];
      if (!perJobTitleJobFamily[jtRaw])   perJobTitleJobFamily[jtRaw]   = [];
      if (!perJobTitleSkillTokens[jtRaw]) perJobTitleSkillTokens[jtRaw] = [];
      if (!perJobTitleExp[jtRaw])         perJobTitleExp[jtRaw]         = [];
      if (r.seniority) perJobTitleSeniority[jtRaw].push(r.seniority);
      if (r.jobfamily) perJobTitleJobFamily[jtRaw].push(r.jobfamily.trim());
      perJobTitleSkillTokens[jtRaw].push(...skillTokens);
      if (r.exp != null) {
        const expStr = String(r.exp).trim();
        if (expStr) perJobTitleExp[jtRaw].push(expStr);
      }
    }
    if (r.compensation) {
      const numMatch = String(r.compensation).replace(/[,\s]/g, '').match(/[\d]+(?:\.\d+)?/);
      if (numMatch) {
        const compNum = parseFloat(numMatch[0]);
        compensationNums.push(compNum);
        if (jtRaw) {
          if (!perJobTitleCompByCountry[jtRaw]) perJobTitleCompByCountry[jtRaw] = {};
          const countryKey = (r.country || '').trim() || COMP_NO_COUNTRY;
          if (!perJobTitleCompByCountry[jtRaw][countryKey]) perJobTitleCompByCountry[jtRaw][countryKey] = [];
          perJobTitleCompByCountry[jtRaw][countryKey].push(compNum);
        }
      }
    }
  }

  // ── Per-job-title verified compensation (country-grouped, verified records only) ──
  // Query the process table for records whose IDs are flagged in compensation_verified.json,
  // then group by job title and country so each Jobtitle entry can embed "Verified Compensation"
  // directly below its regular "Compensation" block.
  const perJobTitleVerifiedCompByCountry = {};  // { jobTitle: { country: [num, ...] } }
  try {
    const compVerifiedData = loadCompensationVerified();
    const verifiedIds = Object.keys(compVerifiedData).filter(id => compVerifiedData[id] && compVerifiedData[id].verified);
    if (verifiedIds.length > 0) {
      const vcNums = verifiedIds.map(id => parseInt(id, 10)).filter(n => !isNaN(n));
      if (vcNums.length > 0) {
        const vcResult = await pool.query(
          `SELECT id, compensation, country, jobtitle, role_tag FROM "process" WHERE userid = $1 AND id = ANY($2::int[])`,
          [userid, vcNums]
        );
        for (const r of vcResult.rows) {
          if (!r.compensation) continue;
          const numMatch = String(r.compensation).replace(/[,\s]/g, '').match(/[\d]+(?:\.\d+)?/);
          if (!numMatch) continue;
          const compNum = parseFloat(numMatch[0]);
          const jtRaw = (r.jobtitle || r.role_tag || '').trim();
          if (!jtRaw) continue;
          const countryKey = (r.country || '').trim() || COMP_NO_COUNTRY;
          if (!perJobTitleVerifiedCompByCountry[jtRaw]) perJobTitleVerifiedCompByCountry[jtRaw] = {};
          if (!perJobTitleVerifiedCompByCountry[jtRaw][countryKey]) perJobTitleVerifiedCompByCountry[jtRaw][countryKey] = [];
          perJobTitleVerifiedCompByCountry[jtRaw][countryKey].push(compNum);
        }
      }
    }
  } catch (vcErr) {
    console.warn('[_buildMLProfileData] Could not build per-title verified compensation (non-fatal):', vcErr.message);
  }

  // Build sector: sector-first format with record-count based confidence.
  // confidence(company, sector) = count(company in sector) / count(company in all sectors).
  // Companies that only appear in one sector get confidence 1.0.
  const sectorFirst = {};
  for (const [companyName, sectorCounts] of Object.entries(perCompanySectorCounts)) {
    const totalCount = Object.values(sectorCounts).reduce((s, c) => s + c, 0);
    if (totalCount === 0) continue;
    for (const [sectorName, count] of Object.entries(sectorCounts)) {
      const confidence = Math.round((count / totalCount) * 1000) / 1000;
      if (!sectorFirst[sectorName]) sectorFirst[sectorName] = {};
      sectorFirst[sectorName][companyName] = confidence;
    }
  }
  const sector = sectorFirst;
  const sourcing_status_distribution = _buildDistribution(statusVals);

  // Top 10 skills by frequency — stored as { skill: confidence } where confidence = count / totalCandidates
  const skillFreq = {};
  for (const token of allSkillTokens) {
    if (token) skillFreq[token] = (skillFreq[token] || 0) + 1;
  }

  // Build per-job-title profiles (used internally for ML_Holding.json sections format)
  const jobTitleCounts = {};
  for (const jt of jobtitles) { if (jt) jobTitleCounts[jt] = (jobTitleCounts[jt] || 0) + 1; }
  const jobTitleProfiles = {};
  for (const [jt, jtCount] of Object.entries(jobTitleCounts)) {
    const profile = {};
    const jtJfDist = _buildDistribution(perJobTitleJobFamily[jt] || []);
    if (jtJfDist) profile.job_family = jtJfDist;
    const jtSenDist = _buildDistribution(perJobTitleSeniority[jt] || []);
    if (jtSenDist) profile.Seniority = jtSenDist;
    const jtSkillFreq = {};
    for (const token of (perJobTitleSkillTokens[jt] || [])) {
      if (token) jtSkillFreq[token] = (jtSkillFreq[token] || 0) + 1;
    }
    const jtSkillEntries = Object.entries(jtSkillFreq).sort((a, b) => b[1] - a[1]).slice(0, 10);
    if (jtSkillEntries.length > 0) {
      profile.top_10_skills = Object.fromEntries(
        jtSkillEntries.map(([skill, c]) => [
          skill,
          Math.round(Math.min(1, c / Math.max(1, jtCount)) * 1000) / 1000,
        ])
      );
    }
    jobTitleProfiles[jt] = profile;
  }

  // ── Must_Have_Skills and Unique_Skills: pre-compute per-title skill sets ──
  // titleSkillSets is used for both the per-family intersection AND global unique-delta computation.
  const titleList = Object.keys(perJobTitleSkillTokens);
  const titleSkillSets = {};
  for (const jt of titleList) {
    titleSkillSets[jt] = new Set((perJobTitleSkillTokens[jt] || []).map(s => s.toLowerCase()));
  }

  // ── Unique_Skills: top 10 skills unique to each job title (global — across ALL titles) ──
  // Skills that do NOT appear in any other job title's candidate pool.
  const uniqueDeltaPerTitle = {};
  for (const jt of titleList) {
    const otherSkills = new Set();
    for (const otherJt of titleList) {
      if (otherJt !== jt) {
        for (const s of (titleSkillSets[otherJt] || [])) otherSkills.add(s);
      }
    }
    const uniqueFreq = {};
    for (const token of (perJobTitleSkillTokens[jt] || [])) {
      if (token && !otherSkills.has(token.toLowerCase())) {
        uniqueFreq[token] = (uniqueFreq[token] || 0) + 1;
      }
    }
    uniqueDeltaPerTitle[jt] = Object.entries(uniqueFreq)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([skill]) => skill);
  }

  // ── Total_Experience: min/max range per job title ──
  const totalExpPerTitle = {};
  for (const [jt, expValues] of Object.entries(perJobTitleExp)) {
    if (expValues.length === 0) continue;
    const numericValues = expValues.map(v => parseFloat(v)).filter(n => !isNaN(n));
    if (numericValues.length > 0) {
      totalExpPerTitle[jt] = {
        min: Math.min(...numericValues),
        max: Math.max(...numericValues),
      };
    }
  }

  // Inject Total_Experience into jobTitleProfiles so that ML_Holding.json
  // carries this value through to ML_Master_Jobfamily_Seniority.json on integration.
  for (const [jt, expVal] of Object.entries(totalExpPerTitle)) {
    if (jobTitleProfiles[jt]) jobTitleProfiles[jt].Total_Experience = expVal;
  }

  // ── Group job titles by job family (case-insensitive consolidation) ──
  // A title is placed into EVERY family it belongs to (all entries in its job_family distribution),
  // not just the dominant one. This ensures cross-family assignments (e.g. a title that appears
  // under two different families in different data sources) are preserved after integration.
  // Keys are normalized to lowercase so "Clinical Operations" and "clinical operations" map to
  // the same family block. The canonical display name is the most frequently occurring raw form.
  // titleRCs: per-family effective record count for each title = round(totalRC * proportion).
  const titlesByFamily = {};  // { normalizedKey: { display: string, titles: [jt, ...], titleRCs: { jt: rc } } }
  for (const [jt] of Object.entries(jobTitleCounts)) {
    const jtJfDist = _buildDistribution(perJobTitleJobFamily[jt] || []);
    const totalRC = jobTitleCounts[jt] || 1;
    if (jtJfDist && Object.keys(jtJfDist).length > 0) {
      for (const [familyRaw, proportion] of Object.entries(jtJfDist)) {
        if (Number(proportion) <= 0) continue;
        const normalizedKey = familyRaw.trim().toLowerCase();
        const displayName   = familyRaw.trim() || 'Unknown';
        const familyRC      = Math.max(1, Math.round(totalRC * Number(proportion)));
        if (!titlesByFamily[normalizedKey]) {
          titlesByFamily[normalizedKey] = { display: displayName, titles: [], titleRCs: {} };
        }
        if (!titlesByFamily[normalizedKey].titles.includes(jt)) {
          titlesByFamily[normalizedKey].titles.push(jt);
        }
        titlesByFamily[normalizedKey].titleRCs[jt] = (titlesByFamily[normalizedKey].titleRCs[jt] || 0) + familyRC;
      }
    } else {
      if (!titlesByFamily['unknown']) {
        titlesByFamily['unknown'] = { display: 'Unknown', titles: [], titleRCs: {} };
      }
      if (!titlesByFamily['unknown'].titles.includes(jt)) {
        titlesByFamily['unknown'].titles.push(jt);
      }
      titlesByFamily['unknown'].titleRCs[jt] = totalRC;
    }
  }

  const ua = useraccess || null;

  // Compute total record count per title across ALL families.
  // Confidence = rc_in_this_family / total_rc_for_title — so titles exclusive to one family
  // always get confidence 1.0; confidence is only reduced by cross-family assignments.
  const totalRCPerTitle = {};
  for (const { titleRCs } of Object.values(titlesByFamily)) {
    for (const [jt, rc] of Object.entries(titleRCs)) {
      totalRCPerTitle[jt] = (totalRCPerTitle[jt] || 0) + rc;
    }
  }

  // ── Job_Families array: one block per distinct job family ──
  // Each block is fully self-contained: Family_Core_DNA (skills shared within this family's titles),
  // Jobtitle (per-title unique skills, experience, confidence), Seniority (reverse map within family).
  const jobFamiliesArray = [];
  for (const [, { display: familyName, titles: familyTitles, titleRCs: familyTitleRCs }] of Object.entries(titlesByFamily)) {
    // Family_Core_DNA.Must_Have_Skills: top 10 skills shared across all job titles in this family.
    // If fewer than 10, supplement with top family-level skills (by frequency within this family).
    const familyTitleSkillSets = {};
    for (const jt of familyTitles) {
      familyTitleSkillSets[jt] = new Set((perJobTitleSkillTokens[jt] || []).map(s => s.toLowerCase()));
    }
    let familyMustHave = [];
    if (familyTitles.length > 0) {
      let intersection = new Set(familyTitleSkillSets[familyTitles[0]]);
      for (let i = 1; i < familyTitles.length; i++) {
        intersection = new Set([...intersection].filter(s => familyTitleSkillSets[familyTitles[i]].has(s)));
      }
      // Sort intersection by global frequency; preserve original casing
      const sharedFreq = {};
      for (const [token, cnt] of Object.entries(skillFreq)) {
        if (intersection.has(token.toLowerCase())) {
          const lc = token.toLowerCase();
          if (!sharedFreq[lc] || cnt > sharedFreq[lc].cnt) sharedFreq[lc] = { token, cnt };
        }
      }
      familyMustHave = Object.values(sharedFreq)
        .sort((a, b) => b.cnt - a.cnt)
        .slice(0, 10)
        .map(({ token }) => token);
      // Supplement with most frequent skills in this family if fewer than 10 shared
      if (familyMustHave.length < 10) {
        const familySkillFreq = {};
        for (const jt of familyTitles) {
          for (const token of (perJobTitleSkillTokens[jt] || [])) {
            if (token) familySkillFreq[token] = (familySkillFreq[token] || 0) + 1;
          }
        }
        const existing = new Set(familyMustHave.map(s => s.toLowerCase()));
        const topFamilySkills = Object.entries(familySkillFreq).sort((a, b) => b[1] - a[1]);
        for (const [token] of topFamilySkills) {
          if (familyMustHave.length >= 10) break;
          if (!existing.has(token.toLowerCase())) {
            familyMustHave.push(token);
            existing.add(token.toLowerCase());
          }
        }
      }
    }

    // Family Confidence_Threshold: max combined (title_conf × max_seniority_proportion) in this family
    // - title_conf = rc_in_family / total_rc_for_title (only cross-family assignments reduce it)
    // - max_seniority_proportion = highest proportion among seniority levels for the title
    //   (e.g. Manager:0.5/Mid:0.5 gives 0.5, reducing threshold vs a single dominant level at 1.0)
    const familyConfidenceThreshold = familyTitles.length > 0
      ? Math.round(Math.max(...familyTitles.map(jt => {
          const rc = familyTitleRCs[jt] || 1;
          const totalRC = totalRCPerTitle[jt] || rc;
          const titleConf = rc / totalRC;
          const senDist = _buildDistribution(perJobTitleSeniority[jt] || []);
          const senVals = senDist ? Object.values(senDist) : [];
          const maxSenProp = senVals.length > 0 ? Math.max(...senVals) : 1;
          return titleConf * maxSenProp;
        })) * 1000) / 1000
      : 0;

    // Jobtitle section: job titles belonging to this family
    // Confidence = rc_in_this_family / total_rc_for_title across ALL families
    // (multiple titles in the same family do NOT reduce each other's confidence;
    //  confidence is only reduced when the title appears in more than one family)
    const familyJobtitle = {};
    for (const jt of familyTitles) {
      const jtSenDist = _buildDistribution(perJobTitleSeniority[jt] || []);
      const rc = familyTitleRCs[jt] || 1;
      const totalRC = totalRCPerTitle[jt] || rc;
      const titleConf = Math.round((rc / totalRC) * 1000) / 1000;
      const entry = {
        Record_Count_Jobtitle: rc,
        Seniority: jtSenDist || {},
        Unique_Skills: uniqueDeltaPerTitle[jt] || [],
        Confidence: titleConf,
      };
      if (totalExpPerTitle[jt]) entry.Total_Experience = totalExpPerTitle[jt];
      // Embed Compensation as an array of per-country objects inside the Jobtitle entry.
      // Each entry covers one country so multiple countries coexist without overwriting.
      const compByCountry = perJobTitleCompByCountry[jt];
      if (compByCountry && Object.keys(compByCountry).length > 0) {
        const compensationArray = [];
        for (const [countryKey, nums] of Object.entries(compByCountry)) {
          const countryVal = countryKey === COMP_NO_COUNTRY ? null : countryKey;
          const compEntry = {
            ...(countryVal ? { country: countryVal } : {}),
            min: String(Math.min(...nums)),
            max: String(Math.max(...nums)),
            count: nums.length,
            _users: [username],
            last_updated: today,
          };
          compensationArray.push(compEntry);
        }
        entry.Compensation = compensationArray;
      }
      // Embed "Verified Compensation" directly below "Compensation" for records tagged as verified.
      // Grouped per country — same shape as Compensation — so verified data stays tied to this title.
      const verifiedCompByCountry = perJobTitleVerifiedCompByCountry[jt];
      if (verifiedCompByCountry && Object.keys(verifiedCompByCountry).length > 0) {
        const verifiedCompensationArray = [];
        for (const [countryKey, nums] of Object.entries(verifiedCompByCountry)) {
          const countryVal = countryKey === COMP_NO_COUNTRY ? null : countryKey;
          verifiedCompensationArray.push({
            ...(countryVal ? { country: countryVal } : {}),
            min: String(Math.min(...nums)),
            max: String(Math.max(...nums)),
            count: nums.length,
            _users: [username],
            last_updated: today,
          });
        }
        if (verifiedCompensationArray.length > 0) entry['Verified Compensation'] = verifiedCompensationArray;
      }
      familyJobtitle[jt] = entry;
    }

    jobFamiliesArray.push({
      Job_Family: familyName,
      last_updated: today,
      username,
      useraccess: ua,
      Family_Core_DNA: {
        Must_Have_Skills: familyMustHave,
        Confidence_Threshold: familyConfidenceThreshold,
      },
      Jobtitle: familyJobtitle,
    });
  }

  // Company section: sector-first with confidence splitting (confidence = 1/n per sector per company)
  const companySection = { last_updated: today, username, useraccess: ua, sector };

  // Compensation section: per-job-title breakdown with Compensation and Verified Compensation arrays.
  // Each job title entry holds a Compensation array (one element per country) and, when available,
  // a "Verified Compensation" array sourced from perJobTitleVerifiedCompByCountry.
  const compensationByJobTitle = {};
  for (const [jt, byCountry] of Object.entries(perJobTitleCompByCountry)) {
    if (!byCountry || Object.keys(byCountry).length === 0) continue;
    // Dominant job family for this title (most frequent value in perJobTitleJobFamily)
    const jfArr = perJobTitleJobFamily[jt] || [];
    let jfDominant;
    if (jfArr.length > 0) {
      const freq = {};
      for (const jf of jfArr) { if (jf) freq[jf] = (freq[jf] || 0) + 1; }
      const sorted = Object.entries(freq).sort((a, b) => b[1] - a[1]);
      if (sorted.length > 0) jfDominant = sorted[0][0];
    }
    // Build per-country Compensation array
    const compensationArray = [];
    for (const [countryKey, nums] of Object.entries(byCountry)) {
      if (!nums || nums.length === 0) continue;
      compensationArray.push({
        ...(countryKey !== COMP_NO_COUNTRY ? { country: countryKey } : {}),
        min: String(Math.min(...nums)),
        max: String(Math.max(...nums)),
        count: nums.length,
        _users: [username],
        last_updated: today,
      });
    }
    const jtEntry = {};
    if (jfDominant) jtEntry.job_family = jfDominant;
    if (compensationArray.length > 0) jtEntry.Compensation = compensationArray;
    // Embed Verified Compensation for this job title if available
    const vcByCountry = perJobTitleVerifiedCompByCountry[jt];
    if (vcByCountry && Object.keys(vcByCountry).length > 0) {
      const vcArray = [];
      for (const [ck, nums] of Object.entries(vcByCountry)) {
        if (!nums || nums.length === 0) continue;
        vcArray.push({
          ...(ck !== COMP_NO_COUNTRY ? { country: ck } : {}),
          min: String(Math.min(...nums)),
          max: String(Math.max(...nums)),
          count: nums.length,
          _users: [username],
          last_updated: today,
        });
      }
      if (vcArray.length > 0) jtEntry['Verified Compensation'] = vcArray;
    }
    if (jtEntry.Compensation || jtEntry['Verified Compensation']) compensationByJobTitle[jt] = jtEntry;
  }
  const compensationSection = { last_updated: today, username, useraccess: ua, compensation_by_job_title: compensationByJobTitle };

  // Build the new grouped format for ML_{username}.json (DB Dock Out format).
  // Compensation is embedded inside Jobtitle entries; no top-level compensation key.
  const data = {
    Job_Families: jobFamiliesArray,
    company: companySection,
  };

  // _sections is used internally by the ml-summary endpoint to write the old-style
  // job_title section to ML_Holding.json (for algorithmic consolidation compatibility).
  // It is stripped before the data is sent as a response or written to a file.
  data._sections = {
    job_title: { last_updated: today, username, useraccess: ua, job_title: jobTitleProfiles },
    company: companySection,
    compensation: compensationSection,
  };

  return data;
}

// _persistMLUserFile — write data to ML_{username}.json (does NOT merge into master).
// Called after bulk candidate saves so confidence scores stay current between Dock Out cycles.
async function _persistMLUserFile(username, data) {
  try {
    fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });
    const safeUsername = String(username).replace(/[^a-zA-Z0-9_-]/g, '_');
    const mlFilepath = path.join(ML_OUTPUT_DIR, `ML_${safeUsername}.json`);
    // Strip internal _sections property before writing to file
    const { _sections: _ignored, ...fileData } = data;
    fs.writeFileSync(mlFilepath, JSON.stringify(fileData, null, 2), 'utf8');
    // Invalidate the in-memory mtime cache so the next read picks up the new file
    _mlProfileCache.delete(safeUsername);
    console.info(`[ml-profile] Updated ${mlFilepath}`);
  } catch (writeErr) {
    console.warn('[ml-profile] Could not write ML user file (non-fatal):', writeErr.message);
  }
}

// POST /candidates/ml-summary — ML analytics summary of the current candidate pool.
// All distributions are computed deterministically server-side from DB data.
// Gemini is used only to derive a clean normalised "role" label from the jobtitles.
app.post('/candidates/ml-summary', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const userid   = String(req.user.id);
    const username = req.user.username;

    // Determine this user's access level and candidate count in a single query to reduce DB round trips.
    // useraccess is always sourced directly from login.useraccess in Postgres — never defaulted to a
    // hardcoded string so that only legitimate values stored in the DB are reflected in ML files.
    let useraccess = null;
    let candidateCount = 0;
    try {
      const statsRes = await pool.query(
        `SELECT l.useraccess, COUNT(p.id) AS cnt
         FROM login l
         LEFT JOIN process p ON p.userid = l.id::text
         WHERE l.id = $1
         GROUP BY l.useraccess`,
        [userid]
      );
      if (statsRes.rows.length > 0) {
        // Always use the actual DB value (may be null if the column is unset)
        useraccess = statsRes.rows[0].useraccess
          ? String(statsRes.rows[0].useraccess).toLowerCase()
          : null;
        candidateCount = Number(statsRes.rows[0].cnt || 0);
      }
    } catch (_) {
      // Non-fatal: fall back to separate queries
      try {
        // Query by username (VARCHAR) rather than id (INTEGER) to avoid implicit type-cast issues.
        const uaRes = await pool.query('SELECT useraccess FROM login WHERE username = $1 LIMIT 1', [username]);
        if (uaRes.rows.length > 0) {
          useraccess = uaRes.rows[0].useraccess
            ? String(uaRes.rows[0].useraccess).toLowerCase()
            : null;
        }
      } catch (_2) { /* ignore */ }
      try {
        const cntRes = await pool.query('SELECT COUNT(*) AS cnt FROM "process" WHERE userid = $1', [userid]);
        candidateCount = Number(cntRes.rows[0]?.cnt || 0);
      } catch (_3) { /* ignore */ }
    }

    // Final safety net: if useraccess is still null after all above queries, query by username
    // directly — the most reliable path since username is always a VARCHAR and never ambiguous.
    if (!useraccess) {
      try {
        const uaFinal = await pool.query('SELECT useraccess FROM login WHERE username = $1 LIMIT 1', [username]);
        if (uaFinal.rows.length > 0 && uaFinal.rows[0].useraccess) {
          useraccess = String(uaFinal.rows[0].useraccess).toLowerCase();
        }
      } catch (_f) { /* ignore */ }
    }

    // Build the full ML profile using the shared helper (useraccess embedded in all sections)
    // _buildMLProfileData will also do a username-based self-fetch if useraccess remains null.
    const data = await _buildMLProfileData(userid, username, useraccess);

    // Load ML transfer thresholds from rate_limits.json
    const rlConfig = loadRateLimits();
    const mlConfig = (rlConfig && rlConfig.ml) || {};
    const confidenceLevels = mlConfig.confidence_level || {};
    const userConsenses    = mlConfig.user_consenses    || {};

    // Case-insensitive lookup: access level keys in rate_limits.json may use a different
    // case than the useraccess string stored in the DB (e.g. "BYOK" vs "byok").
    const mlCiLookup = (map, key) => {
      if (!key) return undefined;
      if (map[key] !== undefined) return map[key];
      const lk = key.toLowerCase();
      const match = Object.keys(map).find(k => k.toLowerCase() === lk);
      return match !== undefined ? map[match] : undefined;
    };

    // Admin is always hard-coded to confidence=0.5 and consenses=1 so that a single
    // admin with a 0.5+ top-score is sufficient to transfer into the master files.
    const isAdmin = String(useraccess || '').toLowerCase() === 'admin';
    const confidenceThreshold = isAdmin ? 0.5 : Number(
      mlCiLookup(confidenceLevels, useraccess) ?? 0.7
    );
    const consensesRequired = isAdmin ? 1 : Number(
      mlCiLookup(userConsenses, useraccess) ?? 3
    );

    // Evaluate confidence score for this user's ML data.
    // New format: Job_Families array — each family block has a Jobtitle dict with per-title Confidence.
    // topSeniorityScore = average of all per-title Confidence values across all families.
    const titleConfValues = [];
    if (Array.isArray(data.Job_Families)) {
      for (const familyBlock of data.Job_Families) {
        if (familyBlock && familyBlock.Jobtitle && typeof familyBlock.Jobtitle === 'object') {
          for (const titleData of Object.values(familyBlock.Jobtitle)) {
            const conf = Number((titleData && titleData.Confidence) || 0);
            if (conf > 0) titleConfValues.push(conf);
          }
        }
      }
    }
    const topSeniorityScore = titleConfValues.length > 0
      ? titleConfValues.reduce((sum, v) => sum + v, 0) / titleConfValues.length
      : 0;

    // AverageConfidenceThreshold: average of per-family Confidence_Threshold values.
    // Represents the overall qualified confidence level across all job families for this user.
    const familyConfThresholds = [];
    if (Array.isArray(data.Job_Families)) {
      for (const familyBlock of data.Job_Families) {
        const ct = familyBlock && familyBlock.Family_Core_DNA && familyBlock.Family_Core_DNA.Confidence_Threshold;
        if (typeof ct === 'number' && !isNaN(ct)) familyConfThresholds.push(ct);
      }
    }
    const averageConfidenceThreshold = familyConfThresholds.length > 0
      ? Math.round((familyConfThresholds.reduce((sum, v) => sum + v, 0) / familyConfThresholds.length) * 1000) / 1000
      : confidenceThreshold;

    // All users are written to ML_Holding.json first.
    // Promotion to the Master ML files only happens when "Integrate All Users into Master Files"
    // is run by an admin (POST /admin/ml-integrate), which checks confidence level and user
    // consensus thresholds for each access level before promoting entries.
    const transferApproved = false;
    const addedToHolding = true;

    // Write the holding entry so the admin integrate step can promote it later.
    // ML_<username>.json is removed from the output folder on Dock Out — the data lives in
    // ML_Holding.json. The individual file is only recreated on Dock In (via ml-restore).
    try {
      fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });
      const safeUsername = String(username).replace(/[^a-zA-Z0-9_-]/g, '_');
      const mlFilepath = path.join(ML_OUTPUT_DIR, `ML_${safeUsername}.json`);

      // Use the internal _sections (old-style job_title section) for ML_Holding.json
      // so that algorithmicConsolidate() can still process it for master file integration.
      const holdingSections = data._sections || {
        job_title: { last_updated: new Date().toISOString().split('T')[0], username: String(username), useraccess },
        company: data.company,
        compensation: { last_updated: new Date().toISOString().split('T')[0], username: String(username), useraccess, compensation_by_job_title: {} },
      };

      // Build Verified Compensation block: query verified candidates for this user and group by country.
      let verifiedCompensationBlock = null;
      try {
        const compVerifiedData = loadCompensationVerified();
        const verifiedIds = Object.keys(compVerifiedData).filter(id => compVerifiedData[id] && compVerifiedData[id].verified);
        if (verifiedIds.length > 0) {
          const vcNums = verifiedIds.map(id => parseInt(id, 10)).filter(n => !isNaN(n));
          if (vcNums.length > 0) {
            const vcResult = await pool.query(
              `SELECT id, compensation, country FROM "process" WHERE userid = $1 AND id = ANY($2::int[])`,
              [userid, vcNums]
            );
            if (vcResult.rows.length > 0) {
              const today = new Date().toISOString().split('T')[0];
              const byCountry = {};
              const COMP_NO_COUNTRY_VC = '__no_country__';
              for (const r of vcResult.rows) {
                if (!r.compensation) continue;
                const numMatch = String(r.compensation).replace(/[,\s]/g, '').match(/[\d]+(?:\.\d+)?/);
                if (!numMatch) continue;
                const compNum = parseFloat(numMatch[0]);
                const countryKey = (r.country || '').trim() || COMP_NO_COUNTRY_VC;
                if (!byCountry[countryKey]) byCountry[countryKey] = [];
                byCountry[countryKey].push(compNum);
              }
              const vcArray = Object.entries(byCountry)
                .filter(([, nums]) => nums.length > 0)
                .map(([country, nums]) => ({
                  ...(country !== COMP_NO_COUNTRY_VC ? { country } : {}),
                  min: String(Math.min(...nums)),
                  max: String(Math.max(...nums)),
                  count: nums.length,
                  _users: [username],
                  last_updated: today,
                }));
              if (vcArray.length > 0) verifiedCompensationBlock = vcArray;
            }
          }
        }
      } catch (vcErr) {
        console.warn('[ml-summary] Could not build verified compensation (non-fatal):', vcErr.message);
      }

      // Always write to ML_Holding.json (all users, all access levels)
      const holdingFp = path.join(ML_OUTPUT_DIR, 'ML_Holding.json');
      let holding = {};
      if (fs.existsSync(holdingFp)) {
        try { holding = JSON.parse(fs.readFileSync(holdingFp, 'utf8')); } catch (_) { holding = {}; }
      }
      const holdingUserSections = { company: holdingSections.company, job_title: holdingSections.job_title, compensation: holdingSections.compensation };
      // Verified Compensation is now embedded per job title in the compensation section (no separate block).
      holding[String(username)] = {
        last_updated: new Date().toISOString().split('T')[0],
        username: String(username),
        useraccess,
        confidence_score: Math.round(topSeniorityScore * 100) / 100,
        sections: holdingUserSections,
      };
      fs.writeFileSync(holdingFp, JSON.stringify(holding, null, 2), 'utf8');
      console.info(`[ml-summary] Written to ML_Holding.json for ${username} (confidence=${Number(topSeniorityScore).toFixed(2)}, access=${useraccess || 'null'}) — awaiting admin integration`);

      // Remove the individual ML_<username>.json from the output folder on Dock Out.
      // The file is recreated on Dock In via POST /candidates/ml-restore.
      if (fs.existsSync(mlFilepath)) {
        fs.unlinkSync(mlFilepath);
        console.info(`[ml-summary] Removed ${mlFilepath} from output folder on Dock Out`);
      }
    } catch (writeErr) {
      console.warn('[ml-summary] Could not write ML_ Holding file (non-fatal):', writeErr.message);
    }
    // Strip internal _sections before sending the response; spread flags on top of the new flat format.
    const { _sections: _stripped, ...responseData } = data;
    res.json({ ...responseData, transferApproved, addedToHolding, confidenceThreshold, AverageConfidenceThreshold: averageConfidenceThreshold, consensesRequired, candidateCount, topSeniorityScore: Math.round(topSeniorityScore * 100) / 100 });
  } catch (err) {
    console.error('/candidates/ml-summary error:', err);
    res.status(500).json({ error: 'ML summary generation failed.' });
  }
});

// ── ML profile per-username mtime cache ──────────────────────────────────────
// Keyed by safeUsername. Stores { data, mtime } so we only re-read the file
// when it has actually changed on disk (same pattern as _userSvcCfgCache).
const _mlProfileCache = new Map();

// GET /candidates/ml-profile — read the user's ML profile from ML_{username}.json only.
// The file is recreated on every Dock In from the embedded ML worksheet in the XLS.
// Returns the stored ML analytics profile so Sync Entries can apply its highest-confidence values.
app.get('/candidates/ml-profile', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const username = String(req.user.username);
    const safeUsername = username.replace(/[^a-zA-Z0-9_-]/g, '_');

    // Only read from the user-specific ML file — never from the master split files.
    // The master files (ML_Master_Company.json, ML_Master_Jobfamily_Seniority.json,
    // ML_Master_Compensation.json) are audit/backup stores only; Sync Entries must
    // operate on the individual file explicitly recreated for this user via Dock In.
    const mlFilepath = path.join(ML_OUTPUT_DIR, `ML_${safeUsername}.json`);
    try {
      // statSync gives existence check + mtime in one syscall
      const { mtimeMs } = fs.statSync(mlFilepath);
      const cached = _mlProfileCache.get(safeUsername);
      let data;
      if (cached && cached.mtime === mtimeMs) {
        data = cached.data;
      } else {
        data = JSON.parse(fs.readFileSync(mlFilepath, 'utf8'));
        _mlProfileCache.set(safeUsername, { data, mtime: mtimeMs });
      }
      return res.json({ found: true, profile: data });
    } catch (statErr) {
      if (statErr.code !== 'ENOENT') console.warn('[ml-profile] stat error:', statErr.message);
      // Fall through to on-the-fly build
    }

    // File not found (e.g. after Dock Out before Dock In, or on first run).
    // Compute the profile on-the-fly from the DB so Sync Entries always has access
    // to ML data without requiring a manual Dock Out / Dock In cycle.
    try {
      const userid = String(req.user.id);
      const data = await _buildMLProfileData(userid, username);
      // Persist for subsequent requests (non-fatal if write fails)
      _persistMLUserFile(username, data).catch(() => {});
      // Strip internal _sections before returning
      const { _sections: _ignored, ...profileData } = data;
      return res.json({ found: true, profile: profileData });
    } catch (buildErr) {
      console.warn('[ml-profile] Could not compute ML profile on-the-fly:', buildErr.message);
    }

    return res.json({ found: false });
  } catch (err) {
    console.warn('[ml-profile] Could not read ML_ JSON file:', err.message);
    return res.json({ found: false });
  }
});

// POST /candidates/ml-restore — called during DB Dock In to recreate ML_{username}.json
// from the ML worksheet embedded in the imported XLS file.
app.post('/candidates/ml-restore', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const username = String(req.user.username);
    const safeUsername = username.replace(/[^a-zA-Z0-9_-]/g, '_');
    const { profile } = req.body;
    if (!profile || typeof profile !== 'object') {
      return res.status(400).json({ error: 'No valid ML profile provided.' });
    }
    fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });
    // Write (or overwrite) the user-specific ML JSON so Sync Entries can reference it
    const mlFilepath = path.join(ML_OUTPUT_DIR, `ML_${safeUsername}.json`);
    fs.writeFileSync(mlFilepath, JSON.stringify(profile, null, 2), 'utf8');
    // Invalidate the in-memory mtime cache so the next read picks up the restored file
    _mlProfileCache.delete(safeUsername);
    console.info(`[ml-restore] Recreated ${mlFilepath} from XLS ML worksheet`);
    res.json({ ok: true });
  } catch (err) {
    console.warn('[ml-restore] Could not restore ML_ JSON file (non-fatal):', err.message);
    res.status(500).json({ error: 'ML profile restore failed.' });
  }
});

// GET /admin/ml-master-files — return contents of all three ML master files (admin only)
app.get('/admin/ml-master-files', dashboardRateLimit, requireAdmin, (req, res) => {
  try {
    const files = {
      company:      'ML_Master_Company.json',
      job_title:    'ML_Master_Jobfamily_Seniority.json',
      compensation: 'ML_Master_Compensation.json',
    };
    const result = {};
    for (const [key, filename] of Object.entries(files)) {
      const fp = path.join(ML_OUTPUT_DIR, filename);
      if (fs.existsSync(fp)) {
        try { result[key] = JSON.parse(fs.readFileSync(fp, 'utf8')); } catch (_) { result[key] = {}; }
      } else {
        result[key] = {};
      }
    }
    res.json(result);
  } catch (err) {
    console.error('[admin/ml-master-files] Error:', err.message);
    res.status(500).json({ error: 'Failed to read ML master files.' });
  }
});

// PUT /admin/ml-master-files/:section — save updated ML master file contents (admin only)
// :section must be one of: company, job_title, compensation
app.put('/admin/ml-master-files/:section', dashboardRateLimit, requireAdmin, (req, res) => {
  const sectionMap = {
    company:      'ML_Master_Company.json',
    job_title:    'ML_Master_Jobfamily_Seniority.json',
    compensation: 'ML_Master_Compensation.json',
  };
  const filename = sectionMap[req.params.section];
  if (!filename) return res.status(400).json({ error: 'Invalid section. Must be company, job_title, or compensation.' });
  try {
    const data = req.body;
    if (!data || typeof data !== 'object') return res.status(400).json({ error: 'Invalid body.' });
    fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });
    const fp = path.join(ML_OUTPUT_DIR, filename);
    fs.writeFileSync(fp, JSON.stringify(data, null, 2), 'utf8');
    console.info(`[admin/ml-master-files] Saved ${fp}`);
    res.json({ ok: true });
  } catch (err) {
    console.error('[admin/ml-master-files] Save error:', err.message);
    res.status(500).json({ error: 'Failed to save ML master file.' });
  }
});

// DELETE /admin/ml-master-files/:section/user/:username — remove a single user's entry
// from the specified ML master file (admin only).
app.delete('/admin/ml-master-files/:section/user/:username', dashboardRateLimit, requireAdmin, (req, res) => {
  const sectionMap = {
    company:      'ML_Master_Company.json',
    job_title:    'ML_Master_Jobfamily_Seniority.json',
    compensation: 'ML_Master_Compensation.json',
  };
  const filename = sectionMap[req.params.section];
  if (!filename) return res.status(400).json({ error: 'Invalid section. Must be company, job_title, or compensation.' });
  const username = req.params.username;
  if (!username) return res.status(400).json({ error: 'Username is required.' });
  try {
    const fp = path.join(ML_OUTPUT_DIR, filename);
    let data = {};
    if (fs.existsSync(fp)) {
      try { data = JSON.parse(fs.readFileSync(fp, 'utf8')); } catch (parseErr) {
        console.warn(`[admin/ml-master-files] Could not parse ${fp}: ${parseErr.message}`);
        data = {};
      }
    }
    if (!(username in data)) return res.status(404).json({ error: `User "${username}" not found in ${filename}.` });
    delete data[username];
    fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });
    fs.writeFileSync(fp, JSON.stringify(data, null, 2), 'utf8');
    console.info(`[admin/ml-master-files] Deleted user "${username}" from ${fp}`);
    res.json({ ok: true, message: `User "${username}" removed from ${filename}.` });
  } catch (err) {
    console.error('[admin/ml-master-files] Delete user error:', err.message);
    res.status(500).json({ error: 'Failed to delete user from ML master file.' });
  }
});

// GET /admin/ml-holding — return contents of ML_Holding.json (admin only)
app.get('/admin/ml-holding', dashboardRateLimit, requireAdmin, (req, res) => {
  try {
    const fp = path.join(ML_OUTPUT_DIR, 'ML_Holding.json');
    const data = fs.existsSync(fp) ? (() => { try { return JSON.parse(fs.readFileSync(fp, 'utf8')); } catch (_) { return {}; } })() : {};
    res.json({ ok: true, holding: data });
  } catch (err) {
    console.error('[admin/ml-holding] Error:', err.message);
    res.status(500).json({ error: 'Failed to read ML_Holding.json.' });
  }
});

// DELETE /admin/ml-holding/user/:username — remove a single user's entry from ML_Holding.json (admin only)
app.delete('/admin/ml-holding/user/:username', dashboardRateLimit, requireAdmin, (req, res) => {
  try {
    const { username } = req.params;
    const fp = path.join(ML_OUTPUT_DIR, 'ML_Holding.json');
    let holding = {};
    if (fs.existsSync(fp)) {
      try { holding = JSON.parse(fs.readFileSync(fp, 'utf8')); } catch (_) { holding = {}; }
    }
    if (!(username in holding)) return res.status(404).json({ error: `User "${username}" not found in ML_Holding.json.` });
    delete holding[username];
    fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });
    fs.writeFileSync(fp, JSON.stringify(holding, null, 2), 'utf8');
    console.info(`[admin/ml-holding] Deleted user "${username}" from ML_Holding.json`);
    res.json({ ok: true });
  } catch (err) {
    console.error('[admin/ml-holding] Delete user error:', err.message);
    res.status(500).json({ error: 'Failed to delete user from ML_Holding.json.' });
  }
});


// them into the three master files using weighted confidence blending (admin only).
// For each section the merge:
//  - Combines all users' entries for the same company/job-title/etc.
//  - Blends numeric confidence values proportionally (equal weight per user).
//  - Preserves non-numeric metadata from the most-recently-updated entry.
app.post('/admin/ml-integrate', dashboardRateLimit, requireAdmin, async (req, res) => {
  try {
    fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });

    // Load current master files — these are username-keyed (each top-level key is a username)
    const masterPaths = {
      company:      path.join(ML_OUTPUT_DIR, 'ML_Master_Company.json'),
      job_title:    path.join(ML_OUTPUT_DIR, 'ML_Master_Jobfamily_Seniority.json'),
      compensation: path.join(ML_OUTPUT_DIR, 'ML_Master_Compensation.json'),
    };
    const masterFiles = {};
    for (const [key, fp] of Object.entries(masterPaths)) {
      masterFiles[key] = fs.existsSync(fp) ? (() => { try { return JSON.parse(fs.readFileSync(fp, 'utf8')); } catch (_) { return {}; } })() : {};
    }

    // ── Phase 1: Promote qualifying entries from ML_Holding.json to master files ──
    // All Dock Out operations write to ML_Holding.json. This phase checks each holding entry
    // against its access-level thresholds (confidence level + user consensus). Entries that
    // meet BOTH thresholds for their access level are promoted to the master files and removed
    // from holding. Entries that do not yet meet the thresholds remain in holding.
    const holdingFp = path.join(ML_OUTPUT_DIR, 'ML_Holding.json');
    let holdingData = {};
    if (fs.existsSync(holdingFp)) {
      try { holdingData = JSON.parse(fs.readFileSync(holdingFp, 'utf8')); } catch (_) { holdingData = {}; }
    }

    let promotedCount = 0;
    if (Object.keys(holdingData).length > 0) {
      const rlCfg = loadRateLimits();
      const mlCfg = (rlCfg && rlCfg.ml) || {};
      const confLevels = mlCfg.confidence_level || {};
      const consCounts  = mlCfg.user_consenses   || {};
      const mlCiLkp = (map, key) => {
        if (!key) return undefined;
        if (map[key] !== undefined) return map[key];
        const lk = key.toLowerCase();
        const match = Object.keys(map).find(k => k.toLowerCase() === lk);
        return match !== undefined ? map[match] : undefined;
      };

      // Group holding entries by access level and compute thresholds per group
      const groups = {};
      for (const [uname, entry] of Object.entries(holdingData)) {
        if (!entry || typeof entry !== 'object') continue;
        const ua = entry.useraccess != null ? String(entry.useraccess) : '__null__';
        if (!groups[ua]) {
          const isAdm = String(ua).toLowerCase() === 'admin';
          groups[ua] = {
            threshold:    isAdm ? 0.5 : Number(mlCiLkp(confLevels, ua === '__null__' ? null : ua) ?? 0.7),
            consensusReq: isAdm ? 1   : Number(mlCiLkp(consCounts,  ua === '__null__' ? null : ua) ?? 3),
            users: [],
          };
        }
        groups[ua].users.push({ uname, entry });
      }

      // For each group, promote qualifying users (meet confidence AND group has enough of them)
      const promoted = new Set();
      for (const [, group] of Object.entries(groups)) {
        const qualifying = group.users.filter(({ entry }) => Number(entry.confidence_score || 0) >= group.threshold);
        if (qualifying.length >= group.consensusReq) {
          for (const { uname, entry } of qualifying) {
            const sections = entry.sections || {};
            const sectionMap = [
              { field: 'company',      mfKey: 'company' },
              { field: 'job_title',    mfKey: 'job_title' },
              { field: 'compensation', mfKey: 'compensation' },
            ];
            for (const { field, mfKey } of sectionMap) {
              if (sections[field]) {
                masterFiles[mfKey][uname] = sections[field];
              }
            }
            promoted.add(uname);
            promotedCount++;
          }
          console.info(`[admin/ml-integrate] Promoted ${qualifying.length} user(s) from holding (access: ${qualifying[0] && qualifying[0].entry.useraccess}) to master files`);
        }
      }

      // Remove promoted entries from holding and persist
      if (promoted.size > 0) {
        for (const uname of promoted) delete holdingData[uname];
        fs.writeFileSync(holdingFp, JSON.stringify(holdingData, null, 2), 'utf8');
        console.info(`[admin/ml-integrate] Removed ${promoted.size} promoted user(s) from ML_Holding.json`);
      }
    }

    // Count contributing users across all master sections (after Phase 1 promotion)
    const allUsers = new Set();
    for (const [sectionName, section] of Object.entries(masterFiles)) {
      if (sectionName === 'job_title' && section && Array.isArray(section.Job_Families)) {
        // New Job_Families array format
        for (const familyBlock of section.Job_Families) {
          if (!familyBlock || typeof familyBlock !== 'object') continue;
          if (Array.isArray(familyBlock._users) && familyBlock._users.length > 0) {
            for (const u of familyBlock._users) allUsers.add(u);
          }
        }
      } else {
        for (const [k, entry] of Object.entries(section || {})) {
          if (!entry || typeof entry !== 'object') continue;
          if (entry.sector || entry.job_title || entry.compensation_by_job_title || entry.username) {
            if (Array.isArray(entry._users) && entry._users.length > 0) {
              for (const u of entry._users) allUsers.add(u);
            } else {
              allUsers.add(k);
            }
          }
        }
      }
    }
    const integrated = allUsers.size;
    if (integrated === 0 && promotedCount === 0) {
      return res.json({ ok: true, message: 'No qualifying entries found in ML_Holding.json or master files to consolidate.', integrated: 0, promoted: 0 });
    }

    // --- Algorithmic consolidation helpers ---
    function blendMaps(existing, incoming, existingWeight, newWeight) {
      const total = existingWeight + newWeight;
      const result = {};
      const keys = new Set([...Object.keys(existing), ...Object.keys(incoming)]);
      for (const k of keys) {
        const a = Number(existing[k] || 0);
        const b = Number(incoming[k] || 0);
        result[k] = Math.round(((a * existingWeight + b * newWeight) / total) * 1000) / 1000;
      }
      const sum = Object.values(result).reduce((s, v) => s + v, 0);
      if (sum > 0) for (const k of Object.keys(result)) result[k] = Math.round((result[k] / sum) * 1000) / 1000;
      return result;
    }

    function algorithmicConsolidate() {
      const today = new Date().toISOString().split('T')[0];
      const consolidated = { company: {}, job_title: {}, compensation: { compensation_by_job_title: {}, last_updated: today } };

      // Helper: resolve the actual contributing users and count from an entry.
      // Handles both fresh username-keyed entries (no _users/_userCount) and
      // previously-consolidated entries (which carry _users and _userCount from a prior integration).
      // This allows ongoing accumulation: after "Integrate All Users" runs, new Dock Out entries
      // are written alongside the consolidated record, and re-running integration re-consolidates
      // them all with correct weighting.
      function entryMeta(keyName, entry) {
        const users = Array.isArray(entry._users) && entry._users.length > 0 ? entry._users : [keyName];
        const count = typeof entry._userCount === 'number' && entry._userCount > 0 ? entry._userCount : users.length;
        return { users, count };
      }

      // ── Company: merge all user sector maps into one "company" record ──
      // Handles input formats:
      //   1. New (dock-out): sector: { sectorName: { companyName: confidence } } — sector-first objects
      //   2. Old dock-out: sector: { companyName: [sectorName, ...] } — company-first arrays
      //   3. Legacy: sector_distribution: { companyName: { sectorName: count } } — company-first objects
      // Output: sector: { sectorName: { companyName: confidence } } — sector-first,
      //         confidence = count(company, sector) / count(company, all sectors) — record-count based.
      const companyRecord = { sector: {}, _users: [], _userCount: 0, last_updated: today };
      // Use a Set for O(1) dedup during accumulation; convert to array at output.
      const companyUsersSet = new Set();
      // Accumulate weighted record counts per (company, sector) across all user entries.
      // For each entry: userCount × confidence_in_sector gives the number of records in that bucket.
      const sectorCounts = {};  // { companyName: { sectorName: totalWeightedCount } }
      for (const [keyName, entry] of Object.entries(masterFiles.company)) {
        if (!entry || typeof entry !== 'object') continue;
        const sectorMap = entry.sector || null;
        const sectorDist = entry.sector_distribution || null;
        if (!sectorMap && !sectorDist) continue;
        const { users, count } = entryMeta(keyName, entry);
        companyRecord._userCount += count;
        for (const u of users) companyUsersSet.add(u);
        if (sectorMap && typeof sectorMap === 'object') {
          const sectorMapEntries = Object.entries(sectorMap);
          if (sectorMapEntries.length === 0) continue;
          const firstVal = sectorMapEntries[0][1];
          if (typeof firstVal === 'object' && !Array.isArray(firstVal) && firstVal !== null) {
            // New dock-out / master format: { sectorName: { companyName: confidence } }
            // Contribution = count × confidence (already record-count proportional)
            for (const [sectorName, companyMap] of sectorMapEntries) {
              if (typeof companyMap !== 'object' || companyMap === null) continue;
              for (const [companyName, conf] of Object.entries(companyMap)) {
                const confNum = Number(conf);
                if (!(confNum > 0)) continue;  // skip zero/undefined confidence — no records to attribute
                if (!sectorCounts[companyName]) sectorCounts[companyName] = {};
                sectorCounts[companyName][sectorName] = (sectorCounts[companyName][sectorName] || 0) + count * confNum;
              }
            }
          } else if (Array.isArray(firstVal)) {
            // Old dock-out format: { companyName: [sectorName, ...] } — distribute count evenly
            for (const [companyName, sectorList] of sectorMapEntries) {
              if (!Array.isArray(sectorList) || sectorList.length === 0) continue;
              const share = count / sectorList.length;
              if (!sectorCounts[companyName]) sectorCounts[companyName] = {};
              for (const s of sectorList) if (s) sectorCounts[companyName][s] = (sectorCounts[companyName][s] || 0) + share;
            }
          }
        } else if (sectorDist && typeof sectorDist === 'object') {
          // Legacy format: company-first { companyName: { sectorName: count } } — use stored counts directly
          for (const [companyName, sectorsObj] of Object.entries(sectorDist)) {
            if (typeof sectorsObj !== 'object' || sectorsObj === null) continue;
            if (!sectorCounts[companyName]) sectorCounts[companyName] = {};
            for (const [sectorName, cnt] of Object.entries(sectorsObj)) {
              sectorCounts[companyName][sectorName] = (sectorCounts[companyName][sectorName] || 0) + (Number(cnt) || 0);
            }
          }
        }
      }
      // Build sector-first output: confidence = count(company, sector) / count(company, all sectors)
      for (const [companyName, sectorCountMap] of Object.entries(sectorCounts)) {
        const totalCount = Object.values(sectorCountMap).reduce((s, c) => s + c, 0);
        if (totalCount <= 0) continue;
        for (const [sectorName, cnt] of Object.entries(sectorCountMap)) {
          const confidence = Math.round((cnt / totalCount) * 1000) / 1000;
          if (!companyRecord.sector[sectorName]) companyRecord.sector[sectorName] = {};
          companyRecord.sector[sectorName][companyName] = confidence;
        }
      }
      companyRecord._users = [...companyUsersSet];
      if (companyRecord._userCount > 0) consolidated.company = { company: companyRecord };

      // ── Job Title: merge each unique title independently ──
      // Handles two entry formats:
      //   Old (already-consolidated, from ML_Master_Jobfamily_Seniority.json):
      //     { job_title: "<string>", Seniority: {...}, job_family: {...}, top_skills: {...}, _users, _userCount }
      //   New (user-keyed, from ML_Holding after recent restructuring):
      //     { username, job_title: { "<Title>": { job_family:{}, Seniority:{}, top_10_skills:{} }, ... } }
      // In both cases every unique job title is kept as its own independent record and blended only
      // when the same title appears from multiple users/contributions.
      //
      // titleLookupIndex: Map<titleNameLower → snakeKey> — O(1) lookup replaces O(n) find().
      const titleLookupIndex = new Map();
      // Per-entry Sets for user deduplication; keyed by snakeKey. Converted to arrays at output.
      const titleUsersSet = new Map(); // snakeKey → Set<username>

      function mergeOneJobTitle(titleName, titleData, users, count) {
        // Normalise skills: accept both "top_skills" and "top_10_skills" field names; convert arrays to obj
        const rawSkills = titleData.top_skills || titleData.top_10_skills || null;
        const incomingSkills = Array.isArray(rawSkills)
          ? Object.fromEntries(rawSkills.filter(s => typeof s === 'string' && s.length > 0).map(s => [s, 1]))
          : (rawSkills && typeof rawSkills === 'object' ? rawSkills : null);

        const snakeKey = titleName.toLowerCase().replace(/\s+/g, '_');
        // O(1) lookup via index instead of O(n) Object.keys().find()
        let existingKey = titleLookupIndex.get(titleName.toLowerCase());
        if (!existingKey && titleLookupIndex.has(snakeKey)) existingKey = snakeKey;

        if (existingKey) {
          const existing = consolidated.job_title[existingKey];
          const existingUserCount = existing._userCount || 1;
          if (titleData.Seniority && existing.Seniority) existing.Seniority = blendMaps(existing.Seniority, titleData.Seniority, existingUserCount, count);
          else if (titleData.Seniority) existing.Seniority = { ...titleData.Seniority };
          if (titleData.job_family && existing.job_family) existing.job_family = blendMaps(existing.job_family, titleData.job_family, existingUserCount, count);
          else if (titleData.job_family) existing.job_family = { ...titleData.job_family };
          if (titleData.sourcing_status && existing.sourcing_status) existing.sourcing_status = blendMaps(existing.sourcing_status, titleData.sourcing_status, existingUserCount, count);
          else if (titleData.sourcing_status) existing.sourcing_status = { ...titleData.sourcing_status };
          if (incomingSkills && existing.top_skills) {
            const total = existingUserCount + count;
            const merged = {};
            const allSkills = new Set([...Object.keys(existing.top_skills), ...Object.keys(incomingSkills)]);
            for (const sk of allSkills) {
              merged[sk] = Math.round(((Number(existing.top_skills[sk] || 0) * existingUserCount + Number(incomingSkills[sk] || 0) * count) / total) * 1000) / 1000;
            }
            existing.top_skills = Object.fromEntries(Object.entries(merged).sort((a, b) => b[1] - a[1]).slice(0, 10));
          } else if (incomingSkills) {
            existing.top_skills = incomingSkills;
          }
          existing._userCount = existingUserCount + count;
          const usSet = titleUsersSet.get(existingKey);
          if (usSet) for (const u of users) usSet.add(u);
          // Merge Total_Experience: expand the range to cover both sets of candidates
          if (titleData.Total_Experience) {
            if (typeof titleData.Total_Experience === 'object' && titleData.Total_Experience !== null && 'min' in titleData.Total_Experience && 'max' in titleData.Total_Experience) {
              if (existing.Total_Experience && typeof existing.Total_Experience === 'object') {
                existing.Total_Experience = {
                  min: Math.min(existing.Total_Experience.min, titleData.Total_Experience.min),
                  max: Math.max(existing.Total_Experience.max, titleData.Total_Experience.max),
                };
              } else {
                existing.Total_Experience = titleData.Total_Experience;
              }
            } else {
              // Legacy string/number value — overwrite if we don't already have a range
              if (!existing.Total_Experience || typeof existing.Total_Experience !== 'object') {
                existing.Total_Experience = titleData.Total_Experience;
              }
            }
          }
          existing.last_updated = today;
        } else {
          // Build the canonical consolidated entry (always uses "top_skills" key for consistency)
          const newEntry = {
            job_title: titleName,
            ...(titleData.Seniority ? { Seniority: titleData.Seniority } : {}),
            ...(titleData.job_family ? { job_family: titleData.job_family } : {}),
            ...(titleData.sourcing_status ? { sourcing_status: titleData.sourcing_status } : {}),
            ...(incomingSkills ? { top_skills: incomingSkills } : {}),
            ...(titleData.Total_Experience ? { Total_Experience: titleData.Total_Experience } : {}),
            _userCount: count,
            _users: [],   // populated from titleUsersSet at output time
            last_updated: today,
          };
          consolidated.job_title[snakeKey] = newEntry;
          // Register in O(1) lookup index
          titleLookupIndex.set(titleName.toLowerCase(), snakeKey);
          titleLookupIndex.set(snakeKey, snakeKey);
          titleUsersSet.set(snakeKey, new Set(users));
        }
      }

      // Handle both new Job_Families array format and user-keyed dict format (from ML_Holding).
      // masterFiles.job_title may contain BOTH at the same time:
      //   - Job_Families array (from ML_Master_Jobfamily_Seniority.json historical data)
      //   - user-keyed entries like { "orlha": { job_title: {...}, username: "orlha", ... } }
      //     (promoted from ML_Holding; these carry Total_Experience and new user data)
      // Both must be processed so that Total_Experience and new contributions are not lost.
      const jtSource = masterFiles.job_title;

      // Pass 1: process the Job_Families array from the master file (if present)
      if (jtSource && Array.isArray(jtSource.Job_Families)) {
        for (const familyBlock of jtSource.Job_Families) {
          if (!familyBlock || typeof familyBlock !== 'object') continue;
          const familyName = (familyBlock.Job_Family || '').trim() || 'Unknown';
          const jobtitleDict = familyBlock.Jobtitle || {};
          const seniorityDict = familyBlock.Seniority || {};
          const users = Array.isArray(familyBlock._users) ? familyBlock._users : [];
          const familyTotalCount = typeof familyBlock._userCount === 'number' && familyBlock._userCount > 0 ? familyBlock._userCount : 1;
          const jobtitleCount = Object.keys(jobtitleDict).length || 1;
          // Reconstruct per-title seniority distribution:
          // New format: Seniority is embedded directly in each Jobtitle entry as a flat { level: proportion } dict.
          // Old format: reconstruct from the family-level Seniority reverse map (Jobtitle_Match lookup).
          for (const [titleName, titleData] of Object.entries(jobtitleDict)) {
            if (!titleData || typeof titleData !== 'object') continue;
            // Use per-title record count (Record_Count_Jobtitle) when available; otherwise distribute
            // the family total evenly. This prevents inflated counts from family-level _userCount.
            const perTitleCount = typeof titleData.Record_Count_Jobtitle === 'number' && titleData.Record_Count_Jobtitle > 0
              ? titleData.Record_Count_Jobtitle
              : Math.max(1, Math.round(familyTotalCount / jobtitleCount));
            const senDist = {};
            const embeddedSen = titleData.Seniority;
            if (embeddedSen && typeof embeddedSen === 'object' && Object.keys(embeddedSen).length > 0) {
              for (const [level, conf] of Object.entries(embeddedSen)) {
                const c = Number(conf);
                if (!isNaN(c) && c >= 0) senDist[level] = c;
              }
            } else {
              for (const [level, levelData] of Object.entries(seniorityDict)) {
                if (!levelData || !Array.isArray(levelData.Jobtitle_Match)) continue;
                if (levelData.Jobtitle_Match.includes(titleName)) {
                  senDist[level] = Number(levelData.Confidence) || 0;
                }
              }
            }
            // Support both new field name (Unique_Skills) and old name (Unique_Delta_Skills)
            const rawSkills = Array.isArray(titleData.Unique_Skills) ? titleData.Unique_Skills
              : Array.isArray(titleData.Unique_Delta_Skills) ? titleData.Unique_Delta_Skills : [];
            const topSkills = Object.fromEntries(rawSkills.map(s => [s, 1]));
            // Include Must_Have_Skills from Family_Core_DNA as shared skills (lower weight)
            const mustHaveSkills = (familyBlock.Family_Core_DNA && Array.isArray(familyBlock.Family_Core_DNA.Must_Have_Skills))
              ? familyBlock.Family_Core_DNA.Must_Have_Skills : [];
            for (const s of mustHaveSkills) {
              if (s && !topSkills[s]) topSkills[s] = 0.5; // half-weight: shared family skill, not title-unique
            }
            mergeOneJobTitle(titleName, {
              Seniority: senDist,
              job_family: { [familyName]: 1 },
              top_skills: topSkills,
              Total_Experience: titleData.Total_Experience,
            }, users, perTitleCount);
          }
        }
      }

      // Pass 2: process user-keyed entries (from ML_Holding promotions and legacy master dict).
      // Runs unconditionally — ML_Holding entries carry Total_Experience and must not be skipped
      // even when the master file already has a Job_Families array (Pass 1 above).
      for (const [keyName, entry] of Object.entries(jtSource || {})) {
        if (keyName === 'Job_Families') continue;  // already handled in Pass 1
        if (!entry || typeof entry !== 'object') continue;
        const jobTitleField = entry.job_title;
        if (!jobTitleField) continue;

        if (typeof jobTitleField === 'string') {
          // Old format: single job-title record keyed by snake_case title
          const { users, count } = entryMeta(keyName, entry);
          mergeOneJobTitle(jobTitleField, entry, users, count);
        } else if (typeof jobTitleField === 'object' && !Array.isArray(jobTitleField)) {
          // New format: dict of per-title records keyed by username (includes Total_Experience)
          const entryUsername = typeof entry.username === 'string' ? entry.username : keyName;
          if (typeof entry.username !== 'string') {
            console.warn(`[ml-integrate] Entry "${keyName}" has a dict job_title but no username field; using key as username`);
          }
          const users = [entryUsername];
          const count = 1;
          for (const [titleName, titleData] of Object.entries(jobTitleField)) {
            if (!titleData || typeof titleData !== 'object') continue;
            mergeOneJobTitle(titleName, titleData, users, count);
          }
        }
      }

      // ── Compensation: merge per-job-title entries from all users ──
      // Supports two formats:
      //   New array format (ML_Holding after restructuring):
      //     compensation_by_job_title[jt] = { job_family, Compensation: [...], "Verified Compensation": [...] }
      //   Old flat format (backward compat):
      //     compensation_by_job_title[jt] = { country, min, max, count, job_family, last_updated }
      // Internal accumulator per job title:
      //   { _jfFreq, _compByCountry, _vcByCountry }
      // Converted to output format at the end of this block.
      const COMP_ACC_NO_COUNTRY = '__no_country__';

      function getOrInitCompAcc(jobTitle) {
        if (!consolidated.compensation.compensation_by_job_title[jobTitle]) {
          consolidated.compensation.compensation_by_job_title[jobTitle] = {
            _jfFreq: {},
            _compByCountry: {},  // { countryKey: { min, max, count, users, last_updated } }
            _vcByCountry: {},    // verified comp
          };
        }
        return consolidated.compensation.compensation_by_job_title[jobTitle];
      }

      function mergeIntoCountryBucket(byCountry, countryKey, minStr, maxStr, cnt, users, lastUpdated) {
        const key = countryKey || COMP_ACC_NO_COUNTRY;
        if (!byCountry[key]) byCountry[key] = { min: Infinity, max: -Infinity, count: 0, _usersSet: new Set(), last_updated: lastUpdated || today };
        const bucket = byCountry[key];
        const inMin = parseFloat(minStr), inMax = parseFloat(maxStr);
        if (!isNaN(inMin)) bucket.min = Math.min(bucket.min, inMin);
        if (!isNaN(inMax)) bucket.max = Math.max(bucket.max, inMax);
        bucket.count += (cnt || 1);
        for (const u of users) bucket._usersSet.add(u);
        if (lastUpdated) bucket.last_updated = lastUpdated;
      }

      function mergeCompArrayIntoAccum(byCountry, compArray, fallbackUsers) {
        if (!Array.isArray(compArray)) return;
        for (const cEntry of compArray) {
          if (!cEntry || typeof cEntry !== 'object') continue;
          const entryUsers = Array.isArray(cEntry._users) && cEntry._users.length > 0 ? cEntry._users : fallbackUsers;
          mergeIntoCountryBucket(byCountry, cEntry.country || null, cEntry.min, cEntry.max, cEntry.count || 1, entryUsers, cEntry.last_updated);
        }
      }

      for (const [keyName, entry] of Object.entries(masterFiles.compensation)) {
        if (!entry || typeof entry !== 'object') continue;

        // ── Flat consolidated format: previously-integrated master file ──
        // The root key "compensation_by_job_title" holds the already-merged dict directly.
        if (keyName === 'compensation_by_job_title') {
          for (const [jobTitle, compEntry] of Object.entries(entry)) {
            if (!compEntry || typeof compEntry !== 'object') continue;
            const acc = getOrInitCompAcc(jobTitle);
            if (Array.isArray(compEntry.Compensation)) {
              // New array format
              const existingUsers = Array.isArray(compEntry._users) ? compEntry._users : [];
              mergeCompArrayIntoAccum(acc._compByCountry, compEntry.Compensation, existingUsers);
              if (Array.isArray(compEntry['Verified Compensation'])) {
                mergeCompArrayIntoAccum(acc._vcByCountry, compEntry['Verified Compensation'], existingUsers);
              }
            } else {
              // Old flat format: single country entry
              const existingUsers = Array.isArray(compEntry._users) ? compEntry._users : [];
              const titleCount = typeof compEntry.count === 'number' ? compEntry.count : 1;
              mergeIntoCountryBucket(acc._compByCountry, compEntry.country || null, compEntry.min, compEntry.max, titleCount, existingUsers, compEntry.last_updated);
            }
            if (compEntry.job_family) acc._jfFreq[compEntry.job_family] = (acc._jfFreq[compEntry.job_family] || 0) + 1;
          }
          continue;
        }

        const { users, count } = entryMeta(keyName, entry);

        if (entry.compensation_by_job_title && typeof entry.compensation_by_job_title === 'object') {
          // Per-user format: iterate each job title
          for (const [jobTitle, compEntry] of Object.entries(entry.compensation_by_job_title)) {
            if (!compEntry || typeof compEntry !== 'object') continue;
            const acc = getOrInitCompAcc(jobTitle);
            if (Array.isArray(compEntry.Compensation)) {
              // New array format
              mergeCompArrayIntoAccum(acc._compByCountry, compEntry.Compensation, users);
              if (Array.isArray(compEntry['Verified Compensation'])) {
                mergeCompArrayIntoAccum(acc._vcByCountry, compEntry['Verified Compensation'], users);
              }
            } else {
              // Old flat format
              const titleCount = compEntry.count || count;
              mergeIntoCountryBucket(acc._compByCountry, compEntry.country || null, compEntry.min, compEntry.max, titleCount, users, compEntry.last_updated);
            }
            if (compEntry.job_family) acc._jfFreq[compEntry.job_family] = (acc._jfFreq[compEntry.job_family] || 0) + 1;
          }
        } else if (entry.by_job_title) {
          // Old format backward compat: treat as a single job title entry
          const jobTitle = entry.by_job_title;
          const range = entry.range || {};
          const acc = getOrInitCompAcc(jobTitle);
          mergeIntoCountryBucket(acc._compByCountry, null, range.min, range.max, count, users, today);
        }
      }
      // Convert internal accumulators to output format
      for (const [jt, acc] of Object.entries(consolidated.compensation.compensation_by_job_title)) {
        const outputEntry = {};
        // Dominant job family
        if (Object.keys(acc._jfFreq).length > 0) {
          outputEntry.job_family = Object.entries(acc._jfFreq).sort((a, b) => b[1] - a[1])[0][0];
        }
        // Compensation array
        const compArray = Object.entries(acc._compByCountry)
          .filter(([, b]) => b.count > 0)
          .map(([ck, b]) => ({
            ...(ck !== COMP_ACC_NO_COUNTRY ? { country: ck } : {}),
            min: String(b.min === Infinity ? 0 : Math.round(b.min)),
            max: String(b.max === -Infinity ? 0 : Math.round(b.max)),
            count: b.count,
            _users: [...b._usersSet],
            last_updated: b.last_updated,
          }));
        if (compArray.length > 0) outputEntry.Compensation = compArray;
        // Verified Compensation array
        const vcArray = Object.entries(acc._vcByCountry)
          .filter(([, b]) => b.count > 0)
          .map(([ck, b]) => ({
            ...(ck !== COMP_ACC_NO_COUNTRY ? { country: ck } : {}),
            min: String(b.min === Infinity ? 0 : Math.round(b.min)),
            max: String(b.max === -Infinity ? 0 : Math.round(b.max)),
            count: b.count,
            _users: [...b._usersSet],
            last_updated: b.last_updated,
          }));
        if (vcArray.length > 0) outputEntry['Verified Compensation'] = vcArray;
        consolidated.compensation.compensation_by_job_title[jt] = outputEntry;
      }

      // Flush per-title user Sets into _users arrays — all mergeOneJobTitle calls are now done.
      for (const [key, usSet] of titleUsersSet) {
        if (consolidated.job_title[key]) consolidated.job_title[key]._users = [...usSet];
      }

      return consolidated;
    }

    // --- Algorithmic consolidation (always runs — deterministic, numerically correct) ---
    // The algorithmic path computes exact _userCount-weighted blends for job_family, Seniority,
    // and top_skills. This guarantees that e.g. merging "Mid: 1" (master) with "Senior: 1"
    // (holding, count=1) always produces "Mid: 0.5, Senior: 0.5" rather than silently keeping
    // the master value unchanged (which Gemini was doing).
    // Company confidence = 1/N (N = number of sectors the company belongs to) — this is computed
    // algorithmically and must NOT be overridden by Gemini (which produces count-based values).
    let mergedMasters = algorithmicConsolidate();
    let mergeMethod = 'algorithmic';

    // ── Post-consolidation dedup: collapse any identical job titles into one entry ──
    // Runs after both Gemini and algorithmic paths to ensure no duplicate title keys remain.
    const today = new Date().toISOString().split('T')[0];
    if (mergedMasters && mergedMasters.job_title && typeof mergedMasters.job_title === 'object') {
      const blendTwoMaps = (existingMap, incomingMap, existingCount, incomingCount) => {
        if (!existingMap && !incomingMap) return undefined;
        if (!existingMap) return { ...incomingMap };
        if (!incomingMap) return { ...existingMap };
        const total = existingCount + incomingCount;
        const merged = {};
        const allKeys = new Set([...Object.keys(existingMap), ...Object.keys(incomingMap)]);
        for (const k of allKeys) {
          merged[k] = Math.round(((Number(existingMap[k] || 0) * existingCount + Number(incomingMap[k] || 0) * incomingCount) / total) * 1000) / 1000;
        }
        return merged;
      };

      const dedupedJobTitles = {};
      for (const [key, entry] of Object.entries(mergedMasters.job_title)) {
        if (!entry || typeof entry !== 'object') continue;
        const canonicalTitle = typeof entry.job_title === 'string'
          ? entry.job_title
          : key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        const snakeKey = canonicalTitle.toLowerCase().replace(/\s+/g, '_');

        if (dedupedJobTitles[snakeKey]) {
          const existing = dedupedJobTitles[snakeKey];
          const existingCount = typeof existing._userCount === 'number' && existing._userCount > 0 ? existing._userCount : 1;
          const incomingCount = typeof entry._userCount === 'number' && entry._userCount > 0 ? entry._userCount : 1;
          if (entry.Seniority) existing.Seniority = blendTwoMaps(existing.Seniority, entry.Seniority, existingCount, incomingCount) || existing.Seniority;
          if (entry.job_family) existing.job_family = blendTwoMaps(existing.job_family, entry.job_family, existingCount, incomingCount) || existing.job_family;
          if (entry.sourcing_status) existing.sourcing_status = blendTwoMaps(existing.sourcing_status, entry.sourcing_status, existingCount, incomingCount) || existing.sourcing_status;
          const incomingSkills = entry.top_skills || entry.top_10_skills;
          if (incomingSkills && typeof incomingSkills === 'object') {
            const blended = blendTwoMaps(existing.top_skills || {}, incomingSkills, existingCount, incomingCount) || {};
            existing.top_skills = Object.fromEntries(Object.entries(blended).sort((a, b) => b[1] - a[1]).slice(0, 10));
          }
          existing._userCount = existingCount + incomingCount;
          const usersSet = new Set(Array.isArray(existing._users) ? existing._users : []);
          for (const u of (Array.isArray(entry._users) ? entry._users : [])) usersSet.add(u);
          existing._users = [...usersSet];
          existing.last_updated = new Date().toISOString().split('T')[0];
        } else {
          const normalized = { ...entry, job_title: canonicalTitle };
          if (normalized.top_10_skills && !normalized.top_skills) {
            normalized.top_skills = normalized.top_10_skills;
            delete normalized.top_10_skills;
          }
          dedupedJobTitles[snakeKey] = normalized;
        }
      }
      mergedMasters.job_title = dedupedJobTitles;
      console.info(`[admin/ml-integrate] Post-dedup: ${Object.keys(dedupedJobTitles).length} unique job title(s) in ML_Master_Jobfamily_Seniority`);

      // ── Convert per-title dict to Job_Families array format for master file ──
      // A title is placed into EVERY family it belongs to (all entries in its job_family distribution),
      // not just the dominant one. This preserves cross-family reassignments that occur during integration
      // (e.g. Cloud Engineer previously under Cloud Engineering + new ML_Holding entry under Software
      // Engineering → both family blocks show Cloud Engineer with proportionally adjusted confidence).
      // familyRC: effective record count for the title in each family = round(totalRC * proportion).
      const jfGroups = {};  // { normalizedKey: { display: string, entries: [{titleName, entry, familyRC}] } }
      for (const [snakeKey, entry] of Object.entries(dedupedJobTitles)) {
        if (!entry || typeof entry !== 'object') continue;
        const titleName = typeof entry.job_title === 'string' ? entry.job_title : snakeKey.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        const jfDist = entry.job_family || {};
        const totalRC = typeof entry._userCount === 'number' && entry._userCount > 0 ? entry._userCount : 1;
        if (Object.keys(jfDist).length > 0) {
          // Place title into each family it belongs to with proportional record count
          for (const [familyRaw, proportion] of Object.entries(jfDist)) {
            if (Number(proportion) <= 0) continue;
            const normalizedKey = familyRaw.trim().toLowerCase();
            const displayName   = familyRaw.trim() || 'Unknown';
            const familyRC      = Math.max(1, Math.round(totalRC * Number(proportion)));
            if (!jfGroups[normalizedKey]) jfGroups[normalizedKey] = { display: displayName, entries: [] };
            jfGroups[normalizedKey].entries.push({ titleName, entry, familyRC });
          }
        } else {
          if (!jfGroups['unknown']) jfGroups['unknown'] = { display: 'Unknown', entries: [] };
          jfGroups['unknown'].entries.push({ titleName, entry, familyRC: totalRC });
        }
      }

      // Compute total record count per title across ALL families.
      // Confidence = rc_in_family / total_rc_for_title — titles exclusive to one family
      // always get confidence 1.0; only cross-family assignments reduce confidence.
      const totalRCPerTitle = {};
      for (const { entries } of Object.values(jfGroups)) {
        for (const { titleName, familyRC } of entries) {
          totalRCPerTitle[titleName] = (totalRCPerTitle[titleName] || 0) + familyRC;
        }
      }

      const jobFamiliesArray = [];
      for (const [, { display: familyName, entries: familyEntries }] of Object.entries(jfGroups)) {
        // Must_Have_Skills: intersection of top_skills across titles in family (supplement if < 10)
        const titleSkillSets = {};
        const allSkillByConf = {};
        for (const { titleName, entry } of familyEntries) {
          const skills = entry.top_skills || {};
          titleSkillSets[titleName] = new Set(Object.keys(skills).map(s => s.toLowerCase()));
          for (const [skill, conf] of Object.entries(skills)) {
            const lc = skill.toLowerCase();
            if (!allSkillByConf[lc] || conf > allSkillByConf[lc].conf) allSkillByConf[lc] = { skill, conf: Number(conf) };
          }
        }
        let mustHaveSkills = [];
        // For both single and multi-title families: compute intersection (all skills for single title),
        // then supplement with top-confidence skills from the family if intersection < 10.
        let intersection = new Set(titleSkillSets[familyEntries[0].titleName]);
        for (let i = 1; i < familyEntries.length; i++) intersection = new Set([...intersection].filter(s => titleSkillSets[familyEntries[i].titleName].has(s)));
        mustHaveSkills = [...intersection].sort((a, b) => (allSkillByConf[b]?.conf || 0) - (allSkillByConf[a]?.conf || 0)).slice(0, 10).map(lc => allSkillByConf[lc]?.skill || lc);
        if (mustHaveSkills.length < 10) {
          const existing = new Set(mustHaveSkills.map(s => s.toLowerCase()));
          for (const [lc, { skill }] of Object.entries(allSkillByConf).sort((a, b) => b[1].conf - a[1].conf)) {
            if (mustHaveSkills.length >= 10) break;
            if (!existing.has(lc)) { mustHaveSkills.push(skill); existing.add(lc); }
          }
        }

        // Unique_Skills: per title, skills not in any other title's skill set
        const uniqueDeltaPerTitle = {};
        for (const { titleName, entry } of familyEntries) {
          const otherSkills = new Set();
          for (const { titleName: ot, entry: oe } of familyEntries) {
            if (ot === titleName) continue;
            for (const s of Object.keys(oe.top_skills || {})) otherSkills.add(s.toLowerCase());
          }
          uniqueDeltaPerTitle[titleName] = Object.entries(entry.top_skills || {}).sort((a, b) => b[1] - a[1]).filter(([s]) => !otherSkills.has(s.toLowerCase())).slice(0, 10).map(([s]) => s);
        }

        // Collect users across titles in family; use familyRC (not entry._userCount) for counts
        // so each title's share within this family reflects its cross-family-proportioned record count.
        const familyUsers = new Set();
        let familyCount = 0;
        for (const { entry, familyRC } of familyEntries) {
          if (Array.isArray(entry._users)) entry._users.forEach(u => familyUsers.add(u));
          familyCount += familyRC;
        }

        // Confidence_Threshold: max combined (title_conf × max_seniority_proportion) in this family
        // - title_conf: cross-family confidence (only drops when title spans multiple families)
        // - max_seniority_proportion: highest seniority level proportion
        //   (e.g. Manager:0.5/Mid:0.5 gives 0.5, reducing threshold vs a single dominant level at 1.0)
        const confThreshold = familyEntries.length > 0
          ? Math.round(Math.max(...familyEntries.map(({ titleName, familyRC, entry }) => {
              const tot = totalRCPerTitle[titleName] || familyRC;
              const titleConf = familyRC / tot;
              const sen = entry.Seniority || {};
              const senVals = Object.values(sen).filter(v => typeof v === 'number');
              const maxSenProp = senVals.length > 0 ? Math.max(...senVals) : 1;
              return titleConf * maxSenProp;
            })) * 1000) / 1000
          : 0;

        // Jobtitle section — Confidence = rc_in_family / total_rc_for_title across ALL families
        // (multiple titles in the same family do NOT reduce each other's confidence;
        //  confidence is only reduced when the title appears in more than one family)
        const jobtitleSection = {};
        for (const { titleName, entry, familyRC: rc } of familyEntries) {
          const totalRCForTitle = totalRCPerTitle[titleName] || rc;
          const titleConf = Math.round((rc / totalRCForTitle) * 1000) / 1000;
          const titleEntry = {
            Record_Count_Jobtitle: rc,
            Seniority: entry.Seniority || {},
            Unique_Skills: uniqueDeltaPerTitle[titleName] || [],
            Confidence: titleConf,
          };
          if (entry.Total_Experience) titleEntry.Total_Experience = entry.Total_Experience;
          jobtitleSection[titleName] = titleEntry;
        }

        jobFamiliesArray.push({
          Job_Family: familyName,
          last_updated: today,
          Family_Core_DNA: { Must_Have_Skills: mustHaveSkills, Confidence_Threshold: confThreshold },
          Jobtitle: jobtitleSection,
          _users: [...familyUsers],
          _userCount: familyCount,
        });
      }
      mergedMasters.job_title = { Job_Families: jobFamiliesArray };
    }

    // Save consolidated master files
    const saveMap = {
      company:      mergedMasters.company,
      job_title:    mergedMasters.job_title,
      compensation: mergedMasters.compensation,
    };
    for (const [key, fp] of Object.entries(masterPaths)) {
      fs.writeFileSync(fp, JSON.stringify(saveMap[key], null, 2), 'utf8');
      console.info(`[admin/ml-integrate] Saved ${fp} (${mergeMethod})`);
    }

    res.json({ ok: true, integrated, promoted: promotedCount, method: mergeMethod, message: `Promoted ${promotedCount} user(s) from holding; consolidated ${integrated} total entries using ${mergeMethod}.` });
  } catch (err) {
    console.error('[admin/ml-integrate] Error:', err.message);
    res.status(500).json({ error: 'ML integration failed: ' + err.message });
  }
});

// POST /candidates/bulletin-export — write finalized bulletin selections to a JSON file (called during DB Dock Out)

// ── Bulletin-export helpers (module-level to avoid per-request re-creation) ──
// Extracts a numeric rating score from JSON object, percentage string, or plain integer.
// Mirrors the LookerDashboard.html extractRatingScore logic.
function _extractRatingScore(val) {
  if (val === null || val === undefined || val === '') return null;
  if (typeof val === 'object') {
    const ts = val.total_score;
    if (ts !== undefined && ts !== null) {
      const m = String(ts).match(/(\d+)/);
      if (m) return parseInt(m[1], 10);
    }
    return null;
  }
  const s = String(val).trim();
  if (s.startsWith('{')) {
    try {
      const obj = JSON.parse(s);
      if (obj && obj.total_score !== undefined) {
        const m = String(obj.total_score).match(/(\d+)/);
        if (m) return parseInt(m[1], 10);
      }
    } catch (_) {}
  }
  const m = s.match(/(\d+)/);
  if (m) return parseInt(m[1], 10);
  return null;
}
const _SENIORITY_RANK = {intern:0,trainee:0,graduate:0,entry:0,junior:1,jr:1,associate:2,mid:3,intermediate:3,senior:4,sr:4,lead:5,principal:5,specialist:5,manager:6,mgr:6,director:7,dir:7,vp:8,vice:8,head:9,chief:9};
function _seniorityRank(s) {
  const lower = (s || '').toLowerCase();
  return Object.entries(_SENIORITY_RANK).reduce((r, [k,v]) => lower.includes(k) ? Math.max(r,v) : r, -1);
}

app.post('/candidates/bulletin-export', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { role_tag, skillsets, countries: selectedCountries, jobfamily, sector, sourcingStatuses, headline, description, imageData, publicPost, company_name } = req.body || {};
    // Fetch cemail for the current user to include in the bulletin JSON
    let cemail = null;
    try {
      const emailResult = await pool.query('SELECT cemail FROM login WHERE username = $1 LIMIT 1', [req.user.username]);
      if (emailResult.rows.length > 0) cemail = emailResult.rows[0].cemail || null;
    } catch (emailErr) {
      console.warn('[Bulletin Export] Could not fetch cemail:', emailErr.message);
    }
    let exportData;
    if (role_tag) {
      const result = await pool.query(
        `SELECT role_tag, seniority, skillset, country, jobfamily, sector, rating, sourcingstatus
         FROM "process" WHERE userid = $1 AND role_tag = $2`,
        [String(req.user.id), role_tag]
      );
      const rows = result.rows;
      // Secondary filter: apply sourcing statuses to refine seniority, avg_rating, and available_profiles
      const sourcingFilter = Array.isArray(sourcingStatuses) && sourcingStatuses.length > 0 ? sourcingStatuses : null;
      const doubleFilteredRows = sourcingFilter
        ? rows.filter(r => sourcingFilter.includes(String(r.sourcingstatus || '').trim()))
        : rows;
      let totalScore = 0, ratedCount = 0;
      doubleFilteredRows.forEach(r => {
        const score = _extractRatingScore(r.rating);
        if (score !== null) { totalScore += score; ratedCount++; }
      });
      const avgRating = ratedCount > 0 ? Math.round(totalScore / ratedCount) + '%' : null;
      const seniorities = [...new Set(doubleFilteredRows.map(r => r.seniority).filter(Boolean))].sort((a,b) => _seniorityRank(a) - _seniorityRank(b));
      const sourcedCount = doubleFilteredRows.length;
      exportData = {
        role_tag,
        email: cemail,
        headline: headline || null,
        description: description || null,
        image_data: (typeof imageData === 'string' && imageData.startsWith('data:image/')) ? imageData : null,
        skillsets: skillsets || [],
        seniority: seniorities.join(', '),
        available_profiles: sourcedCount,
        country: Array.isArray(selectedCountries) ? selectedCountries : [],
        jobfamily: jobfamily || null,
        sector: sector || null,
        avg_rating: avgRating,
        public: publicPost === true,
        company_name: company_name || null,
      };
    } else {
      const result = await pool.query(
        `SELECT role_tag, seniority, skillset, country, jobfamily, sector, rating, sourcingstatus
         FROM "process" WHERE userid = $1`,
        [String(req.user.id)]
      );
      exportData = result.rows;
    }
    const bulletinDir = process.env.BULLETIN_OUTPUT_DIR
      || path.join('F:\\', 'Recruiting Tools', 'Autosourcing', 'output', 'bulletin');
    try {
      fs.mkdirSync(bulletinDir, { recursive: true });
    } catch (mkdirErr) {
      console.warn('[Bulletin Export] Could not create bulletin directory:', mkdirErr.message);
      return res.status(500).json({ error: 'Failed to create bulletin directory.', detail: mkdirErr.message });
    }
    const filename = `${req.user.username}_bulletin.json`;
    const filepath = path.join(bulletinDir, filename);
    try {
      fs.writeFileSync(filepath, JSON.stringify(exportData, null, 2), 'utf8');
      // Invalidate bulletin caches so the next read picks up the new file
      _bulletinCacheAll = null; _bulletinCacheAllTs = 0;
      _bulletinCachePub = null; _bulletinCachePubTs = 0;
    } catch (writeErr) {
      console.error('[Bulletin Export] Could not write bulletin file:', writeErr.message);
      return res.status(500).json({ error: 'Failed to write bulletin file.', detail: writeErr.message });
    }
    const count = Array.isArray(exportData) ? exportData.length : 1;
    res.json({ ok: true, file: filename, count });
  } catch (err) {
    console.error('[Bulletin Export] Export error:', err);
    res.status(500).json({ error: 'Failed to generate bulletin export.' });
  }
});

// ── Bulletin in-memory cache (5 s TTL) ───────────────────────────────────────
// Prevents re-scanning + re-reading every *_bulletin.json file on every request.
// Separate slots for the authenticated (all) and public subsets.
const _BULLETIN_CACHE_MS = parseInt(process.env.BULLETIN_CACHE_MS, 10) || 5_000;
let _bulletinCacheAll    = null, _bulletinCacheAllTs    = 0;
let _bulletinCachePub    = null, _bulletinCachePubTs    = 0;

async function _loadBulletins(publicOnly) {
  const now = Date.now();
  if (publicOnly) {
    if (_bulletinCachePub && now - _bulletinCachePubTs < _BULLETIN_CACHE_MS) return _bulletinCachePub;
  } else {
    if (_bulletinCacheAll && now - _bulletinCacheAllTs < _BULLETIN_CACHE_MS) return _bulletinCacheAll;
  }
  const bulletinDir = process.env.BULLETIN_OUTPUT_DIR
    || path.join('F:\\', 'Recruiting Tools', 'Autosourcing', 'output', 'bulletin');
  let files;
  try {
    files = (await fs.promises.readdir(bulletinDir)).filter(f => f.endsWith('_bulletin.json'));
  } catch (e) {
    if (e.code !== 'ENOENT') throw e;
    const empty = [];
    if (publicOnly) { _bulletinCachePub = empty; _bulletinCachePubTs = now; }
    else            { _bulletinCacheAll = empty; _bulletinCacheAllTs = now; }
    return empty;
  }
  // Read all bulletin files in parallel
  const results = await Promise.all(files.map(async file => {
    try {
      const raw = await fs.promises.readFile(path.join(bulletinDir, file), 'utf8');
      return { file, data: JSON.parse(raw) };
    } catch (e) {
      console.warn('[Community Bulletins] Could not read/parse bulletin file:', file, e.message);
      return null;
    }
  }));
  const bulletins = [];
  for (const item of results) {
    if (!item) continue;
    if (!publicOnly) {
      bulletins.push({ file: item.file, ...item.data });
    } else if (item.data.public === true) {
      const { email: _email, ...safeData } = item.data;
      bulletins.push({ file: item.file, ...safeData });
    }
  }
  if (publicOnly) { _bulletinCachePub = bulletins; _bulletinCachePubTs = now; }
  else            { _bulletinCacheAll = bulletins; _bulletinCacheAllTs = now; }
  return bulletins;
}

// GET /community/bulletins — returns all *_bulletin.json files from BULLETIN_OUTPUT_DIR
app.get('/community/bulletins', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    res.json({ bulletins: await _loadBulletins(false) });
  } catch (err) {
    console.error('[Community Bulletins] Error reading bulletin dir:', err);
    res.status(500).json({ error: 'Failed to load community bulletins.' });
  }
});

// GET /community/bulletins/public — returns only public bulletins (public:true), no login required
app.get('/community/bulletins/public', dashboardRateLimit, async (req, res) => {
  try {
    res.json({ bulletins: await _loadBulletins(true) });
  } catch (err) {
    console.error('[Community Bulletins Public] Error reading bulletin dir:', err);
    res.status(500).json({ error: 'Failed to load public bulletins.' });
  }
});

// GET /candidates/dock-protection-key — returns a per-user worksheet protection key
// derived from the stored password hash. Used to password-protect non-candidate
// worksheets in the DB Dock Out export so they cannot be casually edited in Excel.
app.get('/candidates/dock-protection-key', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const username = req.user.username;
    const userid   = String(req.user.id || '');
    const result = await pool.query('SELECT password FROM login WHERE username = $1 LIMIT 1', [username]);
    if (result.rows.length === 0) {
      return res.status(404).json({ ok: false, error: 'User not found' });
    }
    const storedHash = result.rows[0].password || '';
    // Derive a deterministic protection key using HMAC-SHA256.
    // The raw stored hash is never exposed — only the derived key (first 16 hex chars) is returned.
    const hmac = crypto.createHmac('sha256', storedHash);
    hmac.update('dock-protection:' + username + ':' + userid);
    const key = hmac.digest('hex').slice(0, 16);
    return res.json({ ok: true, key });
  } catch (err) {
    console.error('[Dock Protection Key] Error:', err);
    return res.status(500).json({ ok: false, error: 'Failed to generate protection key' });
  }
});

// GET /candidates/dock-out-criteria — returns JSON files from CRITERIA_DIR as tab data
app.get('/candidates/dock-out-criteria', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    if (!fs.existsSync(CRITERIA_DIR)) return res.json({ files: [] });
    const username = req.user && req.user.username ? String(req.user.username) : '';
    if (!username) return res.json({ files: [] });
    const userSuffix = ` ${username}.json`;
    const entries = fs.readdirSync(CRITERIA_DIR).filter(f =>
      f.toLowerCase().endsWith('.json') &&
      f.length >= userSuffix.length &&
      f.slice(-userSuffix.length).toLowerCase() === userSuffix.toLowerCase()
    );
    const files = [];
    for (const name of entries) {
      try {
        const raw = fs.readFileSync(path.join(CRITERIA_DIR, name), 'utf8');
        let content;
        try { content = JSON.parse(raw); } catch (_) { content = raw; }
        files.push({ name, content });
      } catch (_) { /* skip unreadable files */ }
    }
    res.json({ files });
  } catch (err) {
    console.error('[Dock-Out Criteria] Error reading criteria dir:', err);
    res.status(500).json({ error: 'Failed to load criteria files.' });
  }
});

// POST /candidates/dock-in-criteria — write JSON files to CRITERIA_DIR (called on DB Dock In)
app.post('/candidates/dock-in-criteria', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const { files } = req.body;
    if (!Array.isArray(files) || files.length === 0) return res.json({ ok: true, written: 0 });
    if (!fs.existsSync(CRITERIA_DIR)) fs.mkdirSync(CRITERIA_DIR, { recursive: true });
    let written = 0;
    for (const f of files) {
      if (!f || typeof f.name !== 'string' || !f.name.trim()) continue;
      const safeName = path.basename(f.name.trim()); // prevent path traversal
      if (!safeName.toLowerCase().endsWith('.json')) continue; // only .json files
      const dest = path.join(CRITERIA_DIR, safeName);
      const contentStr = typeof f.content === 'string' ? f.content : JSON.stringify(f.content, null, 2);
      fs.writeFileSync(dest, contentStr, 'utf8');
      written++;
    }
    res.json({ ok: true, written });
  } catch (err) {
    console.error('[Dock-In Criteria] Error writing criteria files:', err);
    res.status(500).json({ error: 'Failed to write criteria files.' });
  }
});

// GET /bulletin/images — list image files from the configured image directory
const BULLETIN_IMAGE_DIR = process.env.IMAGE_DIR || path.join('F:\\', 'Recruiting Tools', 'Autosourcing', 'Image');
const ALLOWED_IMAGE_EXTS = new Set(['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg']);

app.get('/bulletin/images', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    if (!fs.existsSync(BULLETIN_IMAGE_DIR)) {
      return res.json({ ok: true, images: [] });
    }
    const files = fs.readdirSync(BULLETIN_IMAGE_DIR).filter(f => {
      const ext = path.extname(f).toLowerCase();
      return ALLOWED_IMAGE_EXTS.has(ext);
    });
    return res.json({ ok: true, images: files });
  } catch (err) {
    console.error('/bulletin/images error:', err);
    return res.status(500).json({ error: 'Failed to list images.' });
  }
});

// GET /bulletin/image/:filename — serve a single image from the image directory
app.get('/bulletin/image/:filename', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const filename = path.basename(req.params.filename); // strip any path components to prevent traversal
    const ext = path.extname(filename).toLowerCase();
    if (!ALLOWED_IMAGE_EXTS.has(ext)) return res.status(400).json({ error: 'Invalid file type.' });
    const filepath = path.join(BULLETIN_IMAGE_DIR, filename);
    if (!fs.existsSync(filepath)) return res.status(404).json({ error: 'Image not found.' });
    res.sendFile(filepath);
  } catch (err) {
    console.error('/bulletin/image error:', err);
    return res.status(500).json({ error: 'Failed to serve image.' });
  }
});

app.delete('/candidates/:id', requireLogin, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) {
    return res.status(400).json({ error: 'Invalid candidate id.' });
  }

  // Ownership guard
  const ownerOk = await ensureOwnershipOrFail(res, id, req.user.id);
  if (!ownerOk) return;

  try {
    const result = await pool.query('DELETE FROM "process" WHERE id = $1 RETURNING id', [id]);
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Candidate not found.' });
    }

    // Emit deletion event so connected clients can react if they listen
    try {
      broadcastSSE('candidate_deleted', { id });
      broadcastSSE('candidates_changed', { action: 'delete', ids: [id] });
    } catch (_) { /* ignore emit errors */ }

    res.json({ deleted: id });
  } catch (err) {
    console.error('Delete process row error:', err);
    res.status(500).json({ error: 'Failed to delete candidate/process row.' });
  }
});

app.post('/candidates/bulk-delete', requireLogin, userRateLimit('bulk_delete'), async (req, res) => {
  const { ids } = req.body;
  console.log('[API] bulk-delete received ids:', ids);

  if (!Array.isArray(ids) || ids.length === 0) {
    return res.status(400).json({ error: 'No valid candidate ids provided.' });
  }

  const cleanIds = ids
    .map(id => {
      const n = typeof id === 'number' ? id : parseInt(id, 10);
      return Number.isInteger(n) && n > 0 ? n : null;
    })
    .filter(n => n !== null);

  console.log('[API] bulk-delete cleanIds (numeric):', cleanIds);

  if (cleanIds.length === 0) {
    return res.status(400).json({
      error: 'No valid candidate ids provided. Expecting numeric ids only.',
      received: ids
    });
  }

  try {
    // Only delete rows that belong to the requesting user
    const result = await pool.query(
      'DELETE FROM "process" WHERE id = ANY($1::int[]) AND userid = $2 RETURNING id',
      [cleanIds, String(req.user.id)]
    );
    console.log('[API] bulk-delete deletedCount:', result.rowCount);

    // emit event to notify clients
    try {
      broadcastSSE('candidates_changed', { action: 'bulk_delete', ids: result.rows.map(r => r.id) });
    } catch (_) { /* ignore */ }

    res.json({ deletedCount: result.rowCount, attempted: cleanIds.length, ids: result.rows.map(r => r.id) });
    _writeApprovalLog({ action: 'bulk_candidates_delete', username: req.user.username, userid: req.user.id, detail: `Bulk deleted ${result.rowCount} candidates`, source: 'server.js' });
  } catch (err) {
    console.error('Bulk delete error:', err);
    res.status(500).json({ error: 'Bulk delete failed.' });
  }
});

app.post('/generate-skillsets', requireLogin, async (req, res) => {
  try {
    if (!fs.existsSync(mappingPath)) {
      return res.status(500).json({ error: 'Skillset mapping file not found.' });
    }
    const raw = fs.readFileSync(mappingPath, 'utf8');
    const skillsetMap = JSON.parse(raw);

    const candidates = (await pool.query('SELECT id, role_tag, skillset FROM "process"')).rows;

    // Build update pairs: collect only rows whose skillset would actually change
    const ids = [], newSkillsets = [];
    for (const candidate of candidates) {
      const roleTag = candidate.role_tag ? candidate.role_tag.trim() : '';
      const newSkillset = skillsetMap[roleTag] || '';
      if (newSkillset && newSkillset !== candidate.skillset) {
        ids.push(candidate.id);
        newSkillsets.push(newSkillset);
      }
    }
    const updatedCount = ids.length;

    if (updatedCount > 0) {
      // Single UNNEST batch UPDATE — one roundtrip regardless of how many rows changed
      await pool.query(
        `UPDATE "process" AS p
         SET skillset = v.skillset
         FROM UNNEST($1::int[], $2::text[]) AS v(id, skillset)
         WHERE p.id = v.id`,
        [ids, newSkillsets]
      );
    }

    // Let clients know skillsets changed (they can refetch)
    try {
      broadcastSSE('candidates_changed', { action: 'skillset_update', count: updatedCount });
    } catch (_) { /* ignore */ }

    res.json({ message: `Skillsets generated for ${updatedCount} process rows.` });
  } catch (err) {
    console.error('Skillset generation error:', err);
    res.status(500).json({ error: 'Failed to generate skillsets.' });
  }
});

app.get('/org-chart', requireLogin, (req, res) => {
  res.json([{ name: 'Sample Org Chart' }]);
});

/**
 * POST /candidates
 * Create a new process row. Accepts candidate-style keys (role, organisation, job_family, sourcing_status, type)
 * or process-style keys (jobtitle, company, jobfamily, sourcingstatus, product). Returns the created row.
 */
app.post('/candidates', requireLogin, async (req, res) => {
  const body = req.body || {};

  // Acceptable mapping for create (candidate-style -> process column)
  const createFieldMap = {
    // candidate -> process
    role: 'jobtitle',
    jobtitle: 'jobtitle',
    organisation: 'company',
    job_family: 'jobfamily',
    sourcing_status: 'sourcingstatus',
    type: 'product',
    product: 'product',

    // process keys (pass-through)
    jobtitle: 'jobtitle',
    company: 'company',
    jobfamily: 'jobfamily',
    sourcingstatus: 'sourcingstatus',

    // same-name fields
    name: 'name',
    sector: 'sector',
    role_tag: 'role_tag',
    skillset: 'skillset',
    geographic: 'geographic',
    country: 'country',
    email: 'email',
    mobile: 'mobile',
    office: 'office',
    compensation: 'compensation',
    seniority: 'seniority',
    lskillset: 'lskillset',
    linkedinurl: 'linkedinurl',
    comment: 'comment'
  };

  // Build columns and values for insert
  const cols = [];
  const values = [];
  const placeholders = [];
  let idx = 1;

  for (const key of Object.keys(body)) {
  if (!Object.prototype.hasOwnProperty.call(createFieldMap, key)) continue;
  let col = createFieldMap[key];
  let val = body[key];

  // Canonicalize seniority on create
  if (key === 'seniority' && val != null && String(val).trim() !== '') {
    const std = standardizeSeniority(val);
    // persist only canonical value (or null if unrecognized)
    val = std || null;
  }

  // Validate compensation: must be numeric
  if (key === 'compensation' && val != null && val !== '') {
    const n = Number(val);
    if (isNaN(n)) {
      return res.status(400).json({ error: 'Compensation must be a numeric value.' });
    }
    val = n;
  }

  // normalize empty string to null
  if (val === '') val = null;

  cols.push(`"${col}"`);
  values.push(val);
  placeholders.push(`$${idx}`);
  idx++;
}
  // Inject User info
  cols.push(`"userid"`);
  values.push(req.user.id);
  placeholders.push(`$${idx++}`);

  cols.push(`"username"`);
  values.push(req.user.username);
  placeholders.push(`$${idx++}`);

  // Fetch user's JD skill from login table (jskillset) using USERNAME (more reliable)
  let userJskillset = null;
  try {
    const ures = await pool.query('SELECT jskillset FROM login WHERE username = $1', [req.user.username]);
    if (ures.rows.length > 0) userJskillset = ures.rows[0].jskillset || null;
  } catch (e) {
    console.warn('[POST /candidates] unable to fetch user jskillset via username', e && e.message);
    userJskillset = null;
  }

  // NEW: include jskillset column + value
  cols.push(`"jskillset"`);
  values.push(userJskillset);
  placeholders.push(`$${idx++}`);

  if (cols.length === 0) {
    return res.status(400).json({ error: 'No valid fields provided for create.' });
  }

  const sql = `INSERT INTO "process" (${cols.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`;

  try {
    const result = await pool.query(sql, values);
    const r = result.rows[0];

    // After insert, ensure canonical company persisted for consistency
    try {
      await ensureCanonicalFieldsForId(r.id, r.company || r.organisation, r.jobtitle || r.role, null);
    } catch (e) {
      console.warn('[POST_CANON] failed to persist canonical fields', e && e.message);
    }

    // Reload latest row to include persisted canonical fields
    const fresh = (await pool.query('SELECT * FROM "process" WHERE id = $1', [r.id])).rows[0];

    const mapped = {
      ...fresh,
      jobtitle: fresh.jobtitle ?? null,
      company: (normalizeCompanyName(fresh.company || fresh.organisation) ?? (fresh.company ?? null)),
      jobfamily: fresh.jobfamily ?? null,
      sourcingstatus: fresh.sourcingstatus ?? null,
      product: fresh.product ?? null,
      lskillset: fresh.lskillset ?? null,
      linkedinurl: fresh.linkedinurl ?? null,
      jskillset: fresh.jskillset ?? null,

      // candidate-style fallbacks
      role: fresh.role ?? fresh.jobtitle ?? null,
      organisation: (normalizeCompanyName(fresh.company || fresh.organisation) ?? (fresh.organisation ?? fresh.company ?? null)),
      job_family: fresh.job_family ?? fresh.jobfamily ?? null,
      sourcing_status: fresh.sourcing_status ?? fresh.sourcingstatus ?? null,
      type: fresh.product ?? null,
      compensation: fresh.compensation ?? null
    };

    // Emit creation event
    try {
      broadcastSSE('candidate_created', mapped);
      broadcastSSE('candidates_changed', { action: 'create', id: mapped.id });
    } catch (_) { /* ignore */ }

    res.status(201).json(mapped);
  } catch (err) {
    console.error('POST /candidates error', err);
    res.status(500).json({ error: 'Create failed', detail: err.message });
  }
});

/**
 * PUT /candidates/:id
 * Update a process row. Accepts either candidate-style keys (role, organisation, job_family, sourcing_status)
 * or process-style keys (jobtitle, company, jobfamily, sourcingstatus, product). Writes to process table.
 */
app.put('/candidates/:id', requireLogin, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (Number.isNaN(id)) return res.status(400).json({ error: 'Invalid id' });

  // Ownership guard
  const ownerOk = await ensureOwnershipOrFail(res, id, req.user.id);
  if (!ownerOk) return;

  const body = req.body || {};

  const fieldMap = {
    // candidate -> process
    role: 'jobtitle',
    organisation: 'company',
    job_family: 'jobfamily',
    sourcing_status: 'sourcingstatus',
    product: 'product',
    type: 'product', // MAP frontend "type" to backend "product"

    // process keys (pass-through)
    jobtitle: 'jobtitle',
    company: 'company',
    jobfamily: 'jobfamily',
    sourcingstatus: 'sourcingstatus',

    // same-name fields
    name: 'name',
    sector: 'sector',
    role_tag: 'role_tag',
    skillset: 'skillset',
    geographic: 'geographic',
    country: 'country',
    email: 'email',
    mobile: 'mobile',
    office: 'office',
    compensation: 'compensation',
    seniority: 'seniority',
    lskillset: 'lskillset',
    vskillset: 'vskillset',
    linkedinurl: 'linkedinurl',
    comment: 'comment',
    exp: 'exp',
    tenure: 'tenure',
    education: 'education'
  };

  const keys = Object.keys(body).filter(k => Object.prototype.hasOwnProperty.call(fieldMap, k));
  if (keys.length === 0) {
    return res.status(400).json({ error: 'No updatable fields provided.' });
  }

  try {
    // Build unique column -> value map to avoid assigning the same DB column twice
    const colValueMap = new Map();
    for (const k of keys) {
      const col = fieldMap[k];
      let v = body[k];
      if (k === 'seniority' && v != null && String(v).trim() !== '') {
        const std = standardizeSeniority(v);
        v = std || null;
      }
      if (k === 'compensation' && v != null && v !== '') {
        const n = Number(v);
        if (isNaN(n)) {
          return res.status(400).json({ error: 'Compensation must be a numeric value.' });
        }
        v = n;
      }
      colValueMap.set(col, v === '' ? null : v);
    }

    const cols = [];
    const values = [];
    let idx = 1;
    for (const [col, val] of colValueMap.entries()) {
      cols.push(`"${col}" = $${idx}`);
      values.push(val);
      idx++;
    }
    values.push(id);

    const sql = `UPDATE "process" SET ${cols.join(', ')} WHERE id = $${idx} RETURNING *`;

    const result = await pool.query(sql, values);
    if (result.rowCount === 0) return res.status(404).json({ error: 'Not found' });

    let r = result.rows[0];

    // Persist canonical company if needed after the update
    try {
      await ensureCanonicalFieldsForId(r.id, r.company || r.organisation, r.jobtitle || r.role, null);
    } catch (e) {
      console.warn('[PUT_CANON] failed to persist canonical fields', e && e.message);
    }

    // Reload to reflect any canonical updates
    r = (await pool.query('SELECT * FROM "process" WHERE id = $1', [r.id])).rows[0];

    // After reloading r from DB:
    const parsedVskillset = await parseAndPersistVskillset(r.id, r.vskillset);

    // Convert pic to a data URI (or URL) that the frontend can use directly
    const picBase64 = picToDataUri(r.pic);

    // Return row with both process-style and candidate-style fallback keys for frontend convenience
    const mapped = {
      ...r,
      // process-style explicit
      jobtitle: r.jobtitle ?? null,
      company: normalizeCompanyName(r.company || r.organisation) ?? (r.company ?? null),
      jobfamily: r.jobfamily ?? null,
      sourcingstatus: r.sourcingstatus ?? null,
      product: r.product ?? null,
      lskillset: r.lskillset ?? null,
      vskillset: parsedVskillset ?? null, // use parsed object (or null)
      pic: picBase64, // Convert bytea to base64 for frontend
      linkedinurl: r.linkedinurl ?? null,
      jskillset: r.jskillset ?? null,

      // candidate-style fallbacks
      role: r.role ?? r.jobtitle ?? null,
      organisation: normalizeCompanyName(r.company || r.organisation) ?? (r.organisation ?? r.company ?? null),
      job_family: r.job_family ?? r.jobfamily ?? null,
      sourcing_status: r.sourcing_status ?? r.sourcingstatus ?? null,
      type: r.product ?? null,
      compensation: r.compensation ?? null
    };

    // Emit candidate_updated via SSE if connections exist
    try {
      broadcastSSE('candidate_updated', mapped);
    } catch (e) {
      // ignore socket emit errors
    }

    res.json(mapped);
  } catch (err) {
    console.error('PUT /candidates/:id error', err);
    res.status(500).json({ error: 'Update failed', detail: err.message });
  }
});


// ── Second half of routes: loaded from server_routes2.js ─────────────────────
require('./server_routes2')(app, {
  pool,
  requireLogin,
  dashboardRateLimit,
  userRateLimit,
  withExponentialBackoff,
  llmGenerateText,
  incrementGeminiQueryCount,
  _writeApprovalLog,
  _writeInfraLog,
  _aiCompCacheGet,
  _aiCompCacheSet,
  _buildMLProfileData,
  getOrCreateTransporter,
  getSaveStatePath,
  loadEmailVerifConfig,
  loadRateLimits,
  loadSmtpConfig,
  normalizeCompanyName,
  normalizeCountry,
  picToDataUri,
  standardizeSeniority,
  ensureCanonicalFieldsForId,
  firstVal,
  google,
  allowedOrigins,
  ML_OUTPUT_DIR,
  SAVE_STATE_DIR,
  CRITERIA_DIR,
  ICS_URLS_PATH,
  EXTERNAL_API_TIMEOUT_MS,
  _SSE_HEARTBEAT_MS,
  _SSE_COALESCE_DELAY_MS,
  _PORTING_UPLOAD_MAX_BYTES,
  _SCHEDULER_DEFAULT_DURATION,
  _SCHEDULER_DEFAULT_MAX_SLOTS,
  _EMAIL_VERIF_CONFIG_PATHS,
  CONTACT_GEN_IN_EMAIL_VERIF,
});


// ── Job queue + LLM worker initialisation ────────────────────────────────────
// Initialised here so the worker shares the same llmGenerateText, pool, and
// normalizeCompanyName helpers as the rest of server.js.
// broadcastSSE / broadcastSSEBulk are set on the server_routes2 module.exports
// inside registerRoutes() (called synchronously above), so they are available now.
const { queueStats }             = require('./server/queue');
const { initLlmWorker, getLlmQueue } = require('./server/workers/llmWorker');

(function _initQueue() {
  const routes2 = require('./server_routes2');
  initLlmWorker({
    pool,
    llmGenerateText,
    incrementGeminiQueryCount,
    normalizeCompanyName,
    picToDataUri,
    broadcastSSE:     routes2._broadcastSSE     || null,
    broadcastSSEBulk: routes2._broadcastSSEBulk || null,
  });
})();

// Expose the shared LLM queue so other modules can enqueue jobs.
// Usage: require('./server').llmQueue.enqueue({ type: 'calc-unmatched', … })
module.exports.llmQueue = getLlmQueue();

// ── Monitoring: queue depth endpoint ─────────────────────────────────────────
// GET /api/queue-stats — returns per-queue depth and in-flight counts.
// Requires auth so it is not exposed publicly.
app.get('/api/queue-stats', dashboardRateLimit, requireLogin, (req, res) => {
  res.json({ queues: queueStats(), ts: new Date().toISOString() });
});

// Create HTTP server
const server = http.createServer(app);

// ── Global Express error handler ──────────────────────────────────────────────
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  const msg = (err && err.message) ? err.message : String(err);
  _writeErrorLog({ source: 'server.js', severity: 'critical', endpoint: req.path, message: msg });
  console.error('[ERROR]', req.path, msg);
  if (!res.headersSent) res.status(500).json({ error: 'Internal server error' });
});

// ── Process-level uncaught exception / rejection handlers ────────────────────
process.on('uncaughtException', (err) => {
  _writeErrorLog({ source: 'server.js', severity: 'critical', endpoint: '', message: String(err) });
  console.error('[UNCAUGHT EXCEPTION]', err);
});
process.on('unhandledRejection', (reason) => {
  _writeErrorLog({ source: 'server.js', severity: 'error', endpoint: '', message: String(reason) });
  console.error('[UNHANDLED REJECTION]', reason);
});

// START SERVER
server.listen(port, () => {
  console.log(`Backend running on port ${port}`);
});