/**
 * rate-limit-enforcer.js
 *
 * Client-side rate-limit enforcement that mirrors the admin-configured limits
 * stored in rate_limits.json and managed via admin_rate_limits.html.
 *
 * Exposes window.RateLimitEnforcer with:
 *   init({ prefetch })  – installs fetch interceptor; pre-loads limits when prefetch:true
 *   check(feature)      – async; returns true (allowed) or false (blocked + shows overlay)
 *   getLimits()         – async; returns cached per-feature limits object
 *   refresh()           – async; clears cache and re-fetches from /user/rate-limits
 *
 * Usage in LookerDashboard.html:
 *   RateLimitEnforcer.init({ prefetch: true });
 *   if (!(await RateLimitEnforcer.check('candidates'))) return;
 */

/* global sessionStorage, document */

(function (win) {
  'use strict';

  // ── Human-readable labels for each rate-limit feature key ─────────────────
  var FEATURE_LABELS = {
    admin_endpoints:        'Admin Endpoints',
    analytic_batch_size:    'Analytic Batch Size',
    analytic_cv_limit:      'Analytic CV Limit',
    bulk_assess:            'Bulk Assess',
    bulk_assess_status:     'Bulk Assess Status',
    bulk_delete:            'Bulk Delete',
    candidates:             'Candidates',
    dashboard:              'Dashboard',
    gemini:                 'Gemini AI',
    geography:              'Geography',
    highlight_talent_pools: 'Talent Pools',
    login:                  'Login',
    preview_target:         'Preview Target',
    register:               'Register',
    start_job:              'Start Job',
    upload_cv:              'CV Upload',
    upload_multiple_cvs:    'Bulk CV Upload',
    vskillset_infer:        'Skillset Inference',
  };

  // ── Module state ────────────────────────────────────────────────────────────
  var _limits      = null;  // cached { feature: { requests, window_seconds }, ... }
  var _fetchProm   = null;  // in-flight /user/rate-limits promise
  var _windows     = {};    // per-feature sliding window: { resetAt, count }

  // ── Internal helpers ────────────────────────────────────────────────────────

  function _suppressKey(feature) {
    return 'rle_suppress_' + feature;
  }

  function _isSuppressed(feature) {
    try {
      return (typeof sessionStorage !== 'undefined') &&
             sessionStorage.getItem(_suppressKey(feature)) === '1';
    } catch (_) { return false; }
  }

  function _suppress(feature) {
    try {
      if (typeof sessionStorage !== 'undefined') {
        sessionStorage.setItem(_suppressKey(feature), '1');
      }
    } catch (_) { /* ignore */ }
  }

  /** Build and inject the rate-limit overlay into document.body. */
  function _showOverlay(feature, requests, windowSeconds) {
    var existing = document.getElementById('rle-overlay');
    if (existing) existing.remove();

    var label = FEATURE_LABELS[feature] || feature;

    var overlay = document.createElement('div');
    overlay.id = 'rle-overlay';
    overlay.style.cssText = [
      'position:fixed', 'inset:0', 'z-index:99999',
      'background:rgba(0,0,0,0.55)', 'display:flex',
      'align-items:center', 'justify-content:center',
    ].join(';');

    var box = document.createElement('div');
    box.style.cssText = [
      'background:#1e1e2e', 'color:#e0e0e0', 'border-radius:10px',
      'padding:28px 32px', 'max-width:420px', 'width:90%',
      'box-shadow:0 8px 32px rgba(0,0,0,0.6)',
      'font-family:Inter,system-ui,sans-serif',
    ].join(';');

    var title = document.createElement('h3');
    title.style.cssText = 'margin:0 0 10px;font-size:17px;color:#e88;';
    title.textContent = 'Rate Limit Reached';

    var msg = document.createElement('p');
    msg.style.cssText = 'margin:0 0 16px;font-size:14px;line-height:1.5;';
    if (requests != null) {
      msg.textContent = label + ': limit of ' + requests +
        ' request' + (requests !== 1 ? 's' : '') + ' per ' + windowSeconds + 's reached.';
    } else {
      msg.textContent = 'Rate limit reached. Please slow down and try again.';
    }

    var chkWrap = document.createElement('div');
    chkWrap.style.cssText = 'display:flex;align-items:center;gap:8px;margin-bottom:18px;';

    var chk = document.createElement('input');
    chk.type = 'checkbox';
    chk.id = 'rle-dismiss-chk';

    var lbl = document.createElement('label');
    lbl.htmlFor = 'rle-dismiss-chk';
    lbl.style.cssText = 'font-size:13px;cursor:pointer;';
    lbl.textContent = 'Do not show again this session';

    chkWrap.appendChild(chk);
    chkWrap.appendChild(lbl);

    var okBtn = document.createElement('button');
    okBtn.id = 'rle-ok';
    okBtn.textContent = 'OK';
    okBtn.style.cssText = [
      'padding:8px 24px', 'background:#6c63ff', 'color:#fff',
      'border:none', 'border-radius:6px', 'font-size:14px',
      'cursor:pointer',
    ].join(';');

    okBtn.addEventListener('click', function () {
      if (chk.checked) _suppress(feature);
      overlay.remove();
    });

    box.appendChild(title);
    box.appendChild(msg);
    box.appendChild(chkWrap);
    box.appendChild(okBtn);
    overlay.appendChild(box);
    document.body.appendChild(overlay);
  }

  /** Generic overlay used when a server 429 body cannot be parsed as JSON. */
  function _showGenericOverlay() {
    var existing = document.getElementById('rle-overlay');
    if (existing) existing.remove();

    var overlay = document.createElement('div');
    overlay.id = 'rle-overlay';
    overlay.style.cssText = [
      'position:fixed', 'inset:0', 'z-index:99999',
      'background:rgba(0,0,0,0.55)', 'display:flex',
      'align-items:center', 'justify-content:center',
    ].join(';');

    var box = document.createElement('div');
    box.style.cssText = [
      'background:#1e1e2e', 'color:#e0e0e0', 'border-radius:10px',
      'padding:28px 32px', 'max-width:420px', 'width:90%',
      'box-shadow:0 8px 32px rgba(0,0,0,0.6)',
      'font-family:Inter,system-ui,sans-serif',
    ].join(';');

    var title = document.createElement('h3');
    title.style.cssText = 'margin:0 0 10px;font-size:17px;color:#e88;';
    title.textContent = 'Rate Limit Reached';

    var msg = document.createElement('p');
    msg.style.cssText = 'margin:0 0 18px;font-size:14px;line-height:1.5;';
    msg.textContent = 'Server rate limit reached. Please wait a moment and try again.';

    var okBtn = document.createElement('button');
    okBtn.id = 'rle-ok';
    okBtn.textContent = 'OK';
    okBtn.style.cssText = [
      'padding:8px 24px', 'background:#6c63ff', 'color:#fff',
      'border:none', 'border-radius:6px', 'font-size:14px',
      'cursor:pointer',
    ].join(';');
    okBtn.addEventListener('click', function () { overlay.remove(); });

    box.appendChild(title);
    box.appendChild(msg);
    box.appendChild(okBtn);
    overlay.appendChild(box);
    document.body.appendChild(overlay);
  }

  // ── Core API ─────────────────────────────────────────────────────────────────

  /**
   * Fetch effective rate limits for the current user from the server.
   * Results are cached in _limits until refresh() is called.
   */
  function getLimits() {
    if (_limits !== null) return Promise.resolve(_limits);
    if (_fetchProm)       return _fetchProm;

    _fetchProm = fetch('/user/rate-limits', { credentials: 'include' })
      .then(function (res) {
        if (!res.ok) return {};
        return res.json();
      })
      .then(function (body) {
        var limits = (body && body.ok && body.limits) ? body.limits : {};
        _limits    = limits;
        _fetchProm = null;
        return limits;
      })
      .catch(function () {
        _limits    = {};
        _fetchProm = null;
        return {};
      });

    return _fetchProm;
  }

  /**
   * Re-fetch limits from the server (called when admin saves changes).
   * Clears cached limits and all per-feature window state.
   */
  function refresh() {
    _limits    = null;
    _fetchProm = null;
    _windows   = {};
    return getLimits();
  }

  /**
   * Check whether the current user is within the rate limit for `feature`.
   * Returns a Promise<boolean>: true = allowed, false = blocked.
   * When blocked and the overlay is not suppressed, shows a modal.
   */
  function check(feature) {
    return getLimits().then(function (limits) {
      var cfg = limits[feature];
      if (!cfg) return true;

      var maxReq    = parseInt(cfg.requests, 10);
      if (!maxReq || maxReq <= 0) return true;

      var windowMs  = (parseInt(cfg.window_seconds, 10) || 60) * 1000;
      var now       = Date.now();
      var entry     = _windows[feature];

      if (!entry || now >= entry.resetAt) {
        entry = { count: 0, resetAt: now + windowMs };
      }
      entry.count++;
      _windows[feature] = entry;

      if (entry.count > maxReq) {
        if (!_isSuppressed(feature)) {
          _showOverlay(feature, maxReq, cfg.window_seconds);
        }
        return false;
      }
      return true;
    });
  }

  /**
   * Wrap the global fetch so every 429 response surfaces an overlay.
   * Called once by init(); guards against double-patching.
   */
  function _installFetchInterceptor() {
    if (typeof fetch !== 'function' || win.__rleFetchPatched) return;
    var _orig = fetch; // capture before replacement
    win.fetch = function () {
      var args = arguments;
      return _orig.apply(this, args).then(function (res) {
        if (res.status === 429) {
          var clone = res.clone();
          clone.json().then(function (body) {
            if (body && body.feature) {
              _showOverlay(body.feature, body.requests, body.window_seconds);
            } else {
              _showGenericOverlay();
            }
          }).catch(function () {
            _showGenericOverlay();
          });
        }
        return res;
      });
    };
    win.__rleFetchPatched = true;
  }

  /**
   * Initialise the enforcer.
   * @param {Object} [opts]
   * @param {boolean} [opts.prefetch=false]  Pre-load limits in the background.
   */
  function init(opts) {
    var options = opts || {};
    _installFetchInterceptor();
    if (options.prefetch) getLimits();
  }

  // ── Attach to window ─────────────────────────────────────────────────────────
  win.RateLimitEnforcer = { init: init, check: check, getLimits: getLimits, refresh: refresh };

}(typeof window !== 'undefined' ? window : global));
