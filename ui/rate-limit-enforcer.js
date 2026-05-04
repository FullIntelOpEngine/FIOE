/**
 * rate-limit-enforcer.js
 * Client-side rate-limit enforcement helper.
 *
 * Usage:
 *   <script src="ui/rate-limit-enforcer.js"></script>
 *   <script>
 *     // Initialise once (fetches /user/rate-limits).
 *     RateLimitEnforcer.init();
 *
 *     // Before a rate-limited action, call check().
 *     // Returns true if allowed, shows a pop-up and returns false when the
 *     // user has hit the limit.
 *     if (!await RateLimitEnforcer.check('upload_multiple_cvs')) return;
 *     // … proceed with the action …
 *   </script>
 *
 * The message shown in the pop-up is dynamically derived from the admin-set
 * quantity and window, so it automatically reflects any override saved in the
 * admin panel.
 *
 * Per-user overrides (if any) take precedence over global defaults; the
 * module is transparent to this — it simply uses whatever /user/rate-limits
 * returns.
 */
(function (global) {
  'use strict';

  // ── CSS ──────────────────────────────────────────────────────────────────
  const CSS = `
#rle-overlay {
  position: fixed; inset: 0; z-index: 99000;
  background: rgba(0,0,0,.55);
  display: flex; align-items: center; justify-content: center;
}
#rle-box {
  background: #fff; color: #1a1a2e;
  width: min(94vw, 480px);
  border-radius: 14px;
  box-shadow: 0 20px 60px rgba(0,0,0,.35);
  padding: 28px 28px 22px;
  font-family: system-ui, "Segoe UI", sans-serif;
  position: relative;
}
#rle-box h3 {
  margin: 0 0 10px;
  font-size: 17px; font-weight: 700; color: #b91c1c;
  display: flex; align-items: center; gap: 8px;
}
#rle-box p  { margin: 0 0 16px; font-size: 14px; line-height: 1.6; color: #374151; }
#rle-close  {
  position: absolute; top: 12px; right: 16px;
  background: none; border: none; font-size: 20px;
  color: #6b7280; cursor: pointer; line-height: 1;
}
#rle-close:hover { color: #1a1a2e; }
#rle-dismiss-row {
  display: flex; align-items: center; gap: 8px;
  margin: 0 0 16px;
}
#rle-dismiss-chk {
  width: 15px; height: 15px; cursor: pointer; flex-shrink: 0;
  accent-color: #b91c1c;
}
#rle-dismiss-row label {
  font-size: 13px; color: #6b7280; cursor: pointer; user-select: none;
}
#rle-ok {
  display: block; width: 100%; padding: 10px;
  background: #b91c1c; color: #fff; border: none;
  border-radius: 8px; font-size: 14px; font-weight: 600;
  cursor: pointer; text-align: center;
}
#rle-ok:hover { background: #991b1b; }
`;

  // ── State ────────────────────────────────────────────────────────────────
  let _limits = null;         // fetched from /user/rate-limits
  let _fetchPromise = null;   // deduplicate concurrent fetches
  const _counters = {};       // feature -> [timestamp, …]

  // ── Helpers ───────────────────────────────────────────────────────────────
  function _injectCSS() {
    if (document.getElementById('rle-styles')) return;
    const s = document.createElement('style');
    s.id = 'rle-styles';
    s.textContent = CSS;
    document.head.appendChild(s);
  }

  function _featureLabel(feature) {
    const MAP = {
      upload_multiple_cvs: 'Bulk CV Upload',
      upload_cv:           'CV Upload',
      gemini:              'Gemini AI',
      vskillset_infer:     'Skill Inference (AI)',
      start_job:           'Start Sourcing Job',
      candidates:          'Candidates List',
      bulk_delete:         'Bulk Delete',
      login:               'Login',
      register:            'Register',
      geography:           'Geography Lookup',
    };
    return MAP[feature] || feature;
  }

  // ── Session-level suppression (per feature) ──────────────────────────────
  function _suppressKey(feature) { return 'rle_suppress_' + (feature || 'generic'); }
  function _isSuppressed(feature) {
    try { return !!sessionStorage.getItem(_suppressKey(feature)); } catch (_) { return false; }
  }
  function _setSuppressed(feature) {
    try { sessionStorage.setItem(_suppressKey(feature), '1'); } catch (_) {}
  }

  async function _fetchLimits() {
    if (_limits) return _limits;
    if (_fetchPromise) return _fetchPromise;
    _fetchPromise = fetch('/user/rate-limits', { credentials: 'same-origin' })
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        _limits = (data && data.ok) ? (data.limits || {}) : {};
        _fetchPromise = null;
        return _limits;
      })
      .catch(() => {
        _limits = {};
        _fetchPromise = null;
        return _limits;
      });
    return _fetchPromise;
  }

  function _showPopup(feature, maxReq, windowSec) {
    if (_isSuppressed(feature)) return;
    _injectCSS();
    const existing = document.getElementById('rle-overlay');
    if (existing) existing.remove();

    const label   = _featureLabel(feature);
    const winTxt  = (windowSec % 3600 === 0 && windowSec >= 3600)
      ? `${windowSec / 3600} hour${windowSec / 3600 !== 1 ? 's' : ''}`
      : (windowSec % 60 === 0 && windowSec >= 60)
        ? `${windowSec / 60} minute${windowSec / 60 !== 1 ? 's' : ''}`
        : `${windowSec} second${windowSec !== 1 ? 's' : ''}`;

    const overlay = document.createElement('div');
    overlay.id = 'rle-overlay';
    overlay.innerHTML = `
<div id="rle-box" role="alertdialog" aria-modal="true" aria-labelledby="rle-title">
  <button id="rle-close" aria-label="Close">✕</button>
  <h3 id="rle-title">⚠️ Rate Limit Reached</h3>
  <p>
    <strong>${label}</strong> requests are limited to
    <strong>${maxReq}</strong> per <strong>${winTxt}</strong>.<br><br>
    You have used all ${maxReq} allowed requests in this window.
    Please wait for <strong>${winTxt}</strong> before trying again.
  </p>
  <div id="rle-dismiss-row">
    <input type="checkbox" id="rle-dismiss-chk">
    <label for="rle-dismiss-chk">Do not show again this session</label>
  </div>
  <button id="rle-ok">OK, I Understand</button>
</div>`;
    document.body.appendChild(overlay);

    function _close() {
      if (document.getElementById('rle-dismiss-chk') && document.getElementById('rle-dismiss-chk').checked) {
        _setSuppressed(feature);
      }
      overlay.remove();
    }
    document.getElementById('rle-close').onclick = _close;
    document.getElementById('rle-ok').onclick = _close;
    overlay.addEventListener('click', e => { if (e.target === overlay) _close(); });
  }

  // ── Public API ────────────────────────────────────────────────────────────

  /**
   * Initialise the module.  Optionally pass `{ prefetch: true }` to start
   * fetching rate limits immediately (default: lazy).
   *
   * Always installs the global fetch interceptor so 429 responses from the
   * server are caught and surfaced as a pop-up automatically.
   */
  function init(opts) {
    opts = opts || {};
    _installFetchInterceptor();
    if (opts.prefetch) _fetchLimits();
  }

  /**
   * Check whether the current user is allowed to make a request for `feature`.
   *
   * - Returns `true`  → proceed normally.
   * - Returns `false` → user has hit the client-side limit; a pop-up is shown.
   *
   * The check is purely client-side and mirrors the server-side sliding-window
   * logic.  The server will still enforce the real limit; this provides an
   * early, friendly warning.
   *
   * @param {string} feature  Rate-limit feature key (e.g. 'upload_multiple_cvs')
   * @returns {Promise<boolean>}
   */
  async function check(feature) {
    const limits = await _fetchLimits();
    const cfg = limits[feature];
    if (!cfg) return true; // no config → not limited

    const maxReq    = parseInt(cfg.requests, 10) || 999999;
    const windowSec = parseInt(cfg.window_seconds, 10) || 60;
    const windowMs  = windowSec * 1000;
    const now = Date.now();

    const history = (_counters[feature] || []).filter(t => now - t < windowMs);
    _counters[feature] = history;

    if (history.length >= maxReq) {
      _showPopup(feature, maxReq, windowSec);
      return false;
    }

    history.push(now);
    _counters[feature] = history;
    return true;
  }

  /**
   * Force-refresh the cached rate limits from the server.
   * @returns {Promise<object>}
   */
  function refresh() {
    _limits = null;
    return _fetchLimits();
  }

  /**
   * Install a global fetch interceptor that catches 429 responses from the
   * server and shows the rate-limit pop-up automatically — even when the
   * request was not pre-checked via RateLimitEnforcer.check().
   *
   * This covers:
   *  • Server-side per-user rate limits (webbridge.py / server.js)
   *  • Flask-Limiter IP-based limits (process_geography, etc.)
   *
   * The response body is cloned before reading so the original response is
   * not consumed (callers can still read res.json() normally).
   */
  function _installFetchInterceptor() {
    if (global.__rleFetchPatched) return;
    global.__rleFetchPatched = true;
    const _origFetch = global.fetch;
    global.fetch = async function (...args) {
      const res = await _origFetch.apply(this, args);
      if (res.status === 429) {
        // Clone so the original response body can still be read by callers
        res.clone().json().then(body => {
          const feature    = (body && body.feature) || _guessFeatureFromUrl(args[0]);
          const maxReq     = (body && body.requests) ? parseInt(body.requests, 10) : null;
          const windowSec  = (body && body.window_seconds) ? parseInt(body.window_seconds, 10) : null;
          if (maxReq && windowSec) {
            _showPopup(feature, maxReq, windowSec);
          } else if (feature) {
            // Fallback: look up cached limits, or show generic message
            const cfg = (_limits || {})[feature];
            if (cfg) {
              _showPopup(feature, parseInt(cfg.requests, 10), parseInt(cfg.window_seconds, 10));
            } else {
              _showPopupGeneric(feature);
            }
          } else {
            _showPopupGeneric('');
          }
        }).catch(() => {
          // Body was not JSON (e.g. flask-limiter plain-text 429) — show generic
          const feature = _guessFeatureFromUrl(args[0]);
          _showPopupGeneric(feature);
        });
      }
      return res;
    };
  }

  /** Derive a feature name from a request URL string. */
  function _guessFeatureFromUrl(url) {
    if (!url) return '';
    const s = String(url);
    if (s.includes('upload_multiple_cvs')) return 'upload_multiple_cvs';
    if (s.includes('upload_cv'))           return 'upload_cv';
    if (s.includes('start_job'))           return 'start_job';
    if (s.includes('candidates/bulk'))     return 'candidates';
    if (s.includes('/candidates'))         return 'candidates';
    if (s.includes('gemini'))              return 'gemini';
    if (s.includes('vskillset'))           return 'vskillset_infer';
    if (s.includes('process/geography'))   return 'geography';
    return '';
  }

  /** Show a generic rate-limit pop-up when we don't have limit numbers. */
  function _showPopupGeneric(feature) {
    if (_isSuppressed(feature)) return;
    _injectCSS();
    const existing = document.getElementById('rle-overlay');
    if (existing) existing.remove();
    const label = feature ? _featureLabel(feature) : 'This action';
    // Try to get window_seconds from cached limits for a more accurate message
    const _cfg = (_limits || {})[feature];
    let _waitTxt = 'before trying again';
    if (_cfg && _cfg.window_seconds) {
      const _ws = parseInt(_cfg.window_seconds, 10);
      const _wt = (_ws % 3600 === 0 && _ws >= 3600)
        ? `${_ws / 3600} hour${_ws / 3600 !== 1 ? 's' : ''}`
        : (_ws % 60 === 0 && _ws >= 60)
          ? `${_ws / 60} minute${_ws / 60 !== 1 ? 's' : ''}`
          : `${_ws} second${_ws !== 1 ? 's' : ''}`;
      _waitTxt = `for <strong>${_wt}</strong> before trying again`;
    }
    const overlay = document.createElement('div');
    overlay.id = 'rle-overlay';
    overlay.innerHTML = `
<div id="rle-box" role="alertdialog" aria-modal="true" aria-labelledby="rle-title">
  <button id="rle-close" aria-label="Close">✕</button>
  <h3 id="rle-title">⚠️ Rate Limit Reached</h3>
  <p>
    <strong>${label}</strong> — you have exceeded the allowed request rate.<br><br>
    Please wait ${_waitTxt}.
  </p>
  <div id="rle-dismiss-row">
    <input type="checkbox" id="rle-dismiss-chk">
    <label for="rle-dismiss-chk">Do not show again this session</label>
  </div>
  <button id="rle-ok">OK, I Understand</button>
</div>`;
    document.body.appendChild(overlay);
    function _close() {
      if (document.getElementById('rle-dismiss-chk') && document.getElementById('rle-dismiss-chk').checked) {
        _setSuppressed(feature);
      }
      overlay.remove();
    }
    document.getElementById('rle-close').onclick = _close;
    document.getElementById('rle-ok').onclick     = _close;
    overlay.addEventListener('click', e => { if (e.target === overlay) _close(); });
  }

  /**
   * Return the cached limits object (fetch from server if not yet loaded).
   * @returns {Promise<object>}  e.g. { upload_multiple_cvs: { requests: 10, window_seconds: 60 }, … }
   */
  async function getLimits() {
    return _fetchLimits();
  }

  global.RateLimitEnforcer = { init, check, refresh, getLimits };
}(window));