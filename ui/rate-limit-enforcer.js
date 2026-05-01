/**
 * rate-limit-enforcer.js
 *
 * Client-side guard that mirrors the server-side per-user rate limits
 * configured in rate_limits.json.  It fetches effective limits from
 * /user/rate-limits and tracks client-side request timestamps in a
 * sliding window so repeated submissions are rejected before hitting
 * the server.
 *
 * API
 * ---
 * RateLimitEnforcer.init(opts?)         - initialise; opts.prefetch=true fetches
 *                                         limits in the background immediately.
 * RateLimitEnforcer.check(feature)      - Promise<boolean>  true = allowed
 * RateLimitEnforcer.getLimits()         - Promise<object>   full limits response
 */
(function (global) {
  'use strict';

  var _limitsCache   = null;   // cached result from /user/rate-limits
  var _cacheExpiry   = 0;      // epoch-ms when cache expires
  var _CACHE_TTL_MS  = 60000;  // refresh every 60 s

  // Per-feature sliding-window history: { feature: [timestamp, ...] }
  var _history = {};

  /**
   * Fetch (or return cached) rate limits from the server.
   * Returns the parsed JSON response object or null on error.
   */
  function _fetchLimits() {
    var now = Date.now();
    if (_limitsCache && now < _cacheExpiry) {
      return Promise.resolve(_limitsCache);
    }
    return fetch('/user/rate-limits', {
      method: 'GET',
      credentials: 'same-origin',
      cache: 'no-store'
    })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (data) {
        if (data) {
          _limitsCache  = data;
          _cacheExpiry  = Date.now() + _CACHE_TTL_MS;
        }
        return data;
      })
      .catch(function () { return null; });
  }

  /**
   * init(opts) — optional pre-flight.
   * opts.prefetch = true  →  kick off a background fetch immediately so the
   *                          first call to check() is instant.
   */
  function init(opts) {
    opts = opts || {};
    if (opts.prefetch) {
      _fetchLimits().catch(function () {});
    }
  }

  /**
   * getLimits() — Promise<object|null>
   * Resolves with the full /user/rate-limits response.
   */
  function getLimits() {
    return _fetchLimits();
  }

  /**
   * check(feature) — Promise<boolean>
   * Returns true if the request is within the configured rate limit window,
   * false if the client-side budget has been exhausted.
   *
   * Falls back to true (allow) whenever limits cannot be loaded so that a
   * missing or unreachable /user/rate-limits endpoint never blocks the UI.
   */
  function check(feature) {
    return _fetchLimits().then(function (data) {
      if (!data || !data.limits) {
        // No limit info → allow
        return true;
      }
      var cfg = data.limits[feature];
      if (!cfg || !cfg.requests || !cfg.window_seconds) {
        // Feature not configured → allow
        return true;
      }
      var maxReq    = cfg.requests;
      var windowMs  = cfg.window_seconds * 1000;
      var now       = Date.now();
      var hist      = _history[feature] || [];

      // Evict timestamps outside the window
      hist = hist.filter(function (ts) { return now - ts < windowMs; });

      if (hist.length >= maxReq) {
        _history[feature] = hist;
        return false; // rate-limited
      }

      // Record this request
      hist.push(now);
      _history[feature] = hist;
      return true;
    }).catch(function () {
      // Network or parse error → allow (server will enforce)
      return true;
    });
  }

  global.RateLimitEnforcer = { init: init, getLimits: getLimits, check: check };

}(typeof window !== 'undefined' ? window : this));
