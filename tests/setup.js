/**
 * tests/setup.js
 * Jest global setup shims for jsdom environment.
 *
 * Provides a minimal sessionStorage stub and clears module state
 * between test files.
 */

// jsdom includes localStorage/sessionStorage but guard anyway
if (typeof global.sessionStorage === 'undefined') {
  const _store = {};
  global.sessionStorage = {
    getItem: k => _store[k] ?? null,
    setItem: (k, v) => { _store[k] = String(v); },
    removeItem: k => { delete _store[k]; },
    clear: () => { Object.keys(_store).forEach(k => delete _store[k]); },
  };
}
