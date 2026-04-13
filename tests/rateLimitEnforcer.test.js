/**
 * Candidate Analyser/backend/tests/rateLimitEnforcer.test.js
 *
 * Unit tests for rate-limit-enforcer.js (at repo root).
 *
 * How to run (from repo root):
 *   npm test
 *   # or: npx jest "Candidate Analyser/backend/tests/rateLimitEnforcer.test.js" --runInBand
 *
 * Required devDependencies (see package.json):
 *   jest@^29, jest-environment-jsdom@^29
 *
 * @jest-environment jsdom
 */

'use strict';

// ── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Load a fresh copy of rate-limit-enforcer.js into the jsdom `window` / `global`.
 *
 * The module is an IIFE that attaches `window.RateLimitEnforcer`. We use
 * `Function()` constructor to execute the source in the jsdom global scope
 * (where `window === global`), which is the correct test environment.
 * `vm.runInThisContext` cannot be used here because it runs in the Node.js
 * global context rather than the jsdom context set up by jest-environment-jsdom.
 */
function loadEnforcer() {
  // Clear any previously attached globals
  delete global.RateLimitEnforcer;
  delete global.__rleFetchPatched;

  const fs = require('fs');
  const path = require('path');
  const src = fs.readFileSync(path.resolve(__dirname, '../../../rate-limit-enforcer.js'), 'utf8');

  // Execute the IIFE in the jsdom global context. In the jsdom test environment,
  // `window === global`, so the IIFE attaches `RateLimitEnforcer` to the test global.
  // The Function constructor is used instead of bare eval() to confine the execution
  // to a function scope while still running in the current (jsdom) global context.
  // eslint-disable-next-line no-new-func
  new Function(src)(); // safe: trusted local file, not user input
  return global.RateLimitEnforcer;
}

/** Build a sample limits payload that /user/rate-limits would return. */
function makeLimitsPayload(overrides = {}) {
  return {
    ok: true,
    limits: Object.assign(
      { upload_multiple_cvs: { requests: 3, window_seconds: 10 } },
      overrides
    ),
  };
}

// ── Suite 1: check() returns true when under the limit ────────────────────────
describe('RateLimitEnforcer – check() under limit', () => {
  let RLE;

  beforeEach(() => {
    // Clean jsdom body
    document.body.innerHTML = '';
    // Clear sessionStorage
    sessionStorage.clear();

    // Mock fetch to return a sample limits response
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => makeLimitsPayload(),
      clone: function () { return this; },
      status: 200,
    });

    RLE = loadEnforcer();
    RLE.init({ prefetch: false });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('returns true for the first request (well under limit of 3)', async () => {
    const result = await RLE.check('upload_multiple_cvs');
    expect(result).toBe(true);
  });

  it('returns true for requests 1 and 2 (under limit of 3)', async () => {
    const r1 = await RLE.check('upload_multiple_cvs');
    const r2 = await RLE.check('upload_multiple_cvs');
    expect(r1).toBe(true);
    expect(r2).toBe(true);
  });

  it('returns true for exactly maxReq-1 consecutive calls', async () => {
    const results = [];
    for (let i = 0; i < 2; i++) {
      results.push(await RLE.check('upload_multiple_cvs'));
    }
    expect(results.every(Boolean)).toBe(true);
  });

  it('returns true for unknown feature (no config entry)', async () => {
    const result = await RLE.check('nonexistent_feature');
    expect(result).toBe(true);
  });
});

// ── Suite 2: check() returns false when hitting the limit ─────────────────────
describe('RateLimitEnforcer – check() at/over limit', () => {
  let RLE;

  beforeEach(() => {
    document.body.innerHTML = '';
    sessionStorage.clear();

    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => makeLimitsPayload(),
      clone: function () { return this; },
      status: 200,
    });

    RLE = loadEnforcer();
    RLE.init({ prefetch: false });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('returns false after exactly maxReq (3) calls', async () => {
    await RLE.check('upload_multiple_cvs'); // 1
    await RLE.check('upload_multiple_cvs'); // 2
    await RLE.check('upload_multiple_cvs'); // 3  ← uses last allowed slot
    const result = await RLE.check('upload_multiple_cvs'); // 4 → over limit
    expect(result).toBe(false);
  });

  it('creates #rle-overlay in the DOM when limit is reached', async () => {
    for (let i = 0; i < 3; i++) await RLE.check('upload_multiple_cvs');
    await RLE.check('upload_multiple_cvs'); // triggers overlay

    const overlay = document.getElementById('rle-overlay');
    expect(overlay).not.toBeNull();
  });

  it('#rle-overlay contains the feature label and limit details', async () => {
    for (let i = 0; i < 3; i++) await RLE.check('upload_multiple_cvs');
    await RLE.check('upload_multiple_cvs');

    const overlay = document.getElementById('rle-overlay');
    expect(overlay).not.toBeNull();
    expect(overlay.textContent).toContain('Bulk CV Upload');
    expect(overlay.textContent).toContain('3');
  });

  it('#rle-overlay contains the "Do not show again this session" checkbox', async () => {
    for (let i = 0; i < 3; i++) await RLE.check('upload_multiple_cvs');
    await RLE.check('upload_multiple_cvs');

    const chk = document.getElementById('rle-dismiss-chk');
    expect(chk).not.toBeNull();
    expect(chk.type).toBe('checkbox');

    const label = document.querySelector('label[for="rle-dismiss-chk"]');
    expect(label).not.toBeNull();
    expect(label.textContent).toMatch(/do not show again/i);
  });

  it('#rle-ok button is present in the overlay', async () => {
    for (let i = 0; i < 3; i++) await RLE.check('upload_multiple_cvs');
    await RLE.check('upload_multiple_cvs');

    const okBtn = document.getElementById('rle-ok');
    expect(okBtn).not.toBeNull();
  });

  it('does NOT show overlay when suppressed via sessionStorage', async () => {
    // Pre-seed suppression flag
    sessionStorage.setItem('rle_suppress_upload_multiple_cvs', '1');

    for (let i = 0; i < 3; i++) await RLE.check('upload_multiple_cvs');
    await RLE.check('upload_multiple_cvs');

    expect(document.getElementById('rle-overlay')).toBeNull();
  });
});

// ── Suite 3: refresh() reloads limits ──────────────────────────────────────────
describe('RateLimitEnforcer – refresh()', () => {
  let RLE;

  beforeEach(() => {
    document.body.innerHTML = '';
    sessionStorage.clear();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('reloads limits from server and uses new values', async () => {
    // First fetch: limit of 1
    global.fetch = jest.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => makeLimitsPayload({ upload_multiple_cvs: { requests: 1, window_seconds: 60 } }),
      clone: function () { return this; },
      status: 200,
    });

    RLE = loadEnforcer();
    RLE.init({ prefetch: false });

    // Exhaust the limit of 1
    await RLE.check('upload_multiple_cvs'); // 1st → true
    const blocked = await RLE.check('upload_multiple_cvs'); // over limit
    expect(blocked).toBe(false);

    // Now update the mock to return a higher limit
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => makeLimitsPayload({ upload_multiple_cvs: { requests: 10, window_seconds: 60 } }),
      clone: function () { return this; },
      status: 200,
    });

    await RLE.refresh();

    // After refresh, the window is reset and new limit is 10
    const result = await RLE.check('upload_multiple_cvs');
    expect(result).toBe(true);
  });

  it('fetch is called again after refresh()', async () => {
    const fetchMock = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => makeLimitsPayload(),
      clone: function () { return this; },
      status: 200,
    });
    global.fetch = fetchMock;

    RLE = loadEnforcer();
    RLE.init({ prefetch: false });
    await RLE.getLimits(); // first load

    const callsBefore = fetchMock.mock.calls.length;
    await RLE.refresh();
    expect(fetchMock.mock.calls.length).toBeGreaterThan(callsBefore);
  });
});

// ── Suite 4: getLimits() returns cached limits ─────────────────────────────────
describe('RateLimitEnforcer – getLimits()', () => {
  let RLE;

  beforeEach(() => {
    document.body.innerHTML = '';
    sessionStorage.clear();

    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => makeLimitsPayload(),
      clone: function () { return this; },
      status: 200,
    });

    RLE = loadEnforcer();
    RLE.init({ prefetch: false });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('returns an object with the expected feature key', async () => {
    const limits = await RLE.getLimits();
    expect(limits).toHaveProperty('upload_multiple_cvs');
  });

  it('reflects the mocked requests value', async () => {
    const limits = await RLE.getLimits();
    expect(limits.upload_multiple_cvs.requests).toBe(3);
  });

  it('reflects the mocked window_seconds value', async () => {
    const limits = await RLE.getLimits();
    expect(limits.upload_multiple_cvs.window_seconds).toBe(10);
  });

  it('returns the same cached object on repeated calls (no extra fetch)', async () => {
    // Keep a reference to the mock BEFORE init() installs the interceptor wrapper
    const fetchMock = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => makeLimitsPayload(),
      clone: function () { return this; },
      status: 200,
    });
    global.fetch = fetchMock;
    RLE = loadEnforcer();
    RLE.init({ prefetch: false });

    await RLE.getLimits();
    const callsAfterFirst = fetchMock.mock.calls.length;
    await RLE.getLimits();
    // Should NOT have made another fetch
    expect(fetchMock.mock.calls.length).toBe(callsAfterFirst);
  });

  it('handles a server error gracefully (returns empty object)', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      json: async () => ({}),
      clone: function () { return this; },
      status: 500,
    });
    RLE = loadEnforcer();
    RLE.init({ prefetch: false });
    const limits = await RLE.getLimits();
    expect(typeof limits).toBe('object');
    expect(limits).not.toBeNull();
  });

  it('handles a network error gracefully (returns empty object)', async () => {
    global.fetch = jest.fn().mockRejectedValue(new Error('network failure'));
    RLE = loadEnforcer();
    RLE.init({ prefetch: false });
    const limits = await RLE.getLimits();
    expect(typeof limits).toBe('object');
    expect(limits).not.toBeNull();
  });
});

// ── Suite 5: fetch interceptor – 429 with JSON body ───────────────────────────
describe('RateLimitEnforcer – fetch interceptor (429 with JSON)', () => {
  beforeEach(() => {
    document.body.innerHTML = '';
    sessionStorage.clear();
    // Each test sets its own mock and loads the enforcer
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('shows #rle-overlay on 429 with JSON body containing feature/requests/window_seconds', async () => {
    // Install the 429 mock BEFORE loading enforcer so the interceptor wraps it
    global.fetch = jest.fn().mockResolvedValue({
      status: 429,
      ok: false,
      clone() {
        return {
          json: async () => ({
            feature: 'upload_multiple_cvs',
            requests: 3,
            window_seconds: 10,
          }),
        };
      },
    });

    const RLE = loadEnforcer();
    RLE.init({ prefetch: false });

    // global.fetch is now the interceptor; calling it triggers overlay logic
    await global.fetch('/process/upload_multiple_cvs', { method: 'POST' });
    // Allow promise chain / microtasks to settle
    await new Promise(r => setTimeout(r, 50));

    const overlay = document.getElementById('rle-overlay');
    expect(overlay).not.toBeNull();
    expect(overlay.textContent).toContain('Bulk CV Upload');
  });

  it('shows #rle-overlay on 429 with plain-text body (generic popup)', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      status: 429,
      ok: false,
      clone() {
        return {
          json: async () => { throw new SyntaxError('not json'); },
        };
      },
    });

    const RLE = loadEnforcer();
    RLE.init({ prefetch: false });

    await global.fetch('/process/upload_multiple_cvs', { method: 'POST' });
    await new Promise(r => setTimeout(r, 50));

    const overlay = document.getElementById('rle-overlay');
    expect(overlay).not.toBeNull();
    expect(overlay.textContent).toMatch(/rate limit/i);
  });
});

// ── Suite 6: "Do not show again" suppression ──────────────────────────────────
describe('RateLimitEnforcer – session suppression', () => {
  let RLE;

  beforeEach(() => {
    document.body.innerHTML = '';
    sessionStorage.clear();

    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => makeLimitsPayload({ upload_multiple_cvs: { requests: 1, window_seconds: 60 } }),
      clone: function () { return this; },
      status: 200,
    });

    RLE = loadEnforcer();
    RLE.init({ prefetch: false });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('does not show overlay again after checkbox is checked and popup dismissed', async () => {
    // First trigger to show overlay
    await RLE.check('upload_multiple_cvs'); // allowed
    await RLE.check('upload_multiple_cvs'); // over limit → shows overlay

    const chk = document.getElementById('rle-dismiss-chk');
    const okBtn = document.getElementById('rle-ok');
    expect(chk).not.toBeNull();
    expect(okBtn).not.toBeNull();

    // Simulate checking the checkbox then clicking OK
    chk.checked = true;
    okBtn.click();

    // Overlay should be gone
    expect(document.getElementById('rle-overlay')).toBeNull();

    // Trigger again — should NOT show
    await RLE.check('upload_multiple_cvs');
    expect(document.getElementById('rle-overlay')).toBeNull();
  });

  it('sessionStorage key is set after dismissing with checkbox checked', async () => {
    await RLE.check('upload_multiple_cvs');
    await RLE.check('upload_multiple_cvs');

    const chk = document.getElementById('rle-dismiss-chk');
    const okBtn = document.getElementById('rle-ok');
    chk.checked = true;
    okBtn.click();

    expect(sessionStorage.getItem('rle_suppress_upload_multiple_cvs')).toBe('1');
  });
});
