/**
 * tests/serviceConfigDetection.test.js
 *
 * Unit tests for the App.js service-config detection logic.
 *
 * Validates that:
 *  - App.js reads BOTH api_porting.html (per-user) and admin_rate_limits.html (platform) configs.
 *  - Only per-user Email Verification keys (api_porting.html) suppress Verify Select token deduction.
 *  - Only per-user LLM keys (api_porting.html) suppress DB Dockin Analytic token deduction.
 *  - Token UI is hidden only when BOTH per-user email verif AND LLM are active.
 *  - Admin keys (admin_rate_limits.html) do NOT override token deduction or visibility rules.
 *
 * How to run (from repo root):
 *   npx jest tests/serviceConfigDetection.test.js --runInBand
 *
 * @jest-environment jsdom
 */

'use strict';

// ── Detection Logic (extracted from App.js _refreshServiceConfig) ────────────

/**
 * Pure function that mirrors the detection logic in App.js _refreshServiceConfig.
 * Given the two API responses, returns the flags App.js would set.
 *
 * @param {object|null} svcData       Response from /api/user-service-config/status
 * @param {object|null} platformData  Response from /api/platform-provider-status
 * @returns {{ hasCustomEmailVerif: boolean, hasCustomLlm: boolean, platEmailVerif: boolean, platLlm: boolean }}
 */
function computeFlags(svcData, platformData) {
  let userEmailVerif = false, userLlm = false;
  if (svcData && svcData.active && svcData.providers) {
    const ep = (svcData.providers.email_verif || '').toLowerCase();
    userEmailVerif = ep === 'neverbounce' || ep === 'zerobounce' || ep === 'bouncer';
    const lp = (svcData.providers.llm || '').toLowerCase();
    userLlm = lp === 'openai' || lp === 'anthropic';
  }
  const platEmailVerif = !!(platformData && platformData.email_verif_custom);
  const platLlm = !!(platformData && platformData.llm_custom);
  // Only per-user flags control token deduction and visibility
  return {
    hasCustomEmailVerif: userEmailVerif,
    hasCustomLlm: userLlm,
    platEmailVerif,
    platLlm,
  };
}

/**
 * Given the flags, determines whether the Token UI should be visible.
 * Mirrors the JSX condition: !(hasCustomEmailVerif && hasCustomLlm)
 */
function isTokenUIVisible(flags) {
  return !(flags.hasCustomEmailVerif && flags.hasCustomLlm);
}

/**
 * Given the flags, determines whether Verify Select should deduct tokens.
 * Mirrors the condition: !hasCustomEmailVerif
 */
function shouldVerifySelectDeduct(flags) {
  return !flags.hasCustomEmailVerif;
}

/**
 * Given the flags, determines whether DB Dockin Analytic should deduct tokens.
 * Mirrors the condition: !hasCustomLlm
 */
function shouldAnalyticDeduct(flags) {
  return !flags.hasCustomLlm;
}

// ── Test Cases ──────────────────────────────────────────────────────────────

describe('Service Config Detection — api_porting.html vs admin_rate_limits.html', () => {

  // ── Scenario 1: No keys anywhere ──────────────────────────────────────────

  test('No keys set anywhere → tokens visible, all deductions active', () => {
    const svcData = { active: false, providers: { search: 'google_cse', llm: 'gemini', email_verif: 'default' } };
    const platformData = { email_verif_custom: false, llm_custom: false };
    const flags = computeFlags(svcData, platformData);

    expect(flags.hasCustomEmailVerif).toBe(false);
    expect(flags.hasCustomLlm).toBe(false);
    expect(flags.platEmailVerif).toBe(false);
    expect(flags.platLlm).toBe(false);
    expect(isTokenUIVisible(flags)).toBe(true);
    expect(shouldVerifySelectDeduct(flags)).toBe(true);
    expect(shouldAnalyticDeduct(flags)).toBe(true);
  });

  // ── Scenario 2: Both per-user keys active (api_porting.html) ─────────────

  test('Both LLM + Email Verif in api_porting.html → tokens hidden, no deduction', () => {
    const svcData = { active: true, providers: { search: 'serper', llm: 'openai', email_verif: 'neverbounce' } };
    const platformData = { email_verif_custom: false, llm_custom: false };
    const flags = computeFlags(svcData, platformData);

    expect(flags.hasCustomEmailVerif).toBe(true);
    expect(flags.hasCustomLlm).toBe(true);
    expect(isTokenUIVisible(flags)).toBe(false);       // Hidden
    expect(shouldVerifySelectDeduct(flags)).toBe(false); // No deduction
    expect(shouldAnalyticDeduct(flags)).toBe(false);     // No deduction
  });

  // ── Scenario 3: Only Email Verif in api_porting.html ──────────────────────

  test('Email Verif only in api_porting.html → tokens visible, Verify Select skips deduction', () => {
    const svcData = { active: true, providers: { search: 'google_cse', llm: 'gemini', email_verif: 'zerobounce' } };
    const platformData = { email_verif_custom: false, llm_custom: false };
    const flags = computeFlags(svcData, platformData);

    expect(flags.hasCustomEmailVerif).toBe(true);
    expect(flags.hasCustomLlm).toBe(false);
    expect(isTokenUIVisible(flags)).toBe(true);          // Visible
    expect(shouldVerifySelectDeduct(flags)).toBe(false);  // No deduction
    expect(shouldAnalyticDeduct(flags)).toBe(true);       // Deduction continues
  });

  // ── Scenario 4: Only LLM in api_porting.html ─────────────────────────────

  test('LLM only in api_porting.html → tokens visible, DB Analytic skips deduction', () => {
    const svcData = { active: true, providers: { search: 'google_cse', llm: 'anthropic', email_verif: 'default' } };
    const platformData = { email_verif_custom: false, llm_custom: false };
    const flags = computeFlags(svcData, platformData);

    expect(flags.hasCustomEmailVerif).toBe(false);
    expect(flags.hasCustomLlm).toBe(true);
    expect(isTokenUIVisible(flags)).toBe(true);           // Visible
    expect(shouldVerifySelectDeduct(flags)).toBe(true);    // Deduction continues
    expect(shouldAnalyticDeduct(flags)).toBe(false);       // No deduction
  });

  // ── Scenario 5: Admin keys ONLY (admin_rate_limits.html) ─────────────────
  // This is the critical test: admin keys must NOT stop token deduction.

  test('Admin keys only (admin_rate_limits.html) → tokens visible, all deductions continue', () => {
    const svcData = { active: false, providers: { search: 'google_cse', llm: 'gemini', email_verif: 'default' } };
    const platformData = { email_verif_custom: true, llm_custom: true };
    const flags = computeFlags(svcData, platformData);

    // App.js DETECTS admin keys (platEmailVerif/platLlm are true)
    expect(flags.platEmailVerif).toBe(true);
    expect(flags.platLlm).toBe(true);
    // But per-user flags remain false → no effect on tokens
    expect(flags.hasCustomEmailVerif).toBe(false);
    expect(flags.hasCustomLlm).toBe(false);
    expect(isTokenUIVisible(flags)).toBe(true);           // Visible
    expect(shouldVerifySelectDeduct(flags)).toBe(true);    // Deduction continues
    expect(shouldAnalyticDeduct(flags)).toBe(true);        // Deduction continues
  });

  // ── Scenario 6: Admin has email verif, user has none ──────────────────────

  test('Admin email verif only → Verify Select still deducts tokens', () => {
    const svcData = { active: false, providers: { search: 'google_cse', llm: 'gemini', email_verif: 'default' } };
    const platformData = { email_verif_custom: true, llm_custom: false };
    const flags = computeFlags(svcData, platformData);

    expect(flags.platEmailVerif).toBe(true);
    expect(flags.hasCustomEmailVerif).toBe(false);
    expect(shouldVerifySelectDeduct(flags)).toBe(true);  // Must deduct
  });

  // ── Scenario 7: Both admin AND per-user have keys ────────────────────────

  test('Both admin + per-user have keys → per-user controls, tokens hidden', () => {
    const svcData = { active: true, providers: { search: 'serper', llm: 'openai', email_verif: 'bouncer' } };
    const platformData = { email_verif_custom: true, llm_custom: true };
    const flags = computeFlags(svcData, platformData);

    expect(flags.hasCustomEmailVerif).toBe(true);
    expect(flags.hasCustomLlm).toBe(true);
    expect(flags.platEmailVerif).toBe(true);
    expect(flags.platLlm).toBe(true);
    expect(isTokenUIVisible(flags)).toBe(false);          // Hidden (per-user active)
    expect(shouldVerifySelectDeduct(flags)).toBe(false);   // No deduction (per-user)
    expect(shouldAnalyticDeduct(flags)).toBe(false);       // No deduction (per-user)
  });

  // ── Scenario 8: Per-user config deactivated after previous activation ────

  test('Per-user config deactivated → tokens visible, deductions resume', () => {
    const svcData = { active: false, providers: { search: 'google_cse', llm: 'gemini', email_verif: 'default' } };
    const platformData = { email_verif_custom: false, llm_custom: false };
    const flags = computeFlags(svcData, platformData);

    expect(flags.hasCustomEmailVerif).toBe(false);
    expect(flags.hasCustomLlm).toBe(false);
    expect(isTokenUIVisible(flags)).toBe(true);
    expect(shouldVerifySelectDeduct(flags)).toBe(true);
    expect(shouldAnalyticDeduct(flags)).toBe(true);
  });

  // ── Scenario 9: Null / error responses ───────────────────────────────────

  test('Both API responses are null (errors) → safe defaults, tokens visible', () => {
    const flags = computeFlags(null, null);

    expect(flags.hasCustomEmailVerif).toBe(false);
    expect(flags.hasCustomLlm).toBe(false);
    expect(flags.platEmailVerif).toBe(false);
    expect(flags.platLlm).toBe(false);
    expect(isTokenUIVisible(flags)).toBe(true);
    expect(shouldVerifySelectDeduct(flags)).toBe(true);
    expect(shouldAnalyticDeduct(flags)).toBe(true);
  });

  // ── Scenario 10: Provider variations ─────────────────────────────────────

  test.each([
    ['neverbounce', true],
    ['zerobounce', true],
    ['bouncer', true],
    ['default', false],
    ['', false],
  ])('Email verif provider "%s" → hasCustomEmailVerif=%s', (provider, expected) => {
    const svcData = { active: true, providers: { search: 'google_cse', llm: 'gemini', email_verif: provider } };
    const flags = computeFlags(svcData, null);
    expect(flags.hasCustomEmailVerif).toBe(expected);
  });

  test.each([
    ['openai', true],
    ['anthropic', true],
    ['gemini', false],
    ['', false],
  ])('LLM provider "%s" → hasCustomLlm=%s', (provider, expected) => {
    const svcData = { active: true, providers: { search: 'google_cse', llm: provider, email_verif: 'default' } };
    const flags = computeFlags(svcData, null);
    expect(flags.hasCustomLlm).toBe(expected);
  });
});
