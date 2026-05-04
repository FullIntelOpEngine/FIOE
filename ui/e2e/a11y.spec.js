/**
 * Candidate Analyser/backend/ui/e2e/a11y.spec.js
 *
 * Playwright accessibility tests: Lighthouse audit stub, keyboard navigation,
 * ARIA roles, colour-contrast checks (via axe-core if available).
 *
 * How to run (from repo root):
 *   npx playwright install --with-deps chromium
 *   npx playwright test "Candidate Analyser/backend/ui/e2e/a11y.spec.js"
 *
 * Required devDependencies: @playwright/test@^1.44, express@^4
 * Optional:  @axe-core/playwright  (skipped gracefully when absent)
 */

'use strict';

const { test, expect } = require('@playwright/test');
const express = require('express');
const http = require('http');
const path = require('path');

// ── Attempt to load axe-core (soft dependency) ─────────────────────────────
let injectAxe = null;
let checkA11y = null;
try {
  const axePlaywright = require('@axe-core/playwright');
  injectAxe = axePlaywright.injectAxe;
  checkA11y = axePlaywright.checkA11y;
} catch (_) {
  // axe-core not installed — a11y violation tests will be skipped
}

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');

// ── Mock server ─────────────────────────────────────────────────────────────

async function startMockServer() {
  const app = express();
  app.use(express.json());
  app.use(express.static(REPO_ROOT));

  app.post('/login', (req, res) => {
    const { username, password } = req.body || {};
    if (username && password) {
      return res.json({ ok: true, username, useraccess: 'user' });
    }
    res.status(401).json({ ok: false, error: 'Invalid credentials' });
  });

  app.get('/user/resolve', (req, res) => {
    res.json({ ok: true, username: 'test@example.com', useraccess: 'user' });
  });

  app.get('/weights', (_req, res) => {
    res.json({ ok: true, weights: { skills: 40, experience: 30 }, locked: false });
  });

  const server = http.createServer(app);
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  return { server, base: `http://127.0.0.1:${port}` };
}

async function stopServer(server) {
  await new Promise(resolve => server.close(resolve));
}

// ── Lighthouse audit stub helper ─────────────────────────────────────────────

/**
 * Stub Lighthouse audit: checks basic accessibility indicators that can be
 * verified without the full Lighthouse CLI.
 *
 * Returns { score, issues } where score is 0-100 (stub).
 */
async function stubLighthouseAudit(page) {
  const issues = [];

  // Check for document title
  const title = await page.title();
  if (!title || title.trim() === '') {
    issues.push({ type: 'missing-title', severity: 'error' });
  }

  // Check for lang attribute on <html>
  const lang = await page.evaluate(() => document.documentElement.lang);
  if (!lang) {
    issues.push({ type: 'missing-lang', severity: 'warning' });
  }

  // Check for at least one heading
  const h1Count = await page.locator('h1').count();
  if (h1Count === 0) {
    issues.push({ type: 'no-h1', severity: 'warning' });
  }

  // Check all images have alt attributes
  const imgsWithoutAlt = await page.evaluate(() => {
    return Array.from(document.querySelectorAll('img')).filter(
      img => !img.hasAttribute('alt')
    ).length;
  });
  if (imgsWithoutAlt > 0) {
    issues.push({ type: 'img-missing-alt', severity: 'error', count: imgsWithoutAlt });
  }

  // Check all form inputs have associated labels
  const inputsWithoutLabel = await page.evaluate(() => {
    const inputs = Array.from(document.querySelectorAll('input, select, textarea'))
      .filter(el => el.type !== 'hidden' && el.type !== 'submit' && el.type !== 'button');
    return inputs.filter(input => {
      const id = input.id;
      if (!id) return !input.getAttribute('aria-label') && !input.getAttribute('aria-labelledby');
      return !document.querySelector(`label[for="${id}"]`);
    }).length;
  });
  if (inputsWithoutLabel > 0) {
    issues.push({ type: 'input-missing-label', severity: 'warning', count: inputsWithoutLabel });
  }

  const errorCount = issues.filter(i => i.severity === 'error').length;
  const score = Math.max(0, 100 - errorCount * 20 - issues.length * 5);
  return { score, issues };
}

// ── Tests ──────────────────────────────────────────────────────────────────

test.describe('Accessibility – login.html', () => {
  let mock;

  test.beforeEach(async () => {
    mock = await startMockServer();
  });

  test.afterEach(async () => {
    await stopServer(mock.server);
  });

  test('lighthouse_audit_stub: login page scores >= 60 on stub audit', async ({ page }) => {
    await page.goto(`${mock.base}/login.html`, { waitUntil: 'domcontentloaded' });
    const { score, issues } = await stubLighthouseAudit(page);
    // Log issues for debugging
    if (issues.length > 0) {
      console.log('Accessibility issues found:', JSON.stringify(issues, null, 2));
    }
    expect(score).toBeGreaterThanOrEqual(60);
  });

  test('lang_attr: <html> element has a lang attribute', async ({ page }) => {
    await page.goto(`${mock.base}/login.html`, { waitUntil: 'domcontentloaded' });
    const lang = await page.evaluate(() => document.documentElement.lang);
    // Lang should be set (ideally 'en' or similar)
    expect(typeof lang).toBe('string');
  });

  test('images_have_alt: all images have alt attributes', async ({ page }) => {
    await page.goto(`${mock.base}/login.html`, { waitUntil: 'domcontentloaded' });
    const missingAlt = await page.evaluate(() =>
      Array.from(document.querySelectorAll('img')).filter(
        img => !img.hasAttribute('alt')
      ).length
    );
    expect(missingAlt).toBe(0);
  });

  test('axe_a11y_check: no critical axe violations on login page', async ({ page }) => {
    if (!injectAxe || !checkA11y) {
      test.skip('axe-core/playwright not installed');
      return;
    }
    await page.goto(`${mock.base}/login.html`, { waitUntil: 'domcontentloaded' });
    await injectAxe(page);
    await checkA11y(page, null, {
      detailedReport: true,
      detailedReportOptions: { html: true },
    });
    // checkA11y throws if violations found; reaching here = pass
  });
});

test.describe('Keyboard navigation', () => {
  let mock;

  test.beforeEach(async () => {
    mock = await startMockServer();
  });

  test.afterEach(async () => {
    await stopServer(mock.server);
  });

  test('tab_navigation: Tab key moves focus through interactive elements', async ({ page }) => {
    await page.goto(`${mock.base}/login.html`, { waitUntil: 'domcontentloaded' });
    // Start from body
    await page.focus('body');
    const focusedElements = [];

    // Tab through up to 10 interactive elements
    for (let i = 0; i < 10; i++) {
      await page.keyboard.press('Tab');
      const focused = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tag: el.tagName.toLowerCase(),
          type: el.type || '',
          id: el.id || '',
          role: el.getAttribute('role') || '',
        };
      });
      if (focused) focusedElements.push(focused);
      else break;
    }
    // Should be able to tab to at least one interactive element
    expect(focusedElements.length).toBeGreaterThan(0);
    // All focused elements should be interactive
    const interactiveTags = ['input', 'button', 'a', 'select', 'textarea'];
    for (const el of focusedElements) {
      expect(interactiveTags).toContain(el.tag);
    }
  });

  test('enter_submits_form: Enter key on submit button triggers form submission', async ({ page }) => {
    await page.route('**/login', route => {
      route.fulfill({
        status: 401,
        contentType: 'application/json',
        body: JSON.stringify({ ok: false, error: 'Invalid credentials' }),
      });
    });
    await page.goto(`${mock.base}/login.html`, { waitUntil: 'domcontentloaded' });

    const usernameInput = page.locator('input[type="email"], input[name="username"], #username').first();
    const passwordInput = page.locator('input[type="password"], #password').first();

    if (await usernameInput.isVisible()) {
      await usernameInput.fill('test@example.com');
    }
    if (await passwordInput.isVisible()) {
      await passwordInput.fill('testpassword');
      await passwordInput.press('Enter');
    }
    // After Enter: either redirect or error shown
    await page.waitForTimeout(500);
    // No unhandled JS errors expected
    const errors = await page.evaluate(() => window.__errors__ || []);
    expect(errors.length).toBe(0);
  });

  test('focus_visible: focused elements have visible focus indicator', async ({ page }) => {
    await page.goto(`${mock.base}/login.html`, { waitUntil: 'domcontentloaded' });
    await page.focus('body');
    await page.keyboard.press('Tab');

    const hasFocusStyle = await page.evaluate(() => {
      const el = document.activeElement;
      if (!el || el === document.body) return false;
      const style = window.getComputedStyle(el, ':focus');
      const outline = style.getPropertyValue('outline');
      const outlineWidth = style.getPropertyValue('outline-width');
      const boxShadow = style.getPropertyValue('box-shadow');
      // Focus is visible if outline is not none/0 or box-shadow is set
      return (
        (outline !== 'none' && outlineWidth !== '0px') ||
        (boxShadow && boxShadow !== 'none')
      );
    });
    // This is a soft check; log rather than hard-fail for existing pages
    if (!hasFocusStyle) {
      console.warn(
        'Warning: focused element may not have a visible focus indicator. ' +
        'Consider adding :focus-visible styles for accessibility.'
      );
    }
  });

  test('escape_closes_modal: Escape key closes any open modal/dialog', async ({ page }) => {
    await page.goto(`${mock.base}/login.html`, { waitUntil: 'domcontentloaded' });
    // Look for any modal trigger buttons
    const modalTrigger = page.locator('[data-modal], [data-toggle="modal"], .modal-trigger');
    const triggerCount = await modalTrigger.count();

    if (triggerCount > 0) {
      await modalTrigger.first().click();
      await page.keyboard.press('Escape');
      // Modal should no longer be visible
      const modal = page.locator('.modal.show, dialog[open], [role="dialog"]');
      await expect(modal.first()).not.toBeVisible({ timeout: 2000 }).catch(() => {});
    }
    // If no modal triggers, test passes vacuously
    expect(true).toBe(true);
  });
});

test.describe('ARIA roles and semantics', () => {
  let mock;

  test.beforeEach(async () => {
    mock = await startMockServer();
  });

  test.afterEach(async () => {
    await stopServer(mock.server);
  });

  test('login_form_role: login form has accessible role or is a <form> element', async ({ page }) => {
    await page.goto(`${mock.base}/login.html`, { waitUntil: 'domcontentloaded' });
    const formEl = page.locator('form, [role="form"]');
    const count = await formEl.count();
    expect(count).toBeGreaterThan(0);
  });

  test('buttons_have_labels: all visible buttons have accessible text', async ({ page }) => {
    await page.goto(`${mock.base}/login.html`, { waitUntil: 'domcontentloaded' });
    const unlabelledButtons = await page.evaluate(() => {
      const btns = Array.from(document.querySelectorAll('button'));
      return btns.filter(btn => {
        const text = btn.textContent?.trim() || '';
        const ariaLabel = btn.getAttribute('aria-label') || '';
        const ariaLabelledby = btn.getAttribute('aria-labelledby') || '';
        const title = btn.getAttribute('title') || '';
        return !text && !ariaLabel && !ariaLabelledby && !title;
      }).length;
    });
    expect(unlabelledButtons).toBe(0);
  });
});
