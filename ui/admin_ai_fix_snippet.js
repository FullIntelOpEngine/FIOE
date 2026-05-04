/**
 * admin_ai_fix_snippet.js
 * Client-side module for the AI Autofix feature in the admin panel.
 *
 * Usage (add to admin_rate_limits.html):
 *   <script src="ui/admin_ai_fix_snippet.js"></script>
 *   <script>
 *     AdminAiFix.init({ apiBase: '/' });
 *   </script>
 *
 * Then to trigger from the Error Capture panel:
 *   AdminAiFix.show(geminiAnalysisObject);
 *   // geminiAnalysisObject = { error_message, source, explanation, suggested_fix, copilot_prompt }
 */
(function (global) {
  'use strict';

  // ── CSS ────────────────────────────────────────────────────────────────────
  const CSS = `
#aiFix-panel {
  position: fixed; top: 0; left: 0; width: 100%; height: 100%;
  z-index: 9900; background: rgba(0,0,0,.70);
  display: flex; align-items: center; justify-content: center;
}
#aiFix-box {
  background: #181c22; color: #d8d8d8; position: relative;
  width: min(96vw, 900px); max-height: 90vh; overflow-y: auto;
  border-radius: 14px; border: 1px solid rgba(109,234,249,.20);
  box-shadow: 0 24px 80px rgba(0,0,0,.65);
  padding: 28px 32px;
  font-family: "Roboto Condensed", system-ui, sans-serif;
}
#aiFix-box h2 {
  margin: 0 0 18px; font-size: 16px; font-weight: 700;
  font-family: "Orbitron", sans-serif; color: #6deaf9; letter-spacing: .5px;
}
#aiFix-close {
  position: absolute; top: 14px; right: 18px;
  background: none; border: none; color: #87888a; font-size: 22px;
  cursor: pointer; line-height: 1; transition: color .15s;
}
#aiFix-close:hover { color: #d8d8d8; }
.af-section { margin-bottom: 18px; }
.af-label {
  font-size: 10px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .8px; color: #87888a; margin-bottom: 6px;
}
.af-diff {
  background: #0d1117; color: #c9d1d9; font-family: "Courier New", monospace;
  font-size: 12px; border-radius: 8px; padding: 14px;
  overflow: auto; white-space: pre;
  border: 1px solid rgba(109,234,249,.10); max-height: 280px;
}
.af-diff .add { color: #3fb950; }
.af-diff .del { color: #f85149; }
.af-diff .hdr { color: #6deaf9; }
.af-prose {
  background: #1a2030; border-radius: 8px; padding: 12px;
  font-size: 13px; line-height: 1.65; color: #c8d0e0;
  border: 1px solid rgba(255,255,255,.06);
}
.af-risk-badge {
  display: inline-block; padding: 3px 12px; border-radius: 20px;
  font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .5px;
}
.af-risk-low    { background: rgba(63,185,80,.15);  color: #3fb950; border: 1px solid rgba(63,185,80,.3); }
.af-risk-medium { background: rgba(210,153,34,.15); color: #d2991e; border: 1px solid rgba(210,153,34,.3); }
.af-risk-high   { background: rgba(248,81,73,.15);  color: #f85149; border: 1px solid rgba(248,81,73,.3); }
.af-actions { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 22px; }
.af-btn {
  padding: 9px 20px; border: none; border-radius: 8px; cursor: pointer;
  font-size: 13px; font-weight: 600; font-family: "Roboto Condensed", sans-serif;
  transition: opacity .15s, box-shadow .15s;
}
.af-btn:disabled { opacity: .5; cursor: not-allowed; }
.af-btn:not(:disabled):hover { opacity: .88; box-shadow: 0 4px 14px rgba(0,0,0,.25); }
.af-btn-pr     { background: linear-gradient(180deg,#073679,#4c82b8); color: #fff; }
.af-btn-apply  { background: linear-gradient(180deg,#1a6b2a,#3fb950); color: #fff; }
.af-btn-dl     { background: linear-gradient(180deg,#5c3500,#c07000); color: #fff; }
.af-btn-cancel { background: rgba(255,255,255,.08); color: #d8d8d8; border: 1px solid rgba(255,255,255,.12); }
.af-dl-note { font-size: 11px; color: #87888a; margin-top: 6px; }
.af-status { margin-top: 14px; font-size: 13px; padding: 8px 12px; border-radius: 6px; }
.af-status.ok  { background: rgba(63,185,80,.12);  color: #3fb950; border: 1px solid rgba(63,185,80,.25); }
.af-status.err { background: rgba(248,81,73,.12);  color: #f85149; border: 1px solid rgba(248,81,73,.25); }
.af-generating { text-align: center; padding: 48px 0; color: #6deaf9; }
.af-spinner {
  width: 38px; height: 38px;
  border: 3px solid rgba(109,234,249,.2); border-top-color: #6deaf9;
  border-radius: 50%; animation: af-spin .8s linear infinite;
  margin: 0 auto 18px;
}
@keyframes af-spin { to { transform: rotate(360deg); } }
`;

  let _cfg = {};
  let _panel = null;
  let _currentFix = null;

  function _injectCSS() {
    if (document.getElementById('af-styles')) return;
    const s = document.createElement('style');
    s.id = 'af-styles';
    s.textContent = CSS;
    document.head.appendChild(s);
  }

  function _esc(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  function _colorDiff(diff) {
    return diff.split('\n').map(line => {
      if (line.startsWith('---') || line.startsWith('+++'))
        return `<span class="hdr">${_esc(line)}</span>`;
      if (line.startsWith('+'))
        return `<span class="add">${_esc(line)}</span>`;
      if (line.startsWith('-'))
        return `<span class="del">${_esc(line)}</span>`;
      if (line.startsWith('@'))
        return `<span class="hdr">${_esc(line)}</span>`;
      return _esc(line);
    }).join('\n');
  }

  function _buildPanelEl() {
    const el = document.createElement('div');
    el.id = 'aiFix-panel';
    document.body.appendChild(el);
    return el;
  }

  function _renderLoading() {
    _panel.innerHTML = `
<div id="aiFix-box">
  <button id="aiFix-close" title="Close">✕</button>
  <h2>🤖 AI Autofix</h2>
  <div class="af-generating">
    <div class="af-spinner"></div>
    Asking Vertex AI to generate a safe code fix…
  </div>
</div>`;
    document.getElementById('aiFix-close').onclick = close;
  }

  function _renderFix(fix, statusMsg, statusClass) {
    const riskClass = `af-risk-${(fix.risk || 'low').toLowerCase()}`;
    const diffHtml  = fix.diff
      ? `<pre class="af-diff">${_colorDiff(fix.diff)}</pre>`
      : `<pre class="af-diff" style="color:#87888a">No diff produced.</pre>`;

    const testsHtml = fix.tests
      ? `<pre class="af-diff">${_esc(fix.tests)}</pre>`
      : `<pre class="af-diff" style="color:#87888a">No tests produced.</pre>`;

    const filesHtml = (fix.files_changed || []).length
      ? (fix.files_changed).map(_esc).join('<br>')
      : '—';

    const applyBtn = _cfg.enableApplyHost
      ? `<button class="af-btn af-btn-apply" id="af-apply-btn">⚡ Approve &amp; Apply to Host</button>`
      : '';

    // Download buttons — one per impacted file (original is NOT replaced on server)
    const dlBtns = (fix.files_changed || []).length && fix.diff
      ? (fix.files_changed).map((fname, idx) =>
          `<button class="af-btn af-btn-dl" data-af-dl="${idx}" title="Download corrected version of ${_esc(fname)} (original not replaced)">` +
          `\u2B07 Download ${_esc(fname)} (corrected)</button>`
        ).join('')
      : '';

    const statusHtml = statusMsg
      ? `<div class="af-status ${statusClass}">${_esc(statusMsg)}</div>`
      : '';

    _panel.innerHTML = `
<div id="aiFix-box">
  <button id="aiFix-close" title="Close">\u2715</button>
  <h2>\uD83E\uDD16 AI Autofix \u2014 Proposed Code Change</h2>

  <div class="af-section">
    <div class="af-label">Risk</div>
    <span class="af-risk-badge ${riskClass}">${_esc(fix.risk || 'unknown')}</span>
    <span style="margin-left:10px;font-size:13px;color:#87888a">${_esc(fix.risk_reason || '')}</span>
  </div>

  <div class="af-section">
    <div class="af-label">Rationale</div>
    <div class="af-prose">${_esc(fix.rationale || '')}</div>
  </div>

  <div class="af-section">
    <div class="af-label">Unified Diff</div>
    ${diffHtml}
  </div>

  <div class="af-section">
    <div class="af-label">Generated Tests</div>
    ${testsHtml}
  </div>

  <div class="af-section">
    <div class="af-label">Files Changed</div>
    <div class="af-prose" style="font-family:monospace;font-size:12px">${filesHtml}</div>
  </div>

  <div class="af-actions">
    <button class="af-btn af-btn-pr" id="af-pr-btn">\uD83D\uDCE5 Create PR for Review</button>
    ${applyBtn}
    ${dlBtns}
    <button class="af-btn af-btn-cancel" id="af-cancel-btn">Cancel</button>
  </div>
  ${dlBtns ? '<p class="af-dl-note">\u2B07 Download buttons produce a corrected copy for review. The original file on the server is <strong>not</strong> modified.</p>' : ''}
  ${statusHtml}
</div>`;

    document.getElementById('aiFix-close').onclick = close;
    document.getElementById('af-cancel-btn').onclick = close;
    document.getElementById('af-pr-btn').onclick = () => _createPR(fix);
    if (_cfg.enableApplyHost) {
      document.getElementById('af-apply-btn').onclick = () => _applyHost(fix);
    }
    // Wire download buttons
    _panel.querySelectorAll('[data-af-dl]').forEach(function(btn) {
      var idx = parseInt(btn.dataset.afDl, 10);
      var files = fix.files_changed || [];
      if (Number.isFinite(idx) && idx >= 0 && idx < files.length) {
        var fname = files[idx];
        btn.onclick = function() { _downloadCorrected(fname, fix.diff, btn); };
      }
    });
  }

  // ── API call helpers ───────────────────────────────────────────────────────

  function _authHeaders() {
    const h = {
      'Content-Type': 'application/json',
      'X-Requested-With': 'XMLHttpRequest',
    };
    if (_cfg.adminToken) h['Authorization'] = `Bearer ${_cfg.adminToken}`;
    return h;
  }

  async function _createPR(fix) {
    const btn = document.getElementById('af-pr-btn');
    btn.disabled = true;
    btn.textContent = '⏳ Creating PR…';
    try {
      const resp = await fetch(`${_cfg.apiBase}admin/ai-fix/create-pr`, {
        method:      'POST',
        headers:     _authHeaders(),
        credentials: 'same-origin',
        body:        JSON.stringify({ fix }),
      });
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error || resp.statusText);
      _renderFix(fix, `✅ PR created: ${data.pr_url}`, 'ok');
      if (data.pr_url) window.open(data.pr_url, '_blank');
    } catch (err) {
      _renderFix(fix, '❌ PR creation failed: ' + err.message, 'err');
    } finally {
      const b = document.getElementById('af-pr-btn');
      if (b) { b.disabled = false; b.textContent = '📥 Create PR for Review'; }
    }
  }

  async function _applyHost(fix) {
    // Two-step confirmation: require the admin to type 'CONFIRM' before applying
    const answer = prompt(
      '⚠️ This will apply the patch DIRECTLY to the live server.\n\n' +
      'Type CONFIRM (all caps) to proceed or Cancel to abort.'
    );
    if (answer !== 'CONFIRM') return;
    const btn = document.getElementById('af-apply-btn');
    btn.disabled = true;
    btn.textContent = '⏳ Applying…';
    try {
      const resp = await fetch(`${_cfg.apiBase}admin/ai-fix/apply-host`, {
        method:      'POST',
        headers:     _authHeaders(),
        credentials: 'same-origin',
        body:        JSON.stringify({
          diff:          fix.diff,
          files_changed: fix.files_changed,
          build_docker:  _cfg.buildDocker || false,
          push_image:    _cfg.pushImage   || false,
        }),
      });
      const data = await resp.json();
      if (!resp.ok || !data.ok) throw new Error(data.error || data.stderr || 'Apply failed');
      _renderFix(fix, '✅ Patch applied successfully to host.', 'ok');
    } catch (err) {
      _renderFix(fix, '❌ Apply failed: ' + err.message, 'err');
    } finally {
      const b = document.getElementById('af-apply-btn');
      if (b) { b.disabled = false; b.textContent = '⚡ Approve & Apply to Host'; }
    }
  }

  // ── Public API ─────────────────────────────────────────────────────────────

  /** Return a download filename like 'webbridge_corrected.py' from a server path. */
  function _correctedFilename(serverPath) {
    var base = serverPath.replace(/\\/g, '/').split('/').pop();
    var dot  = base.lastIndexOf('.');
    var stem = dot > 0 ? base.slice(0, dot) : base;
    var ext  = dot > 0 ? base.slice(dot)    : '';
    return stem + '_corrected' + ext;
  }

  /**
   * Download a corrected copy of a source file.
   * Calls POST /admin/ai-fix/corrected-file on the server, which applies the
   * unified diff and streams back the patched content.  The original file on
   * the server is NOT modified.
   *
   * @param {string}      filename  e.g. 'webbridge.py'
   * @param {string}      diff      unified diff string
   * @param {HTMLElement} btn       button element (disabled while downloading)
   */
  async function _downloadCorrected(filename, diff, btn) {
    var origLabel = btn ? btn.textContent : '';
    if (btn) { btn.disabled = true; btn.textContent = '\u23F3 Preparing download\u2026'; }
    try {
      var resp = await fetch(_cfg.apiBase + 'admin/ai-fix/corrected-file', {
        method:      'POST',
        headers:     _authHeaders(),
        credentials: 'same-origin',
        body:        JSON.stringify({ filename: filename, diff: diff }),
      });

      if (!resp.ok) {
        // Try to parse JSON error
        var errData;
        try { errData = await resp.json(); } catch (_) { errData = {}; }
        var msg = (errData && errData.error) ? errData.error : resp.statusText;
        if (resp.status === 403) msg = '\u26D4 Access denied (HTTP 403). Ensure you are logged in as an admin.';
        if (resp.status === 401) msg = '\uD83D\uDD10 Not authenticated (HTTP 401). Please log in as an admin.';
        alert('\u274C Download failed: ' + msg);
        return;
      }

      // Derive filename from Content-Disposition or fall back to <name>_corrected<ext>
      var cd = resp.headers.get('Content-Disposition') || '';
      var fnMatch = cd.match(/filename="([^"]+)"/);
      var downloadName = fnMatch ? fnMatch[1] : _correctedFilename(filename);

      var blob = await resp.blob();
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url;
      a.download = downloadName;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err) {
      alert('\u274C Download error: ' + err.message);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = origLabel; }
    }
  }

  /**
   * Initialize the module once.
   * @param {object}  config
   * @param {string}  config.apiBase            API base URL (default: '/')
   * @param {string}  [config.adminToken]        Bearer token (leave blank when using session cookies)
   * @param {boolean} [config.enableApplyHost]   Show "Apply to Host" button (default: false — safer)
   * @param {boolean} [config.buildDocker]       Request Docker rebuild on apply (default: false)
   * @param {boolean} [config.pushImage]         Push image after build (default: false)
   */
  function init(config) {
    _cfg = {
      apiBase:         '/',
      adminToken:      '',
      enableApplyHost: false,
      buildDocker:     false,
      pushImage:       false,
      ...config,
    };
    if (!_cfg.apiBase.endsWith('/')) _cfg.apiBase += '/';
    _injectCSS();
  }

  /**
   * Display the AI Fix panel, call /admin/ai-fix/generate, then show the result.
   * @param {object} geminiAnalysis  { error_message, source, explanation, suggested_fix, copilot_prompt }
   */
  async function show(geminiAnalysis) {
    _injectCSS();
    if (!_panel) _panel = _buildPanelEl();
    _panel.style.display = 'flex';
    _renderLoading();

    try {
      const resp = await fetch(`${_cfg.apiBase}admin/ai-fix/generate`, {
        method:      'POST',
        headers:     _authHeaders(),
        credentials: 'same-origin',
        body:        JSON.stringify({ gemini_analysis: geminiAnalysis }),
      });
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error || resp.statusText);
      _currentFix = data.fix;
      _renderFix(_currentFix, '', '');
    } catch (err) {
      if (_panel) {
        _panel.innerHTML = `
<div id="aiFix-box">
  <button id="aiFix-close" title="Close">✕</button>
  <h2>🤖 AI Autofix</h2>
  <div class="af-status err" style="margin-top:24px">❌ ${_esc(err.message)}</div>
  <div class="af-actions" style="margin-top:18px">
    <button class="af-btn af-btn-cancel" id="af-cancel-btn">Close</button>
  </div>
</div>`;
        document.getElementById('aiFix-close').onclick = close;
        document.getElementById('af-cancel-btn').onclick = close;
      }
    }
  }

  /** Close the panel without taking action. */
  function close() {
    if (_panel) _panel.style.display = 'none';
    _currentFix = null;
  }

  global.AdminAiFix = { init, show, close };
}(window));