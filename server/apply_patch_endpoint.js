'use strict';
/**
 * apply_patch_endpoint.js
 * Utility module that executes scripts/apply_patch_build.sh with a given
 * unified diff.  The Express endpoint is registered in server.js — this
 * module only handles the shell-out logic so it can be tested independently.
 *
 * Exported function:
 *   runApplyPatch(patchContent, options) → Promise<{ ok, exit_code, stdout, stderr }>
 */

const path  = require('path');
const fs    = require('fs');
const os    = require('os');
const { spawn } = require('child_process');

const PROJECT_ROOT = path.resolve(__dirname, '..');
const SCRIPT_PATH  = path.join(PROJECT_ROOT, 'scripts', 'apply_patch_build.sh');

// ── Path allowlist / blocklist ─────────────────────────────────────────────────

/** Relative paths / prefixes that are permitted to be patched. */
const ALLOWED_PATCH_ROOTS = [
  'server.js',
  'webbridge.py',
  'webbridge_cv.py',
  'App.js',
  'admin_rate_limits.html',
  'nav-sidebar.js',
  'CandidateUploader.js',
  'cv-processor.js',
  'server/',
  'ui/',
  'scripts/',
];

/** Patterns that must never appear in a patched file path — security guardrails. */
const BLOCKED_PATH_PATTERNS = [
  /\.env/i,
  /secret/i,
  /credential/i,
  /private[_\-.]?key/i,
  /\.pem$/i,
  /\.key$/i,
  /node_modules/,
  /\.git(\/|\\|$)/,
  /\/etc\//,
  /[Ss]ystem32/,
  /[Ww]indows[/\\]/,
  /passwd/i,
  /shadow/i,
];

/**
 * Validate that a file path is within the allowed set and not blocked.
 * @param {string} filePath
 * @returns {boolean}
 */
function isPathAllowed(filePath) {
  // Normalise: forward slashes, strip leading slashes
  const norm = filePath.replace(/\\/g, '/').replace(/^\/+/, '');
  for (const pat of BLOCKED_PATH_PATTERNS) {
    if (pat.test(norm)) return false;
  }
  return ALLOWED_PATCH_ROOTS.some(root => norm === root || norm.startsWith(root));
}

/**
 * Run scripts/apply_patch_build.sh with the supplied unified diff.
 *
 * @param {string}  patchContent  – complete unified diff text
 * @param {object}  [opts]
 * @param {boolean} [opts.buildDocker=false]  – pass 1 to build Docker image
 * @param {boolean} [opts.pushImage=false]    – pass 1 to push Docker image
 * @returns {Promise<{ ok: boolean, exit_code: number, stdout: string, stderr: string }>}
 */
function runApplyPatch(patchContent, opts = {}) {
  return new Promise((resolve, reject) => {
    const { buildDocker = false, pushImage = false } = opts;

    // Use a unique temp directory to prevent symlink-based path-traversal attacks
    const tmpDir  = fs.mkdtempSync(path.join(os.tmpdir(), 'ai_fix_'));
    const tmpFile = path.join(tmpDir, 'change.patch');
    try {
      fs.writeFileSync(tmpFile, patchContent, 'utf8');
    } catch (e) {
      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch (_) {}
      return reject(new Error('Failed to write patch file: ' + e.message));
    }

    const args = [
      SCRIPT_PATH,
      tmpFile,
      PROJECT_ROOT,
      buildDocker ? '1' : '0',
      pushImage   ? '1' : '0',
    ];

    const child = spawn('bash', args, {
      cwd:   PROJECT_ROOT,
      stdio: ['ignore', 'pipe', 'pipe'],
      env:   { ...process.env },
    });

    let stdout = '';
    let stderr = '';
    child.stdout.on('data', d => { stdout += d.toString(); });
    child.stderr.on('data', d => { stderr += d.toString(); });

    child.on('error', err => {
      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch (_) {}
      reject(new Error('Failed to start patch script: ' + err.message));
    });

    child.on('close', code => {
      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch (_) {}
      resolve({
        ok:        code === 0,
        exit_code: code,
        stdout:    stdout.slice(0, 4000),
        stderr:    stderr.slice(0, 2000),
      });
    });
  });
}

module.exports = { runApplyPatch, isPathAllowed, ALLOWED_PATCH_ROOTS };
