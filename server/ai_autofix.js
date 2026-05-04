'use strict';
/**
 * ai_autofix.js
 * Builds a Vertex AI prompt from an existing Gemini error analysis and calls
 * the Vertex AI REST API to produce a structured patch proposal.
 *
 * Required env vars:
 *   VERTEX_PROJECT    – GCP project ID (or set VERTEX_ENDPOINT directly)
 *   VERTEX_LOCATION   – GCP region (default: us-central1)
 *   VERTEX_MODEL      – model name (default: gemini-1.5-pro-002)
 *   VERTEX_ENDPOINT   – optional full endpoint override
 *   GOOGLE_APPLICATION_CREDENTIALS – path to SA JSON key file
 *   GCP_SA_KEY        – SA JSON string (alternative to file)
 */

const fs     = require('fs');
const crypto = require('crypto');

// ── Google Service Account JWT / token helper ─────────────────────────────────

function _getSaCredentials() {
  if (process.env.GCP_SA_KEY) {
    try { return JSON.parse(process.env.GCP_SA_KEY); } catch (_) {}
  }
  const credFile = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  if (credFile && fs.existsSync(credFile)) {
    try { return JSON.parse(fs.readFileSync(credFile, 'utf8')); } catch (_) {}
  }
  return null;
}

function _b64url(data) {
  const buf = Buffer.isBuffer(data) ? data : Buffer.from(JSON.stringify(data));
  return buf.toString('base64url');
}

async function _getAccessToken() {
  // Prefer google-auth-library (via googleapis) if already installed
  try {
    const { google } = require('googleapis');
    const auth   = new google.auth.GoogleAuth({ scopes: ['https://www.googleapis.com/auth/cloud-platform'] });
    const client = await auth.getClient();
    const { token } = await client.getAccessToken();
    return token;
  } catch (_) {
    // Fall through to manual JWT
  }

  const sa = _getSaCredentials();
  if (!sa) throw new Error('No GCP service account credentials found. Set GOOGLE_APPLICATION_CREDENTIALS or GCP_SA_KEY.');

  const now     = Math.floor(Date.now() / 1000);
  const header  = { alg: 'RS256', typ: 'JWT' };
  const payload = {
    iss:   sa.client_email,
    sub:   sa.client_email,
    aud:   'https://oauth2.googleapis.com/token',
    scope: 'https://www.googleapis.com/auth/cloud-platform',
    iat:   now,
    exp:   now + 1800,   // 30-minute window — short-lived for automated patch operations
  };

  const sigInput = `${_b64url(header)}.${_b64url(payload)}`;
  const sign     = crypto.createSign('RSA-SHA256');
  sign.update(sigInput);
  const sig = sign.sign(sa.private_key, 'base64url');
  const jwt = `${sigInput}.${sig}`;

  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method:  'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body:    new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion:  jwt,
    }).toString(),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`GCP token exchange failed (${resp.status}): ${text.slice(0, 400)}`);
  }
  const { access_token } = await resp.json();
  return access_token;
}

// ── Prompt builder ─────────────────────────────────────────────────────────────

/**
 * Build the Vertex AI prompt from a Gemini analysis object.
 * @param {object} geminiAnalysis – { error_message, source, explanation, suggested_fix, copilot_prompt }
 */
function buildVertexPrompt(geminiAnalysis) {
  const {
    error_message  = '',
    source         = '',
    explanation    = '',
    suggested_fix  = '',
    copilot_prompt = '',
  } = geminiAnalysis || {};

  return `You are a senior software engineer and security-conscious code reviewer.

An automated error capture system has analysed a runtime error. Your task is to produce a minimal, safe code fix.

## Error Analysis (from Gemini)
**Error Message:** ${error_message}
**Source File/Function:** ${source}
**Explanation:** ${explanation}
**Suggested Fix:** ${suggested_fix}
**Copilot Prompt:** ${copilot_prompt}

## Your Task
1. Produce a single unified diff (standard \`diff -u\` format) that applies the minimal fix to the identified file(s).
2. Write one or two short unit test snippets (Jest / pytest) that verify the fix.
3. Write a clear rationale (2-3 sentences) explaining why the fix is correct.
4. Assess the risk: low / medium / high, with a one-sentence justification.

## Safety Rules
- Do NOT modify .env files, secret files, private keys, or infrastructure configs.
- Changes must be confined to application source files only.
- The diff must be minimal — change only what is necessary to fix the error.

## Output Format (strict JSON — no prose outside the JSON block)
Return ONLY a valid JSON object with these exact keys:
{
  "diff": "<unified diff string, or empty string if no change needed>",
  "tests": "<test code string, or empty string>",
  "rationale": "<string>",
  "risk": "low | medium | high",
  "risk_reason": "<one-sentence string>",
  "files_changed": ["<relative/file/path>"]
}`;
}

// ── Vertex AI REST caller ──────────────────────────────────────────────────────

/**
 * Call Vertex AI to generate a patch proposal.
 * @param {object} geminiAnalysis – error analysis from Gemini
 * @returns {Promise<{diff, tests, rationale, risk, risk_reason, files_changed}>}
 */
async function callVertexAI(geminiAnalysis) {
  const project  = process.env.VERTEX_PROJECT;
  const location = process.env.VERTEX_LOCATION || 'us-central1';
  const model    = process.env.VERTEX_MODEL    || 'gemini-1.5-pro-002';
  const endpoint = process.env.VERTEX_ENDPOINT
    || `https://${location}-aiplatform.googleapis.com/v1/projects/${project}/locations/${location}/publishers/google/models/${model}:generateContent`;

  if (!project && !process.env.VERTEX_ENDPOINT) {
    throw new Error('VERTEX_PROJECT environment variable is required (or set VERTEX_ENDPOINT).');
  }

  const token  = await _getAccessToken();
  const prompt = buildVertexPrompt(geminiAnalysis);

  const body = {
    contents:         [{ role: 'user', parts: [{ text: prompt }] }],
    generationConfig: { temperature: 0.1, maxOutputTokens: 4096 },
    safetySettings:   [
      { category: 'HARM_CATEGORY_DANGEROUS_CONTENT', threshold: 'BLOCK_ONLY_HIGH' },
    ],
  };

  const resp = await fetch(endpoint, {
    method:  'POST',
    headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
    body:    JSON.stringify(body),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Vertex AI request failed (${resp.status}): ${text.slice(0, 500)}`);
  }

  const data = await resp.json();
  const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || '';

  let parsed;
  try {
    // Strip optional markdown code fences
    const clean = text.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/, '').trim();
    parsed = JSON.parse(clean);
  } catch (_) {
    throw new Error(`Vertex AI returned non-JSON: ${text.slice(0, 300)}`);
  }

  // Ensure all required keys are present
  const REQUIRED = ['diff', 'tests', 'rationale', 'risk', 'risk_reason', 'files_changed'];
  for (const k of REQUIRED) {
    if (!(k in parsed)) parsed[k] = k === 'files_changed' ? [] : '';
  }
  if (!Array.isArray(parsed.files_changed)) parsed.files_changed = [];

  return parsed;
}

module.exports = { buildVertexPrompt, callVertexAI };
