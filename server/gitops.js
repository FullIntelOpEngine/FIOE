'use strict';
/**
 * gitops.js
 * Minimal GitHub REST-API helpers using native fetch (Node >= 18).
 *
 * Required env vars:
 *   GITHUB_TOKEN         – fine-grained PAT or GitHub App installation token
 *   GITHUB_OWNER         – repository owner (user / org)
 *   GITHUB_REPO          – repository name
 *   GITHUB_BASE_BRANCH   – base branch (default: main)
 */

const GITHUB_API = 'https://api.github.com';

function _headers() {
  const token = process.env.GITHUB_TOKEN;
  if (!token) throw new Error('GITHUB_TOKEN environment variable is required.');
  return {
    'Authorization':        `Bearer ${token}`,
    'Accept':               'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
    'Content-Type':         'application/json',
  };
}

function _cfg() {
  const owner = process.env.GITHUB_OWNER;
  const repo  = process.env.GITHUB_REPO;
  const base  = process.env.GITHUB_BASE_BRANCH || 'main';
  if (!owner || !repo) throw new Error('GITHUB_OWNER and GITHUB_REPO environment variables are required.');
  return { owner, repo, base };
}

async function _ghFetch(url, opts = {}) {
  const resp = await fetch(url, { ...opts, headers: { ..._headers(), ...(opts.headers || {}) } });
  const body = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const msg = body.message || JSON.stringify(body);
    throw new Error(`GitHub API error ${resp.status} at ${url}: ${msg}`);
  }
  return body;
}

/** Get the SHA of the HEAD commit on the base branch. */
async function getBaseSha() {
  const { owner, repo, base } = _cfg();
  const data = await _ghFetch(`${GITHUB_API}/repos/${owner}/${repo}/git/ref/heads/${encodeURIComponent(base)}`);
  return data.object.sha;
}

/** Create a new branch pointing at the current HEAD of the base branch. */
async function createBranch(branchName) {
  const { owner, repo } = _cfg();
  const baseSha = await getBaseSha();
  await _ghFetch(`${GITHUB_API}/repos/${owner}/${repo}/git/refs`, {
    method: 'POST',
    body:   JSON.stringify({ ref: `refs/heads/${branchName}`, sha: baseSha }),
  });
  return baseSha;
}

/**
 * Get the current blob SHA for a file (required by the Contents API to update existing files).
 * Returns null if the file does not yet exist on that branch.
 */
async function getFileSha(filePath, branch) {
  const { owner, repo } = _cfg();
  try {
    // Encode only the path component — separate from the ?ref= query string
    const encodedPath = filePath.split('/').map(encodeURIComponent).join('/');
    const data = await _ghFetch(
      `${GITHUB_API}/repos/${owner}/${repo}/contents/${encodedPath}?ref=${encodeURIComponent(branch)}`
    );
    return data.sha || null;
  } catch (_) {
    return null; // file does not exist
  }
}

/**
 * Commit a list of file updates (complete new content) to a branch.
 * files: [{ path: 'relative/path', content: 'full utf-8 content' }]
 */
async function commitFiles(branchName, files, commitMessage) {
  const { owner, repo } = _cfg();
  for (const file of files) {
    const encodedPath = file.path.split('/').map(encodeURIComponent).join('/');
    const sha  = await getFileSha(file.path, branchName);
    const body = {
      message: commitMessage,
      content: Buffer.from(file.content, 'utf8').toString('base64'),
      branch:  branchName,
    };
    if (sha) body.sha = sha;
    await _ghFetch(
      `${GITHUB_API}/repos/${owner}/${repo}/contents/${encodedPath}`,
      { method: 'PUT', body: JSON.stringify(body) }
    );
  }
}

/**
 * Create a pull request.
 * @param {object} opts
 * @param {string}   opts.branch  – head branch
 * @param {string}   opts.title   – PR title
 * @param {string}   opts.body    – PR description (markdown)
 * @param {string[]} [opts.labels] – optional label names to add after creation
 * @returns {Promise<object>} GitHub PR response object (contains html_url, number, …)
 */
async function createPullRequest({ branch, title, body: prBody, labels = [] }) {
  const { owner, repo, base } = _cfg();
  const pr = await _ghFetch(`${GITHUB_API}/repos/${owner}/${repo}/pulls`, {
    method: 'POST',
    body:   JSON.stringify({ title, body: prBody, head: branch, base, draft: false }),
  });
  if (labels.length) {
    await _ghFetch(`${GITHUB_API}/repos/${owner}/${repo}/issues/${pr.number}/labels`, {
      method: 'POST',
      body:   JSON.stringify({ labels }),
    }).catch(() => {}); // non-fatal — labels may not exist
  }
  return pr;
}

module.exports = { getBaseSha, createBranch, getFileSha, commitFiles, createPullRequest };
