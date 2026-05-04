// server_routes2.js – Routes split from server.js (original lines 6169–11843).
// Exported as registerRoutes(app, ctx) and called by server.js at startup.
// All shared state and helpers are passed through the ctx object.

'use strict';

const fs     = require('fs');
const path   = require('path');
const http   = require('http');
const https  = require('https');
const crypto = require('crypto');
const net    = require('net');
const dns    = require('dns').promises;

// ── Module-level pre-compiled regexes ────────────────────────────────────────
// Compiled once at startup; avoids per-request RegExp construction overhead.

// calculate-unmatched: strip LLM preamble / structural chars from output
const _RE_CALC_UM_INTRO    = /^(Here are|The following|These are).*?[:\n]/gim;
const _RE_CALC_UM_LONG     = /Here are the skills present[^:\n]*[:\s]*/i;
const _RE_CALC_UM_BRACKETS = /[\[\]"']/g;
const _RE_CALC_UM_DELIM    = /[\n\r,]+/g;
const _RE_CALC_UM_BULLET   = /^[-*•]\s+/;

// assess-unmatched: strip markdown code fences from LLM JSON response
const _RE_ASSESS_CODE_FENCE = /```(?:json)?/g;

// sync-entries / verify-data: strip markdown code fences from LLM text/JSON response
const _RE_CODE_FENCE = /```json|```/g;

// ai-comp: when uncached row count exceeds this threshold, return immediately with
// cached results and run the LLM call in the background (fire-and-forget + SSE notify).
const _AI_COMP_ASYNC_THRESHOLD = parseInt(process.env.AI_COMP_ASYNC_THRESHOLD, 10) || 10;

// verify-data: when row count exceeds this threshold, respond 202 immediately and run
// LLM normalisation in the background; result delivered via `verify_data_complete` SSE.
const _VERIFY_DATA_ASYNC_THRESHOLD = parseInt(process.env.VERIFY_DATA_ASYNC_THRESHOLD, 10) || 30;

// assess-unmatched: when the (already-capped) unmatched list exceeds this threshold,
// respond 202 immediately and run the LLM call in the background.
const _ASSESS_UM_ASYNC_THRESHOLD = parseInt(process.env.ASSESS_UM_ASYNC_THRESHOLD, 10) || 20;

module.exports = function registerRoutes(app, ctx) {
  const {
    pool,
    requireLogin,
    dashboardRateLimit,
    userRateLimit,
    withExponentialBackoff,
    llmGenerateText,
    incrementGeminiQueryCount,
    _writeApprovalLog,
    _writeInfraLog,
    _aiCompCacheGet,
    _aiCompCacheSet,
    _buildMLProfileData,
    getOrCreateTransporter,
    getSaveStatePath,
    loadEmailVerifConfig,
    loadRateLimits,
    loadSmtpConfig,
    normalizeCompanyName,
    normalizeCountry,
    picToDataUri,
    standardizeSeniority,
    ensureCanonicalFieldsForId,
    firstVal,
    google,
    allowedOrigins,
    ML_OUTPUT_DIR,
    SAVE_STATE_DIR,
    CRITERIA_DIR,
    ICS_URLS_PATH,
    EXTERNAL_API_TIMEOUT_MS,
    _SSE_HEARTBEAT_MS,
    _SSE_COALESCE_DELAY_MS,
    _PORTING_UPLOAD_MAX_BYTES,
    _SCHEDULER_DEFAULT_DURATION,
    _SCHEDULER_DEFAULT_MAX_SLOTS,
    _EMAIL_VERIF_CONFIG_PATHS,
    CONTACT_GEN_IN_EMAIL_VERIF,
  } = ctx;

// ========== NEW: Calculate Unmatched Skillset ==========
app.post('/candidates/:id/calculate-unmatched', requireLogin, async (req, res) => {
    const id = parseInt(req.params.id, 10);
    if (Number.isNaN(id)) return res.status(400).json({ error: 'Invalid candidate id' });

    try {
        let jdSkillsetRaw = '';
        
        // 1. Fetch JD Skillset from Process table (per-profile jskillset)
        try {
            const pRes = await pool.query('SELECT jskillset FROM "process" WHERE id = $1', [id]);
            if (pRes.rows.length > 0 && pRes.rows[0].jskillset) {
                jdSkillsetRaw = pRes.rows[0].jskillset;
            }
        } catch (e) {
             console.warn('[CALC_UNMATCHED] failed to read process.jskillset', e.message);
        }

        // 2. Fallback: Fetch User's JD Skillset from login table if process.jskillset is missing
        if (!jdSkillsetRaw) {
            try {
                // Use username for consistency
                const uRes = await pool.query('SELECT jskillset FROM login WHERE username = $1', [req.user.username]);
                if (uRes.rows.length > 0) {
                    jdSkillsetRaw = uRes.rows[0].jskillset || '';
                }
            } catch (e) {
                console.warn('[CALC_UNMATCHED] fallback login.jskillset read failed', e.message);
            }
        }
        
        // 3. Fetch Candidate's current skillset, sector, and jobfamily from process table
        const candidateRes = await pool.query('SELECT skillset, sector, jobfamily FROM "process" WHERE id = $1', [id]);
        if (candidateRes.rows.length === 0) {
            return res.status(404).json({ error: 'Candidate not found.' });
        }
        const candidateSkillsetRaw = candidateRes.rows[0].skillset || '';
        const sectorRaw = candidateRes.rows[0].sector ? String(candidateRes.rows[0].sector).trim() : 'Unknown';
        const jobFamilyRaw = candidateRes.rows[0].jobfamily ? String(candidateRes.rows[0].jobfamily).trim() : 'Unknown';

        // 4. Use LLM to Calculate Unmatched Skillset
        const prompt = `
            Compare the Job Description (JD) Skillset and the Candidate Skillset below.
            Context:
            - Sector: "${sectorRaw}"
            - Job Family: "${jobFamilyRaw}"

            Identify the skills that are present in the JD Skillset but are MISSING or UNMATCHED in the Candidate Skillset.
            
            JD Skillset: "${jdSkillsetRaw}"
            Candidate Skillset: "${candidateSkillsetRaw}"
            
            Return the result as a simple list. Do NOT include any introductory or explanatory text.
        `;

        // Return 202 immediately so the HTTP connection is freed; LLM + DB work runs in the background.
        // The client receives the result via the `candidate_updated` SSE event when the job completes.
        const bgUsername = req.user && req.user.username;
        res.status(202).json({ queued: true, id });

        // Fire-and-forget background job (pool/broadcastSSE/helpers remain in scope)
        (async () => {
            try {
                const rawText = await llmGenerateText(prompt, { username: bgUsername, label: 'llm/skill-gap' });
                incrementGeminiQueryCount(bgUsername).catch(() => {});

                // 5. Data Cleansing — use pre-compiled module-level regexes
                let cleaned = rawText.replace(_RE_CALC_UM_INTRO, '');
                cleaned = cleaned.replace(_RE_CALC_UM_LONG, '');
                cleaned = cleaned.replace(_RE_CALC_UM_BRACKETS, '');
                cleaned = cleaned.replace(_RE_CALC_UM_DELIM, ';');

                const tokens = cleaned
                  .split(';')
                  .map(s => s.trim().replace(_RE_CALC_UM_BULLET, '').replace(/^[-*•]/, ''))
                  .filter(s => s.length > 0);

                const unmatchedStr = tokens.join('; ');

                // 6. Update process table column 'lskillset' ONLY
                const updateRes = await pool.query(
                    'UPDATE "process" SET lskillset = $1 WHERE id = $2 RETURNING *',
                    [unmatchedStr, id]
                );

                const r = updateRes.rows[0];
                if (!r) return;

                // 7. Build mapped object and broadcast SSE so the client updates live
                const companyCanonical = normalizeCompanyName(r.company || r.organisation || '');
                const mapped = {
                    ...r,
                    jobtitle: r.jobtitle ?? null,
                    company: companyCanonical ?? (r.company ?? null),
                    lskillset: r.lskillset ?? null,
                    linkedinurl: r.linkedinurl ?? null,
                    jskillset: r.jskillset ?? null,
                    pic: picToDataUri(r.pic),
                    role: r.role ?? r.jobtitle ?? null,
                    organisation: companyCanonical ?? (r.organisation ?? r.company ?? null),
                    type: r.product ?? null,
                    compensation: r.compensation ?? null
                };
                try { broadcastSSE('candidate_updated', mapped); } catch (_) {}
            } catch (bgErr) {
                console.error('[CALC_UNMATCHED BG] error for id', id, bgErr && bgErr.message);
            }
        })();

    } catch (err) {
        console.error('Calculate unmatched error:', err);
        res.status(500).json({ error: 'Failed to calculate unmatched skillset', detail: err.message });
    }
});

// ========== NEW: Assess Unmatched Skills via Gemini ==========
app.post('/candidates/:id/assess-unmatched', requireLogin, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Invalid id' });

    const { source = 'candidate', sourceSkills = [], unmatched = [] } = req.body;
    // sourceSkills = Canonical/JD skills
    // unmatched = Raw tokens found in lskillset or provided list
    
    if (!Array.isArray(unmatched) || !unmatched.length) {
      return res.status(400).json({ error: 'No unmatched skills provided.' });
    }

    // Guard against very large batches that would produce slow/expensive LLM calls.
    // The cap is driven by the 'analytic_batch_size' entry in rate_limits.json
    // (admin-configurable via admin_rate_limits.html).  Falls back to 50 when unset.
    const _rlCfg = loadRateLimits();
    const _rlUsername = req.user && req.user.username;
    const _rlUser = (_rlCfg.users || {})[_rlUsername] || {};
    const _rlDef  = (_rlCfg.defaults || {});
    const _rlBatch = _rlUser.analytic_batch_size || _rlDef.analytic_batch_size;
    const _ASSESS_BATCH_LIMIT = (_rlBatch && parseInt(_rlBatch.requests, 10) > 0)
      ? parseInt(_rlBatch.requests, 10)
      : 50;
    const batchedUnmatched = unmatched.slice(0, _ASSESS_BATCH_LIMIT);
    const wasTruncated = batchedUnmatched.length < unmatched.length;
    if (wasTruncated) {
      console.warn(`[ASSESS_UNMATCHED] input truncated: ${unmatched.length} → ${batchedUnmatched.length} items`);
    }

    // For large batches, respond 202 immediately and complete the LLM call in the background.
    // Result is delivered via the `skill_assessment_result` SSE event.
    const _isAsyncAssess = batchedUnmatched.length > _ASSESS_UM_ASYNC_THRESHOLD;
    if (_isAsyncAssess) {
      res.status(202).json({ pending: batchedUnmatched.length, message: `Assessing ${batchedUnmatched.length} skills in background\u2026` });
    }

    // Build an instruction telling the LLM to compare the two lists and classify each unmatched token
    const instruction = `
You are a skill matching assistant. Inputs:
- sourceSkills: canonical skillset list (comma-separated): ${JSON.stringify(sourceSkills)}
- unmatched: list of tokens to check (array): ${JSON.stringify(batchedUnmatched)}

For each entry in unmatched, return JSON item:
{ "original": "<raw token>", "normalized": "<canonical label or null>", "verdict": "<true-missing|synonym|ignore>", "mappedTo": "<if synonym then canonical skill>" }

Return JSON only:
{ "suggestions": [ ... ] }
    `;

    const text = await llmGenerateText(instruction, { username: req.user && req.user.username, label: 'llm/suggestions' });

    // Attempt to robustly extract JSON — use pre-compiled module-level code-fence regex
    const cleaned = text.replace(_RE_ASSESS_CODE_FENCE, '').trim();
    let parsed;
    try {
      parsed = JSON.parse(cleaned);
    } catch (e) {
      const match = cleaned.match(/\{[\s\S]*\}/);
      if (match) parsed = JSON.parse(match[0]);
    }
    if (!parsed || !Array.isArray(parsed.suggestions)) {
      // Fallback if parsing fails or structure is wrong
      if (_isAsyncAssess) {
        console.error('[ASSESS_UNMATCHED] background: AI response parse failed', text && text.slice(0, 200));
        return;
      }
      return res.status(500).json({ error: 'AI response parse failed.', raw: text });
    }

    // Normalize result structure
    parsed.suggestions = parsed.suggestions.map(s => ({
      original: s.original || s.o || '',
      normalized: s.normalized || s.normal || null,
      verdict: s.verdict || 'true-missing',
      mappedTo: s.mappedTo || s.mapped || null
    }));

    // Inform caller when input was capped so they can paginate or split
    if (wasTruncated) {
      parsed.truncated = true;
      parsed.totalProvided = unmatched.length;
      parsed.processedCount = batchedUnmatched.length;
    }

    if (_isAsyncAssess) {
      broadcastSSE('skill_assessment_result', {
        candidateId: id,
        suggestions: parsed.suggestions,
        ...(wasTruncated ? { truncated: true, totalProvided: unmatched.length, processedCount: batchedUnmatched.length } : {}),
      });
    } else {
      res.json(parsed);
    }
  } catch (err) {
    console.error('/assess-unmatched error', err);
    if (!_isAsyncAssess) {
      res.status(500).json({ error: 'Assessment failed' });
    }
  }
});

/**
 * POST /candidates/bulk-update
 * Accept an array of candidate objects to update in the "process" table.
 * Each item must include a numeric "id" and any updatable fields. Uses the same field mapping as PUT /candidates/:id.
 * Returns the list of updated rows.
 */
app.post('/candidates/bulk-update', requireLogin, async (req, res) => {
  const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
  if (!rows.length) return res.status(400).json({ error: 'No rows provided.' });

  // Field mapping identical to the single PUT endpoint mapping
  const fieldMap = {
    role: 'jobtitle',
    organisation: 'company',
    job_family: 'jobfamily',
    sourcing_status: 'sourcingstatus',
    product: 'product',
    type: 'product',
    jobtitle: 'jobtitle',
    company: 'company',
    jobfamily: 'jobfamily',
    sourcingstatus: 'sourcingstatus',
    name: 'name',
    sector: 'sector',
    role_tag: 'role_tag',
    skillset: 'skillset',
    geographic: 'geographic',
    country: 'country',
    email: 'email',
    mobile: 'mobile',
    office: 'office',
    compensation: 'compensation',
    seniority: 'seniority',
    lskillset: 'lskillset',
    linkedinurl: 'linkedinurl',
    exp: 'exp',
    tenure: 'tenure',
    education: 'education'
  };

  const client = await pool.connect();
  const updatedRows = [];
  const canonicalBatch = [];  // collected for parallel post-commit canonicalization
  const updatedIds     = [];  // collected for post-commit batch reload
  try {
    await client.query('BEGIN');

    // Batch-fetch ownership for all incoming IDs in a single query (was 1 SELECT per row)
    const allIncomingIds = rows
      .map(item => Number(item?.id))
      .filter(n => Number.isInteger(n) && n > 0);
    const ownedIds = new Set();
    if (allIncomingIds.length > 0) {
      try {
        const ownerQ = await client.query(
          'SELECT id, userid FROM "process" WHERE id = ANY($1::int[])',
          [allIncomingIds]
        );
        for (const r of ownerQ.rows) {
          if (String(r.userid) === String(req.user.id)) ownedIds.add(r.id);
        }
      } catch (e) {
        console.warn('[BULK_UPDATE_AUTH] failed batch ownership check', e && e.message);
        // Proceed with empty ownedIds — all rows will be skipped safely
      }
    }

    for (const item of rows) {
      const id = Number(item?.id);
      if (!Number.isInteger(id) || id <= 0) continue;
      if (!ownedIds.has(id)) continue;  // not owned or does not exist

      const keys = Object.keys(item).filter(k => k !== 'id' && Object.prototype.hasOwnProperty.call(fieldMap, k));
      if (!keys.length) continue;

      // Build unique column -> value map to prevent multiple assignments to same column
      const colValueMap = new Map();
      for (const k of keys) {
        const col = fieldMap[k];
        let v = item[k];
        if (k === 'seniority' && v != null && String(v).trim() !== '') {
          const std = standardizeSeniority(v);
          v = std || null;
        }
        if (k === 'compensation' && v != null && v !== '') {
          const n = Number(v);
          v = isNaN(n) ? null : n;
        }
        colValueMap.set(col, v === '' ? null : v);
      }

      const cols = [];
      const values = [];
      let idx = 1;
      for (const [col, val] of colValueMap.entries()) {
        cols.push(`"${col}" = $${idx}`);
        values.push(val);
        idx++;
      }
      values.push(id);
      const sql = `UPDATE "process" SET ${cols.join(', ')} WHERE id = $${idx} RETURNING *`;
      // eslint-disable-next-line no-await-in-loop
      const result = await client.query(sql, values);
      if (result.rowCount === 1) {
        const r = result.rows[0];
        // Collect for parallel canonicalization after COMMIT (avoid blocking the transaction)
        canonicalBatch.push({ id: r.id, company: r.company || r.organisation, jobtitle: r.jobtitle || r.role });
        updatedIds.push(r.id);
      }
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Bulk update error:', err);
    res.status(500).json({ error: 'Bulk update failed.' });
    return;
  } finally {
    client.release();
  }

  // Run canonicalization in parallel now that the transaction is committed
  if (canonicalBatch.length > 0) {
    await Promise.all(canonicalBatch.map(({ id, company, jobtitle }) =>
      ensureCanonicalFieldsForId(id, company, jobtitle, null)
        .catch(e => console.warn('[BULK_UPDATE_CANON] failed for id', id, e && e.message))
    ));
  }

  // Batch-reload all updated rows to get fully canonicalized values
  if (updatedIds.length > 0) {
    try {
      const reloadRes = await pool.query(
        'SELECT * FROM "process" WHERE id = ANY($1::int[])',
        [updatedIds]
      );
      for (const r of reloadRes.rows) {
        updatedRows.push({
          ...r,
          jobtitle: r.jobtitle ?? null,
          company: normalizeCompanyName(r.company || r.organisation) ?? (r.company ?? null),
          jobfamily: r.jobfamily ?? null,
          sourcingstatus: r.sourcingstatus ?? null,
          product: r.product ?? null,
          lskillset: r.lskillset ?? null,
          pic: picToDataUri(r.pic),
          role: r.role ?? r.jobtitle ?? null,
          organisation: normalizeCompanyName(r.company || r.organisation) ?? (r.organisation ?? r.company ?? null),
          job_family: r.job_family ?? r.jobfamily ?? null,
          sourcing_status: r.sourcing_status ?? r.sourcingstatus ?? null,
          type: r.product ?? null,
          compensation: r.compensation ?? null,
          jskillset: r.jskillset ?? null
        });
      }
    } catch (e) {
      console.warn('[BULK_UPDATE_RELOAD] failed to reload rows after canonicalization:', e && e.message);
    }
  }

  // Emit change notification
  try {
    broadcastSSE('candidates_changed', { action: 'bulk_update', count: updatedRows.length });
    broadcastSSEBulk(updatedRows);
  } catch (e) { /* ignore */ }

  res.json({ updatedCount: updatedRows.length, rows: updatedRows });
  _writeApprovalLog({ action: 'bulk_candidates_update', username: req.user.username, userid: req.user.id, detail: `Bulk updated ${updatedRows.length} candidates`, source: 'server.js' });
});

/**
 * ========== Data Verification (Company & Job Title Standardization via Gemini 2.5 Flash Lite) ==========
 * Endpoint: POST /verify-data
 * Body: { rows: [ { id, organisation, jobtitle?, seniority?, geographic?, country? } ] }
 * Response: { corrected: [ { id, organisation?, company?, jobtitle?, standardized_job_title?, personal?, seniority?, geographic?, country? } ] }
 *
 * This endpoint sends organisation, job title, seniority, geographic and country data to Gemini
 * to standardize them (e.g. normalize company names, categorize job titles, normalize countries).
 */
app.post('/verify-data', requireLogin, async (req, res) => {
  const { rows, mlProfile } = req.body;
  if (!rows || !Array.isArray(rows) || rows.length === 0) {
    return res.status(400).json({ error: 'No rows provided.' });
  }

  // For large batches, respond 202 immediately so the HTTP connection is freed.
  // The LLM call and normalisation continue in the background; the result is
  // delivered to all connected clients via the `verify_data_complete` SSE event.
  const _isAsync = rows.length > _VERIFY_DATA_ASYNC_THRESHOLD;
  if (_isAsync) {
    res.status(202).json({ pending: rows.length, message: `Syncing ${rows.length} rows in background\u2026` });
  }

  // Build a map of id → original job title from the request input.
  // Job title is COMPLETELY IMMUTABLE throughout Sync Entries — ML and Gemini must never
  // overwrite it under any circumstances. This map is used only for ML matching so that
  // Seniority and Job Family are bound to the candidate's actual stored job title.
  const originalTitleMap = {};
  for (const row of rows) {
    if (row.id != null) {
      originalTitleMap[row.id] = (row.jobtitle || row.role || '').trim();
    }
  }

  // Derive highest-confidence values from ML profile (if provided).
  // New grouped format (DB Dock Out): { Job_Families: [{ Job_Family, Family_Core_DNA, Jobtitle, Seniority }, ...], company, compensation }
  // Intermediate flat format (backward compat): { Jobtitle: { "<Title>": { Unique_Delta_Skills, ... } }, Seniority, Job_Family, ... }
  // Legacy nested format: { job_title: { job_title: { "<Title>": { job_family, Seniority, top_10_skills } } }, company, ... }
  // ML defaults are applied per-candidate by looking up the candidate's specific job title.
  // sector, seniority, and job_family are applied as ML fallbacks for empty fields.
  // country and sourcing_status are intentionally excluded — Gemini handles those.
  const mlDefaults = {};
  let mlProfileRole = null;  // normalized role string from the ML profile
  const topKey = obj => {
    if (!obj || typeof obj !== 'object') return null;
    return Object.entries(obj).sort((a, b) => b[1] - a[1])[0]?.[0] || null;
  };
  if (mlProfile && typeof mlProfile === 'object') {
    // Yield the event loop before the CPU-heavy mlProfile parsing block so that
    // other pending I/O callbacks (e.g. incoming requests) can run before this
    // synchronous work begins.
    await new Promise(r => setImmediate(r));

    // ── New grouped format detection (has top-level "Job_Families" array) ──
    if (Array.isArray(mlProfile.Job_Families)) {
      // Grouped format: one block per job family, each with its own Jobtitle section.
      // Seniority is embedded directly inside each Jobtitle entry as a flat { level: proportion } dict.
      // Reconstruct a flat jobTitleProfileMap keyed by title name for the per-candidate lookup below.
      const reconstructed = {};
      const allSenEntries = [];
      const familyTitleCounts = {};  // { familyName: titleCount } for global family ranking

      for (const familyBlock of mlProfile.Job_Families) {
        if (!familyBlock || typeof familyBlock !== 'object') continue;
        const familyName = typeof familyBlock.Job_Family === 'string' ? familyBlock.Job_Family.trim() : null;
        const jobtitleSection = (familyBlock.Jobtitle && typeof familyBlock.Jobtitle === 'object')
          ? familyBlock.Jobtitle : {};
        // Backward compat: old files may still carry a family-level Seniority reverse map
        const legacySenioritySection = (familyBlock.Seniority && typeof familyBlock.Seniority === 'object')
          ? familyBlock.Seniority : {};

        // Count titles per family for global family ranking
        if (familyName) {
          familyTitleCounts[familyName] = (familyTitleCounts[familyName] || 0) + Object.keys(jobtitleSection).length;
        }

        for (const [titleName, titleData] of Object.entries(jobtitleSection)) {
          if (!titleData || typeof titleData !== 'object') continue;
          // Prefer per-title embedded Seniority; fall back to legacy reverse map for old files
          const titleSeniority = {};
          const embeddedSen = titleData.Seniority;
          if (embeddedSen && typeof embeddedSen === 'object' && Object.keys(embeddedSen).length > 0) {
            for (const [level, conf] of Object.entries(embeddedSen)) {
              const c = Number(conf);
              if (!isNaN(c) && c >= 0) titleSeniority[level] = c;
            }
          } else {
            // Legacy fallback: reconstruct from family-level Seniority reverse map
            for (const [senLevel, senEntry] of Object.entries(legacySenioritySection)) {
              if (!senEntry || !Array.isArray(senEntry.Jobtitle_Match)) continue;
              if (senEntry.Jobtitle_Match.includes(titleName)) {
                titleSeniority[senLevel] = Number(senEntry.Confidence) || 0;
              }
            }
          }
          // Collect all seniority entries for global dominant-level computation
          for (const [level, conf] of Object.entries(titleSeniority)) {
            allSenEntries.push([level, conf]);
          }
          reconstructed[titleName] = {
            ...(Object.keys(titleSeniority).length > 0 ? { Seniority: titleSeniority } : {}),
            ...(familyName ? { job_family: { [familyName]: 1 } } : {}),
          };
        }
      }
      if (Object.keys(reconstructed).length > 0) {
        mlDefaults.jobTitleProfileMap = reconstructed;
      }
      // Global seniority: dominant level (highest Confidence) across all family blocks
      const topSenEntries = allSenEntries.filter(([, c]) => c > 0);
      if (topSenEntries.length > 0) {
        mlDefaults.seniority = topSenEntries.sort((a, b) => b[1] - a[1])[0][0];
      }
      // Global job family: family with most associated job titles
      const topFamilyEntry = Object.entries(familyTitleCounts).sort((a, b) => b[1] - a[1])[0];
      if (topFamilyEntry) mlDefaults.jobfamily = topFamilyEntry[0];

    } else if (mlProfile.Jobtitle && typeof mlProfile.Jobtitle === 'object') {
      // ── Intermediate flat format (backward compat — has top-level "Jobtitle" dict) ──
      // Reconstruct a jobTitleProfileMap compatible with the per-candidate lookup below.
      const senioritySection = (mlProfile.Seniority && typeof mlProfile.Seniority === 'object')
        ? mlProfile.Seniority : {};
      const reconstructed = {};
      for (const [titleName, titleData] of Object.entries(mlProfile.Jobtitle)) {
        if (!titleData || typeof titleData !== 'object') continue;
        // Build per-title Seniority dict from the reverse-map in the top-level Seniority section
        const titleSeniority = {};
        for (const [senLevel, senEntry] of Object.entries(senioritySection)) {
          if (!senEntry || !Array.isArray(senEntry.Jobtitle_Match)) continue;
          if (senEntry.Jobtitle_Match.includes(titleName)) {
            titleSeniority[senLevel] = Number(senEntry.Confidence) || 0;
          }
        }
        // job_family: prefer per-title Job_Family field; fall back to Job_Family dict reverse-lookup;
        // finally accept legacy string.
        let titleJobFamilyObj = null;
        if (titleData.Job_Family && typeof titleData.Job_Family === 'string' && titleData.Job_Family.trim()) {
          titleJobFamilyObj = { [titleData.Job_Family.trim()]: 1 };
        } else if (mlProfile.Job_Family && typeof mlProfile.Job_Family === 'object') {
          for (const [family, familyEntry] of Object.entries(mlProfile.Job_Family)) {
            if (familyEntry && Array.isArray(familyEntry.Jobtitle_Match) && familyEntry.Jobtitle_Match.includes(titleName)) {
              if (!titleJobFamilyObj) titleJobFamilyObj = {};
              titleJobFamilyObj[family] = Number(familyEntry.Confidence) || 1;
            }
          }
        } else if (typeof mlProfile.Job_Family === 'string' && mlProfile.Job_Family.trim()) {
          titleJobFamilyObj = { [mlProfile.Job_Family.trim()]: 1 };
        }
        reconstructed[titleName] = {
          ...(Object.keys(titleSeniority).length > 0 ? { Seniority: titleSeniority } : {}),
          ...(titleJobFamilyObj ? { job_family: titleJobFamilyObj } : {}),
        };
      }
      if (Object.keys(reconstructed).length > 0) {
        mlDefaults.jobTitleProfileMap = reconstructed;
      }
      // Global seniority: dominant level (highest Confidence) across all seniority entries
      const senEntries = Object.entries(senioritySection)
        .map(([level, entry]) => [level, Number((entry && entry.Confidence) || 0)])
        .filter(([, c]) => c > 0);
      if (senEntries.length > 0) {
        mlDefaults.seniority = senEntries.sort((a, b) => b[1] - a[1])[0][0];
      }
      // Global job family: dominant family (highest Confidence) from dict, or legacy string
      if (mlProfile.Job_Family && typeof mlProfile.Job_Family === 'object') {
        const jfEntries = Object.entries(mlProfile.Job_Family)
          .map(([name, entry]) => [name, Number((entry && entry.Confidence) || 0)])
          .filter(([, c]) => c > 0);
        if (jfEntries.length > 0) {
          mlDefaults.jobfamily = jfEntries.sort((a, b) => b[1] - a[1])[0][0];
        }
      } else if (typeof mlProfile.Job_Family === 'string' && mlProfile.Job_Family.trim()) {
        mlDefaults.jobfamily = mlProfile.Job_Family.trim();
      }
    } else {
      // ── Legacy nested format handling ──
      // Determine which object holds the job_title section.
      // Support:
      //   - Prior format: { job_title: { job_title: { "<Title>": { job_family: {}, Seniority: {}, top_10_skills: {} } }, ... } }
      //   - Older format: { job_title: { job_title, Seniority, job_family, role_tag, ... } } (top-level aggregates)
      //   - Even older: { job_title: { role, seniority_distribution, ... } } (backward compat)
      //   - Legacy nested-role format: { "<role>": { sector_preferences, seniority_distribution, ... } }
      //   - Legacy flat format: { sector_preferences, seniority_distribution, ... }
      let jobTitleSection = mlProfile.job_title || null;

      if (!jobTitleSection) {
        // Detect previous nested-role format: single non-structural key whose value is a plain object
        const roleKeys = Object.keys(mlProfile).filter(k => k !== 'last_updated' && k !== 'company' && k !== 'compensation');
        if (roleKeys.length === 1 && typeof mlProfile[roleKeys[0]] === 'object' && mlProfile[roleKeys[0]] !== null) {
          jobTitleSection = mlProfile[roleKeys[0]];
          mlProfileRole = roleKeys[0];  // role is the key itself in nested-role format
        } else {
          // Legacy flat format — no role restriction
          jobTitleSection = mlProfile;
        }
      }

      // Extract the canonical job title from the job_title section.
      // Old format: job_title.job_title was a single canonical role string.
      if (!mlProfileRole && jobTitleSection && jobTitleSection.job_title) {
        if (typeof jobTitleSection.job_title === 'string') {
          mlProfileRole = jobTitleSection.job_title.trim();
        }
      }
      // Backward compat: old files stored it as job_title.role
      if (!mlProfileRole && jobTitleSection && jobTitleSection.role) {
        mlProfileRole = String(jobTitleSection.role).trim();
      }

      // seniority and job family come from the job_title section.
      if (jobTitleSection) {
        const jtRaw = jobTitleSection.job_title;
        const jtDist = (jtRaw && typeof jtRaw === 'object' && !Array.isArray(jtRaw) ? jtRaw : null)
          || jobTitleSection.job_title_distribution || null;
        if (jtDist) {
          mlDefaults.jobTitleProfileMap = jtDist;
        }
        const titleEntry = jtDist && mlProfileRole
          ? (jtDist[mlProfileRole] ||
             Object.entries(jtDist).find(([k]) => k.toLowerCase() === (mlProfileRole || '').toLowerCase())?.[1] ||
             null)
          : null;
        const seniorityObj = (titleEntry && titleEntry.Seniority)
          || jobTitleSection.Seniority || jobTitleSection.seniority_distribution || null;
        if (seniorityObj) mlDefaults.seniority = topKey(seniorityObj);
        const jobFamilyObj = (titleEntry && titleEntry.job_family && typeof titleEntry.job_family === 'object' ? titleEntry.job_family : null)
          || (typeof jobTitleSection.job_family === 'object' ? jobTitleSection.job_family : null)
          || jobTitleSection.job_family_distribution || null;
        if (jobFamilyObj) mlDefaults.jobfamily = topKey(jobFamilyObj);
      }
    }

    // Sector: build a per-company → sector lookup map from the company section's sector map.
    // Supported formats (checked in priority order):
    //   1. sector: { sectorName: { companyName: confidence } } — new sector-first objects (primary)
    //   2. sector: { companyName: [sectorName, ...] }   — old dock-out company-first arrays
    //   3. sector_distribution: { companyName: { sectorName: count } } — legacy company-first objects
    const companySection = mlProfile.company || null;
    const sectorMap = (companySection && companySection.sector) || null;
    const sectorDist = (companySection && companySection.sector_distribution) || null;
    const companyDist = (companySection && companySection.company_distribution) || (mlProfile.company_distribution) || null;
    if (sectorMap && typeof sectorMap === 'object') {
      const sectorMapEntries = Object.entries(sectorMap);
      if (sectorMapEntries.length > 0) {
      const firstVal = sectorMapEntries[0][1];
      const sectorWeights = {};
      mlDefaults._companySectorMap = mlDefaults._companySectorMap || {};
      if (typeof firstVal === 'object' && !Array.isArray(firstVal) && firstVal !== null) {
        // New sector-first format: { sectorName: { companyName: confidence } }
        for (const [sectorName, companyData] of sectorMapEntries) {
          if (typeof companyData !== 'object' || companyData === null) continue;
          for (const [companyName, conf] of Object.entries(companyData)) {
            if (companyName) mlDefaults._companySectorMap[companyName.toLowerCase()] = sectorName;
            sectorWeights[sectorName] = (sectorWeights[sectorName] || 0) + (Number(conf) || 1);
          }
        }
      } else if (Array.isArray(firstVal)) {
        // Old company-first format: { companyName: [sectorName, ...] }
        for (const [companyName, sectorList] of sectorMapEntries) {
          if (!Array.isArray(sectorList) || sectorList.length === 0) continue;
          // Map company → first (primary) sector; count all sector associations for global weight
          mlDefaults._companySectorMap[companyName.toLowerCase()] = sectorList[0];
          for (const s of sectorList) sectorWeights[s] = (sectorWeights[s] || 0) + 1;
        }
      }
      const topSector = topKey(sectorWeights);
      if (topSector) mlDefaults.sector = topSector;  // global fallback when company not found
      }
    } else if (sectorDist && typeof sectorDist === 'object') {
      // Backward compat: legacy company-first { companyName: { sectorName: proportion/count } }
      const sectorWeights = {};
      for (const [companyName, sectors] of Object.entries(sectorDist)) {
        if (typeof sectors === 'object' && sectors !== null && Object.keys(sectors).length > 0) {
          const dominantSector = Object.entries(sectors).reduce((best, [s, w]) => w > best[1] ? [s, w] : best, ['', -1])[0] || null;
          if (dominantSector) {
            mlDefaults._companySectorMap = mlDefaults._companySectorMap || {};
            mlDefaults._companySectorMap[companyName.toLowerCase()] = dominantSector;
            sectorWeights[dominantSector] = (sectorWeights[dominantSector] || 0) + 1;
          }
        }
      }
      const topSector = topKey(sectorWeights);
      if (topSector) mlDefaults.sector = topSector;
    } else if (companyDist && typeof companyDist === 'object') {
      // Backward compat: old company_distribution with "CompanyName – SectorName" flat keys
      const sectorWeights = {};
      for (const [key, weight] of Object.entries(companyDist)) {
        const sepIdx = key.indexOf(' \u2013 ');  // en-dash separator
        if (sepIdx !== -1) {
          const company = key.slice(0, sepIdx).trim().toLowerCase();
          const sector  = key.slice(sepIdx + 3).trim();
          if (company && sector) mlDefaults._companySectorMap = mlDefaults._companySectorMap || {};
          if (company && sector) mlDefaults._companySectorMap[company] = sector;
          if (sector) sectorWeights[sector] = (sectorWeights[sector] || 0) + (Number(weight) || 0);
        }
      }
      const topSector = topKey(sectorWeights);
      if (topSector) mlDefaults.sector = topSector;  // global fallback when company not found
    }
    // Legacy flat format: sector_preferences is a direct property
    if (!mlDefaults.sector) {
      const legacySection = mlProfile.job_title || mlProfile;
      if (legacySection && legacySection.sector_preferences) {
        mlDefaults.sector = topKey(legacySection.sector_preferences);
      }
    }
    // country and sourcing_status are intentionally excluded — LLM handles those
  }

  try {
    // Construct prompt for batch
    // We send subset of fields: id, organisation, jobtitle (or role), seniority, geographic, country
    const lines = rows.map(r => {
      const org = r.organisation || r.company || '';
      const title = r.jobtitle || r.role || '';
      const sen = r.seniority || '';
      const geo = r.geographic || '';
      const country = r.country || '';
      return JSON.stringify({ id: r.id, org, title, sen, geo, country });
    });

    const prompt = `
      You are a data standardization assistant.
      I will provide a JSON list of candidate records with fields: id, org (company), title (job title), sen (seniority), geo (geographic region), country.
      
      Your task:
      1. Standardize "org" to the canonical company name (e.g. "Tencent Gaming" -> "Tencent", "Tencent Cloud" -> "Tencent", "Mihoyo Co Ltd" -> "Mihoyo").
      2. Standardize "title" to a standard job title (e.g. "Cloud Specialist" -> "Cloud Engineer", "Cloud Developer" -> "Cloud Engineer", but "Cloud Architect" remains "Cloud Architect").
      3. IMPORTANT: Validate and standardize "sen" (seniority) against the "title" (job title) field. Ensure the seniority is consistent with the job title. For example:
         - If title contains "Senior", seniority should be "Senior"
         - If title contains "Lead", seniority should be "Lead"
         - If title contains "Manager", seniority should be "Manager"
         - If title contains "Director", seniority should be "Director"
         - If title contains "Junior" or "Jr", seniority should be "Junior"
         - If no seniority indicators in title, infer from context or keep existing seniority
         - Standardize to one of: Junior, Mid, Senior, Lead, Manager, Director, Expert, Executive
      4. Standardize "country" to canonical country names (e.g. "South Korea" -> "Korea", "USA" -> "United States").
      5. Infer "sector" from the company name (org). Use the industry sector the company operates in (e.g. "Pfizer" -> "Pharmaceuticals", "Roche" -> "Biotechnology", "Medpace" -> "Clinical Research Organisation", "McKinsey" -> "Consulting", "Goldman Sachs" -> "Financial Services"). If unknown, leave blank.
      6. Return a JSON list of objects with keys: "id", "organisation" (standardized), "jobtitle" (standardized), "seniority" (standardized), "country" (standardized), "sector" (inferred from company).
      7. IMPORTANT: Return ONLY the JSON. No markdown formatting.

      Input:
      [${lines.join(',\n')}]
    `;

    const text = await llmGenerateText(prompt, { username: req.user && req.user.username, label: 'llm/sync-entries' });
    incrementGeminiQueryCount(req.user.username).catch(() => {});

    // Clean potential markdown blocks
    const jsonStr = text.replace(_RE_CODE_FENCE, '').trim();
    let data;
    try {
      data = JSON.parse(jsonStr);
    } catch (e) {
      // If direct array parse fails, try finding array bracket
      const match = text.match(/\[.*\]/s);
      if (match) {
        data = JSON.parse(match[0]);
      } else {
        throw new Error("Failed to parse Gemini response");
      }
    }

    if (!Array.isArray(data)) {
        throw new Error("Gemini response is not an array");
    }

    // Apply our local normalization functions to the Gemini results,
    // then fill any still-empty fields with ML-profile highest-confidence values.
    const normalized = data.map(item => {
      const result = { ...item };

      // Apply company normalization with special character removal
      if (result.organisation) {
        result.organisation = normalizeCompanyName(result.organisation);
      }
      
      // Job title is completely immutable — Sync Entries must never change any job title value.
      // The result.jobtitle from Gemini is intentionally discarded; it is not applied to the output.

      // Apply country normalization using countrycode.JSON
      if (result.country) {
        result.country = normalizeCountry(result.country);
      }

      // === Sector assignment (unconditional — company-based, not role-gated) ===
      // ML sector_distribution is always the primary source. Gemini's inferred sector is the
      // fallback only when the candidate's company is not found in the ML map.
      // Sector is deliberately NOT gated by job-title match because sector is a company attribute,
      // independent of what role the candidate holds.
      {
        const orgLower = (result.organisation || '').trim().toLowerCase();
        const companySectorMap = mlDefaults._companySectorMap || {};
        let mlSector = (orgLower && companySectorMap[orgLower]) || null;
        if (!mlSector && orgLower) {
          // Partial match: find a map entry whose key contains or is contained by the org name
          for (const [company, sector] of Object.entries(companySectorMap)) {
            if (company.length >= 4 && (orgLower.includes(company) || company.includes(orgLower))) {
              mlSector = sector;
              break;
            }
          }
        }
        // ML map wins when company is found; fall through to Gemini-inferred sector otherwise.
        // Do NOT use a global ML top-sector fallback — if the company is unknown to ML, Gemini's
        // per-company inference is the correct authority for that record.
        result.sector = mlSector || result.sector || '';
      }

      // === Seniority and Job Family (role-gated — only when job title matches ML profile) ===
      // Seniority and Job Family are tied to the candidate's specific job title.
      // New format: jobTitleProfileMap is a dict of all job titles with per-title distributions.
      // Old format: single mlProfileRole with mlDefaults.seniority / mlDefaults.jobfamily.
      const effectiveCandidateTitle = originalTitleMap[result.id] || result.jobtitle || '';
      const candidateJobTitle = effectiveCandidateTitle.toLowerCase();
      const jobTitleProfileMap = mlDefaults.jobTitleProfileMap || null;
      if (jobTitleProfileMap && candidateJobTitle) {
        // New format: look up the candidate's specific job title in the per-title map.
        const titleEntry = jobTitleProfileMap[effectiveCandidateTitle]
          || Object.entries(jobTitleProfileMap).find(([k]) => k.toLowerCase() === candidateJobTitle)?.[1]
          || null;
        if (titleEntry) {
          if (titleEntry.Seniority && typeof titleEntry.Seniority === 'object') {
            const topSen = topKey(titleEntry.Seniority);
            if (topSen) result.seniority = standardizeSeniority(topSen) || topSen;
          }
          if (titleEntry.job_family && typeof titleEntry.job_family === 'object') {
            const topJF = topKey(titleEntry.job_family);
            if (topJF) result.jobfamily = topJF;
          }
        }
      } else {
        // Old format fallback: single-role gating
        const mlRoleNormalized = mlProfileRole ? mlProfileRole.toLowerCase() : null;
        const jobTitleMatchesMLRole = !!(
          mlRoleNormalized && candidateJobTitle && (
            candidateJobTitle === mlRoleNormalized ||
            (candidateJobTitle.length >= 6 && mlRoleNormalized.includes(candidateJobTitle)) ||
            (mlRoleNormalized.length >= 6 && candidateJobTitle.includes(mlRoleNormalized))
          )
        );
        if (jobTitleMatchesMLRole) {
          if (mlDefaults.seniority) {
            result.seniority = standardizeSeniority(mlDefaults.seniority) || mlDefaults.seniority;
          }
          if (mlDefaults.jobfamily) result.jobfamily = mlDefaults.jobfamily;
        }
      }
      
      return result;
    });

    if (_isAsync) {
      broadcastSSE('verify_data_complete', { corrected: normalized });
    } else {
      res.json({ corrected: normalized, mlDefaults: Object.keys(mlDefaults).length ? mlDefaults : undefined });
    }

  } catch (err) {
    console.error('/verify-data error:', err);
    if (!_isAsync) {
      res.status(500).json({ error: 'Verification failed', detail: err.message });
    }
  }
});

/**
 * ========== AI Compensation Estimation via Gemini ==========
 * Endpoint: POST /ai-comp
 * Body: { ids: [number, ...], selectAll: boolean }
 *   - ids: specific record IDs to estimate compensation for
 *   - selectAll: if true, applies to all records owned by the user
 * Response: { updatedCount: number, rows: [...] }
 *
 * Records with an existing compensation value are skipped.
 * Inputs sent to Gemini: company, jobtitle, seniority, country, sector.
 */
app.post('/ai-comp', requireLogin, userRateLimit('ai_comp'), async (req, res) => {
  const { ids, selectAll } = req.body;

  try {
    let rows;
    if (selectAll) {
      const result = await pool.query(
        'SELECT id, company, jobtitle, seniority, country, sector, compensation FROM "process" WHERE userid = $1',
        [String(req.user.id)]
      );
      rows = result.rows;
    } else {
      if (!Array.isArray(ids) || ids.length === 0) {
        return res.status(400).json({ error: 'No ids provided.' });
      }
      const safeIds = ids.map(Number).filter(n => Number.isInteger(n) && n > 0);
      if (!safeIds.length) {
        return res.status(400).json({ error: 'No valid ids provided.' });
      }
      const placeholders = safeIds.map((_, i) => `$${i + 2}`).join(', ');
      const result = await pool.query(
        `SELECT id, company, jobtitle, seniority, country, sector, compensation FROM "process" WHERE userid = $1 AND id IN (${placeholders})`,
        [String(req.user.id), ...safeIds]
      );
      rows = result.rows;
    }

    // Skip records that already have compensation data
    const pending = rows.filter(r => r.compensation === null || r.compensation === undefined || r.compensation === '');
    if (!pending.length) {
      return res.json({ updatedCount: 0, rows: [], message: 'All selected records already have compensation data.' });
    }

    // Serve from cache for any rows whose profile key is already cached
    const uncached = [];
    const cacheHits = []; // { id, compensation }
    for (const r of pending) {
      const cached = _aiCompCacheGet(r);
      if (cached !== undefined) {
        cacheHits.push({ id: r.id, compensation: cached });
      } else {
        uncached.push(r);
      }
    }

    let data = cacheHits;

    // Helper: build LLM prompt lines from a list of rows
    const _buildCompPrompt = rows => `
You are a compensation estimation assistant.
I will provide a JSON list of candidate records with fields: id, company, jobtitle, seniority, country, sector.

Your task:
Estimate the annual total compensation (in USD) for each candidate based on their company, job title, seniority level, country, and industry sector.
Use your knowledge of typical market salaries and compensation benchmarks.

Rules:
1. Return a JSON array of objects with exactly two keys: "id" (integer) and "compensation" (number, annual USD, no currency symbol).
2. compensation must be a plain number (e.g. 120000), not a string or range.
3. If you cannot determine a reasonable estimate for a record, use null.
4. Return ONLY the JSON array. No markdown, no explanation.

Input:
[${rows.map(r => JSON.stringify({ id: r.id, company: r.company || '', jobtitle: r.jobtitle || '', seniority: r.seniority || '', country: r.country || '', sector: r.sector || '' })).join(',\n')}]
    `.trim();

    // Helper: run UNNEST batch UPDATE and return mapped rows
    const _execCompUpdate = async (items, userId) => {
      const validItems = items.filter(item => {
        const id = Number(item?.id);
        const comp = Number(item?.compensation);
        return Number.isInteger(id) && id > 0 && item.compensation != null && !isNaN(comp);
      });
      if (validItems.length === 0) return [];
      const batchIds = [], batchComps = [];
      for (const item of validItems) { batchIds.push(Number(item.id)); batchComps.push(Number(item.compensation)); }
      const batchRes = await pool.query(
        `UPDATE "process" AS p
         SET compensation = v.comp
         FROM UNNEST($1::int[], $2::double precision[]) AS v(id, comp)
         WHERE p.id = v.id AND p.userid = $3
         RETURNING p.*`,
        [batchIds, batchComps, userId]
      );
      return batchRes.rows.map(r => ({
        ...r,
        compensation: r.compensation ?? null,
        pic: picToDataUri(r.pic),
        role: r.role ?? r.jobtitle ?? null,
        organisation: normalizeCompanyName(r.company || r.organisation) ?? (r.organisation ?? r.company ?? null),
        jobtitle: r.jobtitle ?? null,
        company: normalizeCompanyName(r.company || r.organisation) ?? (r.company ?? null),
      }));
    };

    // Helper: call LLM for uncached rows, populate cache, return merged data array
    const _runCompLLM = async (uncachedRows, username) => {
      const text = await llmGenerateText(_buildCompPrompt(uncachedRows), { username, label: 'llm/ai-comp' });
      incrementGeminiQueryCount(username).catch(() => {});
      const jsonStr = text.replace(_RE_CODE_FENCE, '').trim();
      let geminiData;
      try {
        geminiData = JSON.parse(jsonStr);
      } catch (_) {
        const match = text.match(/\[.*\]/s);
        if (match) {
          geminiData = JSON.parse(match[0]);
        } else {
          throw new Error(`LLM response could not be parsed for compensation. Response (truncated): ${text.slice(0, 200)}`);
        }
      }
      if (!Array.isArray(geminiData)) throw new Error('LLM response is not an array.');
      for (const item of geminiData) {
        const row = uncachedRows.find(r => r.id === item.id);
        if (row && item.compensation != null) _aiCompCacheSet(row, item.compensation);
      }
      return geminiData;
    };

    // ── Background path: uncached batch too large to block the response ──────
    // Return immediately with whatever was served from cache; run the LLM call
    // in the background and notify clients via SSE when it completes.
    if (uncached.length > _AI_COMP_ASYNC_THRESHOLD) {
      const bgUsername = req.user.username;
      const bgUserid = String(req.user.id);

      // Flush cache hits to DB right now so the caller gets immediate data.
      let immediateRows = [];
      if (cacheHits.length > 0) {
        immediateRows = await _execCompUpdate(cacheHits, bgUserid);
        if (immediateRows.length > 0) {
          try {
            broadcastSSE('candidates_changed', { action: 'ai_comp_partial', count: immediateRows.length });
            broadcastSSEBulk(immediateRows);
          } catch (_) { /* ignore */ }
        }
      }

      res.status(cacheHits.length > 0 ? 200 : 202).json({
        updatedCount: immediateRows.length,
        rows: immediateRows,
        pending: uncached.length,
        message: [
          immediateRows.length > 0 ? `${immediateRows.length} cached` : '',
          `${uncached.length} AI estimates in progress…`,
        ].filter(Boolean).join(', '),
      });

      // Fire-and-forget: LLM call + DB update + SSE for uncached rows
      ;(async () => {
        try {
          const geminiData = await _runCompLLM(uncached, bgUsername);
          const bgRows = await _execCompUpdate(geminiData, bgUserid);
          if (bgRows.length > 0) {
            try {
              broadcastSSE('candidates_changed', { action: 'ai_comp', count: bgRows.length });
              broadcastSSEBulk(bgRows);
            } catch (_) { /* ignore */ }
          }
          _writeApprovalLog({ action: 'ai_comp', username: bgUsername, userid: bgUserid, detail: `AI Comp (bg) updated ${bgRows.length} records`, source: 'server_routes2.js' });
        } catch (e) {
          console.error('[AI-COMP BG] error:', e && e.message);
        }
      })().catch(() => {});
      return;
    }

    // ── Synchronous path: small uncached batch, block until LLM responds ─────
    if (uncached.length > 0) {
      const geminiData = await _runCompLLM(uncached, req.user && req.user.username);
      data = [...data, ...geminiData];
    }

    if (!Array.isArray(data)) {
      throw new Error('Gemini response is not an array.');
    }

    // Batch UPDATE compensation using UNNEST — single roundtrip instead of one per row
    const updatedRows = await _execCompUpdate(data, String(req.user.id));

    // Broadcast changes
    try {
      broadcastSSE('candidates_changed', { action: 'ai_comp', count: updatedRows.length });
      broadcastSSEBulk(updatedRows);
    } catch (_) { /* ignore */ }

    _writeApprovalLog({ action: 'ai_comp', username: req.user.username, userid: req.user.id, detail: `AI Comp updated ${updatedRows.length} records`, source: 'server_routes2.js' });
    res.json({ updatedCount: updatedRows.length, rows: updatedRows });

  } catch (err) {
    console.error('/ai-comp error:', err);
    res.status(500).json({ error: 'AI compensation estimation failed.', detail: err.message });
  }
});

// ========== Crowd Compensation Lookup ==========

/**
 * Endpoint: POST /crowd-comp
 * Body: { ids: [number, ...], selectAll: boolean }
 * Looks up each candidate's compensation in ML_Master_Compensation.json by matching
 * Job Title, Job Family (optional), and Country against the "Verified Compensation" entries.
 * Matched records are updated in the DB (average of Range Min / Range Max).
 * Response: { rows: [{ id, min, max, avg, count, compensation }] }
 */
app.post('/crowd-comp', requireLogin, userRateLimit('ai_comp'), async (req, res) => {
  const { ids, selectAll } = req.body;
  try {
    let rows;
    if (selectAll) {
      const result = await pool.query(
        'SELECT id, jobtitle, jobfamily, country FROM "process" WHERE userid = $1',
        [String(req.user.id)]
      );
      rows = result.rows;
    } else {
      if (!Array.isArray(ids) || ids.length === 0) {
        return res.status(400).json({ error: 'No ids provided.' });
      }
      const safeIds = ids.map(Number).filter(n => Number.isInteger(n) && n > 0);
      if (!safeIds.length) return res.status(400).json({ error: 'No valid ids provided.' });
      const placeholders = safeIds.map((_, i) => `$${i + 2}`).join(', ');
      const result = await pool.query(
        `SELECT id, jobtitle, jobfamily, country FROM "process" WHERE userid = $1 AND id IN (${placeholders})`,
        [String(req.user.id), ...safeIds]
      );
      rows = result.rows;
    }

    // Load ML_Master_Compensation.json (cached, with Map index for O(1) lookup)
    const { compMap } = _loadCompMasterCached();

    // Match each row against Verified Compensation data
    const matched = [];
    for (const row of rows) {
      const jtNorm = _normCompTitle(row.jobtitle);
      const jfNorm = _normCompTitle(row.jobfamily);
      const ctryNorm = _normCompTitle(row.country);
      if (!jtNorm) continue;

      // O(1) lookup via pre-built Map (was O(n) linear scan)
      const jtEntry = compMap.get(jtNorm);
      if (!jtEntry || typeof jtEntry !== 'object') continue;

      // Job family must match when both candidate and entry have values
      if (jfNorm && jtEntry.job_family && _normCompTitle(jtEntry.job_family) !== jfNorm) continue;

      // Find matching country in Verified Compensation
      const vcEntries = Array.isArray(jtEntry['Verified Compensation']) ? jtEntry['Verified Compensation'] : [];
      const vcMatch = vcEntries.find(e => _normCompTitle(e.country) === ctryNorm);
      if (!vcMatch) continue;

      const min = Number(vcMatch.min) || 0;
      const max = Number(vcMatch.max) || 0;
      if (!min && !max) continue;
      const avg = Math.round((min + max) / 2);

      matched.push({ id: row.id, min, max, avg, count: Number(vcMatch.count) || 0, compensation: avg });
    }

    if (matched.length === 0) {
      return res.json({ rows: [] });
    }

    // Batch UPDATE compensation using UNNEST — single roundtrip instead of one per row
    const updatedRows = [];
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const batchIds = matched.map(item => item.id);
      const batchComps = matched.map(item => item.avg);
      const batchRes = await client.query(
        `UPDATE "process" AS p
         SET compensation = v.comp
         FROM UNNEST($1::int[], $2::double precision[]) AS v(id, comp)
         WHERE p.id = v.id AND p.userid = $3
         RETURNING p.*`,
        [batchIds, batchComps, String(req.user.id)]
      );
      // Build a lookup from the matched array to carry through min/max/count
      const matchedById = new Map(matched.map(m => [m.id, m]));
      for (const r of batchRes.rows) {
        const m = matchedById.get(r.id) || {};
        updatedRows.push({
          ...m,
          compensation: r.compensation ?? m.avg,
          pic: picToDataUri(r.pic),
          role: r.role ?? r.jobtitle ?? null,
          organisation: normalizeCompanyName(r.company || r.organisation) ?? (r.organisation ?? r.company ?? null),
          jobtitle: r.jobtitle ?? null,
          company: normalizeCompanyName(r.company || r.organisation) ?? (r.company ?? null),
        });
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    try {
      broadcastSSE('candidates_changed', { action: 'crowd_comp', count: updatedRows.length });
      broadcastSSEBulk(updatedRows);
    } catch (_) { /* ignore */ }

    _writeApprovalLog({ action: 'crowd_comp', username: req.user.username, userid: req.user.id, detail: `Crowd Comp updated ${updatedRows.length} records`, source: 'server.js' });
    res.json({ rows: updatedRows });
  } catch (err) {
    console.error('/crowd-comp error:', err);
    res.status(500).json({ error: 'Crowd compensation lookup failed.', detail: err.message });
  }
});

// ========== NEW: Calendar & Google Meet Integration ==========

// Helper to create an OAuth2 client for Google using googleapis and persisted tokens for a username.
// Returns oauth2Client or throws error.
async function getOAuthClientForUser(username) {
  if (!google) throw new Error('googleapis module not available');
  const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
  const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
  const GOOGLE_REDIRECT_URI = process.env.GOOGLE_CALENDAR_REDIRECT || (process.env.GOOGLE_REDIRECT_URI || 'http://localhost:4000/auth/google/calendar/callback');

  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET) {
    throw new Error('Google OAuth client not configured in environment.');
  }

  const oauth2Client = new google.auth.OAuth2(
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI
  );

  // Fetch stored refresh token
  try {
    const r = await pool.query('SELECT google_refresh_token FROM login WHERE username = $1', [username]);
    if (r.rows.length > 0 && r.rows[0].google_refresh_token) {
      oauth2Client.setCredentials({ refresh_token: r.rows[0].google_refresh_token });
    }
  } catch (e) {
    console.warn('[OAUTH] failed to load refresh token for user', username, e && e.message);
  }

  // Listen for new tokens and persist refresh token if provided (idempotent)
  oauth2Client.on && oauth2Client.on('tokens', async (tokens) => {
    if (tokens.refresh_token) {
      try {
        await pool.query('UPDATE login SET google_refresh_token = $1 WHERE username = $2', [tokens.refresh_token, username]);
      } catch (e) {
        console.warn('[OAUTH] failed to persist new refresh token', e && e.message);
      }
    }
    // Optionally persist access token expiry if you want
    if (tokens.expiry_date) {
      try {
        const dt = new Date(tokens.expiry_date);
        await pool.query('UPDATE login SET google_token_expires = $1 WHERE username = $2', [dt.toISOString(), username]);
      } catch (e) {
        // ignore
      }
    }
  });

  return oauth2Client;
}

// Route: start OAuth flow to connect Google Calendar for current logged in user
app.get('/auth/google/calendar/connect', requireLogin, async (req, res) => {
  if (!google) return res.status(500).send('Google APIs not available on server.');
  const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
  const GOOGLE_REDIRECT_URI = process.env.GOOGLE_CALENDAR_REDIRECT || (process.env.GOOGLE_REDIRECT_URI || 'http://localhost:4000/auth/google/calendar/callback');
  if (!GOOGLE_CLIENT_ID) return res.status(500).send('GOOGLE_CLIENT_ID not configured.');

  const oauth2Client = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI
  );

  // Scopes for creating events and reading freebusy
  const scopes = [
    'https://www.googleapis.com/auth/calendar.events',
    'https://www.googleapis.com/auth/calendar'
  ];

  const url = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: scopes,
    prompt: 'consent',
    state: req.user.username // carry username through callback
  });

  res.redirect(url);
});

// Callback: exchange code and persist refresh token to login table
app.get('/auth/google/calendar/callback', requireLogin, async (req, res) => {
  if (!google) return res.status(500).send('Google APIs not available on server.');
  const code = req.query.code;
  const state = req.query.state; // username passed back
  if (!code) return res.status(400).send('Missing code');

  try {
    const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
    const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
    const GOOGLE_REDIRECT_URI = process.env.GOOGLE_CALENDAR_REDIRECT || (process.env.GOOGLE_REDIRECT_URI || 'http://localhost:4000/auth/google/calendar/callback');

    const oauth2Client = new google.auth.OAuth2(
      GOOGLE_CLIENT_ID,
      GOOGLE_CLIENT_SECRET,
      GOOGLE_REDIRECT_URI
    );

    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);

    // Persist refresh_token if present; prefer req.user.username but fallback to state
    const username = req.user && req.user.username ? req.user.username : state;
    if (!username) {
      return res.status(400).send('Cannot determine username to persist OAuth tokens.');
    }

    if (tokens.refresh_token) {
      await pool.query('UPDATE login SET google_refresh_token = $1, google_token_expires = $2 WHERE username = $3', [tokens.refresh_token, tokens.expiry_date ? new Date(tokens.expiry_date).toISOString() : null, username]);
    } else {
      // If no refresh token was returned (possible if already granted and offline access not requested), we can still persist expiry info
      if (tokens.expiry_date) {
        await pool.query('UPDATE login SET google_token_expires = $1 WHERE username = $2', [new Date(tokens.expiry_date).toISOString(), username]);
      }
    }

    // Show a friendly success message (frontend typically navigates here in the popup)
    res.send(`<html><head><meta charset="utf-8"></head><body><h3>Google Calendar connected for ${_escHtml(username)}</h3><p>You can close this window and return to the app.</p><script>window.close()</script></body></html>`);
  } catch (err) {
    console.error('/auth/google/calendar/callback error', err);
    res.status(500).send('OAuth callback failed: ' + (err.message || 'unknown'));
  }
});

// ========== Microsoft Calendar & Teams Integration ==========

// Minimal HTML escaper for user-supplied values embedded in success/error pages
function _escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Make an authenticated request to Microsoft Graph API
async function _msGraphRequest(method, graphPath, accessToken, body, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const bodyStr = body ? JSON.stringify(body) : null;
    const options = {
      hostname: 'graph.microsoft.com',
      path: '/v1.0' + graphPath,
      method: method,
      headers: {
        'Authorization': 'Bearer ' + accessToken,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        ...extraHeaders
      }
    };
    if (bodyStr) options.headers['Content-Length'] = Buffer.byteLength(bodyStr);
    const req = https.request(options, (res) => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        if (!raw || res.statusCode === 204) { resolve({}); return; }
        try {
          const parsed = JSON.parse(raw);
          if (parsed.error) {
            reject(new Error(parsed.error.message || JSON.stringify(parsed.error)));
          } else {
            resolve(parsed);
          }
        } catch (e) {
          reject(new Error('Invalid JSON from Graph API: ' + raw.slice(0, 200)));
        }
      });
    });
    req.on('error', reject);
    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

// Exchange a Microsoft refresh token for a fresh access token, persisting any new tokens
async function getMicrosoftTokenForUser(username) {
  const MS_CLIENT_ID     = process.env.MICROSOFT_CLIENT_ID;
  const MS_CLIENT_SECRET = process.env.MICROSOFT_CLIENT_SECRET;
  if (!MS_CLIENT_ID || !MS_CLIENT_SECRET) {
    throw new Error('Microsoft OAuth client not configured (MICROSOFT_CLIENT_ID / MICROSOFT_CLIENT_SECRET).');
  }
  const r = await pool.query('SELECT ms_refresh_token FROM login WHERE username = $1', [username]);
  if (!r.rows.length || !r.rows[0].ms_refresh_token) {
    throw new Error('Microsoft Calendar not connected. Please click "Connect Microsoft" first.');
  }
  const params = new URLSearchParams({
    client_id:     MS_CLIENT_ID,
    client_secret: MS_CLIENT_SECRET,
    refresh_token: r.rows[0].ms_refresh_token,
    grant_type:    'refresh_token',
    scope:         'offline_access Calendars.ReadWrite OnlineMeetings.ReadWrite'
  });
  const bodyStr = params.toString();
  const tokenData = await new Promise((resolve, reject) => {
    const options = {
      hostname: 'login.microsoftonline.com',
      path: '/common/oauth2/v2.0/token',
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(bodyStr)
      }
    };
    const req = https.request(options, (res) => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); }
        catch (e) { reject(new Error('Invalid JSON from MS token endpoint')); }
      });
    });
    req.on('error', reject);
    req.write(bodyStr);
    req.end();
  });
  if (tokenData.error) {
    throw new Error('Microsoft token refresh failed: ' + (tokenData.error_description || tokenData.error));
  }
  if (tokenData.refresh_token) {
    await pool.query('UPDATE login SET ms_refresh_token = $1 WHERE username = $2', [tokenData.refresh_token, username]);
  }
  if (tokenData.expires_in) {
    const expiresAt = new Date(Date.now() + tokenData.expires_in * 1000).toISOString();
    await pool.query('UPDATE login SET ms_token_expires = $1 WHERE username = $2', [expiresAt, username]);
  }
  return tokenData.access_token;
}

// Route: start Microsoft OAuth flow for Calendar + Teams
app.get('/auth/microsoft/calendar/connect', requireLogin, (req, res) => {
  const MS_CLIENT_ID    = process.env.MICROSOFT_CLIENT_ID;
  const MS_REDIRECT_URI = process.env.MICROSOFT_CALENDAR_REDIRECT || 'http://localhost:4000/auth/microsoft/calendar/callback';
  if (!MS_CLIENT_ID) return res.status(500).send('MICROSOFT_CLIENT_ID not configured.');
  const params = new URLSearchParams({
    client_id:     MS_CLIENT_ID,
    response_type: 'code',
    redirect_uri:  MS_REDIRECT_URI,
    response_mode: 'query',
    scope:         'offline_access Calendars.ReadWrite OnlineMeetings.ReadWrite',
    state:         req.user.username,
    prompt:        'select_account'
  });
  res.redirect('https://login.microsoftonline.com/common/oauth2/v2.0/authorize?' + params.toString());
});

// Callback: exchange code and persist Microsoft refresh token
app.get('/auth/microsoft/calendar/callback', requireLogin, async (req, res) => {
  const code  = req.query.code;
  const state = req.query.state;
  if (!code) return res.status(400).send('Missing code');
  try {
    const MS_CLIENT_ID    = process.env.MICROSOFT_CLIENT_ID;
    const MS_CLIENT_SECRET = process.env.MICROSOFT_CLIENT_SECRET;
    const MS_REDIRECT_URI  = process.env.MICROSOFT_CALENDAR_REDIRECT || 'http://localhost:4000/auth/microsoft/calendar/callback';
    const params = new URLSearchParams({
      client_id:     MS_CLIENT_ID,
      client_secret: MS_CLIENT_SECRET,
      code,
      redirect_uri:  MS_REDIRECT_URI,
      grant_type:    'authorization_code',
      scope:         'offline_access Calendars.ReadWrite OnlineMeetings.ReadWrite'
    });
    const bodyStr = params.toString();
    const tokenData = await new Promise((resolve, reject) => {
      const options = {
        hostname: 'login.microsoftonline.com',
        path: '/common/oauth2/v2.0/token',
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(bodyStr)
        }
      };
      const req2 = https.request(options, (r) => {
        let raw = '';
        r.on('data', d => raw += d);
        r.on('end', () => {
          try { resolve(JSON.parse(raw)); }
          catch (e) { reject(new Error('Invalid JSON from MS token endpoint')); }
        });
      });
      req2.on('error', reject);
      req2.write(bodyStr);
      req2.end();
    });
    if (tokenData.error) {
      return res.status(400).send('Microsoft OAuth failed: ' + (tokenData.error_description || tokenData.error));
    }
    const username = (req.user && req.user.username) ? req.user.username : state;
    if (!username) return res.status(400).send('Cannot determine username.');
    const expiresAt = tokenData.expires_in ? new Date(Date.now() + tokenData.expires_in * 1000).toISOString() : null;
    await pool.query(
      'UPDATE login SET ms_refresh_token = $1, ms_token_expires = $2 WHERE username = $3',
      [tokenData.refresh_token || null, expiresAt, username]
    );
    res.send(`<html><head><meta charset="utf-8"></head><body><h3>Microsoft Calendar connected for ${_escHtml(username)}</h3><p>You can close this window and return to the app.</p><script>window.close()</script></body></html>`);
  } catch (err) {
    console.error('/auth/microsoft/calendar/callback error', err);
    res.status(500).send('OAuth callback failed: ' + (err.message || 'unknown'));
  }
});

// ========== END Microsoft Calendar & Teams Integration ==========

// Utility to build ICS content for event (METHOD:REQUEST recommended)
function buildICS({uid, startISO, endISO, summary, description = '', organizerEmail, attendees = [], timezone = 'UTC', meetLink = '' }) {
  // Convert ISO date to ICS timestamp (UTC) format: YYYYMMDDTHHMMSSZ
  function toUTCStamp(dtISO) {
    const d = new Date(dtISO);
    if (isNaN(d.getTime())) return '';
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    const hh = String(d.getUTCHours()).padStart(2, '0');
    const mi = String(d.getUTCMinutes()).padStart(2, '0');
    const ss = String(d.getUTCSeconds()).padStart(2, '0');
    return `${yyyy}${mm}${dd}T${hh}${mi}${ss}Z`;
  }

  const dtstamp = toUTCStamp(new Date().toISOString());
  const dtstart = toUTCStamp(startISO);
  const dtend = toUTCStamp(endISO);
  const safeSummary = (summary || '').replace(/\r\n/g, '\\n').replace(/\n/g, '\\n');
  const safeDesc = (description || '').replace(/\r\n/g, '\\n').replace(/\n/g, '\\n');
  const organizer = organizerEmail ? `ORGANIZER;CN="Organizer":mailto:${organizerEmail}` : '';

  const lines = [
    'BEGIN:VCALENDAR',
    'PRODID:-//CandidateManagement//EN',
    'VERSION:2.0',
    'CALSCALE:GREGORIAN',
    'METHOD:REQUEST',
    'BEGIN:VEVENT',
    `UID:${uid}`,
    `DTSTAMP:${dtstamp}`,
    dtstart ? `DTSTART:${dtstart}` : '',
    dtend ? `DTEND:${dtend}` : '',
    `SUMMARY:${safeSummary}`,
    `DESCRIPTION:${safeDesc}`,
    meetLink ? `LOCATION:${meetLink}` : '',
    organizer
  ];

  for (const a of attendees || []) {
    const mail = String(a).trim();
    if (!mail) continue;
    // simple attendee line; no CN available
    lines.push(`ATTENDEE;ROLE=REQ-PARTICIPANT;RSVP=TRUE:mailto:${mail}`);
  }

  // Add Google Meet link as an X- property to help Gmail clients
  if (meetLink) {
    lines.push(`X-ALT-DESC;FMTTYPE=text/html:Join via Google Meet: <a href="${meetLink}">${meetLink}</a>`);
  }

  lines.push('END:VEVENT', 'END:VCALENDAR');
  return lines.filter(Boolean).join('\r\n');
}

// Helper: check if a hostname string is a private/loopback address (basic SSRF guard)
function _isPrivateHost(hostname) {
  // Reject loopback, link-local, and private RFC-1918 ranges
  if (hostname === 'localhost') return true;
  // IPv4 patterns
  const ipv4 = hostname.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (ipv4) {
    const [, a, b, c] = ipv4.map(Number);
    if (a === 127 || a === 10) return true;                          // 127.x.x.x, 10.x.x.x
    if (a === 172 && b >= 16 && b <= 31) return true;               // 172.16-31.x.x
    if (a === 192 && b === 168) return true;                         // 192.168.x.x
    if (a === 169 && b === 254) return true;                         // 169.254.x.x link-local
    if (a === 0) return true;                                        // 0.x.x.x
  }
  // IPv6 loopback / link-local
  if (hostname === '::1' || /^fe80:/i.test(hostname) || /^\[::1\]$/.test(hostname)) return true;
  return false;
}

// ── Pre-compiled ICS regex constants ─────────────────────────────────────────
// Hoisted to module level so they are compiled once rather than on every call
// inside the hot ICS-line-processing loop.
const _RE_ICS_UTC_DT      = /^\d{8}T\d{6}Z$/;    // YYYYMMDDTHHMMSSZ — Z must be uppercase per RFC 5545
const _RE_ICS_FLOAT_DT    = /^\d{8}T\d{6}$/;      // YYYYMMDDTHHMMSS (floating)
const _RE_ICS_DATE        = /^\d{8}$/;             // YYYYMMDD (all-day)
const _RE_ICS_TZID        = /TZID=([^;:]+)/;       // property parameters are uppercase per RFC 5545
const _RE_ICS_FBTYPE      = /FBTYPE=([^;:]+)/;     // property parameters are uppercase per RFC 5545
const _RE_ICS_DURATION_P  = /^P/;                  // duration starts with uppercase P per RFC 5545
const _RE_DURATION        = /P(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?/;
const _RE_RRULE_FREQ      = /FREQ=([A-Z]+)/;
const _RE_RRULE_INTERVAL  = /INTERVAL=(\d+)/;
const _RE_RRULE_COUNT     = /COUNT=(\d+)/;
const _RE_RRULE_UNTIL     = /UNTIL=([^\s;]+)/;
const _RE_RRULE_BYDAY     = /BYDAY=([^;]+)/;

// Helper: parse an ICS date/datetime string to a UTC timestamp (ms)
function parseIcsDate(dateStr) {
  if (!dateStr) return NaN;
  const s = dateStr.trim();
  // UTC: YYYYMMDDTHHMMSSZ
  if (_RE_ICS_UTC_DT.test(s)) {
    return Date.UTC(
      parseInt(s.slice(0, 4), 10), parseInt(s.slice(4, 6), 10) - 1, parseInt(s.slice(6, 8), 10),
      parseInt(s.slice(9, 11), 10), parseInt(s.slice(11, 13), 10), parseInt(s.slice(13, 15), 10)
    );
  }
  // Floating (no Z, no TZID in value): YYYYMMDDTHHMMSS — treat as UTC
  if (_RE_ICS_FLOAT_DT.test(s)) {
    return Date.UTC(
      parseInt(s.slice(0, 4), 10), parseInt(s.slice(4, 6), 10) - 1, parseInt(s.slice(6, 8), 10),
      parseInt(s.slice(9, 11), 10), parseInt(s.slice(11, 13), 10), parseInt(s.slice(13, 15), 10)
    );
  }
  // All-day date: YYYYMMDD
  if (_RE_ICS_DATE.test(s)) {
    return Date.UTC(
      parseInt(s.slice(0, 4), 10), parseInt(s.slice(4, 6), 10) - 1, parseInt(s.slice(6, 8), 10)
    );
  }
  // ISO 8601 fallback
  return new Date(s).getTime();
}

/**
 * Parse an ICS datetime string that carries a TZID parameter (e.g. the value of
 * `DTSTART;TZID=America/New_York:20240101T090000`) and return the corresponding
 * UTC millisecond timestamp.
 *
 * @param {string} dateStr - The datetime value portion (after the colon), e.g. "20240101T090000".
 * @param {string|undefined} tzid  - IANA timezone identifier extracted from the TZID param,
 *                                   e.g. "America/New_York".  If absent, falls back to
 *                                   parseIcsDate() which treats floating times as UTC.
 * @returns {number} UTC milliseconds, or NaN on parse failure.
 */
function parseIcsDateWithTzid(dateStr, tzid) {
  if (!tzid) return parseIcsDate(dateStr);
  const s = (dateStr || '').trim();
  // Already UTC — no timezone adjustment needed
  if (_RE_ICS_UTC_DT.test(s)) return parseIcsDate(s);
  // Floating datetime: YYYYMMDDTHHMMSS — convert from tzid to UTC via Intl
  if (_RE_ICS_FLOAT_DT.test(s)) {
    try {
      const y  = parseInt(s.slice(0, 4), 10);
      const mo = parseInt(s.slice(4, 6), 10) - 1;
      const d  = parseInt(s.slice(6, 8), 10);
      const h  = parseInt(s.slice(9, 11), 10);
      const mi = parseInt(s.slice(11, 13), 10);
      const sc = parseInt(s.slice(13, 15), 10);
      // Use the Intl API: find what UTC timestamp shows the given local time in tzid.
      // Method: take the "naive UTC" equivalent, ask what local time the timezone shows
      // at that UTC instant, then compute the offset and adjust.
      const naiveUtcMs = Date.UTC(y, mo, d, h, mi, sc);
      const fmt = new Intl.DateTimeFormat('en-US', {
        timeZone: tzid,
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit',
        hour12: false
      });
      const parts = fmt.formatToParts(new Date(naiveUtcMs));
      const get = (t) => parseInt((parts.find(p => p.type === t) || { value: '0' }).value, 10);
      // hour12:false may return 24 for midnight — normalise
      const localH = get('hour') === 24 ? 0 : get('hour');
      const localMs = Date.UTC(get('year'), get('month') - 1, get('day'), localH, get('minute'), get('second'));
      const offsetMs = localMs - naiveUtcMs; // tz offset at naiveUtcMs
      return naiveUtcMs - offsetMs;
    } catch (_) {
      return parseIcsDate(dateStr); // unknown timezone — fall back to UTC
    }
  }
  return parseIcsDate(dateStr);
}

// Helper: fetch an ICS URL and return busy [{start, end}] intervals overlapping [startISO, endISO]
// Supports http://, https://, and webcal:// (remapped to https://).
// ICS content is stream-parsed line-by-line as chunks arrive (RFC 5545 folding handled across
// chunk boundaries). Avoids buffering the entire file; allows early exit once past rangeEnd.
async function fetchAndParseIcsBusy(icsUrl, startISO, endISO) {
  // Validate and normalise the URL
  const normalised = icsUrl.trim().replace(/^webcal:/i, 'https:');
  let urlObj;
  try { urlObj = new URL(normalised); } catch (_) { throw new Error('Invalid ICS URL.'); }
  if (urlObj.protocol !== 'https:' && urlObj.protocol !== 'http:') {
    throw new Error('ICS URL must use http, https, or webcal protocol.');
  }
  // SSRF guard: reject requests targeting private / loopback addresses
  if (_isPrivateHost(urlObj.hostname)) {
    throw new Error('ICS URL must point to a public host.');
  }
  // Resolve DNS and re-check the resolved IP to prevent DNS-rebinding SSRF
  try {
    const { address } = await dns.lookup(urlObj.hostname);
    if (_isPrivateHost(address)) throw new Error('ICS URL resolves to a private address.');
  } catch (dnsErr) {
    if (dnsErr.message && dnsErr.message.includes('private')) throw dnsErr;
    // DNS lookup may fail in some environments; proceed and let the HTTP layer error out
  }
  const rangeStart = new Date(startISO).getTime();
  const rangeEnd   = new Date(endISO).getTime();

  // ── Helper: parse a DURATION string (e.g. P1DT2H30M) into milliseconds ─────
  function _parseDurationMs(durStr) {
    const d = (durStr || '').toUpperCase();
    let ms = 0;
    const m = d.match(_RE_DURATION);
    if (m) {
      ms = ((parseInt(m[1], 10) || 0) * 7 * 86400
           + (parseInt(m[2], 10) || 0) * 86400
           + (parseInt(m[3], 10) || 0) * 3600
           + (parseInt(m[4], 10) || 0) * 60
           + (parseInt(m[5], 10) || 0)) * 1000;
    }
    return ms;
  }

  // ── Helper: expand recurring event occurrences within [rangeStart, rangeEnd] ─
  // Supports RRULE FREQ=DAILY/WEEKLY/MONTHLY/YEARLY with COUNT/UNTIL/INTERVAL/BYDAY.
  // EXDATE entries (comma-separated) are excluded.
  function _expandRecurrence(evt, durationMs) {
    const rule = (evt.rrule || '').toUpperCase();
    if (!rule) return [];
    const intervals = [];

    const freqMatch = rule.match(_RE_RRULE_FREQ);
    if (!freqMatch) return [];
    const freq = freqMatch[1]; // DAILY | WEEKLY | MONTHLY | YEARLY

    const intervalMatch = rule.match(_RE_RRULE_INTERVAL);
    const interval = intervalMatch ? parseInt(intervalMatch[1], 10) : 1;

    const countMatch = rule.match(_RE_RRULE_COUNT);
    const maxCount = countMatch ? parseInt(countMatch[1], 10) : Infinity;

    const untilMatch = rule.match(_RE_RRULE_UNTIL);
    const untilMs = untilMatch ? parseIcsDate(untilMatch[1]) : Infinity;

    // BYDAY for WEEKLY: e.g. BYDAY=MO,WE,FR
    const bydayMatch = rule.match(_RE_RRULE_BYDAY);
    const byDays = bydayMatch
      ? bydayMatch[1].split(',').map(d => d.trim().slice(-2).toUpperCase())
      : null;
    const dayMap = { SU: 0, MO: 1, TU: 2, WE: 3, TH: 4, FR: 5, SA: 6 };

    // Build EXDATE set (UTC ms values to skip)
    const exdateSet = new Set();
    if (evt.exdate) {
      const parts = evt.exdate.split(',');
      for (const p of parts) {
        const ms = parseIcsDateWithTzid(p.trim(), evt.exdate_tzid || evt.dtstart_tzid);
        if (!isNaN(ms)) exdateSet.add(ms);
      }
    }

    const dtStartMs = parseIcsDateWithTzid(evt.dtstart, evt.dtstart_tzid);
    if (isNaN(dtStartMs)) return [];

    let cursor = dtStartMs;
    let count = 0;

    // Advance cursor forward in steps until past rangeEnd or max iterations (safety limit)
    const MAX_ITER = 1000;
    let iter = 0;
    while (cursor <= rangeEnd && count < maxCount && cursor <= untilMs && iter < MAX_ITER) {
      iter++;
      // Check if this occurrence is excluded
      const isExcluded = exdateSet.has(cursor);
      if (!isExcluded) {
        const occEnd = cursor + durationMs;
        // Emit if overlaps with the query range
        if (occEnd > rangeStart && cursor < rangeEnd) {
          intervals.push({ start: new Date(cursor).toISOString(), end: new Date(occEnd).toISOString() });
        }
      }
      count++;

      // Advance to next occurrence
      const d = new Date(cursor);
      if (freq === 'DAILY') {
        cursor += interval * 86400000;
      } else if (freq === 'WEEKLY') {
        if (byDays && byDays.length > 1) {
          // Multiple days per week: advance to the next listed weekday
          let next = cursor + 86400000;
          let safety = 0;
          while (safety < 14) {
            safety++;
            const wd = new Date(next).getUTCDay();
            const dayName = Object.keys(dayMap).find(k => dayMap[k] === wd);
            if (dayName && byDays.includes(dayName)) break;
            next += 86400000;
          }
          // After a full week cycle, apply the interval
          if (new Date(next).getUTCDay() <= new Date(cursor).getUTCDay() && interval > 1) {
            next += (interval - 1) * 7 * 86400000;
          }
          cursor = next;
        } else {
          cursor += interval * 7 * 86400000;
        }
      } else if (freq === 'MONTHLY') {
        const nd = new Date(d);
        nd.setUTCMonth(nd.getUTCMonth() + interval);
        cursor = nd.getTime();
      } else if (freq === 'YEARLY') {
        const nd = new Date(d);
        nd.setUTCFullYear(nd.getUTCFullYear() + interval);
        cursor = nd.getTime();
      } else {
        break; // Unknown frequency
      }
    }
    return intervals;
  }

  // ── Streaming ICS line processor ─────────────────────────────────────────
  // Shared parser state; populated by processLogicalLine() called from the stream.
  const busyIntervals = [];
  let inVevent    = false;
  let inVfreebusy = false;
  let evt = {};
  let vfb = {};

  function processLogicalLine(line) {
    const upper = line.toUpperCase();

    // ── VFREEBUSY handling (RFC 5545 §3.6.4) ─────────────────────────────────
    if (upper === 'BEGIN:VFREEBUSY') { inVfreebusy = true; vfb = {}; return; }
    if (upper === 'END:VFREEBUSY') {
      inVfreebusy = false;
      if (vfb.freebusy) {
        for (const fbLine of vfb.freebusy) {
          const periods = fbLine.split(',');
          for (const period of periods) {
            const parts = period.trim().split('/');
            if (parts.length !== 2) continue;
            const pStart = parseIcsDate(parts[0].trim());
            let pEnd;
            if (_RE_ICS_DURATION_P.test(parts[1].trim())) {
              pEnd = pStart + _parseDurationMs(parts[1].trim());
            } else {
              pEnd = parseIcsDate(parts[1].trim());
            }
            if (!isNaN(pStart) && !isNaN(pEnd) && pEnd > rangeStart && pStart < rangeEnd) {
              busyIntervals.push({ start: new Date(pStart).toISOString(), end: new Date(pEnd).toISOString() });
            }
          }
        }
      }
      vfb = {};
      return;
    }
    if (inVfreebusy) {
      const colonIdx = line.indexOf(':');
      if (colonIdx === -1) return;
      const rawProp = line.substring(0, colonIdx);
      const value   = line.substring(colonIdx + 1);
      const semiIdx = rawProp.indexOf(';');
      const propKey = (semiIdx === -1 ? rawProp : rawProp.substring(0, semiIdx)).toUpperCase();
      if (propKey === 'FREEBUSY') {
        const fbType = semiIdx !== -1
          ? (rawProp.substring(semiIdx + 1).match(_RE_ICS_FBTYPE) || [])[1] || 'BUSY'
          : 'BUSY';
        if (fbType.toUpperCase() !== 'FREE') {
          if (!vfb.freebusy) vfb.freebusy = [];
          vfb.freebusy.push(value);
        }
      }
      return;
    }

    // ── VEVENT handling ───────────────────────────────────────────────────────
    if (upper === 'BEGIN:VEVENT') { inVevent = true; evt = {}; return; }
    if (upper === 'END:VEVENT') {
      inVevent = false;
      if (evt.dtstart) {
        const status = (evt.status || '').toUpperCase();
        const transp  = (evt.transp  || 'OPAQUE').toUpperCase();
        if (status !== 'CANCELLED' && transp !== 'TRANSPARENT') {
          let startMs = parseIcsDateWithTzid(evt.dtstart, evt.dtstart_tzid);
          let durationMs;
          if (evt.dtend) {
            durationMs = parseIcsDateWithTzid(evt.dtend, evt.dtend_tzid || evt.dtstart_tzid) - startMs;
          } else if (evt.duration) {
            durationMs = _parseDurationMs(evt.duration);
          } else {
            durationMs = 86400000; // all-day event: 1 day
          }
          if (!isNaN(startMs) && durationMs > 0) {
            if (evt.rrule) {
              const occurrences = _expandRecurrence(evt, durationMs);
              busyIntervals.push(...occurrences);
            } else {
              const endMs = startMs + durationMs;
              if (endMs > rangeStart && startMs < rangeEnd) {
                busyIntervals.push({ start: new Date(startMs).toISOString(), end: new Date(endMs).toISOString() });
              }
            }
          }
        }
      }
      evt = {};
      return;
    }
    if (!inVevent) return;

    // ── Parse VEVENT property ─────────────────────────────────────────────────
    const colonIdx = line.indexOf(':');
    if (colonIdx === -1) return;
    const rawProp = line.substring(0, colonIdx);
    const value   = line.substring(colonIdx + 1);
    const semiIdx = rawProp.indexOf(';');
    const propKey = (semiIdx === -1 ? rawProp : rawProp.substring(0, semiIdx)).toUpperCase();

    if (propKey === 'DTSTART') {
      evt.dtstart = value;
      if (semiIdx !== -1) {
        const tzMatch = rawProp.substring(semiIdx + 1).match(_RE_ICS_TZID);
        if (tzMatch) evt.dtstart_tzid = tzMatch[1].trim();
      }
    } else if (propKey === 'DTEND') {
      evt.dtend = value;
      if (semiIdx !== -1) {
        const tzMatch = rawProp.substring(semiIdx + 1).match(_RE_ICS_TZID);
        if (tzMatch) evt.dtend_tzid = tzMatch[1].trim();
      }
    } else if (propKey === 'DURATION') {
      evt.duration = value;
    } else if (propKey === 'RRULE') {
      evt.rrule = value;
    } else if (propKey === 'EXDATE') {
      const existing = evt.exdate ? evt.exdate + ',' : '';
      evt.exdate = existing + value;
      if (semiIdx !== -1) {
        const tzMatch = rawProp.substring(semiIdx + 1).match(_RE_ICS_TZID);
        if (tzMatch) evt.exdate_tzid = tzMatch[1].trim();
      }
    } else if (propKey === 'STATUS') {
      evt.status = value;
    } else if (propKey === 'TRANSP') {
      evt.transp = value;
    }
  }

  // ── Fetch with redirect budget (max 3 hops), stream-parsing each chunk ────
  // RFC 5545 §3.1: long lines are folded by inserting CRLF + SPACE/TAB.
  // We handle folding across chunk boundaries via `pendingLogical`.
  async function doFetch(fetchUrl, hops) {
    if (hops <= 0) throw new Error('Too many redirects fetching ICS feed.');
    const u = new URL(fetchUrl);
    const mod = u.protocol === 'https:' ? https : http;
    return new Promise((resolve, reject) => {
      const req = mod.request(
        { hostname: u.hostname, port: u.port || (u.protocol === 'https:' ? 443 : 80),
          path: u.pathname + (u.search || ''), method: 'GET',
          headers: { 'User-Agent': 'FIOE-Calendar/1.0', 'Accept': 'text/calendar,*/*' },
          timeout: 12000 },
        (res) => {
          if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
            return doFetch(res.headers.location, hops - 1).then(resolve).catch(reject);
          }
          if (res.statusCode !== 200) {
            return reject(new Error(`ICS fetch returned HTTP ${res.statusCode}.`));
          }

          let bytesReceived = 0;
          let chunkTail = '';       // incomplete physical line at the end of the last chunk
          let pendingLogical = '';  // current logical line being assembled (handles RFC 5545 folding)

          res.setEncoding('utf8');
          res.on('data', chunk => {
            bytesReceived += chunk.length;
            if (bytesReceived > 5 * 1024 * 1024) {
              req.destroy();
              reject(new Error('ICS feed too large (>5 MB).'));
              return;
            }

            // Combine leftover from previous chunk with new data, then split physical lines.
            const buf = chunkTail + chunk;
            // Find the last newline to determine the incomplete tail
            const lastNl = Math.max(buf.lastIndexOf('\n'), buf.lastIndexOf('\r'));
            if (lastNl === -1) {
              // No complete line in this chunk yet — accumulate
              chunkTail = buf;
              return;
            }
            chunkTail = buf.slice(lastNl + 1); // remainder after last newline
            const completeData = buf.slice(0, lastNl + 1);
            // Split by any line ending variant
            const physLines = completeData.split(/\r\n|\r|\n/);

            for (const physLine of physLines) {
              if (physLine.length === 0) continue; // skip blank lines between CRLF endings
              // RFC 5545 fold: a physical line beginning with SP or HT is a continuation
              if (physLine.charCodeAt(0) === 0x20 || physLine.charCodeAt(0) === 0x09) {
                pendingLogical += physLine.slice(1);
              } else {
                // Emit the completed logical line and start a new one
                if (pendingLogical) processLogicalLine(pendingLogical);
                pendingLogical = physLine;
              }
            }
          });

          res.on('end', () => {
            // Flush any leftover tail and pending logical line
            const remaining = chunkTail;
            if (remaining.length > 0) {
              if (remaining.charCodeAt(0) === 0x20 || remaining.charCodeAt(0) === 0x09) {
                pendingLogical += remaining.slice(1);
              } else {
                if (pendingLogical) processLogicalLine(pendingLogical);
                pendingLogical = remaining;
              }
            }
            if (pendingLogical) processLogicalLine(pendingLogical);
            resolve(busyIntervals);
          });
        }
      );
      req.on('timeout', () => { req.destroy(); reject(new Error('ICS fetch timed out.')); });
      req.on('error', reject);
      req.end();
    });
  }

  return doFetch(normalised, 3);
}

// Helper: compute simple free slots between timeMin/timeMax avoiding busy intervals
function computeFreeSlots(busyIntervals = [], timeMinISO, timeMaxISO, durationMinutes = _SCHEDULER_DEFAULT_DURATION, businessHours = { startHour: 0, endHour: 24, timezone: 'UTC' }, maxResults = 6) {
  const start = new Date(timeMinISO).getTime();
  const end = new Date(timeMaxISO).getTime();
  if (isNaN(start) || isNaN(end) || start >= end) return [];

  // Convert busy intervals to numeric ranges
  const busyRanges = (busyIntervals || []).map(b => {
    const s = new Date(b.start).getTime();
    const e = new Date(b.end).getTime();
    if (isNaN(s) || isNaN(e)) return null;
    return { start: s, end: e };
  }).filter(Boolean);

  // Merge busy ranges
  busyRanges.sort((a, b) => a.start - b.start);
  const merged = [];
  busyRanges.forEach(r => {
    if (!merged.length) merged.push({ ...r });
    else {
      const last = merged[merged.length - 1];
      if (r.start <= last.end) {
        last.end = Math.max(last.end, r.end);
      } else merged.push({ ...r });
    }
  });

  const durationMs = durationMinutes * 60 * 1000;
  const slots = [];
  // scan from start to end in step of durationMinutes (but aligned to round minutes)
  let cursor = start;
  // Align cursor to next 15-minute boundary for nicer slots
  const d = new Date(cursor);
  const minutes = d.getUTCMinutes();
  const aligned = Math.ceil(minutes / 15) * 15;
  d.setUTCMinutes(aligned);
  d.setUTCSeconds(0);
  d.setUTCMilliseconds(0);
  cursor = d.getTime();

  while (cursor + durationMs <= end && slots.length < maxResults) {
    const slotStart = cursor;
    const slotEnd = cursor + durationMs;

    // Respect business hours in UTC: check startHour/endHour
    const sDate = new Date(slotStart);
    const hourUTC = sDate.getUTCHours();
    if (hourUTC < businessHours.startHour || hourUTC >= businessHours.endHour) {
      cursor += 15 * 60 * 1000; // advance by 15 minutes
      continue;
    }

    // Check overlap with merged busy ranges
    let overlap = false;
    for (const br of merged) {
      if (!(slotEnd <= br.start || slotStart >= br.end)) {
        overlap = true;
        break;
      }
    }
    if (!overlap) {
      slots.push({ start: new Date(slotStart).toISOString(), end: new Date(slotEnd).toISOString() });
    }
    cursor += 15 * 60 * 1000;
  }

  return slots;
}

// Endpoint: query freebusy and return candidate slots (POST body: { startISO, endISO, durationMinutes })
app.post('/calendar/freebusy', requireLogin, async (req, res) => {
  try {
    let { startISO, endISO, durationMinutes = _SCHEDULER_DEFAULT_DURATION, attendees = [], provider = 'google', icsUrl } = req.body;
    if (!startISO || !endISO) return res.status(400).json({ error: 'startISO and endISO required.' });
    // Normalise plain date strings (YYYY-MM-DD) to full RFC 3339 timestamps required by Google Calendar API
    if (/^\d{4}-\d{2}-\d{2}$/.test(startISO)) startISO = new Date(startISO + 'T00:00:00Z').toISOString();
    if (/^\d{4}-\d{2}-\d{2}$/.test(endISO))   endISO   = new Date(endISO   + 'T23:59:59Z').toISOString();

    let primaryBusy = [];

    if (provider === 'microsoft') {
      const accessToken = await getMicrosoftTokenForUser(req.user.username);
      // Use calendarView to retrieve existing events (= busy intervals) in UTC
      const encodedStart = encodeURIComponent(startISO);
      const encodedEnd   = encodeURIComponent(endISO);
      const view = await _msGraphRequest(
        'GET',
        `/me/calendarView?startDateTime=${encodedStart}&endDateTime=${encodedEnd}&$select=start,end&$top=500`,
        accessToken,
        null,
        { 'Prefer': 'outlook.timezone="UTC"' }
      );
      primaryBusy = (view.value || []).map(ev => ({
        start: ev.start.dateTime.includes('Z') ? ev.start.dateTime : ev.start.dateTime + 'Z',
        end:   ev.end.dateTime.includes('Z')   ? ev.end.dateTime   : ev.end.dateTime + 'Z'
      }));
    } else if (provider === 'ics') {
      // Fetch the user-supplied ICS feed and parse busy intervals from VEVENTs
      if (!icsUrl || typeof icsUrl !== 'string' || !icsUrl.trim()) {
        return res.status(400).json({ error: 'icsUrl is required for ICS calendar provider.' });
      }
      primaryBusy = await fetchAndParseIcsBusy(icsUrl, startISO, endISO);
    } else {
      if (!google) return res.status(500).json({ error: 'Google APIs module not available.' });
      const oauth2Client = await getOAuthClientForUser(req.user.username);
      const calendar = google.calendar({ version: 'v3', auth: oauth2Client });
      const fbReq = {
        resource: {
          timeMin: startISO,
          timeMax: endISO,
          items: [{ id: 'primary' }]
        }
      };
      const attendeeItems = (attendees || []).map(email => ({ id: email }));
      if (attendeeItems.length) fbReq.resource.items.push(...attendeeItems);
      const fb = await withExponentialBackoff(() => calendar.freebusy.query(fbReq), { label: 'google/freebusy' });
      primaryBusy = (fb.data && fb.data.calendars && fb.data.calendars.primary && fb.data.calendars.primary.busy) ? fb.data.calendars.primary.busy : [];
    }

    const slots = computeFreeSlots(primaryBusy, startISO, endISO, durationMinutes, { startHour: 0, endHour: 24 }, 1000);
    res.json({ ok: true, slots });
  } catch (err) {
    console.error('/calendar/freebusy error', err);
    res.status(500).json({ error: err.message || 'freebusy failed' });
  }
});

// ── ICS URL persistence (ICS_.json) ──────────────────────────────────────────
// loadIcsUrls() / saveIcsUrls() read and write ICS_.json which stores a mapping
// of { username: icsUrl } so each user's ICS calendar URL survives server restarts.

function loadIcsUrls() {
  try { return JSON.parse(fs.readFileSync(ICS_URLS_PATH, 'utf8')); } catch (_) { return {}; }
}

function saveIcsUrls(data) {
  const tmp = ICS_URLS_PATH + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
  fs.renameSync(tmp, ICS_URLS_PATH);
}

// GET /api/ics-url — return the logged-in user's saved ICS URL (or empty string)
app.get('/api/ics-url', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const data = loadIcsUrls();
    res.json({ url: data[req.user.username] || '' });
  } catch (err) {
    console.error('[ics-url GET]', err.message);
    res.status(500).json({ error: 'Could not read ICS URL.' });
  }
});

// POST /api/ics-url — save (or clear) the logged-in user's ICS URL in ICS_.json
// Body: { icsUrl: string }  — pass empty string to remove the stored URL.
app.post('/api/ics-url', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const { icsUrl } = req.body || {};
    if (typeof icsUrl !== 'string') return res.status(400).json({ error: 'icsUrl must be a string.' });
    const url = icsUrl.trim();
    if (url && !/^(https?:\/\/|webcal:\/\/)/i.test(url)) {
      return res.status(400).json({ error: 'Invalid ICS URL scheme. Must start with http://, https://, or webcal://.' });
    }
    const data = loadIcsUrls();
    if (url) {
      data[req.user.username] = url;
    } else {
      delete data[req.user.username];
    }
    saveIcsUrls(data);
    res.json({ ok: true });
  } catch (err) {
    console.error('[ics-url POST]', err.message);
    res.status(500).json({ error: 'Could not save ICS URL.' });
  }
});

// Endpoint: create calendar event with conferenceData (Meet/Teams) and return meeting link and ICS
// Body: { summary, description, startISO, endISO, attendees: ['a@b.com'], timezone, sendUpdates, provider }
app.post('/calendar/create-event', requireLogin, async (req, res) => {
  try {
    const { summary, description = '', startISO, endISO, attendees = [], timezone = 'UTC', sendUpdates = 'none', provider = 'google' } = req.body;
    if (!startISO || !endISO || !summary) return res.status(400).json({ error: 'summary, startISO and endISO are required.' });

    let meetLink = null;
    let createdEventId = null;
    let organizerEmail = req.user.username || 'organizer@example.com';

    if (provider === 'ics') {
      // ICS feeds are read-only; we cannot write back to the external feed.
      // Create a local calendar event (ICS file) for the organiser to attach to the email.
      createdEventId = `ics-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
      // meetLink stays null — no conferencing service is provisioned for ICS
    } else if (provider === 'microsoft') {
      // Create event via Microsoft Graph API with Teams meeting
      const accessToken = await getMicrosoftTokenForUser(req.user.username);
      const eventBody = {
        subject: summary,
        body: { contentType: 'text', content: description },
        start: { dateTime: startISO, timeZone: 'UTC' },
        end:   { dateTime: endISO,   timeZone: 'UTC' },
        attendees: (attendees || []).filter(Boolean).map(email => ({
          emailAddress: { address: email },
          type: 'required'
        })),
        isOnlineMeeting: true,
        onlineMeetingProvider: 'teamsForBusiness'
      };
      const created = await _msGraphRequest('POST', '/me/events', accessToken, eventBody);
      createdEventId = created.id || null;
      meetLink = (created.onlineMeeting && created.onlineMeeting.joinUrl) ? created.onlineMeeting.joinUrl : null;
      // Try to get organizer email from the event response
      if (created.organizer && created.organizer.emailAddress && created.organizer.emailAddress.address) {
        organizerEmail = created.organizer.emailAddress.address;
      }
    } else {
      if (!google) return res.status(500).json({ error: 'Google APIs module not available.' });
      const oauth2Client = await getOAuthClientForUser(req.user.username);
      const calendar = google.calendar({ version: 'v3', auth: oauth2Client });
      const event = {
        summary,
        description,
        start: { dateTime: startISO, timeZone: timezone },
        end:   { dateTime: endISO,   timeZone: timezone },
        attendees: (attendees || []).filter(Boolean).map(email => ({ email })),
        conferenceData: {
          createRequest: {
            requestId: `meet-${Date.now()}-${Math.random().toString(36).slice(2,8)}`,
            conferenceSolutionKey: { type: 'hangoutsMeet' }
          }
        }
      };
      const resp = await withExponentialBackoff(() => calendar.events.insert({
        calendarId: 'primary',
        conferenceDataVersion: 1,
        sendUpdates,
        resource: event
      }), { label: 'google/calendar-insert' });
      const created = resp.data;
      createdEventId = created.id || null;
      try {
        const entryPoints = created.conferenceData && created.conferenceData.entryPoints ? created.conferenceData.entryPoints : [];
        for (const ep of entryPoints) {
          if (ep.entryPointType === 'video') { meetLink = ep.uri; break; }
        }
      } catch (e) { /* ignore */ }
      try {
        const o = await oauth2Client.getTokenInfo && oauth2Client.getTokenInfo(oauth2Client.credentials.access_token).catch(() => null);
        if (o && o.email) organizerEmail = o.email;
      } catch (e) { /* ignore */ }
    }

    const uid = createdEventId || `ev-${Date.now()}-${Math.random().toString(36).slice(2,6)}`;
    const ics = buildICS({
      uid,
      startISO,
      endISO,
      summary,
      description,
      organizerEmail,
      attendees: attendees || [],
      timezone,
      meetLink: meetLink || ''
    });

    res.json({ ok: true, eventId: createdEventId, meetLink, ics });
  } catch (err) {
    console.error('/calendar/create-event error', err);
    res.status(500).json({ error: err.message || 'create-event failed' });
  }
});

// ========== END Calendar & Meet Integration ==========


// ========== Self-Scheduler: Public Booking System ==========
// Available slots are serialised to a lightweight JSON file so invitees can
// browse and book times without needing a Google Workspace paid booking page.

const SCHEDULER_SLOTS_PATH = process.env.SCHEDULER_SLOTS_PATH
  ? path.resolve(process.env.SCHEDULER_SLOTS_PATH)
  : path.join(__dirname, 'available_slots.json');

// Read the current slots file; returns [] on any error.
// Uses a mtime-based in-memory cache so repeated reads within the same second
// (e.g. concurrent booking requests) skip the disk entirely.
let _schedulerSlotsCache = null;
let _schedulerSlotsMtime = 0;
async function readSchedulerSlots() {
  try {
    // stat first to check mtime; only re-parse when file has changed.
    const stat = await fs.promises.stat(SCHEDULER_SLOTS_PATH).catch(() => null);
    const mtime = stat ? stat.mtimeMs : 0;
    if (_schedulerSlotsCache !== null && mtime === _schedulerSlotsMtime) return _schedulerSlotsCache;
    const raw = await fs.promises.readFile(SCHEDULER_SLOTS_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    _schedulerSlotsCache = Array.isArray(parsed) ? parsed : [];
    _schedulerSlotsMtime = mtime;
    return _schedulerSlotsCache;
  } catch (e) {
    if (e.code !== 'ENOENT') console.error('[scheduler] readSchedulerSlots error', e.message);
    return [];
  }
}

// Write the slots array back to the file atomically (write to tmp then rename).
// Updates the in-memory cache immediately to avoid a re-read on the next call.
async function writeSchedulerSlots(slots) {
  const tmp = SCHEDULER_SLOTS_PATH + '.' + Date.now() + '-' + crypto.randomBytes(4).toString('hex') + '.tmp';
  await fs.promises.writeFile(tmp, JSON.stringify(slots, null, 2), 'utf8');
  await fs.promises.rename(tmp, SCHEDULER_SLOTS_PATH);
  // Refresh cache — mtime may differ slightly from Date.now() due to FS precision;
  // use the actual stat to stay consistent with readSchedulerSlots.
  try {
    const stat = await fs.promises.stat(SCHEDULER_SLOTS_PATH);
    _schedulerSlotsCache = slots;
    _schedulerSlotsMtime = stat.mtimeMs;
  } catch (_) {
    // Non-fatal: cache will be refreshed on the next read.
    _schedulerSlotsCache = null;
  }
}

// POST /scheduler/publish-slots  (requireLogin)
// Body: { startISO, endISO, durationMinutes?, maxSlots? }
//   OR: { slots: [{start,end},...], durationMinutes? }  — publish pre-selected slots
// Queries Google Calendar freebusy (unless pre-selected slots are provided), computes
// free slots, and persists them to the JSON store.
app.post('/scheduler/publish-slots', requireLogin, async (req, res) => {
  try {
    const { startISO, endISO, durationMinutes = _SCHEDULER_DEFAULT_DURATION, maxSlots = _SCHEDULER_DEFAULT_MAX_SLOTS, slots: preSelected } = req.body;

    let freeSlots;
    let resolvedDuration = Number(durationMinutes) || 30;
    if (Array.isArray(preSelected) && preSelected.length > 0) {
      // Caller already picked specific slots (generate-then-select flow) — use as-is.
      // Infer duration from the first slot's start/end if not explicitly provided.
      freeSlots = preSelected.map(s => ({ start: s.start, end: s.end }));
      if (!durationMinutes && preSelected[0] && preSelected[0].start && preSelected[0].end) {
        resolvedDuration = Math.round((new Date(preSelected[0].end) - new Date(preSelected[0].start)) / 60000);
      }
    } else {
      if (!google) return res.status(500).json({ error: 'Google APIs module not available.' });
      if (!startISO || !endISO) {
        return res.status(400).json({ error: 'startISO and endISO are required.' });
      }
      const oauth2Client = await getOAuthClientForUser(req.user.username);
      const calendar = google.calendar({ version: 'v3', auth: oauth2Client });
      const fb = await withExponentialBackoff(() => calendar.freebusy.query({
        resource: { timeMin: startISO, timeMax: endISO, items: [{ id: 'primary' }] }
      }), { label: 'google/freebusy-scheduler' });
      const primaryBusy = (fb.data && fb.data.calendars && fb.data.calendars.primary && fb.data.calendars.primary.busy) || [];
      freeSlots = computeFreeSlots(primaryBusy, startISO, endISO, durationMinutes, { startHour: 0, endHour: 24 }, maxSlots);
    }

    const now = Date.now();
    const slotRecords = freeSlots.map((s, i) => ({
      id: `slot-${now}-${i}`,
      start: s.start,
      end: s.end,
      durationMinutes: resolvedDuration,
      booked: false,
      bookedBy: null,
      eventId: null,
      meetLink: null,
      publishedBy: req.user.username,
      publishedAt: new Date().toISOString()
    }));

    await writeSchedulerSlots(slotRecords);
    res.json({ ok: true, count: slotRecords.length, slots: slotRecords });
  } catch (err) {
    console.error('/scheduler/publish-slots error', err);
    res.status(500).json({ error: err.message || 'publish-slots failed' });
  }
});

// GET /scheduler/slots  (public — no login required)
// Returns only the unbooked slots so invitees can see what is available.
app.get('/scheduler/slots', async (req, res) => {
  try {
    const all = await readSchedulerSlots();
    // Strip internal fields from public response
    const available = all
      .filter(s => !s.booked)
      .map(({ id, start, end, durationMinutes }) => ({ id, start, end, durationMinutes }));
    res.json({ ok: true, slots: available });
  } catch (err) {
    console.error('/scheduler/slots error', err);
    res.status(500).json({ error: 'Failed to read available slots' });
  }
});

// Simple in-memory lock to serialise concurrent booking requests and prevent
// double-booking the same slot. Keyed by slot ID; lock is released on completion.
const _bookingLocks = new Set();

// POST /scheduler/book  (public — no login required)
// Body: { slotId, inviteeName, inviteeEmail, notes? }
// Atomically marks the slot as booked and creates a Google Calendar event
// (with Meet link) on behalf of the slot publisher.
app.post('/scheduler/book', async (req, res) => {
  try {
    const { slotId, inviteeName, inviteeEmail, notes = '' } = req.body;
    if (!slotId || !inviteeEmail) {
      return res.status(400).json({ error: 'slotId and inviteeEmail are required.' });
    }
    // Basic email format check
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(inviteeEmail)) {
      return res.status(400).json({ error: 'Invalid inviteeEmail format.' });
    }

    // Prevent concurrent bookings of the same slot
    if (_bookingLocks.has(slotId)) {
      return res.status(409).json({ error: 'Another booking for this slot is in progress. Please try again shortly.' });
    }
    _bookingLocks.add(slotId);

    try {
      const slots = await readSchedulerSlots();
      const idx = slots.findIndex(s => s.id === slotId);
      if (idx === -1) return res.status(404).json({ error: 'Slot not found.' });

      const slot = slots[idx];
      if (slot.booked) return res.status(409).json({ error: 'This slot has already been booked.' });

      // Mark as booked immediately to prevent double-booking
      slots[idx] = { ...slot, booked: true, bookedBy: inviteeEmail, bookedAt: new Date().toISOString() };
      await writeSchedulerSlots(slots);

      // Attempt to create a Google Calendar event for the publisher
      let meetLink = null;
      let eventId = null;
      let ics = null;

      if (google && slot.publishedBy) {
        try {
          const oauth2Client = await getOAuthClientForUser(slot.publishedBy);
          const calendar = google.calendar({ version: 'v3', auth: oauth2Client });

          const summary = inviteeName
            ? `Interview Confirmation – ${inviteeName}`
            : `Interview Confirmation`;

          // Format slot date and time for the confirmation message
          const slotStart = new Date(slot.start);
          const slotEnd = new Date(slot.end);
          const dateStr = slotStart.toLocaleDateString('en-US', {
            weekday: 'long', year: 'numeric', month: 'long', day: 'numeric', timeZone: 'UTC'
          });
          const timeStartStr = slotStart.toLocaleTimeString('en-US', {
            hour: '2-digit', minute: '2-digit', timeZone: 'UTC'
          });
          const timeEndStr = slotEnd.toLocaleTimeString('en-US', {
            hour: '2-digit', minute: '2-digit', timeZone: 'UTC'
          });

          const description = [
            `Dear ${inviteeName || 'Candidate'},`,
            '',
            'Thank you for confirming your interview schedule. We are pleased to confirm the details below:',
            '',
            'Interview Details',
            `- Date: ${dateStr}`,
            `- Time: ${timeStartStr} – ${timeEndStr} (UTC)`,
            '- Format: Video conference',
            '- Join Link: See the Google Meet link attached to this invite',
            '',
            `Email: ${inviteeEmail}`,
            notes ? `Notes: ${notes}` : '',
            '',
            'If you need to make any changes, please reply to this email.',
            '',
            'We look forward to speaking with you.'
          ].filter(l => l !== undefined).join('\n');

          const event = {
            summary,
            description,
            start: { dateTime: slot.start, timeZone: 'UTC' },
            end: { dateTime: slot.end, timeZone: 'UTC' },
            attendees: [{ email: inviteeEmail }],
            conferenceData: {
              createRequest: {
                requestId: `sched-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`,
                conferenceSolutionKey: { type: 'hangoutsMeet' }
              }
            }
          };

          const resp = await withExponentialBackoff(() => calendar.events.insert({
            calendarId: 'primary',
            conferenceDataVersion: 1,
            sendUpdates: 'all',
            resource: event
          }), { label: 'google/calendar-book' });

          const created = resp.data;
          eventId = created.id || null;

          // Extract Meet link
          const entryPoints = (created.conferenceData && created.conferenceData.entryPoints) || [];
          for (const ep of entryPoints) {
            if (ep.entryPointType === 'video') { meetLink = ep.uri; break; }
          }

          // Build ICS
          ics = buildICS({
            uid: eventId || `sched-${Date.now()}`,
            startISO: slot.start,
            endISO: slot.end,
            summary,
            description,
            organizerEmail: slot.publishedBy,
            attendees: [inviteeEmail],
            timezone: 'UTC',
            meetLink: meetLink || ''
          });

          // Persist event details back to slot record
          slots[idx] = { ...slots[idx], eventId, meetLink };
          await writeSchedulerSlots(slots);
        } catch (calErr) {
          // Calendar creation failure is non-fatal; the slot is still marked booked
          console.error('[scheduler/book] calendar event creation failed', calErr.message);
        }
      }

      res.json({ ok: true, meetLink, eventId, ics });
    } finally {
      _bookingLocks.delete(slotId);
    }
  } catch (err) {
    console.error('/scheduler/book error', err);
    res.status(500).json({ error: err.message || 'booking failed' });
  }
});

// DELETE /scheduler/slots  (requireLogin)
// Clears all published slots (e.g. to republish with new times).
app.delete('/scheduler/slots', requireLogin, async (req, res) => {
  try {
    await writeSchedulerSlots([]);
    res.json({ ok: true, message: 'All published slots cleared.' });
  } catch (err) {
    console.error('/scheduler/slots DELETE error', err);
    res.status(500).json({ error: 'Failed to clear slots' });
  }
});

// ========== END Self-Scheduler ==========


// ========== EMAIL VERIFICATION LOGIC ==========

// Helper: REAL SMTP Handshake
async function smtpVerify(email, mxHost) {
  if (!email || !mxHost) return 'unknown';
  const domain = email.split('@')[1];
  
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(25, mxHost);
    let step = 0;
    
    // Timeout 6s
    socket.setTimeout(6000);
    
    socket.on('connect', () => { /* connected */ });
    socket.on('timeout', () => {
       socket.destroy();
       resolve('timeout');
    });
    socket.on('error', (err) => {
       socket.destroy();
       resolve('connection_error');
    });

    socket.on('data', (data) => {
      const msg = data.toString();
      // 0. Initial greeting 220
      if (step === 0 && msg.startsWith('220')) {
         socket.write(`EHLO ${domain}\r\n`);
         step = 1;
      }
      // 1. EHLO response 250
      else if (step === 1 && msg.startsWith('250')) {
         socket.write(`MAIL FROM:<check@${domain}\r\n`);
         step = 2;
      }
      // 2. MAIL FROM response 250
      else if (step === 2 && msg.startsWith('250')) {
         socket.write(`RCPT TO:<${email}>\r\n`);
         step = 3;
      }
      // 3. RCPT TO response
      else if (step === 3) {
         if (msg.startsWith('250') || msg.startsWith('251')) {
           resolve('valid');
         } else if (msg.startsWith('550')) {
           resolve('invalid');
         } else {
           resolve('unknown_response');
         }
         socket.end();
      }
    });
  });
}

// ========== ContactOut service discovery (reads email_verif_config.json for contactout entry) ==========
app.get('/contact-gen-services', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const config = loadEmailVerifConfig();
    const enabled = [];
    for (const svc of CONTACT_GEN_IN_EMAIL_VERIF) {
      const entry = config[svc] || {};
      if (entry.enabled === 'enabled' && entry.api_key) enabled.push(svc);
    }
    res.json({ services: enabled });
  } catch (err) {
    console.error('/contact-gen-services error:', err);
    res.json({ services: [] });
  }
});

// ========== NEW ENDPOINT: Generate Emails / Generate Contacts ==========
app.post('/generate-email', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { name, company, country, provider, linkedinurl, force_admin } = req.body;

    // ── ContactOut provider path ─────────────────────────────
    if (provider === 'contactout') {
      if (!linkedinurl) {
        return res.status(400).json({ error: 'LinkedIn URL is required for ContactOut lookup.' });
      }
      // Prefer per-user key from api_porting.html Option A; fall back to admin platform key.
      // When force_admin is set, skip per-user key and go straight to admin key.
      let contactoutApiKey = null;
      if (!force_admin) {
        const userSvcCfg = readUserServiceConfig(req.user.username);
        if (userSvcCfg && userSvcCfg.contact_gen?.provider === 'contactout' && userSvcCfg.contact_gen?.CONTACTOUT_API_KEY) {
          contactoutApiKey = userSvcCfg.contact_gen.CONTACTOUT_API_KEY;
        }
      }
      if (!contactoutApiKey) {
        const cgCfg = loadEmailVerifConfig();
        const cusCfg = cgCfg.contactout || {};
        if (!cusCfg.api_key || cusCfg.enabled !== 'enabled') {
          return res.status(400).json({ error: 'ContactOut is not enabled or API key is missing.' });
        }
        contactoutApiKey = cusCfg.api_key;
      }

      // Normalize LinkedIn URL: ensure https:// prefix and clean trailing slashes
      let normalizedUrl = (linkedinurl || '').trim();
      if (normalizedUrl && !normalizedUrl.startsWith('https://') && !normalizedUrl.startsWith('http://')) {
        normalizedUrl = 'https://' + normalizedUrl;
      }
      // Warn and reject Sales Navigator / Recruiter URLs (not supported by ContactOut)
      if (/linkedin\.com\/(sales|talent)\//.test(normalizedUrl)) {
        return res.status(400).json({ error: 'ContactOut does not support Sales Navigator or Recruiter URLs. Please use a standard linkedin.com/in/ profile URL.' });
      }

      // Call ContactOut API: GET /v1/people/linkedin?profile=<url>&include_phone=true&email_type=work
      // email_type=work triggers real-time work email lookup per ContactOut docs
      console.log('[ContactOut] Starting API call — profile:', normalizedUrl, '| key configured:', !!contactoutApiKey);
      const contactRes = await new Promise((resolve, reject) => {
        const apiUrl = new URL('https://api.contactout.com/v1/people/linkedin');
        apiUrl.searchParams.set('profile', normalizedUrl);
        apiUrl.searchParams.set('include_phone', 'true');
        apiUrl.searchParams.set('email_type', 'work');
        const reqOpts = {
          hostname: apiUrl.hostname,
          port: 443,
          path: apiUrl.pathname + apiUrl.search,
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'token': contactoutApiKey,
          },
        };
        console.log('[ContactOut] Request URL:', `https://${apiUrl.hostname}${apiUrl.pathname}${apiUrl.search}`);
        const r = https.request(reqOpts, (resp) => {
          let body = '';
          resp.on('data', d => body += d);
          resp.on('end', () => {
            console.log('[ContactOut] HTTP status:', resp.statusCode);
            console.log('[ContactOut] Raw response body:', body);
            try {
              const parsed = JSON.parse(body);
              // Attach HTTP status for error checking
              parsed._http_status = resp.statusCode;
              resolve(parsed);
            } catch (e) {
              console.error('[ContactOut] Failed to parse response JSON:', e.message, '| Body was:', body);
              reject(new Error('Invalid JSON from ContactOut API'));
            }
          });
        });
        r.on('error', (err) => {
          console.error('[ContactOut] Network error:', err.message);
          reject(err);
        });
        const CONTACTOUT_API_TIMEOUT_MS = 20000;
        r.setTimeout(CONTACTOUT_API_TIMEOUT_MS, () => {
          console.error('[ContactOut] Request timed out after', CONTACTOUT_API_TIMEOUT_MS, 'ms');
          r.destroy();
          reject(new Error('ContactOut API timeout'));
        });
        r.end();
      });

      // Check for API-level errors (status != 200)
      if (contactRes._http_status && contactRes._http_status !== 200) {
        const apiMsg = contactRes.message || contactRes.error || `ContactOut API returned status ${contactRes._http_status}`;
        console.error('[ContactOut] API error response — status:', contactRes._http_status, '| message:', apiMsg);
        return res.status(400).json({ error: apiMsg });
      }

      // Map ContactOut response → structured contact data
      // Per the API docs, all fields (email, work_email, personal_email, phone, github)
      // are returned as ARRAYS inside a top-level "profile" object.
      const profile = contactRes.profile || {};
      console.log('[ContactOut] Parsed profile fields — keys:', Object.keys(profile));
      console.log('[ContactOut] profile.email:', JSON.stringify(profile.email));
      console.log('[ContactOut] profile.work_email:', JSON.stringify(profile.work_email));
      console.log('[ContactOut] profile.personal_email:', JSON.stringify(profile.personal_email));
      console.log('[ContactOut] profile.phone:', JSON.stringify(profile.phone));
      console.log('[ContactOut] profile.github:', JSON.stringify(profile.github));

      // Helper: safely get first non-empty value from array or string field
      const _first = (arrOrStr) => {
        if (Array.isArray(arrOrStr)) return arrOrStr.find(v => v) || '';
        return typeof arrOrStr === 'string' ? arrOrStr : '';
      };

      // Per API docs all fields are arrays; fall back to legacy plural/scalar names for safety
      const email          = _first(profile.email) || _first(profile.emails) || '';
      const phone          = _first(profile.phone) || _first(profile.phones) || '';
      const work_email     = _first(profile.work_email) || _first(profile.work_emails) || '';
      const github         = _first(profile.github) || '';
      const personal_email = _first(profile.personal_email) || _first(profile.personal_emails) || '';

      // Helper: flatten any field (array or scalar string) into an array of non-empty strings
      const _toArr = (v) => {
        if (Array.isArray(v)) return v.filter(Boolean);
        if (typeof v === 'string' && v) return [v];
        return [];
      };

      // Collect all unique emails for the frontend email list
      const allEmails = [
        ..._toArr(profile.email),
        ..._toArr(profile.emails),
        ..._toArr(profile.work_email),
        ..._toArr(profile.work_emails),
        ..._toArr(profile.personal_email),
        ..._toArr(profile.personal_emails),
      ].filter((v, i, a) => v && a.indexOf(v) === i);

      const result = {
        provider: 'contactout',
        email,
        phone,
        work_email,
        github,
        personal_email,
        all_emails: allEmails,
      };
      console.log('[ContactOut] Mapped result:', JSON.stringify(result));
      return res.json(result);
    }

    // ── Apollo provider path ─────────────────────────────────────────────
    if (provider === 'apollo') {
      if (!linkedinurl) {
        return res.status(400).json({ error: 'LinkedIn URL is required for Apollo lookup.' });
      }
      // Prefer per-user key from api_porting.html Option A; fall back to admin platform key.
      // When force_admin is set, skip per-user key and go straight to admin key.
      let apolloApiKey = null;
      if (!force_admin) {
        const userSvcCfgApollo = readUserServiceConfig(req.user.username);
        if (userSvcCfgApollo && userSvcCfgApollo.contact_gen?.provider === 'apollo' && userSvcCfgApollo.contact_gen?.APOLLO_API_KEY) {
          apolloApiKey = userSvcCfgApollo.contact_gen.APOLLO_API_KEY;
        }
      }
      if (!apolloApiKey) {
        const apolloCfg = (loadEmailVerifConfig().apollo) || {};
        if (!apolloCfg.api_key || apolloCfg.enabled !== 'enabled') {
          return res.status(400).json({ error: 'Apollo is not enabled or API key is missing.' });
        }
        apolloApiKey = apolloCfg.api_key;
      }

      // Normalize LinkedIn URL
      let apolloUrl = (linkedinurl || '').trim();
      if (apolloUrl && !apolloUrl.startsWith('https://') && !apolloUrl.startsWith('http://')) {
        apolloUrl = 'https://' + apolloUrl;
      }

      // Helper: make an Apollo HTTPS POST and resolve with {_http_status, ...body}
      const apolloPost = (path, bodyObj) => new Promise((resolve, reject) => {
        const bodyStr = JSON.stringify(bodyObj);
        const reqOpts = {
          hostname: 'api.apollo.io',
          port: 443,
          path,
          method: 'POST',
          headers: {
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'x-api-key': apolloApiKey,
            'Content-Length': Buffer.byteLength(bodyStr),
          },
        };
        console.log('[Apollo] Request: POST https://api.apollo.io' + path + ' body:', bodyStr);
        const r = https.request(reqOpts, (resp) => {
          let body = '';
          resp.on('data', d => body += d);
          resp.on('end', () => {
            console.log('[Apollo] HTTP status:', resp.statusCode);
            console.log('[Apollo] Raw response body:', body);
            try {
              const parsed = JSON.parse(body);
              parsed._http_status = resp.statusCode;
              resolve(parsed);
            } catch (e) {
              console.error('[Apollo] Failed to parse response JSON:', e.message, '| Body:', body);
              reject(new Error('Invalid JSON from Apollo API'));
            }
          });
        });
        r.on('error', (err) => { console.error('[Apollo] Network error:', err.message); reject(err); });
        r.setTimeout(20000, () => {
          console.error('[Apollo] Request timed out');
          r.destroy();
          reject(new Error('Apollo API timeout'));
        });
        r.write(bodyStr);
        r.end();
      });

      // Helper: extract phone fields from a person/contact object
      const extractPhones = (c) => {
        const phoneNumbers = Array.isArray(c.phone_numbers) ? c.phone_numbers : [];
        const mobileEntry = phoneNumbers.find(p => p.type === 'mobile' || (p.type && p.type.includes('mobile')));
        const officeEntry = phoneNumbers.find(p =>
          p.type === 'work_hq' || p.type === 'work_direct' || p.type === 'work' ||
          (p.type && p.type.includes('work'))
        );
        return {
          mobile_phone: c.mobile_phone || (mobileEntry && (mobileEntry.sanitized_number || mobileEntry.raw_number)) || '',
          office_phone: c.office_phone || (officeEntry && (officeEntry.sanitized_number || officeEntry.raw_number)) || '',
          phoneNumbers,
        };
      };

      console.log('[Apollo] Starting API call — linkedin_url:', apolloUrl);

      // Step 1: Search global database via mixed_people/search
      let contact = null;
      let usedFallback = false;

      try {
        const apolloRes = await apolloPost(
          '/api/v1/mixed_people/search',
          { person_linkedin_urls: [apolloUrl], per_page: 1, page: 1 }
        );

        const httpStatus = apolloRes._http_status;
        const apiMsg = (apolloRes.message || apolloRes.error || '').toLowerCase();
        const isPlanRestricted = apiMsg.includes('not accessible') && apiMsg.includes('free plan');

        if (httpStatus === 401) {
          return res.status(401).json({ error: 'Apollo authentication failed (HTTP 401)' });
        }

        if (httpStatus === 200 && !isPlanRestricted) {
          const people = Array.isArray(apolloRes.people) ? apolloRes.people : [];
          if (people.length > 0) {
            contact = people[0];
            console.log('[Apollo] mixed_people/search found person id:', contact.id);
          } else {
            console.log('[Apollo] mixed_people/search returned empty people — will try people/match fallback');
          }
        } else {
          console.log('[Apollo] mixed_people/search failed (status=' + httpStatus + ', plan_restricted=' + isPlanRestricted + ') — will try people/match fallback');
        }
      } catch (primaryErr) {
        console.error('[Apollo] mixed_people/search error:', primaryErr.message, '— will try people/match fallback');
      }

      // Step 2: Fallback to /v1/people/match if primary search did not return a contact
      if (!contact && apolloUrl) {
        try {
          console.log('[Apollo] people/match fallback for linkedin_url:', apolloUrl);
          const matchRes = await apolloPost(
            '/api/v1/people/match',
            { linkedin_url: apolloUrl, reveal_personal_emails: true, reveal_phone_number: true }
          );
          if (matchRes._http_status === 401) {
            return res.status(401).json({ error: 'Apollo authentication failed (HTTP 401)' });
          }
          if (matchRes._http_status === 200 && matchRes.person) {
            contact = matchRes.person;
            usedFallback = true;
            console.log('[Apollo] people/match found person id:', contact.id);
          } else {
            console.log('[Apollo] people/match returned no person — status:', matchRes._http_status);
          }
        } catch (fallbackErr) {
          console.error('[Apollo] people/match fallback error:', fallbackErr.message);
        }
      }

      if (!contact) {
        return res.status(200).json({ emails: [], all_emails: [], error: 'No matching contact found in Apollo. Verify the LinkedIn URL is correct and the contact exists in Apollo\'s database.' });
      }

      console.log('[Apollo] person fields — keys:', Object.keys(contact));
      console.log('[Apollo] contact.email:', JSON.stringify(contact.email));
      console.log('[Apollo] contact.phone_numbers:', JSON.stringify(contact.phone_numbers));

      const email = contact.email || '';
      const { mobile_phone, office_phone, phoneNumbers } = extractPhones(contact);

      const _toArr = (v) => {
        if (Array.isArray(v)) return v.filter(Boolean);
        if (typeof v === 'string' && v) return [v];
        return [];
      };

      const allEmails = _toArr(email).filter((v, i, a) => v && a.indexOf(v) === i);

      // Collect additional details for the comment section
      const _details = {
        name: contact.name || '',
        title: contact.title || '',
        organization_name: contact.organization_name || '',
        linkedin_url: contact.linkedin_url || apolloUrl,
        present_raw_address: contact.present_raw_address || '',
        account_phone: (contact.account && contact.account.phone) || '',
        sanitized_phone: contact.sanitized_phone || '',
        phone_numbers: phoneNumbers,
        email_status: contact.email_status || '',
        existence_level: contact.existence_level || '',
      };

      const result = {
        provider: 'apollo',
        email,
        mobile_phone,
        office_phone,
        all_emails: allEmails,
        _details,
      };
      console.log('[Apollo] Mapped result (' + (usedFallback ? 'people/match' : 'mixed_people/search') + '):', JSON.stringify(result));
      return res.json(result);
    }

    if (provider === 'rocketreach') {
      if (!linkedinurl) {
        return res.status(400).json({ error: 'LinkedIn URL is required for RocketReach lookup.' });
      }
      // Prefer per-user key from api_porting.html Option A; fall back to admin platform key.
      // When force_admin is set, skip per-user key and go straight to admin key.
      let rrApiKey = null;
      if (!force_admin) {
        const userSvcCfgRR = readUserServiceConfig(req.user.username);
        if (userSvcCfgRR && userSvcCfgRR.contact_gen?.provider === 'rocketreach' && userSvcCfgRR.contact_gen?.ROCKETREACH_API_KEY) {
          rrApiKey = userSvcCfgRR.contact_gen.ROCKETREACH_API_KEY;
        }
      }
      if (!rrApiKey) {
        const rrCfg = (loadEmailVerifConfig().rocketreach) || {};
        if (!rrCfg.api_key || rrCfg.enabled !== 'enabled') {
          return res.status(400).json({ error: 'RocketReach is not enabled or API key is missing.' });
        }
        rrApiKey = rrCfg.api_key;
      }

      let rrUrl = (linkedinurl || '').trim();
      if (rrUrl && !rrUrl.startsWith('https://') && !rrUrl.startsWith('http://')) {
        rrUrl = 'https://' + rrUrl;
      }

      console.log('[RocketReach] Starting API call — linkedin_url:', rrUrl);
      const rrRes = await new Promise((resolve, reject) => {
        const rrPath = `/api/v2/person/lookup?linkedin_url=${encodeURIComponent(rrUrl)}`;
        const reqOpts = {
          hostname: 'api.rocketreach.co',
          port: 443,
          path: rrPath,
          method: 'GET',
          headers: {
            'Accept': 'application/json',
            'Api-Key': rrApiKey,
          },
        };
        console.log('[RocketReach] Request: GET https://api.rocketreach.co' + rrPath);
        const r = https.request(reqOpts, (resp) => {
          let body = '';
          resp.on('data', d => body += d);
          resp.on('end', () => {
            console.log('[RocketReach] HTTP status:', resp.statusCode);
            console.log('[RocketReach] Raw response body:', body);
            try {
              const parsed = JSON.parse(body);
              parsed._http_status = resp.statusCode;
              resolve(parsed);
            } catch (e) {
              console.error('[RocketReach] Failed to parse response JSON:', e.message, '| Body:', body);
              reject(new Error('Invalid JSON from RocketReach API'));
            }
          });
        });
        r.on('error', (err) => {
          console.error('[RocketReach] Network error:', err.message);
          reject(err);
        });
        r.setTimeout(20000, () => {
          console.error('[RocketReach] Request timed out');
          r.destroy();
          reject(new Error('RocketReach API timeout'));
        });
        r.end();
      });

      if (rrRes._http_status && rrRes._http_status !== 200) {
        const apiMsg = rrRes.message || rrRes.detail || rrRes.error || `RocketReach API returned status ${rrRes._http_status}`;
        console.error('[RocketReach] API error — status:', rrRes._http_status, '| message:', apiMsg);
        return res.status(400).json({ error: apiMsg });
      }

      console.log('[RocketReach] emails:', JSON.stringify(rrRes.emails));
      console.log('[RocketReach] phones:', JSON.stringify(rrRes.phones));

      const _toArrRR = (v) => {
        if (Array.isArray(v)) return v.filter(Boolean);
        if (typeof v === 'string' && v) return [v];
        return [];
      };

      const emailObjs = _toArrRR(rrRes.emails);
      const emailStrs = emailObjs.map(e => (typeof e === 'object' ? e.email : e)).filter(Boolean);

      const phoneObjs = _toArrRR(rrRes.phones);
      const phoneStr = phoneObjs.length > 0
        ? (typeof phoneObjs[0] === 'object' ? (phoneObjs[0].number || phoneObjs[0].raw_number || '') : String(phoneObjs[0]))
        : '';

      const workEmails = emailObjs.filter(e => typeof e === 'object' && e.type === 'professional').map(e => e.email).filter(Boolean);
      const personalEmails = emailObjs.filter(e => typeof e === 'object' && e.type !== 'professional').map(e => e.email).filter(Boolean);

      const email = emailStrs[0] || '';
      const work_email = workEmails[0] || '';
      const personal_email = personalEmails[0] || '';
      const github = (rrRes.links && rrRes.links.github) ? rrRes.links.github : '';

      const allEmails = emailStrs.filter((v, i, a) => v && a.indexOf(v) === i);

      // ── Call LLM to structure the full profile into a clean comment ──────
      let structured_comment = '';
      try {
        const jobHistoryText = Array.isArray(rrRes.job_history) && rrRes.job_history.length > 0
          ? rrRes.job_history.map(j => {
              const start = j.start_date ? j.start_date.slice(0, 7) : '?';
              const end   = j.end_date   === 'Present' ? 'Present' : (j.end_date || '?').slice(0, 7);
              return `  - ${j.title || '?'} @ ${j.company_name || j.company || '?'} (${start} – ${end})`;
            }).join('\n')
          : '  (none)';

        const educationText = Array.isArray(rrRes.education) && rrRes.education.length > 0
          ? rrRes.education.map(e => {
              const majorPart = e.major ? ` in ${e.major}` : '';
              return `  - ${e.degree || '?'}${majorPart} @ ${e.school || '?'} (${e.start || '?'} – ${e.end || '?'})`;
            }).join('\n')
          : '  (none)';

        const contactEmailsText = allEmails.length > 0 ? allEmails.join(', ') : '(not found)';
        const contactPhoneText  = phoneStr || '(not found)';

        const rrStructurePrompt = `You are a data structuring assistant. Given the following raw profile information from a RocketReach API response, produce a clean plain-text summary organized under exactly four subheaders. Do NOT use markdown, asterisks, or bullet symbols — use plain dashes for list items. Keep each section concise.

Profile:
  Name: ${rrRes.name || '?'}
  Title: ${rrRes.current_title || '?'}
  Employer: ${rrRes.current_employer || '?'}
  Location: ${rrRes.location || (rrRes.city ? `${rrRes.city}, ${rrRes.country_code || ''}` : '?')}
  LinkedIn: ${rrRes.linkedin_url || '?'}

Employment history:
${jobHistoryText}

Education:
${educationText}

Contact:
  Emails: ${contactEmailsText}
  Phone: ${contactPhoneText}

Produce the output in this exact format (keep subheader names exactly as shown, plain text only):

[Profile]
Name: ...
Title: ...
Employer: ...
Location: ...
LinkedIn: ...

[Employment]
(one entry per line: Title @ Company (start – end))

[Education]
(one entry per line: Degree in Major @ School (start – end))

[Contact]
Email: ...
Phone: ...`;

        const llmText = await llmGenerateText(rrStructurePrompt, { username: req.user && req.user.username, label: 'llm/rocketreach-structure' });
        structured_comment = llmText.trim();
        console.log('[RocketReach] LLM structured comment generated, length:', structured_comment.length);
      } catch (llmErr) {
        console.error('[RocketReach] LLM structuring failed (non-fatal):', llmErr.message);
        // Fall back to a simple plain-text structure
        const fallbackParts = [
          `[Profile]`,
          rrRes.name           ? `Name: ${rrRes.name}` : null,
          rrRes.current_title  ? `Title: ${rrRes.current_title}` : null,
          rrRes.current_employer ? `Employer: ${rrRes.current_employer}` : null,
          rrRes.location       ? `Location: ${rrRes.location}` : null,
          rrRes.linkedin_url   ? `LinkedIn: ${rrRes.linkedin_url}` : null,
          '',
          `[Contact]`,
          allEmails.length > 0 ? `Email: ${allEmails.join(', ')}` : null,
          phoneStr             ? `Phone: ${phoneStr}` : null,
        ].filter(v => v !== null);
        structured_comment = fallbackParts.join('\n');
      }

      const result = {
        provider: 'rocketreach',
        email,
        phone: phoneStr,
        work_email,
        github,
        personal_email,
        all_emails: allEmails,
        structured_comment,
      };
      console.log('[RocketReach] Mapped result:', JSON.stringify(result));
      return res.json(result);
    }

    // ── Default FIOE/LLM path needs name + company ───────────────────────
    if (!name || !company) {
      return res.status(400).json({ error: 'Name and Company are required.' });
    }

    // ── FIOE path: check verified_email.json first ────────────────────────
    // Normalise the company key the same way as the save handler does.
    const companyKey = company.toLowerCase().replace(/[^a-z0-9]/g, '_');
    const verifiedEmailData = loadVerifiedEmail();
    const companyEntry = verifiedEmailData[companyKey];

    let genPrompt;
    let emailSource = 'gemini';
    let verifiedConfidence = null;
    if (companyEntry && Array.isArray(companyEntry.Domain) && companyEntry.Domain.length > 0) {
      // Find the entry with the highest confidence value
      const topEntry = companyEntry.Domain.reduce((best, e) =>
        (e.confidence || 0) > (best.confidence || 0) ? e : best,
        companyEntry.Domain[0]
      );
      emailSource = 'verified';
      verifiedConfidence = topEntry.confidence;
      // Ask Gemini to generate 3 variations using the verified domain structure
      genPrompt = `
        You are an email address generator. The following verified email domain structure has been confirmed for the company "${company}":
        - Domain: ${topEntry.domain}
        - Format: ${topEntry.format}
        - Example: ${topEntry.fake_example || '(not available)'}

        Using exactly this domain and format, generate 3 realistic email address variations for a person named "${name}"${country ? ` (located in ${country})` : ''}.
        Sort the list by highest probability of being the correct active email to lowest probability.
        Return strictly a JSON object: { "emails": ["email1", "email2", "email3"] }
        Do not include markdown formatting.
      `;
    } else {
      // No verified data — fall back to Gemini's own LLM knowledge with probability estimates
      genPrompt = `
        Generate the most likely business email addresses for a person named "${name}" working at the company "${company}"${country ? ` (located in ${country})` : ''}.
        Infer the likely domain name based on the company name.
        For each email address candidate, estimate a probability (0–100) that it is the correct active email.
        Return strictly a JSON object: { "emails": [{ "email": "addr1", "probability": 85 }, { "email": "addr2", "probability": 10 }] }
        Sort by highest probability first. Include at least 1 and at most 3 candidates.
        Do not include markdown formatting.
      `;
    }

    const genText = await llmGenerateText(genPrompt, { username: req.user && req.user.username, label: 'llm/email-gen' });
    incrementGeminiQueryCount(req.user.username).catch(() => {});
    
    // Clean markdown if present
    const jsonStr = genText.replace(_RE_CODE_FENCE, '').trim();
    let data;
    try {
      data = JSON.parse(jsonStr);
    } catch(e) {
       const match = genText.match(/\[.*\]/s);
       if (match) data = { emails: JSON.parse(match[0]) };
       else throw new Error("Failed to parse LLM email generation response");
    }

    // Normalise: Gemini fallback returns [{email, probability}] objects; verified path returns strings.
    let candidates = [];
    let topProbability = null;
    let emailProbabilities = [];
    const rawEmails = data.emails || [];
    if (emailSource === 'gemini' && rawEmails.length > 0 && typeof rawEmails[0] === 'object') {
      candidates = rawEmails.map(e => (typeof e === 'object' ? e.email : e)).filter(Boolean);
      emailProbabilities = rawEmails.map(e => (typeof e === 'object' && e.probability != null ? e.probability : null));
      topProbability = emailProbabilities.length > 0 ? emailProbabilities[0] : null;
    } else {
      candidates = rawEmails.map(e => (typeof e === 'object' ? e.email : e)).filter(Boolean);
      // For the verified path generate distinct declining probabilities per email so
      // each selectable tag shows a unique confidence value.
      if (emailSource === 'verified' && candidates.length > 0) {
        const basePct = Math.round((verifiedConfidence || 0.95) * 100);
        const scale = [1.0, 0.85, 0.70];
        emailProbabilities = candidates.map((_, i) => Math.min(100, Math.round(basePct * (scale[i] || 0.70))));
        topProbability = emailProbabilities[0];
      }
    }

    res.json({
      emails: candidates,
      source: emailSource,
      confidence: verifiedConfidence,
      probability: topProbability,
      email_probabilities: emailProbabilities,
    });

  } catch (err) {
    console.error('[generate-email] Unhandled error:', err.message || err);
    if (err.stack) console.error('[generate-email] Stack:', err.stack);
    res.status(500).json({ error: 'Generation failed. Check server logs for details.' });
  }
});

// ── Compensation Verified JSON helpers ───────────────────────────────────────
const COMP_VERIFIED_PATH = path.join(__dirname, 'compensation_verified.json');

// ── ML_Master_Compensation.json TTL cache ────────────────────────────────────
// Avoids a synchronous file read on every /crowd-comp request.
// _compMasterCache.compMap is a Map<normalizedTitle, entry> for O(1) lookups.
let _compMasterCache = null, _compMasterCacheTs = 0;
const _COMP_MASTER_CACHE_MS = parseInt(process.env.COMP_MASTER_CACHE_MS, 10) || 60_000;
const _normCompTitle = s => (s || '').trim().toLowerCase();

function _loadCompMasterCached() {
  const now = Date.now();
  if (_compMasterCache && now - _compMasterCacheTs < _COMP_MASTER_CACHE_MS) return _compMasterCache;
  const compPath = path.join(ML_OUTPUT_DIR, 'ML_Master_Compensation.json');
  let compData = {};
  try { compData = JSON.parse(fs.readFileSync(compPath, 'utf8')); } catch (e) {
    console.warn('[crowd-comp] failed to load ML_Master_Compensation.json:', e && e.message);
  }
  const compByJobTitle = compData.compensation_by_job_title || {};
  // Build Map for O(1) title lookup (avoids O(n) scan per candidate row)
  const compMap = new Map();
  for (const [jtKey, jtVal] of Object.entries(compByJobTitle)) {
    compMap.set(_normCompTitle(jtKey), jtVal);
  }
  _compMasterCache = { compMap };
  _compMasterCacheTs = now;
  return _compMasterCache;
}

// In-memory TTL cache (10 s) — avoids per-request disk reads for a rarely-changed file.
let _compVerifiedCache = null, _compVerifiedCacheTs = 0;
const _COMP_VERIFIED_CACHE_MS = 10_000;

function loadCompensationVerified() {
  const now = Date.now();
  if (_compVerifiedCache !== null && now - _compVerifiedCacheTs < _COMP_VERIFIED_CACHE_MS) return _compVerifiedCache;
  try {
    _compVerifiedCache = JSON.parse(fs.readFileSync(COMP_VERIFIED_PATH, 'utf8'));
  } catch (_) {
    _compVerifiedCache = {};
  }
  _compVerifiedCacheTs = now;
  return _compVerifiedCache;
}

function saveCompensationVerified(data) {
  const tmp = COMP_VERIFIED_PATH + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
  fs.renameSync(tmp, COMP_VERIFIED_PATH);
  _compVerifiedCache = data;
  _compVerifiedCacheTs = Date.now();
}

app.post('/save-compensation-verified', requireLogin, dashboardRateLimit, async (req, res) => {
  const { candidateId } = req.body || {};
  if (!candidateId) return res.status(400).json({ error: 'candidateId is required.' });
  const data = loadCompensationVerified();
  data[String(candidateId)] = { verified: true, saved_at: new Date().toISOString() };
  saveCompensationVerified(data);
  res.json({ ok: true, verifiedIds: Object.keys(data) });
});

app.get('/compensation-verified', requireLogin, dashboardRateLimit, async (req, res) => {
  const data = loadCompensationVerified();
  res.json({ verifiedIds: Object.keys(data) });
});

// ── Verified Email JSON helpers ───────────────────────────────────────────────
const VERIFIED_EMAIL_PATH = path.join(__dirname, 'verified_email.json');

// In-memory TTL cache (10 s) — avoids per-request disk reads for a file that is
// written infrequently but read on every /save-verified-email and /generate-email call.
let _verifiedEmailCache = null, _verifiedEmailCacheTs = 0;
const _VERIFIED_EMAIL_CACHE_MS = 10_000;

function loadVerifiedEmail() {
  const now = Date.now();
  if (_verifiedEmailCache !== null && now - _verifiedEmailCacheTs < _VERIFIED_EMAIL_CACHE_MS) return _verifiedEmailCache;
  let data;
  try {
    data = JSON.parse(fs.readFileSync(VERIFIED_EMAIL_PATH, 'utf8'));
    // Migrate legacy flat-array format to new company-keyed structure
    if (Array.isArray(data)) {
      const converted = {};
      for (const entry of data) {
        const companyKey = (entry.company || 'unknown').toLowerCase().replace(/[^a-z0-9]/g, '_');
        if (!converted[companyKey]) converted[companyKey] = { Domain: [], Confidence_threshold: 1 };
        converted[companyKey].Domain.push({ ...entry, company: companyKey, count: entry.count || 1, confidence: entry.confidence != null ? entry.confidence : 1 });
      }
      data = converted;
    } else {
      // Ensure every entry has a count field (handles data saved before count was introduced)
      for (const companyKey of Object.keys(data)) {
        const companyData = data[companyKey];
        if (Array.isArray(companyData.Domain)) {
          for (const entry of companyData.Domain) {
            if (entry.count === null || entry.count === undefined) entry.count = 1;
          }
        }
      }
    }
  } catch (_) {
    data = {};
  }
  _verifiedEmailCache = data;
  _verifiedEmailCacheTs = now;
  return _verifiedEmailCache;
}

// Redistribute confidence values across all domain entries for a company
// proportionally based on each entry's count (confidence = count / totalCount).
// The last entry receives the remainder to ensure the sum is exactly 1.
function recalculateConfidences(companyData) {
  const entries = companyData.Domain;
  if (!entries || entries.length === 0) return;
  const totalCount = entries.reduce((sum, e) => sum + (e.count || 1), 0);
  if (totalCount === 0) return;
  let sumSoFar = 0;
  entries.forEach((e, i) => {
    if (i === entries.length - 1) {
      e.confidence = parseFloat(Math.max(0, 1 - sumSoFar).toFixed(2));
    } else {
      e.confidence = parseFloat(((e.count || 1) / totalCount).toFixed(2));
      sumSoFar += e.confidence;
    }
  });
}

function saveVerifiedEmail(data) {
  const tmp = VERIFIED_EMAIL_PATH + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
  fs.renameSync(tmp, VERIFIED_EMAIL_PATH);
  _verifiedEmailCache = data;
  _verifiedEmailCacheTs = Date.now();
}

// Gemini-powered email normalization: derive format pattern + fake example for a domain
app.post('/save-verified-email', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { emails, name, company, candidateId } = req.body || {};
    if (!emails || !Array.isArray(emails) || emails.length === 0) {
      return res.status(400).json({ error: 'emails array is required.' });
    }
    if (!name || !company) {
      return res.status(400).json({ error: 'name and company are required.' });
    }

    const existing = loadVerifiedEmail();
    const companyKey = company.toLowerCase().replace(/[^a-z0-9]/g, '_');
    if (!existing[companyKey]) existing[companyKey] = { Domain: [], Confidence_threshold: 1 };
    const companyData = existing[companyKey];

    const newEntries = [];

    for (const email of emails) {
      if (!email || typeof email !== 'string') continue;
      const atIdx = email.lastIndexOf('@');
      if (atIdx < 0) continue;
      const localPart = email.slice(0, atIdx);
      const domain = email.slice(atIdx + 1).toLowerCase();

      // Check if this domain is already stored for this company
      const existingEntry = companyData.Domain.find(e => e.domain === domain);
      if (existingEntry) {
        // Increment count to reflect recruiter re-confirmation of this domain
        existingEntry.count = (existingEntry.count || 1) + 1;
        existingEntry.saved_at = new Date().toISOString();
        if (candidateId != null) existingEntry.candidateId = String(candidateId);
        newEntries.push(existingEntry);
        continue;
      }

      // Use Gemini to infer the format pattern and produce a fake normalized entry
      const normPrompt = `You are an email format analyst. Given:
- Real name: "${name}"
- Company: "${company}"
- Observed email local part: "${localPart}"
- Domain: "${domain}"

Analyze the format used for the local part of the email. Then:
1. Identify the format pattern (e.g. "first_name.last_name", "firstnamelastname", "f.lastname", "firstlastname" etc.)
2. Generate a completely fake example email using a generic made-up name (NOT the real name) that follows the same format.
   The fake name must be realistic-sounding but entirely fictional (e.g. "John Tan", "Oliver Chan").
3. Return ONLY a JSON object with these fields:
   {
     "format": "<pattern string>",
     "fake_example": "<fake_local_part>@${domain}",
     "fake_local_part": "<fake_local_part_only>"
   }
No markdown, no explanation.`;

      let format = localPart;
      let fake_example = '';
      let fake_local_part = '';
      try {
        const llmText = await llmGenerateText(normPrompt, { username: req.user && req.user.username, label: 'llm/email-norm' });
        const jsonStr = llmText.replace(_RE_CODE_FENCE, '').trim();
        const parsed = JSON.parse(jsonStr);
        format = parsed.format || localPart;
        fake_example = parsed.fake_example || '';
        fake_local_part = parsed.fake_local_part || '';
      } catch (e) {
        console.warn('[save-verified-email] LLM normalization failed (non-fatal):', e.message);
        // Fallback: store the format as-is without a fake example
        format = localPart;
        fake_example = '';
        fake_local_part = '';
      }

      const newEntry = {
        company: companyKey,
        domain,
        format,
        fake_example,
        fake_local_part,
        saved_at: new Date().toISOString(),
        candidateId: candidateId != null ? String(candidateId) : undefined,
        count: 1,
        confidence: 1, // placeholder; recalculated below
      };

      companyData.Domain.push(newEntry);
      newEntries.push(newEntry);
    }

    // Recalculate all confidences proportionally based on counts
    recalculateConfidences(companyData);

    if (newEntries.length > 0) {
      saveVerifiedEmail(existing);
    }

    res.json({ ok: true, added: newEntries.length, entries: newEntries });
  } catch (err) {
    console.error('[save-verified-email] error:', err.message || err);
    res.status(500).json({ error: 'Failed to save verified email.' });
  }
});

// ── Helper: normalise external verification API responses ────────────────────
function _httpsGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const options = {
      hostname: parsedUrl.hostname,
      path:     parsedUrl.pathname + parsedUrl.search,
      method:   'GET',
      headers,
    };
    const req = https.request(options, (res) => {
      let raw = '';
      res.on('data', d => { raw += d; });
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); }
        catch (e) { reject(new Error(`Invalid JSON from ${parsedUrl.hostname}: ${raw.slice(0, 200)}`)); }
      });
    });
    req.on('error', reject);
    req.setTimeout(EXTERNAL_API_TIMEOUT_MS, () => { req.destroy(); reject(new Error('Request timeout')); });
    req.end();
  });
}

async function _callExternalVerifService(service, email, apiKey) {
  const parts  = email.split('@');
  const account = parts[0] || '';
  const domain  = parts[1] || '';

  if (service === 'neverbounce') {
    const url = `https://api.neverbounce.com/v4/single/check?key=${encodeURIComponent(apiKey)}&email=${encodeURIComponent(email)}&address_info=1&credits_info=0`;
    const json = await _httpsGet(url);
    const statusMap = { valid: 'Capture All', catchall: 'catch-all', invalid: 'invalid', disposable: 'invalid', unknown: 'catch-all' };
    return {
      status:         statusMap[json.result] || 'unknown',
      sub_status:     json.flags ? json.flags.join(', ') : (json.result || 'unknown'),
      free_email:     (json.address_info && json.address_info.free_email_host) ? 'Yes' : 'No',
      account,
      smtp_provider:  (json.address_info && json.address_info.smtp_provider) || '—',
      first_name:     '—',
      last_name:      '—',
      domain,
      mx_found:       (json.address_info && json.address_info.mx_or_a) ? 'Yes' : 'No',
      mx_record:      '—',
      domain_age_days: 0,
      did_you_mean:   json.suggested_correction || '—',
    };
  }

  if (service === 'zerobounce') {
    // ZeroBounce v2 validate — email and apikey must be plain (not double-encoded)
    // in the query string. Build the URL using URLSearchParams to ensure correct
    // single-level percent-encoding that matches what ZeroBounce expects.
    const zbParams = new URLSearchParams({ api_key: apiKey, email, ip_address: '' });
    const url = `https://api.zerobounce.net/v2/validate?${zbParams.toString()}`;
    const json = await _httpsGet(url);
    // ZeroBounce returns an "error" field on failure (e.g. invalid key or bad request).
    if (json.error) throw new Error(`ZeroBounce: ${json.error}`);
    const statusMap = { valid: 'Capture All', invalid: 'invalid', 'catch-all': 'catch-all', spamtrap: 'invalid', abuse: 'invalid', 'do_not_mail': 'invalid', unknown: 'catch-all' };
    return {
      address:         json.address || email,
      status:          statusMap[json.status] || json.status || 'unknown',
      sub_status:      json.sub_status || '—',
      account:         json.account || account,
      domain:          json.domain || domain,
      did_you_mean:    json.did_you_mean || '—',
      domain_age_days: json.domain_age_days || 0,
      active_in_days:  json.active_in_days || '—',
      free_email:      json.free_email ? 'Yes' : 'No',
      mx_found:        json.mx_found  ? 'Yes' : 'No',
      mx_record:       json.mx_record || '—',
      smtp_provider:   json.smtp_provider || '—',
      first_name:      json.firstname || '—',
      last_name:       json.lastname  || '—',
      gender:          json.gender || '—',
      city:            json.city || '—',
      region:          json.region || '—',
      zipcode:         json.zipcode || '—',
      country:         json.country || '—',
      processed_at:    json.processed_at || '—',
    };
  }

  if (service === 'bouncer') {
    const url = `https://api.usebouncer.com/v1.1/email/verify?email=${encodeURIComponent(email)}&timeout=10`;
    const json = await _httpsGet(url, { 'x-api-key': apiKey });
    // Bouncer returns { status: 'failed', reason: '...' } on auth/API errors
    if (json.status === 'failed' && json.reason && !json.domain) {
      throw new Error(`Bouncer: ${json.reason}`);
    }
    return {
      email:           json.email || email,
      status:          json.status || 'unknown',
      reason:          json.reason || '—',
      domain_name:     (json.domain && json.domain.name) || domain,
      domain_accept_all: (json.domain && json.domain.acceptAll) || '—',
      domain_disposable: (json.domain && json.domain.disposable) || '—',
      domain_free:     (json.domain && json.domain.free) || '—',
      account_role:    (json.account && json.account.role) || '—',
      account_disabled: (json.account && json.account.disabled) || '—',
      account_full_mailbox: (json.account && json.account.fullMailbox) || '—',
      dns_type:        (json.dns && json.dns.type) || '—',
      dns_record:      (json.dns && json.dns.record) || '—',
      provider:        json.provider || '—',
      score:           json.score != null ? json.score : '—',
      toxic:           json.toxic || '—',
      toxicity:        json.toxicity != null ? json.toxicity : '—',
    };
  }

  throw new Error(`Unknown service: ${service}`);
}

// ========== NEW ENDPOINT: Verify Email Details via Gemini + SMTP PING ==========
app.post('/verify-email-details', requireLogin, async (req, res) => {
  try {
    const { email, service = 'default', force_admin } = req.body;
    if (!email) {
      return res.status(400).json({ error: 'Email is required.' });
    }

    // ── External service verification ───────────────────────────────────────
    if (['neverbounce', 'zerobounce', 'bouncer'].includes(service)) {
      // Prefer per-user key from api_porting.html Option A; fall back to admin platform key.
      // When force_admin is set, skip per-user key and go straight to admin key.
      let apiKey = null;
      if (!force_admin) {
        const userSvcCfg = readUserServiceConfig(req.user.username);
        if (userSvcCfg && userSvcCfg.active !== false && userSvcCfg.email_verif?.provider === service) {
          const keyField = service === 'neverbounce' ? 'NEVERBOUNCE_API_KEY'
            : service === 'zerobounce' ? 'ZEROBOUNCE_API_KEY' : 'BOUNCER_API_KEY';
          apiKey = userSvcCfg.email_verif?.[keyField] || null;
        }
      }
      if (!apiKey) {
        const config = loadEmailVerifConfig();
        const svcCfg = config[service] || {};
        if (svcCfg.enabled !== 'enabled' || !svcCfg.api_key) {
          return res.status(400).json({ error: `Service '${service}' is not configured or not enabled.` });
        }
        apiKey = svcCfg.api_key;
      }
      let result;
      try {
        result = await _callExternalVerifService(service, email, apiKey);
      } catch (exErr) {
        return res.status(502).json({ error: `External service error: ${exErr.message}` });
      }
      return res.json(result);
    }

    // ── Default: SMTP + LLM ──────────────────────────────────────────────────
    // 1. Perform Technical Checks First (MX + SMTP)
    const domain = email.split('@')[1];
    let mxRecords = [];
    let mxHost = null;
    let smtpStatus = 'unknown'; // valid, invalid, timeout, etc.

    try {
      mxRecords = await dns.resolveMx(domain);
      if (mxRecords && mxRecords.length > 0) {
        // sort by priority
        mxRecords.sort((a,b) => a.priority - b.priority);
        mxHost = mxRecords[0].exchange;
        
        // Real SMTP Handshake
        smtpStatus = await smtpVerify(email, mxHost);
      } else {
        smtpStatus = 'no_mx';
      }
    } catch (e) {
      smtpStatus = 'dns_error';
    }

    // 2. Ask Gemini to enhance metadata AND interpret result based on Enterprise logic
    // We pass the SMTP result to Gemini so it knows the technical reality
    const prompt = `
      Analyze this email address: "${email}".
      
      Technical Check Result:
      - MX Record: ${mxHost || 'None'}
      - SMTP Handshake Response: ${smtpStatus}

      Act as a strict email verification engine. 
      You must combine the technical check result with enterprise logic.

      Rules for Verification:
      1. STATUS: "Capture All" (Mapped from 'valid')
         - Use this status if SMTP Handshake was "valid" (250 OK).
         - OR if SMTP Handshake was "timeout/unknown" BUT the domain is known to be an Enterprise Gateway (Proofpoint/Mimecast/Google) AND you are highly confident the format is correct.
      2. STATUS: "invalid"
         - Use this if SMTP Handshake was "invalid" (550 User unknown).
         - OR if DNS/MX failed.
      3. STATUS: "catch-all"
         - Use this if the server accepts all emails (wildcard) but you cannot definitively confirm existence.

      Required Fields (Return strictly JSON):
      - status (String: "Capture All", "catch-all", or "invalid")
      - sub_status (String: "None" or failure detail)
      - free_email (String: "Yes" or "No")
      - account (String: part before @)
      - smtp_provider (String: inferred from MX e.g. "proofpoint", "google")
      - first_name (String: inferred)
      - last_name (String: inferred)
      - domain (String)
      - mx_found (String: "Yes" or "No")
      - mx_record (String)
      - domain_age_days (Integer: estimate)
      - did_you_mean (String)

      Example of Success:
      {
        "status": "Capture All",
        "sub_status": "None",
        "free_email": "No",
        "account": "john.doe",
        "smtp_provider": "proofpoint",
        "first_name": "John",
        "last_name": "Doe",
        "domain": "company.com",
        "mx_found": "Yes",
        "mx_record": "mxa-001.proofpoint.com",
        "domain_age_days": 4500,
        "did_you_mean": "Unknown"
      }
    `;

    const text = await llmGenerateText(prompt, { username: req.user && req.user.username, label: 'llm/email-validate' });
    incrementGeminiQueryCount(req.user.username).catch(() => {});

    const jsonStr = text.replace(_RE_CODE_FENCE, '').trim();
    let data;
    try {
      data = JSON.parse(jsonStr);
    } catch (e) {
       const match = text.match(/\{[\s\S]*\}/);
       if(match) data = JSON.parse(match[0]);
       else throw new Error("Failed to parse Gemini response");
    }

    res.json(data);
  } catch (err) {
    console.error('/verify-email-details error:', err);
    res.status(500).json({ error: 'Verification failed' });
  }
});

// ========== NEW: Draft Email Endpoint (AI) ==========
app.post('/draft-email', requireLogin, async (req, res) => {
    try {
        const { prompt: userPrompt, context } = req.body;
        const candidateName = context?.candidateName || 'Candidate';
        const myEmail = context?.myEmail || 'Me';

        const instruction = `
            Act as a professional recruiter. Write a draft email based on this request: "${userPrompt}".
            Context:
            - Recipient Name: ${candidateName}
            - Sender Email/Name: ${myEmail}
            
            Return strictly a JSON object with two fields:
            {
                "subject": "Email Subject Line",
                "body": "Email Body Text (plain text, use \\n for new lines)"
            }
            Do not wrap in markdown code blocks.
        `;

        const text = await llmGenerateText(instruction, { username: req.user && req.user.username, label: 'llm/email-draft' });
        incrementGeminiQueryCount(req.user.username).catch(() => {});
        const jsonStr = text.replace(_RE_CODE_FENCE, '').trim();
        let data;
        try {
            data = JSON.parse(jsonStr);
        } catch (e) {
             // fallback parsing if model output isn't perfect JSON
             const match = text.match(/\{[\s\S]*\}/);
             if(match) data = JSON.parse(match[0]);
             else throw new Error("Failed to parse AI draft response");
        }
        res.json(data);
    } catch (err) {
        console.error('/draft-email error:', err);
        res.status(500).json({ error: 'Drafting failed' });
    }
});

// ========== NEW: Send Email Endpoint (Nodemailer) ==========
app.post('/send-email', requireLogin, async (req, res) => {
    const { to, cc, bcc, subject, body, from, smtpConfig, ics, attachments } = req.body;

    let transporterConfig;

    // Build effective SMTP config: start from whatever the frontend sent, then
    // supplement with the server-side file when the password is absent.
    let effectiveSmtp = smtpConfig || {};
    if (effectiveSmtp.user && !effectiveSmtp.pass) {
        // The frontend knows the host/user but the password was not sent for
        // security reasons — load it from the per-user config file.
        const stored = await loadSmtpConfig(req.user.username);
        if (stored && stored.pass) {
            effectiveSmtp = { ...effectiveSmtp, pass: stored.pass };
        }
    }

    if (effectiveSmtp.user && effectiveSmtp.pass) {
        // Use provided (or file-supplemented) config
        transporterConfig = {
            host: effectiveSmtp.host || 'smtp.gmail.com',
            port: parseInt(effectiveSmtp.port || '587'),
            secure: effectiveSmtp.secure === true || effectiveSmtp.secure === 'true', // Handle string/bool
            auth: {
                user: effectiveSmtp.user,
                pass: effectiveSmtp.pass,
            },
        };
    } else {
        // Fallback: try the per-user config file, then env vars
        const stored = await loadSmtpConfig(req.user.username);
        if (stored && stored.user && stored.pass) {
            transporterConfig = {
                host: stored.host || 'smtp.gmail.com',
                port: parseInt(stored.port || '587'),
                secure: stored.secure === true || stored.secure === 'true',
                auth: { user: stored.user, pass: stored.pass },
            };
        } else {
            if (!process.env.SMTP_USER || !process.env.SMTP_PASS) {
                return res.status(500).json({ error: "Server configuration error: SMTP_USER or SMTP_PASS is missing in environment variables, and no custom config provided." });
            }
            transporterConfig = {
                host: process.env.SMTP_HOST || 'smtp.gmail.com',
                port: parseInt(process.env.SMTP_PORT || '587'),
                secure: process.env.SMTP_SECURE === 'true',
                auth: {
                    user: process.env.SMTP_USER,
                    pass: process.env.SMTP_PASS,
                },
            };
        }
    }

    try {
        // Reuse pooled transporter for the same SMTP config to avoid per-send connection overhead
        const transporter = getOrCreateTransporter(transporterConfig);

        // Build HTML email body: convert newlines to <br/> and render scheduler
        // booking URLs as a professional styled button matching FIOE brand colors
        // (Azure Dragon #073679 background, Desired Dawn #d8d8d8 text, Cool Blue #4c82b8 border).
        const schedulerBookingButton = url =>
          `<a href="${url}" style="display:inline-block;background:#073679;color:#d8d8d8;padding:10px 28px;border-radius:4px;text-decoration:none;font-family:Arial,Helvetica,sans-serif;font-weight:bold;font-size:14px;letter-spacing:0.3px;border:1px solid #4c82b8;">&#128197;&nbsp;Book a Time</a>`;
        const htmlBody = body
          ? body
              .replace(/\n/g, '<br/>')
              .replace(/https?:\/\/[^\s<>"]+\/scheduler\.html/g, schedulerBookingButton)
          : '';

        const mailOptions = {
            from: from || transporterConfig.auth.user, // Prefer user input > smtp user
            to,
            cc,
            bcc,
            subject,
            text: body, // plain text body
            html: htmlBody
        };

        // If ICS string provided, attach it as a calendar alternative to improve compatibility across clients.
        if (ics && typeof ics === 'string') {
          // Attach as an alternative content type for invites
          mailOptions.alternatives = mailOptions.alternatives || [];
          mailOptions.alternatives.push({
            contentType: 'text/calendar; charset="utf-8"; method=REQUEST',
            content: ics
          });
          // Also include as a downloadable attachment in some clients
          mailOptions.attachments = mailOptions.attachments || [];
          mailOptions.attachments.push({
            filename: 'invite.ics',
            content: ics,
            contentType: 'text/calendar'
          });
        }

        // Attach user-supplied files (sent as base64 from the frontend)
        if (Array.isArray(attachments) && attachments.length > 0) {
          mailOptions.attachments = mailOptions.attachments || [];
          for (const att of attachments) {
            if (att && att.filename && att.content) {
              mailOptions.attachments.push({
                filename: att.filename,
                content: Buffer.from(att.content, 'base64'),
                contentType: att.contentType || 'application/octet-stream'
              });
            }
          }
        }

        const info = await transporter.sendMail(mailOptions);
        console.log('Message sent: %s', info.messageId);
        res.json({ message: 'Email sent successfully', messageId: info.messageId });

    } catch (error) {
        console.error('Send email error:', error);
        // Return the error message to the client (which shows up in the alert)
        res.status(500).json({ error: "Failed to send email: " + error.message });
    }
});

// ========================= NEW: DASHBOARD API ENDPOINTS =========================

// Config: Fields allowed for filtering/aggregation
const ALLOWED_FIELDS = {
    country: "country",
    company: "company", 
    jobtitle: "jobtitle",
    sector: "sector",
    jobfamily: "jobfamily",
    geographic: "geographic",
    seniority: "seniority",
    skillset: "skillset", 
    sourcingstatus: "sourcingstatus",
    role_tag: "role_tag",
    product: "product",
    rating: "rating",
    pic: "pic",
    education: "education",
    comment: "comment",
    id: "id", // for simple count
    name: "name",
    linkedinurl: "linkedinurl"
};

/**
 * Helper to build WHERE clause from filters object
 * filters: { country: 'USA', seniority: 'Senior' }
 */
function buildWhereClause(filters, paramStartIdx = 1) {
    const conditions = [];
    const values = [];
    let idx = paramStartIdx;

    if (!filters) return { where: '', values, nextIdx: idx };

    for (const [key, val] of Object.entries(filters)) {
        if (ALLOWED_FIELDS[key] && val) {
            // Handle comma-separated values in filter as OR (simple implementation)
            // Or exact match. Let's do partial match or exact based on field type?
            // Dashboard filters usually imply equality or containment.
            // Using ILIKE for flexibility
            conditions.push(`"${ALLOWED_FIELDS[key]}" ILIKE $${idx}`);
            values.push(`%${val}%`); 
            idx++;
        }
    }

    const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';
    return { where, values, nextIdx: idx };
}

/**
 * POST /api/dashboard/query
 * General purpose endpoint for dashboard charts.
 * Body: { dimension: 'country', measure: 'count', filters: {...} }
 */
app.post('/api/dashboard/query', requireLogin, async (req, res) => {
    try {
        const { dimension, measure, filters } = req.body;
        
        if (!dimension || !ALLOWED_FIELDS[dimension]) {
            return res.status(400).json({ ok: false, error: 'Invalid or missing dimension' });
        }

        const col = ALLOWED_FIELDS[dimension];
        const { where, values } = buildWhereClause(filters);

        // Special handling for 'skillset' or multi-value fields if stored as comma-separated strings
        // For simplicity, we assume standard GROUP BY. 
        // If skillset is comma-separated, proper normalization requires unnesting which depends on DB structure.
        // Assuming simple string column for now as per schema.

        let sql = '';
        
        if (dimension === 'skillset') {
             // Attempt to unnest if it's a string with commas
             // PostgreSQL: unnest(string_to_array(skillset, ','))
             // We need to clean whitespace too.
             sql = `
                SELECT TRIM(s.token) as label, COUNT(*) as value
                FROM "process", unnest(string_to_array(skillset, ',')) as s(token)
                ${where}
                GROUP BY 1
                ORDER BY value DESC
                LIMIT 20
             `;
        } else {
             // Standard Group By
             sql = `
                SELECT "${col}" as label, COUNT(*) as value
                FROM "process"
                ${where}
                GROUP BY 1
                ORDER BY value DESC
                LIMIT 20
             `;
        }
        
        // If measuring ID count (KPI total)
        if (dimension === 'id') {
             sql = `SELECT COUNT(*) as total_rows FROM "process" ${where}`;
             const r = await pool.query(sql, values);
             return res.json({ ok: true, total_rows: parseInt(r.rows[0].total_rows) });
        }

        const result = await pool.query(sql, values);
        
        const labels = [];
        const data = [];
        
        result.rows.forEach(r => {
            if (r.label) {
                labels.push(r.label);
                data.push(parseInt(r.value));
            }
        });

        res.json({ ok: true, labels, data });

    } catch (e) {
        console.error('/api/dashboard/query error', e);
        res.status(500).json({ ok: false, error: e.message });
    }
});


/**
 * GET /api/dashboard/filter-options
 * Get distinct values for a filter dropdown
 * Query: ?field=country
 */
app.get('/api/dashboard/filter-options', requireLogin, async (req, res) => {
    try {
        const field = req.query.field;
        if (!field || !ALLOWED_FIELDS[field]) {
             return res.status(400).json({ ok: false, error: 'Invalid field' });
        }
        
        const col = ALLOWED_FIELDS[field];
        let sql = '';

        if (field === 'skillset') {
             sql = `
                SELECT DISTINCT TRIM(s.token) as val
                FROM "process", unnest(string_to_array(skillset, ',')) as s(token)
                ORDER BY 1 ASC
                LIMIT 100
             `;
        } else {
             sql = `SELECT DISTINCT "${col}" as val FROM "process" ORDER BY 1 ASC LIMIT 100`;
        }

        const result = await pool.query(sql);
        const options = result.rows.map(r => r.val).filter(Boolean);
        
        res.json({ ok: true, options });

    } catch (e) {
        console.error('/api/dashboard/filter-options error', e);
        res.status(500).json({ ok: false, error: e.message });
    }
});

/**
 * ========== NEW: Save Report Template Selection ==========
 */
app.post('/save-report-template', requireLogin, (req, res) => {
    try {
        const { reportId, dsAlias } = req.body;
        const username = req.user.username;
        if (!reportId) return res.status(400).json({ error: 'Report ID required' });
        
        // Validate dsAlias if provided: must be like "ds0", "ds1", ...
        let alias = null;
        if (typeof dsAlias !== 'undefined' && dsAlias !== null) {
            if (!/^ds\d+$/.test(String(dsAlias).trim())) {
                return res.status(400).json({ error: 'Invalid dsAlias. Expected format "ds0", "ds1", ...' });
            }
            alias = String(dsAlias).trim();
        }

        const filename = `template_${username}.json`;
        const filepath = path.resolve(__dirname, 'template', filename);
        
        const data = {
            username: username,
            reportId: reportId,
            dsAlias: alias,
            updatedAt: new Date().toISOString()
        };
        
        // Ensure template directory exists
        try { fs.mkdirSync(path.resolve(__dirname, 'template'), { recursive: true }); } catch (e) {}
        
        fs.writeFileSync(filepath, JSON.stringify(data, null, 2));
        
        res.json({ ok: true, message: 'Template saved', file: filename, dsAlias: alias });
    } catch (e) {
        console.error('Error saving template:', e);
        res.status(500).json({ error: 'Failed to save template' });
    }
});

// ========== PORT TO GOOGLE SHEETS / LOOKER STUDIO ==========

// --- DB Dockout format constants (mirror LookerDashboard.html PORT_COLS) ---
const PORT_COLS_SVR = [
  { header: 'name',           get: r => r.name || '' },
  { header: 'company',        get: r => r.company || '' },
  { header: 'jobtitle',       get: r => r.jobtitle || '' },
  { header: 'country',        get: r => r.country || '' },
  { header: 'linkedinurl',    get: r => r.linkedinurl || '' },
  { header: 'product',        get: r => r.product || '' },
  { header: 'sector',         get: r => r.sector || '' },
  { header: 'jobfamily',      get: r => r.jobfamily || '' },
  { header: 'geographic',     get: r => r.geographic || '' },
  { header: 'seniority',      get: r => r.seniority || '' },
  { header: 'skillset',       get: r => Array.isArray(r.skillset) ? r.skillset.join(', ') : (r.skillset || '') },
  { header: 'sourcingstatus', get: r => r.sourcingstatus || '' },
  { header: 'email',          get: r => r.email || '' },
  { header: 'mobile',         get: r => r.mobile || '' },
  { header: 'office',         get: r => r.office || '' },
  { header: 'comment',        get: r => r.comment || '' },
  { header: 'compensation',   get: r => r.compensation || '' },
];
const PORT_GEO_VALS = ['North America','South America','Western Europe','Eastern Europe','Middle East','Asia','Australia/Oceania','Africa'];
const PORT_SEN_VALS = ['Junior','Mid','Senior','Expert','Lead','Manager','Director','Executive'];
const PORT_ST_VALS  = ['Reviewing','Contacted','Unresponsive','Declined','Unavailable','Screened','Not Proceeding','Prospected'];

async function buildPortSheetCrypto(rows) {
  const EXCLUDE = new Set(['pic', 'cv']);
  const rawJsonStrings = rows.map(r => {
    const o = {};
    for (const [k, v] of Object.entries(r)) { if (!EXCLUDE.has(k)) o[k] = v; }
    return JSON.stringify(o);
  });
  const rawDbContent = rawJsonStrings.join('\n');
  const sha256hex = require('crypto').createHash('sha256').update(rawDbContent, 'utf8').digest('hex');
  let sigB64 = '', pubB64 = '';
  try {
    const subtle = require('crypto').webcrypto.subtle;
    const keyPair = await subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, true, ['sign','verify']);
    const dataBuffer = Buffer.from(rawDbContent, 'utf8');
    const sigBuf = await subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, keyPair.privateKey, dataBuffer);
    const pubBuf = await subtle.exportKey('spki', keyPair.publicKey);
    sigB64 = Buffer.from(sigBuf).toString('base64');
    pubB64 = Buffer.from(pubBuf).toString('base64');
  } catch (e) {
    console.warn('[PORT] ECDSA signing failed:', e.message);
  }
  return { rawJsonStrings, sha256: sha256hex, sigB64, pubB64 };
}

// 1. Initial Route: Redirects to Google Login
app.get('/port-to-looker', requireLogin, (req, res) => {
  if (!google) {
    return res.status(500).send("Google APIs not configured (module missing).");
  }
  const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
  const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || 'http://localhost:4000/auth/google/callback';
  
  if (!GOOGLE_CLIENT_ID) {
    return res.status(500).send("Google Client ID not configured in environment.");
  }

  // Scopes needed: Sheets (read/write), Drive (file creation/copying)
  // UPDATED: Added full drive access to fix 403 insufficient scope error on drive.files.copy
  const scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive' // Full drive access needed to copy arbitrary templates
  ];

  const oauth2Client = new google.auth.OAuth2(
    GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI
  );

  const url = oauth2Client.generateAuthUrl({
    access_type: 'offline', // ensures we get a refresh token if needed, though simple flow works without
    scope: scopes,
    prompt: 'consent', // Force consent screen to ensure new scopes are granted
    state: req.user.username // pass username to callback for tracking context
  });

  res.redirect(url);
});

// 2. Callback Route: Handles Auth Code -> CSV Export -> Sheet Creation -> Template Copy
app.get('/auth/google/callback', requireLogin, async (req, res) => {
  if (!google) return res.status(500).send("Google module missing.");
  
  const code = req.query.code;
  if (!code) return res.status(400).send("Authorization code missing.");

  try {
    const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
    const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
    const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || 'http://localhost:4000/auth/google/callback';
    
    // Check for user-specific template file first
    let LOOKER_TEMPLATE_ID = process.env.LOOKER_TEMPLATE_ID;
    let LOOKER_TEMPLATE_ALIAS = null;
    try {
        const templateFile = path.resolve(__dirname, 'template', `template_${req.user.username}.json`);
        if (fs.existsSync(templateFile)) {
            const tmplData = JSON.parse(fs.readFileSync(templateFile, 'utf8'));
            if (tmplData.reportId) {
                LOOKER_TEMPLATE_ID = tmplData.reportId;
                console.log(`[LOOKER] Using user-selected template: ${LOOKER_TEMPLATE_ID}`);
                if (tmplData.dsAlias && /^ds\d+$/.test(String(tmplData.dsAlias).trim())) {
                  LOOKER_TEMPLATE_ALIAS = String(tmplData.dsAlias).trim();
                  console.log(`[LOOKER] Using saved ds alias for this template: ${LOOKER_TEMPLATE_ALIAS}`);
              }
            }
        }
    } catch (e) {
        console.warn('Error reading user template file, falling back to ENV', e.message);
    }

    const oauth2Client = new google.auth.OAuth2(
      GOOGLE_CLIENT_ID,
      GOOGLE_CLIENT_SECRET,
      GOOGLE_REDIRECT_URI
    );

    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);

    // A. Export Data from Postgres (fetch all columns needed by PORT_COLS_SVR)
    const colsToExport = [
      'id', 'name', 'jobtitle', 'company', 'sector', 'jobfamily', 'role_tag',
      'skillset', 'geographic', 'country', 'email', 'mobile', 'office', 'comment',
      'compensation', 'seniority', 'sourcingstatus', 'product', 'userid', 'username',
      'linkedinurl', 'jskillset', 'lskillset', 'rating', 'tenure'
    ];
    const sqlExport = `SELECT ${colsToExport.map(c => `"${c}"`).join(', ')} FROM "process" WHERE userid = $1`;
    const result = await pool.query(sqlExport, [String(req.user?.id || '')]);
    const rows = result.rows;
    
    if (rows.length === 0) {
      return res.send("No data in database to export.");
    }

    // B. Create Google Sheet in DB Dockout format (mirrors handleDbPortExport / _portDownloadXLS)
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    const drive = google.drive({ version: 'v3', auth: oauth2Client });
    const dateStr = new Date().toISOString().slice(0, 10);

    // Map DB rows to PORT_COLS format (17 user-facing columns)
    const valueRows = [
      PORT_COLS_SVR.map(c => c.header),
      ...rows.map(r => PORT_COLS_SVR.map(col => String(col.get(r) ?? '')))
    ];

    // Build crypto artifacts (sha256 + ECDSA P-256 signature)
    const { rawJsonStrings, sha256, sigB64, pubB64 } = await buildPortSheetCrypto(rows);
    const MAX_CELL = 45000;
    const dbCopyRows = [['__json_export_v1__'], [`__sha256__:${sha256}`]];
    for (const s of rawJsonStrings) {
      const cells = [];
      for (let i = 0; i < s.length; i += MAX_CELL) cells.push(s.slice(i, i + MAX_CELL));
      dbCopyRows.push(cells);
    }

    // Read criteria files for embedding as hidden sheets
    const criteriaFiles = [];
    try {
      if (fs.existsSync(CRITERIA_DIR)) {
        const gsUsername = req.user && req.user.username ? String(req.user.username) : '';
        const gsCriteriaSuffix = gsUsername ? ` ${gsUsername}.json` : null;
        const cEntries = gsCriteriaSuffix
          ? fs.readdirSync(CRITERIA_DIR).filter(f =>
              f.toLowerCase().endsWith('.json') &&
              f.length >= gsCriteriaSuffix.length &&
              f.slice(-gsCriteriaSuffix.length).toLowerCase() === gsCriteriaSuffix.toLowerCase()
            )
          : [];
        for (const cName of cEntries) {
          try {
            const raw = fs.readFileSync(path.join(CRITERIA_DIR, cName), 'utf8');
            let content;
            try { content = JSON.parse(raw); } catch (_) { content = raw; }
            criteriaFiles.push({ name: cName, content, raw });
          } catch (_) { /* skip unreadable */ }
        }
      }
    } catch (e) { console.warn('[Google Sheets] Could not read criteria files:', e.message); }

    // Read orgchart and dashboard save-state files for the user
    const gsSafe = String(req.user.username).replace(/[^a-zA-Z0-9_\-]/g, '_');
    let orgchartState = null;
    let dashboardState = null;
    try {
      const ocPath = path.join(SAVE_STATE_DIR, `orgchart_${gsSafe}.json`);
      if (fs.existsSync(ocPath)) orgchartState = JSON.parse(fs.readFileSync(ocPath, 'utf8'));
    } catch (e) { console.warn('[Google Sheets] Could not read orgchart state:', e.message); }
    try {
      const dPath = getSaveStatePath(req.user.username);
      if (fs.existsSync(dPath)) dashboardState = JSON.parse(fs.readFileSync(dPath, 'utf8'));
    } catch (e) { console.warn('[Google Sheets] Could not read dashboard state:', e.message); }

    // Read ML profile for the user so it can be embedded as a hidden ML sheet
    // (mirrors the ML worksheet added to the Dock Out XLS export)
    let mlProfileData = null;
    try {
      const mlFilepath = path.join(ML_OUTPUT_DIR, `ML_${gsSafe}.json`);
      if (fs.existsSync(mlFilepath)) {
        mlProfileData = JSON.parse(fs.readFileSync(mlFilepath, 'utf8'));
      } else {
        // Compute on-the-fly if no persisted file exists
        mlProfileData = await _buildMLProfileData(String(req.user.id), String(req.user.username));
      }
    } catch (e) { console.warn('[Google Sheets] Could not read ML profile (non-fatal):', e.message); }

    // Build the extra hidden sheet definitions (Criteria1..N, orgchart, dashboard, ML)
    const extraSheetDefs = [];
    let nextSheetId = 3;
    criteriaFiles.forEach((_, idx) => {
      extraSheetDefs.push({ properties: { sheetId: nextSheetId, title: `Criteria${idx + 1}`, index: nextSheetId, hidden: true } });
      nextSheetId++;
    });
    if (orgchartState) {
      extraSheetDefs.push({ properties: { sheetId: nextSheetId, title: 'orgchart',  index: nextSheetId, hidden: true } });
      nextSheetId++;
    }
    if (dashboardState) {
      extraSheetDefs.push({ properties: { sheetId: nextSheetId, title: 'dashboard', index: nextSheetId, hidden: true } });
      nextSheetId++;
    }
    // ML sheet: always included when ML profile data is available (mirrors Dock Out XLS)
    const mlSheetId = mlProfileData ? nextSheetId++ : null;
    if (mlProfileData) {
      extraSheetDefs.push({ properties: { sheetId: mlSheetId, title: 'ML', index: mlSheetId, hidden: true } });
    }

    // Create spreadsheet with all sheets (Candidate Data + DB Copy + Signature + extra hidden)
    const createRes = await withExponentialBackoff(() => sheets.spreadsheets.create({
      resource: {
        properties: { title: `DB Port ${dateStr}` },
        sheets: [
          { properties: { sheetId: 0, title: 'Candidate Data', index: 0 } },
          { properties: { sheetId: 1, title: 'DB Copy',        index: 1, hidden: true } },
          { properties: { sheetId: 2, title: 'Signature',      index: 2, hidden: true } },
          ...extraSheetDefs,
        ]
      }
    }), { label: 'google/sheets-create' });
    const spreadsheetId = createRes.data.spreadsheetId;
    const spreadsheetUrl = createRes.data.spreadsheetUrl;

    // Write all 3 base sheets
    await withExponentialBackoff(() => sheets.spreadsheets.values.update({
      spreadsheetId, range: 'Candidate Data!A1', valueInputOption: 'RAW',
      resource: { values: valueRows }
    }), { label: 'google/sheets-write-data' });
    await withExponentialBackoff(() => sheets.spreadsheets.values.update({
      spreadsheetId, range: 'DB Copy!A1', valueInputOption: 'RAW',
      resource: { values: dbCopyRows }
    }), { label: 'google/sheets-write-copy' });
    await withExponentialBackoff(() => sheets.spreadsheets.values.update({
      spreadsheetId, range: 'Signature!A1', valueInputOption: 'RAW',
      resource: { values: [[sigB64], [pubB64], [String(req.user.username || '')], [String(req.user.id || '')]] }
    }), { label: 'google/sheets-write-sig' });

    // Write hidden Criteria sheets (File | name, JSON | rawJson, Key | Value, ...pairs)
    const flattenObj = (o, prefix) => {
      const r = [];
      for (const [k, v] of Object.entries(o || {})) {
        const key = prefix ? `${prefix}.${k}` : k;
        if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
          r.push(...flattenObj(v, key));
        } else {
          r.push([key, Array.isArray(v) ? v.join(', ') : String(v ?? '')]);
        }
      }
      return r;
    };
    for (let idx = 0; idx < criteriaFiles.length; idx++) {
      const cf = criteriaFiles[idx];
      const sheetTitle = `Criteria${idx + 1}`;
      let rawJson = '';
      let pairs = [];
      if (typeof cf.content !== 'string') {
        // cf.content is already a parsed object
        try { rawJson = JSON.stringify(cf.content); } catch (_) { rawJson = '{}'; }
        try { pairs = flattenObj(cf.content, ''); } catch (_) { /* ignore */ }
      } else {
        // cf.content is the raw string (JSON parsing failed at read time)
        rawJson = cf.raw || cf.content || '';
      }
      const criteriaRows = [
        ['File', cf.name || ''],
        ['JSON', rawJson],
        ['Key', 'Value'],
        ...pairs,
      ];
      await withExponentialBackoff(() => sheets.spreadsheets.values.update({
        spreadsheetId, range: `${sheetTitle}!A1`, valueInputOption: 'RAW',
        resource: { values: criteriaRows }
      }), { label: `google/sheets-write-criteria-${idx + 1}` });
    }

    // Write hidden orgchart sheet
    if (orgchartState) {
      const ocFileName = `orgchart_${gsSafe}.json`;
      let ocRawJson = '';
      try { ocRawJson = JSON.stringify(orgchartState); } catch (_) { ocRawJson = '{}'; }
      await withExponentialBackoff(() => sheets.spreadsheets.values.update({
        spreadsheetId, range: 'orgchart!A1', valueInputOption: 'RAW',
        resource: { values: [['File', ocFileName], ['JSON', ocRawJson]] }
      }), { label: 'google/sheets-write-orgchart' });
    }

    // Write hidden dashboard sheet
    if (dashboardState) {
      const dsFileName = `dashboard_${gsSafe}.json`;
      let dsRawJson = '';
      try { dsRawJson = JSON.stringify(dashboardState); } catch (_) { dsRawJson = '{}'; }
      await withExponentialBackoff(() => sheets.spreadsheets.values.update({
        spreadsheetId, range: 'dashboard!A1', valueInputOption: 'RAW',
        resource: { values: [['File', dsFileName], ['JSON', dsRawJson]] }
      }), { label: 'google/sheets-write-dashboard' });
    }

    // Write hidden ML sheet (mirrors the ML worksheet in the Dock Out XLS)
    // Row 0: Username header, Row 1: full JSON for lossless Dock In recreation,
    // Row 2: blank separator, Row 3: Key/Value header, Row 4+: flattened pairs
    if (mlProfileData) {
      const mlUsername = String(req.user.username || '');
      const mlFlatten = (o, prefix = '') => {
        const result = [];
        for (const [k, v] of Object.entries(o || {})) {
          const key = prefix ? `${prefix}.${k}` : k;
          if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
            result.push(...mlFlatten(v, key));
          } else {
            result.push([key, Array.isArray(v) ? v.join(', ') : String(v ?? '')]);
          }
        }
        return result;
      };
      let mlRawJson = '';
      try { mlRawJson = JSON.stringify(mlProfileData); } catch (_) { mlRawJson = '{}'; }
      const mlPairs = mlFlatten(mlProfileData);
      const mlSheetRows = [
        ['Username', mlUsername],
        ['JSON', mlRawJson],
        [],
        ['Key', 'Value'],
        ...mlPairs,
      ];
      await withExponentialBackoff(() => sheets.spreadsheets.values.update({
        spreadsheetId, range: 'ML!A1', valueInputOption: 'RAW',
        resource: { values: mlSheetRows }
      }), { label: 'google/sheets-write-ml' });
    }

    // Format: bold header, freeze row, column widths, data validation
    const batchRequests = [];
    batchRequests.push({
      repeatCell: {
        range: { sheetId: 0, startRowIndex: 0, endRowIndex: 1 },
        cell: { userEnteredFormat: { textFormat: { bold: true } } },
        fields: 'userEnteredFormat.textFormat.bold'
      }
    });
    batchRequests.push({
      updateSheetProperties: {
        properties: { sheetId: 0, gridProperties: { frozenRowCount: 1 } },
        fields: 'gridProperties.frozenRowCount'
      }
    });
    PORT_COLS_SVR.forEach((col, idx) => {
      batchRequests.push({
        updateDimensionProperties: {
          range: { sheetId: 0, dimension: 'COLUMNS', startIndex: idx, endIndex: idx + 1 },
          properties: { pixelSize: ['linkedinurl','skillset'].includes(col.header) ? 200 : 110 },
          fields: 'pixelSize'
        }
      });
    });
    const makeValidationReq = (colIdx, vals) => ({
      setDataValidation: {
        range: { sheetId: 0, startRowIndex: 1, endRowIndex: 1000, startColumnIndex: colIdx, endColumnIndex: colIdx + 1 },
        rule: {
          condition: { type: 'ONE_OF_LIST', values: vals.map(v => ({ userEnteredValue: v })) },
          showCustomUi: true, strict: false
        }
      }
    });
    const geoIdx = PORT_COLS_SVR.findIndex(c => c.header === 'geographic');
    const senIdx = PORT_COLS_SVR.findIndex(c => c.header === 'seniority');
    const stIdx  = PORT_COLS_SVR.findIndex(c => c.header === 'sourcingstatus');
    if (geoIdx >= 0) batchRequests.push(makeValidationReq(geoIdx, PORT_GEO_VALS));
    if (senIdx >= 0) batchRequests.push(makeValidationReq(senIdx, PORT_SEN_VALS));
    if (stIdx  >= 0) batchRequests.push(makeValidationReq(stIdx,  PORT_ST_VALS));
    await withExponentialBackoff(() => sheets.spreadsheets.batchUpdate({ spreadsheetId, resource: { requests: batchRequests } }), { label: 'google/sheets-format' });

    // C. Copy Looker Studio Template (If configured)
    let lookerUrl = "https://lookerstudio.google.com/"; // Default fallback

    // Normalize LOOKER_TEMPLATE_ID (accept URL or plain id)
    if (LOOKER_TEMPLATE_ID && LOOKER_TEMPLATE_ID.includes('http')) {
      const m = LOOKER_TEMPLATE_ID.match(/[-_A-Za-z0-9]{20,}/);
      if (m) LOOKER_TEMPLATE_ID = m[0];
    }

    // === NEW CHECK === 
    // If the ID looks like a Looker Studio UUID (contains hyphens), we cannot copy it via Drive API.
    // Instead, use the create URL to instantiate a report and inject the sheet ID.
    if (LOOKER_TEMPLATE_ID && /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(LOOKER_TEMPLATE_ID)) {
        console.log('[LOOKER] Detected Looker Studio reportId. Building create URL to inject the new Sheet as data source.');

        const encodedReportId = encodeURIComponent(LOOKER_TEMPLATE_ID);
        const encodedSheetId = encodeURIComponent(spreadsheetId || '');
        const encodedWorksheetId = encodeURIComponent('0');

        // Check if we have a specific valid alias from user configuration
        const aliasValidated = (LOOKER_TEMPLATE_ALIAS && /^ds\d+$/.test(LOOKER_TEMPLATE_ALIAS)) ? LOOKER_TEMPLATE_ALIAS : null;

        if (aliasValidated) {
            // Use specific alias
            lookerUrl = `https://lookerstudio.google.com/reporting/create?c.reportId=${encodedReportId}` +
                        `&${aliasValidated}.connector=googleSheets` +
                        `&${aliasValidated}.spreadsheetId=${encodedSheetId}` +
                        `&${aliasValidated}.worksheetId=${encodedWorksheetId}`;
            console.log('[LOOKER] create URL (using user alias):', lookerUrl);
        } else {
            // Best-effort: include several ds aliases (ds0..ds3) to catch common default aliases
            // This ensures the sheet is bound instantly even if the user didn't specify the alias manually
            const aliases = ['ds0','ds1','ds2','ds3'];
            const params = [`c.reportId=${encodedReportId}`];
            aliases.forEach(a => {
                params.push(`${a}.connector=googleSheets`);
                params.push(`${a}.spreadsheetId=${encodedSheetId}`);
                params.push(`${a}.worksheetId=${encodedWorksheetId}`);
            });
            lookerUrl = `https://lookerstudio.google.com/reporting/create?${params.join('&')}`;
            console.log('[LOOKER] create URL (best-effort multiple aliases):', lookerUrl.slice(0, 1000));
        }
    
    } else if (LOOKER_TEMPLATE_ID) {
      // Otherwise, assume it is a Drive File ID and try to copy
      try {
        // 1) Try to GET file metadata to determine visibility/permission results
        const fileMeta = await withExponentialBackoff(() => drive.files.get({ fileId: LOOKER_TEMPLATE_ID, fields: 'id,name,owners' }), { label: 'google/drive-meta' });
        console.log('[LOOKER] template visible:', fileMeta.data);

        // 2) Now attempt the copy
        const copyRes = await withExponentialBackoff(() => drive.files.copy({
          fileId: LOOKER_TEMPLATE_ID,
          resource: {
            name: `My Talent Dashboard - ${dateStr}`
          }
        }), { label: 'google/drive-copy' });
        
        console.log('[LOOKER] copy success:', copyRes.data);
        const fileInfo = await withExponentialBackoff(() => drive.files.get({
            fileId: copyRes.data.id,
            fields: 'webViewLink'
        }), { label: 'google/drive-link' });
        lookerUrl = fileInfo.data.webViewLink;
      } catch (err) {
        console.warn("Failed to copy template (maybe permissions?):", err.response?.data || err.message || err);
      }
    } else {
        console.log('[LOOKER] LOOKER_TEMPLATE_ID not configured; skipping template copy.');
    }

    // D. Success Response
    res.send(`
      <html>
        <body style="font-family: sans-serif; text-align: center; padding: 50px;">
          <h1 style="color: #1a73e8;">Success!</h1>
          <p>Your data has been ported to Google Drive in DB Dockout format.</p>
          <p style="color: #555; font-size: 14px;">The file contains a <strong>Candidate Data</strong> sheet, a hidden <strong>DB Copy</strong> sheet (digitally signed JSON), a hidden <strong>Signature</strong> sheet, and hidden <strong>Criteria</strong>, <strong>orgchart</strong>, and <strong>dashboard</strong> sheets.</p>
          <div style="margin: 20px 0;">
            <a href="${spreadsheetUrl}" target="_blank" style="display:inline-block; padding: 10px 20px; background: #188038; color: white; text-decoration: none; border-radius: 5px; margin: 5px;">
              Open Google Sheet
            </a>
            <a href="${lookerUrl}" target="_blank" style="display:inline-block; padding: 10px 20px; background: #4285f4; color: white; text-decoration: none; border-radius: 5px; margin: 5px;">
              Open Looker Studio Report
            </a>
          </div>
          <p style="color: #555; font-size: 14px;">
            <strong>Next Step:</strong> Open the Looker Studio report, click "Edit", select the data source, and "Reconnect" it to your new "DB Port" sheet.
          </p>
          <button onclick="window.close()" style="margin-top:20px;">Close Window</button>
        </body>
      </html>
    `);

  } catch (error) {
    console.error("Port to Looker Error:", error);
    res.status(500).send(`Export failed: ${error.message}`);
  }
});

// ========================= END DASHBOARD API =========================

// SSE Connection Management
const sseConnections = new Set();

// Heartbeat: write a comment line every 30 s so proxies/load-balancers don't
// time out idle connections and so dead clients are detected promptly.
// The interval reference is kept so it can be cleared in tests or graceful shutdown.
const _sseHeartbeatInterval = setInterval(() => {
  const dead = [];
  sseConnections.forEach(client => {
    try {
      client.write(':heartbeat\n\n');
    } catch (_) {
      dead.push(client);
    }
  });
  dead.forEach(c => sseConnections.delete(c));
}, _SSE_HEARTBEAT_MS);

// Coalesce rapid `candidates_changed` broadcasts that occur during bulk
// operations (bulk upsert, bulk delete, sync-entries etc.) — only the most
// recent payload is delivered after a 150 ms quiet period.
let _sseCandidatesTimer = null;
let _sseCandidatesPayload = null;

function _broadcastSSEImmediate(event, data) {
  const message = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  const dead = [];
  sseConnections.forEach(client => {
    try {
      client.write(message);
    } catch (e) {
      console.warn(`[SSE] Error broadcasting event '${event}' to client:`, e);
      dead.push(client);
    }
  });
  dead.forEach(c => sseConnections.delete(c));
}

function broadcastSSE(event, data) {
  if (event === 'candidates_changed') {
    // Coalesce: reset the timer on each rapid-fire call; only deliver the last payload
    _sseCandidatesPayload = data;
    if (_sseCandidatesTimer) clearTimeout(_sseCandidatesTimer);
    _sseCandidatesTimer = setTimeout(() => {
      _sseCandidatesTimer = null;
      _broadcastSSEImmediate('candidates_changed', _sseCandidatesPayload);
      _sseCandidatesPayload = null;
    }, _SSE_COALESCE_DELAY_MS);
    return;
  }
  _broadcastSSEImmediate(event, data);
}

// Batch broadcast: send N candidate rows as a single `candidates_batch_updated` event
// instead of N individual `candidate_updated` writes (avoids N×M SSE client writes).
function broadcastSSEBulk(rows) {
  if (!rows || rows.length === 0) return;
  if (rows.length === 1) {
    // No gain from batching a single row — use the standard path
    _broadcastSSEImmediate('candidate_updated', rows[0]);
    return;
  }
  _broadcastSSEImmediate('candidates_batch_updated', rows);
}

// SSE Endpoint for real-time updates
app.get('/api/events', (req, res) => {
  // Set headers for SSE
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  // Use the same CORS origins as the rest of the app
  const origin = req.headers.origin;
  if (allowedOrigins.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Credentials', 'true');
  }
  res.flushHeaders();

  // Add this connection to the set
  sseConnections.add(res);
  console.log('[SSE] client connected, total:', sseConnections.size);

  // Send initial connection confirmation
  res.write(`event: connected\ndata: ${JSON.stringify({ message: 'Connected to SSE' })}\n\n`);

  // Clean up on client disconnect
  req.on('close', () => {
    sseConnections.delete(res);
    console.log('[SSE] client disconnected, total:', sseConnections.size);
  });
});

// ========== API Porting System ==========
// Storage directory for uploaded env / API-key files.
// Defaults to  <project>/porting_input  but can be overridden in .env:
//   PORTING_INPUT_DIR="F:\Recruiting Tools\Autosourcing\input"
//
// Two-level-up fallback: server.js may live in <root>/Candidate Analyser/backend/
// while porting_input/ sits at <root>/ (same dir as webbridge.py).
// Search upward for an existing porting_input/ dir, then for the directory
// containing webbridge.py as the Autosourcing root marker — same reasoning
// as _EMAIL_VERIF_CONFIG_PATHS.
const PORTING_INPUT_DIR = (() => {
  if (process.env.PORTING_INPUT_DIR) return path.resolve(process.env.PORTING_INPUT_DIR);
  // Walk up from __dirname (up to 6 levels) to locate the Autosourcing root.
  // Prioritize the webbridge.py marker so we always match where
  // webbridge_routes.py writes — avoids stale porting_input dirs at closer levels.
  const levels = [];
  let cur = __dirname;
  for (let i = 0; i < 6; i++) {
    levels.push(cur);
    const parent = path.dirname(cur);
    if (parent === cur) break;          // filesystem root reached
    cur = parent;
  }
  // 1) Canonical: the directory that contains webbridge.py (Autosourcing root).
  for (const d of levels) {
    if (fs.existsSync(path.join(d, 'webbridge.py'))) return path.join(d, 'porting_input');
  }
  // 2) Fallback: an already-existing porting_input directory.
  for (const d of levels) {
    const p = path.join(d, 'porting_input');
    if (fs.existsSync(p)) return p;
  }
  return path.join(__dirname, 'porting_input');
})();
console.log('[startup] PORTING_INPUT_DIR resolved to', PORTING_INPUT_DIR);

// Confirmed field-mappings per user, persisted as JSON on disk.
// Same search-up pattern as PORTING_INPUT_DIR.
const PORTING_MAPPINGS_DIR = (() => {
  if (process.env.PORTING_MAPPINGS_DIR) return path.resolve(process.env.PORTING_MAPPINGS_DIR);
  const levels = [];
  let cur = __dirname;
  for (let i = 0; i < 6; i++) {
    levels.push(cur);
    const parent = path.dirname(cur);
    if (parent === cur) break;
    cur = parent;
  }
  for (const d of levels) {
    if (fs.existsSync(path.join(d, 'webbridge.py'))) return path.join(d, 'porting_mappings');
  }
  for (const d of levels) {
    const p = path.join(d, 'porting_mappings');
    if (fs.existsSync(p)) return p;
  }
  return path.join(__dirname, 'porting_mappings');
})();
console.log('[startup] PORTING_MAPPINGS_DIR resolved to', PORTING_MAPPINGS_DIR);

// Output XLS/CSV directory for Autosourcing results.
// Same webbridge.py-first search pattern as PORTING_INPUT_DIR.
const SEARCH_XLS_DIR = (() => {
  if (process.env.SEARCH_XLS_DIR) return path.resolve(process.env.SEARCH_XLS_DIR);
  const levels = [];
  let cur = __dirname;
  for (let i = 0; i < 6; i++) {
    levels.push(cur);
    const parent = path.dirname(cur);
    if (parent === cur) break;
    cur = parent;
  }
  // Canonical: directory containing webbridge.py + '/searchxls'
  for (const d of levels) {
    if (fs.existsSync(path.join(d, 'webbridge.py'))) return path.join(d, 'searchxls');
  }
  // Fallback: an already-existing searchxls directory
  for (const d of levels) {
    const p = path.join(d, 'searchxls');
    if (fs.existsSync(p)) return p;
  }
  return path.join(__dirname, 'searchxls');
})();
console.log('[startup] SEARCH_XLS_DIR resolved to', SEARCH_XLS_DIR);

// All columns present in the `process` table – used for Gemini mapping.
const PROCESS_TABLE_FIELDS = [
  'id','name','company','jobtitle','country','linkedinurl','username','userid',
  'product','sector','jobfamily','geographic','seniority','skillset',
  'sourcingstatus','email','mobile','office','role_tag','experience','cv',
  'education','exp','rating','pic','tenure','comment','vskillset',
  'compensation','lskillset','jskillset',
];

/** Encrypt a buffer with AES-256-GCM.  Returns a single Buffer:
 *  [16 bytes IV][16 bytes authTag][ciphertext] */
function encryptBuffer(buf) {
  const secret = process.env.PORTING_SECRET;
  if (!secret) {
    throw new Error('PORTING_SECRET environment variable is not set. Cannot encrypt data.');
  }
  const key = Buffer.from(secret.padEnd(32, '!').slice(0, 32));
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const encrypted = Buffer.concat([cipher.update(buf), cipher.final()]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, tag, encrypted]);
}

/** Sanitise a username to safe filename characters. */
function safeName(s) {
  return String(s).replace(/[^a-zA-Z0-9_\-]/g, '_');
}

// POST /api/porting/upload
// Accepts JSON body: { type: 'file'|'text', filename?: string, content: <base64|plain text> }
// Encrypts the payload and stores it in PORTING_INPUT_DIR.
app.post('/api/porting/upload', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { type, filename, content } = req.body || {};
    if (!type || !content) {
      return res.status(400).json({ error: 'Missing type or content' });
    }
    if (!['file', 'text'].includes(type)) {
      return res.status(400).json({ error: 'type must be "file" or "text"' });
    }

    // Determine raw buffer to encrypt
    let rawBuf;
    if (type === 'file') {
      // content is expected to be base64-encoded file data
      rawBuf = Buffer.from(content, 'base64');
    } else {
      rawBuf = Buffer.from(content, 'utf8');
    }

    // Enforce a reasonable size limit (1 MB)
    if (rawBuf.length > _PORTING_UPLOAD_MAX_BYTES) {
      return res.status(413).json({ error: `Content too large (max ${Math.round(_PORTING_UPLOAD_MAX_BYTES / 1024)} KB)` });
    }

    // Sanitise filename
    let safeFname = filename
      ? path.basename(String(filename)).replace(/[^a-zA-Z0-9_\-\.]/g, '_')
      : (type === 'file' ? 'upload.env' : 'api_keys.txt');
    // Prepend username + timestamp to avoid collisions
    safeFname = `${safeName(req.user.username)}_${Date.now()}_${safeFname}`;

    // Ensure directory exists
    await fs.promises.mkdir(PORTING_INPUT_DIR, { recursive: true });

    const encrypted = encryptBuffer(rawBuf);
    const destPath = path.join(PORTING_INPUT_DIR, safeFname + '.enc');
    await fs.promises.writeFile(destPath, encrypted);

    res.json({ ok: true, stored: safeFname + '.enc' });
  } catch (err) {
    console.error('[porting/upload]', err);
    res.status(500).json({ error: 'Upload failed', detail: err.message });
  }
});

// POST /api/porting/map
// Body: { names: string[] }  – list of external API field names to map.
// Uses Gemini to return a mapping object { externalName: processTableField }.
app.post('/api/porting/map', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { names } = req.body || {};
    if (!Array.isArray(names) || !names.length) {
      return res.status(400).json({ error: 'names must be a non-empty array' });
    }

    const fieldsStr = PROCESS_TABLE_FIELDS.join(', ');
    const namesStr  = names.map(n => `"${String(n).replace(/"/g, '')}"` ).join(', ');

    const prompt = `You are a database field mapping assistant.
Available target fields (PostgreSQL "process" table): ${fieldsStr}

Map each of the following external API field names to the SINGLE best-matching target field.
If there is no reasonable match, use null.
Return ONLY a JSON object (no markdown, no explanation) where each key is the input name and
each value is the matching target field name or null.

Input names: ${namesStr}`;

    let raw = await llmGenerateText(prompt, { username: req.user && req.user.username, label: 'llm/field-mapping' });
    incrementGeminiQueryCount(req.user.username).catch(() => {});

    // Strip markdown code fences if present
    raw = raw.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '').trim();

    let mapping;
    try {
      mapping = JSON.parse(raw);
    } catch (_) {
      return res.status(500).json({ error: 'LLM returned invalid JSON', raw });
    }

    // Validate: ensure all values are valid field names or null
    const cleaned = {};
    for (const [k, v] of Object.entries(mapping)) {
      cleaned[k] = (v && PROCESS_TABLE_FIELDS.includes(v)) ? v : null;
    }

    res.json({ ok: true, mapping: cleaned });
  } catch (err) {
    console.error('[porting/map]', err);
    res.status(500).json({ error: 'Mapping failed', detail: err.message });
  }
});

// POST /api/porting/confirm
// Body: { mapping: { externalName: processField|null } }
// Saves the confirmed mapping for the current user to disk.
app.post('/api/porting/confirm', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { mapping } = req.body || {};
    if (!mapping || typeof mapping !== 'object') {
      return res.status(400).json({ error: 'mapping is required' });
    }

    // Validate all values
    for (const [k, v] of Object.entries(mapping)) {
      if (v !== null && !PROCESS_TABLE_FIELDS.includes(v)) {
        return res.status(400).json({ error: `Invalid target field: ${v}` });
      }
    }

    await fs.promises.mkdir(PORTING_MAPPINGS_DIR, { recursive: true });
    const filePath = path.join(PORTING_MAPPINGS_DIR, `${safeName(req.user.username)}.json`);
    await fs.promises.writeFile(filePath, JSON.stringify({ username: req.user.username, mapping }, null, 2));

    res.json({ ok: true });
  } catch (err) {
    console.error('[porting/confirm]', err);
    res.status(500).json({ error: 'Confirm failed', detail: err.message });
  }
});

// GET /api/porting/mapping
// Returns the saved mapping for the current user (or null if none).
app.get('/api/porting/mapping', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const filePath = path.join(PORTING_MAPPINGS_DIR, `${safeName(req.user.username)}.json`);
    let data;
    try {
      data = JSON.parse(await fs.promises.readFile(filePath, 'utf8'));
    } catch (e) {
      if (e.code === 'ENOENT') return res.json({ mapping: null });
      throw e;
    }
    res.json({ mapping: data.mapping || null });
  } catch (err) {
    console.error('[porting/mapping]', err);
    res.status(500).json({ error: 'Could not load mapping', detail: err.message });
  }
});

// POST /api/porting/export
// Reads all process-table rows for the current user, applies their saved mapping,
// and returns a JSON file for download (or pushes to a configured target URL).
app.post('/api/porting/export', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const username = req.user.username;

    // Load saved mapping
    const mapFile = path.join(PORTING_MAPPINGS_DIR, `${safeName(username)}.json`);
    let mappingFileData;
    try {
      mappingFileData = JSON.parse(await fs.promises.readFile(mapFile, 'utf8'));
    } catch (e) {
      if (e.code === 'ENOENT') {
        return res.status(400).json({ error: 'No confirmed mapping found. Please complete the mapping step first.' });
      }
      throw e;
    }
    const { mapping } = mappingFileData;

    // Fetch all process rows for this user (exclude binary columns for JSON export)
    const cols = PROCESS_TABLE_FIELDS.filter(c => !['cv','pic'].includes(c));
    const dbRes = await pool.query(
      `SELECT ${cols.map(c => `"${c}"`).join(',')} FROM "process" WHERE username = $1`,
      [username]
    );

    if (!dbRes.rows.length) {
      return res.status(404).json({ error: 'No data found for this user in the process table.' });
    }

    // Apply mapping: rename process-table keys to external names
    const reverseMap = {};
    for (const [ext, proc] of Object.entries(mapping)) {
      if (proc) reverseMap[proc] = ext;
    }

    const exported = dbRes.rows.map(row => {
      const out = {};
      for (const col of cols) {
        const extName = reverseMap[col] || col;
        out[extName] = row[col] ?? null;
      }
      return out;
    });

    const jsonStr = JSON.stringify(exported, null, 2);

    // Optional: push to target URL if configured in request
    const { targetUrl } = req.body || {};
    if (targetUrl) {
      try {
        const urlObj = new URL(targetUrl);
        const lib = urlObj.protocol === 'https:' ? https : http;
        await new Promise((resolve, reject) => {
          const postReq = lib.request(
            { hostname: urlObj.hostname, port: urlObj.port || (urlObj.protocol === 'https:' ? 443 : 80),
              path: urlObj.pathname + urlObj.search, method: 'POST',
              headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(jsonStr) } },
            (r) => { r.resume(); r.on('end', resolve); }
          );
          postReq.on('error', reject);
          postReq.write(jsonStr);
          postReq.end();
        });
      } catch (pushErr) {
        console.warn('[porting/export] push to targetUrl failed:', pushErr.message);
        // Non-fatal; still return the JSON
      }
    }

    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="export_${safeName(username)}_${Date.now()}.json"`);
    res.send(jsonStr);
    _writeApprovalLog({ action: 'export_json_triggered', username, userid: req.user.id, detail: `Porting JSON export (${exported.length} rows)`, source: 'server.js' });
  } catch (err) {
    console.error('[porting/export]', err);
    res.status(500).json({ error: 'Export failed', detail: err.message });
  }
});

// ========== BYOK (Bring Your Own Keys) Endpoints ==========
const BYOK_REQUIRED_KEYS = [
  'GEMINI_API_KEY', 'GOOGLE_CSE_API_KEY', 'GOOGLE_API_KEY',
  'GOOGLE_CSE_CX', 'GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET',
];

function byokFilePath(username) {
  const dir = path.join(PORTING_INPUT_DIR, 'byok');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, `${safeName(username)}.enc`);
}

// POST /api/porting/byok/activate
// Body: { GEMINI_API_KEY, GOOGLE_CSE_API_KEY, GOOGLE_API_KEY, GOOGLE_CSE_CX, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET }
// Admin users must also supply: VERTEX_PROJECT, GOOGLE_APPLICATION_CREDENTIALS
// Validates all required keys are present, encrypts them, and stores per-user.
const BYOK_ADMIN_REQUIRED_KEYS = ['VERTEX_PROJECT', 'GOOGLE_APPLICATION_CREDENTIALS'];
app.post('/api/porting/byok/activate', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    // Check if user is admin
    const uaRes = await pool.query('SELECT useraccess FROM login WHERE username = $1 LIMIT 1', [req.user.username]);
    const isAdmin = uaRes.rows.length > 0 && (uaRes.rows[0].useraccess || '').toLowerCase() === 'admin';

    const keys = {};
    const missing = [];
    for (const k of BYOK_REQUIRED_KEYS) {
      const raw = req.body[k];
      if (typeof raw !== 'string' && typeof raw !== 'number') {
        missing.push(k);
        continue;
      }
      const val = String(raw).trim();
      // Enforce a reasonable value length limit (512 chars covers all known Google key formats)
      if (!val || val.length > 512) {
        missing.push(k);
      } else {
        keys[k] = val;
      }
    }
    // Admin users must also provide Vertex AI configuration
    if (isAdmin) {
      for (const k of BYOK_ADMIN_REQUIRED_KEYS) {
        const raw = req.body[k];
        if (typeof raw !== 'string' && typeof raw !== 'number') { missing.push(k); continue; }
        const val = String(raw).trim();
        if (!val || val.length > 1024) { missing.push(k); } else { keys[k] = val; }
      }
    }
    if (missing.length > 0) {
      return res.status(400).json({ error: `Missing required keys: ${missing.join(', ')}` });
    }
    const raw = Buffer.from(JSON.stringify({ username: req.user.username, keys }), 'utf8');
    const encrypted = encryptBuffer(raw);
    await fs.promises.writeFile(byokFilePath(req.user.username), encrypted);
    res.json({ ok: true, byok_active: true });
  } catch (err) {
    console.error('[porting/byok/activate]', err);
    res.status(500).json({ error: 'BYOK activation failed', detail: err.message });
  }
});

// GET /api/porting/byok/status
// Returns whether BYOK is currently active for the logged-in user.
app.get('/api/porting/byok/status', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const active = fs.existsSync(byokFilePath(req.user.username));
    res.json({ byok_active: active });
  } catch (err) {
    console.error('[porting/byok/status]', err);
    res.status(500).json({ error: 'Could not check BYOK status', detail: err.message });
  }
});

// GET /api/porting/credentials/status
// Returns whether the user has any uploaded credential files stored on disk.
app.get('/api/porting/credentials/status', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const prefix = safeName(req.user.username) + '_';
    let credentialsOnFile = false;
    if (fs.existsSync(PORTING_INPUT_DIR)) {
      credentialsOnFile = fs.readdirSync(PORTING_INPUT_DIR)
        .some(f => f.startsWith(prefix) && f.endsWith('.enc'));
    }
    res.json({ credentials_on_file: credentialsOnFile });
  } catch (err) {
    console.error('[porting/credentials/status]', err);
    res.status(500).json({ error: 'Could not check credential status', detail: err.message });
  }
});


// Removes the stored BYOK key file for the current user.
app.delete('/api/porting/byok/deactivate', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const dest = byokFilePath(req.user.username);
    if (fs.existsSync(dest)) fs.unlinkSync(dest);
    res.json({ ok: true, byok_active: false });
    _writeInfraLog({ event_type: 'byok_deactivated', username: req.user.username, userid: req.user.id, key_type: 'ALL', deactivation_reason: 'manual', detail: 'BYOK keys file removed', status: 'success', source: 'server.js' });
  } catch (err) {
    console.error('[porting/byok/deactivate]', err);
    res.status(500).json({ error: 'Could not deactivate BYOK', detail: err.message });
  }
});

// POST /api/porting/byok/validate
// Validates the supplied BYOK keys by probing live Google Cloud APIs and checking
// credential formats.  Returns a structured results array without storing anything.
// Steps:
//  1. Gemini API  — list models (validates GEMINI_API_KEY + billing)
//  2. Custom Search API — single query (validates GOOGLE_CSE_API_KEY + GOOGLE_CSE_CX)
//  3. GOOGLE_API_KEY format check
//  4. OAuth client credential format check (GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET)
//  5. (Admin only) Vertex AI configuration format check
app.post('/api/porting/byok/validate', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    // Check if user is admin
    const uaRes = await pool.query('SELECT useraccess FROM login WHERE username = $1 LIMIT 1', [req.user.username]);
    const isAdmin = uaRes.rows.length > 0 && (uaRes.rows[0].useraccess || '').toLowerCase() === 'admin';

    const keys = {};
    const missing = [];
    for (const k of BYOK_REQUIRED_KEYS) {
      const raw = req.body[k];
      if (typeof raw !== 'string' && typeof raw !== 'number') { missing.push(k); continue; }
      const val = String(raw).trim();
      if (!val || val.length > 512) missing.push(k); else keys[k] = val;
    }
    if (isAdmin) {
      for (const k of BYOK_ADMIN_REQUIRED_KEYS) {
        const raw = req.body[k];
        if (typeof raw !== 'string' && typeof raw !== 'number') { missing.push(k); continue; }
        const val = String(raw).trim();
        if (!val || val.length > 1024) missing.push(k); else keys[k] = val;
      }
    }
    if (missing.length > 0) {
      return res.status(400).json({ error: `Missing required keys: ${missing.join(', ')}` });
    }

    /** Make a GET request and return { status, body }. Rejects on network error. */
    function httpsGet(url, timeoutMs = 8000) {
      return new Promise((resolve, reject) => {
        const req = https.get(url, (r) => {
          let body = '';
          r.on('data', d => { body += d; });
          r.on('end', () => resolve({ status: r.statusCode, body }));
        });
        req.on('error', reject);
        req.setTimeout(timeoutMs, () => { req.destroy(new Error('timeout')); });
      });
    }

    function errorMsg(body, fallback) {
      try { return JSON.parse(body).error?.message || fallback; } catch (_) { return fallback; }
    }

    const results = [];

    // ── Step 1: Gemini API (GEMINI_API_KEY + billing) ──────────────────────────
    try {
      const { status, body } = await httpsGet(
        `https://generativelanguage.googleapis.com/v1beta/models?key=${encodeURIComponent(keys.GEMINI_API_KEY)}`
      );
      if (status === 200) {
        results.push({ step: 'gemini', label: 'Gemini API', status: 'ok',
          detail: 'API key is valid and billing is active.' });
      } else if (status === 403) {
        results.push({ step: 'gemini', label: 'Gemini API', status: 'error',
          detail: errorMsg(body, 'Gemini API is not enabled or billing is inactive on this project.'),
          consoleUrl: 'https://console.cloud.google.com/apis/library/generativelanguage.googleapis.com' });
      } else if (status === 400) {
        results.push({ step: 'gemini', label: 'Gemini API', status: 'error',
          detail: errorMsg(body, 'Invalid GEMINI_API_KEY.'),
          consoleUrl: 'https://console.cloud.google.com/apis/credentials' });
      } else {
        results.push({ step: 'gemini', label: 'Gemini API', status: 'warn',
          detail: `Unexpected HTTP ${status} — could not definitively confirm API status.` });
      }
    } catch (e) {
      results.push({ step: 'gemini', label: 'Gemini API', status: 'warn',
        detail: `Could not reach Google APIs: ${e.message}` });
    }

    // ── Step 2: Custom Search API (GOOGLE_CSE_API_KEY + GOOGLE_CSE_CX) ─────────
    try {
      const cseUrl = `https://customsearch.googleapis.com/customsearch/v1?key=${encodeURIComponent(keys.GOOGLE_CSE_API_KEY)}&cx=${encodeURIComponent(keys.GOOGLE_CSE_CX)}&q=test&num=1`;
      const { status, body } = await httpsGet(cseUrl);
      if (status === 200) {
        results.push({ step: 'cse', label: 'Custom Search API', status: 'ok',
          detail: 'CSE API key and Search Engine ID are valid.' });
      } else if (status === 403) {
        results.push({ step: 'cse', label: 'Custom Search API', status: 'error',
          detail: errorMsg(body, 'Custom Search API is not enabled or billing is required.'),
          consoleUrl: 'https://console.cloud.google.com/apis/library/customsearch.googleapis.com' });
      } else if (status === 400) {
        results.push({ step: 'cse', label: 'Custom Search API', status: 'error',
          detail: errorMsg(body, 'Invalid GOOGLE_CSE_API_KEY or GOOGLE_CSE_CX Search Engine ID.'),
          consoleUrl: 'https://console.cloud.google.com/apis/credentials' });
      } else {
        results.push({ step: 'cse', label: 'Custom Search API', status: 'warn',
          detail: `Unexpected HTTP ${status} — could not definitively confirm API status.` });
      }
    } catch (e) {
      results.push({ step: 'cse', label: 'Custom Search API', status: 'warn',
        detail: `Could not reach Custom Search API: ${e.message}` });
    }

    // ── Step 3: GOOGLE_API_KEY format ──────────────────────────────────────────
    const googleApiKeyOk = /^AIza[0-9A-Za-z\-_]{35}$/.test(keys.GOOGLE_API_KEY);
    results.push({ step: 'google_api_key', label: 'GOOGLE_API_KEY Format',
      status: googleApiKeyOk ? 'ok' : 'warn',
      detail: googleApiKeyOk
        ? 'Key format is valid (AIza… 39-character format).'
        : 'Key format looks unusual — expected a 39-character key starting with "AIza".',
      consoleUrl: googleApiKeyOk ? undefined : 'https://console.cloud.google.com/apis/credentials',
    });

    // ── Step 4: OAuth client credentials (GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET) ─
    const clientIdOk = /^\d+-[a-zA-Z0-9]+\.apps\.googleusercontent\.com$/.test(keys.GOOGLE_CLIENT_ID);
    const clientSecretOk = /^(GOCSPX-[A-Za-z0-9_\-]{28,}|[A-Za-z0-9_\-]{24,})$/.test(keys.GOOGLE_CLIENT_SECRET);
    if (!clientIdOk) {
      results.push({ step: 'oauth', label: 'OAuth Client Credentials', status: 'error',
        detail: 'GOOGLE_CLIENT_ID must have the format <numbers>-<id>.apps.googleusercontent.com',
        consoleUrl: 'https://console.cloud.google.com/apis/credentials' });
    } else if (!clientSecretOk) {
      results.push({ step: 'oauth', label: 'OAuth Client Credentials', status: 'warn',
        detail: 'GOOGLE_CLIENT_SECRET format looks unusual (expected "GOCSPX-…"). Verify it was copied from Google Cloud Console → Credentials → OAuth 2.0 Client.',
        consoleUrl: 'https://console.cloud.google.com/apis/credentials' });
    } else {
      results.push({ step: 'oauth', label: 'OAuth Client Credentials', status: 'ok',
        detail: 'Client ID and Client Secret formats are valid.' });
    }

    // ── Step 5 (Admin only): Vertex AI configuration format check ──────────────
    if (isAdmin) {
      const vertexProjectOk = /^[a-z][a-z0-9\-]{4,28}[a-z0-9]$/.test(keys.VERTEX_PROJECT || '');
      const gacOk = /\.json$/i.test(keys.GOOGLE_APPLICATION_CREDENTIALS || '');
      let vertexStatus = 'ok', vertexDetail = 'Vertex AI configuration looks valid.';
      const vertexIssues = [];
      if (!vertexProjectOk) vertexIssues.push('VERTEX_PROJECT must be a valid GCP project ID (lowercase, 6-30 chars)');
      if (!gacOk) vertexIssues.push('GOOGLE_APPLICATION_CREDENTIALS must end with .json');
      if (vertexIssues.length > 0) { vertexStatus = 'error'; vertexDetail = vertexIssues.join('; ') + '.'; }
      results.push({ step: 'vertex', label: 'Vertex AI Configuration', status: vertexStatus,
        detail: vertexDetail,
        consoleUrl: vertexStatus === 'error' ? 'https://console.cloud.google.com/vertex-ai' : undefined });
    }

    const allOk = results.every(r => r.status === 'ok' || r.status === 'warn');
    res.json({ ok: allOk, results });
  } catch (err) {
    console.error('[porting/byok/validate]', err);
    res.status(500).json({ error: 'Validation failed', detail: err.message });
  }
});

// ========== User Service Config: per-user provider keys ==========
// Encrypted file (when PORTING_SECRET is set):
//   path.join(PORTING_INPUT_DIR, 'user-services', `${safeName(username)}.enc`)
// Plaintext JSON fallback (when PORTING_SECRET is not set):
//   path.join(PORTING_INPUT_DIR, 'user-services', `${safeName(username)}.json`)

function _userSvcDir() {
  const dir = path.join(PORTING_INPUT_DIR, 'user-services');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return dir;
}
function userServiceConfigPath(username) {
  return path.join(_userSvcDir(), `${safeName(username)}.enc`);
}
function _userServiceJsonPath(username) {
  return path.join(_userSvcDir(), `${safeName(username)}.json`);
}

function decryptBuffer(buf) {
  const secret = process.env.PORTING_SECRET;
  if (!secret) throw new Error('PORTING_SECRET environment variable is not set.');
  const key = Buffer.from(secret.padEnd(32, '!').slice(0, 32));
  const iv  = buf.slice(0, 16);
  const tag = buf.slice(16, 32);
  const ct  = buf.slice(32);
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(ct), decipher.final()]);
}

/**
 * Read the per-user service config. Tries encrypted .enc first, then plaintext .json.
 * Returns the parsed config object, or null if no config exists.
 * Uses an mtime-based per-username in-memory cache to avoid repeated disk reads and
 * decryption on every contact-gen / email-verif request within the same session.
 */
// Cache: Map<username, { data: object|null, mtimeMs: number }>
const _userSvcCfgCache = new Map();

function readUserServiceConfig(username) {
  const encPath  = userServiceConfigPath(username);
  const jsonPath = _userServiceJsonPath(username);
  console.log('[readUserServiceConfig] %s → checking enc=%s  json=%s', username, encPath, jsonPath);

  // Determine the mtime of whichever backing file exists (enc takes priority).
  let activePath = null;
  let mtime = 0;
  try {
    if (fs.existsSync(encPath)) {
      activePath = encPath;
      mtime = fs.statSync(encPath).mtimeMs;
    } else if (fs.existsSync(jsonPath)) {
      activePath = jsonPath;
      mtime = fs.statSync(jsonPath).mtimeMs;
    }
  } catch (_) { /* ignore stat errors; will re-read below */ }

  if (!activePath) {
    _userSvcCfgCache.delete(username);
    return null;
  }

  const cached = _userSvcCfgCache.get(username);
  if (cached && cached.mtimeMs === mtime) return cached.data;

  // Cache miss — read and decrypt/parse the file.
  let data = null;
  if (activePath === encPath) {
    try {
      const raw = decryptBuffer(fs.readFileSync(encPath));
      data = JSON.parse(raw.toString('utf8'));
    } catch (err) {
      console.error('[readUserServiceConfig] .enc decrypt failed for', username, ':', err.message);
      // Fall through to JSON fallback.
      if (fs.existsSync(jsonPath)) {
        try {
          data = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
        } catch (parseErr) {
          console.error('[readUserServiceConfig] .json parse failed for', username, ':', parseErr.message);
        }
      }
    }
  } else {
    try {
      data = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
    } catch (parseErr) {
      console.error('[readUserServiceConfig] .json parse failed for', username, ':', parseErr.message);
    }
  }

  _userSvcCfgCache.set(username, { data, mtimeMs: mtime });
  return data;
}

/**
 * Write the per-user service config. Uses encryption when PORTING_SECRET is set,
 * otherwise writes a plaintext JSON file so the system works without a secret.
 * Invalidates the in-memory cache for this user.
 */
function writeUserServiceConfig(username, cfg) {
  const raw = Buffer.from(JSON.stringify(cfg), 'utf8');
  if (process.env.PORTING_SECRET) {
    fs.writeFileSync(userServiceConfigPath(username), encryptBuffer(raw));
  } else {
    // Plaintext fallback — no sensitive data stored in clear text beyond what
    // the operator has already consented to by not setting PORTING_SECRET.
    fs.writeFileSync(_userServiceJsonPath(username), raw);
  }
  _userSvcCfgCache.delete(username);
}

/**
 * Remove all config files for the user (both encrypted and plaintext).
 * Clears the in-memory cache entry for this user.
 */
function deleteUserServiceConfig(username) {
  [userServiceConfigPath(username), _userServiceJsonPath(username)].forEach(fp => {
    try { if (fs.existsSync(fp)) fs.unlinkSync(fp); } catch (_) {}
  });
  _userSvcCfgCache.delete(username);
}

// GET /api/user-service-config/status
// Returns { active: bool, providers: { search, llm, email_verif, contact_gen } } (masked — no key values)
app.get('/api/user-service-config/status', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const cfg = readUserServiceConfig(req.user.username);
    if (!cfg) {
      console.log('[user-service-config/status] No config found for', req.user.username);
      return res.json({ active: false, providers: { search: 'google_cse', llm: 'gemini', email_verif: 'default' } });
    }
    const providers = {
      search:      cfg.search?.provider      || 'google_cse',
      llm:         cfg.llm?.provider         || 'gemini',
      email_verif: cfg.email_verif?.provider || 'default',
      contact_gen: cfg.contact_gen?.provider || 'gemini',
    };
    console.log('[user-service-config/status] %s → active=true providers=%j', req.user.username, providers);
    res.json({ active: true, providers });
  } catch (err) {
    console.error('[user-service-config/status]', err);
    res.status(500).json({ error: 'Could not read service config', detail: err.message });
  }
});

// POST /api/user-service-config/activate
// Body: { search: { provider, SERPER_API_KEY?, DATAFORSEO_LOGIN?, DATAFORSEO_PASSWORD? },
//         llm:    { provider, OPENAI_API_KEY?, ANTHROPIC_API_KEY? },
//         email_verif: { provider, NEVERBOUNCE_API_KEY?, ZEROBOUNCE_API_KEY?, BOUNCER_API_KEY? },
//         contact_gen: { provider, CONTACTOUT_API_KEY?, APOLLO_API_KEY?, ROCKETREACH_API_KEY? } }
// Encrypts and stores config per user.
app.post('/api/user-service-config/activate', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { search, llm, email_verif, contact_gen } = req.body || {};
    const VALID_SEARCH      = ['google_cse', 'serper', 'dataforseo', 'linkedin'];
    const VALID_LLM         = ['gemini', 'openai', 'anthropic'];
    const VALID_EMAIL       = ['default', 'neverbounce', 'zerobounce', 'bouncer'];
    const VALID_CONTACT_GEN = ['gemini', 'contactout', 'apollo', 'rocketreach'];

    if (!search?.provider || !VALID_SEARCH.includes(search.provider)) {
      return res.status(400).json({ error: 'Invalid or missing search provider' });
    }
    if (!llm?.provider || !VALID_LLM.includes(llm.provider)) {
      return res.status(400).json({ error: 'Invalid or missing LLM provider' });
    }
    if (!email_verif?.provider || !VALID_EMAIL.includes(email_verif.provider)) {
      return res.status(400).json({ error: 'Invalid or missing email_verif provider' });
    }
    if (contact_gen && !VALID_CONTACT_GEN.includes(contact_gen.provider)) {
      return res.status(400).json({ error: 'Invalid contact_gen provider' });
    }

    // Validate that required keys are present for non-default providers
    const missing = [];
    if (search.provider === 'serper' && !search.SERPER_API_KEY?.trim()) missing.push('SERPER_API_KEY');
    if (search.provider === 'dataforseo') {
      if (!search.DATAFORSEO_LOGIN?.trim())    missing.push('DATAFORSEO_LOGIN');
      if (!search.DATAFORSEO_PASSWORD?.trim()) missing.push('DATAFORSEO_PASSWORD');
    }
    if (search.provider === 'linkedin' && !search.LINKEDIN_API_KEY?.trim()) missing.push('LINKEDIN_API_KEY');
    if (llm.provider === 'openai'    && !llm.OPENAI_API_KEY?.trim())    missing.push('OPENAI_API_KEY');
    if (llm.provider === 'anthropic' && !llm.ANTHROPIC_API_KEY?.trim()) missing.push('ANTHROPIC_API_KEY');
    if (email_verif.provider === 'neverbounce' && !email_verif.NEVERBOUNCE_API_KEY?.trim()) missing.push('NEVERBOUNCE_API_KEY');
    if (email_verif.provider === 'zerobounce'  && !email_verif.ZEROBOUNCE_API_KEY?.trim())  missing.push('ZEROBOUNCE_API_KEY');
    if (email_verif.provider === 'bouncer'     && !email_verif.BOUNCER_API_KEY?.trim())     missing.push('BOUNCER_API_KEY');
    if (contact_gen?.provider === 'contactout'  && !contact_gen.CONTACTOUT_API_KEY?.trim())  missing.push('CONTACTOUT_API_KEY');
    if (contact_gen?.provider === 'apollo'      && !contact_gen.APOLLO_API_KEY?.trim())      missing.push('APOLLO_API_KEY');
    if (contact_gen?.provider === 'rocketreach' && !contact_gen.ROCKETREACH_API_KEY?.trim()) missing.push('ROCKETREACH_API_KEY');
    if (missing.length > 0) {
      return res.status(400).json({ error: `Missing required keys: ${missing.join(', ')}` });
    }

    const cfg = {
      username: req.user.username,
      userid:   req.user.id,
      search:   { provider: search.provider },
      llm:      { provider: llm.provider },
      email_verif: { provider: email_verif.provider },
      contact_gen: { provider: contact_gen?.provider || 'gemini' },
    };
    if (search.provider === 'serper')     cfg.search.SERPER_API_KEY = search.SERPER_API_KEY.trim();
    if (search.provider === 'dataforseo') {
      cfg.search.DATAFORSEO_LOGIN    = search.DATAFORSEO_LOGIN.trim();
      cfg.search.DATAFORSEO_PASSWORD = search.DATAFORSEO_PASSWORD.trim();
    }
    if (search.provider === 'linkedin')   cfg.search.LINKEDIN_API_KEY = search.LINKEDIN_API_KEY.trim();
    if (llm.provider === 'openai')    cfg.llm.OPENAI_API_KEY    = llm.OPENAI_API_KEY.trim();
    if (llm.provider === 'anthropic') cfg.llm.ANTHROPIC_API_KEY = llm.ANTHROPIC_API_KEY.trim();
    if (email_verif.provider === 'neverbounce') cfg.email_verif.NEVERBOUNCE_API_KEY = email_verif.NEVERBOUNCE_API_KEY.trim();
    if (email_verif.provider === 'zerobounce')  cfg.email_verif.ZEROBOUNCE_API_KEY  = email_verif.ZEROBOUNCE_API_KEY.trim();
    if (email_verif.provider === 'bouncer')     cfg.email_verif.BOUNCER_API_KEY     = email_verif.BOUNCER_API_KEY.trim();
    if (contact_gen?.provider === 'contactout')  cfg.contact_gen.CONTACTOUT_API_KEY  = contact_gen.CONTACTOUT_API_KEY.trim();
    if (contact_gen?.provider === 'apollo')      cfg.contact_gen.APOLLO_API_KEY      = contact_gen.APOLLO_API_KEY.trim();
    if (contact_gen?.provider === 'rocketreach') cfg.contact_gen.ROCKETREACH_API_KEY = contact_gen.ROCKETREACH_API_KEY.trim();

    writeUserServiceConfig(req.user.username, cfg);
    res.json({ ok: true, active: true });
  } catch (err) {
    console.error('[user-service-config/activate]', err);
    res.status(500).json({ error: 'Activation failed', detail: err.message });
  }
});

// DELETE /api/user-service-config/deactivate
// Removes the config file(s) for the current user.
app.delete('/api/user-service-config/deactivate', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    deleteUserServiceConfig(req.user.username);
    res.json({ ok: true, active: false });
  } catch (err) {
    console.error('[user-service-config/deactivate]', err);
    res.status(500).json({ error: 'Deactivation failed', detail: err.message });
  }
});

// GET /api/user-service-config/search-keys
// Returns the decrypted search credentials for the authenticated user (masked for non-search providers).
// Used by AutoSourcing.html to inject per-user search keys into the /start_job payload.
app.get('/api/user-service-config/search-keys', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const cfg = readUserServiceConfig(req.user.username);
    if (!cfg) return res.json({ provider: 'google_cse' });
    const search = cfg.search || {};
    const result = { provider: search.provider || 'google_cse' };
    if (search.provider === 'serper'     && search.SERPER_API_KEY)     result.SERPER_API_KEY    = search.SERPER_API_KEY;
    if (search.provider === 'dataforseo' && search.DATAFORSEO_LOGIN)   result.DATAFORSEO_LOGIN   = search.DATAFORSEO_LOGIN;
    if (search.provider === 'dataforseo' && search.DATAFORSEO_PASSWORD) result.DATAFORSEO_PASSWORD = search.DATAFORSEO_PASSWORD;
    if (search.provider === 'linkedin'   && search.LINKEDIN_API_KEY)   result.LINKEDIN_API_KEY   = search.LINKEDIN_API_KEY;
    res.json(result);
  } catch (err) {
    console.error('[user-service-config/search-keys]', err);
    res.json({ provider: 'google_cse' });
  }
});

// POST /api/user-service-config/validate
// Validates provided keys by calling each service's API. Does NOT store anything.
// Returns { ok: bool, results: [{label, status, detail}] }
app.post('/api/user-service-config/validate', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { search, llm, email_verif, contact_gen } = req.body || {};

    function httpsGet(url, opts = {}) {
      return new Promise((resolve, reject) => {
        const parsed = new URL(url);
        const reqOpts = {
          hostname: parsed.hostname,
          path: parsed.pathname + parsed.search,
          method: opts.method || 'GET',
          headers: opts.headers || {},
          timeout: 8000,
        };
        const req = https.request(reqOpts, r => {
          let body = '';
          r.on('data', d => { body += d; });
          r.on('end', () => resolve({ status: r.statusCode, body }));
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(new Error('timeout')); });
        if (opts.body) req.write(opts.body);
        req.end();
      });
    }

    const results = [];

    // ── Search Engine ──────────────────────────────────────────────────────────
    if (search?.provider === 'google_cse') {
      results.push({ label: 'Search Engine', status: 'ok', detail: 'Using platform Google CSE — no custom key required.' });
    } else if (search?.provider === 'serper') {
      const key = (search.SERPER_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'Serper.dev', status: 'error', detail: 'SERPER_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet('https://google.serper.dev/search', {
            method: 'POST',
            headers: { 'X-API-KEY': key, 'Content-Type': 'application/json' },
            body: JSON.stringify({ q: 'test', num: 1 }),
          });
          if (status === 200) {
            results.push({ label: 'Serper.dev', status: 'ok', detail: 'API key is valid.' });
          } else if (status === 401 || status === 403) {
            results.push({ label: 'Serper.dev', status: 'error', detail: `Authentication failed (HTTP ${status}). Check your SERPER_API_KEY.` });
          } else {
            results.push({ label: 'Serper.dev', status: 'warn', detail: `Unexpected HTTP ${status} — key may be valid but quota or plan issue possible.` });
          }
        } catch (e) {
          results.push({ label: 'Serper.dev', status: 'warn', detail: `Could not reach Serper API: ${e.message}` });
        }
      }
    } else if (search?.provider === 'dataforseo') {
      const login = (search.DATAFORSEO_LOGIN || '').trim();
      const pass  = (search.DATAFORSEO_PASSWORD || '').trim();
      if (!login || !pass) {
        results.push({ label: 'DataforSEO', status: 'error', detail: 'DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD are both required.' });
      } else {
        try {
          const auth = Buffer.from(`${login}:${pass}`).toString('base64');
          const { status } = await httpsGet(
            'https://api.dataforseo.com/v3/serp/google/organic/task_get/advanced',
            { headers: { Authorization: `Basic ${auth}` } }
          );
          if (status === 200) {
            results.push({ label: 'DataforSEO', status: 'ok', detail: 'Credentials are valid.' });
          } else if (status === 401 || status === 403) {
            results.push({ label: 'DataforSEO', status: 'error', detail: `Authentication failed (HTTP ${status}). Check login/password.` });
          } else {
            results.push({ label: 'DataforSEO', status: 'warn', detail: `Unexpected HTTP ${status}. Credentials may be valid but check your plan.` });
          }
        } catch (e) {
          results.push({ label: 'DataforSEO', status: 'warn', detail: `Could not reach DataforSEO API: ${e.message}` });
        }
      }
    } else if (search?.provider === 'linkedin') {
      const key = (search.LINKEDIN_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'LinkedIn', status: 'error', detail: 'LINKEDIN_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet('https://api.linkedapi.io/v1/search', {
            method: 'POST',
            headers: { 'X-API-KEY': key, 'Content-Type': 'application/json' },
            body: JSON.stringify({ q: 'test', num: 1 }),
          });
          if (status === 200) {
            results.push({ label: 'LinkedIn', status: 'ok', detail: 'API key is valid.' });
          } else if (status === 401 || status === 403) {
            results.push({ label: 'LinkedIn', status: 'error', detail: `Authentication failed (HTTP ${status}). Check your LINKEDIN_API_KEY.` });
          } else {
            results.push({ label: 'LinkedIn', status: 'warn', detail: `Unexpected HTTP ${status} — key may be valid but quota or plan issue possible.` });
          }
        } catch (e) {
          results.push({ label: 'LinkedIn', status: 'warn', detail: `Could not reach LinkedIn API: ${e.message}` });
        }
      }
    }

    // ── LLM ───────────────────────────────────────────────────────────────────
    if (llm?.provider === 'gemini') {
      results.push({ label: 'LLM', status: 'ok', detail: 'Using platform Gemini — no custom key required.' });
    } else if (llm?.provider === 'openai') {
      const key = (llm.OPENAI_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'OpenAI', status: 'error', detail: 'OPENAI_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet('https://api.openai.com/v1/models', {
            headers: { Authorization: `Bearer ${key}` },
          });
          if (status === 200) {
            results.push({ label: 'OpenAI', status: 'ok', detail: 'API key is valid.' });
          } else if (status === 401) {
            results.push({ label: 'OpenAI', status: 'error', detail: 'Authentication failed. Check your OPENAI_API_KEY.' });
          } else {
            results.push({ label: 'OpenAI', status: 'warn', detail: `Unexpected HTTP ${status} — key may be valid but quota issue possible.` });
          }
        } catch (e) {
          results.push({ label: 'OpenAI', status: 'warn', detail: `Could not reach OpenAI API: ${e.message}` });
        }
      }
    } else if (llm?.provider === 'anthropic') {
      const key = (llm.ANTHROPIC_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'Anthropic', status: 'error', detail: 'ANTHROPIC_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
              'x-api-key': key,
              'anthropic-version': '2023-06-01',
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({ model: 'claude-3-haiku-20240307', max_tokens: 1,
              messages: [{ role: 'user', content: 'hi' }] }),
          });
          // 401 = bad key; anything else (including 400 for bad payload) = key accepted
          if (status === 401) {
            results.push({ label: 'Anthropic', status: 'error', detail: 'Authentication failed. Check your ANTHROPIC_API_KEY.' });
          } else {
            results.push({ label: 'Anthropic', status: 'ok', detail: `API key accepted (HTTP ${status}).` });
          }
        } catch (e) {
          results.push({ label: 'Anthropic', status: 'warn', detail: `Could not reach Anthropic API: ${e.message}` });
        }
      }
    }

    // ── Email Verification ────────────────────────────────────────────────────
    if (email_verif?.provider === 'default') {
      results.push({ label: 'Email Verification', status: 'ok', detail: 'Using platform default verification — no custom key required.' });
    } else if (email_verif?.provider === 'neverbounce') {
      const key = (email_verif.NEVERBOUNCE_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'NeverBounce', status: 'error', detail: 'NEVERBOUNCE_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet(
            `https://api.neverbounce.com/v4/account/info?key=${encodeURIComponent(key)}`
          );
          if (status === 200) {
            results.push({ label: 'NeverBounce', status: 'ok', detail: 'API key is valid.' });
          } else if (status === 401 || status === 403) {
            results.push({ label: 'NeverBounce', status: 'error', detail: `Authentication failed (HTTP ${status}). Check your NEVERBOUNCE_API_KEY.` });
          } else {
            results.push({ label: 'NeverBounce', status: 'warn', detail: `Unexpected HTTP ${status}.` });
          }
        } catch (e) {
          results.push({ label: 'NeverBounce', status: 'warn', detail: `Could not reach NeverBounce API: ${e.message}` });
        }
      }
    } else if (email_verif?.provider === 'zerobounce') {
      const key = (email_verif.ZEROBOUNCE_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'ZeroBounce', status: 'error', detail: 'ZEROBOUNCE_API_KEY is required.' });
      } else {
        try {
          const { status, body } = await httpsGet(
            `https://api.zerobounce.net/v2/getcredits?api_key=${encodeURIComponent(key)}`
          );
          if (status === 200) {
            let credits = null;
            try { credits = JSON.parse(body).Credits; } catch (_) {}
            if (credits !== null && Number(credits) > 0) {
              results.push({ label: 'ZeroBounce', status: 'ok', detail: `API key valid. Credits remaining: ${credits}.` });
            } else if (credits === 0 || credits === '0') {
              results.push({ label: 'ZeroBounce', status: 'warn', detail: 'API key valid but account has 0 credits.' });
            } else {
              results.push({ label: 'ZeroBounce', status: 'ok', detail: 'API key accepted.' });
            }
          } else if (status === 400 || status === 401) {
            results.push({ label: 'ZeroBounce', status: 'error', detail: `Authentication failed (HTTP ${status}). Check your ZEROBOUNCE_API_KEY.` });
          } else {
            results.push({ label: 'ZeroBounce', status: 'warn', detail: `Unexpected HTTP ${status}.` });
          }
        } catch (e) {
          results.push({ label: 'ZeroBounce', status: 'warn', detail: `Could not reach ZeroBounce API: ${e.message}` });
        }
      }
    } else if (email_verif?.provider === 'bouncer') {
      const key = (email_verif.BOUNCER_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'Bouncer', status: 'error', detail: 'BOUNCER_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet('https://api.usebouncer.com/v1.1/account', {
            headers: { 'x-api-key': key },
          });
          if (status === 200) {
            results.push({ label: 'Bouncer', status: 'ok', detail: 'API key is valid.' });
          } else if (status === 401 || status === 403) {
            results.push({ label: 'Bouncer', status: 'error', detail: `Authentication failed (HTTP ${status}). Check your BOUNCER_API_KEY.` });
          } else {
            results.push({ label: 'Bouncer', status: 'warn', detail: `Unexpected HTTP ${status}.` });
          }
        } catch (e) {
          results.push({ label: 'Bouncer', status: 'warn', detail: `Could not reach Bouncer API: ${e.message}` });
        }
      }
    }

    // ── Contact Generation ────────────────────────────────────────────────────
    if (!contact_gen || contact_gen.provider === 'gemini') {
      results.push({ label: 'Contact Generation', status: 'ok', detail: 'Using platform Gemini — no custom key required.' });
    } else if (contact_gen.provider === 'contactout') {
      const key = (contact_gen.CONTACTOUT_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'ContactOut', status: 'error', detail: 'CONTACTOUT_API_KEY is required.' });
      } else {
        try {
          // Use email_type=none so no credits are consumed — this only checks auth
          const { status } = await httpsGet(
            'https://api.contactout.com/v1/people/linkedin?profile=https://www.linkedin.com/in/test&email_type=none&include_phone=false',
            { headers: { 'Content-Type': 'application/json', Accept: 'application/json', token: key } }
          );
          if (status === 401) {
            results.push({ label: 'ContactOut', status: 'error', detail: 'Authentication failed (HTTP 401). Check your CONTACTOUT_API_KEY.' });
          } else if (status === 403) {
            // 403 from ContactOut means account suspended or quota exceeded, NOT invalid key
            results.push({ label: 'ContactOut', status: 'warn', detail: 'ContactOut returned HTTP 403 — key may be valid but your account may be suspended or quota exceeded.' });
          } else if (status === 200 || status === 404 || status === 422) {
            // 200 = profile found, 404 = profile not found (key valid), 422 = invalid URL (key valid)
            results.push({ label: 'ContactOut', status: 'ok', detail: 'API key accepted.' });
          } else {
            results.push({ label: 'ContactOut', status: 'warn', detail: `Unexpected HTTP ${status} — key may be valid but check your account or try again.` });
          }
        } catch (e) {
          results.push({ label: 'ContactOut', status: 'warn', detail: `Could not reach ContactOut API: ${e.message}` });
        }
      }
    } else if (contact_gen.provider === 'apollo') {
      const key = (contact_gen.APOLLO_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'Apollo', status: 'error', detail: 'APOLLO_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet('https://api.apollo.io/v1/auth/health', {
            headers: { 'x-api-key': key, 'Content-Type': 'application/json' },
          });
          if (status === 200) {
            results.push({ label: 'Apollo', status: 'ok', detail: 'API key is valid.' });
          } else if (status === 401) {
            results.push({ label: 'Apollo', status: 'error', detail: 'Authentication failed (HTTP 401). Check your APOLLO_API_KEY.' });
          } else if (status === 403) {
            results.push({ label: 'Apollo', status: 'warn', detail: 'Apollo returned HTTP 403 — key may be valid but your account may lack access. Check your plan.' });
          } else if (status >= 500) {
            results.push({ label: 'Apollo', status: 'warn', detail: `Apollo API returned HTTP ${status} — server may be temporarily unavailable. Try again.` });
          } else {
            results.push({ label: 'Apollo', status: 'warn', detail: `Unexpected HTTP ${status} — key may be valid but check your plan.` });
          }
        } catch (e) {
          results.push({ label: 'Apollo', status: 'warn', detail: `Could not reach Apollo API: ${e.message}` });
        }
      }
    } else if (contact_gen.provider === 'rocketreach') {
      const key = (contact_gen.ROCKETREACH_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'RocketReach', status: 'error', detail: 'ROCKETREACH_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet('https://api.rocketreach.co/api/v2/checkStatus', {
            headers: { 'Api-Key': key },
          });
          if (status === 200) {
            results.push({ label: 'RocketReach', status: 'ok', detail: 'API key is valid.' });
          } else if (status === 401) {
            results.push({ label: 'RocketReach', status: 'error', detail: 'Authentication failed (HTTP 401). Check your ROCKETREACH_API_KEY.' });
          } else if (status === 403) {
            results.push({ label: 'RocketReach', status: 'warn', detail: 'RocketReach returned HTTP 403 — key may be valid but your account may be suspended or quota exceeded.' });
          } else if (status >= 500) {
            results.push({ label: 'RocketReach', status: 'warn', detail: `RocketReach API returned HTTP ${status} — server may be temporarily unavailable. Try again.` });
          } else {
            results.push({ label: 'RocketReach', status: 'warn', detail: `Unexpected HTTP ${status} — key may be valid but check your plan.` });
          }
        } catch (e) {
          results.push({ label: 'RocketReach', status: 'warn', detail: `Could not reach RocketReach API: ${e.message}` });
        }
      }
    }

    const hasError = results.some(r => r.status === 'error');
    res.json({ ok: !hasError, results });
  } catch (err) {
    console.error('[user-service-config/validate]', err);
    res.status(500).json({ error: 'Validation failed', detail: err.message });
  }
});

// ========== Dashboard Save / Load / Delete State ==========
// State files are stored per-user as dashboard_<username>.json / orgchart_<username>.json in SAVE_STATE_DIR.
// SAVE_STATE_DIR and getSaveStatePath() are declared earlier in the file (before /candidates/clear-user).

// POST /dashboard/save-state  –  save dashboard + slide state as JSON
app.post('/dashboard/save-state', dashboardRateLimit, requireLogin, async (req, res) => {
    try {
        const { dashboard, slide } = req.body || {};
        const username = req.user.username;

        // Ensure directory exists
        await fs.promises.mkdir(SAVE_STATE_DIR, { recursive: true }).catch(e => {
            // recursive:true never throws EEXIST; any other error is a real failure
            console.error('Failed to create save-state directory:', e.message);
            throw e;
        });

        const filepath = getSaveStatePath(username);
        const payload = {
            username,
            savedAt: new Date().toISOString(),
            dashboard: dashboard || null,
            slide: slide || null
        };

        await fs.promises.writeFile(filepath, JSON.stringify(payload, null, 2), 'utf8');
        res.json({ ok: true, message: 'State saved', file: path.basename(filepath) });
    } catch (e) {
        console.error('/dashboard/save-state error:', e);
        res.status(500).json({ ok: false, error: 'Failed to save state' });
    }
});

// GET /dashboard/load-state  –  load state for the logged-in user
app.get('/dashboard/load-state', dashboardRateLimit, requireLogin, async (req, res) => {
    try {
        const filepath = getSaveStatePath(req.user.username);
        let raw;
        try {
            raw = await fs.promises.readFile(filepath, 'utf8');
        } catch (e) {
            if (e.code === 'ENOENT') return res.json({ ok: true, found: false });
            throw e;
        }
        const payload = JSON.parse(raw);
        res.json({ ok: true, found: true, data: payload });
    } catch (e) {
        console.error('/dashboard/load-state error:', e);
        res.status(500).json({ ok: false, error: 'Failed to load state' });
    }
});

// DELETE /dashboard/delete-state  –  delete the logged-in user's state file
app.delete('/dashboard/delete-state', dashboardRateLimit, requireLogin, (req, res) => {
    try {
        const filepath = getSaveStatePath(req.user.username);
        if (fs.existsSync(filepath)) {
            fs.unlinkSync(filepath);
        }
        res.json({ ok: true, message: 'State deleted' });
    } catch (e) {
        console.error('/dashboard/delete-state error:', e);
        res.status(500).json({ ok: false, error: 'Failed to delete state' });
    }
});

// POST /orgchart/save-state  –  save org chart manual-parent overrides as JSON
// File is stored as orgchart_<username>.json in SAVE_STATE_DIR
app.post('/orgchart/save-state', dashboardRateLimit, requireLogin, async (req, res) => {
    try {
        const { overrides, candidates } = req.body || {};
        const username = req.user.username;

        await fs.promises.mkdir(SAVE_STATE_DIR, { recursive: true }).catch(e => {
            console.error('Failed to create save-state directory:', e.message);
            throw e;
        });

        const safe = String(username).replace(/[^a-zA-Z0-9_\-]/g, '_');
        const filepath = path.join(SAVE_STATE_DIR, `orgchart_${safe}.json`);
        const payload = {
            username,
            savedAt: new Date().toISOString(),
            overrides: overrides || {},
            candidates: candidates || []
        };

        await fs.promises.writeFile(filepath, JSON.stringify(payload, null, 2), 'utf8');
        res.json({ ok: true, message: 'Org chart state saved', file: path.basename(filepath) });
    } catch (e) {
        console.error('/orgchart/save-state error:', e);
        res.status(500).json({ ok: false, error: 'Failed to save org chart state' });
    }
});

// GET /orgchart/load-state  –  load org chart state for the logged-in user
app.get('/orgchart/load-state', dashboardRateLimit, requireLogin, async (req, res) => {
    try {
        const safe = String(req.user.username).replace(/[^a-zA-Z0-9_\-]/g, '_');
        const filepath = path.join(SAVE_STATE_DIR, `orgchart_${safe}.json`);
        let raw;
        try {
            raw = await fs.promises.readFile(filepath, 'utf8');
        } catch (e) {
            if (e.code === 'ENOENT') return res.json({ ok: true, found: false });
            throw e;
        }
        const payload = JSON.parse(raw);
        res.json({ ok: true, found: true, data: payload });
    } catch (e) {
        console.error('/orgchart/load-state error:', e);
        res.status(500).json({ ok: false, error: 'Failed to load org chart state' });
    }
});
};
