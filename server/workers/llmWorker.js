'use strict';
/**
 * server/workers/llmWorker.js
 *
 * Registers job handlers on the shared 'llm' queue for all LLM-backed
 * background tasks.  Import and call `initLlmWorker(ctx)` once from
 * server.js after the queue module has been required.
 *
 * Each handler receives a `job` object shaped as:
 *   { id: string, data: { type: string, …payload… } }
 *
 * Supported job types:
 *   'calc-unmatched'   — calculate unmatched skillset for a candidate
 *   'assess-unmatched' — full skill-gap score assessment
 *   'verify-data'      — LLM normalisation of candidate data fields
 *   'ai-comp'          — AI compensation estimation
 *
 * How to enqueue from server_routes2.js (example):
 *   const { getLlmQueue } = require('../workers/llmWorker');
 *   const { id: jobId } = getLlmQueue().enqueue({ type: 'calc-unmatched', candidateId: 42, username: 'alice', … });
 *   res.status(202).json({ queued: true, jobId, candidateId: 42 });
 */

const { createQueue, createWorker } = require('../queue');

const QUEUE_NAME = 'llm';

let _queue = null;

/** Return the shared LLM queue (created on first call). */
function getLlmQueue() {
  if (!_queue) _queue = createQueue(QUEUE_NAME);
  return _queue;
}

/**
 * Register all LLM job handlers.
 * Must be called once at startup with the same ctx passed to registerRoutes.
 *
 * @param {object} ctx  Shared server context (pool, llmGenerateText, …)
 */
function initLlmWorker(ctx) {
  const {
    pool,
    llmGenerateText,
    incrementGeminiQueryCount,
    normalizeCompanyName,
    picToDataUri,
  } = ctx;

  // Import regexes that are already module-level in server_routes2.js.
  // We duplicate them here so the worker module is self-contained and does not
  // create a circular dependency on server_routes2.
  const _RE_CALC_UM_INTRO    = /^(Here are|The following|These are).*?[:\n]/gim;
  const _RE_CALC_UM_LONG     = /Here are the skills present[^:\n]*[:\s]*/i;
  const _RE_CALC_UM_BRACKETS = /[\[\]"']/g;
  const _RE_CALC_UM_DELIM    = /[\n\r,]+/g;
  const _RE_CALC_UM_BULLET   = /^[-*•]\s+/;
  const _RE_ASSESS_CODE_FENCE = /```(?:json)?/g;

  // broadcastSSE is a module-level closure in server_routes2.js; we accept it
  // optionally via ctx so the worker can notify SSE clients.
  const broadcastSSE     = ctx.broadcastSSE     || (() => {});
  const broadcastSSEBulk = ctx.broadcastSSEBulk || (() => {});

  // ── Handler: calc-unmatched ────────────────────────────────────────────────
  async function handleCalcUnmatched(data) {
    const { candidateId, jdSkillset, candidateSkillset, sector, jobFamily, username } = data;

    const prompt = `
      Compare the Job Description (JD) Skillset and the Candidate Skillset below.
      Context:
      - Sector: "${sector || 'Unknown'}"
      - Job Family: "${jobFamily || 'Unknown'}"

      Identify the skills that are present in the JD Skillset but are MISSING or UNMATCHED in the Candidate Skillset.
      
      JD Skillset: "${jdSkillset || ''}"
      Candidate Skillset: "${candidateSkillset || ''}"
      
      Return the result as a simple list. Do NOT include any introductory or explanatory text.
    `;

    const rawText = await llmGenerateText(prompt, { username, label: 'llm/skill-gap' });
    incrementGeminiQueryCount(username).catch(() => {});

    let cleaned = rawText.replace(_RE_CALC_UM_INTRO, '');
    cleaned = cleaned.replace(_RE_CALC_UM_LONG, '');
    cleaned = cleaned.replace(_RE_CALC_UM_BRACKETS, '');
    cleaned = cleaned.replace(_RE_CALC_UM_DELIM, ';');

    const tokens = cleaned
      .split(';')
      .map(s => s.trim().replace(_RE_CALC_UM_BULLET, '').replace(/^[-*•]/, ''))
      .filter(s => s.length > 0);

    const unmatchedStr = tokens.join('; ');

    const updateRes = await pool.query(
      'UPDATE "process" SET lskillset = $1 WHERE id = $2 RETURNING *',
      [unmatchedStr, candidateId]
    );

    const r = updateRes.rows[0];
    if (!r) return;

    const companyCanonical = normalizeCompanyName(r.company || r.organisation || '');
    const mapped = {
      ...r,
      jobtitle:     r.jobtitle    ?? null,
      company:      companyCanonical ?? (r.company ?? null),
      lskillset:    r.lskillset   ?? null,
      linkedinurl:  r.linkedinurl ?? null,
      jskillset:    r.jskillset   ?? null,
      pic:          picToDataUri(r.pic),
      role:         r.role        ?? r.jobtitle ?? null,
      organisation: companyCanonical ?? (r.organisation ?? r.company ?? null),
      type:         r.product     ?? null,
      compensation: r.compensation ?? null,
    };
    try { broadcastSSE('candidate_updated', mapped); } catch (_) {}
  }

  // ── Handler: assess-unmatched ──────────────────────────────────────────────
  async function handleAssessUnmatched(data) {
    const { candidateId, unmatchedSkills, username } = data;

    if (!unmatchedSkills || unmatchedSkills.length === 0) {
      broadcastSSE('skill_assessment_result', {
        candidateId,
        score: 100,
        matchedSkills: [],
        unmatchedSkills: [],
      });
      return;
    }

    const prompt = `
      You are a recruitment analyst. Given a list of unmatched skills for a candidate, assess the overall skill gap.
      Unmatched skills: ${unmatchedSkills.join(', ')}
      Return a JSON object with:
      - score (0-100, where 100 means no gap)
      - matchedSkills (array)
      - unmatchedSkills (array)
      Return only valid JSON, no markdown.
    `;

    const rawText = await llmGenerateText(prompt, { username, label: 'llm/assess-unmatched' });
    incrementGeminiQueryCount(username).catch(() => {});

    const cleaned = rawText.replace(_RE_ASSESS_CODE_FENCE, '').trim();
    let parsed;
    try {
      parsed = JSON.parse(cleaned);
    } catch (_) {
      parsed = { score: 0, matchedSkills: [], unmatchedSkills };
    }

    broadcastSSE('skill_assessment_result', { candidateId, ...parsed });
  }

  // ── Register all handlers on the queue ────────────────────────────────────
  createWorker(QUEUE_NAME, async (job) => {
    const { type, ...data } = job.data;
    switch (type) {
      case 'calc-unmatched':
        return handleCalcUnmatched(data);
      case 'assess-unmatched':
        return handleAssessUnmatched(data);
      default:
        throw new Error(`[llmWorker] Unknown job type: ${type}`);
    }
  });

  console.log('[llmWorker] Registered handlers on queue:', QUEUE_NAME);
}

module.exports = { initLlmWorker, getLlmQueue };
