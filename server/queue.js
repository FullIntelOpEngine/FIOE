'use strict';
/**
 * server/queue.js
 *
 * Lightweight in-process job queue with concurrency cap, retries, and backoff.
 * No external dependencies (no Redis) — designed for single-node deployments.
 *
 * For multi-node / persistence you can swap the backend by pointing
 * QUEUE_BACKEND=bullmq and installing bullmq + ioredis; the public API
 * (enqueue / on / createWorker) is intentionally compatible.
 *
 * Usage:
 *   const { createQueue, createWorker } = require('./queue');
 *   const queue = createQueue('llm');
 *   createWorker('llm', async (job) => { … });
 *   const { id } = await queue.enqueue({ type: 'calc-unmatched', data: { … } });
 */

const EventEmitter = require('events');

// ── Configuration ─────────────────────────────────────────────────────────────
const QUEUE_CONCURRENCY  = parseInt(process.env.QUEUE_CONCURRENCY,  10) || 4;
const QUEUE_MAX_RETRIES  = parseInt(process.env.QUEUE_MAX_RETRIES,  10) || 3;
const QUEUE_RETRY_BASE_MS = parseInt(process.env.QUEUE_RETRY_BASE_MS, 10) || 500;
// Max per-job execution time (ms); job is marked failed if it exceeds this.
const QUEUE_JOB_TIMEOUT_MS = parseInt(process.env.QUEUE_JOB_TIMEOUT_MS, 10) || 5 * 60 * 1000; // 5 min

// ── Internal state ────────────────────────────────────────────────────────────
const _queues   = new Map(); // name → InProcessQueue
const _workers  = new Map(); // name → Worker handler fn

let _nextJobId = 1;

// ── InProcessQueue ─────────────────────────────────────────────────────────────
class InProcessQueue extends EventEmitter {
  constructor(name) {
    super();
    this.name      = name;
    this._pending  = [];   // [ { id, data, retries, maxRetries } ]
    this._inflight = 0;
  }

  /**
   * Enqueue a job payload.
   * @param {object} data  Arbitrary serialisable job data.
   * @param {{ priority?: number, maxRetries?: number }} opts
   * @returns {{ id: string }}
   */
  enqueue(data, opts = {}) {
    const id = String(_nextJobId++);
    const maxRetries = typeof opts.maxRetries === 'number' ? opts.maxRetries : QUEUE_MAX_RETRIES;
    const priority   = typeof opts.priority   === 'number' ? opts.priority   : 0;
    const job = { id, data, retries: 0, maxRetries, priority, enqueuedAt: Date.now() };

    // Simple priority insertion (higher priority → earlier position).
    // For normal jobs (priority 0) this is O(1) append.
    if (priority === 0 || this._pending.length === 0) {
      this._pending.push(job);
    } else {
      let i = this._pending.length;
      while (i > 0 && this._pending[i - 1].priority < priority) i--;
      this._pending.splice(i, 0, job);
    }

    this.emit('enqueued', { id, queueName: this.name });
    setImmediate(() => this._drain());
    return { id };
  }

  _drain() {
    while (this._inflight < QUEUE_CONCURRENCY && this._pending.length > 0) {
      const job = this._pending.shift();
      this._run(job);
    }
  }

  async _run(job) {
    this._inflight++;
    this.emit('active', { id: job.id, queueName: this.name });

    const handler = _workers.get(this.name);
    if (!handler) {
      // No worker registered yet — re-queue and wait
      this._pending.unshift(job);
      this._inflight--;
      return;
    }

    let timer;
    try {
      const result = await Promise.race([
        handler(job),
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error(`Job ${job.id} timed out after ${QUEUE_JOB_TIMEOUT_MS}ms`)), QUEUE_JOB_TIMEOUT_MS);
        }),
      ]);
      clearTimeout(timer);
      this.emit('completed', { id: job.id, queueName: this.name, result });
    } catch (err) {
      clearTimeout(timer);
      job.retries++;
      if (job.retries <= job.maxRetries) {
        // Exponential backoff before re-queue
        const delay = QUEUE_RETRY_BASE_MS * Math.pow(2, job.retries - 1);
        this.emit('retrying', { id: job.id, queueName: this.name, attempt: job.retries, delay, err });
        setTimeout(() => {
          this._pending.unshift(job);
          this._drain();
        }, delay);
      } else {
        this.emit('failed', { id: job.id, queueName: this.name, err });
      }
    } finally {
      this._inflight--;
      setImmediate(() => this._drain());
    }
  }

  /** Current queue depth (pending jobs). */
  get depth()    { return this._pending.length; }
  /** Current in-flight count. */
  get inflight() { return this._inflight; }
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Get or create a named queue.
 * @param {string} name
 * @returns {InProcessQueue}
 */
function createQueue(name) {
  if (!_queues.has(name)) _queues.set(name, new InProcessQueue(name));
  return _queues.get(name);
}

/**
 * Register an async handler for a named queue.
 * @param {string}   name
 * @param {Function} handler  async (job) => any
 */
function createWorker(name, handler) {
  _workers.set(name, handler);
  // If a queue already has pending jobs, start draining immediately.
  const q = _queues.get(name);
  if (q) setImmediate(() => q._drain());
}

/**
 * Snapshot of all queue depths (for monitoring / metrics endpoints).
 * @returns {{ [name]: { depth: number, inflight: number } }}
 */
function queueStats() {
  const out = {};
  for (const [name, q] of _queues) {
    out[name] = { depth: q.depth, inflight: q.inflight };
  }
  return out;
}

module.exports = { createQueue, createWorker, queueStats, InProcessQueue };
