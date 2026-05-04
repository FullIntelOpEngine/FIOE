#!/usr/bin/env node
/**
 * scripts/log_rotate.js
 *
 * Rotates (deletes) log files in AUTOSOURCING_LOG_DIR that are older than
 * LOG_RETAIN_DAYS (default: 30).
 *
 * Usage (cron, daily 02:00):
 *   0 2 * * * /usr/bin/node /path/to/scripts/log_rotate.js >> /var/log/fioe_rotate.log 2>&1
 *
 * Environment variables:
 *   AUTOSOURCING_LOG_DIR  — path containing *.txt log files (required in production)
 *   LOG_RETAIN_DAYS       — number of days of logs to retain (default: 30)
 *   LOG_ROTATE_DRY_RUN    — set to "1" to print what would be deleted without deleting
 */

'use strict';

const fs   = require('fs');
const path = require('path');

const LOG_DIR     = process.env.AUTOSOURCING_LOG_DIR || String.raw`F:\Recruiting Tools\Autosourcing\log`;
const RETAIN_DAYS = parseInt(process.env.LOG_RETAIN_DAYS || '30', 10);
const DRY_RUN     = process.env.LOG_ROTATE_DRY_RUN === '1';
const NOW_MS      = Date.now();
const CUTOFF_MS   = RETAIN_DAYS * 24 * 60 * 60 * 1000;

function iso() {
  return new Date().toISOString();
}

function main() {
  if (!fs.existsSync(LOG_DIR)) {
    console.log(`[${iso()}] log_rotate: LOG_DIR does not exist (${LOG_DIR}), nothing to do.`);
    process.exit(0);
  }

  let entries;
  try {
    entries = fs.readdirSync(LOG_DIR);
  } catch (err) {
    console.error(`[${iso()}] log_rotate: cannot read LOG_DIR: ${err.message}`);
    process.exit(1);
  }

  // Only rotate files that match our naming convention: <prefix>_YYYY-MM-DD.txt
  const LOG_NAME_RE = /^.+_\d{4}-\d{2}-\d{2}\.txt$/;

  let deleted = 0;
  let skipped = 0;
  let errors  = 0;

  for (const fname of entries) {
    if (!LOG_NAME_RE.test(fname)) continue;

    const fpath = path.join(LOG_DIR, fname);
    let stat;
    try {
      stat = fs.statSync(fpath);
    } catch (_) {
      continue; // race condition / symlink — skip
    }

    if (!stat.isFile()) continue;

    const ageMs = NOW_MS - stat.mtimeMs;
    if (ageMs < CUTOFF_MS) {
      skipped++;
      continue;
    }

    if (DRY_RUN) {
      const ageDays = (ageMs / (24 * 60 * 60 * 1000)).toFixed(1);
      console.log(`[${iso()}] log_rotate: [DRY-RUN] would delete ${fname} (${ageDays}d old)`);
      deleted++;
      continue;
    }

    try {
      fs.unlinkSync(fpath);
      const ageDays = (ageMs / (24 * 60 * 60 * 1000)).toFixed(1);
      console.log(`[${iso()}] log_rotate: deleted ${fname} (${ageDays}d old)`);
      deleted++;
    } catch (err) {
      console.error(`[${iso()}] log_rotate: failed to delete ${fname}: ${err.message}`);
      errors++;
    }
  }

  console.log(`[${iso()}] log_rotate: done — deleted=${deleted} skipped=${skipped} errors=${errors} retain=${RETAIN_DAYS}d dir=${LOG_DIR}`);
  process.exit(errors > 0 ? 1 : 0);
}

main();
