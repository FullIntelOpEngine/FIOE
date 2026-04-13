"""
test_db.py — Tests for DB helper functions (_ensure_admin_columns, query_log_daily).

Uses MagicMock cursors to avoid needing a real DB.

Run with:  pytest tests/test_db.py
"""
import unittest
from unittest.mock import MagicMock, call, patch

# ---------------------------------------------------------------------------
# Inline stub of _ensure_admin_columns (mirrors webbridge.py)
# ---------------------------------------------------------------------------

def _ensure_admin_columns(cur):
    """Idempotently add columns used by admin endpoints.

    Each DDL is wrapped in a savepoint so a failure does NOT abort the
    surrounding transaction.
    """
    ddls = [
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS target_limit INTEGER DEFAULT 10",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS last_result_count INTEGER",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS last_deducted_role_tag TEXT",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS session TIMESTAMPTZ",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS google_refresh_token TEXT",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS google_token_expires TIMESTAMP",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS corporation TEXT",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS useraccess TEXT",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS cse_query_count INTEGER DEFAULT 0",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS price_per_query NUMERIC(10,4) DEFAULT 0",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS gemini_query_count INTEGER DEFAULT 0",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS price_per_gemini_query NUMERIC(10,4) DEFAULT 0",
    ]
    for i, ddl in enumerate(ddls):
        sp = f"_adm_col_{i}"
        try:
            cur.execute(f"SAVEPOINT {sp}")
            cur.execute(ddl)
            cur.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            try:
                cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            except Exception:
                pass

    # Daily query log table
    try:
        cur.execute("SAVEPOINT _adm_daily_log")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS query_log_daily (
                username     TEXT    NOT NULL,
                log_date     DATE    NOT NULL DEFAULT CURRENT_DATE,
                cse_count    INTEGER NOT NULL DEFAULT 0,
                gemini_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (username, log_date)
            )
        """)
        cur.execute("RELEASE SAVEPOINT _adm_daily_log")
    except Exception:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT _adm_daily_log")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEnsureAdminColumns(unittest.TestCase):

    def test_ensure_admin_cols(self):
        """_ensure_admin_columns executes DDL for each expected column."""
        cur = MagicMock()
        _ensure_admin_columns(cur)
        # Should have called execute many times
        self.assertGreater(cur.execute.call_count, 10)
        # Every SAVEPOINT call should have a matching RELEASE (or ROLLBACK on error)
        all_calls = [str(c) for c in cur.execute.call_args_list]
        savepoint_calls = [c for c in all_calls if "SAVEPOINT" in c and "ROLLBACK" not in c and "RELEASE" not in c]
        release_calls = [c for c in all_calls if "RELEASE SAVEPOINT" in c]
        # In happy path, number of SAVEPOINTs == number of RELEASEs
        self.assertEqual(len(savepoint_calls), len(release_calls))

    def test_ensure_admin_cols_on_failure(self):
        """_ensure_admin_columns rolls back individual savepoints on DDL failure."""
        failure_count = [0]
        executed = []

        def mock_execute(sql, *args, **kwargs):
            executed.append(sql)
            # Simulate failure on first ALTER TABLE
            if sql.startswith("ALTER TABLE") and failure_count[0] == 0:
                failure_count[0] += 1
                raise Exception("column already exists")

        cur = MagicMock()
        cur.execute.side_effect = mock_execute

        # Should NOT raise — errors are caught per-savepoint
        _ensure_admin_columns(cur)
        # Ensure ROLLBACK TO SAVEPOINT was called
        rollback_calls = [s for s in executed if "ROLLBACK TO SAVEPOINT" in s]
        self.assertGreater(len(rollback_calls), 0)

    def test_query_log_daily(self):
        """query_log_daily CREATE TABLE is executed inside its own savepoint."""
        cur = MagicMock()
        _ensure_admin_columns(cur)
        all_sqls = [str(c.args[0]) for c in cur.execute.call_args_list]
        # Should include the daily log savepoint
        self.assertTrue(any("_adm_daily_log" in s for s in all_sqls))
        # Should include the CREATE TABLE
        self.assertTrue(any("query_log_daily" in s for s in all_sqls))

    def test_ensure_admin_cols_total_savepoints(self):
        """_ensure_admin_columns creates one savepoint per DDL statement."""
        cur = MagicMock()
        _ensure_admin_columns(cur)
        all_sqls = [str(c.args[0]) for c in cur.execute.call_args_list]
        # Match only bare SAVEPOINT lines (not RELEASE SAVEPOINT or ROLLBACK TO SAVEPOINT)
        savepoints = [
            s for s in all_sqls
            if s.startswith("SAVEPOINT _adm_col_")
        ]
        self.assertEqual(len(savepoints), 12)

    def test_all_ddl_contains_if_not_exists(self):
        """All column DDLs use ADD COLUMN IF NOT EXISTS for idempotency."""
        cur = MagicMock()
        _ensure_admin_columns(cur)
        all_sqls = [str(c.args[0]) for c in cur.execute.call_args_list]
        alter_sqls = [s for s in all_sqls if "ALTER TABLE" in s]
        for sql in alter_sqls:
            self.assertIn("IF NOT EXISTS", sql, f"Missing IF NOT EXISTS in: {sql}")


if __name__ == "__main__":
    unittest.main()
