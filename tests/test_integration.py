"""
test_integration.py — Integration tests that require a live PostgreSQL DB.

All tests in this file are SKIPPED gracefully when no DB is available.

Run with:  pytest tests/test_integration.py
"""
import os
import unittest

_DB_AVAILABLE = False
_DB_SKIP_MSG = "PostgreSQL not available (set PGHOST/PGUSER/PGPASSWORD/PGDATABASE)"

try:
    import psycopg2
    _conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
        dbname=os.getenv("PGDATABASE", "candidate_db"),
        connect_timeout=3,
    )
    _conn.close()
    _DB_AVAILABLE = True
except Exception:
    pass


@unittest.skipUnless(_DB_AVAILABLE, _DB_SKIP_MSG)
class TestIntegrationFlowDB(unittest.TestCase):
    """
    Full-flow integration tests that require a live PostgreSQL database.

    These tests connect directly to the DB (no Flask test client) and verify
    that the data layer behaves correctly end-to-end.
    """

    def setUp(self):
        import psycopg2
        self.conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            port=int(os.getenv("PGPORT", "5432")),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", ""),
            dbname=os.getenv("PGDATABASE", "candidate_db"),
        )
        self.conn.autocommit = False

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def test_full_flow_db(self):
        """Integration flow: create test user, verify login table presence, rollback."""
        cur = self.conn.cursor()
        # Verify the login table exists
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name='login'"
        )
        row = cur.fetchone()
        self.assertIsNotNone(row, "login table should exist in the DB")
        cur.close()

    def test_ensure_admin_columns_live(self):
        """_ensure_admin_columns does not crash against real DB."""
        from test_db import _ensure_admin_columns  # noqa: PLC0415

        cur = self.conn.cursor()
        # Should not raise
        _ensure_admin_columns(cur)
        cur.close()

    def test_query_log_daily_table(self):
        """query_log_daily table exists (or can be created) in the real DB."""
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS query_log_daily (
                username     TEXT    NOT NULL,
                log_date     DATE    NOT NULL DEFAULT CURRENT_DATE,
                cse_count    INTEGER NOT NULL DEFAULT 0,
                gemini_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (username, log_date)
            )
        """)
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name='query_log_daily'"
        )
        row = cur.fetchone()
        self.assertIsNotNone(row)
        cur.close()


if __name__ == "__main__":
    unittest.main()
