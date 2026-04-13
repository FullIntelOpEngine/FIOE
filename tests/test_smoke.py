"""
test_smoke.py — Smoke tests for startup-time environment checks.

Tests startup behaviour WITHOUT importing webbridge.py directly
(which would start Flask and require all production deps).

Run with:  pytest tests/test_smoke.py   (from Candidate Analyser/backend/)
"""
import logging
import os
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock

# ---------------------------------------------------------------------------
# Inline stubs that replicate startup logic from webbridge.py
# ---------------------------------------------------------------------------

def _startup_check_secret(env: dict, is_production: bool = True):
    """
    Replicate the FLASK_SECRET_KEY check from webbridge.py startup.
    Raises SystemExit(1) when production=True and key is missing/placeholder.
    """
    import secrets as _secrets
    flask_secret = env.get("FLASK_SECRET_KEY", "")
    if not flask_secret or flask_secret == "change-me-in-production-webbridge":
        if is_production:
            raise SystemExit(1)
        logging.warning(
            "FLASK_SECRET_KEY is not set. A random key has been generated for "
            "this session — sessions will not persist across restarts."
        )


def _startup_check_gemini(env: dict):
    """
    Replicate the GEMINI_API_KEY absence warning from webbridge.py.
    Returns a log message string (for test assertions).
    """
    if not env.get("GEMINI_API_KEY"):
        msg = "GEMINI_API_KEY not set; Gemini features will be unavailable."
        logging.warning(msg)
        return msg
    return None


def _ensure_required_dirs(env: dict):
    """
    Replicate the directory-creation guard from webbridge.py startup.
    Returns the list of paths that would be created.
    """
    output_dir = env.get("OUTPUT_DIR", os.path.join(tempfile.gettempdir(), "fioe_output"))
    search_xls_dir = env.get("SEARCH_XLS_DIR", os.path.join(tempfile.gettempdir(), "fioe_xls"))
    report_tmpl_dir = env.get("REPORT_TEMPLATES_DIR", os.path.join(tempfile.gettempdir(), "fioe_tmpl"))
    created = []
    for d in (output_dir, search_xls_dir, report_tmpl_dir):
        os.makedirs(d, exist_ok=True)
        created.append(d)
    return created


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestStartupSmoke(unittest.TestCase):

    def test_startup_no_gemini(self):
        """GEMINI_API_KEY absence should log a warning, not crash."""
        env = {"FLASK_SECRET_KEY": "test-secret-key-not-real"}
        with self.assertLogs(level="WARNING") as cm:
            msg = _startup_check_gemini(env)
        self.assertIsNotNone(msg)
        self.assertTrue(any("GEMINI_API_KEY" in line for line in cm.output))

    def test_startup_no_secret(self):
        """Missing FLASK_SECRET_KEY in production must raise SystemExit."""
        env = {}
        with self.assertRaises(SystemExit) as ctx:
            _startup_check_secret(env, is_production=True)
        self.assertEqual(ctx.exception.code, 1)

    def test_startup_placeholder_secret_production(self):
        """Placeholder FLASK_SECRET_KEY in production must also raise SystemExit."""
        env = {"FLASK_SECRET_KEY": "change-me-in-production-webbridge"}
        with self.assertRaises(SystemExit):
            _startup_check_secret(env, is_production=True)

    def test_startup_no_secret_dev(self):
        """Missing FLASK_SECRET_KEY in dev should only warn (not crash)."""
        env = {}
        with self.assertLogs(level="WARNING") as cm:
            _startup_check_secret(env, is_production=False)
        self.assertTrue(any("FLASK_SECRET_KEY" in line for line in cm.output))

    def test_required_dirs(self):
        """OUTPUT_DIR, SEARCH_XLS_DIR, REPORT_TEMPLATES_DIR created if missing."""
        with tempfile.TemporaryDirectory() as td:
            env = {
                "OUTPUT_DIR": os.path.join(td, "output"),
                "SEARCH_XLS_DIR": os.path.join(td, "xls"),
                "REPORT_TEMPLATES_DIR": os.path.join(td, "tmpl"),
            }
            created = _ensure_required_dirs(env)
            for path in created:
                self.assertTrue(os.path.isdir(path), f"Expected directory to exist: {path}")

    def test_required_dirs_already_exist(self):
        """_ensure_required_dirs is idempotent — no error if dirs already exist."""
        with tempfile.TemporaryDirectory() as td:
            env = {
                "OUTPUT_DIR": td,
                "SEARCH_XLS_DIR": td,
                "REPORT_TEMPLATES_DIR": td,
            }
            # Should not raise
            _ensure_required_dirs(env)
            _ensure_required_dirs(env)


if __name__ == "__main__":
    unittest.main()
