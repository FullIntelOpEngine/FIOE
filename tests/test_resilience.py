"""
test_resilience.py — Tests for resilience: DB drop, Gemini unreachable, disk failure.

Stubs demonstrate error handling behaviour without importing webbridge.py.

Run with:  pytest tests/test_resilience.py
"""
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Test app that demonstrates resilience patterns
# ---------------------------------------------------------------------------

def _build_resilience_app():
    app = Flask(__name__)
    app.config["TESTING"] = True

    @app.get("/db-query")
    def db_query():
        """Endpoint that hits the DB; returns 500 on connection drop."""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host="unreachable-host", port=5432,
                user="user", password="pass", dbname="db",
                connect_timeout=1,
            )
            conn.close()
            return jsonify({"ok": True}), 200
        except Exception as e:
            return jsonify({"error": f"Database unavailable: {e}"}), 500

    @app.get("/gemini-check")
    def gemini_check():
        """Endpoint that calls Gemini; returns 503 when unreachable."""
        try:
            import requests
            r = requests.get("http://unreachable-gemini:9999/health", timeout=1)
            return jsonify({"ok": True}), 200
        except Exception as e:
            return jsonify({"error": f"Gemini/Node service unavailable: {e}"}), 503

    @app.post("/atomic-write")
    def atomic_write():
        """Writes a file atomically; original intact if write fails."""
        body = request.get_json(force=True, silent=True) or {}
        path = body.get("path", "")
        content = body.get("content", "")
        if not path:
            return jsonify({"error": "path required"}), 400
        tmp = path + ".tmp"
        try:
            with open(tmp, "w") as fh:
                fh.write(content)
            os.replace(tmp, path)
            return jsonify({"ok": True}), 200
        except Exception as e:
            # Original file is untouched; clean up tmp if it exists
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass
            return jsonify({"error": f"Write failed: {e}"}), 500

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestResilience(unittest.TestCase):

    def setUp(self):
        self.app = _build_resilience_app()
        self.client = self.app.test_client()

    def test_db_conn_drop(self):
        """DB connection drop → 500 with helpful message."""
        try:
            import psycopg2  # noqa: F401
        except ImportError:
            self.skipTest("psycopg2 not installed")
        resp = self.client.get("/db-query")
        self.assertEqual(resp.status_code, 500)
        data = resp.get_json()
        self.assertIn("error", data)
        self.assertIn("Database", data["error"])

    def test_gemini_unreachable(self):
        """Gemini/Node unreachable → 503."""
        try:
            import requests  # noqa: F401
        except ImportError:
            self.skipTest("requests not installed")
        resp = self.client.get("/gemini-check")
        self.assertEqual(resp.status_code, 503)
        data = resp.get_json()
        self.assertIn("error", data)
        self.assertIn("unavailable", data["error"].lower())

    def test_disk_full_atomic(self):
        """Failing file write leaves original intact and returns 500."""
        with tempfile.TemporaryDirectory() as td:
            original_path = os.path.join(td, "data.json")
            original_content = json.dumps({"original": True})
            with open(original_path, "w") as fh:
                fh.write(original_content)

            # Patch open to raise on tmp file write
            real_open = open

            def _fake_open(path, *args, **kwargs):
                if path.endswith(".tmp"):
                    raise OSError("No space left on device")
                return real_open(path, *args, **kwargs)

            with patch("builtins.open", side_effect=_fake_open):
                resp = self.client.post(
                    "/atomic-write",
                    json={"path": original_path, "content": '{"new": true}'},
                )
            self.assertEqual(resp.status_code, 500)

            # Original file must be untouched
            with open(original_path) as fh:
                data = json.load(fh)
            self.assertTrue(data.get("original"))

    def test_atomic_write_success(self):
        """Successful atomic write produces correct file."""
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "output.txt")
            resp = self.client.post(
                "/atomic-write",
                json={"path": path, "content": "hello world"},
            )
            self.assertEqual(resp.status_code, 200)
            with open(path) as fh:
                self.assertEqual(fh.read(), "hello world")
            # No .tmp file should remain
            self.assertFalse(os.path.exists(path + ".tmp"))

    def test_atomic_write_no_path(self):
        """POST /atomic-write without path → 400."""
        resp = self.client.post("/atomic-write", json={"content": "data"})
        self.assertEqual(resp.status_code, 400)


if __name__ == "__main__":
    unittest.main()
