"""
test_observability.py — Tests for logging / observability endpoints.

Stubs replicate admin_get_logs and admin_client_error from webbridge.py.

Run with:  pytest tests/test_observability.py
"""
import json
import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Inline log store stub
# ---------------------------------------------------------------------------

class _LogStore:
    """In-memory log store that mimics app_logger behaviour."""

    def __init__(self):
        self._entries: list = []

    def log_error(self, event: str, **kwargs):
        self._entries.append({"event": event, "ts": datetime.utcnow().isoformat(), **kwargs})

    def read_all_logs(self, since: str = None, until: str = None) -> dict:
        return {
            "errors": [e for e in self._entries if "error" in e.get("event", "").lower()],
            "all": self._entries,
        }


def _build_observability_app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    store = _LogStore()
    app._log_store = store

    @app.get("/admin/logs")
    def admin_get_logs():
        since = request.args.get("since")
        until = request.args.get("until")
        logs = store.read_all_logs(since=since, until=until)
        return jsonify({
            "ok": True,
            "errors": logs.get("errors", []),
            "total": len(logs.get("all", [])),
        }), 200

    @app.post("/admin/client-error")
    def admin_client_error():
        body = request.get_json(force=True, silent=True) or {}
        message = body.get("message", "")
        stack = body.get("stack", "")
        url = body.get("url", "")
        if not message:
            return jsonify({"error": "message required"}), 400
        store.log_error(
            "client_error",
            message=message,
            stack=stack,
            url=url,
            user_agent=request.headers.get("User-Agent", ""),
        )
        return jsonify({"ok": True}), 200

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestObservability(unittest.TestCase):

    def setUp(self):
        self.app = _build_observability_app()
        self.client = self.app.test_client()

    def test_admin_get_logs(self):
        """GET /admin/logs returns JSON with expected keys."""
        resp = self.client.get("/admin/logs")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data.get("ok"))
        self.assertIn("errors", data)
        self.assertIn("total", data)

    def test_client_error_log(self):
        """POST /admin/client-error writes to log store and returns ok:true."""
        resp = self.client.post(
            "/admin/client-error",
            json={
                "message": "TypeError: Cannot read property 'x' of undefined",
                "stack": "TypeError at app.js:123",
                "url": "http://localhost:3000/dashboard",
            },
        )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json().get("ok"))

        # Verify it was stored
        logs_resp = self.client.get("/admin/logs")
        data = logs_resp.get_json()
        self.assertGreater(data.get("total", 0), 0)

    def test_client_error_missing_message(self):
        """POST /admin/client-error without message → 400."""
        resp = self.client.post("/admin/client-error", json={"stack": "some stack"})
        self.assertEqual(resp.status_code, 400)

    def test_admin_logs_returns_list(self):
        """GET /admin/logs errors field is always a list."""
        resp = self.client.get("/admin/logs")
        data = resp.get_json()
        self.assertIsInstance(data.get("errors"), list)

    def test_admin_logs_after_multiple_errors(self):
        """Multiple client errors accumulate in the log."""
        for i in range(3):
            self.client.post(
                "/admin/client-error",
                json={"message": f"Error #{i}", "stack": "", "url": ""},
            )
        logs_resp = self.client.get("/admin/logs")
        data = logs_resp.get_json()
        self.assertGreaterEqual(data.get("total", 0), 3)


if __name__ == "__main__":
    unittest.main()
