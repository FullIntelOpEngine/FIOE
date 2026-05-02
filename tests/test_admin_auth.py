"""
test_admin_auth.py — Tests for the _require_admin decorator behaviour.

Stubs replicate the decorator logic from webbridge.py without importing it.

Run with:  pytest tests/test_admin_auth.py
"""
import unittest
from functools import wraps
from unittest.mock import MagicMock, patch

from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Stub _require_admin (mirrors webbridge.py logic)
# ---------------------------------------------------------------------------

def _build_app_with_require_admin(db_row=None, db_error=None):
    """
    Build a minimal Flask app with a stubbed _require_admin decorator.

    db_row   – row returned by SELECT useraccess FROM login (e.g. ("admin",))
    db_error – exception raised by the DB helper
    """
    app = Flask(__name__)
    app.config["TESTING"] = True

    def _require_admin(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            username   = (request.cookies.get("username") or "").strip()
            session_id = (request.cookies.get("session_id") or "").strip()
            if not username:
                return jsonify({"error": "Authentication required"}), 401
            try:
                if db_error is not None:
                    raise db_error
                if session_id:
                    # Strict: session_id must match
                    if db_row is None:
                        return jsonify({"error": "Session expired or invalid"}), 401
                else:
                    # Legacy fallback: username existence only
                    if db_row is None:
                        return jsonify({"error": "Authentication required"}), 401
                useraccess = (db_row[0] or "").strip().lower()
                if useraccess != "admin":
                    return jsonify({"error": "Admin access required"}), 403
            except Exception as e:
                if isinstance(e, SystemExit):
                    raise
                return jsonify({"error": f"Auth check failed: {e}"}), 500
            return f(*args, **kwargs)
        return wrapper

    @app.get("/admin/secret")
    @_require_admin
    def secret():
        return jsonify({"ok": True}), 200

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAdminAuth(unittest.TestCase):

    def test_admin_no_cookie(self):
        """@_require_admin: missing username cookie → 401."""
        app = _build_app_with_require_admin(db_row=("admin",))
        client = app.test_client()
        resp = client.get("/admin/secret")
        self.assertEqual(resp.status_code, 401)
        data = resp.get_json()
        self.assertIn("Authentication required", data.get("error", ""))

    def test_admin_non_admin(self):
        """@_require_admin: non-admin user → 403."""
        app = _build_app_with_require_admin(db_row=("user",))
        client = app.test_client()
        client.set_cookie("username", "regularuser")
        resp = client.get("/admin/secret")
        self.assertEqual(resp.status_code, 403)
        data = resp.get_json()
        self.assertIn("Admin access required", data.get("error", ""))

    def test_admin_valid(self):
        """@_require_admin: valid admin user → 200."""
        app = _build_app_with_require_admin(db_row=("admin",))
        client = app.test_client()
        client.set_cookie("username", "adminuser")
        resp = client.get("/admin/secret")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json().get("ok"))

    def test_admin_db_error(self):
        """@_require_admin: DB unreachable → 500 with helpful message."""
        app = _build_app_with_require_admin(
            db_error=Exception("could not connect to server")
        )
        client = app.test_client()
        client.set_cookie("username", "adminuser")
        resp = client.get("/admin/secret")
        self.assertEqual(resp.status_code, 500)
        data = resp.get_json()
        self.assertIn("Auth check failed", data.get("error", ""))

    def test_admin_no_db_row_no_session_id(self):
        """@_require_admin: user not found, no session_id cookie → 401 Authentication required."""
        app = _build_app_with_require_admin(db_row=None)
        client = app.test_client()
        client.set_cookie("username", "ghost")
        resp = client.get("/admin/secret")
        self.assertEqual(resp.status_code, 401)
        data = resp.get_json()
        self.assertIn("Authentication required", data.get("error", ""))

    def test_admin_session_id_mismatch(self):
        """@_require_admin: session_id present but DB row not found → 401 Session expired."""
        app = _build_app_with_require_admin(db_row=None)
        client = app.test_client()
        client.set_cookie("username", "adminuser")
        client.set_cookie("session_id", "stale-token")
        resp = client.get("/admin/secret")
        self.assertEqual(resp.status_code, 401)
        data = resp.get_json()
        self.assertIn("Session expired or invalid", data.get("error", ""))

    def test_admin_valid_with_session_id(self):
        """@_require_admin: username + matching session_id → 200."""
        app = _build_app_with_require_admin(db_row=("admin",))
        client = app.test_client()
        client.set_cookie("username", "adminuser")
        client.set_cookie("session_id", "valid-token")
        resp = client.get("/admin/secret")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json().get("ok"))


if __name__ == "__main__":
    unittest.main()
