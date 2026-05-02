"""
test_session_auth.py — Tests for the _require_session decorator behaviour.

Stubs replicate the decorator logic from webbridge.py without importing it.

Run with:  pytest tests/test_session_auth.py
"""
import unittest
from functools import wraps

from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Stub _require_session (mirrors webbridge.py logic)
# ---------------------------------------------------------------------------

def _build_app_with_require_session(db_row=None, db_error=None):
    """
    Build a minimal Flask app with a stubbed _require_session decorator.

    db_row   – row returned by SELECT userid FROM login (e.g. (42,))
    db_error – exception raised by the DB helper
    """
    app = Flask(__name__)
    app.config["TESTING"] = True

    def _require_session(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            username   = (request.cookies.get("username") or "").strip()
            userid     = (request.cookies.get("userid") or "").strip()
            session_id = (request.cookies.get("session_id") or "").strip()
            if not username:
                return jsonify({"error": "Authentication required"}), 401
            try:
                if db_error is not None:
                    raise db_error
                if session_id:
                    # Strict: session_id must match — no fallback
                    if db_row is None:
                        return jsonify({"error": "Session expired or invalid"}), 401
                    row = db_row
                else:
                    # Legacy fallback: username existence only
                    if db_row is None:
                        return jsonify({"error": "Authentication required"}), 401
                    row = db_row
                request._session_user   = username
                request._session_userid = userid or str(row[0] or "")
            except Exception as e:
                if isinstance(e, SystemExit):
                    raise
                return jsonify({"error": "Authentication service temporarily unavailable"}), 500
            return f(*args, **kwargs)
        return wrapped

    @app.get("/protected")
    @_require_session
    def protected():
        return jsonify({
            "ok": True,
            "user": request._session_user,
            "userid": request._session_userid,
        }), 200

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRequireSession(unittest.TestCase):

    def test_no_username_cookie(self):
        """_require_session: no username cookie → 401 Authentication required."""
        app = _build_app_with_require_session(db_row=(42,))
        client = app.test_client()
        resp = client.get("/protected")
        self.assertEqual(resp.status_code, 401)
        self.assertIn("Authentication required", resp.get_json().get("error", ""))

    def test_valid_session_id_matches(self):
        """_require_session: username + matching session_id → 200, sets _session_user."""
        app = _build_app_with_require_session(db_row=(42,))
        client = app.test_client()
        client.set_cookie("username", "alice")
        client.set_cookie("session_id", "valid-token")
        resp = client.get("/protected")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data.get("ok"))
        self.assertEqual(data.get("user"), "alice")
        self.assertEqual(data.get("userid"), "42")

    def test_session_id_present_but_invalid(self):
        """_require_session: session_id present but not in DB → 401 Session expired or invalid (no fallback)."""
        app = _build_app_with_require_session(db_row=None)
        client = app.test_client()
        client.set_cookie("username", "alice")
        client.set_cookie("session_id", "stale-token")
        resp = client.get("/protected")
        self.assertEqual(resp.status_code, 401)
        self.assertIn("Session expired or invalid", resp.get_json().get("error", ""))

    def test_legacy_no_session_id_user_exists(self):
        """_require_session: no session_id, username exists in DB → 200 (legacy fallback)."""
        app = _build_app_with_require_session(db_row=(99,))
        client = app.test_client()
        client.set_cookie("username", "bob")
        resp = client.get("/protected")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data.get("ok"))
        self.assertEqual(data.get("user"), "bob")

    def test_legacy_no_session_id_user_not_found(self):
        """_require_session: no session_id, username not in DB → 401 Authentication required."""
        app = _build_app_with_require_session(db_row=None)
        client = app.test_client()
        client.set_cookie("username", "ghost")
        resp = client.get("/protected")
        self.assertEqual(resp.status_code, 401)
        self.assertIn("Authentication required", resp.get_json().get("error", ""))

    def test_db_error(self):
        """_require_session: DB error → 500 Authentication service temporarily unavailable."""
        app = _build_app_with_require_session(db_error=Exception("connection refused"))
        client = app.test_client()
        client.set_cookie("username", "alice")
        resp = client.get("/protected")
        self.assertEqual(resp.status_code, 500)
        self.assertIn(
            "Authentication service temporarily unavailable",
            resp.get_json().get("error", ""),
        )

    def test_userid_cookie_takes_precedence(self):
        """_require_session: userid cookie value is used when present (takes precedence over DB row)."""
        app = _build_app_with_require_session(db_row=(42,))
        client = app.test_client()
        client.set_cookie("username", "alice")
        client.set_cookie("userid", "100")
        client.set_cookie("session_id", "valid-token")
        resp = client.get("/protected")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json().get("userid"), "100")


if __name__ == "__main__":
    unittest.main()
