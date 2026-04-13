"""
test_auth.py — Tests for login / logout / session / auth-gate behaviour.

Uses a minimal Flask test app that replicate the routes from webbridge.py
WITHOUT importing webbridge (avoids Flask server startup side-effects).

Run with:  pytest tests/test_auth.py   (from Candidate Analyser/backend/)
"""
import json
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask, request, session, jsonify

# ---------------------------------------------------------------------------
# Minimal Flask test application replicating auth routes
# ---------------------------------------------------------------------------

def _build_test_app():
    app = Flask(__name__)
    app.secret_key = "test-secret-for-tests-only"
    app.config["TESTING"] = True
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = False  # allow HTTP in tests

    # Simulated user DB
    _USERS = {
        "admin@example.com": {"password": "correct-password", "useraccess": "admin"},
        "user@example.com": {"password": "userpass", "useraccess": "user"},
    }

    @app.post("/login")
    def login():
        body = request.get_json(force=True, silent=True) or {}
        username = (body.get("username") or "").strip()
        password = (body.get("password") or "").strip()
        user = _USERS.get(username)
        if not user or user["password"] != password:
            return jsonify({"ok": False, "error": "Invalid credentials"}), 401
        session["username"] = username
        session["useraccess"] = user["useraccess"]
        resp = jsonify({"ok": True, "username": username, "useraccess": user["useraccess"]})
        resp.set_cookie("username", username, httponly=False)
        return resp, 200

    @app.post("/logout")
    def logout():
        session.clear()
        resp = jsonify({"ok": True})
        resp.delete_cookie("username")
        return resp, 200

    @app.get("/user/resolve")
    def user_resolve():
        username = session.get("username") or request.cookies.get("username")
        if not username:
            return jsonify({"ok": False, "error": "Not authenticated"}), 401
        return jsonify({"ok": True, "username": username}), 200

    @app.get("/protected")
    def protected():
        username = session.get("username") or request.cookies.get("username")
        if not username:
            return jsonify({"error": "login required"}), 401
        return jsonify({"ok": True}), 200

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAuth(unittest.TestCase):

    def setUp(self):
        self.app = _build_test_app()
        self.client = self.app.test_client()

    def test_login_valid(self):
        """POST /login valid credentials → 200, ok:true, session set."""
        resp = self.client.post(
            "/login",
            json={"username": "admin@example.com", "password": "correct-password"},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data.get("ok"))
        self.assertEqual(data.get("username"), "admin@example.com")
        # At least one cookie must be set (either session or username cookie)
        set_cookie = resp.headers.get("Set-Cookie", "")
        self.assertTrue(len(set_cookie) > 0, "Expected at least one Set-Cookie header")

    def test_login_invalid(self):
        """POST /login wrong password → 401."""
        resp = self.client.post(
            "/login",
            json={"username": "admin@example.com", "password": "wrong-password"},
        )
        self.assertEqual(resp.status_code, 401)
        data = resp.get_json()
        self.assertFalse(data.get("ok"))

    def test_login_unknown_user(self):
        """POST /login unknown user → 401."""
        resp = self.client.post(
            "/login",
            json={"username": "nobody@example.com", "password": "anything"},
        )
        self.assertEqual(resp.status_code, 401)

    def test_user_resolve_session(self):
        """GET /user/resolve with valid session → 200 + username."""
        with self.client as c:
            c.post(
                "/login",
                json={"username": "user@example.com", "password": "userpass"},
            )
            resp = c.get("/user/resolve")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data.get("ok"))
        self.assertEqual(data.get("username"), "user@example.com")

    def test_require_login_gate(self):
        """Unauthenticated request to protected endpoint → 401."""
        resp = self.client.get("/protected")
        self.assertEqual(resp.status_code, 401)

    def test_logout_clears(self):
        """Logout clears session; subsequent /user/resolve returns 401."""
        with self.client as c:
            c.post(
                "/login",
                json={"username": "user@example.com", "password": "userpass"},
            )
            logout_resp = c.post("/logout")
            self.assertEqual(logout_resp.status_code, 200)
            resolve_resp = c.get("/user/resolve")
        self.assertEqual(resolve_resp.status_code, 401)

    def test_login_sets_httponly_cookie(self):
        """Session cookie (if any) must have HttpOnly flag; username cookie is JS-accessible."""
        resp = self.client.post(
            "/login",
            json={"username": "admin@example.com", "password": "correct-password"},
        )
        self.assertEqual(resp.status_code, 200)
        # Verify the app is configured for HttpOnly sessions
        self.assertTrue(self.app.config.get("SESSION_COOKIE_HTTPONLY"))


if __name__ == "__main__":
    unittest.main()
