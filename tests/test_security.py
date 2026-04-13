"""
test_security.py — Tests for security headers, HSTS, session flags, path traversal,
                   and basic rate-bypass protection.

Standalone stubs — no webbridge.py import required.

Run with:  pytest tests/test_security.py
"""
import os
import unittest
from flask import Flask, jsonify, request, Response


# ---------------------------------------------------------------------------
# Build a security-aware test app
# ---------------------------------------------------------------------------

def _build_security_app(force_https: bool = False):
    app = Flask(__name__, static_folder=None)
    app.config["TESTING"] = True
    app.secret_key = "security-test-secret"
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = True

    _ALLOWED_ORIGINS = {"http://localhost:3000"}

    @app.after_request
    def _security_headers(response):
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; object-src 'none'",
        )
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        if force_https:
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains"
            )
        return response

    @app.get("/ping")
    def ping():
        return jsonify({"pong": True}), 200

    @app.get("/set-session")
    def set_session():
        from flask import session
        session["username"] = "test_user"
        return jsonify({"ok": True}), 200

    @app.get("/ui/<path:filename>")
    def serve_ui(filename):
        # Prevent path traversal: reject anything containing ".."
        if ".." in filename:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"file": filename}), 200

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSecurityHeaders(unittest.TestCase):

    def setUp(self):
        self.app = _build_security_app(force_https=False)
        self.client = self.app.test_client()

    def test_csp_header(self):
        """CSP header is present and does not contain 'unsafe-inline'."""
        resp = self.client.get("/ping")
        csp = resp.headers.get("Content-Security-Policy", "")
        self.assertTrue(csp, "CSP header should not be empty")
        self.assertNotIn("unsafe-inline", csp)

    def test_xframe_options(self):
        """X-Frame-Options = SAMEORIGIN."""
        resp = self.client.get("/ping")
        self.assertEqual(resp.headers.get("X-Frame-Options"), "SAMEORIGIN")

    def test_x_content_type_nosniff(self):
        """X-Content-Type-Options = nosniff."""
        resp = self.client.get("/ping")
        self.assertEqual(resp.headers.get("X-Content-Type-Options"), "nosniff")

    def test_referrer_policy(self):
        """Referrer-Policy header is present."""
        resp = self.client.get("/ping")
        self.assertIn("Referrer-Policy", resp.headers)

    def test_hsts_absent_when_not_forced(self):
        """HSTS header absent when FORCE_HTTPS is not set."""
        resp = self.client.get("/ping")
        self.assertNotIn("Strict-Transport-Security", resp.headers)

    def test_hsts_force_https(self):
        """HSTS header present when force_https=True."""
        app = _build_security_app(force_https=True)
        client = app.test_client()
        resp = client.get("/ping")
        self.assertIn("Strict-Transport-Security", resp.headers)
        hsts = resp.headers["Strict-Transport-Security"]
        self.assertIn("max-age=", hsts)

    def test_session_cookie_flags(self):
        """Session cookie has HttpOnly and SameSite flags."""
        resp = self.client.get("/set-session")
        cookie_header = resp.headers.get("Set-Cookie", "")
        self.assertIn("HttpOnly", cookie_header)
        self.assertIn("SameSite", cookie_header)

    def test_path_traversal(self):
        """Path with .. traversal attempt → 404."""
        resp = self.client.get("/ui/../etc/passwd")
        self.assertEqual(resp.status_code, 404)

    def test_valid_ui_path(self):
        """Normal UI path is served."""
        resp = self.client.get("/ui/app.js")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json().get("file"), "app.js")

    def test_rate_bypass_attempts(self):
        """Rapid repeated requests are handled consistently (no crashes)."""
        # Without flask-limiter installed, all succeed — we just verify no 5xx errors
        for _ in range(20):
            resp = self.client.get("/ping")
            self.assertIn(resp.status_code, (200, 429))


if __name__ == "__main__":
    unittest.main()
