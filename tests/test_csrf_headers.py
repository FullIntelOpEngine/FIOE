"""
test_csrf_headers.py — Tests for CSRF protection, CORS, and security headers.

Stubs replicate the relevant after-request hooks and decorators from webbridge.py.

Run with:  pytest tests/test_csrf_headers.py
"""
import unittest
from functools import wraps

from flask import Flask, jsonify, request, Response

# ---------------------------------------------------------------------------
# Helpers that mirror webbridge.py behaviour
# ---------------------------------------------------------------------------

_ALLOWED_ORIGINS = {
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:4000",
    "http://localhost:8091",
}


def _build_csrf_app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    app.secret_key = "csrf-test-secret"

    def _csrf_required(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if request.method in ("POST", "PUT", "PATCH", "DELETE"):
                if not (
                    request.headers.get("X-Requested-With")
                    or request.headers.get("X-CSRF-Token")
                ):
                    return jsonify({"error": "Missing required header"}), 403
            return f(*args, **kwargs)
        return wrapped

    @app.after_request
    def _apply_security(response):
        origin = request.headers.get("Origin", "")
        if origin.lower() in {o.lower() for o in _ALLOWED_ORIGINS}:
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Credentials"] = "true"
        # Security headers
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; object-src 'none'",
        )
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response

    @app.route("/state-change", methods=["GET", "POST", "OPTIONS"])
    @_csrf_required
    def state_change():
        if request.method == "OPTIONS":
            origin = request.headers.get("Origin", "")
            resp = Response("", status=204)
            if origin.lower() in {o.lower() for o in _ALLOWED_ORIGINS}:
                resp.headers["Access-Control-Allow-Origin"] = origin
                resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
                resp.headers["Access-Control-Allow-Headers"] = (
                    "Content-Type, Authorization, X-Requested-With, X-CSRF-Token"
                )
                resp.headers["Access-Control-Allow-Credentials"] = "true"
            return resp
        return jsonify({"ok": True}), 200

    @app.get("/ping")
    def ping():
        return jsonify({"pong": True}), 200

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCSRFHeaders(unittest.TestCase):

    def setUp(self):
        self.app = _build_csrf_app()
        self.client = self.app.test_client()

    def test_csrf_reject_missing(self):
        """POST without X-Requested-With or X-CSRF-Token → 403."""
        resp = self.client.post(
            "/state-change",
            json={"data": "value"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 403)
        self.assertIn("Missing required header", resp.get_json().get("error", ""))

    def test_csrf_accept_x_requested_with(self):
        """POST with X-Requested-With: XMLHttpRequest → 200."""
        resp = self.client.post(
            "/state-change",
            json={"data": "value"},
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 200)

    def test_csrf_accept_x_csrf_token(self):
        """POST with X-CSRF-Token → 200."""
        resp = self.client.post(
            "/state-change",
            json={"data": "value"},
            headers={"X-CSRF-Token": "any-token"},
        )
        self.assertEqual(resp.status_code, 200)

    def test_preflight_cors(self):
        """OPTIONS from allowed origin → 204 + ACAO header."""
        resp = self.client.options(
            "/state-change",
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(resp.status_code, 204)
        self.assertEqual(
            resp.headers.get("Access-Control-Allow-Origin"), "http://localhost:3000"
        )

    def test_cors_not_wildcard(self):
        """Non-allowlisted origin gets no Access-Control-Allow-Origin header."""
        resp = self.client.get(
            "/ping",
            headers={"Origin": "http://evil.example.com"},
        )
        self.assertNotIn("Access-Control-Allow-Origin", resp.headers)

    def test_security_headers(self):
        """CSP, X-Content-Type-Options, X-Frame-Options, Referrer-Policy present."""
        resp = self.client.get("/ping")
        headers = resp.headers
        self.assertIn("Content-Security-Policy", headers)
        self.assertEqual(headers.get("X-Content-Type-Options"), "nosniff")
        self.assertEqual(headers.get("X-Frame-Options"), "SAMEORIGIN")
        self.assertIn("Referrer-Policy", headers)

    def test_csp_no_unsafe_inline(self):
        """CSP header must not contain 'unsafe-inline'."""
        resp = self.client.get("/ping")
        csp = resp.headers.get("Content-Security-Policy", "")
        self.assertNotIn("unsafe-inline", csp)

    def test_allowed_origin_gets_acao(self):
        """Request from an allowed origin gets Access-Control-Allow-Origin set."""
        resp = self.client.get(
            "/ping",
            headers={"Origin": "http://localhost:4000"},
        )
        self.assertEqual(
            resp.headers.get("Access-Control-Allow-Origin"), "http://localhost:4000"
        )


if __name__ == "__main__":
    unittest.main()
