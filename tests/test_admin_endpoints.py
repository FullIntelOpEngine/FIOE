"""
test_admin_endpoints.py — Tests for /admin/* REST endpoints.

Uses a minimal Flask test app with mocked DB to avoid importing webbridge.py.

Run with:  pytest tests/test_admin_endpoints.py
"""
import json
import os
import tempfile
import unittest
from functools import wraps

from flask import Flask, jsonify, request


# ---------------------------------------------------------------------------
# Test app factory
# ---------------------------------------------------------------------------

def _build_admin_app(rate_limits_path: str, email_verif_path: str):
    app = Flask(__name__)
    app.config["TESTING"] = True
    app.secret_key = "admin-endpoint-test-secret"

    # Stub _require_admin: always passes in test mode
    def _require_admin(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapper

    # Stub _csrf_required: always passes in test mode
    def _csrf_required(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapper

    def _load_rate_limits():
        try:
            with open(rate_limits_path, "r") as fh:
                return json.load(fh)
        except Exception:
            return {"defaults": {}, "users": {}}

    def _save_rate_limits(config):
        tmp = rate_limits_path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(config, fh, indent=2)
        os.replace(tmp, rate_limits_path)

    def _load_email_verif_config():
        try:
            with open(email_verif_path, "r") as fh:
                return json.load(fh)
        except Exception:
            return {
                "neverbounce": {"api_key": "", "enabled": "disabled"},
                "zerobounce": {"api_key": "", "enabled": "disabled"},
                "bouncer": {"api_key": "", "enabled": "disabled"},
            }

    def _save_email_verif_config(config):
        tmp = email_verif_path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(config, fh, indent=2)
        os.replace(tmp, email_verif_path)

    @app.get("/admin/rate-limits")
    @_require_admin
    def get_rate_limits():
        return jsonify({"ok": True, "config": _load_rate_limits()}), 200

    @app.post("/admin/rate-limits")
    @_require_admin
    @_csrf_required
    def save_rate_limits():
        body = request.get_json(force=True, silent=True) or {}
        config = body.get("config")
        if not isinstance(config, dict):
            return jsonify({"error": "config must be an object"}), 400
        _save_rate_limits(config)
        return jsonify({"ok": True}), 200

    @app.get("/admin/email-verif-config")
    @_require_admin
    def get_email_verif():
        return jsonify({"ok": True, "config": _load_email_verif_config()}), 200

    @app.post("/admin/email-verif-config")
    @_require_admin
    @_csrf_required
    def save_email_verif():
        body = request.get_json(force=True, silent=True) or {}
        if not isinstance(body, dict):
            return jsonify({"error": "invalid body"}), 400
        _save_email_verif_config(body)
        return jsonify({"ok": True}), 200

    # Simulated DB of tokens (keyed by username)
    _token_store = {}

    @app.post("/admin/update-token")
    @_require_admin
    @_csrf_required
    def update_token():
        body = request.get_json(force=True, silent=True) or {}
        username = (body.get("username") or "").strip()
        token = (body.get("token") or "").strip()
        if not username or not token:
            return jsonify({"error": "username and token required"}), 400
        _token_store[username] = token
        return jsonify({"ok": True}), 200

    # Simulated DB of target limits
    _limits_store: dict = {}

    @app.post("/admin/update-target-limit")
    @_require_admin
    @_csrf_required
    def update_target_limit():
        body = request.get_json(force=True, silent=True) or {}
        username = (body.get("username") or "").strip()
        limit = body.get("target_limit")
        if not username:
            return jsonify({"error": "username required"}), 400
        if limit is None or not str(limit).lstrip("-").isdigit():
            return jsonify({"error": "target_limit must be an integer"}), 400
        if username not in _limits_store:
            return jsonify({"error": "user not found"}), 404
        _limits_store[username] = int(limit)
        return jsonify({"ok": True}), 200

    @app.post("/admin/update-price-per-query")
    @_require_admin
    @_csrf_required
    def update_price():
        body = request.get_json(force=True, silent=True) or {}
        username = (body.get("username") or "").strip()
        price = body.get("price_per_query")
        if not username:
            return jsonify({"error": "username required"}), 400
        try:
            float(price)
        except (TypeError, ValueError):
            return jsonify({"error": "price_per_query must be a number"}), 400
        return jsonify({"ok": True}), 200

    # Expose store for tests
    app._limits_store = _limits_store
    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAdminEndpoints(unittest.TestCase):

    def setUp(self):
        self.td = tempfile.mkdtemp()
        self.rl_path = os.path.join(self.td, "rate_limits.json")
        self.ev_path = os.path.join(self.td, "email_verif_config.json")
        with open(self.rl_path, "w") as f:
            json.dump({"defaults": {}, "users": {}}, f)
        self.app = _build_admin_app(self.rl_path, self.ev_path)
        self.client = self.app.test_client()

    def test_rate_limits_get_post(self):
        """GET and POST /admin/rate-limits → 200."""
        resp = self.client.get("/admin/rate-limits")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json().get("ok"))

        resp = self.client.post(
            "/admin/rate-limits",
            json={"config": {"defaults": {}, "users": {}}},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json().get("ok"))

    def test_rate_limits_post_invalid(self):
        """POST /admin/rate-limits with non-dict config → 400."""
        resp = self.client.post(
            "/admin/rate-limits",
            json={"config": "bad-value"},
        )
        self.assertEqual(resp.status_code, 400)

    def test_email_verif_get_post(self):
        """GET and POST /admin/email-verif-config → 200."""
        resp = self.client.get("/admin/email-verif-config")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json().get("ok"))

        resp = self.client.post(
            "/admin/email-verif-config",
            json={"neverbounce": {"api_key": "test-key", "enabled": "enabled"}},
        )
        self.assertEqual(resp.status_code, 200)

    def test_update_token_valid(self):
        """POST /admin/update-token valid body → 200."""
        resp = self.client.post(
            "/admin/update-token",
            json={"username": "alice", "token": "tok-12345"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json().get("ok"))

    def test_update_token_invalid(self):
        """POST /admin/update-token missing token → 400."""
        resp = self.client.post(
            "/admin/update-token",
            json={"username": "alice"},
        )
        self.assertEqual(resp.status_code, 400)

    def test_update_limits_valid(self):
        """POST /admin/update-target-limit for known user → 200."""
        self.app._limits_store["bob"] = 5
        resp = self.client.post(
            "/admin/update-target-limit",
            json={"username": "bob", "target_limit": 20},
        )
        self.assertEqual(resp.status_code, 200)

    def test_update_limits_not_found(self):
        """POST /admin/update-target-limit for unknown user → 404."""
        resp = self.client.post(
            "/admin/update-target-limit",
            json={"username": "nobody", "target_limit": 10},
        )
        self.assertEqual(resp.status_code, 404)

    def test_update_limits_bad_value(self):
        """POST /admin/update-target-limit non-integer limit → 400."""
        self.app._limits_store["charlie"] = 5
        resp = self.client.post(
            "/admin/update-target-limit",
            json={"username": "charlie", "target_limit": "banana"},
        )
        self.assertEqual(resp.status_code, 400)

    def test_update_price_per_query(self):
        """POST /admin/update-price-per-query valid → 200."""
        resp = self.client.post(
            "/admin/update-price-per-query",
            json={"username": "alice", "price_per_query": 0.05},
        )
        self.assertEqual(resp.status_code, 200)

    def test_update_price_per_query_invalid(self):
        """POST /admin/update-price-per-query non-numeric → 400."""
        resp = self.client.post(
            "/admin/update-price-per-query",
            json={"username": "alice", "price_per_query": "free"},
        )
        self.assertEqual(resp.status_code, 400)


if __name__ == "__main__":
    unittest.main()
