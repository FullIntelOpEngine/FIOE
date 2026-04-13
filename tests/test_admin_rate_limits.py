"""
test_admin_rate_limits.py — Tests for the per-user and global rate limiters.

Stubs replicate _UserRateLimiter and _check_user_rate from webbridge.py.

Run with:  pytest tests/test_admin_rate_limits.py
"""
import json
import tempfile
import os
import time
import threading
import unittest
from functools import wraps

from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Inline _UserRateLimiter stub (mirrors webbridge.py)
# ---------------------------------------------------------------------------

_NO_LIMIT = 999999


class _UserRateLimiter:
    def __init__(self, rate_limits_path: str):
        self._path = rate_limits_path
        self._state: dict = {}
        self._lock = threading.Lock()

    def _load(self):
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return {"defaults": {}, "users": {}}

    def is_allowed(self, username: str, feature: str) -> bool:
        if not username:
            return True
        config = self._load()
        user_limits = config.get("users", {}).get(username, {})
        default_limits = config.get("defaults", {})
        limit_cfg = user_limits.get(feature) or default_limits.get(feature)
        if not limit_cfg:
            return True
        max_req = int(limit_cfg.get("requests", _NO_LIMIT))
        window = int(limit_cfg.get("window_seconds", 60))
        now = time.time()
        key = (username, feature)
        with self._lock:
            history = [t for t in self._state.get(key, []) if now - t < window]
            if len(history) >= max_req:
                self._state[key] = history
                return False
            history.append(now)
            self._state[key] = history
            return True

    def get_limit_cfg(self, username: str, feature: str) -> dict:
        config = self._load()
        user_limits = config.get("users", {}).get(username, {})
        default_limits = config.get("defaults", {})
        return user_limits.get(feature) or default_limits.get(feature) or {}


def _build_rate_limited_app(rate_limits_path: str):
    app = Flask(__name__)
    app.config["TESTING"] = True
    limiter = _UserRateLimiter(rate_limits_path)

    def _check_user_rate(feature: str):
        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                username = (
                    request.cookies.get("username")
                    or (request.get_json(force=True, silent=True) or {}).get("username")
                    or ""
                ).strip()
                if username and not limiter.is_allowed(username, feature):
                    cfg = limiter.get_limit_cfg(username, feature)
                    return jsonify({
                        "error": f"Rate limit exceeded for feature '{feature}'",
                        "feature": feature,
                        "requests": cfg.get("requests"),
                        "window_seconds": cfg.get("window_seconds"),
                    }), 429
                return f(*args, **kwargs)
            return wrapper
        return decorator

    @app.post("/api/limited")
    @_check_user_rate("test_feature")
    def limited_endpoint():
        return jsonify({"ok": True}), 200

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPerUserRateLimiter(unittest.TestCase):

    def _make_config(self, tmpdir, requests=2, window_seconds=60, username="testuser"):
        cfg = {
            "defaults": {},
            "users": {
                username: {
                    "test_feature": {
                        "requests": requests,
                        "window_seconds": window_seconds,
                    }
                }
            },
        }
        path = os.path.join(tmpdir, "rate_limits.json")
        with open(path, "w") as fh:
            json.dump(cfg, fh)
        return path

    def test_per_user_limiter(self):
        """_check_user_rate: low-limit user → 429 after threshold."""
        with tempfile.TemporaryDirectory() as td:
            path = self._make_config(td, requests=2, window_seconds=60)
            app = _build_rate_limited_app(path)
            client = app.test_client()
            # First two requests should succeed
            for _ in range(2):
                resp = client.post(
                    "/api/limited",
                    json={"username": "testuser"},
                )
                self.assertEqual(resp.status_code, 200)
            # Third request should be rate-limited
            resp = client.post(
                "/api/limited",
                json={"username": "testuser"},
            )
            self.assertEqual(resp.status_code, 429)

    def test_rate_limit_response(self):
        """429 body includes feature, requests, window_seconds keys."""
        with tempfile.TemporaryDirectory() as td:
            path = self._make_config(td, requests=1, window_seconds=60)
            app = _build_rate_limited_app(path)
            client = app.test_client()
            # Exhaust the one allowed request
            client.post("/api/limited", json={"username": "testuser"})
            resp = client.post("/api/limited", json={"username": "testuser"})
            self.assertEqual(resp.status_code, 429)
            data = resp.get_json()
            self.assertIn("feature", data)
            self.assertIn("requests", data)
            self.assertIn("window_seconds", data)
            self.assertEqual(data["feature"], "test_feature")

    def test_global_limiter(self):
        """User without specific limit passes through (no 429)."""
        with tempfile.TemporaryDirectory() as td:
            # Config with no user overrides for this user
            cfg = {"defaults": {}, "users": {}}
            path = os.path.join(td, "rate_limits.json")
            with open(path, "w") as fh:
                json.dump(cfg, fh)
            app = _build_rate_limited_app(path)
            client = app.test_client()
            # Many requests should all pass (no limit configured)
            for _ in range(10):
                resp = client.post("/api/limited", json={"username": "freeuser"})
                self.assertEqual(resp.status_code, 200)

    def test_anonymous_not_rate_limited(self):
        """Requests without username bypass per-user rate limiter."""
        with tempfile.TemporaryDirectory() as td:
            path = self._make_config(td, requests=1)
            app = _build_rate_limited_app(path)
            client = app.test_client()
            # Multiple anonymous requests should always pass
            for _ in range(5):
                resp = client.post("/api/limited", json={})
                self.assertEqual(resp.status_code, 200)

    def test_window_resets_after_expiry(self):
        """Requests after window expiry are allowed again."""
        with tempfile.TemporaryDirectory() as td:
            path = self._make_config(td, requests=1, window_seconds=1)
            app = _build_rate_limited_app(path)
            client = app.test_client()
            # First request: OK
            resp = client.post("/api/limited", json={"username": "testuser"})
            self.assertEqual(resp.status_code, 200)
            # Immediately: blocked
            resp = client.post("/api/limited", json={"username": "testuser"})
            self.assertEqual(resp.status_code, 429)
            # After window expires: OK again
            time.sleep(1.1)
            resp = client.post("/api/limited", json={"username": "testuser"})
            self.assertEqual(resp.status_code, 200)


# ---------------------------------------------------------------------------
# _make_flask_limit stub (mirrors webbridge.py)
# ---------------------------------------------------------------------------

def _make_flask_limit_stub(rate_limits_path, key, default_req=30, default_win=60):
    """Inline replica of webbridge._make_flask_limit for testing."""
    def _limit():
        try:
            with open(rate_limits_path, "r", encoding="utf-8") as fh:
                cfg = json.load(fh).get(key, {})
        except Exception:
            cfg = {}
        req = int(cfg.get("requests", default_req))
        win = int(cfg.get("window_seconds", default_win))
        return f"{req} per {win} seconds"
    return _limit


class TestMakeFlaskLimit(unittest.TestCase):
    """Tests for the _make_flask_limit factory used across all webbridge files."""

    def test_make_flask_limit_reads_json(self):
        """_make_flask_limit: reads requests/window_seconds from rate_limits.json per call."""
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "rate_limits.json")
            config = {"geography": {"requests": 50, "window_seconds": 30}}
            with open(path, "w") as fh:
                json.dump(config, fh)
            limit_fn = _make_flask_limit_stub(path, "geography")
            result = limit_fn()
            self.assertEqual(result, "50 per 30 seconds")

    def test_make_flask_limit_defaults(self):
        """_make_flask_limit: uses default_req/default_win when key absent from JSON."""
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "rate_limits.json")
            with open(path, "w") as fh:
                json.dump({}, fh)
            limit_fn = _make_flask_limit_stub(path, "nonexistent_key", default_req=20, default_win=45)
            result = limit_fn()
            self.assertEqual(result, "20 per 45 seconds")

    def test_make_flask_limit_live_reload(self):
        """_make_flask_limit: limit changes in rate_limits.json take effect immediately (no restart)."""
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "rate_limits.json")
            # Initial config
            config = {"geography": {"requests": 100, "window_seconds": 60}}
            with open(path, "w") as fh:
                json.dump(config, fh)
            limit_fn = _make_flask_limit_stub(path, "geography")
            self.assertEqual(limit_fn(), "100 per 60 seconds")
            # Simulate admin changing the value without restart
            config["geography"] = {"requests": 200, "window_seconds": 30}
            with open(path, "w") as fh:
                json.dump(config, fh)
            # Calling the same callable picks up new value
            self.assertEqual(limit_fn(), "200 per 30 seconds")

    def test_make_flask_limit_missing_file(self):
        """_make_flask_limit: falls back to defaults gracefully when file is missing."""
        limit_fn = _make_flask_limit_stub("/nonexistent/path/rate_limits.json", "geography", default_req=30, default_win=60)
        result = limit_fn()
        self.assertEqual(result, "30 per 60 seconds")

    def test_make_flask_limit_different_keys(self):
        """_make_flask_limit: each key reads its own section independently."""
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "rate_limits.json")
            config = {
                "geography": {"requests": 100, "window_seconds": 30},
                "upload_multiple_cvs": {"requests": 5, "window_seconds": 3600},
                "gemini": {"requests": 60, "window_seconds": 60},
            }
            with open(path, "w") as fh:
                json.dump(config, fh)
            self.assertEqual(_make_flask_limit_stub(path, "geography")(), "100 per 30 seconds")
            self.assertEqual(_make_flask_limit_stub(path, "upload_multiple_cvs")(), "5 per 3600 seconds")
            self.assertEqual(_make_flask_limit_stub(path, "gemini")(), "60 per 60 seconds")


if __name__ == "__main__":
    unittest.main()
