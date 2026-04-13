"""
test_proxy_node.py — Tests for the proxy-to-Node helper.

Stubs replicate the _proxy_to_node_admin logic from webbridge.py.

Run with:  pytest tests/test_proxy_node.py
"""
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask, jsonify, request
import requests as _requests_lib

# ---------------------------------------------------------------------------
# Inline stub of proxy helper
# ---------------------------------------------------------------------------

_NODE_BASE_URL = "http://localhost:3000"
_ADMIN_API_TOKEN = ""


def _proxy_to_node_admin(path: str, method: str, headers: dict,
                         body: bytes, node_base: str, admin_token: str) -> tuple:
    """
    Stub proxy that mirrors the logic of _proxy_to_node_admin in webbridge.py.
    Returns (response_json, status_code).
    """
    proxy_headers = {"Content-Type": "application/json"}
    if admin_token:
        proxy_headers["Authorization"] = f"Bearer {admin_token}"
    elif headers.get("username"):
        proxy_headers["Cookie"] = f"username={headers['username']}"

    try:
        r = _requests_lib.request(
            method,
            f"{node_base}{path}",
            headers=proxy_headers,
            data=body,
            timeout=10,
        )
        return r.json(), r.status_code
    except Exception as e:
        return {"error": f"Node service unavailable: {e}"}, 503


def _build_proxy_app(node_base: str, admin_token: str):
    app = Flask(__name__)
    app.config["TESTING"] = True

    @app.post("/proxy/admin/<path:path>")
    def proxy_admin(path):
        body = request.get_data()
        incoming_headers = {"username": request.cookies.get("username", "")}
        data, status = _proxy_to_node_admin(
            f"/{path}", request.method, incoming_headers, body, node_base, admin_token
        )
        return jsonify(data), status

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestProxyNode(unittest.TestCase):

    def test_proxy_with_token(self):
        """ADMIN_API_TOKEN set → Authorization Bearer header forwarded."""
        sent_headers = {}

        def mock_request(method, url, headers=None, data=None, timeout=None):
            sent_headers.update(headers or {})
            r = MagicMock()
            r.json.return_value = {"ok": True}
            r.status_code = 200
            return r

        with patch("requests.request", side_effect=mock_request):
            data, status = _proxy_to_node_admin(
                "/admin/users", "POST", {}, b"{}",
                "http://node:3000", "my-secret-token"
            )
        self.assertEqual(status, 200)
        self.assertTrue(data.get("ok"))
        self.assertIn("Authorization", sent_headers)
        self.assertEqual(sent_headers["Authorization"], "Bearer my-secret-token")

    def test_proxy_with_cookie(self):
        """No admin token + username cookie → Cookie header forwarded."""
        sent_headers = {}

        def mock_request(method, url, headers=None, data=None, timeout=None):
            sent_headers.update(headers or {})
            r = MagicMock()
            r.json.return_value = {"ok": True}
            r.status_code = 200
            return r

        with patch("requests.request", side_effect=mock_request):
            data, status = _proxy_to_node_admin(
                "/admin/users", "GET", {"username": "alice"}, b"",
                "http://node:3000", ""
            )
        self.assertEqual(status, 200)
        self.assertIn("Cookie", sent_headers)
        self.assertIn("alice", sent_headers["Cookie"])
        self.assertNotIn("Authorization", sent_headers)

    def test_proxy_node_down(self):
        """Node unreachable → 503 with helpful error message."""
        with patch("requests.request", side_effect=ConnectionError("refused")):
            data, status = _proxy_to_node_admin(
                "/admin/users", "GET", {}, b"",
                "http://unreachable:3000", ""
            )
        self.assertEqual(status, 503)
        self.assertIn("error", data)
        self.assertIn("unavailable", data["error"].lower())

    def test_proxy_no_token_no_cookie(self):
        """No token and no username → request goes through without auth headers."""
        sent_headers = {}

        def mock_request(method, url, headers=None, data=None, timeout=None):
            sent_headers.update(headers or {})
            r = MagicMock()
            r.json.return_value = {"ok": True}
            r.status_code = 200
            return r

        with patch("requests.request", side_effect=mock_request):
            data, status = _proxy_to_node_admin(
                "/admin/health", "GET", {}, b"",
                "http://node:3000", ""
            )
        self.assertEqual(status, 200)
        self.assertNotIn("Authorization", sent_headers)
        self.assertNotIn("Cookie", sent_headers)


if __name__ == "__main__":
    unittest.main()
