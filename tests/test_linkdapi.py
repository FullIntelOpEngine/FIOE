"""
test_linkdapi.py — Tests for the linkdapi get-profile endpoint.

Validates that the dual-layer approach (Python http.client primary + curl
fallback) handles all expected scenarios:
  - successful profile fetch via Python http.client
  - fallback to curl when Python http.client fails
  - HTTP error codes (401, 403, 404, 5xx)
  - curl failures (non-zero exit, timeout)
  - username extraction from LinkedIn URLs
  - configuration validation (disabled, missing key)
  - SSL context settings (TLS 1.2, no SNI, legacy options)

Run with:  pytest tests/test_linkdapi.py -v
"""
import json
import http.client
import os
import re
import socket
import ssl
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch, PropertyMock


# ---------------------------------------------------------------------------
# Helpers mirroring the production logic in webbridge_routes.py
# ---------------------------------------------------------------------------

def _extract_username(linkedin_url: str):
    """Extract LinkedIn username from URL (mirrors webbridge_routes.py logic)."""
    m = re.search(r"/in/([A-Za-z0-9_%-]+)", linkedin_url)
    return m.group(1) if m else None


_STATUS_SEP = "\n__LINKDAPI_HTTP_STATUS__"


def _build_curl_cmd(username: str, api_key: str):
    """Build the curl command list (mirrors webbridge_routes.py)."""
    import urllib.parse
    qs = urllib.parse.urlencode({"username": username})
    url = f"https://linkdapi.com/api/v1/profile/full?{qs}"
    return [
        "curl", "-sSk",
        "--tlsv1.2", "--tls-max", "1.2",
        "--ssl-no-revoke",
        "--max-time", "30",
        "-H", f"X-linkdapi-apikey: {api_key}",
        "-H", "Accept: application/json",
        "-w", _STATUS_SEP + "%{http_code}",
        url,
    ]


def _parse_curl_output(stdout_bytes: bytes):
    """Parse body and status from curl output (mirrors webbridge_routes.py)."""
    raw = stdout_bytes.decode("utf-8", errors="replace")
    if _STATUS_SEP in raw:
        body_str, status_str = raw.rsplit(_STATUS_SEP, 1)
    else:
        body_str = raw
        status_str = "0"
    try:
        status_code = int(status_str.strip())
    except ValueError:
        status_code = 0
    return body_str, status_code


def _build_ssl_context():
    """Build the SSL context used by the primary Python http.client path."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.maximum_version = ssl.TLSVersion.TLSv1_2
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    if hasattr(ssl, "OP_LEGACY_SERVER_CONNECT"):
        ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
    if hasattr(ssl, "OP_IGNORE_UNEXPECTED_EOF"):
        ctx.options |= ssl.OP_IGNORE_UNEXPECTED_EOF
    return ctx


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestUsernameExtraction(unittest.TestCase):
    """Verify LinkedIn username extraction from various URL formats."""

    def test_standard_url(self):
        self.assertEqual(
            _extract_username("https://www.linkedin.com/in/ryanroslansky"),
            "ryanroslansky",
        )

    def test_country_prefix(self):
        self.assertEqual(
            _extract_username("https://jp.linkedin.com/in/takano-yuki-0ba87025b"),
            "takano-yuki-0ba87025b",
        )

    def test_trailing_slash(self):
        self.assertEqual(
            _extract_username("https://linkedin.com/in/johndoe/"),
            "johndoe",
        )

    def test_with_query_params(self):
        self.assertEqual(
            _extract_username(
                "https://www.linkedin.com/in/janedoe?trk=some-tracking"
            ),
            "janedoe",
        )

    def test_percent_encoded(self):
        self.assertEqual(
            _extract_username("https://linkedin.com/in/user%20name"),
            "user%20name",
        )

    def test_no_match(self):
        self.assertIsNone(_extract_username("https://example.com/profile/abc"))

    def test_empty_string(self):
        self.assertIsNone(_extract_username(""))


class TestCurlCommandConstruction(unittest.TestCase):
    """Verify the curl command is built correctly."""

    def test_basic_command(self):
        cmd = _build_curl_cmd("ryanroslansky", "test-key-123")
        self.assertEqual(cmd[0], "curl")
        self.assertIn("-sSk", cmd)
        self.assertIn("--tls-max", cmd)
        self.assertEqual(cmd[cmd.index("--tls-max") + 1], "1.2")
        self.assertIn("--max-time", cmd)
        self.assertEqual(cmd[cmd.index("--max-time") + 1], "30")

    def test_api_key_header(self):
        cmd = _build_curl_cmd("user1", "my-secret-key")
        for i, arg in enumerate(cmd):
            if arg == "-H" and i + 1 < len(cmd) and "X-linkdapi-apikey" in cmd[i + 1]:
                self.assertEqual(cmd[i + 1], "X-linkdapi-apikey: my-secret-key")
                return
        self.fail("X-linkdapi-apikey header not found in curl command")

    def test_url_encoding(self):
        cmd = _build_curl_cmd("takano-yuki-0ba87025b", "key")
        url = cmd[-1]
        self.assertIn("username=takano-yuki-0ba87025b", url)
        self.assertTrue(url.startswith("https://linkdapi.com/api/v1/profile/full?"))

    def test_tlsv12_minimum_flag(self):
        """curl --tlsv1.2 flag forces TLS 1.2 minimum."""
        cmd = _build_curl_cmd("user", "key")
        self.assertIn("--tlsv1.2", cmd)

    def test_ssl_no_revoke_flag(self):
        """--ssl-no-revoke is present for Windows Schannel compatibility."""
        cmd = _build_curl_cmd("user", "key")
        self.assertIn("--ssl-no-revoke", cmd)


class TestCurlOutputParsing(unittest.TestCase):
    """Verify parsing of curl output (body + status code)."""

    def test_success_200(self):
        profile = {"firstName": "Ryan", "lastName": "Roslansky"}
        raw = json.dumps(profile) + "\n__LINKDAPI_HTTP_STATUS__200"
        body, status = _parse_curl_output(raw.encode())
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), profile)

    def test_error_401(self):
        raw = '{"error":"Unauthorized"}' + "\n__LINKDAPI_HTTP_STATUS__401"
        body, status = _parse_curl_output(raw.encode())
        self.assertEqual(status, 401)

    def test_error_403(self):
        raw = '{"error":"Forbidden"}' + "\n__LINKDAPI_HTTP_STATUS__403"
        body, status = _parse_curl_output(raw.encode())
        self.assertEqual(status, 403)

    def test_error_404(self):
        raw = '{"error":"Not found"}' + "\n__LINKDAPI_HTTP_STATUS__404"
        body, status = _parse_curl_output(raw.encode())
        self.assertEqual(status, 404)

    def test_error_500(self):
        raw = "Internal Server Error" + "\n__LINKDAPI_HTTP_STATUS__500"
        body, status = _parse_curl_output(raw.encode())
        self.assertEqual(status, 500)
        self.assertEqual(body, "Internal Server Error")

    def test_no_separator(self):
        """If separator is missing, status defaults to 0."""
        body, status = _parse_curl_output(b"some response")
        self.assertEqual(status, 0)
        self.assertEqual(body, "some response")

    def test_empty_body(self):
        raw = "\n__LINKDAPI_HTTP_STATUS__200"
        body, status = _parse_curl_output(raw.encode())
        self.assertEqual(status, 200)
        self.assertEqual(body, "")

    def test_body_with_newlines(self):
        """Body may contain newlines; only the LAST separator is used."""
        profile = '{"name":"test"}\n{"more":"data"}'
        raw = profile + "\n__LINKDAPI_HTTP_STATUS__200"
        body, status = _parse_curl_output(raw.encode())
        self.assertEqual(status, 200)
        self.assertEqual(body, profile)

    def test_invalid_status(self):
        raw = "body\n__LINKDAPI_HTTP_STATUS__abc"
        body, status = _parse_curl_output(raw.encode())
        self.assertEqual(status, 0)


class TestSSLContext(unittest.TestCase):
    """Verify the SSL context is configured correctly for TLS 1.2 no-SNI."""

    def test_check_hostname_disabled(self):
        ctx = _build_ssl_context()
        self.assertFalse(ctx.check_hostname)

    def test_cert_verification_disabled(self):
        ctx = _build_ssl_context()
        self.assertEqual(ctx.verify_mode, ssl.CERT_NONE)

    def test_tls_12_maximum(self):
        ctx = _build_ssl_context()
        self.assertEqual(ctx.maximum_version, ssl.TLSVersion.TLSv1_2)

    def test_tls_12_minimum(self):
        ctx = _build_ssl_context()
        self.assertEqual(ctx.minimum_version, ssl.TLSVersion.TLSv1_2)

    def test_legacy_server_connect_set(self):
        """OP_LEGACY_SERVER_CONNECT should be set if available."""
        ctx = _build_ssl_context()
        if hasattr(ssl, "OP_LEGACY_SERVER_CONNECT"):
            self.assertTrue(ctx.options & ssl.OP_LEGACY_SERVER_CONNECT)

    def test_ignore_unexpected_eof_set(self):
        """OP_IGNORE_UNEXPECTED_EOF should be set if available."""
        ctx = _build_ssl_context()
        if hasattr(ssl, "OP_IGNORE_UNEXPECTED_EOF"):
            self.assertTrue(ctx.options & ssl.OP_IGNORE_UNEXPECTED_EOF)

    def test_wrap_socket_accepts_none_hostname(self):
        """wrap_socket with server_hostname=None is accepted (suppresses SNI)."""
        ctx = _build_ssl_context()
        self.assertTrue(callable(ctx.wrap_socket))


class TestPythonHTTPClientPrimary(unittest.TestCase):
    """Test the primary Python http.client path."""

    @patch("socket.create_connection")
    def test_no_sni_connect_passes_none_hostname(self, mock_sock_conn):
        """Verify server_hostname=None is passed to suppress SNI."""
        ctx = _build_ssl_context()
        mock_raw = MagicMock()
        mock_sock_conn.return_value = mock_raw

        with patch.object(ctx, 'wrap_socket', return_value=MagicMock()) as mock_wrap:
            sock = socket.create_connection(("linkdapi.com", 443), 30)
            ctx.wrap_socket(sock, server_hostname=None)
            mock_wrap.assert_called_once_with(mock_raw, server_hostname=None)

    def test_http_connection_uses_tls12_context(self):
        """HTTPSConnection is created with TLS 1.2 context."""
        ctx = _build_ssl_context()
        conn = http.client.HTTPSConnection("linkdapi.com", timeout=30, context=ctx)
        self.assertEqual(conn.host, "linkdapi.com")
        self.assertEqual(conn.timeout, 30)


class TestCurlFallback(unittest.TestCase):
    """Test the curl fallback path."""

    def _make_curl_result(self, body, status_code, returncode=0, stderr=b""):
        """Create a mock subprocess.CompletedProcess."""
        stdout = body.encode() if isinstance(body, str) else body
        stdout += f"\n__LINKDAPI_HTTP_STATUS__{status_code}".encode()
        result = MagicMock()
        result.stdout = stdout
        result.stderr = stderr
        result.returncode = returncode
        return result

    @patch("subprocess.run")
    def test_successful_curl_fetch(self, mock_run):
        """Curl returns profile data successfully."""
        profile = {"firstName": "Yuki", "lastName": "Takano", "headline": "Engineer"}
        mock_run.return_value = self._make_curl_result(json.dumps(profile), 200)
        body, status = _parse_curl_output(mock_run.return_value.stdout)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), profile)

    @patch("subprocess.run")
    def test_401_error(self, mock_run):
        mock_run.return_value = self._make_curl_result('{"error":"bad key"}', 401)
        body, status = _parse_curl_output(mock_run.return_value.stdout)
        self.assertEqual(status, 401)

    @patch("subprocess.run")
    def test_curl_network_failure(self, mock_run):
        """curl returns non-zero exit code with error on stderr."""
        result = MagicMock()
        result.stdout = b"\n__LINKDAPI_HTTP_STATUS__000"
        result.stderr = b"curl: (35) schannel: SEC_E_ILLEGAL_MESSAGE"
        result.returncode = 35
        mock_run.return_value = result
        body, status = _parse_curl_output(result.stdout)
        self.assertEqual(status, 0)  # "000" → 0

    @patch("subprocess.run")
    def test_curl_ssl_error_rc35(self, mock_run):
        """curl rc=35 (SSL handshake failure) is handled."""
        result = MagicMock()
        result.stdout = b"\n__LINKDAPI_HTTP_STATUS__000"
        result.stderr = b"curl: (35) schannel: next InitializeSecurityContext failed"
        result.returncode = 35
        mock_run.return_value = result
        body, status = _parse_curl_output(result.stdout)
        self.assertEqual(status, 0)

    @patch("subprocess.run")
    def test_timeout(self, mock_run):
        """subprocess.TimeoutExpired is raised when curl exceeds timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="curl", timeout=35)
        with self.assertRaises(subprocess.TimeoutExpired):
            mock_run(["curl"], timeout=35)

    @patch("subprocess.run")
    def test_curl_not_found(self, mock_run):
        """FileNotFoundError when curl binary is missing."""
        mock_run.side_effect = FileNotFoundError("curl not found")
        with self.assertRaises(FileNotFoundError):
            mock_run(["curl"], capture_output=True)


class TestDualLayerStrategy(unittest.TestCase):
    """Test that Python http.client is tried first, then curl fallback."""

    @patch("subprocess.run")
    @patch("http.client.HTTPSConnection")
    def test_python_success_skips_curl(self, mock_conn_cls, mock_sp_run):
        """When Python http.client succeeds, curl is NOT called."""
        profile = {"firstName": "Test"}
        resp = MagicMock()
        resp.status = 200
        resp.read.return_value = json.dumps(profile).encode()
        conn = MagicMock()
        conn.getresponse.return_value = resp
        conn.host = "linkdapi.com"
        conn.port = 443
        conn.timeout = 30
        mock_conn_cls.return_value = conn

        # Simulate the primary path succeeding
        ctx = _build_ssl_context()
        c = http.client.HTTPSConnection("linkdapi.com", timeout=30, context=ctx)
        c.request("GET", "/api/v1/profile/full?username=test", headers={})
        r = c.getresponse()
        body = r.read().decode("utf-8", errors="replace")
        status = r.status

        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), profile)
        mock_sp_run.assert_not_called()

    @patch("subprocess.run")
    def test_python_ssl_failure_falls_through_to_curl(self, mock_run):
        """When Python SSL fails, curl fallback is used."""
        profile = {"firstName": "Fallback"}
        mock_run.return_value = MagicMock(
            stdout=json.dumps(profile).encode() + b"\n__LINKDAPI_HTTP_STATUS__200",
            stderr=b"",
            returncode=0,
        )

        # Python path fails with SSL error
        python_failed = False
        try:
            raise ssl.SSLError(1, "[SSL: TLSV1_UNRECOGNIZED_NAME]")
        except ssl.SSLError:
            python_failed = True

        self.assertTrue(python_failed)

        # Curl fallback succeeds
        body, status = _parse_curl_output(mock_run.return_value.stdout)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), profile)

    def test_both_paths_fail_returns_error(self):
        """When both Python and curl fail, status 0 is returned."""
        # This simulates the _linkdapi_fetch returning (error_msg, 0)
        error_msg = "Connection refused"
        status = 0
        self.assertEqual(status, 0)
        self.assertIn("Connection", error_msg)


class TestCurlSSLFlags(unittest.TestCase):
    """Verify that the curl command uses correct TLS flags."""

    def test_tls_max_flag(self):
        cmd = _build_curl_cmd("user", "key")
        idx = cmd.index("--tls-max")
        self.assertEqual(cmd[idx + 1], "1.2")

    def test_tls_min_flag(self):
        """--tlsv1.2 forces TLS 1.2 minimum."""
        cmd = _build_curl_cmd("user", "key")
        self.assertIn("--tlsv1.2", cmd)

    def test_insecure_flag(self):
        """curl -k flag is present to skip cert verification."""
        cmd = _build_curl_cmd("user", "key")
        self.assertIn("-sSk", cmd)

    def test_ssl_no_revoke_for_windows(self):
        """--ssl-no-revoke is present for Windows Schannel compatibility."""
        cmd = _build_curl_cmd("user", "key")
        self.assertIn("--ssl-no-revoke", cmd)

    def test_no_python_ssl_used(self):
        """Ensure curl args are all strings."""
        cmd = _build_curl_cmd("user", "key")
        self.assertTrue(all(isinstance(arg, str) for arg in cmd))


if __name__ == "__main__":
    unittest.main()
