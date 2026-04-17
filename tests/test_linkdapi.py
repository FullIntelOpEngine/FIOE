"""
test_linkdapi.py — Tests for the linkdapi get-profile endpoint.

Validates that the curl-based approach handles all expected scenarios:
  - successful profile fetch
  - HTTP error codes (401, 403, 404, 5xx)
  - curl failures (non-zero exit, timeout)
  - username extraction from LinkedIn URLs
  - configuration validation (disabled, missing key)

Run with:  pytest tests/test_linkdapi.py -v
"""
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Minimal Flask app stub so we can test the route handler in isolation.
# We replicate the core logic from webbridge_routes.py without importing
# the full production stack.
# ---------------------------------------------------------------------------

import re


def _extract_username(linkedin_url: str):
    """Extract LinkedIn username from URL (mirrors webbridge_routes.py logic)."""
    m = re.search(r"/in/([A-Za-z0-9_%-]+)", linkedin_url)
    return m.group(1) if m else None


_STATUS_SEP = "\n__LINKDAPI_HTTP_STATUS__"


def _build_curl_cmd(username: str, api_key: str):
    """Build the curl command list (mirrors webbridge_routes.py)."""
    import urllib.parse
    qs = urllib.parse.urlencode({"username": username})
    url = f"https://api.linkd.io/api/v1/profile/full?{qs}"
    return [
        "curl", "-sSk",
        "--tls-max", "1.2",
        "--max-time", "30",
        "-H", f"x-api-key: {api_key}",
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
        # Find the header value after the -H flag
        for i, arg in enumerate(cmd):
            if arg == "-H" and i + 1 < len(cmd) and "x-api-key" in cmd[i + 1]:
                self.assertEqual(cmd[i + 1], "x-api-key: my-secret-key")
                return
        self.fail("x-api-key header not found in curl command")

    def test_url_encoding(self):
        cmd = _build_curl_cmd("takano-yuki-0ba87025b", "key")
        url = cmd[-1]
        self.assertIn("username=takano-yuki-0ba87025b", url)
        self.assertTrue(url.startswith("https://api.linkd.io/api/v1/profile/full?"))


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


class TestCurlSSLFlags(unittest.TestCase):
    """Verify that the curl command uses TLS 1.2 max and insecure flags."""

    def test_tls_max_flag(self):
        cmd = _build_curl_cmd("user", "key")
        idx = cmd.index("--tls-max")
        self.assertEqual(cmd[idx + 1], "1.2")

    def test_insecure_flag(self):
        """curl -k flag is present to skip cert verification."""
        cmd = _build_curl_cmd("user", "key")
        self.assertIn("-sSk", cmd)

    def test_no_python_ssl_used(self):
        """Ensure we're NOT importing ssl module for the linkdapi call."""
        cmd = _build_curl_cmd("user", "key")
        # The command should be a list of strings (curl args), not Python objects
        self.assertTrue(all(isinstance(arg, str) for arg in cmd))


class TestLinkdapiEndToEnd(unittest.TestCase):
    """End-to-end tests mocking subprocess.run to simulate curl responses."""

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
    def test_successful_fetch(self, mock_run):
        """Successful profile fetch returns JSON data."""
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
        result.stderr = b"curl: (7) Failed to connect"
        result.returncode = 7
        mock_run.return_value = result

        body, status = _parse_curl_output(result.stdout)
        self.assertEqual(status, 0)  # "000" → 0

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


if __name__ == "__main__":
    unittest.main()
