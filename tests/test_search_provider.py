"""
Unit tests for Search Provider (Serper.dev / DataforSEO / LinkedIn / Google CSE) helpers.

Covers:
  - _load_search_provider_config / _save_search_provider_config  (read/write + defaults)
  - unified_search_page routing:
      • Serper when enabled+keyed
      • DataforSEO when enabled+credentialed (and Serper not enabled)
      • LinkedIn when enabled+keyed (and Serper/DataforSEO not enabled)
      • Google CSE fallback when all disabled / missing credentials
  - serper_active_disables_cse  (Serper enabled → serper_search_page called, CSE skipped)
  - dataforseo_active_disables_cse (DataforSEO enabled → dataforseo_search_page called, CSE skipped)
  - cse_fallback_when_no_serper (all disabled → google_cse_search_page called)
  - provider_label_in_messages  (job messages reflect active provider name)

NOTE: These tests use self-contained stubs that mirror the production functions
without importing the full Flask app (avoids side-effects and heavy dependencies).
When the production logic changes the stubs here should be updated to match.

Run with: pytest tests/test_search_provider.py
"""

import json
import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock

# ---------------------------------------------------------------------------
# Minimal stubs — replicate the functions under test
# ---------------------------------------------------------------------------

def _make_default_config():
    return {
        "serper": {"api_key": "", "enabled": "disabled"},
        "dataforseo": {"login": "", "password": "", "enabled": "disabled"},
        "linkedin": {"api_key": "", "enabled": "disabled"},
    }


def _load_search_provider_config(path):
    """Stub of webbridge._load_search_provider_config, accepts explicit path."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return _make_default_config()


def _save_search_provider_config(config, path):
    """Stub of webbridge._save_search_provider_config, accepts explicit path."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2)
    os.replace(tmp, path)


def unified_search_page(query, num, start_index, gl_hint=None,
                        cfg=None, serper_fn=None, dataforseo_fn=None, linkedin_fn=None, cse_fn=None):
    """
    Stub of webbridge_routes.unified_search_page.

    Priority: Serper → DataforSEO → LinkedIn → Google CSE.
    The real implementation uses module-level globals for the callables; this
    stub accepts them as parameters so tests can inject mocks without patching
    module globals.
    """
    page = max(1, ((start_index - 1) // max(num, 1)) + 1)

    serper_cfg = (cfg or {}).get("serper", {})
    serper_key = serper_cfg.get("api_key", "")
    if serper_cfg.get("enabled", "disabled") == "enabled" and serper_key:
        return serper_fn(query, serper_key, num, gl_hint=gl_hint, page=page)

    dfs_cfg = (cfg or {}).get("dataforseo", {})
    dfs_login    = dfs_cfg.get("login", "")
    dfs_password = dfs_cfg.get("password", "")
    if dfs_cfg.get("enabled", "disabled") == "enabled" and dfs_login and dfs_password:
        return dataforseo_fn(query, dfs_login, dfs_password, num, gl_hint=gl_hint, page=page)

    li_cfg = (cfg or {}).get("linkedin", {})
    li_key = li_cfg.get("api_key", "")
    if li_cfg.get("enabled", "disabled") == "enabled" and li_key:
        return linkedin_fn(query, li_key, num, gl_hint=gl_hint, page=page)

    return cse_fn(query, num, start_index, gl_hint=gl_hint)


def _get_provider_label(cfg):
    """Stub that mirrors the provider-label logic in _perform_cse_queries."""
    serper_on = (
        cfg.get("serper", {}).get("enabled", "disabled") == "enabled"
        and bool(cfg.get("serper", {}).get("api_key"))
    )
    dfs_on = (
        cfg.get("dataforseo", {}).get("enabled", "disabled") == "enabled"
        and bool(cfg.get("dataforseo", {}).get("login"))
        and bool(cfg.get("dataforseo", {}).get("password"))
    )
    li_on = (
        cfg.get("linkedin", {}).get("enabled", "disabled") == "enabled"
        and bool(cfg.get("linkedin", {}).get("api_key"))
    )
    if serper_on:
        return "Serper"
    if dfs_on:
        return "DataforSEO"
    if li_on:
        return "LinkedIn"
    return "CSE"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestLoadSaveSearchProviderConfig(unittest.TestCase):
    """_load_search_provider_config / _save_search_provider_config: read/write"""

    def test_load_save_search_provider(self):
        """Round-trip: save a config then load it back and verify values."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
            path = tf.name
        try:
            config = {
                "serper": {"api_key": "sk-test-key", "enabled": "enabled"},
                "dataforseo": {"login": "user@example.com", "password": "pass", "enabled": "disabled"},
            }
            _save_search_provider_config(config, path)
            loaded = _load_search_provider_config(path)
            self.assertEqual(loaded["serper"]["api_key"], "sk-test-key")
            self.assertEqual(loaded["serper"]["enabled"], "enabled")
            self.assertEqual(loaded["dataforseo"]["login"], "user@example.com")
            self.assertEqual(loaded["dataforseo"]["enabled"], "disabled")
        finally:
            os.unlink(path)

    def test_load_returns_defaults_on_missing_file(self):
        """Missing config file → defaults returned (no crash)."""
        loaded = _load_search_provider_config("/nonexistent/path/config.json")
        self.assertIn("serper", loaded)
        self.assertIn("dataforseo", loaded)
        self.assertEqual(loaded["serper"]["enabled"], "disabled")
        self.assertEqual(loaded["dataforseo"]["enabled"], "disabled")

    def test_load_returns_defaults_on_invalid_json(self):
        """Invalid JSON config → defaults returned (no crash)."""
        with tempfile.NamedTemporaryFile(suffix=".json", mode="w",
                                         delete=False, encoding="utf-8") as tf:
            tf.write("{ this is not valid json }")
            path = tf.name
        try:
            loaded = _load_search_provider_config(path)
            self.assertIn("serper", loaded)
            self.assertEqual(loaded["serper"]["enabled"], "disabled")
        finally:
            os.unlink(path)

    def test_atomic_write_uses_tmp_then_replace(self):
        """Save must not leave a .tmp file behind after writing."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
            path = tf.name
        try:
            _save_search_provider_config({"serper": {"api_key": "x", "enabled": "disabled"},
                                          "dataforseo": {"login": "", "password": "", "enabled": "disabled"}}, path)
            self.assertFalse(os.path.exists(path + ".tmp"),
                             "Temporary .tmp file must be cleaned up after atomic write")
        finally:
            os.unlink(path)


class TestSerperActiveDisablesCse(unittest.TestCase):
    """unified_search_page: Serper enabled → routes to Serper, bypasses CSE keys"""

    def setUp(self):
        self.serper_fn = MagicMock(return_value=(
            [{"link": "https://linkedin.com/in/alice", "title": "Alice", "snippet": "", "displayLink": ""}],
            1000,
        ))
        self.dataforseo_fn = MagicMock(return_value=([], 0))
        self.linkedin_fn = MagicMock(return_value=([], 0))
        self.cse_fn = MagicMock(return_value=([], 0))

    def serper_active_disables_cse(self):
        cfg = {
            "serper": {"api_key": "valid-serper-key", "enabled": "enabled"},
            "dataforseo": {"login": "", "password": "", "enabled": "disabled"},
        }
        results, total = unified_search_page(
            "software engineer site:linkedin.com", 10, 1,
            cfg=cfg, serper_fn=self.serper_fn, dataforseo_fn=self.dataforseo_fn, linkedin_fn=self.linkedin_fn, cse_fn=self.cse_fn,
        )
        self.serper_fn.assert_called_once()
        self.dataforseo_fn.assert_not_called()
        self.linkedin_fn.assert_not_called()
        self.cse_fn.assert_not_called()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["link"], "https://linkedin.com/in/alice")
        self.assertEqual(total, 1000)

    def test_serper_active_disables_cse(self):
        self.serper_active_disables_cse()

    def test_serper_active_page_number_computed_from_start_index(self):
        """start_index=11, num=10 → page 2."""
        cfg = {
            "serper": {"api_key": "k", "enabled": "enabled"},
            "dataforseo": {"login": "", "password": "", "enabled": "disabled"},
        }
        unified_search_page("q", 10, 11, cfg=cfg,
                            serper_fn=self.serper_fn, dataforseo_fn=self.dataforseo_fn, linkedin_fn=self.linkedin_fn, cse_fn=self.cse_fn)
        _, kwargs = self.serper_fn.call_args
        self.assertEqual(kwargs.get("page"), 2)


class TestDataforSeoActiveDisablesCse(unittest.TestCase):
    """unified_search_page: DataforSEO enabled → routes to DataforSEO, bypasses CSE keys"""

    def setUp(self):
        self.serper_fn = MagicMock(return_value=([], 0))
        self.dataforseo_fn = MagicMock(return_value=(
            [{"link": "https://linkedin.com/in/carol", "title": "Carol", "snippet": "", "displayLink": ""}],
            750,
        ))
        self.linkedin_fn = MagicMock(return_value=([], 0))
        self.cse_fn = MagicMock(return_value=([], 0))

    def dataforseo_active_disables_cse(self):
        cfg = {
            "serper": {"api_key": "", "enabled": "disabled"},
            "dataforseo": {"login": "user@example.com", "password": "secret", "enabled": "enabled"},
        }
        results, total = unified_search_page(
            "product manager site:linkedin.com", 10, 1,
            cfg=cfg, serper_fn=self.serper_fn, dataforseo_fn=self.dataforseo_fn, linkedin_fn=self.linkedin_fn, cse_fn=self.cse_fn,
        )
        self.dataforseo_fn.assert_called_once()
        self.serper_fn.assert_not_called()
        self.linkedin_fn.assert_not_called()
        self.cse_fn.assert_not_called()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["link"], "https://linkedin.com/in/carol")
        self.assertEqual(total, 750)

    def test_dataforseo_active_disables_cse(self):
        self.dataforseo_active_disables_cse()

    def test_dataforseo_enabled_but_missing_login_uses_cse(self):
        """Enabled but login missing → falls through to CSE."""
        cfg = {
            "serper": {"api_key": "", "enabled": "disabled"},
            "dataforseo": {"login": "", "password": "secret", "enabled": "enabled"},
        }
        unified_search_page("q", 10, 1, cfg=cfg,
                            serper_fn=self.serper_fn, dataforseo_fn=self.dataforseo_fn, linkedin_fn=self.linkedin_fn, cse_fn=self.cse_fn)
        self.cse_fn.assert_called_once()
        self.dataforseo_fn.assert_not_called()

    def test_dataforseo_enabled_but_missing_password_uses_cse(self):
        """Enabled but password missing → falls through to CSE."""
        cfg = {
            "serper": {"api_key": "", "enabled": "disabled"},
            "dataforseo": {"login": "user@example.com", "password": "", "enabled": "enabled"},
        }
        unified_search_page("q", 10, 1, cfg=cfg,
                            serper_fn=self.serper_fn, dataforseo_fn=self.dataforseo_fn, linkedin_fn=self.linkedin_fn, cse_fn=self.cse_fn)
        self.cse_fn.assert_called_once()
        self.dataforseo_fn.assert_not_called()

    def test_serper_takes_priority_over_dataforseo(self):
        """If both Serper and DataforSEO are enabled, Serper wins."""
        self.serper_fn = MagicMock(return_value=([{"link": "https://s.com", "title": "", "snippet": "", "displayLink": ""}], 1))
        cfg = {
            "serper": {"api_key": "serper-key", "enabled": "enabled"},
            "dataforseo": {"login": "u", "password": "p", "enabled": "enabled"},
        }
        unified_search_page("q", 10, 1, cfg=cfg,
                            serper_fn=self.serper_fn, dataforseo_fn=self.dataforseo_fn, linkedin_fn=self.linkedin_fn, cse_fn=self.cse_fn)
        self.serper_fn.assert_called_once()
        self.dataforseo_fn.assert_not_called()


class TestCseFallbackWhenNoSerper(unittest.TestCase):
    """unified_search_page: both disabled → falls back to Google CSE adapter"""

    def setUp(self):
        self.serper_fn = MagicMock(return_value=([], 0))
        self.dataforseo_fn = MagicMock(return_value=([], 0))
        self.linkedin_fn = MagicMock(return_value=([], 0))
        self.cse_fn = MagicMock(return_value=(
            [{"link": "https://linkedin.com/in/bob", "title": "Bob", "snippet": "", "displayLink": ""}],
            500,
        ))

    def cse_fallback_when_no_serper(self):
        cfg = {
            "serper": {"api_key": "", "enabled": "disabled"},
            "dataforseo": {"login": "", "password": "", "enabled": "disabled"},
        }
        results, total = unified_search_page(
            "data engineer site:linkedin.com", 10, 1,
            cfg=cfg, serper_fn=self.serper_fn, dataforseo_fn=self.dataforseo_fn, linkedin_fn=self.linkedin_fn, cse_fn=self.cse_fn,
        )
        self.cse_fn.assert_called_once()
        self.serper_fn.assert_not_called()
        self.dataforseo_fn.assert_not_called()
        self.linkedin_fn.assert_not_called()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["link"], "https://linkedin.com/in/bob")
        self.assertEqual(total, 500)

    def test_cse_fallback_when_no_serper(self):
        self.cse_fallback_when_no_serper()

    def test_serper_key_set_but_disabled_uses_cse(self):
        """Key present but toggle is 'disabled' → must still fall through to CSE."""
        cfg = {
            "serper": {"api_key": "some-key", "enabled": "disabled"},
            "dataforseo": {"login": "", "password": "", "enabled": "disabled"},
        }
        unified_search_page("q", 10, 1, cfg=cfg,
                            serper_fn=self.serper_fn, dataforseo_fn=self.dataforseo_fn, linkedin_fn=self.linkedin_fn, cse_fn=self.cse_fn)
        self.cse_fn.assert_called_once()
        self.serper_fn.assert_not_called()

    def test_serper_enabled_but_no_key_uses_cse(self):
        """Toggle is 'enabled' but no key → must fall through to CSE."""
        cfg = {
            "serper": {"api_key": "", "enabled": "enabled"},
            "dataforseo": {"login": "", "password": "", "enabled": "disabled"},
        }
        unified_search_page("q", 10, 1, cfg=cfg,
                            serper_fn=self.serper_fn, dataforseo_fn=self.dataforseo_fn, linkedin_fn=self.linkedin_fn, cse_fn=self.cse_fn)
        self.cse_fn.assert_called_once()
        self.serper_fn.assert_not_called()


class TestProviderLabelInMessages(unittest.TestCase):
    """Job messages reflect active provider: "Running Serper" / "Running DataforSEO" / "Running LinkedIn" / "Running CSE" """

    def provider_label_in_messages(self):
        # Serper enabled+keyed → label is "Serper"
        self.assertEqual(_get_provider_label({"serper": {"api_key": "key", "enabled": "enabled"},
                                              "dataforseo": {"login": "", "password": "", "enabled": "disabled"}}), "Serper")
        # DataforSEO enabled+credentialed (Serper off) → label is "DataforSEO"
        self.assertEqual(_get_provider_label({"serper": {"api_key": "", "enabled": "disabled"},
                                              "dataforseo": {"login": "u", "password": "p", "enabled": "enabled"}}), "DataforSEO")
        # LinkedIn enabled+keyed (others off) → label is "LinkedIn"
        self.assertEqual(_get_provider_label({"serper": {"api_key": "", "enabled": "disabled"},
                                              "dataforseo": {"login": "", "password": "", "enabled": "disabled"},
                                              "linkedin": {"api_key": "li-key", "enabled": "enabled"}}), "LinkedIn")
        # Both disabled → label is "CSE"
        self.assertEqual(_get_provider_label({"serper": {"api_key": "", "enabled": "disabled"},
                                              "dataforseo": {"login": "", "password": "", "enabled": "disabled"}}), "CSE")
        # Empty/missing config → label is "CSE"
        self.assertEqual(_get_provider_label({}), "CSE")

    def test_provider_label_in_messages(self):
        self.provider_label_in_messages()

    def test_message_format_running(self):
        """Formatted 'Running {provider}: ...' message uses correct provider name."""
        cases = [
            ({"serper": {"api_key": "k", "enabled": "enabled"}, "dataforseo": {"login": "", "password": "", "enabled": "disabled"}}, "Running Serper:"),
            ({"serper": {"api_key": "", "enabled": "disabled"}, "dataforseo": {"login": "u", "password": "p", "enabled": "enabled"}}, "Running DataforSEO:"),
            ({"serper": {"api_key": "k", "enabled": "disabled"}, "dataforseo": {"login": "", "password": "", "enabled": "disabled"}}, "Running CSE:"),
        ]
        for cfg, expected in cases:
            label = _get_provider_label(cfg)
            msg = f"Running {label}: test query target=50 (need 50 more to reach 50)"
            self.assertTrue(msg.startswith(expected),
                            f"Expected message to start with '{expected}', got: {msg!r}")

    def test_message_format_done(self):
        """Formatted '{provider} done (collected N)...' message uses correct provider name."""
        cases = [
            ({"serper": {"api_key": "k", "enabled": "enabled"}, "dataforseo": {"login": "", "password": "", "enabled": "disabled"}}, "Serper done"),
            ({"serper": {"api_key": "", "enabled": "disabled"}, "dataforseo": {"login": "u", "password": "p", "enabled": "enabled"}}, "DataforSEO done"),
            ({"serper": {"api_key": "", "enabled": "disabled"}, "dataforseo": {"login": "", "password": "", "enabled": "disabled"}}, "CSE done"),
        ]
        for cfg, expected_prefix in cases:
            label = _get_provider_label(cfg)
            msg = f"{label} done (collected 10). pages=1"
            self.assertTrue(msg.startswith(expected_prefix),
                            f"Expected message to start with '{expected_prefix}', got: {msg!r}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
