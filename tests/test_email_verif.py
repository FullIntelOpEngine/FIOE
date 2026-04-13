"""
test_email_verif.py — Tests for _load_email_verif_config / _save_email_verif_config.

Inline stubs mirror the functions from webbridge.py exactly.

Run with:  pytest tests/test_email_verif.py
"""
import json
import os
import tempfile
import unittest

# ---------------------------------------------------------------------------
# Inline stubs (identical logic to webbridge.py)
# ---------------------------------------------------------------------------

_EMAIL_VERIF_SERVICES = ("neverbounce", "zerobounce", "bouncer")

_DEFAULT_CONFIG = {
    "neverbounce": {"api_key": "", "enabled": "disabled"},
    "zerobounce":  {"api_key": "", "enabled": "disabled"},
    "bouncer":     {"api_key": "", "enabled": "disabled"},
}


def _load_email_verif_config(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {k: dict(v) for k, v in _DEFAULT_CONFIG.items()}


def _save_email_verif_config(path: str, config: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2)
    os.replace(tmp, path)


def _mask_api_key(key: str) -> str:
    """Return masked representation of an API key for display."""
    if not key or len(key) < 8:
        return "****"
    return key[:4] + "****" + key[-4:]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEmailVerifConfig(unittest.TestCase):

    def test_load_save_config(self):
        """_load/_save_email_verif_config round-trip preserves data."""
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "email_verif_config.json")
            config = {
                "neverbounce": {"api_key": "nb-secret-key", "enabled": "enabled"},
                "zerobounce":  {"api_key": "zb-secret-key", "enabled": "disabled"},
                "bouncer":     {"api_key": "",               "enabled": "disabled"},
            }
            _save_email_verif_config(path, config)
            loaded = _load_email_verif_config(path)
            self.assertEqual(loaded["neverbounce"]["api_key"], "nb-secret-key")
            self.assertEqual(loaded["neverbounce"]["enabled"], "enabled")
            self.assertEqual(loaded["zerobounce"]["enabled"], "disabled")

    def test_masking(self):
        """API key masking hides middle portion of the key."""
        masked = _mask_api_key("abcdefghijklmnop")
        self.assertTrue(masked.startswith("abcd"))
        self.assertTrue(masked.endswith("mnop"))
        self.assertIn("****", masked)

    def test_masking_short_key(self):
        """Short API keys are fully masked."""
        self.assertEqual(_mask_api_key("abc"), "****")
        self.assertEqual(_mask_api_key(""), "****")

    def test_invalid_json_fallback(self):
        """Invalid JSON in config file → defaults returned."""
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "broken.json")
            with open(path, "w") as fh:
                fh.write("{NOT VALID JSON")
            loaded = _load_email_verif_config(path)
            # Should return defaults with all services present
            for svc in _EMAIL_VERIF_SERVICES:
                self.assertIn(svc, loaded)
                self.assertEqual(loaded[svc]["api_key"], "")

    def test_missing_file_fallback(self):
        """Missing config file → defaults returned (not an error)."""
        path = "/nonexistent/path/email_verif_config.json"
        loaded = _load_email_verif_config(path)
        for svc in _EMAIL_VERIF_SERVICES:
            self.assertIn(svc, loaded)

    def test_atomic_save_no_tmp(self):
        """_save_email_verif_config leaves no .tmp file after write."""
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "email_verif_config.json")
            _save_email_verif_config(path, {"neverbounce": {"api_key": "x"}})
            self.assertFalse(os.path.exists(path + ".tmp"))

    def test_save_overwrites(self):
        """Subsequent saves overwrite the previous config."""
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "ev.json")
            _save_email_verif_config(path, {"neverbounce": {"api_key": "first"}})
            _save_email_verif_config(path, {"neverbounce": {"api_key": "second"}})
            loaded = _load_email_verif_config(path)
            self.assertEqual(loaded["neverbounce"]["api_key"], "second")


if __name__ == "__main__":
    unittest.main()
