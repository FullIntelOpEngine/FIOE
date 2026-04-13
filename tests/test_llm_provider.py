"""
Unit tests for LLM Provider (Gemini / ChatGPT / Claude) helpers.

Covers:
  - _load_llm_provider_config / _save_llm_provider_config  (read/write + defaults)
  - unified_llm_call_text routing:
      • Gemini when active_provider == 'gemini'
      • OpenAI when active_provider == 'openai' + key set + enabled
      • Anthropic when active_provider == 'anthropic' + key set + enabled
      • Gemini fallback when active_provider is unknown
  - gemini_active_routing      — Gemini config routes to gemini_call_text
  - openai_active_routing      — OpenAI config routes to openai_call_text
  - anthropic_active_routing   — Anthropic config routes to anthropic_call_text
  - gemini_fallback             — Unknown / unconfigured provider falls back to Gemini
  - mutual_exclusion_enforcement — POST logic: enabling one provider disables others
  - invalid_model_rejected      — Model name not in allowed list is rejected

NOTE: These tests use self-contained stubs that mirror production functions
without importing the full Flask app (avoids side-effects and heavy dependencies).

Run with: pytest tests/test_llm_provider.py
"""

import copy
import json
import os
import tempfile
import unittest

# ---------------------------------------------------------------------------
# Allowed models (mirrors webbridge._ALLOWED_LLM_MODELS)
# ---------------------------------------------------------------------------

_ALLOWED_LLM_MODELS = {
    "gemini": [
        "gemini-3.1-pro", "gemini-3-flash", "gemini-3.1-flash-lite",
        "gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite",
        "gemini-2.0-flash", "gemini-2.0-flash-lite",
    ],
    "openai": [
        "gpt-5.4", "gpt-5.4-mini", "gpt-5.4-nano",
        "gpt-4.5",
        "gpt-4.1", "gpt-4.1-mini", "gpt-4.1-nano",
        "gpt-4o", "gpt-4o-mini",
        "gpt-4-turbo",
        "o3", "o4-mini",
        "o1", "o1-mini",
    ],
    "anthropic": [
        "claude-opus-4-6", "claude-sonnet-4-6",
        "claude-opus-4-20250514", "claude-sonnet-4-20250514",
        "claude-haiku-4-5-20251001",
        "claude-opus-4-5", "claude-sonnet-4-5",
        "claude-3-7-sonnet-20250219",
        "claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022",
        "claude-3-opus-20240229",
    ],
}

_LLM_PROVIDER_DEFAULTS = {
    "active_provider": "gemini",
    "default_model": "gemini-2.5-flash-lite",
    "gemini": {
        "api_key": "",
        "model": "gemini-2.5-flash-lite",
        "enabled": "enabled",
    },
    "openai": {
        "api_key": "",
        "model": "gpt-4o-mini",
        "enabled": "disabled",
    },
    "anthropic": {
        "api_key": "",
        "model": "claude-3-5-haiku-20241022",
        "enabled": "disabled",
    },
}

# ---------------------------------------------------------------------------
# Stubs — replicate production functions
# ---------------------------------------------------------------------------

def _load_llm_provider_config(path):
    """Stub: load LLM provider config from an explicit path."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
        for k, v in _LLM_PROVIDER_DEFAULTS.items():
            if k not in cfg:
                cfg[k] = copy.deepcopy(v)
            elif isinstance(v, dict):
                for sk, sv in v.items():
                    if sk not in cfg[k]:
                        cfg[k][sk] = sv
        return cfg
    except Exception:
        return copy.deepcopy(_LLM_PROVIDER_DEFAULTS)


def _save_llm_provider_config(config, path):
    """Stub: atomically write LLM provider config to explicit path."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2)
    os.replace(tmp, path)


def unified_llm_call_text(prompt, cfg,
                          gemini_fn=None, openai_fn=None, anthropic_fn=None,
                          fallback_key=None, fallback_model="gemini-2.5-flash-lite"):
    """
    Stub of webbridge_routes.unified_llm_call_text.

    Routes to the provider indicated by cfg['active_provider'], using
    the injected adapter functions (gemini_fn / openai_fn / anthropic_fn).
    Returns the result of the called adapter, or None if no suitable provider.
    """
    active = cfg.get("active_provider", "gemini")

    if active == "openai":
        oai = cfg.get("openai", {})
        key = (oai.get("api_key") or "").strip()
        model = oai.get("model", "gpt-4o-mini")
        if key and oai.get("enabled") == "enabled" and openai_fn:
            result = openai_fn(prompt, key, model)
            if result is not None:
                return result

    if active == "anthropic":
        ant = cfg.get("anthropic", {})
        key = (ant.get("api_key") or "").strip()
        model = ant.get("model", "claude-3-5-haiku-20241022")
        if key and ant.get("enabled") == "enabled" and anthropic_fn:
            result = anthropic_fn(prompt, key, model)
            if result is not None:
                return result

    # Gemini path (default / fallback)
    gem = cfg.get("gemini", {})
    gem_key = (gem.get("api_key") or "").strip() or (fallback_key or "").strip()
    gem_model = gem.get("model", fallback_model)
    if gem_key and gemini_fn:
        return gemini_fn(prompt, gem_key, gem_model)

    return None


def _apply_mutual_exclusion(current, provider_being_enabled):
    """Stub: mirrors POST /admin/llm-provider-config mutual-exclusion logic."""
    for p in ("gemini", "openai", "anthropic"):
        if p not in current:
            current[p] = copy.deepcopy(_LLM_PROVIDER_DEFAULTS[p])
    current[provider_being_enabled]["enabled"] = "enabled"
    current["active_provider"] = provider_being_enabled
    for other in ("gemini", "openai", "anthropic"):
        if other != provider_being_enabled:
            current[other]["enabled"] = "disabled"
    return current


def _validate_model(provider, model):
    """Returns True if model is allowed for provider."""
    return model in _ALLOWED_LLM_MODELS.get(provider, [])


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------

class TestLoadSaveLlmProviderConfig(unittest.TestCase):
    """_load_llm_provider_config / _save_llm_provider_config read/write."""

    def test_defaults_on_missing_file(self):
        cfg = _load_llm_provider_config("/nonexistent/path/llm_provider_config.json")
        self.assertEqual(cfg["active_provider"], "gemini")
        self.assertEqual(cfg["gemini"]["model"], "gemini-2.5-flash-lite")
        self.assertIn("openai", cfg)
        self.assertIn("anthropic", cfg)

    def test_round_trip(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            f.write("{}")
            path = f.name
        try:
            data = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
            data["gemini"]["model"] = "gemini-2.5-pro"
            data["active_provider"] = "openai"
            _save_llm_provider_config(data, path)
            loaded = _load_llm_provider_config(path)
            self.assertEqual(loaded["active_provider"], "openai")
            self.assertEqual(loaded["gemini"]["model"], "gemini-2.5-pro")
        finally:
            os.unlink(path)

    def test_missing_keys_filled_with_defaults(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            json.dump({"active_provider": "openai"}, f)
            path = f.name
        try:
            cfg = _load_llm_provider_config(path)
            # gemini block should be filled from defaults
            self.assertIn("gemini", cfg)
            self.assertEqual(cfg["gemini"]["model"], "gemini-2.5-flash-lite")
        finally:
            os.unlink(path)

    def test_atomic_write_no_tmp_leftover(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "llm_provider_config.json")
            _save_llm_provider_config(copy.deepcopy(_LLM_PROVIDER_DEFAULTS), path)
            self.assertTrue(os.path.exists(path))
            self.assertFalse(os.path.exists(path + ".tmp"))


class TestGeminiActiveRouting(unittest.TestCase):
    """unified_llm_call_text: Gemini active → calls gemini_call_text."""

    def test_gemini_active_routing(self):
        cfg = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        cfg["active_provider"] = "gemini"
        cfg["gemini"]["api_key"] = "gem-key"
        cfg["gemini"]["enabled"] = "enabled"

        calls = []

        def fake_gemini(prompt, key, model):
            calls.append(("gemini", prompt, key, model))
            return "gemini-result"

        result = unified_llm_call_text("hello", cfg, gemini_fn=fake_gemini)
        self.assertEqual(result, "gemini-result")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "gemini")
        self.assertEqual(calls[0][2], "gem-key")

    def test_gemini_uses_configured_model(self):
        cfg = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        cfg["active_provider"] = "gemini"
        cfg["gemini"]["api_key"] = "gem-key"
        cfg["gemini"]["model"] = "gemini-2.5-pro"

        captured = []

        def fake_gemini(prompt, key, model):
            captured.append(model)
            return "ok"

        unified_llm_call_text("test", cfg, gemini_fn=fake_gemini)
        self.assertEqual(captured[0], "gemini-2.5-pro")


class TestOpenAIActiveRouting(unittest.TestCase):
    """unified_llm_call_text: OpenAI active + key set → calls openai_call_text."""

    def test_openai_active_routing(self):
        cfg = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        cfg["active_provider"] = "openai"
        cfg["openai"]["api_key"] = "oai-key"
        cfg["openai"]["enabled"] = "enabled"
        cfg["openai"]["model"] = "gpt-4o-mini"

        calls = []

        def fake_openai(prompt, key, model):
            calls.append(("openai", prompt, key, model))
            return "openai-result"

        result = unified_llm_call_text("hello", cfg, openai_fn=fake_openai)
        self.assertEqual(result, "openai-result")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][2], "oai-key")

    def test_openai_missing_key_falls_through_to_gemini(self):
        """OpenAI enabled but no key → falls through to Gemini fallback."""
        cfg = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        cfg["active_provider"] = "openai"
        cfg["openai"]["api_key"] = ""   # no key
        cfg["openai"]["enabled"] = "enabled"
        cfg["gemini"]["api_key"] = "gem-key"

        calls = []

        def fake_openai(prompt, key, model):
            calls.append("openai")
            return "openai-result"

        def fake_gemini(prompt, key, model):
            calls.append("gemini")
            return "gemini-result"

        result = unified_llm_call_text("hello", cfg, openai_fn=fake_openai, gemini_fn=fake_gemini)
        self.assertEqual(result, "gemini-result")
        self.assertNotIn("openai", calls)
        self.assertIn("gemini", calls)

    def test_openai_disabled_falls_through(self):
        """OpenAI key set but disabled → falls through."""
        cfg = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        cfg["active_provider"] = "openai"
        cfg["openai"]["api_key"] = "oai-key"
        cfg["openai"]["enabled"] = "disabled"
        cfg["gemini"]["api_key"] = "gem-key"

        calls = []
        def fake_openai(prompt, key, model):
            calls.append("openai")
            return "openai-result"
        def fake_gemini(prompt, key, model):
            calls.append("gemini")
            return "gemini-result"

        result = unified_llm_call_text("q", cfg, openai_fn=fake_openai, gemini_fn=fake_gemini)
        self.assertEqual(result, "gemini-result")
        self.assertNotIn("openai", calls)


class TestAnthropicActiveRouting(unittest.TestCase):
    """unified_llm_call_text: Anthropic active + key set → calls anthropic_call_text."""

    def test_anthropic_active_routing(self):
        cfg = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        cfg["active_provider"] = "anthropic"
        cfg["anthropic"]["api_key"] = "ant-key"
        cfg["anthropic"]["enabled"] = "enabled"
        cfg["anthropic"]["model"] = "claude-3-5-haiku-20241022"

        calls = []

        def fake_anthropic(prompt, key, model):
            calls.append(("anthropic", prompt, key, model))
            return "claude-result"

        result = unified_llm_call_text("hello", cfg, anthropic_fn=fake_anthropic)
        self.assertEqual(result, "claude-result")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][2], "ant-key")

    def test_anthropic_missing_key_falls_through_to_gemini(self):
        cfg = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        cfg["active_provider"] = "anthropic"
        cfg["anthropic"]["api_key"] = ""
        cfg["anthropic"]["enabled"] = "enabled"
        cfg["gemini"]["api_key"] = "gem-key"

        calls = []
        def fake_anthropic(prompt, key, model):
            calls.append("anthropic")
            return "claude-result"
        def fake_gemini(prompt, key, model):
            calls.append("gemini")
            return "gemini-result"

        result = unified_llm_call_text("q", cfg, anthropic_fn=fake_anthropic, gemini_fn=fake_gemini)
        self.assertEqual(result, "gemini-result")
        self.assertNotIn("anthropic", calls)


class TestGeminiFallback(unittest.TestCase):
    """unified_llm_call_text: fallback to Gemini when provider unavailable."""

    def test_no_provider_returns_none(self):
        cfg = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        cfg["active_provider"] = "gemini"
        cfg["gemini"]["api_key"] = ""   # no key, no fallback

        result = unified_llm_call_text("q", cfg, gemini_fn=None)
        self.assertIsNone(result)

    def test_gemini_fallback_when_unknown_provider(self):
        """active_provider set to unknown value → falls back to Gemini."""
        cfg = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        cfg["active_provider"] = "unknown_provider"
        cfg["gemini"]["api_key"] = "gem-key"

        calls = []
        def fake_gemini(prompt, key, model): calls.append("gemini"); return "gemini-result"

        result = unified_llm_call_text("q", cfg, gemini_fn=fake_gemini)
        self.assertEqual(result, "gemini-result")
        self.assertIn("gemini", calls)

    def test_fallback_key_used_when_gemini_cfg_key_empty(self):
        """GEMINI_API_KEY env fallback is used when config has no gemini api_key."""
        cfg = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        cfg["active_provider"] = "gemini"
        cfg["gemini"]["api_key"] = ""

        calls = []
        def fake_gemini(prompt, key, model): calls.append(key); return "ok"

        result = unified_llm_call_text("q", cfg, gemini_fn=fake_gemini, fallback_key="env-gemini-key")
        self.assertEqual(result, "ok")
        self.assertEqual(calls[0], "env-gemini-key")


class TestMutualExclusionEnforcement(unittest.TestCase):
    """Enabling one LLM provider disables the others."""

    def test_enabling_openai_disables_gemini_and_anthropic(self):
        current = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        current["gemini"]["enabled"] = "enabled"
        current["anthropic"]["enabled"] = "enabled"
        result = _apply_mutual_exclusion(current, "openai")
        self.assertEqual(result["openai"]["enabled"], "enabled")
        self.assertEqual(result["gemini"]["enabled"], "disabled")
        self.assertEqual(result["anthropic"]["enabled"], "disabled")
        self.assertEqual(result["active_provider"], "openai")

    def test_enabling_anthropic_disables_gemini_and_openai(self):
        current = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        current["openai"]["enabled"] = "enabled"
        result = _apply_mutual_exclusion(current, "anthropic")
        self.assertEqual(result["anthropic"]["enabled"], "enabled")
        self.assertEqual(result["gemini"]["enabled"], "disabled")
        self.assertEqual(result["openai"]["enabled"], "disabled")
        self.assertEqual(result["active_provider"], "anthropic")

    def test_enabling_gemini_disables_openai_and_anthropic(self):
        current = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
        current["openai"]["enabled"] = "enabled"
        current["anthropic"]["enabled"] = "enabled"
        result = _apply_mutual_exclusion(current, "gemini")
        self.assertEqual(result["gemini"]["enabled"], "enabled")
        self.assertEqual(result["openai"]["enabled"], "disabled")
        self.assertEqual(result["anthropic"]["enabled"], "disabled")

    def test_mutual_exclusion_enforcement(self):
        """Verifies mutual exclusion enforcement: enabling one provider automatically disables all others."""
        for provider in ("gemini", "openai", "anthropic"):
            with self.subTest(provider=provider):
                current = copy.deepcopy(_LLM_PROVIDER_DEFAULTS)
                # Pre-enable all three to stress test disabling
                current["gemini"]["enabled"] = "enabled"
                current["openai"]["enabled"] = "enabled"
                current["anthropic"]["enabled"] = "enabled"
                result = _apply_mutual_exclusion(current, provider)
                self.assertEqual(result[provider]["enabled"], "enabled")
                self.assertEqual(result["active_provider"], provider)
                for other in ("gemini", "openai", "anthropic"):
                    if other != provider:
                        self.assertEqual(
                            result[other]["enabled"], "disabled",
                            f"{other} should be disabled when {provider} is active"
                        )


class TestInvalidModelRejected(unittest.TestCase):
    """Invalid model names are rejected by the validation function."""

    def test_valid_gemini_models_accepted(self):
        for m in _ALLOWED_LLM_MODELS["gemini"]:
            self.assertTrue(_validate_model("gemini", m), f"Expected {m} to be valid for gemini")

    def test_valid_openai_models_accepted(self):
        for m in _ALLOWED_LLM_MODELS["openai"]:
            self.assertTrue(_validate_model("openai", m))

    def test_valid_anthropic_models_accepted(self):
        for m in _ALLOWED_LLM_MODELS["anthropic"]:
            self.assertTrue(_validate_model("anthropic", m))

    def test_invalid_model_rejected(self):
        self.assertFalse(_validate_model("gemini", "gpt-4o"))
        self.assertFalse(_validate_model("openai", "gemini-2.5-pro"))
        self.assertFalse(_validate_model("anthropic", "gpt-3.5-turbo"))
        self.assertFalse(_validate_model("gemini", "some-fake-model"))

    def test_empty_model_rejected(self):
        self.assertFalse(_validate_model("gemini", ""))
        self.assertFalse(_validate_model("openai", ""))

    def test_unknown_provider_returns_false(self):
        self.assertFalse(_validate_model("unknown", "gpt-4o"))


if __name__ == "__main__":
    unittest.main()
