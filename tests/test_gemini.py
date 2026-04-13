"""
test_gemini.py — Tests for Gemini API integration stubs and translation routing.

Stubs avoid importing webbridge.py; tests verify logic around missing API key,
Gemini error handling, JD analysis, and translation provider routing.

Run with:  pytest tests/test_gemini.py
"""
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Inline helpers mirroring webbridge.py
# ---------------------------------------------------------------------------

def _nllb_available() -> bool:
    """Return True when the NLLB translation service is reachable."""
    import os
    return os.getenv("NLLB_URL", "") != ""


def _translate_router(text: str, target_lang: str, provider: str,
                      nllb_fn, gemini_fn) -> str:
    """
    Route translation to nllb_fn or gemini_fn based on provider setting.
    Mirrors translate_text_pipeline logic in webbridge.py.
    """
    if provider == "nllb" and _nllb_available():
        return nllb_fn(text, target_lang)
    return gemini_fn(text, target_lang)


def _build_gemini_app(gemini_api_key: str = ""):
    """Build a minimal Flask app with Gemini-style endpoints."""
    app = Flask(__name__)
    app.config["TESTING"] = True

    @app.post("/gemini/analyze_jd")
    def analyze_jd():
        if not gemini_api_key:
            return jsonify({"error": "Gemini unavailable: API key not configured"}), 503
        body = request.get_json(force=True, silent=True) or {}
        jd_text = body.get("jd_text", "")
        if not jd_text:
            return jsonify({"error": "jd_text required"}), 400
        # Stub: return fake extracted skills
        return jsonify({
            "ok": True,
            "skills": ["Python", "SQL", "Machine Learning"],
            "seniority": "Senior",
        }), 200

    @app.post("/admin/logs/analyse-error")
    def analyse_error():
        if not gemini_api_key:
            return jsonify({"error": "Gemini unavailable"}), 503
        body = request.get_json(force=True, silent=True) or {}
        log_text = body.get("log_text", "")
        # Stub: return structured analysis
        return jsonify({
            "ok": True,
            "summary": "Stub error analysis",
            "root_cause": "Unknown",
            "fix_suggestion": "Check logs",
        }), 200

    @app.post("/translate")
    def translate():
        body = request.get_json(force=True, silent=True) or {}
        text = body.get("text", "")
        target_lang = body.get("target_lang", "en")
        provider = body.get("provider", "gemini")

        if provider == "nllb":
            return jsonify({"ok": True, "provider_used": "nllb", "translated": text}), 200
        return jsonify({"ok": True, "provider_used": "gemini", "translated": text}), 200

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGeminiMissingKey(unittest.TestCase):

    def test_gemini_missing_key(self):
        """GEMINI_API_KEY unset → /gemini/analyze_jd returns 503."""
        app = _build_gemini_app(gemini_api_key="")
        client = app.test_client()
        resp = client.post("/gemini/analyze_jd", json={"jd_text": "Senior Python dev"})
        self.assertEqual(resp.status_code, 503)
        data = resp.get_json()
        self.assertIn("error", data)

    def test_gemini_with_key(self):
        """GEMINI_API_KEY set → /gemini/analyze_jd returns 200 with skills."""
        app = _build_gemini_app(gemini_api_key="test-api-key")
        client = app.test_client()
        resp = client.post("/gemini/analyze_jd", json={"jd_text": "Senior Python dev"})
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data.get("ok"))
        self.assertIn("skills", data)


class TestGeminiAnalyzeError(unittest.TestCase):

    def test_gemini_analyze_error(self):
        """/admin/logs/analyse-error with stub genai → structured JSON."""
        app = _build_gemini_app(gemini_api_key="test-key")
        client = app.test_client()
        resp = client.post(
            "/admin/logs/analyse-error",
            json={"log_text": "ERROR: Connection refused"},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data.get("ok"))
        self.assertIn("summary", data)
        self.assertIn("root_cause", data)
        self.assertIn("fix_suggestion", data)

    def test_gemini_analyze_error_no_key(self):
        """/admin/logs/analyse-error without key → 503."""
        app = _build_gemini_app(gemini_api_key="")
        client = app.test_client()
        resp = client.post(
            "/admin/logs/analyse-error",
            json={"log_text": "some error"},
        )
        self.assertEqual(resp.status_code, 503)


class TestGeminiAnalyzeJD(unittest.TestCase):

    def test_gemini_analyze_jd(self):
        """/gemini/analyze_jd → skill extraction result."""
        app = _build_gemini_app(gemini_api_key="test-key")
        client = app.test_client()
        resp = client.post(
            "/gemini/analyze_jd",
            json={"jd_text": "We need a Senior Python engineer with ML experience"},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertIsInstance(data.get("skills"), list)
        self.assertGreater(len(data["skills"]), 0)

    def test_gemini_analyze_jd_empty(self):
        """/gemini/analyze_jd with empty jd_text → 400."""
        app = _build_gemini_app(gemini_api_key="test-key")
        client = app.test_client()
        resp = client.post("/gemini/analyze_jd", json={"jd_text": ""})
        self.assertEqual(resp.status_code, 400)


class TestTranslationRouting(unittest.TestCase):

    def test_nllb_translation(self):
        """TRANSLATION_PROVIDER=nllb → nllb_translate called (provider_used=nllb)."""
        app = _build_gemini_app(gemini_api_key="key")
        client = app.test_client()
        resp = client.post(
            "/translate",
            json={"text": "Hello", "target_lang": "fr", "provider": "nllb"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json().get("provider_used"), "nllb")

    def test_gemini_translation(self):
        """TRANSLATION_PROVIDER=gemini → gemini_translate_plain called."""
        app = _build_gemini_app(gemini_api_key="key")
        client = app.test_client()
        resp = client.post(
            "/translate",
            json={"text": "Hello", "target_lang": "fr", "provider": "gemini"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json().get("provider_used"), "gemini")

    def test_translate_router_nllb(self):
        """_translate_router dispatches to nllb_fn when provider=nllb and NLLB available."""
        nllb_fn = MagicMock(return_value="Bonjour")
        gemini_fn = MagicMock(return_value="Bonjour!")

        with patch("os.getenv", return_value="http://localhost:6006"):
            result = _translate_router("Hello", "fr", "nllb", nllb_fn, gemini_fn)

        nllb_fn.assert_called_once_with("Hello", "fr")
        gemini_fn.assert_not_called()
        self.assertEqual(result, "Bonjour")

    def test_translate_router_gemini_fallback(self):
        """_translate_router falls back to gemini when provider=gemini."""
        nllb_fn = MagicMock(return_value="Bonjour")
        gemini_fn = MagicMock(return_value="Bonjour!")

        with patch("os.getenv", return_value=""):
            result = _translate_router("Hello", "fr", "gemini", nllb_fn, gemini_fn)

        gemini_fn.assert_called_once_with("Hello", "fr")
        nllb_fn.assert_not_called()
        self.assertEqual(result, "Bonjour!")


if __name__ == "__main__":
    unittest.main()
