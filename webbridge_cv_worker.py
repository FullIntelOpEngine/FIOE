# webbridge_cv_worker.py
# ---------------------------------------------------------------------------
# Standalone CV-analysis worker module.
#
# IMPORTANT: This module MUST NOT import webbridge, webbridge_cv,
# webbridge_routes, or Flask.  It is loaded by ProcessPoolExecutor worker
# processes (via the "spawn" start method) and needs to start up quickly
# without triggering a full Flask app init, DB connection-pool setup, or
# any other heavyweight initialisation.
#
# All required config is read from environment variables, which are
# automatically inherited by spawned child processes.
#
# Public API used by webbridge_cv.py:
#   worker_process_init()              -- ProcessPoolExecutor initializer
#   analyze_cv_bytes_worker_entry(pdf_bytes) -> dict | None
# ---------------------------------------------------------------------------

import copy
import io
import json
import logging
import os
import re
import time
from datetime import datetime

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — read once per worker process from inherited env vars
# ---------------------------------------------------------------------------
_BASE_DIR: str = os.path.dirname(os.path.abspath(__file__))

GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
GEMINI_SUGGEST_MODEL: str = os.getenv("GEMINI_SUGGEST_MODEL", "gemini-2.5-flash-lite")

CV_ANALYSIS_MAX_CHARS: int = int(os.getenv("CV_ANALYSIS_MAX_CHARS", "") or 15000)
CV_TRANSLATION_MAX_CHARS: int = int(os.getenv("CV_TRANSLATION_MAX_CHARS", "") or 10000)
LANG_DETECTION_SAMPLE_LENGTH: int = int(os.getenv("LANG_DETECTION_SAMPLE_LENGTH", "") or 1000)

TRANSLATION_ENABLED: bool = os.getenv("TRANSLATION_ENABLED", "1") != "0"
TRANSLATION_PROVIDER: str = (os.getenv("TRANSLATION_PROVIDER", "auto") or "auto").lower()
TRANSLATOR_BASE: str = (os.getenv("TRANSLATOR_BASE", "") or "").rstrip("/")
NLLB_TIMEOUT: float = float(os.getenv("NLLB_TIMEOUT", "") or 10.0)

_LLM_PROVIDER_CONFIG_PATH: str = os.path.join(_BASE_DIR, "llm_provider_config.json")

_LLM_PROVIDER_DEFAULTS: dict = {
    "active_provider": "gemini",
    "default_model": "gemini-2.5-flash-lite",
    "gemini": {"api_key": "", "model": "gemini-2.5-flash-lite", "enabled": "enabled"},
    "openai": {"api_key": "", "model": "gpt-4o-mini", "enabled": "disabled"},
    "anthropic": {"api_key": "", "model": "claude-3-5-haiku-20241022", "enabled": "disabled"},
}

# Module-level Gemini client, initialised once per worker process by
# worker_process_init(); lazily created on first use as a fallback.
_genai_model = None

# ---------------------------------------------------------------------------
# Process-pool initializer
# ---------------------------------------------------------------------------

def worker_process_init() -> None:
    """ProcessPoolExecutor initializer: pre-import heavy libs once per worker
    process, and configure the Gemini client so the first real request doesn't
    pay any setup cost."""
    global _genai_model
    # Pre-warm pypdf import
    try:
        import pypdf  # noqa: F401
    except ImportError:
        pass
    # Pre-warm pdfplumber import (optional heavier PDF lib)
    try:
        import pdfplumber  # noqa: F401
    except ImportError:
        pass
    # Configure Gemini client once for this process
    _genai_model = _build_genai_model()


# ---------------------------------------------------------------------------
# LLM helpers (standalone — no webbridge dependency)
# ---------------------------------------------------------------------------

def _load_llm_provider_config() -> dict:
    """Return parsed llm_provider_config.json; falls back to defaults."""
    try:
        with open(_LLM_PROVIDER_CONFIG_PATH, "r", encoding="utf-8") as fh:
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


def _build_genai_model():
    """Create and return a google.generativeai GenerativeModel, or None."""
    try:
        import google.generativeai as _genai  # type: ignore
        cfg = _load_llm_provider_config()
        gem = cfg.get("gemini", {})
        key = (gem.get("api_key") or "").strip() or GEMINI_API_KEY.strip()
        model_name = gem.get("model", GEMINI_SUGGEST_MODEL)
        if key:
            _genai.configure(api_key=key)
            return _genai.GenerativeModel(model_name)
    except Exception as exc:
        logger.warning("[worker] Could not build genai model: %s", exc)
    return None


def _gemini_call_text(prompt: str, temperature: float = None,
                      max_output_tokens: int = None) -> "str | None":
    """Direct Gemini call using the per-process cached model."""
    global _genai_model
    if _genai_model is None:
        _genai_model = _build_genai_model()
    if _genai_model is None:
        return None
    try:
        gen_cfg: dict = {}
        if temperature is not None:
            gen_cfg["temperature"] = temperature
        if max_output_tokens is not None:
            gen_cfg["max_output_tokens"] = max_output_tokens
        resp = _genai_model.generate_content(
            prompt,
            generation_config=gen_cfg if gen_cfg else None,
        )
        return resp.text
    except Exception as exc:
        logger.warning("[worker] Gemini call failed: %s", exc)
        return None


def _openai_call_text(prompt: str, api_key: str, model: str,
                      temperature: float = None,
                      max_output_tokens: int = None) -> "str | None":
    """Direct OpenAI call (no caching)."""
    try:
        import openai as _openai  # type: ignore
    except ImportError:
        return None
    try:
        client = _openai.OpenAI(api_key=api_key.strip())
        kwargs: dict = {"model": model, "messages": [{"role": "user", "content": prompt}]}
        if temperature is not None:
            kwargs["temperature"] = temperature
        if max_output_tokens is not None:
            kwargs["max_tokens"] = max_output_tokens
        resp = client.chat.completions.create(**kwargs)
        return resp.choices[0].message.content
    except Exception as exc:
        logger.warning("[worker] OpenAI call failed: %s", exc)
        return None


def _anthropic_call_text(prompt: str, api_key: str, model: str,
                         temperature: float = None,
                         max_output_tokens: int = None) -> "str | None":
    """Direct Anthropic call (no caching)."""
    try:
        import anthropic as _anthropic  # type: ignore
    except ImportError:
        return None
    try:
        client = _anthropic.Anthropic(api_key=api_key.strip())
        kwargs: dict = {
            "model": model,
            "max_tokens": max_output_tokens if max_output_tokens is not None else 4096,
            "messages": [{"role": "user", "content": prompt}],
        }
        if temperature is not None:
            kwargs["temperature"] = temperature
        resp = client.messages.create(**kwargs)
        return resp.content[0].text
    except Exception as exc:
        logger.warning("[worker] Anthropic call failed: %s", exc)
        return None


def _worker_llm_call(prompt: str, temperature: float = None,
                     max_output_tokens: int = None) -> "str | None":
    """Route an LLM call through the active provider from llm_provider_config.json.
    No caching — worker processes are stateless and short-lived."""
    cfg = _load_llm_provider_config()
    active = cfg.get("active_provider", "gemini")

    if active == "openai":
        oai = cfg.get("openai", {})
        key = (oai.get("api_key") or "").strip()
        model = oai.get("model", "gpt-4o-mini")
        if key and oai.get("enabled") == "enabled":
            result = _openai_call_text(prompt, key, model, temperature, max_output_tokens)
            if result is not None:
                return result

    if active == "anthropic":
        ant = cfg.get("anthropic", {})
        key = (ant.get("api_key") or "").strip()
        model = ant.get("model", "claude-3-5-haiku-20241022")
        if key and ant.get("enabled") == "enabled":
            result = _anthropic_call_text(prompt, key, model, temperature, max_output_tokens)
            if result is not None:
                return result

    # Gemini path (default / fallback)
    return _gemini_call_text(prompt, temperature, max_output_tokens)


# ---------------------------------------------------------------------------
# Translation helpers (standalone)
# ---------------------------------------------------------------------------

def _nllb_available() -> bool:
    return bool(TRANSLATION_ENABLED and TRANSLATOR_BASE)


_NLLB_LANG: dict = {
    "en": "eng_Latn", "fr": "fra_Latn", "de": "deu_Latn", "es": "spa_Latn",
    "it": "ita_Latn", "pt": "por_Latn", "ja": "jpn_Jpan", "zh": "zho_Hans",
    "zh-hans": "zho_Hans", "zh-hant": "zho_Hant", "nl": "nld_Latn",
    "pl": "pol_Latn", "cs": "ces_Latn", "ru": "rus_Cyrl", "ko": "kor_Hang",
    "vi": "vie_Latn", "th": "tha_Thai", "sv": "swe_Latn", "no": "nob_Latn",
    "da": "dan_Latn", "fi": "fin_Latn", "tr": "tur_Latn",
}


def _map_lang_nllb(code: str, default: str) -> str:
    c = (code or "").strip().lower()
    return _NLLB_LANG.get(c) or _NLLB_LANG.get(default.lower()) or "eng_Latn"


def _nllb_translate(text: str, src_lang: str, tgt_lang: str) -> "str | None":
    if not _nllb_available():
        return None
    try:
        import requests as _requests
        url = f"{TRANSLATOR_BASE}/translate"
        payload = {
            "text": text,
            "src": _map_lang_nllb(src_lang or "en", "en"),
            "tgt": _map_lang_nllb(tgt_lang or "en", "en"),
            "max_length": 200,
        }
        r = _requests.post(url, json=payload, timeout=NLLB_TIMEOUT)
        if r.status_code != 200:
            return None
        return (r.json().get("translation") or "").strip() or None
    except Exception:
        return None


def _worker_translate(text: str, target_lang: str,
                      source_lang: str = "en") -> dict:
    """Standalone translate_text_pipeline for worker processes."""
    if not TRANSLATION_ENABLED or not text or not target_lang:
        return {"translated": text, "engine": "disabled", "status": "unchanged"}
    if TRANSLATION_PROVIDER in ("nllb", "auto") and _nllb_available():
        out = _nllb_translate(text, source_lang, target_lang)
        if out:
            status = "translated" if out.lower() != text.lower() else "unchanged"
            return {"translated": out, "engine": "nllb", "status": status}
        if TRANSLATION_PROVIDER == "nllb":
            return {"translated": text, "engine": "nllb", "status": "fallback_original"}
    # LLM translation via worker LLM call
    try:
        prompt = (
            f"Translate from {source_lang} to {target_lang}. "
            "Keep proper nouns if commonly untranslated. Output only the final text.\n\n"
            f"{text}"
        )
        out = (_worker_llm_call(prompt) or "").strip()
        out = re.sub(r'^\s*["""\'`]+|["""\'`]+\s*$', "", out)
        if out:
            status = "translated" if out.lower() != text.lower() else "unchanged"
            return {"translated": out, "engine": "llm", "status": status}
    except Exception as exc:
        logger.warning("[worker] LLM translation failed: %s", exc)
    return {"translated": text, "engine": "fallback", "status": "unchanged"}


# ---------------------------------------------------------------------------
# Pure-Python CV helpers (copied from webbridge_cv.py / webbridge.py;
# no webbridge imports allowed here)
# ---------------------------------------------------------------------------

def _extract_json_object(text: str):
    if not text:
        return None
    s = text.strip()
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(s[start:end + 1])
        except Exception:
            return None
    return None


def _strip_level_suffix(seniority: str) -> str:
    if not seniority:
        return ""
    return re.sub(r"-level$", "", seniority).strip()


def _normalize_seniority_to_8_levels(seniority_text: str,
                                      total_experience_years=None) -> str:
    if not seniority_text:
        if total_experience_years is not None:
            try:
                y = float(total_experience_years)
                if y < 2:
                    return "Junior-level"
                elif y < 5:
                    return "Mid-level"
                elif y < 8:
                    return "Senior-level"
                elif y < 12:
                    return "Lead-level"
                else:
                    return "Expert-level"
            except Exception:
                pass
        return ""

    s = str(seniority_text).strip().lower()
    _exp_years = None
    if total_experience_years is not None:
        try:
            _exp_years = float(total_experience_years)
        except Exception:
            pass

    exact = {
        "junior-level": "Junior-level", "mid-level": "Mid-level",
        "senior-level": "Senior-level", "lead-level": "Lead-level",
        "manager-level": "Manager-level", "expert-level": "Expert-level",
        "director-level": "Director-level", "executive-level": "Executive-level",
    }
    if s in exact:
        return exact[s]

    def _kw(kw, txt):
        return bool(re.search(r"\b" + re.escape(kw) + r"\b", txt))

    for kw in ["executive", "ceo", "cto", "cfo", "coo", "cxo", "chief",
               "president", "vp", "vice president", "c-level", "founder"]:
        if _kw(kw, s):
            return "Executive-level"
    for kw in ["director", "head of", "group director"]:
        if _kw(kw, s):
            return "Director-level"
    for kw in ["expert", "principal", "staff", "distinguished", "fellow", "architect"]:
        if _kw(kw, s):
            return "Expert-level"
    for kw in ["manager", "mgr", "supervisor", "team lead"]:
        if _kw(kw, s):
            return "Manager-level"
    if _kw("lead", s):
        return "Lead-level"
    if _kw("senior", s):
        return "Senior-level"
    for kw in ["mid", "intermediate", "associate", "specialist"]:
        if _kw(kw, s):
            return "Mid-level"
    if _kw("coordinator", s):
        return "Junior-level"
    for kw in ["junior", "entry", "trainee", "intern", "graduate", "jr", "assistant"]:
        if _kw(kw, s):
            return "Junior-level"

    if _exp_years is not None:
        if _exp_years < 2:
            return "Junior-level"
        elif _exp_years < 5:
            return "Mid-level"
        elif _exp_years < 8:
            return "Senior-level"
        elif _exp_years < 12:
            return "Lead-level"
        else:
            return "Expert-level"
    return ""


def _is_internship_role(job_title) -> bool:
    if not job_title:
        return False
    return bool(re.search(r"\bintern\b|\binternship\b", job_title, re.IGNORECASE))


def _normalize_company_name(company_name) -> "str | None":
    if not company_name:
        return None
    normalized = company_name.lower().strip()
    normalized = re.sub(
        r"\s+(inc\.?|llc\.?|ltd\.?|corp\.?|corporation|company|co\.?|limited|group|plc)$",
        "",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = normalized.strip()
    return normalized if normalized else None


def _recalculate_tenure_and_experience(experience_list: list) -> dict:
    empty = {"total_experience_years": 0.0, "baseline_years": 0.0,
             "tenure": 0.0, "employer_count": 0, "total_roles": 0}
    if not experience_list or not isinstance(experience_list, list):
        return empty

    current_year = datetime.now().year
    all_periods: list = []
    employer_periods: dict = {}
    total_roles = len(experience_list)

    for entry in experience_list:
        if not entry or not isinstance(entry, str):
            continue
        parts = [p.strip() for p in entry.split(",")]
        if len(parts) < 3:
            continue
        job_title = parts[0]
        company = parts[1]
        duration_str = ", ".join(parts[2:])
        is_intern = _is_internship_role(job_title)

        m = re.search(
            r"(?:\w+\s+)?(\d{4})\s*(?:to|[-\u2013\u2014])\s*(?:\w+\s+)?(present|\d{4})",
            duration_str,
            re.IGNORECASE,
        )
        if not m:
            years_found = re.findall(r"\b(\d{4})\b", duration_str)
            present_in_str = bool(re.search(r"\bpresent\b", duration_str, re.IGNORECASE))
            if len(years_found) >= 2:
                start_year, end_year = int(years_found[0]), int(years_found[-1])
            elif len(years_found) == 1 and present_in_str:
                start_year, end_year = int(years_found[0]), current_year
            else:
                continue
        else:
            start_year = int(m.group(1))
            ep = m.group(2).lower()
            end_year = current_year if ep == "present" else int(ep)

        if start_year < 1950 or start_year > current_year:
            continue
        if end_year < start_year or (end_year - start_year) > 50:
            continue

        if not is_intern:
            nc = _normalize_company_name(company)
            if nc:
                employer_periods.setdefault(nc, []).append((start_year, end_year))
                all_periods.append((start_year, end_year))

    if all_periods:
        baseline_years = float(current_year - min(s for s, _ in all_periods))
    else:
        baseline_years = 0.0

    all_periods.sort()
    merged_global: list = []
    for start, end in all_periods:
        if merged_global and start <= merged_global[-1][1]:
            merged_global[-1] = (merged_global[-1][0], max(merged_global[-1][1], end))
        else:
            merged_global.append((start, end))
    total_experience = sum(e - s for s, e in merged_global)

    all_emp_merged: list = []
    for _emp_periods in employer_periods.values():
        _sorted = sorted(_emp_periods)
        _merged: list = []
        for _s, _e in _sorted:
            if _merged and _s <= _merged[-1][1]:
                _merged[-1] = (_merged[-1][0], max(_merged[-1][1], _e))
            else:
                _merged.append((_s, _e))
        all_emp_merged.extend(_merged)

    all_emp_merged.sort()
    merged_windows: list = []
    for start, end in all_emp_merged:
        if merged_windows and start < merged_windows[-1][1]:
            merged_windows[-1] = (merged_windows[-1][0], max(merged_windows[-1][1], end))
        else:
            merged_windows.append((start, end))
    effective_employer_count = len(merged_windows)
    employer_count = len(employer_periods)
    tenure = (
        round(total_experience / effective_employer_count, 1)
        if effective_employer_count > 0
        else 0.0
    )

    return {
        "total_experience_years": round(baseline_years, 1),
        "baseline_years": baseline_years,
        "tenure": tenure,
        "employer_count": employer_count,
        "total_roles": total_roles,
    }


# ---------------------------------------------------------------------------
# Core CV analysis — standalone implementation (no webbridge dependency)
# ---------------------------------------------------------------------------

def _analyze_cv_bytes_sync_worker(pdf_bytes: bytes) -> "dict | None":
    """Synchronous CV analysis for use in worker processes.

    Equivalent to webbridge_cv._analyze_cv_bytes_sync, but uses only
    worker-local helpers (no imports from webbridge / webbridge_cv).
    Returns structured dict or None.
    """
    try:
        from pypdf import PdfReader
    except ImportError:
        logger.warning("[CV Worker] pypdf not installed")
        return None

    t_pdf_cpu = time.process_time()
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text = ""
        for page in reader.pages:
            t = page.extract_text()
            if t:
                text += t + "\n"
    except Exception as exc:
        logger.warning("[CV Worker] PDF parse error: %s", exc)
        return None

    pdf_cpu_s = time.process_time() - t_pdf_cpu
    logger.debug(
        '{"event":"cv_pdf_extract_worker","cpu_time_ms":%.1f,"size_bytes":%d}',
        pdf_cpu_s * 1000,
        len(pdf_bytes),
    )

    if not text.strip():
        return None

    # Language detection + translation
    original_text = text
    try:
        lang_prompt = (
            "Analyze this text and determine if it's primarily in English or another language.\n"
            'Return JSON: {"language": "<language_code>", "is_english": true/false}\n'
            f"Text sample (first {LANG_DETECTION_SAMPLE_LENGTH} chars): "
            f"{text[:LANG_DETECTION_SAMPLE_LENGTH]}"
        )
        lang_resp = _worker_llm_call(lang_prompt)
        lang_obj = _extract_json_object((lang_resp or "").strip())
        if lang_obj and not lang_obj.get("is_english", True):
            source_lang = lang_obj.get("language", "")
            logger.info(
                "[CV Worker] Non-English CV detected (%s), translating", source_lang
            )
            tr = _worker_translate(
                text[:CV_TRANSLATION_MAX_CHARS], "english", source_lang
            )
            if tr and tr.get("translated"):
                text = tr["translated"]
                if len(original_text) > CV_TRANSLATION_MAX_CHARS:
                    logger.warning(
                        "[CV Worker] CV truncated for translation "
                        "(%d > %d chars)",
                        len(original_text),
                        CV_TRANSLATION_MAX_CHARS,
                    )
    except Exception as exc:
        logger.warning(
            "[CV Worker] Language detection/translation failed: %s", exc
        )
        text = original_text

    prompt = (
        "SYSTEM:\n"
        "SOURCE OF TRUTH: You are analyzing a CV from the cv column in the process table (Postgres).\n"
        "This CV is the EXCLUSIVE source for all information. Do not infer or add skills not explicitly in the CV.\n\n"
        "Analyze the following CV text.\n"
        "Return STRICT JSON only with these keys:\n"
        "{\n"
        '  "name": "<Full Name of the candidate>",\n'
        '  "skillset": ["Skill1", "Skill2", ...],\n'
        '  "total_experience_years": <number>,\n'
        '  "tenure": <number>,\n'
        '  "experience": ["Job Title, Company, StartYear to EndYear|present", ...],\n'
        '  "education": ["University Name, Degree Type, Discipline", ...],\n'
        '  "product_list": ["Product1", "Product2", ...],\n'
        '  "company": "<Current/Latest Company Name>",\n'
        '  "job_title": "<Current/Latest Job Title>",\n'
        '  "country": "<Country or Location>",\n'
        '  "seniority": "<Seniority>",\n'
        '  "sector": "<Sector>",\n'
        '  "job_family": "<Job Family>"\n'
        "}\n"
        "Rules:\n"
        "0. Name: Extract the candidate's full name from the CV. It is typically the very first prominent line of the document. Return an empty string if not found.\n"
        "1. Skillset: Extract ONLY skills explicitly mentioned in the CV. Max 15 items. Do not infer or add skills.\n"
        "2. Total Experience: Calculate sum of all employment durations in years, EXCLUDING internships and intern positions. Only count full-time, part-time, and regular employment. Return a number rounded to 1 decimal place.\n"
        "3. Tenure: Calculate average tenure. Formula: total_experience_years / number of NON-OVERLAPPING employment windows. Rules: (a) Treat repeated employment at the same company as ONE employer. (b) When two DIFFERENT employers overlap in time (concurrent/dual employment), count them as ONE employment window in the divisor \u2014 do not count both separately. (c) Exclude internships and intern positions from both the numerator and the window count. Return a number rounded to 1 decimal place.\n"
        "   Example 1: Someone at Google 2015-2017 and again 2019-2021 \u2192 total_exp=4, windows=1 (Google), tenure=4/1=4.0\n"
        "   Example 2: Google 3yr, Amazon 2yr, Intern at Microsoft 1yr (excluded) \u2192 total_exp=5, windows=2, tenure=5/2=2.5\n"
        "   Example 3: IQVIA 2021-present AND Milky Pharmacy Jan-2025-present (concurrent/overlapping) \u2192 they form 1 window, not 2. If total_exp=13 over 3 windows (TOMOKI, Parexel, IQVIA+Milky), tenure=13/3=4.3\n"
        "4. Experience: STRICTLY parse employment history in format 'Job Title, Company, StartYear to EndYear'. If current job, use 'present' instead of EndYear. MANDATORY: Include EVERY SINGLE employment entry from the CV - do not omit any job.\n"
        "5. Education: Format each entry as 'University Name, Degree Type, Discipline'. MANDATORY: Include ALL educational qualifications - degrees, certifications, diplomas. Do not omit any.\n"
        "6. Products: Identify the LATEST company in the employment history. List its specific products, drugs, therapeutics, software platforms, or services. "
        "Use the company name to infer known products if they are not explicitly mentioned in the CV (e.g., AstraZeneca \u2192 Tagrisso, Farxiga; Pfizer \u2192 Eliquis, Xeljanz; Roche \u2192 Herceptin, Avastin; Novartis \u2192 Cosentyx, Entresto). "
        "For non-pharma companies, list their core product lines or service offerings. "
        "MANDATORY: Always return at least 1\u20133 items in product_list. If specific products cannot be identified, return the company's primary service domain (e.g., 'Clinical Trial Management', 'Drug Development', 'Medical Devices').\n"
        "7. Identify CURRENT employment details (company, job_title, country).\n"
        "8. Infer Seniority, Sector, and Job Family based on the profile.\n"
        "9. CRITICAL REQUIREMENT: Parse COMPLETE employment history without ANY omissions. Every job mentioned must be in the experience array.\n"
        "10. CRITICAL REQUIREMENT: Parse COMPLETE education history without ANY omissions. Every degree/certification must be in the education array.\n"
        '11. IMPORTANT: If a field value cannot be determined, return an empty string "" instead of \'unknown\', \'N/A\', or similar placeholders.\n'
        "12. No commentary, no extra keys. Output only valid JSON.\n\n"
        f"CV TEXT:\n{text[:CV_ANALYSIS_MAX_CHARS]}\n\nJSON:"
    )

    resp_text = _worker_llm_call(prompt)
    raw = (resp_text or "").strip()
    obj = _extract_json_object(raw)

    # Post-process
    if obj:
        for field in ["company", "job_title", "seniority", "sector", "job_family", "country"]:
            if obj.get(field):
                value = re.sub(r'^[\s"\'`]+|[\s"\'`]+$', "", str(obj[field])).strip()
                if value.lower() in ("unknown", "n/a", "na", "not specified", "not available"):
                    obj[field] = ""
                else:
                    obj[field] = value

        if obj.get("seniority"):
            norm = _normalize_seniority_to_8_levels(
                obj["seniority"], obj.get("total_experience_years")
            )
            obj["seniority"] = _strip_level_suffix(norm)

        if obj.get("job_title"):
            jt = str(obj["job_title"]).strip().lower()
            if re.search(r"\bcoordinator\b", jt):
                obj["seniority"] = "Junior"
            elif re.search(r"\bmanager\b", jt) and str(
                obj.get("seniority", "")
            ).lower() not in ("director", "expert", "executive"):
                obj["seniority"] = "Manager"

        experience_list = obj.get("experience", [])
        if experience_list and isinstance(experience_list, list):
            gemini_total = obj.get("total_experience_years", 0)
            gemini_tenure = obj.get("tenure", 0)
            recalc = _recalculate_tenure_and_experience(experience_list)
            obj["total_experience_years"] = recalc["total_experience_years"]
            obj["tenure"] = recalc["tenure"]
            if abs(recalc["total_experience_years"] - float(gemini_total or 0)) > 0.5:
                logger.info("[CV Worker] Recalculated total_experience_years differs from Gemini estimate")
            if abs(recalc["tenure"] - float(gemini_tenure or 0)) > 0.5:
                logger.info("[CV Worker] Recalculated tenure differs from Gemini estimate")

    return obj


# ---------------------------------------------------------------------------
# Process-pool entry point
# ---------------------------------------------------------------------------

def analyze_cv_bytes_worker_entry(pdf_bytes: bytes) -> "dict | None":
    """Entry point for ProcessPoolExecutor.submit().

    Wraps _analyze_cv_bytes_sync_worker with per-process CPU-time measurement
    (time.process_time() gives accurate CPU time for this OS process).
    Adds ``_proc_cpu_s`` to the result so the caller can log it.
    """
    t_cpu = time.process_time()
    result = _analyze_cv_bytes_sync_worker(pdf_bytes)
    cpu_elapsed = time.process_time() - t_cpu
    if isinstance(result, dict):
        result["_proc_cpu_s"] = cpu_elapsed
    return result
