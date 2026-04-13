"""
test_normalization.py — Tests for normalization helpers from webbridge.py.

Inline stubs replicate the pure helper functions without importing the Flask app.

Recent changes reflected in this file:
  • _map_gemini_seniority_to_dropdown stub updated: "coordinator" added to the
    junior/associate token group (mirrors webbridge.py PR change) so that
    "Project Coordinator" no longer maps to a management-level seniority.
  • New TestSeniorityMap.test_coordinator_maps_to_junior test added.

Run with:  pytest tests/test_normalization.py
"""
import re
import unittest

# ---------------------------------------------------------------------------
# Stubs mirroring webbridge.py helpers
# ---------------------------------------------------------------------------

def _infer_region_from_country(country: str) -> str:
    _MAP = {
        "united states": "North America",
        "usa": "North America",
        "us": "North America",
        "canada": "North America",
        "united kingdom": "Europe",
        "uk": "Europe",
        "germany": "Europe",
        "france": "Europe",
        "spain": "Europe",
        "netherlands": "Europe",
        "switzerland": "Europe",
        "australia": "Asia-Pacific",
        "singapore": "Asia-Pacific",
        "japan": "Asia-Pacific",
        "china": "Asia-Pacific",
        "india": "Asia-Pacific",
        "uae": "Middle East & Africa",
        "united arab emirates": "Middle East & Africa",
        "saudi arabia": "Middle East & Africa",
        "south africa": "Middle East & Africa",
        "brazil": "Latin America",
        "mexico": "Latin America",
        "argentina": "Latin America",
    }
    if not country:
        return "Unknown"
    return _MAP.get(country.strip().lower(), "Unknown")


def _map_gemini_seniority_to_dropdown(seniority_text: str, total_experience_years=None) -> str:
    if not seniority_text:
        return "Mid-Level"
    text = seniority_text.strip().lower()
    # coordinator added to the junior/associate group (PR: prevents misclassification)
    if any(k in text for k in ("intern", "graduate", "junior", "entry", "coordinator")):
        return "Junior"
    if any(k in text for k in ("director", "vp", "vice president", "c-level", "cto", "ceo", "cfo")):
        return "Director+"
    if any(k in text for k in ("senior", "lead", "principal", "staff")):
        return "Senior"
    if any(k in text for k in ("manager", "head of", "associate director")):
        return "Manager"
    if total_experience_years is not None:
        if total_experience_years < 3:
            return "Junior"
        if total_experience_years >= 10:
            return "Senior"
    return "Mid-Level"


def _is_pharma_company(name: str) -> bool:
    _PHARMA_KEYWORDS = (
        "pharma", "pharmaceutical", "bioscience", "biotech",
        "therapeutics", "drug", "vaccine", "clinical",
    )
    if not name:
        return False
    name_lower = name.lower()
    return any(kw in name_lower for kw in _PHARMA_KEYWORDS)


def _sectors_allow_pharma(sectors: list) -> bool:
    if not sectors:
        return False
    pharma_sectors = {"healthcare", "pharmaceutical", "biotech", "life sciences"}
    return any(s.strip().lower() in pharma_sectors for s in sectors)


def _find_best_sector_match_for_text(candidate: str, sectors_token_index: list,
                                      min_jaccard: float = 0.12) -> str | None:
    def _token_set(s):
        if not s:
            return set()
        normalized = re.sub(r'&amp;|&', 'and', s.lower())
        return set(re.findall(r'\w+', normalized))

    if not candidate or not sectors_token_index:
        return None
    cand_tokens = _token_set(candidate)
    if not cand_tokens:
        return None
    best = None
    best_score = 0.0
    best_abs = 0
    for label, label_tokens in sectors_token_index:
        if not label_tokens:
            continue
        intersection = cand_tokens & label_tokens
        abs_overlap = len(intersection)
        if abs_overlap == 0:
            continue
        score = abs_overlap / len(cand_tokens | label_tokens)
        if (score > best_score or
                (score == best_score and abs_overlap > best_abs)):
            best_score = score
            best_abs = abs_overlap
            best = label
    if best and (best_score >= min_jaccard or (len(cand_tokens) <= 2 and best_abs >= 1)):
        return best
    return None


def _normalize_rows(rows: list) -> list:
    """Normalize a list of candidate row dicts (strip whitespace, lower some fields)."""
    result = []
    for row in rows:
        normalized = {}
        for k, v in row.items():
            if isinstance(v, str):
                normalized[k] = v.strip()
            else:
                normalized[k] = v
        result.append(normalized)
    return result


def _extract_titles(text: str) -> list:
    """Extract likely job titles from free text (simple heuristic: title-case words)."""
    if not text:
        return []
    pattern = r'\b(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,4})\b'
    return list(dict.fromkeys(re.findall(pattern, text)))


def _extract_names(text: str) -> list:
    """Extract candidate names (2-word title-case sequences)."""
    if not text:
        return []
    pattern = r'\b([A-Z][a-z]+\s+[A-Z][a-z]+)\b'
    return list(dict.fromkeys(re.findall(pattern, text)))


def get_reference_mapping(job_title: str, mapping: dict) -> str | None:
    """Look up a normalized job title in the reference mapping."""
    return mapping.get(job_title.strip().lower())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestNormalizeRows(unittest.TestCase):

    def test_normalize_rows(self):
        """normalize_rows strips whitespace from string fields."""
        rows = [{"name": "  Alice  ", "score": 90}, {"name": "Bob", "score": None}]
        result = _normalize_rows(rows)
        self.assertEqual(result[0]["name"], "Alice")
        self.assertEqual(result[1]["name"], "Bob")
        self.assertIsNone(result[1]["score"])

    def test_extract_titles(self):
        """_extract_titles finds title-case job titles in text."""
        text = "Looking for a Senior Product Manager with experience as a Data Scientist"
        titles = _extract_titles(text)
        self.assertTrue(any("Senior" in t or "Product" in t for t in titles))

    def test_extract_names(self):
        """_extract_names extracts name-like patterns from text."""
        text = "Candidates include Alice Smith, Bob Jones, and Dr. Carol White"
        names = _extract_names(text)
        self.assertIn("Alice Smith", names)
        self.assertIn("Bob Jones", names)


class TestReferenceMapping(unittest.TestCase):

    _MAPPING = {
        "software engineer": "Engineering",
        "product manager": "Product",
        "data scientist": "Data & Analytics",
    }

    def test_reference_mapping(self):
        """get_reference_mapping returns correct family for known title."""
        result = get_reference_mapping("Software Engineer", self._MAPPING)
        self.assertEqual(result, "Engineering")

    def test_reference_mapping_case_insensitive(self):
        """get_reference_mapping is case-insensitive."""
        result = get_reference_mapping("PRODUCT MANAGER", self._MAPPING)
        self.assertEqual(result, "Product")

    def test_reference_mapping_unknown(self):
        """get_reference_mapping returns None for unknown title."""
        result = get_reference_mapping("Galactic Overlord", self._MAPPING)
        self.assertIsNone(result)


class TestRegionInference(unittest.TestCase):

    def test_region_inference(self):
        """Common countries map to correct regions."""
        self.assertEqual(_infer_region_from_country("Germany"), "Europe")
        self.assertEqual(_infer_region_from_country("United States"), "North America")
        self.assertEqual(_infer_region_from_country("Singapore"), "Asia-Pacific")
        self.assertEqual(_infer_region_from_country("UAE"), "Middle East & Africa")
        self.assertEqual(_infer_region_from_country("Brazil"), "Latin America")

    def test_region_inference_empty(self):
        """Empty/None input → 'Unknown'."""
        self.assertEqual(_infer_region_from_country(""), "Unknown")
        self.assertEqual(_infer_region_from_country(None), "Unknown")  # type: ignore[arg-type]

    def test_region_inference_unknown_country(self):
        """Unrecognised country → 'Unknown'."""
        self.assertEqual(_infer_region_from_country("Wakanda"), "Unknown")


class TestSeniorityMap(unittest.TestCase):

    def test_seniority_map(self):
        """Various seniority strings map to correct dropdown values."""
        self.assertEqual(_map_gemini_seniority_to_dropdown("Senior Engineer"), "Senior")
        self.assertEqual(_map_gemini_seniority_to_dropdown("Junior Developer"), "Junior")
        self.assertEqual(_map_gemini_seniority_to_dropdown("VP of Engineering"), "Director+")
        self.assertEqual(_map_gemini_seniority_to_dropdown("Manager"), "Manager")
        self.assertEqual(_map_gemini_seniority_to_dropdown(""), "Mid-Level")

    def test_seniority_experience_override(self):
        """Experience years influence seniority when title text is ambiguous."""
        self.assertEqual(
            _map_gemini_seniority_to_dropdown("Specialist", total_experience_years=2),
            "Junior",
        )
        self.assertEqual(
            _map_gemini_seniority_to_dropdown("Specialist", total_experience_years=12),
            "Senior",
        )

    def test_coordinator_maps_to_junior(self):
        """'coordinator' in seniority text → 'Junior' (PR: prevents misclassification).

        Before the change Gemini could return e.g. 'Project Coordinator' as a
        seniority string which would fall through to 'Mid-Level'.  The PR added
        'coordinator' to the junior/associate token group so it now maps to
        'Junior' consistently.
        """
        self.assertEqual(_map_gemini_seniority_to_dropdown("coordinator"), "Junior")
        self.assertEqual(_map_gemini_seniority_to_dropdown("project coordinator"), "Junior")
        self.assertEqual(_map_gemini_seniority_to_dropdown("Coordinator"), "Junior")

    def test_coordinator_never_maps_to_manager_or_director(self):
        """Coordinator-containing seniority text must not map to Manager or Director."""
        for text in ("coordinator", "project coordinator", "Coordinator"):
            result = _map_gemini_seniority_to_dropdown(text)
            self.assertNotIn(result, ("Manager", "Director+"),
                             f"coordinator should not map to management level; got {result!r} for {text!r}")


class TestSectorMatch(unittest.TestCase):

    _INDEX = [
        ("Technology > Cloud & Infrastructure", frozenset({"technology", "cloud", "infrastructure", "and"})),
        ("Financial Services > Banking", frozenset({"financial", "services", "banking"})),
        ("Healthcare > Biotechnology", frozenset({"healthcare", "biotechnology"})),
    ]

    def test_sector_match(self):
        """_find_best_sector_match_for_text returns correct sector for matching text."""
        result = _find_best_sector_match_for_text("cloud infrastructure", self._INDEX)
        self.assertEqual(result, "Technology > Cloud & Infrastructure")

    def test_sector_match_no_match(self):
        """Unrelated text returns None."""
        result = _find_best_sector_match_for_text("pottery wheel repair", self._INDEX)
        self.assertIsNone(result)


class TestPharmaCheck(unittest.TestCase):

    def test_is_pharma_company(self):
        """_is_pharma_company identifies pharma companies."""
        self.assertTrue(_is_pharma_company("Roche Pharmaceuticals"))
        self.assertTrue(_is_pharma_company("BioNTech Vaccine"))
        self.assertFalse(_is_pharma_company("Google Cloud"))
        self.assertFalse(_is_pharma_company(""))

    def test_sectors_allow_pharma(self):
        """_sectors_allow_pharma returns True for healthcare/pharma sectors."""
        self.assertTrue(_sectors_allow_pharma(["Healthcare", "Pharmaceutical"]))
        self.assertFalse(_sectors_allow_pharma(["Technology", "Finance"]))
        self.assertFalse(_sectors_allow_pharma([]))


if __name__ == "__main__":
    unittest.main()
