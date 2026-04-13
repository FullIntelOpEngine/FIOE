"""
test_data_sorter.py — Tests for job-family inference, enrichment update helpers.

Stubs replicate the relevant logic from webbridge.py / data_sorter flows.

Run with:  pytest tests/test_data_sorter.py
"""
import unittest
from unittest.mock import MagicMock, patch, call

from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Inline stubs
# ---------------------------------------------------------------------------

def _infer_job_families(job_titles: list, reference_mapping: dict) -> dict:
    """
    Given a list of job titles and a reference mapping {title: family},
    return {title: inferred_family}.  Unknown titles map to 'Other'.
    """
    result = {}
    for title in job_titles:
        family = reference_mapping.get(title.strip().lower())
        result[title] = family if family else "Other"
    return result


def _build_update_enrichment_sql(username: str, role_tag: str, updates: dict) -> list:
    """
    Return a list of (sql, params) tuples for the enrichment update.
    Mirrors the WHERE clauses used in webbridge.py.
    """
    stmts = []
    for field, value in updates.items():
        sql = (
            f"UPDATE process SET {field} = %s "
            f"WHERE username = %s AND role_tag = %s"
        )
        stmts.append((sql, (value, username, role_tag)))
    return stmts


def _infer_sector_from_title(title: str, keyword_map: dict) -> str | None:
    """Simple keyword-based sector inference stub."""
    title_lower = title.lower()
    for keyword, sector in keyword_map.items():
        if keyword in title_lower:
            return sector
    return None


def _build_infer_app():
    app = Flask(__name__)
    app.config["TESTING"] = True

    _REF_MAPPING = {
        "software engineer": "Engineering",
        "data scientist": "Data & Analytics",
        "product manager": "Product",
        "hr manager": "Human Resources",
    }

    @app.post("/infer_job_families")
    def infer_job_families():
        body = request.get_json(force=True, silent=True) or {}
        job_titles = body.get("job_titles", [])
        mapping = body.get("mapping", _REF_MAPPING)
        result = _infer_job_families(job_titles, {k.lower(): v for k, v in mapping.items()})
        return jsonify({"ok": True, "result": result}), 200

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestInferJobFamilies(unittest.TestCase):

    def setUp(self):
        self.ref = {
            "software engineer": "Engineering",
            "data scientist": "Data & Analytics",
            "product manager": "Product",
        }

    def test_infer_job_families(self):
        """Known job titles are mapped to their families."""
        result = _infer_job_families(
            ["Software Engineer", "Data Scientist"],
            self.ref,
        )
        self.assertEqual(result["Software Engineer"], "Engineering")
        self.assertEqual(result["Data Scientist"], "Data & Analytics")

    def test_infer_job_families_unknown(self):
        """Unknown job titles fall back to 'Other'."""
        result = _infer_job_families(["Galactic Explorer"], self.ref)
        self.assertEqual(result["Galactic Explorer"], "Other")

    def test_infer_job_families_empty(self):
        """Empty title list → empty result."""
        result = _infer_job_families([], self.ref)
        self.assertEqual(result, {})

    def test_infer_endpoint_both_flows(self):
        """POST /infer_job_families returns mapping results."""
        app = _build_infer_app()
        client = app.test_client()
        resp = client.post(
            "/infer_job_families",
            json={"job_titles": ["Software Engineer", "Unknown Role"]},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data.get("ok"))
        result = data.get("result", {})
        self.assertEqual(result.get("Software Engineer"), "Engineering")
        self.assertEqual(result.get("Unknown Role"), "Other")


class TestUpdateEnrichment(unittest.TestCase):

    def test_update_enrichment(self):
        """_build_update_enrichment_sql produces correct WHERE clauses."""
        stmts = _build_update_enrichment_sql(
            "alice", "senior_dev", {"sector": "Technology", "seniority": "Senior"}
        )
        self.assertEqual(len(stmts), 2)
        for sql, params in stmts:
            self.assertIn("WHERE username = %s AND role_tag = %s", sql)
            self.assertEqual(params[1], "alice")
            self.assertEqual(params[2], "senior_dev")

    def test_update_enrichment_values(self):
        """Each statement targets the correct field."""
        stmts = _build_update_enrichment_sql("bob", "analyst", {"sector": "Finance"})
        sql, params = stmts[0]
        self.assertIn("sector", sql)
        self.assertEqual(params[0], "Finance")


class TestSectorInfer(unittest.TestCase):

    _KEYWORD_MAP = {
        "engineer": "Technology",
        "nurse": "Healthcare",
        "analyst": "Finance",
    }

    def test_sector_infer(self):
        """Known keyword in title → correct sector returned."""
        result = _infer_sector_from_title("Software Engineer", self._KEYWORD_MAP)
        self.assertEqual(result, "Technology")

    def test_sector_infer_no_match(self):
        """No keyword match → None returned."""
        result = _infer_sector_from_title("Galactic Overlord", self._KEYWORD_MAP)
        self.assertIsNone(result)

    def test_sector_infer_db_updates(self):
        """Inferred sector produces correct DB update statements."""
        sector = _infer_sector_from_title("Senior Analyst", self._KEYWORD_MAP)
        self.assertEqual(sector, "Finance")
        stmts = _build_update_enrichment_sql("user1", "tag1", {"sector": sector})
        self.assertEqual(stmts[0][1][0], "Finance")


if __name__ == "__main__":
    unittest.main()
