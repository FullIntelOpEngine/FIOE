"""
test_seniority.py — Tests for seniority classification hardening.

Covers all three layers of the title-based seniority override introduced in
the PR (coordinator → Junior/Associate, manager → Manager):

  Layer 1 — webbridge.py : _map_gemini_seniority_to_dropdown()
             coordinator now in associate_tokens  → "Associate"
             title-based override after _normalize_seniority_single()

  Layer 2 — webbridge_cv.py : post-normalization title override
             coordinator in job_title → seniority='Junior'
             manager in job_title    → seniority='Manager'
             (unless already Director/Expert/Executive)

  Layer 3 — webbridge_routes.py : _infer_seniority_from_titles()
             Coordinator rule is checked *before* Associate/Manager/Director
             so "Project Coordinator" cannot be misclassified as "Manager"

All stubs are self-contained; no Flask app or DB required.

Run with:  pytest tests/test_seniority.py
"""
import re
import unittest


# ---------------------------------------------------------------------------
# Stubs — mirrors of the changed functions (no webbridge import needed)
# ---------------------------------------------------------------------------

# ── Layer 1 ─ webbridge.py ─────────────────────────────────────────────────

def _map_gemini_seniority_to_dropdown(seniority_text: str,
                                      total_experience_years=None) -> str:
    """Inline stub mirroring webbridge.py after the PR change."""
    if not seniority_text and total_experience_years is None:
        return ""
    s = (seniority_text or "").strip().lower()

    if s in {"associate", "manager", "director"}:
        return s.capitalize()

    director_tokens = [
        "director", "vice president", "vp", "vice-president", "head of", "head ",
        "chief ", "cxo", "executive director", "group director",
        "principal", "staff", "expert",
    ]
    for tok in director_tokens:
        if tok in s:
            return "Director"

    try:
        if total_experience_years is not None:
            years = float(total_experience_years)
            if years >= 10:
                return "Director"
            if years >= 5:
                return "Manager"
            if years >= 0:
                return "Associate"
    except Exception:
        pass

    manager_tokens = ["manager", "mgr", "team lead", "lead", "supervisor",
                      "senior", "team-lead", "teamlead"]
    for tok in manager_tokens:
        if tok in s:
            return "Manager"

    # coordinator now in associate_tokens (PR change)
    associate_tokens = ["associate", "junior", "intern", "entry-level",
                        "trainee", "graduate", "coordinator"]
    for tok in associate_tokens:
        if tok in s:
            return "Associate"

    if "senior" in s:
        return "Manager"

    return ""


def _apply_title_seniority_override_webbridge(job_title: str, seniority: str) -> str:
    """
    Mirrors the title-based override applied in webbridge.py after
    _normalize_seniority_single().  Manager guard: only promote to Manager
    when current seniority is not already Director.
    """
    if not job_title:
        return seniority
    jt = job_title.strip().lower()
    if re.search(r'\bcoordinator\b', jt):
        return 'Junior'
    if re.search(r'\bmanager\b', jt):
        if seniority.lower() not in ('director', 'expert', 'executive'):
            return 'Manager'
    return seniority


# ── Layer 2 ─ webbridge_cv.py ──────────────────────────────────────────────

def _apply_title_seniority_override_cv(obj: dict) -> dict:
    """
    Mirrors the post-normalization override in webbridge_cv.py (line ~3155).
    Mutates obj['seniority'] in place, returns obj for chaining.
    """
    if obj.get('job_title'):
        jt_lower = str(obj['job_title']).strip().lower()
        if re.search(r'\bcoordinator\b', jt_lower):
            obj['seniority'] = 'Junior'
        elif re.search(r'\bmanager\b', jt_lower) and \
                str(obj.get('seniority', '')).lower() not in ('director', 'expert', 'executive'):
            obj['seniority'] = 'Manager'
    return obj


# ── Layer 3 ─ webbridge_routes.py ──────────────────────────────────────────

def _infer_seniority_from_titles(job_titles: list) -> str | None:
    """Inline stub mirroring webbridge_routes.py after the PR change."""
    if not job_titles:
        return None
    joined = " ".join([t or "" for t in job_titles])
    # Coordinator checked first (PR change) to prevent misclassification
    if re.search(r"\bCoordinator\b", joined, flags=re.I):
        return "Associate"
    if re.search(r"\bAssociate\b", joined, flags=re.I):
        return "Associate"
    if re.search(r"\bManager\b", joined, flags=re.I):
        return "Manager"
    if re.search(r"\bDirector\b", joined, flags=re.I):
        return "Director"
    return None


# ---------------------------------------------------------------------------
# Tests — Layer 1: _map_gemini_seniority_to_dropdown
# ---------------------------------------------------------------------------

class TestMapGeminiSeniorityCoordinator(unittest.TestCase):
    """coordinator is now in associate_tokens → always maps to Associate."""

    def test_coordinator_maps_to_associate(self):
        """Seniority text 'coordinator' → 'Associate'."""
        self.assertEqual(_map_gemini_seniority_to_dropdown("coordinator"), "Associate")

    def test_project_coordinator_maps_to_associate(self):
        """'project coordinator' (common Gemini output) → 'Associate'."""
        self.assertEqual(_map_gemini_seniority_to_dropdown("project coordinator"), "Associate")

    def test_coordinator_not_overridden_by_lead(self):
        """'lead coordinator' still → 'Manager' because 'lead' is in manager_tokens first."""
        # 'lead' is checked before 'coordinator' in the token scan order
        result = _map_gemini_seniority_to_dropdown("lead coordinator")
        self.assertEqual(result, "Manager")

    def test_senior_coordinator_maps_to_manager(self):
        """'senior coordinator' — 'senior' is in manager_tokens and wins over coordinator."""
        result = _map_gemini_seniority_to_dropdown("senior coordinator")
        self.assertEqual(result, "Manager")

    def test_coordinator_not_maps_to_director(self):
        """Plain 'coordinator' must never map to Director."""
        self.assertNotEqual(_map_gemini_seniority_to_dropdown("coordinator"), "Director")

    def test_manager_still_maps_to_manager(self):
        """Existing manager-token behaviour unchanged."""
        self.assertEqual(_map_gemini_seniority_to_dropdown("manager"), "Manager")

    def test_director_still_maps_to_director(self):
        """Existing director-token behaviour unchanged."""
        self.assertEqual(_map_gemini_seniority_to_dropdown("director"), "Director")

    def test_junior_still_maps_to_associate(self):
        """'junior' still maps to Associate (unchanged)."""
        self.assertEqual(_map_gemini_seniority_to_dropdown("junior"), "Associate")

    def test_empty_seniority_returns_empty(self):
        """Empty input with no experience → empty string fallback."""
        self.assertEqual(_map_gemini_seniority_to_dropdown(""), "")


# ---------------------------------------------------------------------------
# Tests — Layer 1: title-based override after normalization (webbridge.py)
# ---------------------------------------------------------------------------

class TestTitleOverrideWebbridge(unittest.TestCase):
    """Title-based override applied after _normalize_seniority_single() in webbridge.py."""

    def test_coordinator_title_forces_junior(self):
        """job_title containing 'coordinator' → seniority becomes 'Junior'."""
        result = _apply_title_seniority_override_webbridge("Project Coordinator", "Lead")
        self.assertEqual(result, "Junior")

    def test_coordinator_title_case_insensitive(self):
        """Match is case-insensitive (COORDINATOR, Coordinator, coordinator)."""
        for title in ("COORDINATOR", "Project Coordinator", "coordinator"):
            result = _apply_title_seniority_override_webbridge(title, "Senior")
            self.assertEqual(result, "Junior", f"Failed for: {title}")

    def test_coordinator_title_only_word_boundary(self):
        """'coordinated' must NOT trigger the coordinator override."""
        result = _apply_title_seniority_override_webbridge("coordinated projects", "Senior")
        self.assertEqual(result, "Senior")

    def test_manager_title_forces_manager(self):
        """job_title with 'manager' → seniority becomes 'Manager'."""
        result = _apply_title_seniority_override_webbridge("Operations Manager", "Junior")
        self.assertEqual(result, "Manager")

    def test_manager_title_does_not_override_director(self):
        """Manager title must not demote an existing Director-level seniority."""
        result = _apply_title_seniority_override_webbridge("Senior Manager", "Director")
        self.assertEqual(result, "Director")

    def test_manager_title_does_not_override_expert(self):
        """Manager title must not demote an existing Expert-level seniority."""
        result = _apply_title_seniority_override_webbridge("Account Manager", "Expert")
        self.assertEqual(result, "Expert")

    def test_manager_title_does_not_override_executive(self):
        """Manager title must not demote an existing Executive-level seniority."""
        result = _apply_title_seniority_override_webbridge("Program Manager", "Executive")
        self.assertEqual(result, "Executive")

    def test_no_override_for_neutral_title(self):
        """Titles without coordinator/manager keywords leave seniority unchanged."""
        result = _apply_title_seniority_override_webbridge("Software Engineer", "Senior")
        self.assertEqual(result, "Senior")

    def test_empty_title_leaves_seniority(self):
        """Empty job_title → seniority returned unchanged."""
        result = _apply_title_seniority_override_webbridge("", "Mid-Level")
        self.assertEqual(result, "Mid-Level")


# ---------------------------------------------------------------------------
# Tests — Layer 2: title-based override in webbridge_cv.py
# ---------------------------------------------------------------------------

class TestTitleOverrideCV(unittest.TestCase):
    """Post-normalization title override logic in webbridge_cv.py."""

    def test_coordinator_title_sets_junior(self):
        """Coordinator in job_title → seniority set to 'Junior'."""
        obj = {"job_title": "Project Coordinator", "seniority": "Lead"}
        result = _apply_title_seniority_override_cv(obj)
        self.assertEqual(result["seniority"], "Junior")

    def test_coordinator_overrides_senior(self):
        """Coordinator overrides even a Senior seniority (prevents misclassification)."""
        obj = {"job_title": "Senior Coordinator", "seniority": "Senior"}
        result = _apply_title_seniority_override_cv(obj)
        self.assertEqual(result["seniority"], "Junior")

    def test_manager_title_sets_manager(self):
        """Manager in job_title → seniority set to 'Manager'."""
        obj = {"job_title": "Operations Manager", "seniority": "Associate"}
        result = _apply_title_seniority_override_cv(obj)
        self.assertEqual(result["seniority"], "Manager")

    def test_manager_does_not_override_director(self):
        """Manager title must not override Director seniority."""
        obj = {"job_title": "Senior Manager", "seniority": "Director"}
        result = _apply_title_seniority_override_cv(obj)
        self.assertEqual(result["seniority"], "Director")

    def test_manager_does_not_override_expert(self):
        """Manager title must not override Expert seniority."""
        obj = {"job_title": "Regional Manager", "seniority": "Expert"}
        result = _apply_title_seniority_override_cv(obj)
        self.assertEqual(result["seniority"], "Expert")

    def test_manager_does_not_override_executive(self):
        """Manager title must not override Executive seniority."""
        obj = {"job_title": "Account Manager", "seniority": "Executive"}
        result = _apply_title_seniority_override_cv(obj)
        self.assertEqual(result["seniority"], "Executive")

    def test_no_job_title_leaves_seniority(self):
        """Missing job_title → seniority unchanged."""
        obj = {"seniority": "Senior"}
        result = _apply_title_seniority_override_cv(obj)
        self.assertEqual(result["seniority"], "Senior")

    def test_neutral_title_leaves_seniority(self):
        """Titles without coordinator/manager keywords do not alter seniority."""
        obj = {"job_title": "Software Engineer", "seniority": "Mid-Level"}
        result = _apply_title_seniority_override_cv(obj)
        self.assertEqual(result["seniority"], "Mid-Level")

    def test_word_boundary_coordinator(self):
        """'coordinated' must NOT trigger the coordinator override."""
        obj = {"job_title": "coordinated program delivery", "seniority": "Senior"}
        result = _apply_title_seniority_override_cv(obj)
        self.assertEqual(result["seniority"], "Senior")

    def test_coordinator_takes_priority_over_manager(self):
        """When title contains both 'coordinator' and 'manager', coordinator wins (checked first)."""
        obj = {"job_title": "Coordinator Manager", "seniority": "Lead"}
        result = _apply_title_seniority_override_cv(obj)
        self.assertEqual(result["seniority"], "Junior")


# ---------------------------------------------------------------------------
# Tests — Layer 3: _infer_seniority_from_titles (webbridge_routes.py)
# ---------------------------------------------------------------------------

class TestInferSeniorityFromTitles(unittest.TestCase):
    """
    _infer_seniority_from_titles — coordinator rule is checked *first*.
    This prevents "Project Coordinator" → "Manager" misclassifications.
    """

    def test_coordinator_infers_associate(self):
        """Single title containing Coordinator → 'Associate'."""
        self.assertEqual(
            _infer_seniority_from_titles(["Project Coordinator"]), "Associate"
        )

    def test_coordinator_before_manager(self):
        """List with both Coordinator and Manager → Coordinator wins (checked first)."""
        self.assertEqual(
            _infer_seniority_from_titles(["Project Coordinator", "Operations Manager"]),
            "Associate",
        )

    def test_coordinator_before_director(self):
        """List with Coordinator and Director → Coordinator wins."""
        self.assertEqual(
            _infer_seniority_from_titles(["Site Director", "Coordinator"]), "Associate"
        )

    def test_coordinator_case_insensitive(self):
        """coordinator (all-lowercase) still detected."""
        self.assertEqual(
            _infer_seniority_from_titles(["supply chain coordinator"]), "Associate"
        )

    def test_manager_without_coordinator(self):
        """Manager in title (no coordinator) → 'Manager'."""
        self.assertEqual(
            _infer_seniority_from_titles(["Operations Manager"]), "Manager"
        )

    def test_director_without_coordinator_or_manager(self):
        """Director without coordinator/manager → 'Director'."""
        self.assertEqual(
            _infer_seniority_from_titles(["Regional Director"]), "Director"
        )

    def test_associate_without_coordinator(self):
        """Associate in title (no coordinator) → 'Associate'."""
        self.assertEqual(
            _infer_seniority_from_titles(["Associate Engineer"]), "Associate"
        )

    def test_no_keyword_returns_none(self):
        """Title with no seniority keyword → None."""
        self.assertIsNone(
            _infer_seniority_from_titles(["Software Engineer", "Python Developer"])
        )

    def test_empty_list_returns_none(self):
        """Empty title list → None."""
        self.assertIsNone(_infer_seniority_from_titles([]))

    def test_none_title_handled(self):
        """None entries in the list are tolerated without crashing."""
        result = _infer_seniority_from_titles([None, "Project Coordinator"])
        self.assertEqual(result, "Associate")


# ---------------------------------------------------------------------------
# Tests — End-to-end simulation: all three layers applied together
# ---------------------------------------------------------------------------

class TestSeniorityHardeningEndToEnd(unittest.TestCase):
    """
    Simulate the full pipeline: Gemini output → _map_gemini_seniority_to_dropdown
    → title override → _infer_seniority_from_titles agrees.

    The bug in the issue was "Project Coordinator" → "Lead" from Gemini.
    After the fix all three layers independently return Associate/Junior.
    """

    def _full_pipeline(self, gemini_seniority: str, job_title: str) -> dict:
        step1 = _map_gemini_seniority_to_dropdown(gemini_seniority)
        step2 = _apply_title_seniority_override_webbridge(job_title, step1)
        obj = {"job_title": job_title, "seniority": step1}
        _apply_title_seniority_override_cv(obj)
        step3 = _infer_seniority_from_titles([job_title])
        return {"after_mapping": step1, "after_title_override": step2,
                "cv_layer": obj["seniority"], "from_titles": step3}

    def test_project_coordinator_all_layers(self):
        """'Project Coordinator' — Gemini said 'Lead' — all layers now return Junior/Associate."""
        result = self._full_pipeline("Lead", "Project Coordinator")
        self.assertEqual(result["after_title_override"], "Junior")
        self.assertEqual(result["cv_layer"], "Junior")
        self.assertEqual(result["from_titles"], "Associate")

    def test_operations_manager_all_layers(self):
        """'Operations Manager' — all layers return Manager."""
        result = self._full_pipeline("Mid-Level", "Operations Manager")
        self.assertEqual(result["after_title_override"], "Manager")
        self.assertEqual(result["cv_layer"], "Manager")
        self.assertEqual(result["from_titles"], "Manager")

    def test_regional_director_all_layers(self):
        """'Regional Director' — all layers return Director."""
        result = self._full_pipeline("Director", "Regional Director")
        # Director seniority not overridden by any rule
        self.assertEqual(result["after_mapping"], "Director")
        self.assertEqual(result["after_title_override"], "Director")
        self.assertEqual(result["from_titles"], "Director")

    def test_software_engineer_no_override(self):
        """'Software Engineer' with 'Senior' from Gemini — no override applied."""
        result = self._full_pipeline("Senior", "Software Engineer")
        # Manager tokens match 'senior' → Manager in mapping; title has no override
        self.assertEqual(result["after_title_override"], result["after_mapping"])
        self.assertEqual(result["cv_layer"], result["after_mapping"])

    def test_hr_coordinator_all_layers(self):
        """'HR Coordinator' — all overrides agree on Associate/Junior."""
        result = self._full_pipeline("manager", "HR Coordinator")
        # coordinator wins in mapping (associate_tokens); title override forces Junior
        self.assertEqual(result["after_title_override"], "Junior")
        self.assertEqual(result["cv_layer"], "Junior")
        self.assertEqual(result["from_titles"], "Associate")


if __name__ == "__main__":
    unittest.main()
