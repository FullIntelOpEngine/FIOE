"""
test_regression.py — Regression tests for weight lock UX and job-family rule updates.

These tests use in-memory stubs and do not require a DB or Flask server.

Run with:  pytest tests/test_regression.py
"""
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Stubs for weight-lock and job-family rule update logic
# ---------------------------------------------------------------------------

class _WeightLockConfig:
    """Simulates the weight-lock state returned by the backend."""

    def __init__(self, locked: bool = False, locked_by: str = ""):
        self.locked = locked
        self.locked_by = locked_by

    def to_dict(self):
        return {"locked": self.locked, "locked_by": self.locked_by}


class _JobFamilyRules:
    """
    Simulates the job-family rules store (mirrors data_sorter.json structure).
    """

    def __init__(self, titles: list = None, recent_updates: list = None):
        self.titles = titles or []
        self.recent_updates = recent_updates or []

    def add_titles(self, new_titles: list) -> list:
        """Append new titles (no duplicates); return updated list."""
        existing = set(t.lower() for t in self.titles)
        added = []
        for t in new_titles:
            if t.strip().lower() not in existing:
                self.titles.append(t.strip())
                existing.add(t.strip().lower())
                added.append(t.strip())
        return added

    def record_update(self, event: str, actor: str = "system"):
        entry = {
            "event": event,
            "actor": actor,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.recent_updates.append(entry)
        return entry

    def to_dict(self):
        return {"titles": self.titles, "recent_updates": self.recent_updates}


def _render_weight_lock_ui_state(config: _WeightLockConfig) -> dict:
    """
    Derive UI element states from the weight-lock config.
    Returns dict of {element: enabled/disabled/visible/hidden}.
    """
    if config.locked:
        return {
            "banner_visible": True,
            "sliders_disabled": True,
            "reset_button_disabled": True,
            "banner_text": f"Weights locked by {config.locked_by}",
        }
    return {
        "banner_visible": False,
        "sliders_disabled": False,
        "reset_button_disabled": False,
        "banner_text": "",
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestWeightLockUX(unittest.TestCase):

    def test_weight_lock_ux_locked(self):
        """When locked: banner visible, sliders disabled, reset disabled."""
        config = _WeightLockConfig(locked=True, locked_by="admin@example.com")
        ui = _render_weight_lock_ui_state(config)
        self.assertTrue(ui["banner_visible"])
        self.assertTrue(ui["sliders_disabled"])
        self.assertTrue(ui["reset_button_disabled"])
        self.assertIn("admin@example.com", ui["banner_text"])

    def test_weight_lock_ux_unlocked(self):
        """When unlocked: banner hidden, sliders enabled, reset enabled."""
        config = _WeightLockConfig(locked=False)
        ui = _render_weight_lock_ui_state(config)
        self.assertFalse(ui["banner_visible"])
        self.assertFalse(ui["sliders_disabled"])
        self.assertFalse(ui["reset_button_disabled"])
        self.assertEqual(ui["banner_text"], "")

    def test_weight_lock_banner_text(self):
        """Locked banner text includes the locker's identity."""
        config = _WeightLockConfig(locked=True, locked_by="hr_lead@company.com")
        ui = _render_weight_lock_ui_state(config)
        self.assertIn("hr_lead@company.com", ui["banner_text"])


class TestJobFamilyUpdate(unittest.TestCase):

    def test_jobfamily_update_new_titles_appended(self):
        """New job titles are appended to the existing list."""
        rules = _JobFamilyRules(titles=["Software Engineer", "Data Scientist"])
        added = rules.add_titles(["Product Manager", "UX Designer"])
        self.assertIn("Product Manager", rules.titles)
        self.assertIn("UX Designer", rules.titles)
        self.assertEqual(len(added), 2)

    def test_jobfamily_update_no_duplicates(self):
        """Existing titles are not added again (case-insensitive dedup)."""
        rules = _JobFamilyRules(titles=["Software Engineer"])
        added = rules.add_titles(["software engineer", "Software Engineer", "SOFTWARE ENGINEER"])
        self.assertEqual(len(added), 0)
        self.assertEqual(rules.titles.count("Software Engineer"), 1)

    def test_jobfamily_update_recent_updates_added(self):
        """RecentUpdates list gains an entry when titles are updated."""
        rules = _JobFamilyRules()
        rules.add_titles(["Analyst"])
        entry = rules.record_update("add_titles", actor="test_actor")
        self.assertEqual(len(rules.recent_updates), 1)
        self.assertEqual(entry["event"], "add_titles")
        self.assertEqual(entry["actor"], "test_actor")
        self.assertIn("timestamp", entry)

    def test_jobfamily_to_dict(self):
        """to_dict serializes rules correctly."""
        rules = _JobFamilyRules(titles=["Engineer"], recent_updates=[])
        rules.record_update("init")
        d = rules.to_dict()
        self.assertIn("Engineer", d["titles"])
        self.assertEqual(len(d["recent_updates"]), 1)

    def test_jobfamily_update_preserves_existing(self):
        """Adding new titles preserves previously existing titles."""
        original = ["Software Engineer", "Data Scientist"]
        rules = _JobFamilyRules(titles=list(original))
        rules.add_titles(["Product Manager"])
        for t in original:
            self.assertIn(t, rules.titles)


if __name__ == "__main__":
    unittest.main()
