"""conftest.py — adds the repo root to sys.path so tests can import helpers."""
import sys
import os

# Add repo root so sector_mappings, app_logger, etc. can be found when needed
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
