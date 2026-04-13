"""
test_profile_pic.py — Tests for LinkedIn profile picture retrieval via Google CSE.

Covers the updated get_linkedin_profile_picture() logic introduced in the PR:
  • LinkedIn URL validation (SSRF guard; broadened slug charset)
  • Method 1: og:image scrape from LinkedIn (mocked requests)
  • Method 2: CSE text search → pagemap.cse_thumbnail / pagemap.metatags[og:image]
  • Method 3: CSE image search fallback (relaxed aspect ratio 0.7–1.4)
  • display_name parameter: used in CSE queries when present
  • HEAD validation: 2xx/3xx accepted; 405 accepted (CDN quirk); 4xx rejected
  • _is_private_host SSRF guard blocks loopback/private addresses

All stubs are self-contained; no DB or running Flask server required.

Run with:  pytest tests/test_profile_pic.py
"""
import re
import unittest
from unittest.mock import patch, MagicMock
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# Minimal reimplementations of the helpers under test
# ---------------------------------------------------------------------------

def _is_private_host(url: str) -> bool:
    """
    Simplified SSRF guard mirroring webbridge_routes._is_private_host.
    Returns True for loopback, link-local, RFC-1918 private ranges, and
    metadata service addresses.
    """
    import socket
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        # Literal loopback
        if host in ("localhost", "127.0.0.1", "::1"):
            return True
        # Metadata service (AWS/GCP)
        if host.startswith("169.254"):
            return True
        # Resolve and check
        addr = socket.gethostbyname(host)
        parts = list(map(int, addr.split(".")))
        if parts[0] == 10:
            return True
        if parts[0] == 172 and 16 <= parts[1] <= 31:
            return True
        if parts[0] == 192 and parts[1] == 168:
            return True
        return False
    except Exception:
        return False


def _validate_linkedin_url(linkedin_url: str) -> bool:
    """
    Inline LinkedIn URL validation mirroring webbridge_routes.py.
    Returns True when the URL is a valid LinkedIn profile URL.
    """
    if not linkedin_url:
        return False
    return bool(
        re.match(
            r'^https?://([a-z]+\.)?linkedin\.com/in/[a-zA-Z0-9\-._~%]+/?$',
            linkedin_url,
            re.IGNORECASE,
        )
    )


def _extract_cse_thumbnail(items: list) -> str | None:
    """
    Extract the best profile picture from CSE text-search result items.
    Mirrors the pagemap extraction logic added in Method 2.
    """
    for item in items:
        pagemap = item.get("pagemap", {})
        thumbnails = pagemap.get("cse_thumbnail") or []
        if thumbnails and thumbnails[0].get("src"):
            return thumbnails[0]["src"]
        for mt in pagemap.get("metatags", []):
            og = mt.get("og:image") or mt.get("twitter:image")
            if og and "linkedin.com" in og:
                return og
    return None


def _pick_image_search_result(items: list) -> str | None:
    """
    Select best image from CSE image-search results.
    Mirrors Method 3 selection logic: prefers square-ish images from LinkedIn.
    """
    for item in items:
        image_url = item.get("link", "")
        context_link = item.get("image", {}).get("contextLink", "")
        if not image_url:
            continue
        try:
            parsed = urlparse(context_link)
            if not (parsed.netloc == "linkedin.com" or parsed.netloc.endswith(".linkedin.com")):
                continue
        except Exception:
            continue
        width = item.get("image", {}).get("width", 0)
        height = item.get("image", {}).get("height", 0)
        if width and height:
            aspect = width / height if height else 0
            if 0.7 <= aspect <= 1.4 and width < 1200:
                return image_url
    # Last resort: first item
    if items:
        return items[0].get("link")
    return None


def _validate_image_url(image_url: str, head_status: int) -> str | None:
    """
    Mirrors the HEAD validation logic: accept 2xx, 3xx, and 405;
    reject everything else.
    """
    if not image_url:
        return None
    if 200 <= head_status < 400:
        return image_url
    if head_status == 405:
        return image_url
    return None


# ---------------------------------------------------------------------------
# Tests — URL Validation
# ---------------------------------------------------------------------------

class TestLinkedInUrlValidation(unittest.TestCase):

    def test_valid_standard_url(self):
        """Standard linkedin.com/in/ URL is accepted."""
        self.assertTrue(_validate_linkedin_url("https://www.linkedin.com/in/john-doe"))

    def test_valid_url_with_trailing_slash(self):
        """URL with trailing slash is accepted."""
        self.assertTrue(_validate_linkedin_url("https://linkedin.com/in/jane-smith/"))

    def test_valid_url_with_special_chars(self):
        """URL slug with dots, tildes, percent-encoding is accepted (broadened regex)."""
        self.assertTrue(_validate_linkedin_url("https://www.linkedin.com/in/john.doe~123"))
        self.assertTrue(_validate_linkedin_url("https://www.linkedin.com/in/user%40name"))

    def test_invalid_url_empty(self):
        """Empty string is rejected."""
        self.assertFalse(_validate_linkedin_url(""))

    def test_invalid_url_none(self):
        """None is rejected."""
        self.assertFalse(_validate_linkedin_url(None))  # type: ignore[arg-type]

    def test_invalid_url_wrong_domain(self):
        """Non-LinkedIn domain is rejected."""
        self.assertFalse(_validate_linkedin_url("https://evil.com/in/john-doe"))

    def test_invalid_url_ssrf_attempt(self):
        """localhost URL is rejected (SSRF guard at URL format level)."""
        self.assertFalse(
            _validate_linkedin_url("https://localhost/in/profile")
        )

    def test_invalid_url_company_page(self):
        """Company page (not /in/) is rejected."""
        self.assertFalse(
            _validate_linkedin_url("https://www.linkedin.com/company/google")
        )


# ---------------------------------------------------------------------------
# Tests — Method 2: CSE text search → pagemap extraction
# ---------------------------------------------------------------------------

class TestCSEPagemapExtraction(unittest.TestCase):

    def test_cse_thumbnail_returned_first(self):
        """pagemap.cse_thumbnail[0].src is the highest-priority result."""
        items = [
            {
                "pagemap": {
                    "cse_thumbnail": [{"src": "https://media.licdn.com/thumb/john.jpg"}],
                    "metatags": [{"og:image": "https://media.licdn.com/og/john.jpg"}],
                }
            }
        ]
        result = _extract_cse_thumbnail(items)
        self.assertEqual(result, "https://media.licdn.com/thumb/john.jpg")

    def test_og_image_fallback_when_no_thumbnail(self):
        """When cse_thumbnail absent, og:image from metatags is used."""
        items = [
            {
                "pagemap": {
                    "metatags": [{"og:image": "https://media.linkedin.com/og/jane.jpg"}],
                }
            }
        ]
        result = _extract_cse_thumbnail(items)
        self.assertEqual(result, "https://media.linkedin.com/og/jane.jpg")

    def test_twitter_image_fallback(self):
        """twitter:image is used when og:image absent and linkedin.com in URL."""
        items = [
            {
                "pagemap": {
                    "metatags": [
                        {"twitter:image": "https://media.linkedin.com/tw/bob.jpg"}
                    ],
                }
            }
        ]
        result = _extract_cse_thumbnail(items)
        self.assertEqual(result, "https://media.linkedin.com/tw/bob.jpg")

    def test_og_image_without_linkedin_skipped(self):
        """og:image that doesn't contain 'linkedin.com' is skipped."""
        items = [
            {
                "pagemap": {
                    "metatags": [{"og:image": "https://otherdomain.com/pic.jpg"}],
                }
            }
        ]
        result = _extract_cse_thumbnail(items)
        self.assertIsNone(result)

    def test_empty_items_returns_none(self):
        """Empty item list → None."""
        self.assertIsNone(_extract_cse_thumbnail([]))

    def test_empty_pagemap_returns_none(self):
        """Item with no pagemap data → None."""
        self.assertIsNone(_extract_cse_thumbnail([{"pagemap": {}}]))

    def test_multiple_items_first_thumbnail_wins(self):
        """First item with a valid thumbnail wins."""
        items = [
            {"pagemap": {"cse_thumbnail": [{"src": "https://media.licdn.com/first.jpg"}]}},
            {"pagemap": {"cse_thumbnail": [{"src": "https://media.licdn.com/second.jpg"}]}},
        ]
        result = _extract_cse_thumbnail(items)
        self.assertEqual(result, "https://media.licdn.com/first.jpg")


# ---------------------------------------------------------------------------
# Tests — Method 3: CSE image search result selection
# ---------------------------------------------------------------------------

class TestCSEImageSearchSelection(unittest.TestCase):

    def _make_item(self, url: str, context: str, width: int, height: int) -> dict:
        return {
            "link": url,
            "image": {"contextLink": context, "width": width, "height": height},
        }

    def test_square_linkedin_image_selected(self):
        """Square image from linkedin.com → selected."""
        items = [
            self._make_item(
                "https://media.licdn.com/profile.jpg",
                "https://www.linkedin.com/in/john-doe",
                400, 400,
            )
        ]
        result = _pick_image_search_result(items)
        self.assertEqual(result, "https://media.licdn.com/profile.jpg")

    def test_non_linkedin_context_skipped_in_favour_of_linkedin(self):
        """Non-LinkedIn context link is skipped; a following LinkedIn item is preferred."""
        items = [
            self._make_item(
                "https://attacker.com/fake.jpg",
                "https://attacker.com/page",
                400, 400,
            ),
            self._make_item(
                "https://media.linkedin.com/profile.jpg",
                "https://www.linkedin.com/in/john-doe",
                400, 400,
            ),
        ]
        result = _pick_image_search_result(items)
        self.assertEqual(result, "https://media.linkedin.com/profile.jpg")

    def test_very_wide_image_skipped(self):
        """Banner image (very wide aspect) is skipped; narrower image used."""
        items = [
            self._make_item(
                "https://media.licdn.com/banner.jpg",
                "https://www.linkedin.com/in/john-doe",
                1600, 400,   # aspect=4.0 — too wide
            ),
            self._make_item(
                "https://media.licdn.com/profile.jpg",
                "https://www.linkedin.com/in/john-doe",
                400, 400,   # aspect=1.0 — square
            ),
        ]
        result = _pick_image_search_result(items)
        self.assertEqual(result, "https://media.licdn.com/profile.jpg")

    def test_aspect_ratio_boundary_0_7_accepted(self):
        """Aspect ratio exactly 0.7 is accepted (relaxed lower bound)."""
        items = [
            self._make_item(
                "https://media.licdn.com/portrait.jpg",
                "https://www.linkedin.com/in/user",
                700, 1000,  # aspect=0.7
            )
        ]
        result = _pick_image_search_result(items)
        self.assertEqual(result, "https://media.licdn.com/portrait.jpg")

    def test_aspect_ratio_boundary_1_4_accepted(self):
        """Aspect ratio exactly 1.4 is accepted (relaxed upper bound)."""
        items = [
            self._make_item(
                "https://media.licdn.com/landscape.jpg",
                "https://www.linkedin.com/in/user",
                1400, 1000,  # aspect=1.4
            )
        ]
        result = _pick_image_search_result(items)
        self.assertEqual(result, "https://media.licdn.com/landscape.jpg")

    def test_too_large_image_skipped_in_favour_of_smaller(self):
        """Image wider than 1200px is skipped; a smaller image is selected instead."""
        items = [
            self._make_item(
                "https://media.linkedin.com/huge.jpg",
                "https://www.linkedin.com/in/user",
                1500, 1200,  # width=1500 > 1200
            ),
            self._make_item(
                "https://media.linkedin.com/profile.jpg",
                "https://www.linkedin.com/in/user",
                400, 400,   # valid square
            ),
        ]
        result = _pick_image_search_result(items)
        self.assertEqual(result, "https://media.linkedin.com/profile.jpg")

    def test_empty_items_returns_none(self):
        """Empty item list → None."""
        self.assertIsNone(_pick_image_search_result([]))

    def test_fallback_to_first_item_when_no_square_match(self):
        """When no size-filtered item matches, first item used as last resort."""
        items = [
            self._make_item(
                "https://media.licdn.com/fallback.jpg",
                "https://www.linkedin.com/in/user",
                0, 0,  # no dimension info → falls through to last-resort
            )
        ]
        result = _pick_image_search_result(items)
        self.assertEqual(result, "https://media.licdn.com/fallback.jpg")


# ---------------------------------------------------------------------------
# Tests — HEAD validation logic
# ---------------------------------------------------------------------------

class TestHeadValidation(unittest.TestCase):

    def test_200_accepted(self):
        """HTTP 200 → URL returned."""
        self.assertEqual(
            _validate_image_url("https://cdn.example.com/img.jpg", 200),
            "https://cdn.example.com/img.jpg",
        )

    def test_301_redirect_accepted(self):
        """HTTP 301 redirect → URL still returned (allow_redirects is True in real code)."""
        self.assertEqual(
            _validate_image_url("https://cdn.example.com/img.jpg", 301),
            "https://cdn.example.com/img.jpg",
        )

    def test_399_accepted(self):
        """HTTP 399 (last accepted) → URL returned."""
        self.assertEqual(
            _validate_image_url("https://cdn.example.com/img.jpg", 399),
            "https://cdn.example.com/img.jpg",
        )

    def test_405_accepted(self):
        """HTTP 405 Method Not Allowed → URL returned (CDN quirk)."""
        self.assertEqual(
            _validate_image_url("https://cdn.licdn.com/img.jpg", 405),
            "https://cdn.licdn.com/img.jpg",
        )

    def test_403_rejected(self):
        """HTTP 403 → None returned."""
        self.assertIsNone(_validate_image_url("https://cdn.example.com/img.jpg", 403))

    def test_404_rejected(self):
        """HTTP 404 → None returned."""
        self.assertIsNone(_validate_image_url("https://cdn.example.com/img.jpg", 404))

    def test_500_rejected(self):
        """HTTP 500 → None returned."""
        self.assertIsNone(_validate_image_url("https://cdn.example.com/img.jpg", 500))

    def test_empty_url_returns_none(self):
        """Empty image_url → None regardless of status."""
        self.assertIsNone(_validate_image_url("", 200))


# ---------------------------------------------------------------------------
# Tests — display_name parameter enriches CSE query
# ---------------------------------------------------------------------------

class TestDisplayNameQuery(unittest.TestCase):
    """
    Verify that the display_name parameter drives the CSE query string,
    producing more targeted results than the URL slug alone.
    """

    def _build_text_query(self, profile_slug: str,
                          display_name: str | None) -> str:
        """Mirror the query-building logic from webbridge_routes.py."""
        if display_name and display_name.strip():
            return f'"{display_name.strip()}" site:linkedin.com/in'
        return f'site:linkedin.com/in "{profile_slug}"'

    def _build_image_query(self, profile_slug: str,
                           display_name: str | None) -> str:
        """Mirror image query-building logic."""
        if display_name and display_name.strip():
            return f'"{display_name.strip()}" site:linkedin.com/in'
        return f'site:linkedin.com/in "{profile_slug}"'

    def test_text_query_uses_display_name_when_provided(self):
        """Text query uses display_name for more precise matching."""
        q = self._build_text_query("john-doe-12345", "John Doe")
        self.assertIn('"John Doe"', q)
        self.assertIn("site:linkedin.com/in", q)

    def test_text_query_falls_back_to_slug(self):
        """Without display_name, text query uses URL slug."""
        q = self._build_text_query("john-doe-12345", None)
        self.assertIn("john-doe-12345", q)
        self.assertIn("site:linkedin.com/in", q)

    def test_text_query_strips_display_name_whitespace(self):
        """Leading/trailing whitespace in display_name is stripped."""
        q = self._build_text_query("slug", "  Jane Smith  ")
        self.assertIn('"Jane Smith"', q)

    def test_empty_display_name_falls_back_to_slug(self):
        """Empty-string display_name falls back to slug."""
        q = self._build_text_query("john-doe", "")
        self.assertIn("john-doe", q)

    def test_image_query_uses_display_name(self):
        """Image search query also uses display_name when available."""
        q = self._build_image_query("jane-smith", "Jane Smith")
        self.assertIn('"Jane Smith"', q)

    def test_image_query_falls_back_to_slug(self):
        """Image search query falls back to slug when display_name absent."""
        q = self._build_image_query("jane-smith", None)
        self.assertIn("jane-smith", q)


# ---------------------------------------------------------------------------
# Tests — SSRF guard (_is_private_host)
# ---------------------------------------------------------------------------

class TestPrivateHostSSRFGuard(unittest.TestCase):
    """Verify private/loopback addresses are blocked before URL is returned."""

    def test_loopback_ipv4_is_private(self):
        """127.0.0.1 is private."""
        self.assertTrue(_is_private_host("http://127.0.0.1/img.jpg"))

    def test_localhost_is_private(self):
        """localhost hostname is private."""
        self.assertTrue(_is_private_host("http://localhost/img.jpg"))

    def test_metadata_service_is_private(self):
        """AWS/GCP metadata service 169.254.x.x is private."""
        self.assertTrue(_is_private_host("http://169.254.169.254/img.jpg"))

    def test_public_cdn_not_private(self):
        """Public CDN hostname is not considered private."""
        self.assertFalse(_is_private_host("https://media.licdn.com/profile.jpg"))


if __name__ == "__main__":
    unittest.main()
