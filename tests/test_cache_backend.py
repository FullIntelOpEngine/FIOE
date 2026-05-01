"""Tests for cache_backend.py — verifies the in-process dict fallback works
correctly when Redis is not configured (the common local dev / CI scenario).
"""
import os
import time
import importlib

import pytest


# ---------------------------------------------------------------------------
# Fixture: reload cache_backend with no REDIS_URL so it stays in-process
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_cache_backend(monkeypatch):
    """Ensure each test starts with a clean cache_backend module state."""
    monkeypatch.delenv("REDIS_URL", raising=False)
    import cache_backend
    # Reset module-level state
    cache_backend._redis_client = None
    cache_backend._redis_unavailable = False
    with cache_backend._local_lock:
        cache_backend._local_cache.clear()
    yield
    with cache_backend._local_lock:
        cache_backend._local_cache.clear()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_cache_miss_returns_none():
    from cache_backend import cache_get
    assert cache_get("nonexistent_key_xyz") is None


def test_set_and_get_roundtrip():
    from cache_backend import cache_get, cache_set
    cache_set("test_key", {"foo": "bar"}, ttl=60)
    result = cache_get("test_key")
    assert result == {"foo": "bar"}


def test_set_and_get_string():
    from cache_backend import cache_get, cache_set
    cache_set("string_key", "hello world", ttl=60)
    assert cache_get("string_key") == "hello world"


def test_set_and_get_list():
    from cache_backend import cache_get, cache_set
    cache_set("list_key", [1, 2, 3], ttl=60)
    assert cache_get("list_key") == [1, 2, 3]


def test_expired_entry_returns_none():
    """Entry should be evicted after its TTL expires."""
    from cache_backend import cache_get, cache_set
    cache_set("expiring_key", "value", ttl=0)   # ttl=0 → no expiry set
    # With ttl=0 the _local_set call sets expires_at = monotonic() + 0 which
    # is instantly expired.  Let's use a negative internal to verify expiry.
    import cache_backend
    import time as _time
    cache_backend._local_cache["expiring_key2"] = ("val2", _time.monotonic() - 1)
    assert cache_get("expiring_key2") is None


def test_overwrite_existing():
    from cache_backend import cache_get, cache_set
    cache_set("k", "first", ttl=60)
    cache_set("k", "second", ttl=60)
    assert cache_get("k") == "second"


def test_get_redis_returns_none_without_redis_url():
    """Without REDIS_URL the redis client should never be initialised."""
    from cache_backend import _get_redis
    assert _get_redis() is None


def test_suggest_cache_ttl_default():
    """SUGGEST_CACHE_TTL should default to 3600 when env var is absent."""
    from cache_backend import SUGGEST_CACHE_TTL
    assert SUGGEST_CACHE_TTL == 3600


def test_llm_cache_ttl_default():
    from cache_backend import LLM_CACHE_TTL
    assert LLM_CACHE_TTL == 86400


def test_custom_ttl_from_env(monkeypatch):
    monkeypatch.setenv("SUGGEST_CACHE_TTL_SECONDS", "120")
    import cache_backend as cb
    importlib.reload(cb)
    assert cb.SUGGEST_CACHE_TTL == 120
    # Restore to avoid affecting other tests
    importlib.reload(cb)
