"""Thin cache abstraction for FIOE.

Priority:
  1. Redis (Memorystore) when REDIS_URL is set.
  2. In-process dict fallback (default, zero config, per-replica).

Environment variables
---------------------
REDIS_URL               redis://host:port[/db]  — enables Redis backend.
SUGGEST_CACHE_TTL_SECONDS   int (default 3600)  — TTL for suggestion cache entries.
LLM_CACHE_TTL_SECONDS       int (default 86400) — TTL for LLM response cache entries.
"""

import json
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "")
SUGGEST_CACHE_TTL = int(os.getenv("SUGGEST_CACHE_TTL_SECONDS", "3600"))
LLM_CACHE_TTL = int(os.getenv("LLM_CACHE_TTL_SECONDS", "86400"))

# ---------------------------------------------------------------------------
# Redis client (lazy-initialised once)
# ---------------------------------------------------------------------------

_redis_client = None
_redis_init_lock = threading.Lock()
_redis_unavailable = False  # flip to True on first connection error to avoid retrying every call


def _get_redis():
    """Return a connected redis.Redis instance, or None if unavailable."""
    global _redis_client, _redis_unavailable
    if _redis_unavailable:
        return None
    if _redis_client is not None:
        return _redis_client
    if not REDIS_URL:
        return None
    with _redis_init_lock:
        if _redis_client is not None:
            return _redis_client
        try:
            import redis  # type: ignore
            client = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)
            client.ping()
            _redis_client = client
            logger.info("[cache_backend] Redis connected: %s", REDIS_URL)
            return _redis_client
        except Exception as exc:
            logger.warning("[cache_backend] Redis unavailable, using in-process dict: %s", exc)
            _redis_unavailable = True
            return None


# ---------------------------------------------------------------------------
# In-process fallback with TTL
# ---------------------------------------------------------------------------

_local_cache: dict = {}          # key -> (value, expires_at)
_local_lock = threading.Lock()


def _local_get(key: str):
    with _local_lock:
        entry = _local_cache.get(key)
    if entry is None:
        return None
    value, expires_at = entry
    if expires_at and time.monotonic() > expires_at:
        with _local_lock:
            _local_cache.pop(key, None)
        return None
    return value


def _local_set(key: str, value, ttl: int):
    expires_at = time.monotonic() + ttl if ttl else None
    with _local_lock:
        _local_cache[key] = (value, expires_at)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def cache_get(key: str):
    """Return the cached value for *key*, or None if absent / expired."""
    r = _get_redis()
    if r is not None:
        try:
            raw = r.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as exc:
            logger.warning("[cache_backend] Redis get error: %s", exc)
    return _local_get(key)


def cache_set(key: str, value, ttl: int = SUGGEST_CACHE_TTL):
    """Store *value* under *key* with the given TTL (seconds)."""
    r = _get_redis()
    if r is not None:
        try:
            r.setex(key, ttl, json.dumps(value, ensure_ascii=False))
            return
        except Exception as exc:
            logger.warning("[cache_backend] Redis set error: %s", exc)
    _local_set(key, value, ttl)
