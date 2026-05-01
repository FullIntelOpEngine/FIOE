"""Smoke tests for the CV process-pool offload logic.

Tests exercise analyze_cv_bytes_offload via a self-contained harness that
does NOT import Flask/psycopg2.  The harness reimplements the env-var
defaults and the offload function so the tests validate the logic paths
(fallback, cpu-limit gate, sync path, pool error) without needing any
real PDF, database, or Gemini API.

Run with:  pytest tests/test_cv_offload.py -v
"""
import concurrent.futures
import threading
import time
import types
import logging
import pytest


# ---------------------------------------------------------------------------
# Self-contained harness — mirrors webbridge_cv offload logic exactly
# ---------------------------------------------------------------------------

def _build_harness(
    workers=1,
    max_concurrency=1,
    offload_timeout=5.0,
    use_queue=False,
    hourly_cpu_limit=0.0,
):
    """Return a module-like namespace containing the offload function and state."""
    ns = types.SimpleNamespace()
    ns.logger = logging.getLogger("test_cv_offload")
    ns.CV_ANALYZE_WORKERS = workers
    ns.CV_ANALYZE_OFFLOAD_TIMEOUT = offload_timeout
    ns.CV_ANALYZE_USE_QUEUE = use_queue
    ns.CV_ANALYZE_HOURLY_CPU_LIMIT = hourly_cpu_limit
    ns._CV_ANALYZE_SEMAPHORE = threading.Semaphore(max_concurrency)
    ns._CV_ANALYZE_PROCESS_POOL = None
    ns._CV_POOL_LOCK = threading.Lock()
    ns._cv_cpu_seconds_total = 0.0
    ns._cv_cpu_window_start = time.monotonic()
    ns._cv_cpu_lock = threading.Lock()

    # Default sync: returns None (callers should handle)
    ns._analyze_cv_bytes_sync = lambda pdf_bytes: None

    def _record_cv_cpu(duration_seconds):
        with ns._cv_cpu_lock:
            now = time.monotonic()
            if now - ns._cv_cpu_window_start >= 3600:
                ns._cv_cpu_seconds_total = 0.0
                ns._cv_cpu_window_start = now
            ns._cv_cpu_seconds_total += duration_seconds

    ns._record_cv_cpu = _record_cv_cpu

    def analyze_cv_bytes_offload(pdf_bytes, timeout=None):
        effective_timeout = timeout if timeout is not None else ns.CV_ANALYZE_OFFLOAD_TIMEOUT
        size_bytes = len(pdf_bytes)
        t_start = time.perf_counter()

        # Cost guard
        if ns.CV_ANALYZE_HOURLY_CPU_LIMIT > 0:
            _limit_exceeded = False
            with ns._cv_cpu_lock:
                now_m = time.monotonic()
                if now_m - ns._cv_cpu_window_start >= 3600:
                    ns._cv_cpu_seconds_total = 0.0
                    ns._cv_cpu_window_start = now_m
                if ns._cv_cpu_seconds_total >= ns.CV_ANALYZE_HOURLY_CPU_LIMIT:
                    _limit_exceeded = True
            if _limit_exceeded:
                ns.logger.warning("cv_analysis_fallback_sync: hourly_cpu_limit_exceeded")
                result = ns._analyze_cv_bytes_sync(pdf_bytes)
                _record_cv_cpu(time.perf_counter() - t_start)
                return result

        acquired = ns._CV_ANALYZE_SEMAPHORE.acquire(timeout=1)
        if not acquired:
            if ns.CV_ANALYZE_USE_QUEUE:
                ns.logger.info("cv_analysis_queued")
            ns.logger.warning("cv_analysis_fallback_sync: semaphore_timeout")
            result = ns._analyze_cv_bytes_sync(pdf_bytes)
            _record_cv_cpu(time.perf_counter() - t_start)
            return result

        try:
            ns.logger.info("cv_analysis_start size=%d" % size_bytes)
            with ns._CV_POOL_LOCK:
                if ns._CV_ANALYZE_PROCESS_POOL is None:
                    try:
                        ns._CV_ANALYZE_PROCESS_POOL = concurrent.futures.ProcessPoolExecutor(
                            max_workers=ns.CV_ANALYZE_WORKERS
                        )
                    except Exception as pool_err:
                        ns.logger.warning("cv_pool_create_failed: %s" % pool_err)
                        ns._CV_ANALYZE_PROCESS_POOL = None

            pool = ns._CV_ANALYZE_PROCESS_POOL
            if pool is None:
                raise RuntimeError("ProcessPoolExecutor creation failed")

            future = pool.submit(ns._analyze_cv_bytes_sync, pdf_bytes)
            result = future.result(timeout=effective_timeout)
            elapsed_ms = (time.perf_counter() - t_start) * 1000
            ns.logger.info("cv_analysis_end duration_ms=%.1f" % elapsed_ms)
            _record_cv_cpu(elapsed_ms / 1000)
            return result

        except (
            concurrent.futures.TimeoutError,
            concurrent.futures.process.BrokenProcessPool,
            OSError,
            RuntimeError,
        ) as exc:
            ns.logger.warning("cv_analysis_fallback_sync: %s" % type(exc).__name__)
            if isinstance(exc, concurrent.futures.process.BrokenProcessPool):
                with ns._CV_POOL_LOCK:
                    ns._CV_ANALYZE_PROCESS_POOL = None
            result = ns._analyze_cv_bytes_sync(pdf_bytes)
            _record_cv_cpu(time.perf_counter() - t_start)
            return result
        except Exception as exc:
            ns.logger.warning("cv_analysis_fallback_sync: %s" % type(exc).__name__)
            result = ns._analyze_cv_bytes_sync(pdf_bytes)
            _record_cv_cpu(time.perf_counter() - t_start)
            return result
        finally:
            ns._CV_ANALYZE_SEMAPHORE.release()

    ns.analyze_cv_bytes_offload = analyze_cv_bytes_offload
    return ns


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_offload_returns_none_for_invalid_pdf():
    """When sync returns None, offload must return None (not raise)."""
    h = _build_harness()
    h._analyze_cv_bytes_sync = lambda b: None
    result = h.analyze_cv_bytes_offload(b"not-a-pdf")
    assert result is None


def test_offload_returns_dict_on_success():
    """When sync returns a dict, offload returns that dict unchanged."""
    h = _build_harness()
    expected = {"skillset": ["Python"], "total_experience_years": 5}
    h._analyze_cv_bytes_sync = lambda b: expected

    # Use a mock pool so no real subprocess is spawned (lambdas are not picklable)
    class _GoodFuture:
        def result(self, timeout=None):
            return expected

    class _GoodPool:
        def submit(self, fn, *args, **kw):
            return _GoodFuture()

    h._CV_ANALYZE_PROCESS_POOL = _GoodPool()
    result = h.analyze_cv_bytes_offload(b"fake-pdf-bytes")
    assert result == expected


def test_offload_falls_back_on_runtime_error():
    """Pool creation/submission error → sync fallback → dict returned."""
    h = _build_harness()
    expected = {"skillset": ["Java"], "total_experience_years": 3}
    h._analyze_cv_bytes_sync = lambda b: expected

    # Force pool submission to raise
    class _BadPool:
        def submit(self, *a, **kw):
            raise RuntimeError("simulated pool error")

    h._CV_ANALYZE_PROCESS_POOL = _BadPool()
    result = h.analyze_cv_bytes_offload(b"fake-pdf-bytes")
    assert result == expected


def test_offload_falls_back_on_timeout():
    """TimeoutError during future.result() → sync fallback → dict returned."""
    h = _build_harness(offload_timeout=0.001)
    expected = {"name": "Alice", "total_experience_years": 7}
    h._analyze_cv_bytes_sync = lambda b: expected

    # Pool that submits a future that always times out
    class _SlowFuture:
        def result(self, timeout=None):
            raise concurrent.futures.TimeoutError()

    class _SlowPool:
        def submit(self, *a, **kw):
            return _SlowFuture()

    h._CV_ANALYZE_PROCESS_POOL = _SlowPool()
    result = h.analyze_cv_bytes_offload(b"fake-pdf-bytes")
    assert result == expected


def test_cpu_limit_triggers_sync_fallback():
    """Hourly CPU limit already exceeded → sync path used, dict returned."""
    h = _build_harness(hourly_cpu_limit=0.001)
    expected = {"skillset": [], "total_experience_years": 0}
    h._analyze_cv_bytes_sync = lambda b: expected
    h._cv_cpu_seconds_total = 1.0  # already exceeded
    result = h.analyze_cv_bytes_offload(b"fake-pdf-bytes")
    assert result == expected


def test_semaphore_unavailable_falls_back():
    """All semaphore slots taken → sync fallback (not a hang)."""
    h = _build_harness(max_concurrency=1)
    expected = {"skillset": ["Go"], "total_experience_years": 2}
    h._analyze_cv_bytes_sync = lambda b: expected
    # Consume the only slot
    h._CV_ANALYZE_SEMAPHORE.acquire()
    try:
        result = h.analyze_cv_bytes_offload(b"fake-pdf-bytes")
        assert result == expected
    finally:
        h._CV_ANALYZE_SEMAPHORE.release()


def test_broken_pool_is_reset():
    """BrokenProcessPool causes pool to be cleared so it is recreated next call."""
    h = _build_harness()
    expected = {"skillset": ["Rust"], "total_experience_years": 1}
    h._analyze_cv_bytes_sync = lambda b: expected

    class _BrokenPool:
        def submit(self, *a, **kw):
            raise concurrent.futures.process.BrokenProcessPool("simulated")

    h._CV_ANALYZE_PROCESS_POOL = _BrokenPool()
    result = h.analyze_cv_bytes_offload(b"fake-pdf-bytes")
    assert result == expected
    assert h._CV_ANALYZE_PROCESS_POOL is None  # pool was cleared


def test_cpu_counter_increments():
    """_record_cv_cpu accumulates CPU time."""
    h = _build_harness()
    h._cv_cpu_seconds_total = 0.0
    h._record_cv_cpu(2.5)
    assert h._cv_cpu_seconds_total == pytest.approx(2.5)
    h._record_cv_cpu(1.0)
    assert h._cv_cpu_seconds_total == pytest.approx(3.5)
