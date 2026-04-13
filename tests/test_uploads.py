"""
test_uploads.py — Tests for file upload size limits, PDF detection, and semaphore.

Standalone stubs — no webbridge.py import required.

Run with:  pytest tests/test_uploads.py
"""
import io
import threading
import time
import unittest
from functools import wraps

from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Constants mirrored from webbridge.py
# ---------------------------------------------------------------------------

_MAX_CONTENT_LENGTH = 80 * 1024 * 1024   # 80 MB
_SINGLE_FILE_MAX    =  6 * 1024 * 1024   #  6 MB


# ---------------------------------------------------------------------------
# Inline _is_pdf_bytes (exact copy from webbridge.py)
# ---------------------------------------------------------------------------

def _is_pdf_bytes(b: bytes) -> bool:
    """Return True only if b starts with the PDF magic bytes (%PDF-)."""
    return isinstance(b, (bytes, bytearray)) and len(b) >= 5 and b[:5] == b'%PDF-'


# ---------------------------------------------------------------------------
# Minimal upload app
# ---------------------------------------------------------------------------

def _build_upload_app(semaphore: threading.Semaphore):
    app = Flask(__name__)
    app.config["TESTING"] = True
    app.config["MAX_CONTENT_LENGTH"] = _MAX_CONTENT_LENGTH

    @app.post("/upload/single")
    def upload_single():
        f = request.files.get("file")
        if f is None:
            return jsonify({"error": "No file"}), 400
        data = f.read()
        if len(data) > _SINGLE_FILE_MAX:
            return jsonify({"error": "File too large", "max_bytes": _SINGLE_FILE_MAX}), 413
        if not _is_pdf_bytes(data):
            return jsonify({"error": "Not a PDF"}), 415
        return jsonify({"ok": True, "size": len(data)}), 200

    @app.post("/upload/analyze")
    def upload_analyze():
        """Endpoint that uses the semaphore to limit concurrent analysis."""
        acquired = semaphore.acquire(blocking=False)
        if not acquired:
            return jsonify({"error": "Server busy, try again later"}), 503
        try:
            time.sleep(0.01)  # simulate work
            return jsonify({"ok": True}), 200
        finally:
            semaphore.release()

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPdfDetection(unittest.TestCase):

    def test_pdf_detection_valid(self):
        """_is_pdf_bytes recognises %PDF- magic bytes."""
        data = b"%PDF-1.4\n%some pdf content"
        self.assertTrue(_is_pdf_bytes(data))

    def test_pdf_detection_invalid(self):
        """_is_pdf_bytes rejects non-PDF bytes."""
        self.assertFalse(_is_pdf_bytes(b"\x89PNG\r\n"))
        self.assertFalse(_is_pdf_bytes(b"JFIF"))
        self.assertFalse(_is_pdf_bytes(b""))

    def test_pdf_detection_short(self):
        """_is_pdf_bytes returns False for data shorter than 5 bytes."""
        self.assertFalse(_is_pdf_bytes(b"%PDF"))

    def test_pdf_detection_bytearray(self):
        """_is_pdf_bytes works with bytearray too."""
        self.assertTrue(_is_pdf_bytes(bytearray(b"%PDF-1.5")))

    def test_pdf_detection_none(self):
        """_is_pdf_bytes returns False for None input."""
        self.assertFalse(_is_pdf_bytes(None))  # type: ignore[arg-type]

    def test_pdf_detection_string(self):
        """_is_pdf_bytes returns False for a plain string (not bytes)."""
        self.assertFalse(_is_pdf_bytes("%PDF-1.4"))  # type: ignore[arg-type]


class TestUploadSizeLimits(unittest.TestCase):

    def setUp(self):
        sem = threading.Semaphore(4)
        self.app = _build_upload_app(sem)
        self.client = self.app.test_client()

    def _make_pdf(self, size_bytes: int) -> bytes:
        """Return a fake PDF blob of the given size."""
        header = b"%PDF-1.4\n"
        padding = b"x" * max(0, size_bytes - len(header))
        return header + padding

    def test_pdf_upload_ok(self):
        """Valid PDF under size limit → 200."""
        data = self._make_pdf(1024)
        resp = self.client.post(
            "/upload/single",
            data={"file": (io.BytesIO(data), "test.pdf")},
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 200)

    def test_single_file_max(self):
        """Single-file > _SINGLE_FILE_MAX → 413."""
        data = self._make_pdf(_SINGLE_FILE_MAX + 1)
        resp = self.client.post(
            "/upload/single",
            data={"file": (io.BytesIO(data), "big.pdf")},
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 413)

    def test_non_pdf_rejected(self):
        """Non-PDF file → 415."""
        data = b"PK\x03\x04" + b"0" * 100  # ZIP header
        resp = self.client.post(
            "/upload/single",
            data={"file": (io.BytesIO(data), "file.zip")},
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 415)


class TestCVSemaphore(unittest.TestCase):

    def test_cv_semaphore(self):
        """Semaphore limits concurrent CV analysis to configured cap."""
        limit = 4
        sem = threading.Semaphore(limit)
        app = _build_upload_app(sem)
        client = app.test_client()

        # Manually drain the semaphore
        for _ in range(limit):
            sem.acquire()

        # Now the semaphore is exhausted — next request should get 503
        resp = client.post("/upload/analyze")
        self.assertEqual(resp.status_code, 503)

        # Release slots so the semaphore recovers
        for _ in range(limit):
            sem.release()

        # After recovery, request succeeds
        resp = client.post("/upload/analyze")
        self.assertEqual(resp.status_code, 200)


if __name__ == "__main__":
    unittest.main()
