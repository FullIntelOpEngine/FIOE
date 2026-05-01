# syntax=docker/dockerfile:1
# ============================================================
# FIOE — multi-stage Dockerfile
#
# Two build targets:
#   web    — lightweight web-frontend image (no heavy ML deps)
#   worker — full-fat worker image (adds pdfplumber, ML deps)
#
# Usage:
#   docker build --target web -t fioe-web .
#   docker build --target worker -t fioe-worker .
# ============================================================

# ── Base: shared system deps ────────────────────────────────
FROM python:3.12-slim AS base

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System packages needed by psycopg2-binary and Pillow
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        libgl1 \
    && rm -rf /var/lib/apt/lists/*

# ── Web: install only lightweight deps ─────────────────────
FROM base AS web-deps

COPY requirements.txt .

# Install everything except the heavy ML/PDF deps.
# Individual packages are pinned here for faster layer caching; requirements.txt
# is also installed so any additions there are picked up automatically.
RUN pip install \
        flask \
        flask-limiter \
        "gunicorn[gevent]" \
        gevent \
        requests \
        psycopg2-binary \
        google-generativeai \
        openpyxl \
        google-cloud-storage \
        google-cloud-tasks \
        redis \
        werkzeug \
        python-dotenv
RUN pip install -r requirements.txt

# ── Worker: everything in web-deps + heavy deps ────────────
FROM web-deps AS worker-deps

RUN pip install \
        pdfplumber \
        pillow

# ── Web final image ─────────────────────────────────────────
FROM web-deps AS web

COPY . .

# Default port matches local dev convention
ENV PORT=8091

EXPOSE ${PORT}

CMD ["gunicorn", "-c", "gunicorn.conf.py", "webbridge:app"]

# ── Worker final image ──────────────────────────────────────
FROM worker-deps AS worker

COPY . .

ENV PORT=8092

# Worker runs on a different port to avoid conflict if co-located
EXPOSE ${PORT}

CMD ["gunicorn", "-c", "gunicorn.conf.py", "webbridge:app"]
