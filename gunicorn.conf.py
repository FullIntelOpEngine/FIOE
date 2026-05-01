"""Gunicorn configuration for FIOE webbridge.

Tune via environment variables — no code changes required to adjust concurrency.

Key variables
-------------
GUNICORN_WORKERS          Number of worker processes (default: 1).
                          For a pure-IO web frontend, 1 gevent worker with many
                          concurrent connections is usually optimal on Cloud Run.
                          Increase for CPU-bound workloads.
GUNICORN_WORKER_CLASS     Worker class (default: gevent).
                          Set to "sync" to revert to the classic threading model.
GUNICORN_CONNECTIONS      Max greenlet connections per gevent worker (default: 100).
PORT                      Bind port (default: 8091, matches local dev default).
"""

import os

workers = int(os.getenv("GUNICORN_WORKERS", "1"))
worker_class = os.getenv("GUNICORN_WORKER_CLASS", "gevent")
worker_connections = int(os.getenv("GUNICORN_CONNECTIONS", "100"))
bind = f"0.0.0.0:{os.getenv('PORT', '8091')}"
timeout = int(os.getenv("GUNICORN_TIMEOUT", "300"))
keepalive = 5
preload_app = os.getenv("GUNICORN_PRELOAD", "1") == "1"
accesslog = "-"   # stdout
errorlog = "-"    # stderr
loglevel = os.getenv("GUNICORN_LOG_LEVEL", "info")
