#!/usr/bin/env python3
"""Celery worker entrypoint for the SEO Engine.

Usage
-----
Run with all queues (recommended for development):
    python -m queue.worker

Run specific queues (recommended for production):
    celery -A queue.celery_app worker -Q analysis,execution --loglevel=info
    celery -A queue.celery_app worker -Q learning,monitoring --loglevel=info

Environment variables
---------------------
  REDIS_URL        Redis connection string (default: redis://localhost:6379/0)
  CELERY_CONCURRENCY   Number of worker processes (default: CPU count)
  CELERY_LOGLEVEL      Log level: debug|info|warning|error (default: info)
"""

from __future__ import annotations

import os
import sys

# Ensure project root is on the path when running as `python -m queue.worker`
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from queue.celery_app import app  # noqa: E402 — must come after sys.path fixup


def main() -> None:
    """Start a Celery worker consuming all queues."""
    concurrency = os.environ.get("CELERY_CONCURRENCY", "")
    loglevel = os.environ.get("CELERY_LOGLEVEL", "info")

    argv = [
        "worker",
        "--queues", "analysis,execution,learning,monitoring",
        "--loglevel", loglevel,
        "--without-gossip",
        "--without-mingle",
        "--without-heartbeat",
    ]
    if concurrency:
        argv += ["--concurrency", concurrency]

    app.worker_main(argv=argv)


if __name__ == "__main__":
    main()
