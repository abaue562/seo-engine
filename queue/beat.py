#!/usr/bin/env python3
"""Celery beat scheduler entrypoint for the SEO Engine.

The beat process fires periodic tasks according to the schedule defined in
``queue/celery_app.py``.  Only ONE beat process should run at a time.

Usage
-----
    python -m queue.beat

Or via the Celery CLI:
    celery -A queue.celery_app beat --loglevel=info

With a custom schedule database (useful to persist last-run times):
    celery -A queue.celery_app beat --loglevel=info \
           --scheduler django_celery_beat.schedulers:DatabaseScheduler

Scheduled tasks (from celery_app.py beat_schedule)
---------------------------------------------------
  run-daily-analysis   every 24 h  → queue.tasks.daily_analysis_cycle
  run-rank-check       every 168 h → queue.tasks.check_rankings   (weekly)
  run-content-decay    every 72 h  → queue.tasks.scan_content_decay (3 days)
  run-learning-cycle   every 168 h → queue.tasks.run_learning      (weekly)

Environment variables
---------------------
  REDIS_URL        Redis connection string (default: redis://localhost:6379/0)
  CELERY_LOGLEVEL  Log level: debug|info|warning|error (default: info)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure project root is on the path when running as `python -m queue.beat`
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from queue.celery_app import app  # noqa: E402 — must come after sys.path fixup


def main() -> None:
    """Start the Celery beat scheduler."""
    loglevel = os.environ.get("CELERY_LOGLEVEL", "info")

    argv = [
        "beat",
        "--loglevel", loglevel,
        "--scheduler", "celery.beat:PersistentScheduler",
        "--schedule", "celerybeat-schedule",  # SQLite DB for last-run tracking
    ]

    app.start(argv=argv)


if __name__ == "__main__":
    main()
