"""Celery application configuration for the SEO Engine task queue.

Broker and result backend are both Redis.  Set the REDIS_URL environment
variable to override the default (redis://localhost:6379/0).

Queues
------
  analysis    — brain analysis, orchestration, decay scans
  execution   — task execution, IndexNow submissions
  learning    — weekly/monthly learning cycles
  monitoring  — rank checks, AI citation monitoring

Beat schedule (periodic tasks)
-------------------------------
  run-daily-analysis   every 24 h  → queue.tasks.daily_analysis_cycle
  run-rank-check       every 168 h → queue.tasks.check_rankings
  run-content-decay    every 72 h  → queue.tasks.scan_content_decay
  run-learning-cycle   every 168 h → queue.tasks.run_learning
"""

from __future__ import annotations

import os
from celery import Celery
from celery.schedules import crontab
from kombu import Queue, Exchange

# ---------------------------------------------------------------------------
# Redis connection
# ---------------------------------------------------------------------------
REDIS_URL: str = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------
app = Celery("seo_engine")

app.conf.update(
    # Transport
    broker_url=REDIS_URL,
    result_backend=REDIS_URL,

    # Serialisation
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # Timezone
    timezone="UTC",
    enable_utc=True,

    # Time limits (seconds)
    task_soft_time_limit=300,
    task_time_limit=600,

    # Retry defaults (tasks may override per-call)
    task_max_retries=3,
    task_acks_late=True,
    task_reject_on_worker_lost=True,

    # Result expiry — keep results for 7 days
    result_expires=604800,

    # Worker concurrency / prefetch
    worker_prefetch_multiplier=1,

    # Queues
    task_queues=[
        Queue("analysis",   Exchange("analysis"),   routing_key="analysis"),
        Queue("execution",  Exchange("execution"),  routing_key="execution"),
        Queue("learning",   Exchange("learning"),   routing_key="learning"),
        Queue("monitoring", Exchange("monitoring"), routing_key="monitoring"),
    ],
    task_default_queue="analysis",
    task_default_exchange="analysis",
    task_default_routing_key="analysis",

    # Explicit task → queue routing
    task_routes={
        "queue.tasks.analyze_business":     {"queue": "analysis"},
        "queue.tasks.orchestrate_business": {"queue": "analysis"},
        "queue.tasks.daily_analysis_cycle": {"queue": "analysis"},
        "queue.tasks.execute_seo_task":     {"queue": "execution"},
        "queue.tasks.submit_to_indexnow":   {"queue": "execution"},
        "queue.tasks.run_learning":         {"queue": "learning"},
        "queue.tasks.check_rankings":       {"queue": "monitoring"},
        "queue.tasks.scan_content_decay":   {"queue": "monitoring"},
        "queue.tasks.monitor_ai_citations": {"queue": "monitoring"},
    },

    # Beat schedule — all times in UTC
    beat_schedule={
        "run-daily-analysis": {
            "task": "queue.tasks.daily_analysis_cycle",
            "schedule": 86400,          # 24 h in seconds
            "options": {"queue": "analysis"},
        },
        "run-rank-check": {
            "task": "queue.tasks.check_rankings",
            "schedule": 604800,         # 168 h / 7 days
            "options": {"queue": "monitoring"},
        },
        "run-content-decay": {
            "task": "queue.tasks.scan_content_decay",
            "schedule": 259200,         # 72 h / 3 days
            "options": {"queue": "monitoring"},
        },
        "run-learning-cycle": {
            "task": "queue.tasks.run_learning",
            "schedule": 604800,         # 168 h / 7 days
            "options": {"queue": "learning"},
        },
    },
)

# Auto-discover tasks in the queue package
app.autodiscover_tasks(["queue"])
