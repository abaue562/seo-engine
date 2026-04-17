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
  run-daily-analysis   every 24 h  → taskq.tasks.daily_analysis_cycle
  run-rank-check       every 168 h → taskq.tasks.check_rankings
  run-content-decay    every 72 h  → taskq.tasks.scan_content_decay
  run-learning-cycle   every 168 h → taskq.tasks.run_learning
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
    result_expires=86400,  # 24h TTL (P1 bloat fix)

    # Worker concurrency / prefetch
    worker_prefetch_multiplier=1,

    # Queues (dead-letter queue routes failed tasks back to analysis for retry)
    task_queues=[
        Queue("analysis",   Exchange("analysis"),   routing_key="analysis"),
        Queue("execution",  Exchange("execution"),  routing_key="execution"),
        Queue("learning",   Exchange("learning"),   routing_key="learning"),
        Queue("monitoring", Exchange("monitoring"), routing_key="monitoring"),
        # Dead-letter queue — tasks that exhausted all retries land here
        Queue("dead_letter", Exchange("dead_letter"), routing_key="dead_letter"),
    ],
    task_default_queue="analysis",
    task_default_exchange="analysis",
    task_default_routing_key="analysis",

    # Explicit task → queue routing
    task_routes={
        # Analysis
        "taskq.tasks.analyze_business":      {"queue": "analysis"},
        "taskq.tasks.orchestrate_business":  {"queue": "analysis"},
        "taskq.tasks.daily_analysis_cycle":  {"queue": "analysis"},
        "taskq.tasks._execute_top_task":     {"queue": "execution"},
        "taskq.tasks.run_topical_gap_check": {"queue": "analysis"},
        # Content pipeline
        "taskq.tasks.run_content_pipeline":  {"queue": "execution"},
        "taskq.tasks.generate_content":      {"queue": "execution"},
        "taskq.tasks.publish_content":       {"queue": "execution"},
        "taskq.tasks.inject_internal_links": {"queue": "execution"},
        "taskq.tasks.indexnow_and_track":    {"queue": "execution"},
        # Existing execution tasks
        "taskq.tasks.execute_seo_task":      {"queue": "execution"},
        "taskq.tasks.submit_to_indexnow":    {"queue": "execution"},
        # Learning
        "taskq.tasks.run_learning":          {"queue": "learning"},
        "taskq.tasks.run_feedback_loop":     {"queue": "learning"},
        # Monitoring
        "taskq.tasks.check_rankings":        {"queue": "monitoring"},
        "taskq.tasks.scan_content_decay":    {"queue": "monitoring"},
        "taskq.tasks.monitor_ai_citations":  {"queue": "monitoring"},
        "taskq.tasks.run_citation_monitor":  {"queue": "monitoring"},
        "taskq.tasks.run_cwv_audit":         {"queue": "monitoring"},
        "taskq.tasks.send_daily_summary":    {"queue": "monitoring"},
        "taskq.tasks.send_ranking_report": {"queue": "monitoring"},
        "taskq.tasks.submit_sitemap": {"queue": "execution"},
        "taskq.tasks.run_gbp_posts":          {"queue": "execution"},
        "taskq.tasks.run_citation_builder":   {"queue": "execution"},
        "taskq.tasks.run_wikidata_sync":       {"queue": "analysis"},
        # New Phase 2-14 tasks
        "taskq.tasks.run_programmatic_batch": {"queue": "execution"},
        "taskq.tasks.run_haro_check":         {"queue": "execution"},
        "taskq.tasks.run_link_reclamation":   {"queue": "execution"},
        "taskq.tasks.check_indexing_queue":   {"queue": "execution"},
        "taskq.tasks.run_system_health":      {"queue": "monitoring"},
        "taskq.tasks.run_orphan_detection":   {"queue": "analysis"},
        "taskq.tasks.sync_aion_signals":           {"queue": "analysis"},
        # Phase 3 — AION wiring
        "taskq.tasks.sync_twitter_intel":          {"queue": "analysis"},
        "taskq.tasks.auto_content_briefs":         {"queue": "analysis"},
        "taskq.tasks.deploy_llms_txt":             {"queue": "execution"},
        "taskq.tasks.sync_entity_knowledge_graph": {"queue": "analysis"},
        "taskq.tasks.competitor_content_alerts":   {"queue": "monitoring"},
    },

    # Beat schedule — all times in UTC
    # Jitter is applied in task implementations to avoid thundering herd
    beat_schedule={
        "run-daily-analysis": {
            "task": "taskq.tasks.daily_analysis_cycle",
            "schedule": 86400,          # 24 h
            "options": {"queue": "analysis"},
        },
        "run-rank-check": {
            "task": "taskq.tasks.check_rankings",
            "schedule": 604800,         # 7 days
            "options": {"queue": "monitoring"},
        },
        "run-content-decay": {
            "task": "taskq.tasks.scan_content_decay",
            "schedule": 259200,         # 3 days
            "options": {"queue": "monitoring"},
        },
        "run-learning-cycle": {
            "task": "taskq.tasks.run_learning",
            "schedule": 604800,         # 7 days
            "options": {"queue": "learning"},
        },
        # ── New schedules from dominance audit ─────────────────────────
        "run-citation-monitoring": {
            "task": "taskq.tasks.run_citation_monitor",
            "schedule": 604800,         # 7 days — weekly AI citation check
            "options": {"queue": "monitoring"},
        },
        "run-cwv-audit": {
            "task": "taskq.tasks.run_cwv_audit",
            "schedule": 604800,         # 7 days — weekly PageSpeed check
            "options": {"queue": "monitoring"},
        },
        "run-topical-gap-check": {
            "task": "taskq.tasks.run_topical_gap_check",
            "schedule": 604800,         # 7 days — weekly topical coverage check
            "options": {"queue": "analysis"},
        },
        "run-daily-alert-summary": {
            "task": "taskq.tasks.send_daily_summary",
            "schedule": 86400,          # 24 h — daily alert digest
            "options": {"queue": "monitoring"},
        },
        # ── Phase 2-14 new schedules ───────────────────────────────────────
        "run-haro-check": {
            "task": "taskq.tasks.run_haro_check",
            "schedule": 28800,          # 8 h — matches HARO digest cadence
            "options": {"queue": "execution"},
        },
        "run-system-health": {
            "task": "taskq.tasks.run_system_health",
            "schedule": 900,            # 15 min — critical infrastructure check
            "options": {"queue": "monitoring"},
        },
        "check-indexing-queue": {
            "task": "taskq.tasks.check_indexing_queue",
            "schedule": 3600,           # 1 h — submit pending URLs to Google+Bing
            "options": {"queue": "execution"},
        },
        "run-link-reclamation": {
            "task": "taskq.tasks.run_link_reclamation",
            "schedule": 604800,         # 7 days — weekly link reclamation outreach
            "options": {"queue": "execution"},
        },
        "run-orphan-detection": {
            "task": "taskq.tasks.run_orphan_detection",
            "schedule": 259200,         # 3 days — internal link health check
            "options": {"queue": "analysis"},
        },
        "sync-aion-signals": {
            "task": "taskq.tasks.sync_aion_signals",
            "schedule": 21600,          # 6 hours — AION Research Aggregator signals
            "options": {"queue": "analysis"},
        },
        "send-ranking-report": {
            "task": "taskq.tasks.send_ranking_report",
            "schedule": 604800,
            "options": {"queue": "monitoring"},
        },
        "submit-sitemap": {
            "task": "taskq.tasks.submit_sitemap",
            "schedule": 604800,
            "options": {"queue": "execution"},
        },
        "run-wikidata-sync": {
            "task": "taskq.tasks.run_wikidata_sync",
            "schedule": 604800,          # 7 days
            "options": {"queue": "analysis"},
        },
        "run-gbp-posts": {
            "task": "taskq.tasks.run_gbp_posts",
            "schedule": 604800,         # 7 days
            "options": {"queue": "execution"},
        },
        # ── Phase 3 — AION wiring schedules ───────────────────────────────
        "sync-twitter-intel": {
            "task": "taskq.tasks.sync_twitter_intel",
            "schedule": 14400,          # 4 hours — Twitter Intel market signals
            "options": {"queue": "analysis"},
        },
        "auto-content-briefs": {
            "task": "taskq.tasks.auto_content_briefs",
            "schedule": 86400,          # 24 hours — auto-generate content briefs
            "options": {"queue": "analysis"},
        },
        "deploy-llms-txt": {
            "task": "taskq.tasks.deploy_llms_txt",
            "schedule": 604800,         # 7 days — weekly llms.txt deployment
            "options": {"queue": "execution"},
        },
        "sync-entity-knowledge-graph": {
            "task": "taskq.tasks.sync_entity_knowledge_graph",
            "schedule": 43200,          # 12 hours — entity graph sync
            "options": {"queue": "analysis"},
        },
        "run-programmatic-batch": {
            "task": "taskq.tasks.run_programmatic_batch",
            "schedule": 86400,          # 24 hours — publish 5 programmatic pages/day
            "options": {"queue": "execution"},
        },

        "run-freshness-injector": {
            "task": "taskq.tasks.inject_content_freshness",
            "schedule": 604800,          # 7 days — update stale articles with new stats
            "options": {"queue": "execution"},
        },
        "run-medium-syndication": {
            "task": "taskq.tasks.syndicate_to_medium",
            "schedule": 86400,           # daily — syndicate articles >7 days old
            "options": {"queue": "execution"},
        },
        "run-reddit-answer-queue": {
            "task": "taskq.tasks.run_reddit_answer_agent",
            "schedule": 604800,          # 7 days — find & queue Reddit answers for review
            "options": {"queue": "execution"},
        },
        "run-gsc-data-pull": {
            "task": "taskq.tasks.pull_gsc_data",
            "schedule": 86400,           # daily — pull GSC clicks/impressions/CTR
            "options": {"queue": "analysis"},
        },
        "run-review-solicitation": {
            "task": "taskq.tasks.send_review_requests",
            "schedule": 86400,           # daily — send post-job review request emails
            "options": {"queue": "execution"},
        },
        "run-citation-batch": {
            "task": "taskq.tasks.run_citation_builder",
            "schedule": 604800,          # 7 days — submit next 3 pending citation directories
            "options": {"queue": "execution"},
        },
        "competitor-content-alerts": {
            "task": "taskq.tasks.competitor_content_alerts",
            "schedule": 86400,          # 24 hours — competitor content monitoring
            "options": {"queue": "monitoring"},
        },
    },
)

# Auto-discover tasks in the queue package
app.autodiscover_tasks(["taskq"])


# ---------------------------------------------------------------------------
# Dead-letter routing — when a task exhausts max_retries it is re-published
# to the dead_letter queue instead of being silently dropped.
# ---------------------------------------------------------------------------
from celery.signals import task_failure  # noqa: E402


@task_failure.connect
def handle_task_failure(sender=None, task_id=None, exception=None,
                        traceback=None, einfo=None, **kwargs):
    """On final failure (no retries left), publish to dead_letter queue."""
    import logging
    import json
    from datetime import datetime, timezone

    _log = logging.getLogger(__name__)

    # Check if this task still has retries available
    request = kwargs.get("request") or getattr(sender, "request", None)
    if request:
        retries = getattr(request, "retries", 0)
        max_retries = getattr(sender, "max_retries", 3)
        if retries < max_retries:
            return  # still has retries left, don't dead-letter yet

    task_name = getattr(sender, "name", "unknown")
    _log.error(
        "task.dead_letter  task_id=%s  task=%s  exc=%s",
        task_id, task_name, str(exception)[:200],
    )

    # Write failure record to disk so it can be inspected / replayed
    from pathlib import Path
    dead_dir = Path("data/storage/dead_letter")
    dead_dir.mkdir(parents=True, exist_ok=True)
    record = {
        "task_id":   task_id,
        "task_name": task_name,
        "exception": str(exception),
        "failed_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (dead_dir / f"{task_id}.json").write_text(json.dumps(record, indent=2))

    # Fire alert webhook
    try:
        from execution.notify import notify_task_failure
        request = kwargs.get("request") or getattr(sender, "request", None)
        retries = getattr(request, "retries", 0) if request else 0
        notify_task_failure(
            task_id=task_id or "",
            task_name=task_name,
            exception=exception or Exception("unknown"),
            retries=retries,
            max_retries=getattr(sender, "max_retries", 3),
        )
    except Exception as notify_err:
        _log.warning("dead_letter.notify_fail  err=%s", notify_err)


# Distributed trace propagation (P1-A)
try:
    from core.tracing import setup_celery_tracing
    setup_celery_tracing(app)
except Exception as _te:
    import logging
    logging.getLogger(__name__).warning("tracing.setup_fail  err=%s", _te)


# Worker observability metrics (P1-G)
try:
    from core.metrics import setup_celery_metrics
    setup_celery_metrics(app)
except Exception as _me:
    import logging
    logging.getLogger(__name__).warning("metrics.setup_fail  err=%s", _me)
