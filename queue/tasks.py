"""Celery tasks for the SEO Engine autonomous pipeline.

All imports of project modules are done *inside* task functions (lazy imports)
to avoid circular-import issues and to keep worker startup fast.

Retry strategy: exponential back-off starting at 60 s.
  countdown = 2 ** self.request.retries * 60
  retries 0 → 60 s, 1 → 120 s, 2 → 240 s, 3 → 480 s
"""

from __future__ import annotations

import json
import logging
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from queue.celery_app import app

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_RESULTS_DIR = Path("data/storage/task_results")


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _save_result(task_id: str, payload: dict) -> None:
    """Persist a task result to disk as JSON."""
    _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = _RESULTS_DIR / f"{task_id}.json"
    out.write_text(json.dumps(payload, indent=2, default=str))
    log.debug("task_result.saved  path=%s", out)


def _run_async(coro) -> Any:
    """Run an async coroutine from a synchronous Celery task."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Task: analyze_business
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="analysis", max_retries=3, name="queue.tasks.analyze_business")
def analyze_business(self, business_id: str, business_data: dict) -> dict:
    """Run the SEO Brain analysis for a single business.

    Args:
        business_id:    Unique identifier for the business.
        business_data:  Dict that maps to ``models.business.BusinessContext``.

    Returns:
        dict with keys: status, business_id, tasks, task_count, timestamp, task_id.
    """
    started_at = _utc_now()
    log.info("analyze_business.start  task_id=%s  business_id=%s  ts=%s",
             self.request.id, business_id, started_at)
    try:
        from core.agents.brain import SEOBrain
        from models.business import BusinessContext

        business = BusinessContext(**business_data)
        brain = SEOBrain()
        task_batch = _run_async(brain.analyze(business))

        tasks_out = (
            [t.model_dump() if hasattr(t, "model_dump") else vars(t)
             for t in task_batch.tasks]
            if hasattr(task_batch, "tasks")
            else []
        )

        result = {
            "status": "success",
            "business_id": business_id,
            "tasks": tasks_out,
            "task_count": len(tasks_out),
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("analyze_business.done  task_id=%s  business_id=%s  tasks=%d",
                 self.request.id, business_id, len(tasks_out))
        return result

    except Exception as exc:
        log.exception("analyze_business.error  task_id=%s  business_id=%s  exc=%s",
                      self.request.id, business_id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: orchestrate_business
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="analysis", max_retries=3, name="queue.tasks.orchestrate_business")
def orchestrate_business(self, business_id: str, business_data: dict) -> dict:
    """Run the full multi-agent orchestrator pipeline for a business.

    Args:
        business_id:    Unique identifier for the business.
        business_data:  Dict that maps to ``models.business.BusinessContext``.

    Returns:
        dict with keys: status, business_id, pipeline_log, timestamp, task_id.
    """
    started_at = _utc_now()
    log.info("orchestrate_business.start  task_id=%s  business_id=%s  ts=%s",
             self.request.id, business_id, started_at)
    try:
        from core.agents.orchestrator import AgentOrchestrator
        from models.business import BusinessContext

        business = BusinessContext(**business_data)
        orchestrator = AgentOrchestrator()
        pipeline_result = _run_async(orchestrator.run(business))

        result = {
            "status": "success",
            "business_id": business_id,
            "pipeline_log": (
                pipeline_result.to_dict()
                if hasattr(pipeline_result, "to_dict")
                else pipeline_result
            ),
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("orchestrate_business.done  task_id=%s  business_id=%s",
                 self.request.id, business_id)
        return result

    except Exception as exc:
        log.exception("orchestrate_business.error  task_id=%s  business_id=%s  exc=%s",
                      self.request.id, business_id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: execute_seo_task
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="execution", max_retries=2, name="queue.tasks.execute_seo_task")
def execute_seo_task(
    self,
    business_id: str,
    task_data: dict,
    mode: str = "shadow",
) -> dict:
    """Execute a single SEO task through the execution router.

    Args:
        business_id:  Business identifier.
        task_data:    Dict representing an ``models.task.SEOTask``.
        mode:         Execution mode — 'shadow', 'live', or 'approval_queue'.

    Returns:
        dict with keys: status, business_id, exec_result, mode, timestamp, task_id.
    """
    started_at = _utc_now()
    log.info("execute_seo_task.start  task_id=%s  business_id=%s  mode=%s  ts=%s",
             self.request.id, business_id, mode, started_at)
    try:
        from execution.router import ExecutionRouter
        from models.task import SEOTask, ExecutionMode
        from models.business import BusinessContext

        exec_mode = ExecutionMode(mode) if mode in ExecutionMode.__members__.values() else ExecutionMode.SHADOW

        seo_task = SEOTask(**task_data)
        router = ExecutionRouter(shadow_mode=(exec_mode == ExecutionMode.SHADOW))

        # execute_task requires a BusinessContext; build a minimal one from business_id
        business = BusinessContext(
            business_name=task_data.get("business_name", business_id),
            website=task_data.get("website", ""),
            primary_service=task_data.get("primary_service", ""),
            primary_city=task_data.get("primary_city", ""),
        )

        exec_result = _run_async(router.execute_task(seo_task, business, exec_mode))

        result = {
            "status": "success",
            "business_id": business_id,
            "exec_result": (
                exec_result.model_dump() if hasattr(exec_result, "model_dump") else vars(exec_result)
            ),
            "mode": mode,
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("execute_seo_task.done  task_id=%s  business_id=%s  mode=%s",
                 self.request.id, business_id, mode)
        return result

    except Exception as exc:
        log.exception("execute_seo_task.error  task_id=%s  business_id=%s  exc=%s",
                      self.request.id, business_id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: run_learning
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="learning", max_retries=1, name="queue.tasks.run_learning")
def run_learning(self, business_id: str = None) -> dict:
    """Run the weekly learning cycle for one or all businesses.

    Args:
        business_id: If provided, run only for this business; otherwise run
                     for every business found in the local database.

    Returns:
        dict with keys: status, business_id, report, timestamp, task_id.
    """
    started_at = _utc_now()
    log.info("run_learning.start  task_id=%s  business_id=%s  ts=%s",
             self.request.id, business_id, started_at)
    try:
        from learning.loops import LearningEngine

        engine = LearningEngine()
        if business_id:
            report = _run_async(engine.weekly_cycle(business_id))
            reports = [report.model_dump() if hasattr(report, "model_dump") else vars(report)]
        else:
            # Run for each business discovered in local storage
            businesses_file = Path("data/storage/businesses.json")
            if businesses_file.exists():
                businesses = json.loads(businesses_file.read_text())
            else:
                businesses = []
            reports = []
            for biz in businesses:
                bid = biz.get("id") or biz.get("business_id", "")
                if not bid:
                    continue
                r = _run_async(engine.weekly_cycle(bid))
                reports.append(r.model_dump() if hasattr(r, "model_dump") else vars(r))

        result = {
            "status": "success",
            "business_id": business_id,
            "reports": reports,
            "businesses_processed": len(reports),
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("run_learning.done  task_id=%s  businesses=%d",
                 self.request.id, len(reports))
        return result

    except Exception as exc:
        log.exception("run_learning.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: daily_analysis_cycle
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="analysis", max_retries=2, name="queue.tasks.daily_analysis_cycle")
def daily_analysis_cycle(self) -> dict:
    """Scheduled daily task: analyse every business then shadow-execute the top recommendation.

    For each business in ``data/storage/businesses.json``:
      1. Chain ``analyze_business`` to get scored tasks.
      2. Fire ``execute_seo_task`` for the highest-priority task in shadow mode.

    Returns:
        dict with keys: status, businesses_queued, timestamp, task_id.
    """
    started_at = _utc_now()
    log.info("daily_analysis_cycle.start  task_id=%s  ts=%s", self.request.id, started_at)
    try:
        from celery import chain as celery_chain

        businesses_file = Path("data/storage/businesses.json")
        if not businesses_file.exists():
            log.warning("daily_analysis_cycle.no_businesses_file  path=%s", businesses_file)
            result = {
                "status": "skipped",
                "reason": "businesses.json not found",
                "businesses_queued": 0,
                "timestamp": _utc_now(),
                "task_id": self.request.id,
            }
            _save_result(self.request.id, result)
            return result

        businesses: list[dict] = json.loads(businesses_file.read_text())
        if not businesses:
            result = {
                "status": "skipped",
                "reason": "no businesses configured",
                "businesses_queued": 0,
                "timestamp": _utc_now(),
                "task_id": self.request.id,
            }
            _save_result(self.request.id, result)
            return result

        queued: list[str] = []
        for biz in businesses:
            business_id = biz.get("id") or biz.get("business_id", "")
            if not business_id:
                log.warning("daily_analysis_cycle.skip_no_id  biz=%s", biz)
                continue

            # Build a minimal business_data dict; real configs should have all fields
            business_data: dict = {k: v for k, v in biz.items() if k not in ("id", "business_id")}
            business_data.setdefault("business_name", biz.get("name", business_id))
            business_data.setdefault("website", biz.get("website", ""))
            business_data.setdefault("primary_service", biz.get("primary_service", ""))
            business_data.setdefault("primary_city", biz.get("primary_city", ""))

            # Step 1: analyse_business — result is passed to the shadow-execute via a chord
            # We use a chain: analyze → a callback that executes the top task.
            # Because the callback needs the analysis result, we use a dedicated helper task.
            _chain = celery_chain(
                analyze_business.s(business_id, business_data),
                _execute_top_task.s(business_id, business_data),
            )
            _chain.apply_async()
            queued.append(business_id)
            log.info("daily_analysis_cycle.queued  business_id=%s", business_id)

        result = {
            "status": "success",
            "businesses_queued": len(queued),
            "business_ids": queued,
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("daily_analysis_cycle.done  task_id=%s  queued=%d",
                 self.request.id, len(queued))
        return result

    except Exception as exc:
        log.exception("daily_analysis_cycle.error  task_id=%s  exc=%s",
                      self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


@app.task(bind=True, queue="execution", max_retries=2, name="queue.tasks._execute_top_task")
def _execute_top_task(self, analysis_result: dict, business_id: str, business_data: dict) -> dict:
    """Internal callback — picks the top task from an analysis result and shadow-executes it.

    This is the second step of the daily_analysis_cycle chain and is not intended
    to be called directly.
    """
    tasks: list[dict] = analysis_result.get("tasks", [])
    if not tasks:
        log.info("_execute_top_task.no_tasks  business_id=%s", business_id)
        return {"status": "skipped", "reason": "no tasks from analysis", "business_id": business_id}

    top_task = tasks[0]
    log.info("_execute_top_task.firing  business_id=%s  action=%s",
             business_id, top_task.get("action", "?"))
    return execute_seo_task.apply_async(
        args=[business_id, top_task],
        kwargs={"mode": "shadow"},
    ).get(timeout=300)


# ---------------------------------------------------------------------------
# Task: check_rankings
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="monitoring", max_retries=3, name="queue.tasks.check_rankings")
def check_rankings(self, business_id: str = None) -> dict:
    """Weekly rank check — pull current positions from DataForSEO/GSC and diff.

    Args:
        business_id: If provided, check only this business; otherwise all.

    Returns:
        dict with keys: status, business_id, rank_deltas, timestamp, task_id.
    """
    started_at = _utc_now()
    log.info("check_rankings.start  task_id=%s  business_id=%s  ts=%s",
             self.request.id, business_id, started_at)
    try:
        # rank_tracker module does not yet exist — import defensively
        try:
            from data.connectors import rank_tracker as rt
            tracker = rt.RankTracker()
        except (ImportError, AttributeError):
            # Fall back to GSC connector which has ranking data
            from data.connectors.gsc import fetch_rankings  # type: ignore
            tracker = None

        businesses_file = Path("data/storage/businesses.json")
        businesses = json.loads(businesses_file.read_text()) if businesses_file.exists() else []

        targets = (
            [b for b in businesses if (b.get("id") or b.get("business_id")) == business_id]
            if business_id
            else businesses
        )

        rank_deltas: list[dict] = []
        for biz in targets:
            bid = biz.get("id") or biz.get("business_id", "")
            site_url = biz.get("website", "")
            if not (bid and site_url):
                continue
            try:
                if tracker:
                    delta = _run_async(tracker.check(bid, site_url))
                    rank_deltas.append({"business_id": bid, "delta": delta})
                else:
                    rank_deltas.append({
                        "business_id": bid,
                        "delta": {},
                        "note": "rank_tracker not available; use GSC dashboard",
                    })
            except Exception as biz_exc:
                log.warning("check_rankings.biz_error  bid=%s  exc=%s", bid, biz_exc)
                rank_deltas.append({"business_id": bid, "error": str(biz_exc)})

        result = {
            "status": "success",
            "business_id": business_id,
            "rank_deltas": rank_deltas,
            "businesses_checked": len(rank_deltas),
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("check_rankings.done  task_id=%s  checked=%d",
                 self.request.id, len(rank_deltas))
        return result

    except Exception as exc:
        log.exception("check_rankings.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: scan_content_decay
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="monitoring", max_retries=2, name="queue.tasks.scan_content_decay")
def scan_content_decay(self, business_id: str = None) -> dict:
    """Scan for pages with >20 % traffic decline and queue content-refresh tasks.

    Args:
        business_id: If provided, scan only this business; otherwise all.

    Returns:
        dict with keys: status, decaying_pages, refresh_tasks_queued, timestamp, task_id.
    """
    started_at = _utc_now()
    log.info("scan_content_decay.start  task_id=%s  business_id=%s  ts=%s",
             self.request.id, business_id, started_at)
    try:
        from data.analyzers.content_decay import analyze_content_decay
        from data.connectors.gsc import GSCConnector  # type: ignore

        businesses_file = Path("data/storage/businesses.json")
        businesses = json.loads(businesses_file.read_text()) if businesses_file.exists() else []

        targets = (
            [b for b in businesses if (b.get("id") or b.get("business_id")) == business_id]
            if business_id
            else businesses
        )

        all_decaying: list[dict] = []
        refresh_queued = 0

        for biz in targets:
            bid = biz.get("id") or biz.get("business_id", "")
            site_url = biz.get("website", "")
            if not (bid and site_url):
                continue
            try:
                connector = GSCConnector(site_url=site_url)
                gsc_rows = _run_async(connector.fetch_page_data(months=12))
                gsc_data = [r if isinstance(r, dict) else r.model_dump() for r in gsc_rows]
            except Exception as gsc_exc:
                log.warning("scan_content_decay.gsc_error  bid=%s  exc=%s", bid, gsc_exc)
                gsc_data = []

            decaying = analyze_content_decay(gsc_data, months=12, min_peak_clicks=10)

            # Filter to >20% decay
            severe = [p for p in decaying if p.get("decay_pct", 0) > 20]

            for page in severe:
                url = page.get("url", "")
                decay_pct = page.get("decay_pct", 0)
                log.info("scan_content_decay.decay_found  bid=%s  url=%s  decay_pct=%.1f",
                         bid, url, decay_pct)
                # Build a refresh task and queue it for shadow execution
                refresh_task_data = {
                    "action": f"Refresh and update content for {url} (traffic down {decay_pct:.1f}%)",
                    "type": "CONTENT",
                    "priority_rank": 1,
                    "url": url,
                    "decay_pct": decay_pct,
                }
                execute_seo_task.apply_async(
                    args=[bid, refresh_task_data],
                    kwargs={"mode": "shadow"},
                )
                refresh_queued += 1

            all_decaying.extend([{**p, "business_id": bid} for p in severe])

        result = {
            "status": "success",
            "business_id": business_id,
            "decaying_pages": all_decaying,
            "decay_count": len(all_decaying),
            "refresh_tasks_queued": refresh_queued,
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("scan_content_decay.done  task_id=%s  decaying=%d  queued=%d",
                 self.request.id, len(all_decaying), refresh_queued)
        return result

    except Exception as exc:
        log.exception("scan_content_decay.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: submit_to_indexnow
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="execution", max_retries=2, name="queue.tasks.submit_to_indexnow")
def submit_to_indexnow(self, urls: list, business_id: str = None) -> dict:
    """Submit a batch of URLs to the IndexNow API for instant indexing.

    Args:
        urls:         List of URL strings to submit.
        business_id:  Optional business identifier for logging/storage.

    Returns:
        dict with keys: status, submitted, failed, business_id, timestamp, task_id.
    """
    started_at = _utc_now()
    log.info("submit_to_indexnow.start  task_id=%s  business_id=%s  urls=%d  ts=%s",
             self.request.id, business_id, len(urls), started_at)
    try:
        try:
            from execution import indexnow as indexnow_module
            submitter = indexnow_module.IndexNowSubmitter()
            submission = _run_async(submitter.submit(urls, business_id=business_id))
        except (ImportError, AttributeError):
            # indexnow module not yet present — use direct HTTP fallback
            import httpx, os
            api_key = os.environ.get("INDEXNOW_KEY", "")
            host = "api.indexnow.org"
            submitted_urls = []
            failed_urls = []
            if api_key and urls:
                payload = {
                    "host": host,
                    "key": api_key,
                    "urlList": urls[:10000],  # IndexNow cap
                }
                with httpx.Client(timeout=30) as client:
                    resp = client.post(
                        f"https://{host}/indexnow",
                        json=payload,
                        headers={"Content-Type": "application/json; charset=utf-8"},
                    )
                if resp.status_code in (200, 202):
                    submitted_urls = urls
                else:
                    log.warning("submit_to_indexnow.http_error  status=%d  body=%s",
                                resp.status_code, resp.text[:200])
                    failed_urls = urls
            else:
                log.warning("submit_to_indexnow.no_api_key  skipping")
                failed_urls = urls

            submission = {
                "submitted": submitted_urls,
                "failed": failed_urls,
            }

        result = {
            "status": "success",
            "business_id": business_id,
            "submitted": submission.get("submitted", []),
            "failed": submission.get("failed", []),
            "submitted_count": len(submission.get("submitted", [])),
            "failed_count": len(submission.get("failed", [])),
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("submit_to_indexnow.done  task_id=%s  submitted=%d  failed=%d",
                 self.request.id, result["submitted_count"], result["failed_count"])
        return result

    except Exception as exc:
        log.exception("submit_to_indexnow.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: monitor_ai_citations
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="monitoring", max_retries=3, name="queue.tasks.monitor_ai_citations")
def monitor_ai_citations(self, business_name: str, business_id: str = None) -> dict:
    """Check AI-generated search responses for brand mentions / citations.

    Args:
        business_name:  Display name of the business to search for.
        business_id:    Optional identifier for logging and storage.

    Returns:
        dict with keys: status, business_id, citations, citation_count, timestamp, task_id.
    """
    started_at = _utc_now()
    log.info("monitor_ai_citations.start  task_id=%s  business_id=%s  name=%s  ts=%s",
             self.request.id, business_id, business_name, started_at)
    try:
        try:
            from monitoring import brand_mentions as bm_module
            checker = bm_module.BrandMentionChecker()
            citations = _run_async(checker.check(business_name, business_id=business_id))
        except (ImportError, AttributeError):
            # monitoring.brand_mentions not yet present; log and return empty
            log.warning("monitor_ai_citations.module_missing  using_stub")
            citations = []

        result = {
            "status": "success",
            "business_id": business_id,
            "business_name": business_name,
            "citations": citations,
            "citation_count": len(citations),
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("monitor_ai_citations.done  task_id=%s  business_id=%s  citations=%d",
                 self.request.id, business_id, len(citations))
        return result

    except Exception as exc:
        log.exception("monitor_ai_citations.error  task_id=%s  exc=%s",
                      self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)
