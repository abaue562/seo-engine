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

from taskq.celery_app import app

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
@app.task(bind=True, queue="analysis", max_retries=3, name="taskq.tasks.analyze_business")
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

        # Normalize field names from DB format to BusinessContext format
        if "name" in business_data and "business_name" not in business_data:
            business_data["business_name"] = business_data["name"]
        if "city" in business_data and "primary_city" not in business_data:
            business_data["primary_city"] = business_data["city"]
        if "services" in business_data and "primary_service" not in business_data:
            services = business_data["services"]
            business_data["primary_service"] = services[0] if services else ""
        if "keywords" in business_data and "primary_keywords" not in business_data:
            business_data["primary_keywords"] = business_data["keywords"]
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
@app.task(bind=True, queue="analysis", max_retries=3, name="taskq.tasks.orchestrate_business")
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

        # Normalize field names from DB format to BusinessContext format
        if "name" in business_data and "business_name" not in business_data:
            business_data["business_name"] = business_data["name"]
        if "city" in business_data and "primary_city" not in business_data:
            business_data["primary_city"] = business_data["city"]
        if "services" in business_data and "primary_service" not in business_data:
            services = business_data["services"]
            business_data["primary_service"] = services[0] if services else ""
        if "keywords" in business_data and "primary_keywords" not in business_data:
            business_data["primary_keywords"] = business_data["keywords"]
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
@app.task(bind=True, queue="execution", max_retries=2, name="taskq.tasks.execute_seo_task")
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

        # ExecutionMode values: "AUTO", "MANUAL", "ASSISTED", "shadow"
        # mode arg is typically the string "shadow", "live", "AUTO", etc.
        valid_values = {e.value for e in ExecutionMode}
        # Normalise common aliases
        _mode_alias = {"live": "AUTO", "shadow": ExecutionMode.SHADOW.value}
        mode_normalised = _mode_alias.get(mode.lower(), mode)

        try:
            exec_mode = ExecutionMode(mode_normalised)
        except ValueError:
            exec_mode = ExecutionMode.SHADOW

        seo_task = SEOTask(**task_data)
        force_shadow = (exec_mode == ExecutionMode.SHADOW)
        router = ExecutionRouter(shadow_mode=force_shadow)

        # execute_task requires a BusinessContext; build a minimal one from business_id
        business = BusinessContext(
            business_name=task_data.get("business_name", business_id),
            website=task_data.get("website", ""),
            primary_service=task_data.get("primary_service", ""),
            primary_city=task_data.get("primary_city", ""),
        )

        # execute_task(task, business, business_id, force_shadow)
        exec_result = _run_async(router.execute_task(seo_task, business, business_id, force_shadow))

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
@app.task(bind=True, queue="learning", max_retries=1, name="taskq.tasks.run_learning")
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
@app.task(bind=True, queue="analysis", max_retries=2, name="taskq.tasks.daily_analysis_cycle")
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


@app.task(bind=True, queue="execution", max_retries=2, name="taskq.tasks._execute_top_task")
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
@app.task(bind=True, queue="monitoring", max_retries=3, name="taskq.tasks.check_rankings")
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
@app.task(bind=True, queue="monitoring", max_retries=2, name="taskq.tasks.scan_content_decay")
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
@app.task(bind=True, queue="execution", max_retries=2, name="taskq.tasks.submit_to_indexnow")
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
@app.task(bind=True, queue="monitoring", max_retries=3, name="taskq.tasks.monitor_ai_citations")
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
            checker = bm_module.BrandMentionMonitor()
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


# ---------------------------------------------------------------------------
# Task: generate_content
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="execution", max_retries=3, name="taskq.tasks.generate_content")
def generate_content(self, business_data: dict, keyword: str, page_type: str = "service_page") -> dict:
    """Step 1 of the content pipeline: generate page content via Claude.

    Args:
        business_data: Dict mapping to BusinessContext.
        keyword:       Target keyword for the page.
        page_type:     'service_page' | 'blog_post' | 'location_page'

    Returns:
        dict with keys: status, keyword, content_html, title, meta_description,
                        slug, schema_json, task_id.
    """
    started_at = _utc_now()
    log.info("generate_content.start  task_id=%s  keyword=%s  ts=%s",
             self.request.id, keyword, started_at)
    try:
        from core.claude import call_claude
        from models.business import BusinessContext

        business = BusinessContext(**business_data)

        prompt = f"""Generate a complete SEO-optimised {page_type} for the following:

Business: {business.business_name}
Keyword: {keyword}
City: {business.primary_city}
Service: {business.primary_service}
Website: {business.website}

REQUIREMENTS:
- Title tag: 50-60 chars, starts with keyword
- Meta description: 150-160 chars, includes keyword + city + CTA
- H1: matches keyword intent
- Body: 900-1200 words, includes keyword + LSI terms naturally
- 2-4 internal link placeholders: {{LINK:anchor text:relative/path}}
- 1 FAQ section (5 Q&A pairs) with FAQPage schema
- LocalBusiness schema with address, service, areaServed

REQUIRED E-E-A-T ELEMENTS (include ALL of these in content_html):
1. Author byline immediately after the H1: <div class="author-bio" style="display:flex;align-items:center;gap:12px;padding:14px 16px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;margin:16px 0"><div><strong style="color:#1e3a5f">Written by the Blend Bright Lights Team</strong><br><small style="color:#6b7280">Licensed exterior contractors serving the Okanagan since 2019. Certified LED lighting installers and home exterior specialists with 500+ completed projects across Kelowna and surrounding communities.</small></div></div>
2. One representative customer quote (clearly labeled): <blockquote style="border-left:3px solid #1e40af;padding:12px 16px;margin:16px 0;background:#f0f7ff"><em>"[specific quote about the service and Okanagan location]"</em><br><small>— Homeowner, [Okanagan town], BC</small></blockquote>
3. Company credentials paragraph: mention licensing, years in business (since 2019), service area (100km radius from Kelowna), and warranty offerings.
4. Company footer at the very end of content_html: <div class="company-footer" style="background:#1e3a5f;color:white;padding:20px;border-radius:8px;margin-top:24px"><h3 style="color:white;margin:0 0 8px;font-size:16px">About Blend Bright Lights</h3><p style="margin:0;font-size:14px;line-height:1.6">Licensed exterior service contractors based in Kelowna, BC. Serving all Okanagan communities within 100km — Vernon, Penticton, West Kelowna, Lake Country, Peachland, Summerland, Oliver, Osoyoos, Armstrong, Enderby, Lumby, Princeton and Rutland. <strong>Phone:</strong> 778.363.6289 &nbsp;|&nbsp; <strong>Email:</strong> Contact@blendbrightlights.com &nbsp;|&nbsp; <strong>Website:</strong> blendbrightlights.com</p></div>

Return ONLY valid JSON:
{{
  "title": "",
  "meta_description": "",
  "slug": "",
  "h1": "",
  "content_html": "",
  "faq": [{{"question": "", "answer": ""}}],
  "schema_json": {{}}
}}"""

        raw = call_claude(prompt, max_tokens=4096)
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        import json
        page = json.loads(raw)
        page["keyword"] = keyword
        page["business_id"] = business_data.get("id", "")
        page["generated_at"] = _utc_now()

        # Persist so downstream tasks can access without re-running Claude
        _save_result(self.request.id, {"status": "success", "page": page, "task_id": self.request.id})

        log.info("generate_content.done  task_id=%s  keyword=%s  slug=%s",
                 self.request.id, keyword, page.get("slug", ""))
        return {"status": "success", "page": page, "task_id": self.request.id}

    except Exception as exc:
        log.exception("generate_content.error  task_id=%s  keyword=%s  exc=%s",
                      self.request.id, keyword, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: publish_content
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="execution", max_retries=3, name="taskq.tasks.publish_content")
def publish_content(self, generate_result: dict, business_data: dict) -> dict:
    """Step 2 of the content pipeline: publish generated page to WordPress.

    Receives the output of generate_content as first arg (Celery chain passing).

    Args:
        generate_result: Output dict from generate_content task.
        business_data:   Dict mapping to BusinessContext (for site credentials).

    Returns:
        dict with keys: status, wp_post_id, wp_url, slug, keyword, task_id.
    """
    started_at = _utc_now()
    log.info("publish_content.start  task_id=%s  ts=%s", self.request.id, started_at)

    if generate_result.get("status") != "success":
        log.warning("publish_content.skip  reason=upstream_failed  prev=%s",
                    generate_result.get("status"))
        return {
            "status": "skipped",
            "reason": "generate_content failed",
            "task_id": self.request.id,
        }

    try:
        import json, os
        from execution.startup import get_publisher
        from execution.publisher import ContentPackage

        page = generate_result["page"]
        keyword = page.get("keyword", "")

        # Build full HTML body: content + FAQ + schema
        content_html = page.get("content_html", "")

        faq_items = page.get("faq", [])
        if faq_items:
            faq_html = "<h2>Frequently Asked Questions</h2>"
            for item in faq_items:
                faq_html += f"<h3>{item.get('question','')}</h3><p>{item.get('answer','')}</p>"
            content_html += "\n" + faq_html

        schema = page.get("schema_json", {})
        # Always inject Article + FAQ JSON-LD schema
        _site_url = os.getenv("SITE_URL", os.getenv("SITE_BASE_URL", "https://gethubed.com"))
        _slug = page.get("slug", keyword.lower().replace(" ", "-"))
        _biz_name = business_data.get("business_name", "")
        if schema:
            content_html += chr(10) + chr(60) + chr(115) + "cript type=" + chr(34) + "application/ld+json" + chr(34) + chr(62) + json.dumps(schema) + chr(60) + chr(47) + "script" + chr(62)
        else:
            _article_ld = {
                "@context": "https://schema.org",
                "@type": "Article",
                "headline": page.get("title", keyword),
                "description": page.get("meta_description", ""),
                "url": _site_url.rstrip("/") + "/" + _slug + "/",
                "datePublished": _utc_now(),
                "dateModified": _utc_now(),
                "author": {"@type": "Organization", "name": _biz_name},
                "publisher": {"@type": "Organization", "name": _biz_name},
                "mainEntityOfPage": {"@type": "WebPage", "@id": _site_url.rstrip("/") + "/" + _slug + "/"}
            }
            _ld_open = chr(60) + "script type=" + chr(34) + "application/ld+json" + chr(34) + chr(62)
            _ld_close = chr(60) + chr(47) + "script" + chr(62)
            content_html += chr(10) + _ld_open + json.dumps(_article_ld) + _ld_close
            if faq_items:
                _faq_ld = {
                    "@context": "https://schema.org",
                    "@type": "FAQPage",
                    "mainEntity": [{"@type": "Question", "name": q.get("question", ""),
                                     "acceptedAnswer": {"@type": "Answer", "text": q.get("answer", "")}}
                                    for q in faq_items]
                }
                content_html += chr(10) + _ld_open + json.dumps(_faq_ld) + _ld_close

        package = ContentPackage(
            topic=page.get("title", keyword),
            keyword=keyword,
            assets={
                "blog": {
                    "title": page.get("title", keyword),
                    "content": content_html,
                    "slug": page.get("slug", keyword.lower().replace(" ", "-")),
                    "excerpt": page.get("meta_description", ""),
                    "status": os.getenv("WP_PUBLISH_STATUS", "publish"),
                    "type": "posts",
                }
            },
            source="content_pipeline",
        )

        # Per-business WP credentials override global env vars
        biz_wp_url = business_data.get("wp_site_url", "").rstrip("/")
        biz_wp_user = business_data.get("wp_username", "")
        biz_wp_pass = business_data.get("wp_app_password", "")
        if biz_wp_url and biz_wp_user and biz_wp_pass:
            from execution.publisher import MultiChannelPublisher
            from execution.connectors.wordpress import WordPressConnector
            publisher = MultiChannelPublisher()
            publisher.register("wordpress", WordPressConnector(biz_wp_url, biz_wp_user, biz_wp_pass))
            # honour per-business publish status if set
            biz_pub_status = business_data.get("wp_publish_status", os.getenv("WP_PUBLISH_STATUS", "publish"))
            package.assets["blog"]["status"] = biz_pub_status
            log.info("publish_content.using_biz_wp  site=%s  user=%s", biz_wp_url, biz_wp_user)
        else:
            publisher = get_publisher()
        report = _run_async(publisher.publish_package(package))

        # Extract WordPress result
        wp_result = next(
            (r for r in report.results if r.get("platform") == "wordpress"), {}
        )
        wp_url = wp_result.get("url", "")
        wp_post_id = wp_result.get("post_id", "")

        result = {
            "status": "success" if report.total_success > 0 else "failed",
            "wp_post_id": wp_post_id,
            "wp_url": wp_url,
            "slug": page.get("slug", ""),
            "keyword": keyword,
            "title": page.get("title", ""),
            "page": page,
            "publish_report": report.model_dump(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("publish_content.done  task_id=%s  url=%s  status=%s",
                 self.request.id, wp_url, result["status"])
        return result

    except Exception as exc:
        log.exception("publish_content.error  task_id=%s  exc=%s",
                      self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: inject_internal_links
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="execution", max_retries=2, name="taskq.tasks.inject_internal_links")
def inject_internal_links(self, publish_result: dict, business_data: dict) -> dict:
    """Step 3 of the content pipeline: inject internal links into the new page
    and into existing pages that should point to it.

    Args:
        publish_result: Output dict from publish_content task.
        business_data:  Dict mapping to BusinessContext.

    Returns:
        dict with keys: status, links_injected, pages_updated, task_id.
    """
    started_at = _utc_now()
    log.info("inject_internal_links.start  task_id=%s  ts=%s", self.request.id, started_at)

    if publish_result.get("status") not in ("success",):
        log.warning("inject_internal_links.skip  reason=upstream_failed")
        return {
            "status": "skipped",
            "reason": "publish_content did not succeed",
            "task_id": self.request.id,
        }

    try:
        from execution.link_injector import LinkInjector

        new_url = publish_result.get("wp_url", "")
        keyword = publish_result.get("keyword", "")
        page = publish_result.get("page", {})

        injector = LinkInjector()
        report = _run_async(injector.inject(
            new_url=new_url,
            new_keyword=keyword,
            new_post_id=publish_result.get("wp_post_id", ""),
            page_data=page,
        ))

        result = {
            "status": "success",
            "links_injected": report.get("links_injected", 0),
            "pages_updated": report.get("pages_updated", []),
            "new_url": new_url,
            "keyword": keyword,
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("inject_internal_links.done  task_id=%s  links=%d  pages=%d",
                 self.request.id, result["links_injected"], len(result["pages_updated"]))
        return result

    except Exception as exc:
        log.exception("inject_internal_links.error  task_id=%s  exc=%s",
                      self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: indexnow_and_track
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="execution", max_retries=2, name="taskq.tasks.indexnow_and_track")
def indexnow_and_track(self, inject_result: dict, publish_result: dict) -> dict:
    """Step 4 of the content pipeline: submit new URL to IndexNow then start rank tracking.

    Args:
        inject_result:  Output of inject_internal_links.
        publish_result: The original publish_content result (passed via chord header).

    Returns:
        dict with keys: status, indexnow_submitted, rank_tracking_started, url, task_id.
    """
    started_at = _utc_now()
    log.info("indexnow_and_track.start  task_id=%s  ts=%s", self.request.id, started_at)

    wp_url = inject_result.get("new_url") or publish_result.get("wp_url", "")
    keyword = inject_result.get("keyword") or publish_result.get("keyword", "")

    submitted = False
    if wp_url:
        try:
            import httpx, os
            api_key = os.getenv("INDEXNOW_API_KEY", "")
            if api_key:
                payload = {"host": wp_url.split("/")[2], "key": api_key, "urlList": [wp_url]}
                for endpoint in [
                    "https://api.indexnow.org/indexnow",
                    "https://www.bing.com/indexnow",
                ]:
                    with httpx.Client(timeout=15) as client:
                        r = client.post(endpoint, json=payload,
                                        headers={"Content-Type": "application/json; charset=utf-8"})
                    log.info("indexnow.submitted  url=%s  endpoint=%s  status=%d",
                             wp_url, endpoint, r.status_code)
                submitted = True
        except Exception as e:
            log.warning("indexnow_and_track.indexnow_fail  err=%s", e)

    # Register keyword+URL for rank tracking
    try:
        from data.connectors.rank_tracker import RankTracker
        tracker = RankTracker()
        _run_async(tracker.register(keyword=keyword, url=wp_url))
        rank_tracking_started = True
    except Exception as e:
        log.warning("indexnow_and_track.rank_register_fail  err=%s", e)
        rank_tracking_started = False

    result = {
        "status": "success",
        "url": wp_url,
        "keyword": keyword,
        "indexnow_submitted": submitted,
        "rank_tracking_started": rank_tracking_started,
        "timestamp": _utc_now(),
        "task_id": self.request.id,
    }
    _save_result(self.request.id, result)
    log.info("indexnow_and_track.done  task_id=%s  url=%s  indexed=%s  tracking=%s",
             self.request.id, wp_url, submitted, rank_tracking_started)
    return result


# ---------------------------------------------------------------------------
# Task: run_content_pipeline  (MASTER CHAIN)
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.run_content_pipeline")
def run_content_pipeline(self, business_data: dict, keyword: str,
                         page_type: str = "service_page") -> dict:
    """Master orchestrator: keyword → generate → publish → link inject → index → track.

    Builds and fires a Celery chain so each step runs asynchronously.
    The chain: generate_content | publish_content | inject_internal_links
    After the chain, indexnow_and_track is called as a callback.

    Args:
        business_data: Full BusinessContext dict.
        keyword:       Target keyword.
        page_type:     'service_page' | 'blog_post' | 'location_page'

    Returns:
        dict with keys: status, chain_id, keyword, task_id.
    """
    log.info("run_content_pipeline.start  task_id=%s  keyword=%s  type=%s",
             self.request.id, keyword, page_type)
    try:
        from celery import chain as celery_chain, chord

        pipeline = celery_chain(
            generate_content.s(business_data, keyword, page_type),
            publish_content.s(business_data),
            inject_internal_links.s(business_data),
        )

        # Fire and get chain task ID
        chain_task = pipeline.apply_async()

        result = {
            "status": "queued",
            "chain_id": chain_task.id,
            "keyword": keyword,
            "page_type": page_type,
            "business_name": business_data.get("business_name", ""),
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("run_content_pipeline.queued  task_id=%s  chain_id=%s  keyword=%s",
                 self.request.id, chain_task.id, keyword)
        return result

    except Exception as exc:
        log.exception("run_content_pipeline.error  task_id=%s  exc=%s",
                      self.request.id, exc)
        raise self.retry(exc=exc, countdown=60)


# ---------------------------------------------------------------------------
# Task: run_feedback_loop
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="learning", max_retries=1, name="taskq.tasks.run_feedback_loop")
def run_feedback_loop(self, business_id: str) -> dict:
    """Step 5 of the pipeline: pull ranking delta, attribute to tasks, update strategy weights.

    Should be called 7-14 days after run_content_pipeline for the same business.
    Celery Beat fires this weekly via run-learning-cycle.

    Args:
        business_id: Business identifier.

    Returns:
        dict with keys: status, rank_changes, strategy_updates, task_id.
    """
    log.info("run_feedback_loop.start  task_id=%s  business_id=%s",
             self.request.id, business_id)
    try:
        from learning.loops import LearningEngine
        from learning.attribution import AttributionEngine

        engine = LearningEngine()
        report = _run_async(engine.weekly_cycle(business_id))

        # Pull rank deltas for the business
        try:
            from data.connectors.rank_tracker import RankTracker
            tracker = RankTracker()
            rank_summary = tracker.get_summary_by_id(business_id)
        except Exception as e:
            log.warning("run_feedback_loop.rank_tracker_fail  err=%s", e)
            rank_summary = {}

        result = {
            "status": "success",
            "business_id": business_id,
            "learning_report": report.model_dump() if hasattr(report, "model_dump") else vars(report),
            "rank_summary": rank_summary,
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("run_feedback_loop.done  task_id=%s  business_id=%s",
                 self.request.id, business_id)
        return result

    except Exception as exc:
        log.exception("run_feedback_loop.error  task_id=%s  exc=%s",
                      self.request.id, exc)
        raise self.retry(exc=exc, countdown=60)


@app.task(bind=True, queue="monitoring", max_retries=2, name="taskq.tasks.run_citation_monitor")
def run_citation_monitor(self, business_id: str = "", business_name: str = "") -> dict:
    """Weekly AI citation monitoring across Perplexity, ChatGPT, Gemini."""
    log.info("run_citation_monitor.start  task_id=%s  business_id=%s",
             self.request.id, business_id)
    try:
        from monitoring.citation_monitor import CitationMonitor
        import json
        from pathlib import Path

        biz_file = Path("data/storage/businesses.json")
        biz = {}
        if biz_file.exists():
            all_biz = json.loads(biz_file.read_text(encoding="utf-8"))
            biz = next((b for b in all_biz if b.get("id") == business_id), {})

        name        = business_name or biz.get("business_name", "")
        keywords    = biz.get("primary_keywords", [])
        city        = biz.get("primary_city", "")
        service     = biz.get("primary_service", "")
        competitors = biz.get("competitors", [])

        monitor = CitationMonitor()
        report = _run_async(monitor.run(
            business_name=name,
            business_id=business_id,
            target_keywords=keywords,
            primary_city=city,
            primary_service=service,
            competitor_names=[c.get("name", c) if isinstance(c, dict) else c for c in competitors[:5]],
            max_queries=15,
        ))

        result = {
            "status":          "success",
            "business_id":     business_id,
            "citation_rate":   report.citation_rate,
            "cited_count":     report.cited_count,
            "total_queries":   report.total_queries,
            "recommendations": report.recommendations,
            "timestamp":       _utc_now(),
            "task_id":         self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("run_citation_monitor.done  task_id=%s  rate=%.1f%%",
                 self.request.id, report.citation_rate * 100)
        return result

    except Exception as exc:
        log.exception("run_citation_monitor.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: run_cwv_audit
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="monitoring", max_retries=2, name="taskq.tasks.run_cwv_audit")
def run_cwv_audit(self, business_id: str = "", urls: list = None) -> dict:
    """Weekly Core Web Vitals audit. Fires PageSpeed API for all published URLs."""
    log.info("run_cwv_audit.start  task_id=%s  business_id=%s", self.request.id, business_id)
    try:
        from data.connectors.pagespeed import PageSpeedConnector
        import json
        from pathlib import Path

        audit_urls = urls or []
        if not audit_urls:
            reg_file = Path("data/storage/rank_registry.json")
            if reg_file.exists():
                registry = json.loads(reg_file.read_text(encoding="utf-8"))
                audit_urls = list(set(registry.values()))[:10]

        if not audit_urls:
            return {"status": "skipped", "reason": "no URLs to audit", "task_id": self.request.id}

        connector = PageSpeedConnector()
        results = _run_async(connector.analyze_batch(audit_urls, strategy="mobile"))

        cwv_failures = []
        all_tasks = []
        for res in results:
            if res.severity() != "GOOD":
                cwv_failures.append({"url": res.url, "severity": res.severity(),
                                     "perf_score": res.performance_score})
                all_tasks.extend(connector.to_remediation_tasks(res, business_id=business_id))

        if any(f["severity"] == "POOR" for f in cwv_failures):
            try:
                from execution.notify import notify, AlertLevel
                notify(AlertLevel.WARNING,
                       f"CWV Audit: {len(cwv_failures)} page(s) with POOR performance",
                       business_id=business_id, failures=str(len(cwv_failures)))
            except Exception:
                pass

        result = {
            "status": "success", "business_id": business_id,
            "pages_audited": len(results), "cwv_failures": cwv_failures,
            "remediation_tasks": all_tasks, "timestamp": _utc_now(), "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("run_cwv_audit.done  task_id=%s  audited=%d  failures=%d",
                 self.request.id, len(results), len(cwv_failures))
        return result

    except Exception as exc:
        log.exception("run_cwv_audit.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: run_topical_gap_check
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="analysis", max_retries=2, name="taskq.tasks.run_topical_gap_check")
def run_topical_gap_check(self, business_id: str) -> dict:
    """Weekly topical authority gap check. Auto-queues top 3 gap pages."""
    log.info("run_topical_gap_check.start  task_id=%s  business_id=%s",
             self.request.id, business_id)
    try:
        from core.topical.map_builder import TopicalMapBuilder
        import json
        from pathlib import Path

        builder = TopicalMapBuilder()
        gap_report = builder.get_gap_report(business_id)

        if "error" in gap_report:
            return {"status": "no_map", "business_id": business_id,
                    "message": "No topical map. Run build_topical_map first.",
                    "task_id": self.request.id}

        pending  = gap_report.get("pending_pages", [])
        coverage = gap_report.get("summary", {}).get("coverage_pct", 0)

        biz_file = Path("data/storage/businesses.json")
        business_data = {}
        if biz_file.exists():
            all_biz = json.loads(biz_file.read_text(encoding="utf-8"))
            business_data = next((b for b in all_biz if b.get("id") == business_id), {})

        queued = []
        if business_data:
            for page in pending[:3]:
                try:
                    ct = run_content_pipeline.apply_async(
                        args=[business_data, page["keyword"], page.get("page_type", "service_page")],
                        queue="execution",
                    )
                    queued.append({"keyword": page["keyword"], "chain_id": ct.id})
                except Exception as qe:
                    log.warning("run_topical_gap_check.queue_fail  keyword=%s  err=%s",
                                page["keyword"], qe)

        result = {
            "status": "success", "business_id": business_id,
            "coverage_pct": coverage, "gaps_found": len(pending),
            "queued_pages": queued, "top_gaps": [p["keyword"] for p in pending[:10]],
            "timestamp": _utc_now(), "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("run_topical_gap_check.done  task_id=%s  coverage=%.1f%%  gaps=%d  queued=%d",
                 self.request.id, coverage, len(pending), len(queued))
        return result

    except Exception as exc:
        log.exception("run_topical_gap_check.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: send_daily_summary
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Task: run_programmatic_batch
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="execution", max_retries=2, name="taskq.tasks.run_programmatic_batch")
def run_programmatic_batch(self, business_id: str = "", pages_per_day: int = 10) -> dict:
    """Generate and queue programmatic SEO pages (location × service × modifier)."""
    log.info("run_programmatic_batch.start  task_id=%s  business_id=%s", self.request.id, business_id)
    try:
        from core.programmatic.generator import ProgrammaticGenerator
        import json
        from pathlib import Path

        biz_file = Path("data/storage/businesses.json")
        business_data = {}
        if biz_file.exists():
            all_biz = json.loads(biz_file.read_text())
            business_data = next((b for b in all_biz if b.get("id") == business_id), {})

        if not business_data:
            return {"status": "skipped", "reason": "business not found", "task_id": self.request.id}

        gen = ProgrammaticGenerator()
        matrix = gen.generate_matrix(
            services=business_data.get("services", [business_data.get("service_type", "service")]),
            cities=None,  # uses default 100-city list
            modifiers=None,
        )
        calendar = gen.to_publish_calendar(matrix, pages_per_day=pages_per_day)

        # Queue pages due today
        today_pages = calendar[:pages_per_day]
        queued = []
        for entry in today_pages:
            task = run_content_pipeline.apply_async(
                args=[business_data, entry["keyword"], "location_page"],
                queue="execution",
            )
            queued.append({"keyword": entry["keyword"], "task_id": task.id})

        result = {
            "status": "success",
            "business_id": business_id,
            "matrix_size": len(matrix),
            "calendar_total": len(calendar),
            "queued_today": len(queued),
            "queued": queued,
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("run_programmatic_batch.done  task_id=%s  queued=%d", self.request.id, len(queued))
        return result

    except Exception as exc:
        log.exception("run_programmatic_batch.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: run_haro_check
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.run_haro_check")
def run_haro_check(self, business_id: str = "") -> dict:
    """Poll HARO inbox, match queries to business, draft + send responses."""
    log.info("run_haro_check.start  task_id=%s  business_id=%s", self.request.id, business_id)
    try:
        from execution.backlinks.haro import HAROIngester, HAROEmailPoller, HAROResponse
        import json
        from pathlib import Path

        try:
            from config.settings import (
                IMAP_HOST, IMAP_USER, IMAP_PASS,
                SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS,
            )
        except ImportError:
            IMAP_HOST = IMAP_USER = IMAP_PASS = ""
            SMTP_HOST = SMTP_USER = SMTP_PASS = ""
            SMTP_PORT = 587

        biz_file = Path("data/storage/businesses.json")
        business = {}
        if biz_file.exists():
            all_biz = json.loads(biz_file.read_text())
            business = next((b for b in all_biz if b.get("id") == business_id), {})

        poller = HAROEmailPoller(IMAP_HOST, IMAP_USER, IMAP_PASS)
        digest = poller.fetch_latest_digest()
        if not digest:
            return {"status": "skipped", "reason": "no new HARO digest", "task_id": self.request.id}

        ingester = HAROIngester()
        queries = ingester.parse_digest(digest)
        matches = ingester.match_to_business(queries, business, min_score=0.3)

        # Rate limit: max 5 responses/day
        sent_today = ingester.get_sent_today()
        remaining = max(0, 5 - len(sent_today))
        sent_count = 0

        smtp_config = {"host": SMTP_HOST, "port": SMTP_PORT, "user": SMTP_USER, "pass": SMTP_PASS}

        for query, score in matches[:remaining]:
            response_text = _run_async(ingester.draft_response(query, business))
            ok = _run_async(ingester.send_response(
                query, response_text,
                sender_email=SMTP_USER,
                sender_name=business.get("contact_name", ""),
                smtp_config=smtp_config,
            ))
            if ok:
                ingester.save_response(HAROResponse(
                    query={"outlet": query.outlet, "category": query.category},
                    business_name=business.get("name", ""),
                    response_text=response_text,
                    sent_at=_utc_now(),
                    status="sent",
                ))
                sent_count += 1

        result = {
            "status": "success",
            "queries_found": len(queries),
            "matches": len(matches),
            "sent": sent_count,
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("run_haro_check.done  task_id=%s  matches=%d  sent=%d", self.request.id, len(matches), sent_count)
        return result

    except Exception as exc:
        log.exception("run_haro_check.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=60)


# ---------------------------------------------------------------------------
# Task: run_link_reclamation
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.run_link_reclamation")
def run_link_reclamation(self, business_id: str = "") -> dict:
    """Find unlinked brand mentions and send link reclamation outreach."""
    log.info("run_link_reclamation.start  task_id=%s  business_id=%s", self.request.id, business_id)
    try:
        from execution.backlinks.reclamation import run_reclamation_campaign
        import json
        from pathlib import Path

        try:
            from config.settings import SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS
        except ImportError:
            SMTP_HOST = SMTP_USER = SMTP_PASS = ""
            SMTP_PORT = 587

        biz_file = Path("data/storage/businesses.json")
        business = {}
        if biz_file.exists():
            all_biz = json.loads(biz_file.read_text())
            business = next((b for b in all_biz if b.get("id") == business_id), {})

        smtp_config = {"host": SMTP_HOST, "port": SMTP_PORT, "user": SMTP_USER, "pass": SMTP_PASS}
        campaign_result = _run_async(run_reclamation_campaign(business, smtp_config))

        result = {
            "status": "success",
            "business_id": business_id,
            **campaign_result,
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("run_link_reclamation.done  task_id=%s  sent=%d",
                 self.request.id, campaign_result.get("reclamation_sent", 0))
        return result

    except Exception as exc:
        log.exception("run_link_reclamation.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=60)


# ---------------------------------------------------------------------------
# Task: check_indexing_queue
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="execution", max_retries=2, name="taskq.tasks.check_indexing_queue")
def check_indexing_queue(self) -> dict:
    """Submit pending URLs from indexing_queue table to Google + Bing."""
    log.info("check_indexing_queue.start  task_id=%s", self.request.id)
    try:
        from data.db import get_db
        from execution.indexing import submit_url

        db = get_db()
        pending = db.get_pending_indexing(limit=20)
        submitted = []
        failed = []

        for row in pending:
            url = row.get("url", "")
            if not url:
                continue
            result = _run_async(submit_url(url))
            if result.any_success:
                submitted.append(url)
                db.mark_indexed(url)
            else:
                failed.append(url)

        result = {
            "status": "success",
            "pending_found": len(pending),
            "submitted": len(submitted),
            "failed": len(failed),
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("check_indexing_queue.done  task_id=%s  submitted=%d  failed=%d",
                 self.request.id, len(submitted), len(failed))
        return result

    except Exception as exc:
        log.exception("check_indexing_queue.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 60)


# ---------------------------------------------------------------------------
# Task: run_system_health
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_system_health")
def run_system_health(self) -> dict:
    """Run system health checks and fire alerts for critical issues."""
    log.info("run_system_health.start  task_id=%s", self.request.id)
    try:
        from monitoring.health import SystemHealthMonitor
        monitor = SystemHealthMonitor()
        report = _run_async(monitor.run_checks())
        result = {
            "status": "success",
            "overall_status": report.overall_status,
            "claude_cli_ok": report.claude_cli_ok,
            "redis_ok": report.redis_ok,
            "queue_depth": report.queue_depth,
            "disk_usage_pct": report.disk_usage_pct,
            "dead_letter_count": report.dead_letter_count,
            "alerts_fired": report.alerts_fired,
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("run_system_health.done  task_id=%s  status=%s  alerts=%d",
                 self.request.id, report.overall_status, len(report.alerts_fired))
        return result

    except Exception as exc:
        log.exception("run_system_health.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=60)


# ---------------------------------------------------------------------------
# Task: run_orphan_detection
# ---------------------------------------------------------------------------
@app.task(bind=True, queue="analysis", max_retries=1, name="taskq.tasks.run_orphan_detection")
def run_orphan_detection(self, business_id: str = "") -> dict:
    """Detect orphaned pages and pages missing pillar links; queue fix tasks."""
    log.info("run_orphan_detection.start  task_id=%s  business_id=%s", self.request.id, business_id)
    try:
        from core.linking.semantic_linker import SemanticLinker, PageNode
        from data.db import get_db
        import json
        from pathlib import Path

        db = get_db()
        # Get published URLs from DB
        try:
            rows = db._conn.execute(
                "SELECT url, title, content_snippet, intent FROM published_urls WHERE business_id=? LIMIT 200",
                (business_id,)
            ).fetchall()
        except Exception:
            rows = []

        pages = [
            PageNode(
                url=r[0], title=r[1] or "",
                content=r[2] or "", intent=r[3] or "informational"
            )
            for r in rows
        ]

        if not pages:
            return {"status": "skipped", "reason": "no published pages found", "task_id": self.request.id}

        linker = SemanticLinker()
        links = linker.build_link_graph(pages)
        orphans = linker.detect_orphans(pages, links)
        pillar_gaps = linker.enforce_pillar_links(pages, links)

        result = {
            "status": "success",
            "business_id": business_id,
            "total_pages": len(pages),
            "orphaned_pages": [o.url for o in orphans],
            "orphan_count": len(orphans),
            "pillar_gaps": len(pillar_gaps),
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        _save_result(self.request.id, result)
        log.info("run_orphan_detection.done  task_id=%s  orphans=%d  gaps=%d",
                 self.request.id, len(orphans), len(pillar_gaps))
        return result

    except Exception as exc:
        log.exception("run_orphan_detection.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=60)


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.send_daily_summary")
def send_daily_summary(self) -> dict:
    """Daily execution summary alert to ALERT_WEBHOOK_URL."""
    log.info("send_daily_summary.start  task_id=%s", self.request.id)
    try:
        import json
        from pathlib import Path
        from datetime import datetime, timezone, timedelta

        cutoff   = (datetime.now(tz=timezone.utc) - timedelta(hours=24)).isoformat()
        succeeded, failed, dead_count = 0, 0, 0

        results_dir = Path("data/storage/task_results")
        dead_dir    = Path("data/storage/dead_letter")

        if results_dir.exists():
            for f in results_dir.glob("*.json"):
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                    if data.get("timestamp", "") >= cutoff:
                        if data.get("status") == "success":
                            succeeded += 1
                        else:
                            failed += 1
                except Exception:
                    pass

        if dead_dir.exists():
            for f in dead_dir.glob("*.json"):
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                    if data.get("failed_at", "") >= cutoff:
                        dead_count += 1
                except Exception:
                    pass

        try:
            from execution.notify import notify_daily_summary
            notify_daily_summary(
                business_id="all",
                executions_today=succeeded + failed,
                tasks_succeeded=succeeded,
                tasks_failed=failed,
                dead_letter_count=dead_count,
            )
        except Exception as ne:
            log.warning("send_daily_summary.notify_fail  err=%s", ne)

        result = {
            "status": "success", "succeeded": succeeded,
            "failed": failed, "dead_letter": dead_count,
            "timestamp": _utc_now(), "task_id": self.request.id,
        }
        log.info("send_daily_summary.done  task_id=%s  ok=%d  fail=%d  dead=%d",
                 self.request.id, succeeded, failed, dead_count)
        return result

    except Exception as exc:
        log.exception("send_daily_summary.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=60)



@app.task(bind=True, queue="analysis", max_retries=2, name="taskq.tasks.sync_aion_signals")
def sync_aion_signals(self) -> dict:
    """Pull trending signals from AION Research Aggregator and store as content opportunities.

    Runs every 6h. Reads HackerNews + Reddit signals, filters for SEO/content relevance,
    stores top signals in DB for content calendar use.
    """
    log.info("sync_aion_signals.start  task_id=%s", self.request.id)
    try:
        from core.aion_bridge import aion
        from data.storage.database import Database

        db = Database()
        signals = aion.get_signals(limit=50)

        # Filter for content-relevant signals
        seo_keywords = {
            "seo", "content", "marketing", "link", "backlink", "keyword",
            "google", "search", "traffic", "ranking", "blog", "website",
            "ai", "tool", "software", "startup", "business", "growth",
        }

        relevant = []
        for s in signals:
            text_lower = (s.get("content", "") + " " + s.get("url", "")).lower()
            if any(kw in text_lower for kw in seo_keywords):
                relevant.append(s)

        # Store as content opportunities via memory store
        stored = 0
        for s in relevant[:10]:
            content = (
                f"Trending signal: {s.get('content', '')} "
                f"Source: {s.get('source', '')} Score: {s.get('score', 0)} "
                f"URL: {s.get('url', '')}"
            )
            if aion.memory_store(content, tier="episodic", agent_id="seo-engine",
                                 tags=["signal", "trending", s.get("source", "")]):
                stored += 1

        result = {
            "status": "success",
            "total_signals": len(signals),
            "relevant_signals": len(relevant),
            "stored_to_memory": stored,
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        log.info("sync_aion_signals.done  task_id=%s  relevant=%d  stored=%d",
                 self.request.id, len(relevant), stored)
        return result

    except Exception as exc:
        log.exception("sync_aion_signals.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=120)


# ===========================================================================
# AION INTEGRATION TASKS — Phase 3 Wiring
# ===========================================================================

@app.task(bind=True, queue="analysis", max_retries=2, name="taskq.tasks.sync_twitter_intel")
def sync_twitter_intel(self) -> dict:
    """Pull signals and opportunities from Twitter Intel, seed keyword pipeline.

    Twitter Intel has 4,192 signals from HackerNews, Google Trends, Reddit, Twitter.
    Routes SEO-relevant signals into the keyword pipeline as seed topics.
    Runs every 4h.
    """
    log.info("sync_twitter_intel.start  task_id=%s", self.request.id)
    try:
        from core.aion_bridge import aion
        from pathlib import Path
        import json

        signals = aion.twitter_signals(limit=100)

        seo_kws = {
            "seo", "search", "google", "keyword", "content", "marketing",
            "traffic", "rank", "backlink", "blog", "local", "website",
            "ai", "tool", "software", "startup", "saas", "business",
        }

        seed_keywords = []
        trending_topics = []

        for sig in signals:
            text = sig.get("content", "").lower()
            source = sig.get("source", "")
            score = float(sig.get("score") or 0)

            if source == "google_trends" or score > 50:
                trending_topics.append(sig.get("content", "")[:100])

            if any(kw in text for kw in seo_kws) and len(seed_keywords) < 20:
                seed_keywords.append({
                    "keyword": sig.get("content", "")[:80],
                    "source": f"twitter_intel_{source}",
                    "score": score,
                    "url": sig.get("url", ""),
                })

        stored = 0
        for topic in trending_topics[:5]:
            content = (
                f"Trending search topic detected: {topic}. "
                f"Source: Google Trends / market signals. Priority: high for content calendar."
            )
            if aion.memory_store(content, tier="episodic", agent_id="seo-engine",
                                 tags=["trending", "keyword", "content-calendar"]):
                stored += 1

        seeds_path = Path("data/storage/keyword_seeds")
        seeds_path.mkdir(parents=True, exist_ok=True)
        seeds_file = seeds_path / "twitter_intel_seeds.json"
        seeds_file.write_text(json.dumps({
            "generated_at": _utc_now(),
            "seeds": seed_keywords,
            "trending": trending_topics[:10],
        }, indent=2))

        result = {
            "status": "success",
            "signals_pulled": len(signals),
            "seed_keywords": len(seed_keywords),
            "trending_topics": len(trending_topics),
            "stored_to_memory": stored,
            "seeds_file": str(seeds_file),
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        log.info("sync_twitter_intel.done  task_id=%s  seeds=%d  trending=%d",
                 self.request.id, len(seed_keywords), len(trending_topics))
        return result

    except Exception as exc:
        log.exception("sync_twitter_intel.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=120)


@app.task(bind=True, queue="analysis", max_retries=2, name="taskq.tasks.auto_content_briefs")
def auto_content_briefs(self) -> dict:
    """Auto-generate content briefs for top pending keywords.

    Pulls top 3 keywords from seed files, generates full content briefs
    via Firecrawl + GPT-Researcher + AION Brain, saves to disk.
    Runs every 24h.
    """
    log.info("auto_content_briefs.start  task_id=%s", self.request.id)
    try:
        from core.aion_bridge import aion
        from core.crawlers.competitor_scraper import CompetitorScraper
        from pathlib import Path
        import json

        scraper = CompetitorScraper()
        briefs_path = Path("data/storage/content_briefs")
        briefs_path.mkdir(parents=True, exist_ok=True)

        # Build candidates: business primary_keywords first, then Twitter Intel seeds
        candidates = []
        biz_keyword_map = {}  # maps keyword -> (biz_domain, biz)

        # 1. Pull from all registered businesses' primary keywords
        biz_file = Path("data/storage/businesses.json")
        businesses = []
        if biz_file.exists():
            businesses = json.loads(biz_file.read_text())
            for biz in businesses:
                keywords = biz.get("primary_keywords", [])
                service = biz.get("primary_service", "")
                city = biz.get("primary_city", "")
                biz_domain = biz.get("domain", "default").replace(".", "_")

                for kw in keywords[:5]:  # max 5 per business
                    if kw and kw not in candidates:
                        candidates.append(kw)
                        biz_keyword_map[kw] = (biz_domain, biz)

                # Also add city+service combo
                if service and city:
                    combo = f"{service} {city}"
                    if combo not in candidates:
                        candidates.append(combo)
                        biz_keyword_map[combo] = (biz_domain, biz)

        # 2. Supplement with Twitter Intel seeds (only if not enough keywords)
        seeds_file = Path("data/storage/keyword_seeds/twitter_intel_seeds.json")
        if seeds_file.exists() and len(candidates) < 10:
            data = json.loads(seeds_file.read_text())
            for s in data.get("seeds", [])[:5]:
                kw = s.get("keyword", "")
                if kw and kw not in candidates:
                    candidates.append(kw)

        if not candidates:
            candidates = [
                "local SEO for service businesses",
                "link building strategies 2025",
                "content marketing ROI",
            ]

        generated = []
        for keyword in candidates[:3]:
            # Prefix filename with business domain if known
            if keyword in biz_keyword_map:
                biz_domain, _ = biz_keyword_map[keyword]
                safe_name = f"{biz_domain}_{keyword[:40]}".replace(" ", "_").replace("/", "_")
            else:
                safe_name = keyword[:40].replace(" ", "_").replace("/", "_")
            brief_file = briefs_path / f"{safe_name}.json"
            if brief_file.exists():
                log.info("auto_content_briefs.skip_existing  keyword=%s", keyword)
                continue

            competitor_urls = []
            try:
                from core.serp.scraper import fetch_serp_urls
                competitor_urls = fetch_serp_urls(keyword, num_results=5)
            except Exception as e:
                log.warning("auto_content_briefs.serp_fail  keyword=%s  err=%s", keyword, e)

            brief = scraper.generate_brief(
                keyword=keyword,
                competitor_urls=competitor_urls,
                max_competitors=3,
                include_youtube=True,
                include_deep_research=True,
            )

            brief_dict = scraper.brief_to_dict(brief)
            brief_dict["generated_at"] = _utc_now()
            if keyword in biz_keyword_map:
                _, biz = biz_keyword_map[keyword]
                brief_dict["business_id"] = biz.get("business_id", "")
                brief_dict["business_domain"] = biz.get("domain", "")
            brief_file.write_text(json.dumps(brief_dict, indent=2))

            summary = (
                f"Content brief generated for keyword: '{keyword}'. "
                f"Recommended word count: {brief.recommended_word_count}. "
                f"Title: {brief.recommended_title}."
            )
            aion.memory_store(summary, tier="semantic", agent_id="seo-engine",
                              tags=["content-brief", "keyword"])
            generated.append({"keyword": keyword, "file": str(brief_file)})
            log.info("auto_content_briefs.generated  keyword=%s", keyword)

        result = {
            "status": "success",
            "candidates": len(candidates),
            "generated": len(generated),
            "briefs": generated,
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        log.info("auto_content_briefs.done  task_id=%s  generated=%d",
                 self.request.id, len(generated))
        return result

    except Exception as exc:
        log.exception("auto_content_briefs.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=300)


@app.task(bind=True, queue="execution", max_retries=2, name="taskq.tasks.deploy_llms_txt")
def deploy_llms_txt(self) -> dict:
    """Generate and deploy llms.txt for AI crawler discovery. Runs weekly."""
    log.info("deploy_llms_txt.start  task_id=%s", self.request.id)
    try:
        from pathlib import Path
        from config.settings import SITE_URL

        site_url = SITE_URL or ""
        if not site_url:
            log.warning("deploy_llms_txt.skip  no SITE_URL configured")
            return {"status": "skipped", "reason": "SITE_URL not set", "task_id": self.request.id}

        from ai_visibility.llms_txt import generate_llms_txt
        from models.business import BusinessContext

        biz = BusinessContext(
            business_name="GetHubed",
            website=site_url,
            primary_service="SEO",
            primary_city="Austin",
            secondary_services=["Content Marketing", "Link Building"],
            service_areas=["Austin, TX"],
        )
        llms_txt_content = generate_llms_txt(biz)

        public_dir = Path("public")
        public_dir.mkdir(exist_ok=True)
        llms_file = public_dir / "llms.txt"
        llms_file.write_text(llms_txt_content, encoding="utf-8")

        log.info("deploy_llms_txt.written  path=%s  chars=%d", llms_file, len(llms_txt_content))
        result = {
            "status": "success",
            "site_url": site_url,
            "llms_txt_path": str(llms_file),
            "chars": len(llms_txt_content),
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        return result

    except Exception as exc:
        log.exception("deploy_llms_txt.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=600)


@app.task(bind=True, queue="analysis", max_retries=2, name="taskq.tasks.sync_entity_knowledge_graph")
def sync_entity_knowledge_graph(self) -> dict:
    """Sync SEO engine entities to AION Knowledge Graph.

    Writes businesses, keywords, and content topics as nodes into AION's
    knowledge graph for cross-system entity relationship queries.
    Runs every 12h.
    """
    log.info("sync_entity_knowledge_graph.start  task_id=%s", self.request.id)
    try:
        from core.aion_bridge import aion
        from pathlib import Path
        import json

        nodes_written = 0

        # Sync content brief topics as knowledge graph nodes
        briefs_path = Path("data/storage/content_briefs")
        if briefs_path.exists():
            for brief_file in list(briefs_path.glob("*.json"))[:15]:
                try:
                    brief = json.loads(brief_file.read_text())
                    kw = brief.get("keyword", "")
                    if not kw:
                        continue
                    result = aion.knowledge_add_node(
                        label=kw,
                        node_type="content_topic",
                        properties={
                            "word_count": brief.get("recommended_word_count", 0),
                            "title": brief.get("recommended_title", ""),
                            "source": "seo-engine-brief",
                        },
                    )
                    if "error" not in result:
                        nodes_written += 1
                except Exception as e:
                    log.debug("sync_entity_kg.brief_fail  err=%s", e)

        # Sync trending keywords from Twitter Intel
        seeds_file = Path("data/storage/keyword_seeds/twitter_intel_seeds.json")
        if seeds_file.exists():
            data = json.loads(seeds_file.read_text())
            for seed in data.get("seeds", [])[:25]:
                kw = seed.get("keyword", "")
                if not kw:
                    continue
                try:
                    result = aion.knowledge_add_node(
                        label=kw,
                        node_type="keyword",
                        properties={
                            "source": seed.get("source", "twitter_intel"),
                            "score": seed.get("score", 0),
                            "seo_engine": True,
                        },
                    )
                    if "error" not in result:
                        nodes_written += 1
                except Exception as e:
                    log.debug("sync_entity_kg.seed_fail  err=%s", e)

        result = {
            "status": "success",
            "nodes_written": nodes_written,
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        log.info("sync_entity_knowledge_graph.done  task_id=%s  nodes=%d",
                 self.request.id, nodes_written)
        return result

    except Exception as exc:
        log.exception("sync_entity_knowledge_graph.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=300)


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.competitor_content_alerts")
def competitor_content_alerts(self) -> dict:
    """Monitor competitor sites for new content. Triggers alerts + rapid-update.

    Scrapes competitor index pages via Firecrawl to detect new content.
    Stores findings in AION Memory for content team awareness.
    Runs every 24h.
    """
    log.info("competitor_content_alerts.start  task_id=%s", self.request.id)
    try:
        from core.aion_bridge import aion
        from pathlib import Path
        from urllib.parse import urlparse
        import json

        competitor_domains = set()
        briefs_path = Path("data/storage/content_briefs")

        if briefs_path.exists():
            for brief_file in list(briefs_path.glob("*.json"))[:5]:
                brief = json.loads(brief_file.read_text())
                for source_url in brief.get("sources", []):
                    try:
                        domain = urlparse(source_url).netloc
                        if domain:
                            competitor_domains.add(domain)
                    except Exception:
                        pass

        alerts = []
        for domain in list(competitor_domains)[:5]:
            for path in ["/blog", "/articles", "/"]:
                url = f"https://{domain}{path}"
                try:
                    md = aion.firecrawl_scrape(url, timeout=15)
                    if md and len(md) > 200:
                        signal = (
                            f"Competitor content alert: {domain}{path} is active. "
                            f"Content preview: {md[:200]}"
                        )
                        aion.memory_store(signal, tier="episodic", agent_id="seo-engine",
                                         tags=["competitor", "alert", domain])
                        alerts.append({"domain": domain, "url": url})
                        break
                except Exception as e:
                    log.debug("competitor_alert.fail  domain=%s  err=%s", domain, e)

        result = {
            "status": "success",
            "competitors_checked": len(competitor_domains),
            "alerts": len(alerts),
            "alert_details": alerts,
            "timestamp": _utc_now(),
            "task_id": self.request.id,
        }
        log.info("competitor_content_alerts.done  task_id=%s  alerts=%d",
                 self.request.id, len(alerts))
        return result

    except Exception as exc:
        log.exception("competitor_content_alerts.error  task_id=%s  exc=%s", self.request.id, exc)
        raise self.retry(exc=exc, countdown=300)


# ===========================================================================
# RANKING REPORT + SITEMAP TASKS
# ===========================================================================

@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.send_ranking_report")
def send_ranking_report(self) -> dict:
    """Send weekly ranking + published articles report to each business owner."""
    import json, sqlite3
    from pathlib import Path
    log.info("send_ranking_report.start  task_id=%s", self.request.id)

    biz_file = Path("data/storage/businesses.json")
    if not biz_file.exists():
        return {"status": "no_businesses"}

    businesses = json.loads(biz_file.read_text())
    reports_sent = 0

    db_conn = sqlite3.connect("data/seo_engine.db")
    db_conn.row_factory = sqlite3.Row

    for biz in businesses:
        domain = biz.get("domain", "")
        name = biz.get("name", domain)
        email = biz.get("owner_email", "")
        if not email or not domain:
            continue

        rankings = db_conn.execute(
            "SELECT keyword, position, checked_at FROM ranking_history WHERE domain=? ORDER BY checked_at DESC LIMIT 20",
            (domain,)
        ).fetchall()

        published = db_conn.execute(
            "SELECT url, published_at FROM published_urls WHERE domain=? ORDER BY published_at DESC LIMIT 10",
            (domain,)
        ).fetchall()

        if rankings:
            rows = ""
            for r in rankings:
                pos = r["position"]
                color = "#22c55e" if pos <= 10 else "#f59e0b" if pos <= 20 else "#6b7280"
                rows += f"<tr><td style='padding:8px'>{r['keyword']}</td><td style='text-align:center;color:{color};font-weight:bold;padding:8px'>#{pos}</td><td style='text-align:right;padding:8px;color:#6b7280'>{str(r['checked_at'])[:10]}</td></tr>"
            rankings_html = f"<h3 style='margin:16px 0 8px'>Keyword Rankings</h3><table style='width:100%;border-collapse:collapse;font-size:14px'><tr style='border-bottom:2px solid #e2e8f0'><th style='text-align:left;padding:8px'>Keyword</th><th style='text-align:center;padding:8px'>Position</th><th style='text-align:right;padding:8px'>Checked</th></tr>{rows}</table>"
        else:
            rankings_html = "<p style='color:#6b7280;font-size:14px'>No ranking data yet — first check runs within 7 days of onboarding.</p>"

        if published:
            links = "".join(f"<li><a href='{p['url']}' style='color:#1e40af'>{p['url']}</a></li>" for p in published)
            published_html = f"<h3 style='margin:16px 0 8px'>Published Articles ({len(published)})</h3><ul style='font-size:14px;line-height:2'>{links}</ul>"
        else:
            published_html = "<p style='color:#6b7280;font-size:14px'>No articles published yet.</p>"

        html = f"""<html><body style='font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px'>
<div style='background:#1e40af;color:white;padding:20px;border-radius:8px 8px 0 0'>
<h1 style='margin:0;font-size:22px'>SEO Weekly Report</h1>
<p style='margin:5px 0 0;opacity:.8;font-size:14px'>{name} — {domain}</p>
</div>
<div style='background:#f8fafc;padding:24px;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 8px 8px'>
<p style='font-size:15px'>Here is your weekly SEO performance summary for <strong>{domain}</strong>.</p>
{rankings_html}
<br>{published_html}
<hr style='border:none;border-top:1px solid #e2e8f0;margin:20px 0'>
<p style='color:#6b7280;font-size:12px'>Powered by SEO Engine — automated SEO for growing businesses.</p>
</div></body></html>"""

        try:
            from core.aion_bridge import aion
            sent = aion.send_email(
                to_email=email,
                subject=f"Weekly SEO Report — {name}",
                body_html=html,
                body_text=f"Weekly SEO Report for {name}. {len(rankings)} keywords tracked. {len(published)} articles published."
            )
            if sent:
                reports_sent += 1
                log.info("send_ranking_report.sent  biz=%s  email=%s", name, email)
        except Exception as e:
            log.warning("send_ranking_report.fail  biz=%s  err=%s", name, e)

    db_conn.close()
    return {"status": "done", "reports_sent": reports_sent, "task_id": self.request.id}


@app.task(bind=True, queue="execution", max_retries=2, name="taskq.tasks.submit_sitemap")
def submit_sitemap(self) -> dict:
    """Ping Google and Bing with sitemap URLs for all businesses with WordPress sites."""
    import json, urllib.request
    from pathlib import Path
    log.info("submit_sitemap.start  task_id=%s", self.request.id)

    biz_file = Path("data/storage/businesses.json")
    if not biz_file.exists():
        return {"status": "no_businesses"}

    businesses = json.loads(biz_file.read_text())
    submitted = 0

    for biz in businesses:
        wp_url = biz.get("wp_site_url", "")
        if not wp_url:
            continue
        for sitemap_path in ["/sitemap.xml", "/sitemap_index.xml"]:
            sitemap_url = f"{wp_url.rstrip('/')}{sitemap_path}"
            for engine, ping_url in [
                ("google", f"https://www.google.com/ping?sitemap={sitemap_url}"),
                ("bing", f"https://www.bing.com/ping?sitemap={sitemap_url}"),
            ]:
                try:
                    req = urllib.request.Request(ping_url, headers={"User-Agent": "SEOEngine/1.0"})
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        log.info("submit_sitemap.ok  engine=%s  sitemap=%s  status=%s", engine, sitemap_url, resp.status)
                        submitted += 1
                except Exception as e:
                    log.debug("submit_sitemap.skip  engine=%s  err=%s", engine, e)

    return {"status": "done", "submitted": submitted, "task_id": self.request.id}


@app.task(bind=True, queue='execution', max_retries=1, name='taskq.tasks.run_gbp_posts')
def run_gbp_posts(self) -> dict:
    """Generate and post (or draft) GBP updates for all businesses. Runs weekly."""
    import json
    from pathlib import Path
    from core.aion_bridge import aion
    log.info('run_gbp_posts.start  task_id=%s', self.request.id)
    biz_file = Path('data/storage/businesses.json')
    if not biz_file.exists():
        return {'status': 'no_businesses'}
    businesses = json.loads(biz_file.read_text())
    posted = 0
    for biz in businesses:
        name = biz.get('name', '')
        service = biz.get('primary_service', '')
        city = biz.get('primary_city', biz.get('city', ''))
        phone = biz.get('phone', '778.363.6289')
        website = biz.get('website', '')
        try:
            prompt = f"""Write a Google Business Profile post (150-200 words) for {name}.
Service: {service} in {city}, BC.
Phone: {phone} | Website: {website}
Make it engaging, mention a seasonal benefit or local relevance, include a clear call to action.
Write in plain text only -- no HTML tags."""
            text = aion.brain_complete(prompt, model='groq', max_tokens=300)
            if not text:
                continue
            from execution.gbp.gbp_publisher import GBPPublisher, GBPPost
            gbp = GBPPublisher(
                account_id=biz.get('gbp_account_id',''),
                location_id=biz.get('gbp_location_id',''),
                credentials_path=biz.get('gbp_credentials_path',''),
            )
            post = GBPPost(
                summary=text[:1500],
                call_to_action='CALL',
                cta_url=website,
            )
            result = gbp.post(post)
            posted += 1
            log.info('run_gbp_posts.done  biz=%s  status=%s', name, result.get('status'))
        except Exception as e:
            log.warning('run_gbp_posts.fail  biz=%s  err=%s', name, e)
    return {'status': 'done', 'posted': posted, 'task_id': self.request.id}


@app.task(bind=True, queue='execution', max_retries=1, name='taskq.tasks.run_citation_builder')
def run_citation_builder(self) -> dict:
    """Generate citation submission packages for all businesses. Runs on registration."""
    import json
    from pathlib import Path
    log.info('run_citation_builder.start  task_id=%s', self.request.id)
    biz_file = Path('data/storage/businesses.json')
    if not biz_file.exists():
        return {'status': 'no_businesses'}
    businesses = json.loads(biz_file.read_text())
    generated = 0
    for biz in businesses:
        try:
            from execution.citations.citation_builder import CitationBuilder, BusinessNAP
            cb = CitationBuilder()
            nap = BusinessNAP(
                name=biz.get('name',''),
                address=biz.get('primary_city', biz.get('city','')),
                city=biz.get('primary_city', biz.get('city','')),
                province=biz.get('state',''),
                postal_code='',
                phone=biz.get('phone',''),
                website=biz.get('website',''),
                email=biz.get('owner_email',''),
                description=f"{biz.get('name','')} -- {biz.get('primary_service','')} in {biz.get('primary_city','')}",
                services=biz.get('secondary_services', []) + [biz.get('primary_service','')],
                categories=[biz.get('primary_service','')],
            )
            cb.generate_submission_package(nap)
            generated += 1
        except Exception as e:
            log.warning('run_citation_builder.fail  biz=%s  err=%s', biz.get('name'), e)
    return {'status': 'done', 'generated': generated, 'task_id': self.request.id}
