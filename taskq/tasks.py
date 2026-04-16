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
        if schema:
            content_html += f'\n<script type="application/ld+json">{json.dumps(schema)}</script>'

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

