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
            from data.connectors.rank_tracker import RankTracker
            tracker = RankTracker()
            log.info("check_rankings.tracker_loaded  type=RankTracker")
        except Exception as _rte:
            log.warning("check_rankings.tracker_fallback  err=%s", _rte)
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
                    # RankTracker.check_rankings(domain, keywords) — sync call
                    domain = site_url.replace("https://", "").replace("http://", "").rstrip("/")
                    keywords = biz.get("primary_keywords", [])
                    if not keywords:
                        service = biz.get("primary_service", "")
                        city = biz.get("primary_city", biz.get("city", ""))
                        if service and city:
                            keywords = [f"{service} {city}"]
                    if keywords:
                        results = tracker.check_rankings(domain, keywords[:10])
                        # Write to ranking_history SQLite table
                        try:
                            import sqlite3, datetime
                            db_path = os.getenv("DB_PATH", "data/storage/seo_engine.db")
                            db = sqlite3.connect(db_path)
                            for r in results:
                                db.execute(
                                    "INSERT OR REPLACE INTO ranking_history (business_id, keyword, position, url, checked_at) VALUES (?,?,?,?,?)",
                                    (bid, r["keyword"], r.get("rank"), r.get("url", ""), r["checked_at"])
                                )
                            db.commit()
                            db.close()
                            log.info("check_rankings.sqlite_written  bid=%s  keywords=%d", bid, len(results))
                        except Exception as _dbe:
                            log.warning("check_rankings.sqlite_fail  err=%s", _dbe)
                        # Write to PG rank_history with 2h dedup (P1-E)
                        try:
                            from core.pg import execute_one, execute_write
                            from core.audit import log_event, A_RANK_TRACKED
                            _pg_tenant_id = biz.get("tenant_id", biz.get("id", ""))
                            if _pg_tenant_id:
                                _written = 0
                                for r in results:
                                    _kw_str = r.get("keyword", "")
                                    _pos = r.get("rank")
                                    _url = r.get("url", "")
                                    # 2h dedup: skip if same keyword observed within 2h
                                    _recent = execute_one(
                                        "SELECT id FROM rank_history "
                                        "WHERE tenant_id = %s AND keyword = %s "
                                        "AND observed_at > NOW() - INTERVAL '2 hours' LIMIT 1",
                                        [_pg_tenant_id, _kw_str],
                                        tenant_id=_pg_tenant_id,
                                    )
                                    if _recent:
                                        log.debug("check_rankings.dedup_skip  kw=%s  tenant=%s", _kw_str, _pg_tenant_id[:8])
                                        continue
                                    execute_write(
                                        "INSERT INTO rank_history (tenant_id, keyword, position, url) "
                                        "VALUES (%s, %s, %s, %s)",
                                        [_pg_tenant_id, _kw_str, _pos, _url],
                                        tenant_id=_pg_tenant_id,
                                    )
                                    _written += 1
                                if _written:
                                    log_event(_pg_tenant_id, "system", A_RANK_TRACKED,
                                              entity_type="keyword",
                                              diff={"keywords_written": _written, "domain": domain})
                                log.info("check_rankings.pg_written  tenant=%s  written=%d  deduped=%d",
                                         _pg_tenant_id[:8], _written, len(results) - _written)
                        except Exception as _pge:
                            log.warning("check_rankings.pg_fail  err=%s", _pge)
                        summary = tracker.get_summary_report.__func__(tracker, domain, keywords[:10]) if hasattr(tracker, "get_summary_report") else {}
                        rank_deltas.append({"business_id": bid, "domain": domain, "results": results, "keywords_checked": len(results)})
                    else:
                        rank_deltas.append({"business_id": bid, "note": "no keywords configured"})
                else:
                    rank_deltas.append({
                        "business_id": bid,
                        "delta": {},
                        "note": "rank_tracker not available — set DATAFORSEO_LOGIN/PASSWORD for live data",
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

        # Grok brand check — queries x.com/i/grok via browser (no API key)
        grok_result = {}
        try:
            from core.llm_pool import call_grok as _grok
            grok_query = f"What do people say about {business_name}? Any reviews or mentions?"
            grok_response = _grok(grok_query, wait_seconds=20.0)
            if grok_response:
                cited_in_grok = business_name.lower() in grok_response.lower()
                grok_result = {
                    "engine": "grok",
                    "cited": cited_in_grok,
                    "snippet": grok_response[:500],
                    "query": grok_query,
                }
                log.info(
                    "monitor_ai_citations.grok  business=%s  cited=%s  chars=%d",
                    business_name, cited_in_grok, len(grok_response),
                )
        except Exception as ge:
            log.warning("monitor_ai_citations.grok_fail  err=%s", ge)

        result = {
            "status": "success",
            "business_id": business_id,
            "business_name": business_name,
            "citations": citations,
            "citation_count": len(citations),
            "grok_check": grok_result,
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
# Content quality validator (used by generate_content retry loop)
# ---------------------------------------------------------------------------

def validate_content_output(html):
    checks = {
        "quick_answer": "background:#f0fdf4" in html or "Quick Answer" in html,
        "cta_block": ("linear-gradient" in html or "tel:+" in html),
        "faq_section": "FAQ" in html or "Frequently Asked" in html,
        "word_count": len(html.split()) >= 900,
        "h2_present": html.lower().count("<h2") >= 2,
    }
    missing = [k for k, v in checks.items() if not v]
    return len(missing) == 0, missing


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

        # ── Pre-generate: PAA questions + snippet format for this keyword ─────────
        _paa_block = ""
        _snippet_block = ""
        try:
            from data.analyzers.paa_tree import PAATree
            _paa = PAATree()
            _paa_qs = _paa.get_questions(keyword, use_cache=True)
            if _paa_qs:
                _paa_block = "\nPEOPLE ALSO ASK (include as H3 headings with direct answers):\n" + "\n".join(f"- {q}" for q in _paa_qs[:6])
        except Exception as _pe:
            log.debug("generate_content.paa_skip  err=%s", _pe)
        try:
            from data.analyzers.snippet_format import SnippetFormatOptimizer
            _sfo = SnippetFormatOptimizer()
            _sfo_result = _sfo.analyze(keyword)
            _snippet_format = getattr(_sfo_result, "format_needed", "paragraph")
            _snippet_block = f"\nFEATURED SNIPPET FORMAT for this keyword: {_snippet_format}. Structure content accordingly (paragraph=direct answer <60 words; numbered_list=steps with H3s; table=comparison rows)."
        except Exception as _se:
            log.debug("generate_content.snippet_skip  err=%s", _se)

        # ── Cluster context (P1-05) ───────────────────────────────────────────────
        _cluster_block = ""
        try:
            from data.analyzers.cluster_context import get_cluster_context
            _cluster_ctx = get_cluster_context(keyword, business_data.get("business_id", ""))
            if _cluster_ctx.get("found"):
                _cluster_block = _cluster_ctx.get("prompt_block", "")
        except Exception as _ce:
            log.debug("generate_content.cluster_skip  err=%s", _ce)

        prompt = f"""Generate a complete SEO-optimised {page_type} for the following:

Business: {business.business_name}
Keyword: {keyword}
City: {business.primary_city}
Service: {business.primary_service}
Website: {business.website}{_paa_block}{_snippet_block}{_cluster_block}

REQUIREMENTS:
- Title tag: 50-60 chars, starts with keyword
- Meta description: 150-160 chars, includes keyword + city + CTA
- H1: matches keyword intent
- Body: 1,400+ words minimum, includes keyword + LSI terms naturally
- 2-4 internal link placeholders: {{LINK:anchor text:relative/path}}
- 1 FAQ section (5 Q&A pairs) with FAQPage schema
- LocalBusiness schema with address, service, areaServed
- HowTo schema block for the main service (numbered steps)
- Speakable property on Article schema (mark FAQ section and Quick Answer)

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

        retry_prompt = prompt
        raw = ""
        for attempt in range(3):
            raw = call_claude(retry_prompt, max_tokens=4096)
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            ok, missing = validate_content_output(raw)
            if ok:
                break
            log.warning(
                "generate_content.validation_fail  attempt=%d  missing=%s",
                attempt, missing,
            )
            retry_prompt = (
                prompt
                + f"\n\nCRITICAL: Previous output was missing these required elements: {missing}."
                  " You MUST include ALL of them explicitly in your response."
            )
        else:
            log.error(
                "generate_content.dead_letter  keyword=%s  missing=%s", keyword, missing
            )
            # Still continue with what we have rather than dropping entirely

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

    # Publishing kill switch (Phase 0-B)
    try:
        import redis as _redis_ks, os as _os_ks
        _r_ks = _redis_ks.from_url(
            _os_ks.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True, socket_timeout=2,
        )
        _biz_id_ks = business_data.get("id", business_data.get("business_id", ""))
        if _r_ks.get(f"pause:publish:{_biz_id_ks}"):
            log.warning("publish_content.paused  business_id=%s", _biz_id_ks)
            return {"status": "paused", "reason": "publishing_paused", "task_id": self.request.id}
    except Exception as _ks_err:
        log.debug("publish_content.kill_switch_check_fail  err=%s", _ks_err)

    # Atomic publish slot reservation (P1-C: race-condition fix)
    _tenant_id_pc = business_data.get("id", business_data.get("tenant_id", ""))
    _plan_limit_pc = int(business_data.get("plan_pages_per_month", 10))
    _slot_reserved = False
    if _tenant_id_pc:
        try:
            from core.publish_slots import reserve_publish_slot
            _slot_reserved = reserve_publish_slot(_tenant_id_pc, _plan_limit_pc)
            if not _slot_reserved:
                return {
                    "status": "quota_exceeded",
                    "reason": "daily_publish_limit_reached",
                    "tenant_id": _tenant_id_pc,
                    "task_id": self.request.id,
                }
        except Exception as _slot_err:
            log.warning("publish_content.slot_check_fail  err=%s  (proceeding)", _slot_err)

    # Idempotency check: prevent duplicate WP publish on retry (P1-B)
    _idem_key_pc = f"wp_publish:{_tenant_id_pc}:{self.request.id}"
    try:
        from core.idempotency import get_result as _idem_get
        _cached_pc = _idem_get(_idem_key_pc)
        if _cached_pc:
            log.info("publish_content.idempotent_replay  task_id=%s", self.request.id)
            return {**_cached_pc, "idempotent_replay": True}
    except Exception as _idem_err:
        log.debug("publish_content.idem_check_fail  err=%s", _idem_err)

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

        # ── Content quality gate (validate before publish) ────────────────────
        _gate_passed = True
        try:
            from execution.validators.content_gate import ContentGate
            import asyncio
            _gate = ContentGate(
                originality_api_key=os.getenv("ORIGINALITY_API_KEY", ""),
                ai_threshold=float(os.getenv("AI_SCORE_THRESHOLD", "0.45")),
            )
            _intent = page.get("intent", "informational")
            _gate_result = asyncio.get_event_loop().run_until_complete(
                _gate.check_and_humanise(
                    content_html,
                    keyword,
                    intent=_intent,
                    title=page.get("title", keyword),
                    meta_description=page.get("meta_description", ""),
                )
            )
            if _gate_result.humanised_html:
                content_html = _gate_result.humanised_html
                log.info("publish_content.humanised  keyword=%s  ai_score=%.2f", keyword, _gate_result.scores.get("ai_score", 0))
            if not _gate_result.passed:
                # Log blocking failures but only hard-block on word count (not AI score when key missing)
                hard_blocks = [f for f in _gate_result.blocking_failures if "ai_score" not in f or os.getenv("ORIGINALITY_API_KEY", "")]
                if hard_blocks:
                    log.warning("publish_content.gate_fail  keyword=%s  failures=%s", keyword, hard_blocks)
                    _gate_passed = False
            log.info("publish_content.gate  keyword=%s  passed=%s  wc=%d  ai=%.2f  warnings=%s",
                     keyword, _gate_result.passed, _gate_result.scores.get("word_count", 0),
                     _gate_result.scores.get("ai_score", 0.0), _gate_result.warnings[:2])
        except Exception as _ge:
            log.warning("publish_content.gate_skip  err=%s", _ge)

        # Fail-closed (Phase 0-C): hold for review instead of publishing broken content
        if not _gate_passed:
            log.error("publish_content.needs_review  keyword=%s  holding_draft", keyword)
            _review_result = {
                "status": "needs_review",
                "reason": "content_gate_failure",
                "keyword": keyword,
                "page": page,
                "task_id": self.request.id,
            }
            _save_result(self.request.id, _review_result)
            return _review_result

        # HTML sanitization before publish (S3-B: XSS prevention)
        try:
            from core.html_sanitizer import sanitize_html
            _orig_len = len(content_html)
            content_html = sanitize_html(content_html)
            if len(content_html) != _orig_len:
                log.info("publish_content.sanitized  keyword=%s  orig_len=%d  clean_len=%d",
                         keyword, _orig_len, len(content_html))
        except Exception as _san_err:
            log.warning("publish_content.sanitize_fail  err=%s  (proceeding_with_raw)", _san_err)

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
        # Mark idempotency key so retries skip the publish (P1-B)
        try:
            from core.idempotency import mark_seen as _idem_mark_ok
            _idem_mark_ok(_idem_key_pc, result, ttl=86400)
        except Exception:
            pass
        # Audit log: content published (P1-H)
        if _tenant_id_pc and result.get("status") == "success":
            try:
                from core.audit import log_event, A_CONTENT_PUBLISHED
                log_event(_tenant_id_pc, "system", A_CONTENT_PUBLISHED,
                          entity_type="content",
                          diff={"wp_url": wp_url, "keyword": result.get("keyword", ""),
                                "wp_post_id": str(wp_post_id), "task_id": self.request.id})
            except Exception as _ae:
                log.debug("publish_content.audit_fail  err=%s", _ae)
        log.info("publish_content.done  task_id=%s  url=%s  status=%s",
                 self.request.id, wp_url, result["status"])
        return result

    except Exception as exc:
        log.exception("publish_content.error  task_id=%s  exc=%s",
                      self.request.id, exc)
        # Release publish slot on failure so quota is not wasted (P1-C)
        if _slot_reserved and _tenant_id_pc:
            try:
                from core.publish_slots import release_publish_slot
                release_publish_slot(_tenant_id_pc)
            except Exception:
                pass
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
    # Schedule first indexing verification check in 6h (Phase 0-E)
    if wp_url:
        try:
            verify_indexing_status.apply_async(
                args=[wp_url, keyword, 0],
                countdown=_VERIFY_COUNTDOWNS[0],
            )
            log.info("indexnow_and_track.verification_scheduled  url=%s  in_6h", wp_url)
        except Exception as _ve:
            log.debug("indexnow_and_track.verify_schedule_fail  err=%s", _ve)

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

        # --- Content velocity control: max 2 articles/day ---
        import sqlite3, datetime
        today_str = datetime.datetime.utcnow().strftime('%Y-%m-%d')
        published_today = 0
        try:
            db_conn = sqlite3.connect('data/seo_engine.db')
            row = db_conn.execute(
                "SELECT COUNT(*) FROM published_urls WHERE published_at LIKE ?",
                (today_str + '%',)
            ).fetchone()
            published_today = row[0] if row else 0
            db_conn.close()
        except Exception:
            pass
        max_today = 2
        can_publish = max(0, max_today - published_today)
        if can_publish == 0:
            log.info('auto_content_briefs.velocity_limit  published_today=%d  max=%d', published_today, max_today)
            return {'status': 'velocity_limited', 'published_today': published_today, 'task_id': self.request.id}
        # Respect velocity: only generate for what can be published today
        candidates = candidates[:can_publish]

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
                        # Record to indexing_log (Medium priority fix)
                        try:
                            _biz_tid = biz.get("tenant_id", "")
                            if _biz_tid:
                                from core.pg import execute_write
                                execute_write(
                                    "INSERT INTO indexing_log (tenant_id, url, method, success, attempt) "
                                    "VALUES (%s, %s, %s, %s, %s)",
                                    [_biz_tid, sitemap_url, f"sitemap_ping_{engine}", True, 1],
                                    tenant_id=_biz_tid,
                                )
                        except Exception as _sle:
                            log.debug("submit_sitemap.log_fail  err=%s", _sle)
                except Exception as e:
                    log.warning("submit_sitemap.fail  engine=%s  sitemap=%s  err=%s", engine, sitemap_url, e)
                    # Record failure to indexing_log
                    try:
                        _biz_tid = biz.get("tenant_id", "")
                        if _biz_tid:
                            from core.pg import execute_write
                            execute_write(
                                "INSERT INTO indexing_log (tenant_id, url, method, success, attempt) "
                                "VALUES (%s, %s, %s, %s, %s)",
                                [_biz_tid, sitemap_url, f"sitemap_ping_{engine}", False, 1],
                                tenant_id=_biz_tid,
                            )
                    except Exception:
                        pass

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


# ===========================================================================
# WIKIDATA ENTITY SYNC TASK
# ===========================================================================

@app.task(bind=True, queue='analysis', max_retries=1, name='taskq.tasks.run_wikidata_sync')
def run_wikidata_sync(self) -> dict:
    """Create or verify Wikidata entities for each business.

    Attempts to create a minimal Wikidata entity (Q-item) for each business
    using the QuickStatements API.  If Wikidata credentials are not configured
    (most common case) it outputs a ready-to-submit QuickStatements CSV
    to data/storage/wikidata/ for manual submission.  Runs weekly.
    """
    import json, datetime
    from pathlib import Path
    log.info('run_wikidata_sync.start  task_id=%s', self.request.id)

    biz_file = Path('data/storage/businesses.json')
    if not biz_file.exists():
        return {'status': 'no_businesses', 'task_id': self.request.id}

    businesses = json.loads(biz_file.read_text())
    wd_dir = Path('data/storage/wikidata')
    wd_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for biz in businesses:
        name = biz.get('name', '')
        domain = biz.get('website', biz.get('domain', ''))
        city = biz.get('primary_city', biz.get('city', ''))
        province = biz.get('state', 'BC')
        service = biz.get('primary_service', '')
        if not name:
            continue

        # Build QuickStatements batch for manual submission
        # P31 = instance of (Q4830453 = business)
        # P18 = image (skip)
        # P856 = official website
        # P131 = located in administrative unit (Q2256158 = Kelowna)
        # P749 = parent organization (skip)
        city_qid_map = {
            'Kelowna': 'Q2256158', 'Vernon': 'Q234764', 'Penticton': 'Q1140340',
            'Salmon Arm': 'Q1058906', 'West Kelowna': 'Q2522718',
        }
        city_qid = city_qid_map.get(city, '')
        safe_name = name.replace('"', '')
        qs_lines = [
            'CREATE',
            f'LAST|Len|"{safe_name}"',
            f'LAST|Den|"company providing {service} services in {city}, {province}, Canada"',
            'LAST|P31|Q4830453',  # instance of: business
            'LAST|P17|Q16',       # country: Canada
        ]
        if city_qid:
            qs_lines.append(f'LAST|P131|{city_qid}')
        if domain:
            website = domain if domain.startswith('http') else f'https://{domain}'
            qs_lines.append(f'LAST|P856|"{website}"')

        qs_content = chr(10).join(qs_lines)
        safe_biz = name.lower().replace(' ', '_').replace('/', '_')[:30]
        qs_file = wd_dir / f'{safe_biz}_quickstatements.txt'
        qs_file.write_text(qs_content)

        # Check if a Wikidata QID is already stored
        existing_qid = biz.get('wikidata_qid', '')
        if existing_qid:
            log.info('run_wikidata_sync.has_qid  biz=%s  qid=%s', name, existing_qid)
            results.append({'business': name, 'status': 'existing_qid', 'qid': existing_qid})
        else:
            # Attempt live search via Wikidata API
            try:
                import urllib.request, urllib.parse
                search_url = (
                    'https://www.wikidata.org/w/api.php?action=wbsearchentities'
                    '&search=' + urllib.parse.quote(name) +
                    '&language=en&format=json&limit=3'
                )
                req = urllib.request.Request(search_url, headers={'User-Agent': 'SEOEngine/1.0 (contact@seoengine.ca)'})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read())
                matches = data.get('search', [])
                # Check if any match is plausibly our business
                found_qid = None
                for m in matches:
                    if name.lower() in m.get('label', '').lower():
                        found_qid = m.get('id', '')
                        break
                if found_qid:
                    log.info('run_wikidata_sync.found  biz=%s  qid=%s', name, found_qid)
                    results.append({'business': name, 'status': 'found', 'qid': found_qid, 'qs_file': str(qs_file)})
                else:
                    log.info('run_wikidata_sync.not_found  biz=%s  qs_file=%s', name, qs_file)
                    results.append({'business': name, 'status': 'needs_creation', 'qs_file': str(qs_file),
                                    'instructions': 'Submit qs_file content at https://quickstatements.toolforge.org'})
            except Exception as e:
                log.warning('run_wikidata_sync.api_fail  biz=%s  err=%s', name, e)
                results.append({'business': name, 'status': 'api_error', 'qs_file': str(qs_file)})

    log.info('run_wikidata_sync.done  task_id=%s  businesses=%d', self.request.id, len(results))
    return {'status': 'done', 'results': results, 'task_id': self.request.id}


# ===========================================================================
# STUB TASKS — Beat-scheduled; real implementations added iteratively
# ===========================================================================

@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.inject_content_freshness")
def inject_content_freshness(self) -> dict:
    """Weekly freshness pass: update stale articles with new stats, bump dateModified.
    
    Picks the 5 oldest articles, inserts a fresh stat or date reference,
    bumps `dateModified` in schema JSON-LD, re-submits to IndexNow.
    """
    log.info("inject_content_freshness.start  task_id=%s", self.request.id)
    try:
        import json, datetime
        from pathlib import Path

        db_path = Path("data/seo_engine.db")
        if not db_path.exists():
            return {"status": "skipped", "reason": "no db", "task_id": self.request.id}

        import sqlite3
        conn = sqlite3.connect(str(db_path))
        # Get 5 oldest published URLs
        rows = conn.execute(
            "SELECT url, business_id FROM published_urls ORDER BY published_at ASC LIMIT 5"
        ).fetchall()
        conn.close()

        if not rows:
            return {"status": "skipped", "reason": "no published urls", "task_id": self.request.id}

        updated = []
        for url, business_id in rows:
            try:
                # Queue a re-index submission for each stale URL
                submit_to_indexnow.apply_async(args=[url], queue="monitoring")
                updated.append(url)
            except Exception as e:
                log.warning("inject_content_freshness.url_fail  url=%s  err=%s", url, e)

        log.info("inject_content_freshness.done  task_id=%s  updated=%d", self.request.id, len(updated))
        return {"status": "success", "updated": updated, "task_id": self.request.id}

    except Exception as exc:
        log.exception("inject_content_freshness.error  task_id=%s", self.request.id)
        raise self.retry(exc=exc, countdown=300)


@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.syndicate_to_medium")
def syndicate_to_medium(self) -> dict:
    """Daily: syndicate articles >7 days old to Medium with canonical tag back to original.
    
    Requires MEDIUM_INTEGRATION_TOKEN in .env.
    """
    log.info("syndicate_to_medium.start  task_id=%s", self.request.id)
    try:
        import os, json, sqlite3
        from pathlib import Path

        token = os.getenv("MEDIUM_INTEGRATION_TOKEN", "")
        if not token:
            log.info("syndicate_to_medium.skip  reason=no_token")
            return {"status": "skipped", "reason": "MEDIUM_INTEGRATION_TOKEN not set", "task_id": self.request.id}

        db_path = Path("data/seo_engine.db")
        if not db_path.exists():
            return {"status": "skipped", "reason": "no db", "task_id": self.request.id}

        conn = sqlite3.connect(str(db_path))
        # Get articles published >7 days ago not yet syndicated
        rows = conn.execute(
            "SELECT url, business_id FROM published_urls "
            "WHERE published_at < datetime('now', '-7 days') "
            "AND url NOT IN (SELECT COALESCE(canonical_url,'') FROM syndications WHERE platform='medium') "
            "LIMIT 1"
        ).fetchall()
        conn.close()

        if not rows:
            return {"status": "skipped", "reason": "no articles ready for syndication", "task_id": self.request.id}

        log.info("syndicate_to_medium.done  task_id=%s  candidates=%d", self.request.id, len(rows))
        return {"status": "success", "candidates": len(rows), "task_id": self.request.id}

    except Exception as exc:
        log.exception("syndicate_to_medium.error  task_id=%s", self.request.id)
        raise self.retry(exc=exc, countdown=300)


@app.task(bind=True, queue="analysis", max_retries=1, name="taskq.tasks.pull_gsc_data")
def pull_gsc_data(self) -> dict:
    """Daily: pull Google Search Console clicks/impressions/CTR/position per URL+query.
    
    Requires GSC_OAUTH_CLIENT_ID, GSC_OAUTH_CLIENT_SECRET, GSC_REFRESH_TOKEN in .env.
    Writes to gsc_data table. Used by scan_content_decay for real signal.
    """
    log.info("pull_gsc_data.start  task_id=%s", self.request.id)
    import os
    client_id = os.getenv("GSC_OAUTH_CLIENT_ID", "")
    if not client_id:
        log.info("pull_gsc_data.skip  reason=no_gsc_credentials")
        return {"status": "skipped", "reason": "GSC credentials not configured", "task_id": self.request.id}
    # Full GSC API implementation will be added when credentials are set
    return {"status": "pending_credentials", "task_id": self.request.id}


@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.send_review_requests")
def send_review_requests(self) -> dict:
    """Daily: send post-job review request emails to recent customers.
    
    Requires CRM/job-completion webhook OR manual customer CSV.
    Sends via SMTP (SES) with 1-click links to Google + HomeStars review pages.
    """
    log.info("send_review_requests.start  task_id=%s", self.request.id)
    import os
    smtp_host = os.getenv("SMTP_HOST", "")
    if not smtp_host:
        log.info("send_review_requests.skip  reason=no_smtp")
        return {"status": "skipped", "reason": "SMTP not configured", "task_id": self.request.id}
    # Full implementation requires customer job-completion data source
    return {"status": "pending_crm_integration", "task_id": self.request.id}


@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.run_reddit_answer_agent")
def run_reddit_answer_agent(self) -> dict:
    """Weekly: find unanswered Reddit questions matching target keywords.
    
    Queues answers for human review (does NOT auto-post).
    Requires REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD.
    Searches: r/kelowna, r/britishcolumbia, r/HomeImprovement, r/ChristmasLights.
    """
    log.info("run_reddit_answer_agent.start  task_id=%s", self.request.id)
    import os, json
    from pathlib import Path

    reddit_id = os.getenv("REDDIT_CLIENT_ID", "")
    if not reddit_id:
        log.info("run_reddit_answer_agent.skip  reason=no_reddit_credentials")
        return {"status": "skipped", "reason": "Reddit credentials not configured", "task_id": self.request.id}

    # Full PRAW-based implementation added when credentials are set
    return {"status": "pending_credentials", "task_id": self.request.id}


# ---------------------------------------------------------------------------
# Task: verify_indexing_status  (Phase 0-E: correct backoff schedule)
# ---------------------------------------------------------------------------
# Verification schedule: +6h, +24h, +72h, +7d = 10.25 days total
# Old spec was 1d->2d->4d->8d->16d = 31 days -- too slow to detect issues
_VERIFY_COUNTDOWNS = [
    6 * 3600,          # attempt 0: check after  6 hours
    24 * 3600,         # attempt 1: check after 24 hours
    72 * 3600,         # attempt 2: check after 72 hours  (soft alert if not indexed)
    7 * 24 * 3600,     # attempt 3: check after  7 days   (hard alert + resubmit)
]


@app.task(bind=True, queue="monitoring", max_retries=4, name="taskq.tasks.verify_indexing_status")
def verify_indexing_status(self, url: str, business_id: str = "", attempt: int = 0) -> dict:
    """Verify that a published URL has been indexed by Google.

    Scheduling:
        Attempt 0 (+6h)   -- initial check
        Attempt 1 (+24h)  -- if not indexed yet
        Attempt 2 (+72h)  -- soft alert: "not indexed after 3 days"
        Attempt 3 (+7d)   -- hard alert + forced resubmission
        Attempt 4+        -- give up, log permanent_failure

    Args:
        url:         The published page URL to verify.
        business_id: Business context for cohort alerting.
        attempt:     Current verification attempt number (0-indexed).
    """
    log.info(
        "verify_indexing.start  task_id=%s  url=%s  attempt=%d",
        self.request.id, url, attempt,
    )
    try:
        from execution.indexing import IndexingSystem
        import os

        system = IndexingSystem(
            gsc_credentials_path=os.getenv("GSC_CREDENTIALS_PATH", ""),
        )
        is_indexed = _run_async(system.verify_indexed(url))

        if is_indexed:
            log.info("verify_indexing.ok  url=%s  attempt=%d", url, attempt)
            _save_result(self.request.id, {
                "status": "indexed",
                "url": url,
                "attempt": attempt,
                "task_id": self.request.id,
            })
            return {"status": "indexed", "url": url, "attempt": attempt}

        # Not yet indexed -- decide what to do based on attempt
        log.info("verify_indexing.not_yet  url=%s  attempt=%d", url, attempt)

        if attempt == 2:
            # Soft alert after 3 days
            log.warning(
                "verify_indexing.soft_alert  url=%s  not_indexed_after_3_days  business_id=%s",
                url, business_id,
            )
            try:
                _alert_webhook = os.getenv("ALERT_WEBHOOK_URL", "")
                if _alert_webhook:
                    import httpx
                    from core.ssrf import validate_url, SSRFError
                    try:
                        validate_url(_alert_webhook)
                        with httpx.Client(timeout=10) as hx:
                            hx.post(_alert_webhook, json={
                                "text": f":clock3: Page not indexed after 3 days\nURL: {url}\nBusiness: {business_id}\nAction: Monitoring, will check again in 7 days",
                            })
                    except SSRFError:
                        log.debug("verify_indexing.alert_ssrf_blocked  url=%s", _alert_webhook[:50])
            except Exception as _ae:
                log.debug("verify_indexing.alert_fail  err=%s", _ae)

        elif attempt == 3:
            # Hard alert + resubmit after 7 days
            log.error(
                "verify_indexing.hard_alert  url=%s  not_indexed_after_10_days  business_id=%s",
                url, business_id,
            )
            # Force resubmission
            try:
                from execution.indexing import submit_url
                resubmit_result = _run_async(submit_url(url))
                log.info(
                    "verify_indexing.resubmitted  url=%s  google_api=%s  bing=%s",
                    url, resubmit_result.google_api, resubmit_result.bing_indexnow,
                )
            except Exception as _re:
                log.warning("verify_indexing.resubmit_fail  url=%s  err=%s", url, _re)

            try:
                _alert_webhook = os.getenv("ALERT_WEBHOOK_URL", "")
                if _alert_webhook:
                    import httpx
                    from core.ssrf import validate_url, SSRFError
                    try:
                        validate_url(_alert_webhook)
                        with httpx.Client(timeout=10) as hx:
                            hx.post(_alert_webhook, json={
                                "text": f":rotating_light: Page NOT indexed after 10 days -- RESUBMITTED\nURL: {url}\nBusiness: {business_id}\nAction: URL resubmitted to Google + Bing IndexNow",
                            })
                    except SSRFError:
                        log.debug("verify_indexing.alert_ssrf_blocked  url=%s", _alert_webhook[:50])
            except Exception as _ae:
                log.debug("verify_indexing.alert_fail  err=%s", _ae)

        # Schedule the next verification check
        if attempt < len(_VERIFY_COUNTDOWNS):
            next_countdown = _VERIFY_COUNTDOWNS[attempt]
            verify_indexing_status.apply_async(
                args=[url, business_id, attempt + 1],
                countdown=next_countdown,
            )
            log.info(
                "verify_indexing.rescheduled  url=%s  next_attempt=%d  in_hours=%.1f",
                url, attempt + 1, next_countdown / 3600,
            )
        else:
            # All attempts exhausted
            log.error(
                "verify_indexing.permanent_failure  url=%s  business_id=%s  gave_up_after_4_attempts",
                url, business_id,
            )
            _save_result(self.request.id, {
                "status": "permanent_failure",
                "url": url,
                "attempts": attempt + 1,
                "task_id": self.request.id,
            })

        return {
            "status": "not_indexed_yet",
            "url": url,
            "attempt": attempt,
            "task_id": self.request.id,
        }

    except Exception as exc:
        log.exception("verify_indexing.error  task_id=%s  url=%s  exc=%s",
                      self.request.id, url, exc)
        if attempt < len(_VERIFY_COUNTDOWNS):
            raise self.retry(exc=exc, countdown=_VERIFY_COUNTDOWNS[attempt])
        return {"status": "error", "url": url, "error": str(exc), "task_id": self.request.id}


# Doc 06 tasks

@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_tech_audit")
def run_tech_audit(self, business_id: str = "") -> dict:
    log.info("run_tech_audit.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from data.connectors.tech_audit import run_audit, audit_to_dict
        import json, sqlite3
        from pathlib import Path
        all_biz = json.loads(Path("data/storage/businesses.json").read_text())
        biz = next((b for b in (all_biz if isinstance(all_biz, list) else all_biz.values())
                    if b.get("id") == business_id or b.get("business_id") == business_id or not business_id), {})
        domain = biz.get("domain", "").replace("https://", "").replace("http://", "").rstrip("/")
        urls = [r[0] for r in sqlite3.connect("data/storage/seo_engine.db").execute(
            "SELECT url FROM published_urls WHERE business_id=? AND status=? LIMIT 5", [business_id, "live"]).fetchall()]
        result = audit_to_dict(run_audit(domain, urls))
        _save_result(self.request.id, {"status": "success", **result, "task_id": self.request.id})
        log.info("run_tech_audit.done  score=%d  critical=%d", result["score"], result["critical_count"])
        return {"status": "success", **result, "task_id": self.request.id}
    except Exception as exc:
        log.exception("run_tech_audit.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_site_health_check")
def run_site_health_check(self, business_id: str = "") -> dict:
    log.info("run_site_health_check.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from monitoring.site_health import run_uptime_check, run_pagespeed_sample
        import sqlite3
        urls = [r[0] for r in sqlite3.connect("data/storage/seo_engine.db").execute(
            "SELECT url FROM published_urls WHERE business_id=? AND status=? LIMIT 50", [business_id, "live"]).fetchall()]
        uptime = run_uptime_check(business_id, urls)
        pagespeed = run_pagespeed_sample(business_id, urls)
        result = {"status": "success", "uptime": uptime, "pagespeed": pagespeed, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_site_health_check.done  ok=%d  failed=%d", uptime["ok"], uptime["failed"])
        return result
    except Exception as exc:
        log.exception("run_site_health_check.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_cannibalization_check")
def run_cannibalization_check(self, business_id: str = "") -> dict:
    log.info("run_cannibalization_check.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.cannibalization import detect_serp_cannibalization
        cases = detect_serp_cannibalization(business_id)
        result = {"status": "success", "cases": cases, "count": len(cases), "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_cannibalization_check.done  cases=%d", len(cases))
        return result
    except Exception as exc:
        log.exception("run_cannibalization_check.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.run_refresh_queue")
def run_refresh_queue(self, business_id: str = "") -> dict:
    log.info("run_refresh_queue.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.refresh_schedule import get_refresh_queue
        queue = get_refresh_queue(business_id, limit=5)
        stale = [q for q in queue if q["needs_refresh"]]
        result = {"status": "success", "stale_count": len(stale), "queue": stale[:10], "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_refresh_queue.done  stale=%d", len(stale))
        return result
    except Exception as exc:
        log.exception("run_refresh_queue.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_competitor_tracking")
def run_competitor_tracking(self, business_id: str = "") -> dict:
    log.info("run_competitor_tracking.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from data.connectors.competitor_tracker import run_competitor_tracking as _track
        import json
        from pathlib import Path
        all_biz = json.loads(Path("data/storage/businesses.json").read_text())
        biz = next((b for b in (all_biz if isinstance(all_biz, list) else all_biz.values())
                    if b.get("id") == business_id or b.get("business_id") == business_id or not business_id), {})
        competitors = biz.get("competitors", [])
        if not competitors:
            return {"status": "skipped", "reason": "no_competitors_configured", "task_id": self.request.id}
        results = _track(business_id, competitors)
        changes = sum(1 for r in results if r.get("has_changes"))
        result = {"status": "success", "competitors_checked": len(results), "with_changes": changes, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_competitor_tracking.done  checked=%d  changes=%d", len(results), changes)
        return result
    except Exception as exc:
        log.exception("run_competitor_tracking.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


# Doc 07 tasks

@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.run_llm_judge_sample")
def run_llm_judge_sample(self, business_id: str = "", content_html: str = "", keyword: str = "", intent: str = "informational") -> dict:
    log.info("run_llm_judge_sample.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.llm_judge import judge_content
        result = judge_content(content_html, keyword, intent, business_id, sample_rate=1.0)
        if result is None:
            return {"status": "skipped", "task_id": self.request.id}
        out = {"status": "success", "passed": result.passed, "overall": result.overall, "scores": result.scores, "task_id": self.request.id}
        _save_result(self.request.id, out)
        log.info("run_llm_judge_sample.done  passed=%s  overall=%.1f", result.passed, result.overall)
        return out
    except Exception as exc:
        log.exception("run_llm_judge_sample.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_hypothesis_generation")
def run_hypothesis_generation(self, business_id: str = "") -> dict:
    log.info("run_hypothesis_generation.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.hypothesis_engine import generate_hypotheses, promote_winning_hypotheses
        hypotheses = generate_hypotheses(business_id)
        promoted = promote_winning_hypotheses()
        result = {"status": "success", "hypotheses_generated": len(hypotheses), "promoted": promoted, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_hypothesis_generation.done  generated=%d  promoted=%d", len(hypotheses), promoted)
        return result
    except Exception as exc:
        log.exception("run_hypothesis_generation.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_tenant_strategy_update")
def run_tenant_strategy_update(self, business_id: str = "") -> dict:
    log.info("run_tenant_strategy_update.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.tenant_strategy import update_strategy
        import json
        from pathlib import Path
        all_biz = json.loads(Path("data/storage/businesses.json").read_text())
        biz_ids = [b.get("id") or b.get("business_id") for b in (all_biz if isinstance(all_biz, list) else all_biz.values()) if b.get("id") or b.get("business_id")]
        if business_id:
            biz_ids = [business_id]
        updated = 0
        for bid in biz_ids:
            try:
                update_strategy(bid)
                updated += 1
            except Exception:
                pass
        result = {"status": "success", "tenants_updated": updated, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_tenant_strategy_update.done  updated=%d", updated)
        return result
    except Exception as exc:
        log.exception("run_tenant_strategy_update.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_competitor_exploit")
def run_competitor_exploit(self, business_id: str = "") -> dict:
    log.info("run_competitor_exploit.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.competitor_exploit import find_exploit_opportunities
        opps = find_exploit_opportunities(business_id)
        result = {"status": "success", "opportunities_found": len(opps), "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_competitor_exploit.done  opportunities=%d", len(opps))
        return result
    except Exception as exc:
        log.exception("run_competitor_exploit.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_threshold_tuning")
def run_threshold_tuning(self) -> dict:
    log.info("run_threshold_tuning.start  task_id=%s", self.request.id)
    try:
        from core.threshold_tuner import tune_thresholds
        tuned = tune_thresholds()
        result = {"status": "success", "thresholds_adjusted": tuned, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_threshold_tuning.done  adjusted=%d", tuned)
        return result
    except Exception as exc:
        log.exception("run_threshold_tuning.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


# Doc 08 tasks

@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_pagespeed_sample")
def run_pagespeed_sample(self, business_id: str = "") -> dict:
    log.info("run_pagespeed_sample.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from data.connectors.pagespeed import PageSpeedClient
        client = PageSpeedClient()
        results = client.sample_tenant_pages(business_id, sample_n=5)
        failed = [r for r in results if not r.get("psi", {}).get("passed", True)]
        result = {"status": "success", "sampled": len(results), "failed_cwv": len(failed), "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_pagespeed_sample.done  sampled=%d  failed=%d", len(results), len(failed))
        return result
    except Exception as exc:
        log.exception("run_pagespeed_sample.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="execution", max_retries=2, name="taskq.tasks.push_lead_to_crm")
def push_lead_to_crm(self, business_id: str = "", lead: dict = None) -> dict:
    log.info("push_lead_to_crm.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from data.connectors.crm_ghl import GHLConnector
        connector = GHLConnector()
        ok = connector.push_lead(lead or {}, business_id)
        result = {"status": "success" if ok else "failed", "task_id": self.request.id}
        _save_result(self.request.id, result)
        return result
    except Exception as exc:
        log.exception("push_lead_to_crm.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_schema_validation_sweep")
def run_schema_validation_sweep(self, business_id: str = "") -> dict:
    log.info("run_schema_validation_sweep.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.schema_validator import validate_schema
        import sqlite3, requests
        conn = sqlite3.connect("data/storage/seo_engine.db")
        urls = [r[0] for r in conn.execute("SELECT url FROM published_urls WHERE business_id=? AND status='live' LIMIT 20", [business_id]).fetchall()]
        conn.close()
        errors_total = 0
        for url in urls:
            try:
                html = requests.get(url, timeout=10).text
                res = validate_schema(html, url)
                errors_total += len(res.get("errors", []))
            except Exception:
                pass
        result = {"status": "success", "urls_checked": len(urls), "total_schema_errors": errors_total, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_schema_validation_sweep.done  urls=%d  errors=%d", len(urls), errors_total)
        return result
    except Exception as exc:
        log.exception("run_schema_validation_sweep.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


# Doc 09 tasks

@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_health_score_sweep")
def run_health_score_sweep(self) -> dict:
    log.info("run_health_score_sweep.start  task_id=%s", self.request.id)
    try:
        from core.health_score import batch_compute_health_scores
        count = batch_compute_health_scores()
        result = {"status": "success", "tenants_scored": count, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_health_score_sweep.done  count=%d", count)
        return result
    except Exception as exc:
        log.exception("run_health_score_sweep.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_expansion_sweep")
def run_expansion_sweep(self) -> dict:
    log.info("run_expansion_sweep.start  task_id=%s", self.request.id)
    try:
        from core.expansion import run_expansion_sweep as _sweep
        result = _sweep()
        result["task_id"] = self.request.id
        _save_result(self.request.id, result)
        log.info("run_expansion_sweep.done  tenants=%d", result.get("tenants_checked", 0))
        return result
    except Exception as exc:
        log.exception("run_expansion_sweep.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_case_study_scan")
def run_case_study_scan(self) -> dict:
    log.info("run_case_study_scan.start  task_id=%s", self.request.id)
    try:
        from core.case_study import scan_for_eligible_tenants
        drafted = scan_for_eligible_tenants()
        result = {"status": "success", "case_studies_drafted": drafted, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_case_study_scan.done  drafted=%d", drafted)
        return result
    except Exception as exc:
        log.exception("run_case_study_scan.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


# Doc 10 tasks

@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_outcome_snapshot_sweep")
def run_outcome_snapshot_sweep(self) -> dict:
    log.info("run_outcome_snapshot_sweep.start  task_id=%s", self.request.id)
    try:
        from core.content_provenance import get_corpus_stats
        import sqlite3
        stats = get_corpus_stats()
        result = {"status": "success", "corpus_stats": stats, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_outcome_snapshot_sweep.done  total=%d  with_90d=%d", stats.get("total_pages", 0), stats.get("pages_with_90d_outcome", 0))
        return result
    except Exception as exc:
        log.exception("run_outcome_snapshot_sweep.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_signal_layer_sweep")
def run_signal_layer_sweep(self) -> dict:
    log.info("run_signal_layer_sweep.start  task_id=%s", self.request.id)
    try:
        from core.signal_layer import get_signal_stats
        stats = get_signal_stats()
        result = {"status": "success", "signal_stats": stats, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_signal_layer_sweep.done  signals=%d  cohorts=%d", stats.get("total_signals", 0), stats.get("unique_cohorts", 0))
        return result
    except Exception as exc:
        log.exception("run_signal_layer_sweep.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_ai_version_evaluation")
def run_ai_version_evaluation(self) -> dict:
    log.info("run_ai_version_evaluation.start  task_id=%s", self.request.id)
    try:
        from core.ai_version_registry import COMPONENTS, get_active_version, evaluate_version
        results = []
        for component in COMPONENTS:
            active = get_active_version(component)
            if active:
                eval_result = evaluate_version(active["id"])
                results.append(eval_result)
        result = {"status": "success", "components_evaluated": len(results), "results": results, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_ai_version_evaluation.done  evaluated=%d", len(results))
        return result
    except Exception as exc:
        log.exception("run_ai_version_evaluation.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


# GEO/AEO tasks

@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_ai_answer_monitor")
def run_ai_answer_monitor(self, business_id: str = "") -> dict:
    log.info("run_ai_answer_monitor.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.ai_answer_monitor import run_keyword_monitor
        import json
        from pathlib import Path
        if business_id:
            result = run_keyword_monitor(business_id)
        else:
            all_biz = json.loads(Path("data/storage/businesses.json").read_text())
            biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
            total_gaps = 0
            total_wins = 0
            for biz in biz_list:
                bid = biz.get("id") or biz.get("business_id")
                if bid:
                    r = run_keyword_monitor(bid, max_keywords=10)
                    total_gaps += r.get("citation_gaps", 0)
                    total_wins += r.get("you_cited", 0)
            result = {"status": "success", "total_gaps": total_gaps, "total_wins": total_wins}
        result["task_id"] = self.request.id
        _save_result(self.request.id, result)
        log.info("run_ai_answer_monitor.done  gaps=%s  wins=%s", result.get("citation_gaps", result.get("total_gaps")), result.get("you_cited", result.get("total_wins")))
        return result
    except Exception as exc:
        log.exception("run_ai_answer_monitor.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.run_geo_optimization_sweep")
def run_geo_optimization_sweep(self, business_id: str = "") -> dict:
    log.info("run_geo_optimization_sweep.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.geo_optimizer import score_geo_readiness
        import sqlite3, requests as req_lib
        conn = sqlite3.connect("data/storage/seo_engine.db")
        urls = [(r[0], r[1], r[2]) for r in conn.execute(
            "SELECT url, keyword, business_id FROM published_urls WHERE business_id=? AND status='live' LIMIT 20",
            [business_id]).fetchall()]
        conn.close()
        scores = []
        failing = 0
        for url, keyword, bid in urls:
            try:
                html = req_lib.get(url, timeout=10).text
                score = score_geo_readiness(html)
                scores.append({"url": url, "keyword": keyword, "score": score["score"], "passing": score["passing"]})
                if not score["passing"]:
                    failing += 1
            except Exception:
                pass
        avg_score = round(sum(s["score"] for s in scores) / max(len(scores), 1), 1)
        result = {"status": "success", "pages_checked": len(scores), "avg_geo_score": avg_score, "failing_geo": failing, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_geo_optimization_sweep.done  pages=%d  avg_score=%.1f  failing=%d", len(scores), avg_score, failing)
        return result
    except Exception as exc:
        log.exception("run_geo_optimization_sweep.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.run_llms_txt_deploy")
def run_llms_txt_deploy(self, business_id: str = "") -> dict:
    log.info("run_llms_txt_deploy.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.llms_txt_builder import deploy_llms_txt, build_llms_txt
        from core.geo_prompts import register_geo_prompts
        register_geo_prompts()
        if business_id:
            content = build_llms_txt(business_id)
            ok = deploy_llms_txt(business_id, output_path=f"public/{business_id}_llms.txt")
            result = {"status": "success" if ok else "partial", "business_id": business_id, "content_length": len(content), "task_id": self.request.id}
        else:
            from core.llms_txt_builder import generate_platform_llms_txt
            content = generate_platform_llms_txt()
            with open("public/llms.txt", "w") as f:
                f.write(content)
            result = {"status": "success", "type": "platform", "content_length": len(content), "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_llms_txt_deploy.done  length=%d", result.get("content_length", 0))
        return result
    except Exception as exc:
        log.exception("run_llms_txt_deploy.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue='monitoring', max_retries=1, name='taskq.tasks.run_eeat_sweep')
def run_eeat_sweep(self, business_id: str = '') -> dict:
    log.info('run_eeat_sweep.start  task_id=%s  biz=%s', self.request.id, business_id)
    try:
        import sqlite3
        from core.eeat_pipeline import score_eeat
        conn = sqlite3.connect('data/storage/seo_engine.db')
        urls = [r[0] for r in conn.execute(
            'SELECT url FROM published_urls WHERE business_id=? AND status=? LIMIT 50',
            [business_id, 'live']).fetchall()]
        conn.close()
        low_scores = []
        for url in urls:
            try:
                import urllib.request
                html = urllib.request.urlopen(url, timeout=8).read().decode('utf-8', errors='ignore')
                s = score_eeat(html)
                if not s['passing']:
                    low_scores.append({'url': url, 'score': s['total'], 'missing': s['missing']})
            except Exception:
                pass
        result = {'status': 'success', 'checked': len(urls), 'below_threshold': len(low_scores),
                  'low_score_urls': low_scores[:20], 'task_id': self.request.id}
        _save_result(self.request.id, result)
        log.info('run_eeat_sweep.done  checked=%d  low=%d', len(urls), len(low_scores))
        return result
    except Exception as exc:
        log.exception('run_eeat_sweep.error  task_id=%s', self.request.id)
        return {'status': 'error', 'error': str(exc), 'task_id': self.request.id}


@app.task(bind=True, queue='monitoring', max_retries=1, name='taskq.tasks.run_backlink_prospecting')
def run_backlink_prospecting(self, business_id: str = '') -> dict:
    log.info('run_backlink_prospecting.start  task_id=%s  biz=%s', self.request.id, business_id)
    try:
        from core.backlink_prospector import run_prospect_sweep
        result = run_prospect_sweep(business_id)
        _save_result(self.request.id, {'status': 'success', **result, 'task_id': self.request.id})
        log.info('run_backlink_prospecting.done  total=%d', result.get('total_found', 0))
        return {'status': 'success', **result, 'task_id': self.request.id}
    except Exception as exc:
        log.exception('run_backlink_prospecting.error  task_id=%s', self.request.id)
        return {'status': 'error', 'error': str(exc), 'task_id': self.request.id}


@app.task(bind=True, queue='execution', max_retries=1, name='taskq.tasks.run_backlink_health_check')
def run_backlink_health_check(self, business_id: str = '') -> dict:
    log.info('run_backlink_health_check.start  task_id=%s  biz=%s', self.request.id, business_id)
    try:
        from core.backlink_prospector import check_backlink_health
        result = check_backlink_health(business_id)
        _save_result(self.request.id, {'status': 'success', **result, 'task_id': self.request.id})
        log.info('run_backlink_health_check.done  live=%d  dead=%d', result['live'], result['dead'])
        return {'status': 'success', **result, 'task_id': self.request.id}
    except Exception as exc:
        log.exception('run_backlink_health_check.error  task_id=%s', self.request.id)
        return {'status': 'error', 'error': str(exc), 'task_id': self.request.id}


@app.task(bind=True, queue='monitoring', max_retries=1, name='taskq.tasks.run_entity_sweep')
def run_entity_sweep(self, business_id: str = '') -> dict:
    log.info('run_entity_sweep.start  task_id=%s  biz=%s', self.request.id, business_id)
    try:
        from core.brand_entity import run_entity_sweep as _sweep
        result = _sweep(business_id)
        _save_result(self.request.id, {'status': 'success', **result, 'task_id': self.request.id})
        log.info('run_entity_sweep.done  score=%d  mentions=%d', result.get('entity_score', 0), result.get('mentions_found', 0))
        return {'status': 'success', **result, 'task_id': self.request.id}
    except Exception as exc:
        log.exception('run_entity_sweep.error  task_id=%s', self.request.id)
        return {'status': 'error', 'error': str(exc), 'task_id': self.request.id}


@app.task(bind=True, queue='monitoring', max_retries=1, name='taskq.tasks.run_serp_rank_sweep')
def run_serp_rank_sweep(self, business_id: str = '') -> dict:
    log.info('run_serp_rank_sweep.start  task_id=%s  biz=%s', self.request.id, business_id)
    try:
        import json
        from pathlib import Path
        all_biz = json.loads(Path('data/storage/businesses.json').read_text())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
        biz = next((b for b in biz_list if b.get('id') == business_id or b.get('business_id') == business_id), {})
        domain = biz.get('domain', '').replace('https://', '').replace('http://', '').rstrip('/')
        location = biz.get('city', '')
        import sqlite3
        keywords = [r[0] for r in sqlite3.connect('data/storage/seo_engine.db').execute(
            'SELECT DISTINCT keyword FROM ranking_history WHERE business_id=? LIMIT 30', [business_id]).fetchall()]
        if not keywords:
            keywords = biz.get('target_keywords', [])[:20]
        from core.serp_scraper import run_rank_tracking_sweep
        result = run_rank_tracking_sweep(business_id, keywords, domain, location)
        _save_result(self.request.id, {'status': 'success', **result, 'task_id': self.request.id})
        log.info('run_serp_rank_sweep.done  keywords=%d  top3=%d', result.get('keywords_checked', 0), result.get('top_3', 0))
        return {'status': 'success', **result, 'task_id': self.request.id}
    except Exception as exc:
        log.exception('run_serp_rank_sweep.error  task_id=%s', self.request.id)
        return {'status': 'error', 'error': str(exc), 'task_id': self.request.id}


@app.task(bind=True, queue='monitoring', max_retries=1, name='taskq.tasks.run_competitor_crawl')
def run_competitor_crawl(self, business_id: str = '') -> dict:
    log.info('run_competitor_crawl.start  task_id=%s  biz=%s', self.request.id, business_id)
    try:
        from core.backlink_crawler import crawl_competitor_suite
        result = crawl_competitor_suite(business_id)
        _save_result(self.request.id, {'status': 'success', **result, 'task_id': self.request.id})
        log.info('run_competitor_crawl.done  comps=%d  gaps=%d', result.get('competitors_crawled', 0), result.get('backlink_gaps_found', 0))
        return {'status': 'success', **result, 'task_id': self.request.id}
    except Exception as exc:
        log.exception('run_competitor_crawl.error  task_id=%s', self.request.id)
        return {'status': 'error', 'error': str(exc), 'task_id': self.request.id}


@app.task(bind=True, queue='monitoring', max_retries=1, name='taskq.tasks.run_keyword_opportunity_sweep')
def run_keyword_opportunity_sweep(self, business_id: str = '') -> dict:
    log.info('run_keyword_opportunity_sweep.start  task_id=%s  biz=%s', self.request.id, business_id)
    try:
        import json
        from pathlib import Path
        all_biz = json.loads(Path('data/storage/businesses.json').read_text())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
        biz = next((b for b in biz_list if b.get('id') == business_id or b.get('business_id') == business_id), {})
        niche = biz.get('niche', biz.get('service_type', 'home services'))
        location = biz.get('city', '')
        from core.keyword_intel import get_keyword_opportunities
        opportunities = get_keyword_opportunities(business_id, niche, location, limit=20)
        result = {'status': 'success', 'opportunities_found': len(opportunities),
                  'top_opportunities': opportunities[:5], 'task_id': self.request.id}
        _save_result(self.request.id, result)
        log.info('run_keyword_opportunity_sweep.done  found=%d', len(opportunities))
        return result
    except Exception as exc:
        log.exception('run_keyword_opportunity_sweep.error  task_id=%s', self.request.id)
        return {'status': 'error', 'error': str(exc), 'task_id': self.request.id}
