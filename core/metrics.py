"""Worker-level observability metrics.

Exports Prometheus-compatible metrics for:
  - Per-queue depth, throughput, failure rate
  - Per-task p50/p95/p99 duration, retry count, failure reason
  - Per-tenant tasks-in-flight, circuit-breaker status
  - Per-external-service latency + error rate

Works in two modes:
  1. Full Prometheus: if prometheus_client is installed and
     PROMETHEUS_PORT is set (default 9100), starts an HTTP metrics server.
  2. Structured-log mode: emits metric events as JSON log lines for
     Loki/Grafana without any extra infra.

Usage:
    from core.metrics import task_started, task_finished, task_failed
    from core.metrics import external_call, record_queue_depth

    # In Celery signals (auto-wired via setup_celery_metrics):
    task_started("publish_content", tenant_id="abc")
    task_finished("publish_content", duration_ms=1420, tenant_id="abc")
    task_failed("publish_content", reason="ConnectionError", tenant_id="abc")
"""
from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Any

log = logging.getLogger(__name__)

_PROM_AVAILABLE = False
_prom = None

try:
    import prometheus_client as _pc
    _PROM_AVAILABLE = True
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Prometheus metric objects (created lazily on first use)
# ---------------------------------------------------------------------------
_TASK_DURATION = None
_TASK_TOTAL = None
_TASK_FAILURES = None
_EXTERNAL_DURATION = None
_EXTERNAL_FAILURES = None
_QUEUE_DEPTH = None


def _init_prometheus():
    global _TASK_DURATION, _TASK_TOTAL, _TASK_FAILURES
    global _EXTERNAL_DURATION, _EXTERNAL_FAILURES, _QUEUE_DEPTH, _prom
    if not _PROM_AVAILABLE or _prom == "init_done":
        return
    try:
        import prometheus_client as pc
        _TASK_DURATION = pc.Histogram(
            "seo_task_duration_seconds",
            "Celery task execution time",
            ["task_name", "queue"],
            buckets=[0.1, 0.5, 1, 5, 10, 30, 60, 120, 300],
        )
        _TASK_TOTAL = pc.Counter(
            "seo_task_total",
            "Total tasks executed",
            ["task_name", "queue", "status"],
        )
        _TASK_FAILURES = pc.Counter(
            "seo_task_failures_total",
            "Task failure count by reason",
            ["task_name", "reason"],
        )
        _EXTERNAL_DURATION = pc.Histogram(
            "seo_external_call_duration_seconds",
            "External HTTP call latency",
            ["service", "operation"],
            buckets=[0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10],
        )
        _EXTERNAL_FAILURES = pc.Counter(
            "seo_external_failures_total",
            "External call failures",
            ["service", "operation", "error_type"],
        )
        _QUEUE_DEPTH = pc.Gauge(
            "seo_queue_depth",
            "Current Celery queue depth",
            ["queue"],
        )
        # Start HTTP server if PROMETHEUS_PORT is set
        port = int(os.getenv("PROMETHEUS_PORT", "0"))
        if port:
            pc.start_http_server(port)
            log.info("metrics.prometheus_server_started  port=%d", port)
        _prom = "init_done"
    except Exception as e:
        log.warning("metrics.prometheus_init_fail  err=%s", e)
        _prom = "init_failed"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def task_started(task_name: str, queue: str = "", tenant_id: str = "") -> float:
    """Record task start. Returns monotonic start time for duration calc."""
    _init_prometheus()
    t0 = time.monotonic()
    log.debug(
        "metric.task_start  task=%s  queue=%s  tenant=%s",
        task_name, queue, tenant_id[:8] if tenant_id else "",
    )
    return t0


def task_finished(
    task_name: str,
    t0: float,
    queue: str = "",
    tenant_id: str = "",
    status: str = "success",
) -> None:
    """Record task completion with duration."""
    duration = time.monotonic() - t0
    duration_ms = int(duration * 1000)
    if _TASK_DURATION:
        try:
            _TASK_DURATION.labels(task_name=task_name, queue=queue).observe(duration)
        except Exception:
            pass
    if _TASK_TOTAL:
        try:
            _TASK_TOTAL.labels(task_name=task_name, queue=queue, status=status).inc()
        except Exception:
            pass
    log.info(
        "metric.task_done  task=%s  queue=%s  status=%s  duration_ms=%d  tenant=%s",
        task_name, queue, status, duration_ms, tenant_id[:8] if tenant_id else "",
    )


def task_failed(
    task_name: str,
    t0: float | None = None,
    queue: str = "",
    reason: str = "unknown",
    tenant_id: str = "",
) -> None:
    """Record task failure."""
    if _TASK_FAILURES:
        try:
            _TASK_FAILURES.labels(task_name=task_name, reason=reason[:40]).inc()
        except Exception:
            pass
    if t0 is not None and _TASK_TOTAL:
        try:
            _TASK_TOTAL.labels(task_name=task_name, queue=queue, status="failed").inc()
        except Exception:
            pass
    log.warning(
        "metric.task_fail  task=%s  queue=%s  reason=%s  tenant=%s",
        task_name, queue, reason, tenant_id[:8] if tenant_id else "",
    )


@contextmanager
def external_call(service: str, operation: str):
    """Context manager to time + record external HTTP calls.

    Usage:
        with external_call("wordpress", "create_post"):
            result = wp_client.create_post(...)
    """
    _init_prometheus()
    t0 = time.monotonic()
    try:
        yield
        duration = time.monotonic() - t0
        if _EXTERNAL_DURATION:
            try:
                _EXTERNAL_DURATION.labels(service=service, operation=operation).observe(duration)
            except Exception:
                pass
        log.debug(
            "metric.ext_call_ok  service=%s  op=%s  duration_ms=%d",
            service, operation, int(duration * 1000),
        )
    except Exception as exc:
        duration = time.monotonic() - t0
        error_type = type(exc).__name__
        if _EXTERNAL_FAILURES:
            try:
                _EXTERNAL_FAILURES.labels(
                    service=service, operation=operation, error_type=error_type
                ).inc()
            except Exception:
                pass
        log.warning(
            "metric.ext_call_fail  service=%s  op=%s  duration_ms=%d  err=%s",
            service, operation, int(duration * 1000), error_type,
        )
        raise


def record_queue_depth(queue: str, depth: int) -> None:
    """Update the gauge for a queue depth (call from monitoring task)."""
    _init_prometheus()
    if _QUEUE_DEPTH:
        try:
            _QUEUE_DEPTH.labels(queue=queue).set(depth)
        except Exception:
            pass
    log.info("metric.queue_depth  queue=%s  depth=%d", queue, depth)


def snapshot_queue_depths() -> dict[str, int]:
    """Probe Redis for current Celery queue depths and update gauges.

    Returns dict of {queue_name: depth}.
    """
    depths = {}
    try:
        import redis
        r = redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            socket_timeout=2,
        )
        for q in ["analysis", "execution", "learning", "monitoring", "dead_letter"]:
            depth = r.llen(q)
            depths[q] = depth
            record_queue_depth(q, depth)
    except Exception as e:
        log.warning("metrics.queue_depth_fail  err=%s", e)
    return depths


# ---------------------------------------------------------------------------
# Celery signal auto-wiring
# ---------------------------------------------------------------------------

def setup_celery_metrics(celery_app) -> None:
    """Wire task metrics into a Celery app via signals.

    Call once after creating the Celery app:
        from core.metrics import setup_celery_metrics
        setup_celery_metrics(app)
    """
    _init_prometheus()
    from celery import signals

    _task_start_times: dict[str, float] = {}

    @signals.task_prerun.connect
    def _prerun(task_id, task, **kw):
        _task_start_times[task_id] = task_started(
            task_name=task.name,
            queue=getattr(task, "queue", ""),
        )

    @signals.task_success.connect
    def _success(sender, result, **kw):
        task_id = sender.request.id
        t0 = _task_start_times.pop(task_id, None)
        if t0:
            task_finished(
                task_name=sender.name,
                t0=t0,
                queue=getattr(sender, "queue", ""),
                status="success",
            )

    @signals.task_failure.connect
    def _failure(sender, task_id, exception, **kw):
        t0 = _task_start_times.pop(task_id, None)
        task_failed(
            task_name=sender.name,
            t0=t0,
            queue=getattr(sender, "queue", ""),
            reason=type(exception).__name__,
        )

    @signals.task_retry.connect
    def _retry(sender, request, reason, **kw):
        log.info(
            "metric.task_retry  task=%s  task_id=%s  reason=%s",
            sender.name, request.id, str(reason)[:80],
        )
