"""Distributed trace context for the SEO Engine.

Provides trace_id / span_id propagation across Celery tasks and HTTP calls.
Works in two modes:
  1. Full OTEL: if opentelemetry-api + opentelemetry-sdk are installed and
     OTEL_EXPORTER_OTLP_ENDPOINT is set, exports spans to that endpoint.
  2. Structured-log mode (default): emits JSON trace events to the logger
     so Loki/Grafana can index them. Zero extra infra required.

Usage:
    from core.tracing import get_trace_ctx, new_trace, span, inject_celery_headers, extract_celery_headers

    # Start a new trace (at API entrypoint or scheduler dispatch)
    ctx = new_trace(tenant_id="abc-123", operation="publish_content")

    # Open a span (context manager)
    with span("wp_publish", url=wp_url, tenant_id=tenant_id):
        result = wp_client.create_post(...)

    # Propagate into Celery task kwargs
    headers = inject_celery_headers()
    task.apply_async(kwargs={...}, headers=headers)

    # Restore context inside a Celery task
    extract_celery_headers(self.request.headers or {})
"""
from __future__ import annotations

import logging
import os
import secrets
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trace context dataclass
# ---------------------------------------------------------------------------
@dataclass
class TraceContext:
    trace_id: str
    span_id: str
    tenant_id: str = ""
    operation: str = ""
    parent_span_id: str = ""
    baggage: dict = field(default_factory=dict)


_ctx_var: ContextVar[TraceContext | None] = ContextVar("trace_ctx", default=None)

_OTEL_AVAILABLE = False
try:
    from opentelemetry import trace as _otel_trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    _OTEL_AVAILABLE = True
except ImportError:
    pass

_tracer = None


def _init_otel() -> Any | None:
    """Initialise OpenTelemetry exporter if configured. Called once lazily."""
    global _tracer
    if _tracer is not None:
        return _tracer
    if not _OTEL_AVAILABLE:
        return None
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    if not endpoint:
        return None
    try:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry import trace as otel_trace
        from opentelemetry.sdk.resources import Resource

        resource = Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME", "seo-engine")})
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        otel_trace.set_tracer_provider(provider)
        _tracer = otel_trace.get_tracer("seo-engine")
        log.info("tracing.otel_init_ok  endpoint=%s", endpoint)
        return _tracer
    except Exception as e:
        log.warning("tracing.otel_init_fail  err=%s  (falling back to structured logs)", e)
        return None


def _new_id(length: int = 16) -> str:
    """Generate a random hex ID."""
    return secrets.token_hex(length // 2)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def new_trace(tenant_id: str = "", operation: str = "") -> TraceContext:
    """Start a new root trace. Sets the current context and returns it."""
    ctx = TraceContext(
        trace_id=_new_id(32),
        span_id=_new_id(16),
        tenant_id=tenant_id,
        operation=operation,
    )
    _ctx_var.set(ctx)
    log.debug(
        "trace.new  trace_id=%s  tenant=%s  op=%s",
        ctx.trace_id, tenant_id[:8] if tenant_id else "", operation,
    )
    return ctx


def get_trace_ctx() -> TraceContext | None:
    """Return the current trace context, or None if not set."""
    return _ctx_var.get(None)


def get_trace_id() -> str:
    """Return the current trace_id, or a zero string if no context."""
    ctx = _ctx_var.get(None)
    return ctx.trace_id if ctx else "0" * 32


def get_tenant_id() -> str:
    """Return the current tenant_id from trace context."""
    ctx = _ctx_var.get(None)
    return ctx.tenant_id if ctx else ""


def inject_celery_headers(ctx: TraceContext | None = None) -> dict:
    """Return a dict of headers to pass to Celery task.apply_async(headers=...)."""
    ctx = ctx or _ctx_var.get(None)
    if ctx is None:
        return {}
    return {
        "x-trace-id": ctx.trace_id,
        "x-span-id": ctx.span_id,
        "x-tenant-id": ctx.tenant_id,
        "x-operation": ctx.operation,
    }


def extract_celery_headers(headers: dict) -> TraceContext | None:
    """Restore trace context from Celery task headers. Call at task start."""
    trace_id = headers.get("x-trace-id", "")
    if not trace_id:
        return None
    ctx = TraceContext(
        trace_id=trace_id,
        span_id=_new_id(16),       # new child span
        parent_span_id=headers.get("x-span-id", ""),
        tenant_id=headers.get("x-tenant-id", ""),
        operation=headers.get("x-operation", ""),
    )
    _ctx_var.set(ctx)
    return ctx


@contextmanager
def span(name: str, **attrs):
    """Open a trace span. Logs start/end + duration. Uses OTEL if available.

    Usage:
        with span("wp_publish", url=url, tenant_id=tenant_id):
            ...
    """
    ctx = _ctx_var.get(None)
    span_id = _new_id(16)
    trace_id = ctx.trace_id if ctx else _new_id(32)
    tenant_id = attrs.pop("tenant_id", ctx.tenant_id if ctx else "")

    t0 = time.monotonic()
    otel_tracer = _init_otel()
    otel_span_ctx = None

    if otel_tracer:
        try:
            otel_span_ctx = otel_tracer.start_span(name, attributes={
                "tenant.id": tenant_id,
                **{k: str(v) for k, v in attrs.items()},
            })
            otel_span_ctx.__enter__()
        except Exception:
            otel_span_ctx = None

    log.debug("span.start  trace=%s  span=%s  name=%s  tenant=%s  attrs=%s",
              trace_id, span_id, name, tenant_id[:8] if tenant_id else "", attrs)
    try:
        yield span_id
        duration_ms = int((time.monotonic() - t0) * 1000)
        log.info(
            "span.ok  trace=%s  span=%s  name=%s  tenant=%s  duration_ms=%d",
            trace_id, span_id, name, tenant_id[:8] if tenant_id else "", duration_ms,
        )
    except Exception as exc:
        duration_ms = int((time.monotonic() - t0) * 1000)
        log.error(
            "span.err  trace=%s  span=%s  name=%s  tenant=%s  duration_ms=%d  err=%s",
            trace_id, span_id, name, tenant_id[:8] if tenant_id else "", duration_ms, exc,
        )
        if otel_span_ctx:
            try:
                from opentelemetry.trace import StatusCode
                otel_span_ctx.set_status(StatusCode.ERROR, str(exc))
            except Exception:
                pass
        raise
    finally:
        if otel_span_ctx:
            try:
                otel_span_ctx.__exit__(None, None, None)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Celery signal hooks (auto-propagation)
# ---------------------------------------------------------------------------

def setup_celery_tracing(celery_app) -> None:
    """Wire trace propagation into a Celery app via signals.

    Call once after creating the Celery app:
        from core.tracing import setup_celery_tracing
        setup_celery_tracing(app)
    """
    from celery import signals

    @signals.task_prerun.connect
    def _task_prerun(task_id, task, args, kwargs, **kw):
        headers = getattr(task.request, "headers", None) or {}
        ctx = extract_celery_headers(headers)
        if ctx is None:
            # No upstream trace -- start a fresh one
            ctx = new_trace(operation=task.name)
        log.debug(
            "celery.task_start  task=%s  task_id=%s  trace=%s  tenant=%s",
            task.name, task_id, ctx.trace_id, ctx.tenant_id[:8] if ctx.tenant_id else "",
        )

    @signals.task_postrun.connect
    def _task_postrun(task_id, task, retval, state, **kw):
        ctx = _ctx_var.get(None)
        trace_id = ctx.trace_id if ctx else ""
        log.debug(
            "celery.task_done  task=%s  task_id=%s  state=%s  trace=%s",
            task.name, task_id, state, trace_id,
        )
