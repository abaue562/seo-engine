"""Tenant audit log -- append-only event recording.

Every workflow step that touches tenant data should call log_event().
The tenant_audit_log table (Phase 0-F schema) stores all events.

Usage:
    from core.audit import log_event

    log_event(
        tenant_id="abc-123",
        actor="system",
        action="content.published",
        entity_type="content",
        entity_id="page-uuid",
        diff={"wp_url": "https://...", "keyword": "plumber toronto"},
    )
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

log = logging.getLogger(__name__)

# Actors
ACTOR_SYSTEM = "system"
ACTOR_TENANT = "tenant_user"
ACTOR_ADMIN = "platform_admin"

# Action constants
A_KEYWORD_ADDED = "keyword.added"
A_BRIEF_CREATED = "brief.created"
A_CONTENT_GENERATED = "content.generated"
A_CONTENT_PUBLISHED = "content.published"
A_CONTENT_UPDATED = "content.updated"
A_CONTENT_ROLLED_BACK = "content.rolled_back"
A_CONTENT_NEEDS_REVIEW = "content.needs_review"
A_CREDENTIAL_STORED = "credential.stored"
A_CREDENTIAL_ACCESSED = "credential.accessed"
A_CREDENTIAL_DELETED = "credential.deleted"
A_PLAN_CHANGED = "plan.changed"
A_PUBLISHING_PAUSED = "publishing.paused"
A_PUBLISHING_RESUMED = "publishing.resumed"
A_OUTREACH_SENT = "outreach.sent"
A_BACKLINK_ACQUIRED = "backlink.acquired"
A_RANK_TRACKED = "rank.tracked"
A_SITEMAP_PINGED = "sitemap.pinged"
A_INDEXING_SUBMITTED = "indexing.submitted"
A_ONBOARDING_STEP = "onboarding.step"
A_SCHEMA_DRIFT = "schema.drift_detected"
A_QUOTA_EXCEEDED = "quota.exceeded"


def log_event(
    tenant_id: str,
    actor: str,
    action: str,
    entity_type: str = "",
    entity_id: str | None = None,
    diff: Any = None,
    trace_id: str | None = None,
) -> None:
    """Append an event to the tenant audit log.

    Fails silently -- audit logging must never break the main flow.

    Args:
        tenant_id:   Tenant UUID string.
        actor:       "system", "tenant_user", or "platform_admin".
        action:      Event name (e.g. "content.published").
        entity_type: What kind of entity this action affected.
        entity_id:   UUID of the affected entity (optional).
        diff:        JSON-serialisable payload with before/after data (optional).
        trace_id:    Trace ID for cross-service correlation (optional).
    """
    try:
        from core.pg import execute_write
        if trace_id is None:
            try:
                from core.tracing import get_trace_id
                trace_id = get_trace_id()
            except Exception:
                trace_id = None

        diff_json = json.dumps(diff, default=str) if diff is not None else None
        execute_write(
            "INSERT INTO tenant_audit_log "
            "(tenant_id, actor, action, entity_type, entity_id, diff, trace_id) "
            "VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)",
            [tenant_id, actor, action, entity_type or "", entity_id, diff_json, trace_id],
            tenant_id=tenant_id,
        )
        log.debug(
            "audit.logged  tenant=%s  actor=%s  action=%s",
            tenant_id[:8] if tenant_id else "", actor, action,
        )
    except Exception as e:
        log.warning(
            "audit.log_fail  tenant=%s  action=%s  err=%s",
            tenant_id[:8] if tenant_id else "", action, e,
        )


def log_batch(events: list[dict]) -> None:
    """Log multiple audit events in one DB round-trip.

    Each dict should have keys: tenant_id, actor, action, and optionally
    entity_type, entity_id, diff, trace_id.
    """
    if not events:
        return
    try:
        from core.pg import admin_write
        from core.tracing import get_trace_id
        default_trace = None
        try:
            from core.tracing import get_trace_id
            default_trace = get_trace_id()
        except Exception:
            pass

        for ev in events:
            diff = ev.get("diff")
            diff_json = json.dumps(diff, default=str) if diff is not None else None
            # Use execute_write per event (simple; batching via COPY would be overkill here)
            from core.pg import execute_write
            execute_write(
                "INSERT INTO tenant_audit_log "
                "(tenant_id, actor, action, entity_type, entity_id, diff, trace_id) "
                "VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)",
                [
                    ev["tenant_id"],
                    ev.get("actor", ACTOR_SYSTEM),
                    ev["action"],
                    ev.get("entity_type", ""),
                    ev.get("entity_id"),
                    diff_json,
                    ev.get("trace_id", default_trace),
                ],
                tenant_id=ev["tenant_id"],
            )
    except Exception as e:
        log.warning("audit.batch_fail  count=%d  err=%s", len(events), e)
