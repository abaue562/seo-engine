"""Centralised alert / notification system.

Sends structured alerts to ALERT_WEBHOOK_URL (Slack / Discord / any
incoming-webhook compatible endpoint) for every critical system event.

Usage
-----
    from execution.notify import notify, AlertLevel

    notify(AlertLevel.ERROR, "Task failed", task_id="abc", business_id="xyz")
    notify(AlertLevel.INFO,  "Daily summary", passed=42, failed=0)

The webhook payload uses Slack's Block Kit format which Discord also accepts
when the endpoint is a Discord webhook (the text field is the fallback).

Environment variables
---------------------
    ALERT_WEBHOOK_URL   — Slack / Discord incoming webhook URL (optional).
                          If empty, alerts are only logged; nothing is posted.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import httpx

log = logging.getLogger(__name__)

_WEBHOOK_URL = lambda: os.getenv("ALERT_WEBHOOK_URL", "")


class AlertLevel(str, Enum):
    DEBUG   = "debug"
    INFO    = "info"
    WARNING = "warning"
    ERROR   = "error"
    CRITICAL = "critical"


# Emoji prefix per level for Slack readability
_LEVEL_EMOJI = {
    AlertLevel.DEBUG:    ":white_circle:",
    AlertLevel.INFO:     ":large_blue_circle:",
    AlertLevel.WARNING:  ":large_yellow_circle:",
    AlertLevel.ERROR:    ":red_circle:",
    AlertLevel.CRITICAL: ":rotating_light:",
}

_LEVEL_COLOR = {
    AlertLevel.DEBUG:    "#aaaaaa",
    AlertLevel.INFO:     "#0ea5e9",
    AlertLevel.WARNING:  "#f59e0b",
    AlertLevel.ERROR:    "#ef4444",
    AlertLevel.CRITICAL: "#7c3aed",
}


def notify(
    level: AlertLevel,
    message: str,
    *,
    task_id: str | None = None,
    business_id: str | None = None,
    task_name: str | None = None,
    **extra: Any,
) -> bool:
    """Send an alert to the configured webhook URL.

    Args:
        level:       Severity level.
        message:     Short human-readable description of the event.
        task_id:     Celery task ID if relevant.
        business_id: Business identifier if relevant.
        task_name:   Celery task name if relevant.
        **extra:     Any additional key-value pairs to include in the payload.

    Returns:
        True if the webhook call succeeded (or no URL configured), False on error.
    """
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    emoji = _LEVEL_EMOJI.get(level, "")
    color = _LEVEL_COLOR.get(level, "#aaaaaa")

    # Always log locally
    log_fn = {
        AlertLevel.DEBUG:    log.debug,
        AlertLevel.INFO:     log.info,
        AlertLevel.WARNING:  log.warning,
        AlertLevel.ERROR:    log.error,
        AlertLevel.CRITICAL: log.critical,
    }.get(level, log.info)
    log_fn("alert.%s  msg=%s  task_id=%s  business=%s  extra=%s",
           level.value, message, task_id, business_id, extra)

    url = _WEBHOOK_URL()
    if not url:
        return True  # silently skip if not configured

    # Build Slack-compatible Block Kit payload (also renders on Discord)
    fields: list[dict] = []
    if task_id:
        fields.append({"type": "mrkdwn", "text": f"*Task ID*\n`{task_id}`"})
    if business_id:
        fields.append({"type": "mrkdwn", "text": f"*Business*\n`{business_id}`"})
    if task_name:
        fields.append({"type": "mrkdwn", "text": f"*Task*\n`{task_name}`"})
    for k, v in extra.items():
        fields.append({"type": "mrkdwn", "text": f"*{k.replace('_', ' ').title()}*\n`{v}`"})

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{emoji} *{level.value.upper()}* — {message}\n_{ts}_",
            },
        }
    ]
    if fields:
        blocks.append({"type": "section", "fields": fields[:10]})

    payload = {
        "text": f"{emoji} {level.value.upper()}: {message}",
        "attachments": [
            {
                "color": color,
                "blocks": blocks,
            }
        ],
    }

    try:
        resp = httpx.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error("notify.webhook_fail  url=%s  err=%s", url[:40], e)
        return False


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------

def notify_task_failure(
    task_id: str,
    task_name: str,
    exception: Exception,
    business_id: str | None = None,
    *,
    retries: int = 0,
    max_retries: int = 3,
) -> None:
    """Fire when a Celery task hits its dead-letter (exhausted retries)."""
    notify(
        AlertLevel.ERROR,
        f"Task exhausted retries and moved to dead-letter: `{task_name}`",
        task_id=task_id,
        business_id=business_id,
        task_name=task_name,
        exception=str(exception)[:200],
        retries=f"{retries}/{max_retries}",
    )


def notify_ranking_drop(
    business_id: str,
    keyword: str,
    old_position: int,
    new_position: int,
) -> None:
    """Fire when a primary keyword drops 3+ positions."""
    drop = new_position - old_position
    notify(
        AlertLevel.WARNING,
        f"Ranking drop detected: `{keyword}` fell {drop} positions (#{old_position} → #{new_position})",
        business_id=business_id,
        keyword=keyword,
        old_position=str(old_position),
        new_position=str(new_position),
        drop=str(drop),
    )


def notify_indexing_failure(
    url: str,
    business_id: str | None = None,
    days_since_publish: int = 7,
) -> None:
    """Fire when a published page is not yet indexed after N days."""
    notify(
        AlertLevel.WARNING,
        f"Page not indexed after {days_since_publish}d: {url}",
        business_id=business_id,
        url=url,
        days=str(days_since_publish),
    )


def notify_daily_summary(
    business_id: str,
    executions_today: int,
    tasks_succeeded: int,
    tasks_failed: int,
    dead_letter_count: int,
) -> None:
    """Fire once per day with execution statistics."""
    level = AlertLevel.ERROR if dead_letter_count > 0 else AlertLevel.INFO
    notify(
        level,
        f"Daily summary — {tasks_succeeded} succeeded, {tasks_failed} failed, {dead_letter_count} dead-letter",
        business_id=business_id,
        executions_today=str(executions_today),
        succeeded=str(tasks_succeeded),
        failed=str(tasks_failed),
        dead_letter=str(dead_letter_count),
    )


def notify_content_published(
    url: str,
    keyword: str,
    business_id: str | None = None,
) -> None:
    """Fire when a new page is successfully published and indexed."""
    notify(
        AlertLevel.INFO,
        f"Content published: `{keyword}`",
        business_id=business_id,
        keyword=keyword,
        url=url,
    )


def notify_citation_detected(
    business_name: str,
    source: str,
    citation_url: str,
    business_id: str | None = None,
) -> None:
    """Fire when an AI engine (Perplexity, ChatGPT, Gemini) cites the business."""
    notify(
        AlertLevel.INFO,
        f"AI citation detected for `{business_name}` on {source}",
        business_id=business_id,
        source=source,
        citation_url=citation_url[:100],
    )
