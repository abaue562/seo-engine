"""Event-Driven System — reactive triggers instead of loop-only execution.

Events:
  ranking_drop → trigger recovery plan
  competitor_move → trigger counter-action
  ctr_low → trigger CTR test
  goal_reached → trigger expansion
  stagnation → trigger rapid update
"""

from __future__ import annotations

import logging
from datetime import datetime
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)


class SystemEvent(BaseModel):
    type: str
    keyword: str = ""
    detail: str = ""
    severity: str = "medium"   # critical / high / medium / low
    data: dict = {}
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# Event handler registry
_handlers: dict[str, list] = {}


def on_event(event_type: str, handler):
    """Register a handler for an event type."""
    if event_type not in _handlers:
        _handlers[event_type] = []
    _handlers[event_type].append(handler)
    log.debug("event.registered  type=%s  handler=%s", event_type, handler.__name__)


def emit(event: SystemEvent):
    """Emit an event — all registered handlers will be called."""
    handlers = _handlers.get(event.type, [])
    log.info("event.emit  type=%s  keyword=%s  handlers=%d", event.type, event.keyword, len(handlers))
    for handler in handlers:
        try:
            handler(event)
        except Exception as e:
            log.error("event.handler_fail  type=%s  handler=%s  err=%s", event.type, handler.__name__, e)


def detect_events_from_state(old_rankings: dict[str, int], new_rankings: dict[str, int]) -> list[SystemEvent]:
    """Compare rankings and emit appropriate events."""
    events = []

    for kw, new_pos in new_rankings.items():
        old_pos = old_rankings.get(kw, 0)
        if old_pos == 0:
            continue

        diff = old_pos - new_pos  # positive = improved

        if diff <= -3:
            events.append(SystemEvent(
                type="ranking_drop",
                keyword=kw,
                detail=f"Dropped {abs(diff)} positions (#{old_pos} → #{new_pos})",
                severity="critical" if abs(diff) >= 5 else "high",
                data={"from": old_pos, "to": new_pos, "diff": diff},
            ))
        elif new_pos <= 3 and old_pos > 3:
            events.append(SystemEvent(
                type="goal_reached",
                keyword=kw,
                detail=f"Reached top 3! (#{old_pos} → #{new_pos})",
                severity="high",
                data={"from": old_pos, "to": new_pos},
            ))
        elif 5 <= new_pos <= 15 and diff == 0:
            events.append(SystemEvent(
                type="stagnation",
                keyword=kw,
                detail=f"Stuck at #{new_pos} — needs push",
                severity="medium",
                data={"position": new_pos},
            ))

    return events
