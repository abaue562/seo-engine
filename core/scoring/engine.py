"""Task Scoring Engine v3 — tiered classification with minimum task guarantee.

Scoring formula (1-10 scale):
  total = (impact × 0.35) + (ease × 0.15) + (speed × 0.30) + (confidence × 0.20)

Tasks are classified into tiers:
  PRIMARY:      impact >= 8 AND confidence >= 7 — main ranking push
  SUPPORTING:   impact >= 6 AND confidence >= 5 — helps primary succeed
  EXPERIMENTAL: everything else that isn't garbage

Garbage (killed): impact < 4 OR confidence < 3

Output guarantee: always return 3-5 tasks (2 primary + 2 supporting + 1 experimental)
"""

from __future__ import annotations

import re
from models.task import SEOTask, ImpactLevel, TaskRole

# --- Weights (must sum to 1.0) — speed bias for fast wins ---
WEIGHT_IMPACT = 0.35
WEIGHT_EASE = 0.15
WEIGHT_SPEED = 0.30
WEIGHT_CONFIDENCE = 0.20

# --- Garbage threshold (only kill truly bad tasks) ---
GARBAGE_IMPACT = 4
GARBAGE_CONFIDENCE = 3

# --- Tier thresholds ---
PRIMARY_IMPACT = 8
PRIMARY_CONFIDENCE = 7
SUPPORTING_IMPACT = 6
SUPPORTING_CONFIDENCE = 5

# --- Output limits ---
MIN_TASKS = 3
MAX_TASKS = 5


# =====================================================================
# Server-side score validation
# =====================================================================

_TIME_PATTERN = re.compile(r"(\d+)\s*(?:-\s*\d+\s*)?(day|week|month|hour)", re.IGNORECASE)
_TIME_TO_DAYS = {"hour": 0.04, "day": 1, "week": 7, "month": 30}


def _extract_days(time_str: str) -> float:
    m = _TIME_PATTERN.search(time_str)
    if not m:
        return 30.0
    return int(m.group(1)) * _TIME_TO_DAYS.get(m.group(2).lower(), 30.0)


def _clamp(val: float, lo: float = 1.0, hi: float = 10.0) -> float:
    return max(lo, min(hi, val))


def _validate_impact(task: SEOTask) -> float:
    if task.impact_score != 0:
        return _clamp(task.impact_score)
    base = {ImpactLevel.HIGH: 9, ImpactLevel.MEDIUM: 7, ImpactLevel.LOW: 4}
    score = base.get(task.impact, 6)
    result_lower = task.estimated_result.lower()
    for kw in ("conversion", "revenue", "leads", "calls", "booking"):
        if kw in result_lower:
            score = min(10, score + 1)
            break
    return float(score)


def _validate_ease(task: SEOTask) -> float:
    if task.ease_score != 0:
        return _clamp(task.ease_score)
    score = 6.0
    exec_lower = task.execution.lower()
    for kw in ("developer", "redesign", "migration", "custom code", "rewrite", "overhaul"):
        if kw in exec_lower:
            score -= 2
    for kw in ("update", "add", "edit", "publish", "post", "reply", "meta", "title tag"):
        if kw in exec_lower:
            score += 1
    return _clamp(score)


def _validate_speed(task: SEOTask) -> float:
    if task.speed_score != 0:
        return _clamp(task.speed_score)
    days = _extract_days(task.time_to_result)
    if days <= 7:
        return 9.0
    elif days <= 28:
        return 7.0
    elif days <= 90:
        return 4.0
    return 2.0


def _validate_confidence(task: SEOTask) -> float:
    if task.confidence_score != 0:
        return _clamp(task.confidence_score)
    return 6.0


# =====================================================================
# Scoring + Classification
# =====================================================================

def compute_total(impact: float, ease: float, speed: float, confidence: float) -> float:
    return round(
        WEIGHT_IMPACT * impact
        + WEIGHT_EASE * ease
        + WEIGHT_SPEED * speed
        + WEIGHT_CONFIDENCE * confidence,
        2,
    )


def score_task(task: SEOTask) -> SEOTask:
    """Validate scores, compute total, assign role."""
    task.impact_score = _validate_impact(task)
    task.ease_score = _validate_ease(task)
    task.speed_score = _validate_speed(task)
    task.confidence_score = _validate_confidence(task)
    task.total_score = compute_total(
        task.impact_score, task.ease_score, task.speed_score, task.confidence_score,
    )

    # Classify into tier
    if task.impact_score >= PRIMARY_IMPACT and task.confidence_score >= PRIMARY_CONFIDENCE:
        task.role = TaskRole.PRIMARY
    elif task.impact_score >= SUPPORTING_IMPACT and task.confidence_score >= SUPPORTING_CONFIDENCE:
        task.role = TaskRole.SUPPORTING
    else:
        task.role = TaskRole.EXPERIMENTAL

    return task


def filter_garbage(tasks: list[SEOTask]) -> tuple[list[SEOTask], int]:
    """Only kill truly garbage tasks (impact < 4 or confidence < 3)."""
    kept = []
    for t in tasks:
        if t.impact_score >= GARBAGE_IMPACT and t.confidence_score >= GARBAGE_CONFIDENCE:
            kept.append(t)
    return kept, len(tasks) - len(kept)


def rank_tasks(tasks: list[SEOTask]) -> list[SEOTask]:
    """Sort by total_score desc and assign priority_rank."""
    tasks.sort(key=lambda t: t.total_score, reverse=True)
    for i, t in enumerate(tasks):
        t.priority_rank = i + 1
    return tasks


def select_tiered(tasks: list[SEOTask]) -> list[SEOTask]:
    """Select 3-5 tasks across tiers: primary + supporting + experimental."""
    primary = [t for t in tasks if t.role == TaskRole.PRIMARY]
    supporting = [t for t in tasks if t.role == TaskRole.SUPPORTING]
    experimental = [t for t in tasks if t.role == TaskRole.EXPERIMENTAL]

    # Build final list: up to 2 primary, up to 2 supporting, up to 1 experimental
    final: list[SEOTask] = []
    final.extend(primary[:2])
    final.extend(supporting[:2])
    final.extend(experimental[:1])

    # If we have fewer than MIN_TASKS, pull more from available pools
    if len(final) < MIN_TASKS:
        remaining = [t for t in tasks if t not in final]
        remaining.sort(key=lambda t: t.total_score, reverse=True)
        for t in remaining:
            if len(final) >= MIN_TASKS:
                break
            final.append(t)

    return final[:MAX_TASKS]


def score_and_rank(tasks: list[SEOTask], apply_filters: bool = True) -> tuple[list[SEOTask], int]:
    """Full pipeline: score → filter garbage → classify tiers → select → rank."""
    scored = [score_task(t) for t in tasks]

    filtered_count = 0
    if apply_filters:
        scored, filtered_count = filter_garbage(scored)

    # Tier selection (guarantees 3-5 tasks)
    selected = select_tiered(scored)

    # Final ranking
    ranked = rank_tasks(selected)

    return ranked, filtered_count


# Backward compat
def score_tasks(tasks: list[SEOTask]) -> list[SEOTask]:
    ranked, _ = score_and_rank(tasks)
    return ranked
