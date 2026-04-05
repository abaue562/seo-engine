"""Data Freshness Layer — enforces the rule: stale data = lower confidence.

Every dataset gets a freshness tag. If data is stale:
  - confidence_score gets penalized
  - Strategy agent is warned
  - High-impact decisions are blocked on stale sources
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum
from pydantic import BaseModel

from config.settings import FRESHNESS_RANKINGS, FRESHNESS_REVIEWS, FRESHNESS_TRAFFIC


class FreshnessLevel(str, Enum):
    FRESH = "fresh"      # Within threshold
    AGING = "aging"      # 1-2x threshold
    STALE = "stale"      # Beyond 2x threshold
    MISSING = "missing"  # No data at all


class DataSource(BaseModel):
    """Metadata for any ingested dataset."""
    source: str                          # GSC, GBP, CRAWL, COMPETITOR, KEYWORD
    fetched_at: datetime
    record_count: int = 0
    freshness: FreshnessLevel = FreshnessLevel.MISSING
    confidence: str = "low"              # high / medium / low

    def evaluate_freshness(self, threshold_days: int) -> None:
        """Set freshness and confidence based on age vs threshold."""
        age = (datetime.utcnow() - self.fetched_at).days

        if age <= threshold_days:
            self.freshness = FreshnessLevel.FRESH
            self.confidence = "high"
        elif age <= threshold_days * 2:
            self.freshness = FreshnessLevel.AGING
            self.confidence = "medium"
        else:
            self.freshness = FreshnessLevel.STALE
            self.confidence = "low"


class DataFreshnessReport(BaseModel):
    """Aggregated freshness state for all sources — injected into agents."""
    sources: dict[str, DataSource] = {}

    def add(self, source: DataSource) -> None:
        self.sources[source.source] = source

    def overall_confidence(self) -> str:
        """Lowest confidence across all sources."""
        levels = {"high": 3, "medium": 2, "low": 1}
        if not self.sources:
            return "low"
        worst = min(levels.get(s.confidence, 1) for s in self.sources.values())
        return {3: "high", 2: "medium", 1: "low"}[worst]

    def confidence_penalty(self) -> int:
        """Points to subtract from confidence_score when data is stale (0-4)."""
        c = self.overall_confidence()
        return {"high": 0, "medium": 1, "low": 3}.get(c, 3)

    def to_prompt_block(self) -> str:
        """Render as context block for agents."""
        if not self.sources:
            return "DATA FRESHNESS: No data loaded. All confidence scores should be LOW."

        lines = ["DATA FRESHNESS:"]
        for name, src in self.sources.items():
            age = (datetime.utcnow() - src.fetched_at).days
            lines.append(f"  {name}: {src.freshness.value} ({age}d old, {src.record_count} records, confidence={src.confidence})")

        overall = self.overall_confidence()
        penalty = self.confidence_penalty()
        lines.append(f"\nOVERALL DATA CONFIDENCE: {overall}")
        if penalty > 0:
            lines.append(f"WARNING: Reduce confidence_score by {penalty} for all tasks due to stale data.")
            lines.append("Avoid high-impact decisions when data confidence is low.")

        return "\n".join(lines)


# --- Convenience constructors ---

def evaluate_gsc(fetched_at: datetime, record_count: int) -> DataSource:
    src = DataSource(source="GSC", fetched_at=fetched_at, record_count=record_count)
    src.evaluate_freshness(FRESHNESS_RANKINGS)
    return src


def evaluate_gbp(fetched_at: datetime, record_count: int) -> DataSource:
    src = DataSource(source="GBP", fetched_at=fetched_at, record_count=record_count)
    src.evaluate_freshness(FRESHNESS_REVIEWS)
    return src


def evaluate_traffic(fetched_at: datetime, record_count: int) -> DataSource:
    src = DataSource(source="TRAFFIC", fetched_at=fetched_at, record_count=record_count)
    src.evaluate_freshness(FRESHNESS_TRAFFIC)
    return src


def evaluate_competitors(fetched_at: datetime, record_count: int) -> DataSource:
    src = DataSource(source="COMPETITOR", fetched_at=fetched_at, record_count=record_count)
    src.evaluate_freshness(FRESHNESS_RANKINGS)
    return src
