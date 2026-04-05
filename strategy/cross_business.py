"""Cross-Business Learning Engine — learns from ALL clients and applies globally.

When you have multiple businesses:
  - Patterns that work for Business A get applied to Business B
  - Winning action types get boosted across all accounts
  - Failed strategies get killed system-wide
  - Industry-specific learnings emerge from aggregate data

This is the ultimate scaling advantage — every new client makes all others better.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pydantic import BaseModel, Field

from data.storage.database import Database
from learning.patterns import PatternMemory, ActionPattern

log = logging.getLogger(__name__)


class IndustryInsight(BaseModel):
    """Learned pattern specific to an industry/niche."""
    industry: str              # plumbing / lighting / dental / etc.
    pattern: str               # What works
    success_rate: float = 0.0
    avg_performance: float = 0.0
    sample_size: int = 0
    applicable_to: list[str] = Field(default_factory=list)  # Which other industries this applies to


class CrossBusinessReport(BaseModel):
    """Aggregate learnings across all businesses."""
    total_businesses: int = 0
    total_patterns: int = 0
    universal_winners: list[dict] = Field(default_factory=list)    # Work everywhere
    universal_losers: list[dict] = Field(default_factory=list)     # Fail everywhere
    industry_insights: list[IndustryInsight] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=datetime.utcnow)


class CrossBusinessLearner:
    """Aggregates learnings across all managed businesses."""

    def __init__(self, db: Database | None = None):
        self.db = db or Database()

    async def aggregate_patterns(self, business_ids: list[str]) -> CrossBusinessReport:
        """Analyze patterns across all businesses and find universal insights."""
        report = CrossBusinessReport(total_businesses=len(business_ids))

        all_patterns: dict[str, list[ActionPattern]] = {}

        for biz_id in business_ids:
            patterns = PatternMemory(self.db)
            biz_patterns = await patterns.get_all_patterns()
            for p in biz_patterns:
                key = p.pattern
                if key not in all_patterns:
                    all_patterns[key] = []
                all_patterns[key].append(p)

        report.total_patterns = len(all_patterns)

        # Find universal winners (work across 60%+ of businesses)
        for pattern_name, instances in all_patterns.items():
            if len(instances) < 2:
                continue

            avg_success = sum(p.success_rate for p in instances) / len(instances)
            avg_perf = sum(p.avg_performance for p in instances) / len(instances)
            total_uses = sum(p.times_used for p in instances)

            if avg_success >= 0.6:
                report.universal_winners.append({
                    "pattern": pattern_name,
                    "avg_success_rate": round(avg_success, 2),
                    "avg_performance": round(avg_perf, 2),
                    "businesses_tested": len(instances),
                    "total_uses": total_uses,
                })
            elif avg_success < 0.4 and total_uses >= 5:
                report.universal_losers.append({
                    "pattern": pattern_name,
                    "avg_success_rate": round(avg_success, 2),
                    "businesses_tested": len(instances),
                    "total_uses": total_uses,
                })

        # Sort by performance
        report.universal_winners.sort(key=lambda x: x["avg_performance"], reverse=True)
        report.universal_losers.sort(key=lambda x: x["avg_success_rate"])

        # Generate recommendations
        if report.universal_winners:
            top = report.universal_winners[0]
            report.recommendations.append(
                f"Boost '{top['pattern']}' across all businesses — "
                f"{top['avg_success_rate']:.0%} success rate across {top['businesses_tested']} businesses"
            )
        if report.universal_losers:
            worst = report.universal_losers[0]
            report.recommendations.append(
                f"Kill '{worst['pattern']}' system-wide — "
                f"only {worst['avg_success_rate']:.0%} success rate"
            )

        log.info("cross_business.aggregate  businesses=%d  patterns=%d  winners=%d  losers=%d",
                 len(business_ids), len(all_patterns),
                 len(report.universal_winners), len(report.universal_losers))

        return report

    async def apply_cross_learnings(
        self,
        source_business_id: str,
        target_business_id: str,
    ) -> list[str]:
        """Apply winning patterns from one business to another."""
        source_patterns = PatternMemory(self.db)
        all_source = await source_patterns.get_all_patterns()

        applied = []
        for pattern in all_source:
            if pattern.success_rate >= 0.7 and pattern.times_used >= 3:
                applied.append(
                    f"Apply '{pattern.pattern}' (success: {pattern.success_rate:.0%}, "
                    f"avg perf: {pattern.avg_performance:.1f}) from {source_business_id}"
                )

        log.info("cross_business.apply  from=%s  to=%s  applicable=%d",
                 source_business_id, target_business_id, len(applied))
        return applied

    def to_prompt_block(self, report: CrossBusinessReport) -> str:
        """Render cross-business insights as agent context."""
        lines = [
            f"CROSS-BUSINESS INTELLIGENCE ({report.total_businesses} businesses analyzed):",
        ]

        if report.universal_winners:
            lines.append("\n  PROVEN WINNERS (boost these for all businesses):")
            for w in report.universal_winners[:5]:
                lines.append(f"    {w['pattern']}: {w['avg_success_rate']:.0%} success, "
                             f"perf={w['avg_performance']:.1f} ({w['businesses_tested']} biz, {w['total_uses']} uses)")

        if report.universal_losers:
            lines.append("\n  UNIVERSAL FAILURES (avoid for all businesses):")
            for l in report.universal_losers[:3]:
                lines.append(f"    {l['pattern']}: {l['avg_success_rate']:.0%} success — KILL")

        for rec in report.recommendations:
            lines.append(f"\n  REC: {rec}")

        return "\n".join(lines)
