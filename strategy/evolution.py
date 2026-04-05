"""Self-Evolving Strategy Engine — the system that improves its own strategy.

Instead of fixed rules, this engine:
  1. Tracks what actually moved rankings
  2. Identifies which signal types had the most impact
  3. Automatically adjusts weights and priorities
  4. Doubles down on what works, kills what doesn't

This is the loop:
  Execute → Measure → Learn → Adjust weights → Execute better
"""

from __future__ import annotations

import logging
from datetime import datetime
from pydantic import BaseModel, Field

from learning.patterns import PatternMemory, ActionPattern

log = logging.getLogger(__name__)


class StrategyWeights(BaseModel):
    """Dynamic weights that evolve based on what actually works."""
    content_optimization: float = 1.0   # Title/meta/content updates
    link_building: float = 1.0          # Backlinks
    gbp_activity: float = 1.0           # GBP posts, reviews, photos
    content_creation: float = 1.0       # New pages, articles
    social_signals: float = 1.0         # TikTok, social posts
    entity_building: float = 1.0        # Schema, citations, NAP
    demand_generation: float = 1.0      # Branded search campaigns
    behavioral: float = 1.0             # Dwell time, click depth
    last_updated: datetime = Field(default_factory=datetime.utcnow)


class StrategyEvolution:
    """Evolves strategy weights based on real performance data."""

    def __init__(self, patterns: PatternMemory):
        self.patterns = patterns
        self.weights = StrategyWeights()

    async def evolve(self) -> StrategyWeights:
        """Analyze all patterns and adjust strategy weights."""
        all_patterns = await self.patterns.get_all_patterns()

        if not all_patterns:
            log.info("strategy.evolve  no_patterns — using defaults")
            return self.weights

        # Map patterns to weight categories
        category_map = {
            "meta_optimization": "content_optimization",
            "page_creation": "content_creation",
            "content_creation": "content_creation",
            "gbp_post": "gbp_activity",
            "review_response": "gbp_activity",
            "outreach": "link_building",
            "citation": "entity_building",
            "linking": "content_optimization",
        }

        # Aggregate performance by category
        category_scores: dict[str, list[float]] = {}
        for pattern in all_patterns:
            # Extract category from pattern name (e.g., "WEBSITE:meta_optimization" → "meta_optimization")
            pattern_type = pattern.pattern.split(":")[-1] if ":" in pattern.pattern else pattern.pattern
            category = category_map.get(pattern_type, "content_optimization")

            if category not in category_scores:
                category_scores[category] = []
            category_scores[category].append(pattern.avg_performance)

        # Update weights: higher performance → higher weight
        for category, scores in category_scores.items():
            if not scores:
                continue
            avg = sum(scores) / len(scores)

            # Scale: avg_perf of 7+ → weight 1.5, avg_perf of 3- → weight 0.5
            if avg >= 7:
                new_weight = 1.5
            elif avg >= 5:
                new_weight = 1.0 + (avg - 5) * 0.25  # 5→1.0, 7→1.5
            elif avg >= 3:
                new_weight = 0.75
            else:
                new_weight = 0.5

            if hasattr(self.weights, category):
                setattr(self.weights, category, round(new_weight, 2))
                log.info("strategy.weight_update  %s=%.2f  (avg_perf=%.1f)", category, new_weight, avg)

        # Kill patterns auto-adjust
        killed = await self.patterns.get_killed_patterns()
        for pattern_name in killed:
            pattern_type = pattern_name.split(":")[-1] if ":" in pattern_name else pattern_name
            category = category_map.get(pattern_type)
            if category and hasattr(self.weights, category):
                current = getattr(self.weights, category)
                setattr(self.weights, category, max(0.2, current - 0.3))
                log.warning("strategy.killed_penalty  %s reduced due to killed pattern %s", category, pattern_name)

        self.weights.last_updated = datetime.utcnow()
        log.info("strategy.evolved  weights=%s", self.weights.model_dump())
        return self.weights

    def weights_to_prompt_block(self) -> str:
        """Render current strategy weights as agent context."""
        w = self.weights
        lines = [
            "STRATEGY WEIGHTS (learned from real performance):",
            f"  Content Optimization: {w.content_optimization:.1f}x",
            f"  Link Building: {w.link_building:.1f}x",
            f"  GBP Activity: {w.gbp_activity:.1f}x",
            f"  Content Creation: {w.content_creation:.1f}x",
            f"  Social Signals: {w.social_signals:.1f}x",
            f"  Entity Building: {w.entity_building:.1f}x",
            f"  Demand Generation: {w.demand_generation:.1f}x",
            f"  Behavioral Signals: {w.behavioral:.1f}x",
            "",
            "Higher weight = historically more effective. Prioritize accordingly.",
            f"Last updated: {w.last_updated.strftime('%Y-%m-%d %H:%M')}",
        ]
        return "\n".join(lines)

    def apply_to_scores(self, tasks: list, weight_field: str = "type") -> list:
        """Apply strategy weights to task scores — boost proven strategies."""
        type_to_weight = {
            "WEBSITE": max(self.weights.content_optimization, self.weights.content_creation),
            "CONTENT": self.weights.content_creation,
            "GBP": self.weights.gbp_activity,
            "AUTHORITY": self.weights.link_building,
        }

        for task in tasks:
            task_type = getattr(task, weight_field, "WEBSITE")
            if isinstance(task_type, str):
                multiplier = type_to_weight.get(task_type, 1.0)
            else:
                multiplier = type_to_weight.get(task_type.value, 1.0)

            if hasattr(task, "total_score"):
                task.total_score = round(task.total_score * multiplier, 2)

        return tasks
