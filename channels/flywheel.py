"""Content Flywheel — feedback loop that amplifies what works.

Content → Traffic → Engagement → Authority → Higher Rankings → More Traffic

When content performs well on any channel:
  → boost related keywords
  → create more content variations
  → build backlinks to it
  → redistribute
"""

from __future__ import annotations

import logging
from datetime import datetime
from pydantic import BaseModel, Field

from channels.models import ContentPerformance

log = logging.getLogger(__name__)


class FlywheelAction(BaseModel):
    """Action triggered by the content flywheel."""
    trigger: str           # what triggered this action
    action: str            # what to do
    keyword: str
    channel: str
    priority: str = "high"


class ContentFlywheel:
    """Monitors content performance and triggers amplification."""

    def evaluate(self, performances: list[ContentPerformance]) -> list[FlywheelAction]:
        """Evaluate content performance and generate flywheel actions."""
        actions: list[FlywheelAction] = []

        for perf in performances:
            # High SEO impact → double down on content + links
            if perf.seo_impact >= 7:
                actions.append(FlywheelAction(
                    trigger=f"'{perf.keyword}' ranking improved significantly",
                    action="Build 2-3 additional backlinks to this page to cement position",
                    keyword=perf.keyword,
                    channel="authority",
                ))
                actions.append(FlywheelAction(
                    trigger=f"'{perf.keyword}' ranking improved significantly",
                    action="Create supporting blog post to strengthen topical authority",
                    keyword=perf.keyword,
                    channel="blog",
                ))

            # High social engagement → create more similar content
            if perf.social_engagement >= 7:
                actions.append(FlywheelAction(
                    trigger=f"'{perf.keyword}' high social engagement",
                    action="Create 2 more TikTok variations on this topic",
                    keyword=perf.keyword,
                    channel="tiktok",
                ))
                actions.append(FlywheelAction(
                    trigger=f"'{perf.keyword}' went viral on social",
                    action="Boost this keyword's SEO priority — social signals indicate demand",
                    keyword=perf.keyword,
                    channel="seo",
                    priority="high",
                ))

            # Good traffic but low conversions → optimize CTA
            if perf.traffic_generated >= 100 and perf.conversions < 3:
                actions.append(FlywheelAction(
                    trigger=f"'{perf.keyword}' getting traffic but not converting",
                    action="Optimize CTA and conversion elements on this page",
                    keyword=perf.keyword,
                    channel="website",
                ))

            # High conversions → invest more in this keyword
            if perf.conversions >= 5:
                actions.append(FlywheelAction(
                    trigger=f"'{perf.keyword}' driving real conversions",
                    action="Create keyword cluster around this topic — it converts",
                    keyword=perf.keyword,
                    channel="seo",
                    priority="high",
                ))

            # Low performance everywhere → reduce investment
            if perf.composite_score < 3 and perf.traffic_generated < 20:
                actions.append(FlywheelAction(
                    trigger=f"'{perf.keyword}' underperforming across all channels",
                    action="Deprioritize this keyword — reallocate effort to winners",
                    keyword=perf.keyword,
                    channel="all",
                    priority="low",
                ))

        log.info("flywheel.evaluate  inputs=%d  actions=%d", len(performances), len(actions))
        return actions

    @staticmethod
    def actions_to_prompt_block(actions: list[FlywheelAction]) -> str:
        """Render flywheel actions as agent context."""
        if not actions:
            return "FLYWHEEL: No cross-channel amplification actions triggered."

        lines = ["CONTENT FLYWHEEL ACTIONS:"]
        for a in actions:
            lines.append(f"  [{a.priority.upper()}] {a.action}")
            lines.append(f"    Trigger: {a.trigger}")
            lines.append(f"    Channel: {a.channel}")
        return "\n".join(lines)
