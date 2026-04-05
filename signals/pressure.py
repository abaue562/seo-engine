"""Multi-Channel Pressure System — overwhelm Google with relevance signals.

When targeting a keyword, don't just create a page.
Create simultaneous pressure across ALL channels:
  blog + service page + TikTok + GBP post + social + backlinks

Google sees: overwhelming relevance + activity = trust = rank.
"""

from __future__ import annotations

import logging
from datetime import datetime

from signals.models import PressureCampaign
from channels.models import ContentBundle
from models.business import BusinessContext

log = logging.getLogger(__name__)

# Default pressure targets per keyword campaign
PRESSURE_TARGETS = {
    "service_page": 1,
    "blog_articles": 3,
    "tiktok_scripts": 5,
    "gbp_posts": 3,
    "social_posts": 5,
    "backlinks": 3,
}


class PressureEngine:
    """Orchestrates multi-channel pressure campaigns for keyword domination."""

    def plan_campaign(
        self,
        keyword: str,
        cluster_keywords: list[str],
        business: BusinessContext,
        intensity: str = "standard",  # standard / aggressive / blitz
    ) -> PressureCampaign:
        """Plan a full pressure campaign for a keyword cluster."""

        multiplier = {"standard": 1, "aggressive": 2, "blitz": 3}.get(intensity, 1)

        assets = {}
        for channel, base_count in PRESSURE_TARGETS.items():
            assets[channel] = base_count * multiplier

        campaign = PressureCampaign(
            keyword=keyword,
            cluster_keywords=cluster_keywords[:10],
            assets=assets,
            total_assets=sum(assets.values()),
            backlinks_targeted=assets.get("backlinks", 3),
            status="planned",
        )

        log.info("pressure.planned  keyword=%s  intensity=%s  total_assets=%d",
                 keyword, intensity, campaign.total_assets)
        return campaign

    def track_progress(self, campaign: PressureCampaign, completed: dict[str, int]) -> dict:
        """Track progress against pressure targets."""
        progress = {}
        for channel, target in campaign.assets.items():
            done = completed.get(channel, 0)
            progress[channel] = {
                "target": target,
                "completed": done,
                "remaining": max(0, target - done),
                "pct": round(done / target * 100) if target > 0 else 100,
            }

        overall = sum(v["completed"] for v in progress.values())
        total = sum(v["target"] for v in progress.values())

        return {
            "channels": progress,
            "overall_pct": round(overall / total * 100) if total > 0 else 100,
            "status": "complete" if overall >= total else "in_progress",
        }

    @staticmethod
    def campaign_to_prompt_block(campaign: PressureCampaign) -> str:
        """Render campaign as agent context."""
        lines = [
            f"PRESSURE CAMPAIGN for '{campaign.keyword}':",
            f"  Cluster keywords: {', '.join(campaign.cluster_keywords[:5])}",
            f"  Total assets planned: {campaign.total_assets}",
            f"  Status: {campaign.status}",
            "",
            "  Asset targets:",
        ]
        for channel, count in campaign.assets.items():
            lines.append(f"    {channel}: {count}")
        return "\n".join(lines)
