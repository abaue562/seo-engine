"""Distribution Engine — publishes content bundles across all channels.

Handles scheduling, rate limiting, and tracking for:
  - Website (service pages, blog articles)
  - TikTok (scripts ready for production)
  - GBP (posts)
  - Social media (posts)
"""

from __future__ import annotations

import logging
from datetime import datetime
from collections import defaultdict
from pydantic import BaseModel, Field

from channels.models import ContentBundle, DistributionSchedule

log = logging.getLogger(__name__)


class DistributionResult(BaseModel):
    keyword: str
    channel: str
    status: str = "ready"         # ready / published / scheduled / rate_limited
    content: dict = {}
    scheduled_for: datetime | None = None


class DistributionEngine:
    """Distributes content bundles across channels with scheduling + rate limiting."""

    def __init__(self, schedule: DistributionSchedule | None = None):
        self.schedule = schedule or DistributionSchedule()
        self._daily_counts: dict[str, int] = defaultdict(int)
        self._day_key: str = ""

    def _reset_if_new_day(self) -> None:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if today != self._day_key:
            self._daily_counts.clear()
            self._day_key = today

    def _check_limit(self, channel: str) -> bool:
        """Check if we can publish to this channel today."""
        self._reset_if_new_day()
        limits = {
            "tiktok": self.schedule.tiktok_per_day,
            "gbp": self.schedule.gbp_per_week // 7 + 1,  # daily allowance
            "blog": self.schedule.blog_per_week // 7 + 1,
            "social": self.schedule.social_per_day,
        }
        limit = limits.get(channel, 5)
        return self._daily_counts[channel] < limit

    def _record(self, channel: str) -> None:
        self._reset_if_new_day()
        self._daily_counts[channel] += 1

    async def distribute(self, bundle: ContentBundle) -> list[DistributionResult]:
        """Distribute a content bundle across all channels."""
        results = []

        # Website — service page
        if bundle.service_page:
            results.append(DistributionResult(
                keyword=bundle.keyword,
                channel="website_page",
                status="ready",
                content=bundle.service_page,
            ))

        # Blog
        if bundle.blog_article:
            if self._check_limit("blog"):
                self._record("blog")
                results.append(DistributionResult(
                    keyword=bundle.keyword,
                    channel="blog",
                    status="ready",
                    content=bundle.blog_article,
                ))
            else:
                results.append(DistributionResult(
                    keyword=bundle.keyword,
                    channel="blog",
                    status="rate_limited",
                ))

        # TikTok
        if bundle.tiktok_script:
            if self._check_limit("tiktok"):
                self._record("tiktok")
                results.append(DistributionResult(
                    keyword=bundle.keyword,
                    channel="tiktok",
                    status="ready",
                    content=bundle.tiktok_script,
                ))
            else:
                results.append(DistributionResult(
                    keyword=bundle.keyword,
                    channel="tiktok",
                    status="rate_limited",
                ))

        # GBP
        if bundle.gbp_post:
            if self._check_limit("gbp"):
                self._record("gbp")
                results.append(DistributionResult(
                    keyword=bundle.keyword,
                    channel="gbp",
                    status="ready",
                    content=bundle.gbp_post,
                ))
            else:
                results.append(DistributionResult(
                    keyword=bundle.keyword,
                    channel="gbp",
                    status="rate_limited",
                ))

        # Social
        if bundle.social_post:
            if self._check_limit("social"):
                self._record("social")
                results.append(DistributionResult(
                    keyword=bundle.keyword,
                    channel="social",
                    status="ready",
                    content=bundle.social_post,
                ))
            else:
                results.append(DistributionResult(
                    keyword=bundle.keyword,
                    channel="social",
                    status="rate_limited",
                ))

        published = sum(1 for r in results if r.status == "ready")
        limited = sum(1 for r in results if r.status == "rate_limited")
        log.info("distribute.done  keyword=%s  published=%d  rate_limited=%d", bundle.keyword, published, limited)

        return results
