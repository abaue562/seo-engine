"""Cross-channel content models."""

from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field


class ContentBundle(BaseModel):
    """One keyword → multiple format outputs for cross-channel distribution."""
    keyword: str
    city: str = ""
    service: str = ""

    # Generated content for each channel
    service_page: dict = {}        # title, meta, h1, content
    blog_article: dict = {}        # title, content_html, word_count
    tiktok_script: dict = {}       # hook, body, cta, caption
    gbp_post: dict = {}            # text, cta, image_prompt
    social_post: dict = {}         # text, hashtags
    email_snippet: dict = {}       # subject, preview

    generated_at: datetime = Field(default_factory=datetime.utcnow)

    @property
    def format_count(self) -> int:
        return sum(1 for d in [self.service_page, self.blog_article, self.tiktok_script,
                               self.gbp_post, self.social_post] if d)


class ContentPerformance(BaseModel):
    """Cross-channel performance tracking for a content piece."""
    keyword: str
    seo_impact: float = 0.0        # ranking change
    social_engagement: float = 0.0  # likes, shares, comments
    traffic_generated: int = 0
    conversions: int = 0
    composite_score: float = 0.0
    measured_at: datetime = Field(default_factory=datetime.utcnow)


class DistributionSchedule(BaseModel):
    """Scheduling rules for cross-channel publishing."""
    tiktok_per_day: int = 1
    gbp_per_week: int = 3
    blog_per_week: int = 2
    social_per_day: int = 2
