"""Signal layer data models — demand generation, behavioral influence, pressure campaigns."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class SignalType(str, Enum):
    BRANDED_SEARCH = "branded_search"     # Drive "brand + service" searches
    CLICK_PATTERN = "click_pattern"       # Social → search → click flow
    ENGAGEMENT_SPIKE = "engagement_spike" # Coordinated content burst
    ENTITY_MENTION = "entity_mention"     # Brand mentions across platforms
    CONTENT_VELOCITY = "content_velocity" # Flood of assets for a keyword cluster


class DemandCampaign(BaseModel):
    """A demand generation campaign — creates the searches that lead to rankings."""
    keyword: str
    brand: str
    city: str
    target_search: str = ""          # The exact search we want people to make
    channels: list[str] = []         # tiktok, instagram, youtube, social
    content_hooks: list[str] = []    # Curiosity/problem hooks that drive search
    expected_branded_searches: int = 0
    status: str = "planned"          # planned / active / completed
    created_at: datetime = Field(default_factory=datetime.utcnow)


class PressureCampaign(BaseModel):
    """Multi-channel pressure for a single keyword — overwhelming relevance signal."""
    keyword: str
    cluster_keywords: list[str] = []
    assets: dict[str, int] = {}      # channel: count of assets created
    total_assets: int = 0
    backlinks_targeted: int = 0
    status: str = "planned"
    created_at: datetime = Field(default_factory=datetime.utcnow)


class BehavioralSignal(BaseModel):
    """Behavioral influence tactic (legit — real traffic, not bots)."""
    tactic: str                      # internal_link_loop, content_chain, cta_optimization
    target_page: str
    expected_effect: str             # dwell_time, repeat_visits, click_depth
    implementation: str


class CompetitiveAction(BaseModel):
    """Suppression action against a competitor."""
    competitor: str
    keyword: str
    our_rank: int
    their_rank: int
    action: str                      # outpublish, outlink, out-engage
    detail: str
    priority: str = "high"
