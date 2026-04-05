"""Prediction data models — page scores, ranking predictions, gap analysis."""

from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field


class PageScore(BaseModel):
    """Multi-factor score for a single page's ranking potential."""
    url: str
    keyword: str
    content_score: float = 0.0       # 0-10: keyword match, depth, structure
    authority_score: float = 0.0     # 0-10: backlinks (domain + page level)
    ctr_score: float = 0.0          # 0-10: title + meta effectiveness
    freshness_score: float = 0.0    # 0-10: recent updates, activity
    competition_strength: float = 0.0  # 0-10: how strong top 3 are

    composite: float = 0.0
    predicted_rank: int = 0
    current_rank: int = 0

    scored_at: datetime = Field(default_factory=datetime.utcnow)


class RankingGap(BaseModel):
    """What's missing to reach top 3 for a keyword."""
    url: str
    keyword: str
    current_rank: int = 0
    predicted_rank: int = 0
    content_gap: str = ""           # "+600 words needed" etc
    link_gap: str = ""              # "+3 backlinks needed"
    ctr_gap: str = ""               # "weak title" etc
    freshness_gap: str = ""         # "not updated in 6 months"
    actions_needed: list[str] = Field(default_factory=list)
    estimated_days_to_top3: int = 0
    confidence: float = 0.0


class SERPProfile(BaseModel):
    """Reverse-engineered profile of top-ranking pages for a keyword."""
    keyword: str
    avg_word_count: int = 0
    avg_backlinks: int = 0
    avg_authority: float = 0.0
    common_keywords: list[str] = Field(default_factory=list)
    content_structure: list[str] = Field(default_factory=list)   # common H2s
    weaknesses: list[str] = Field(default_factory=list)
    analyzed_at: datetime = Field(default_factory=datetime.utcnow)


class RankingTimeline(BaseModel):
    """Predicted ranking trajectory for a keyword."""
    keyword: str
    url: str
    current_position: int
    predicted_position: int
    time_to_rank_days: int
    confidence: float
    acceleration_possible: bool = False   # True if close to page 1


class KeywordCluster(BaseModel):
    """Group of related keywords for cluster domination."""
    primary_keyword: str
    variants: list[str] = Field(default_factory=list)
    long_tail: list[str] = Field(default_factory=list)
    local_modifiers: list[str] = Field(default_factory=list)
    high_intent: list[str] = Field(default_factory=list)
    cluster_pages: list[str] = Field(default_factory=list)  # URLs in the cluster
