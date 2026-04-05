"""AI Visibility data models — answer optimization, entity saturation, mention tracking."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class AISource(str, Enum):
    """Platforms where AI systems pull answers from."""
    WIKIPEDIA = "wikipedia"
    DIRECTORY = "directory"
    QA_SITE = "qa_site"            # Quora, Reddit, StackExchange
    REVIEW_PLATFORM = "review"     # Yelp, BBB, Trustpilot
    BLOG = "blog"
    NEWS = "news"
    SOCIAL = "social"
    SCHEMA = "schema"              # On-site structured data
    KNOWLEDGE_PANEL = "knowledge_panel"


class AnswerBlock(BaseModel):
    """Content structured for AI extraction — direct answer first."""
    question: str
    direct_answer: str              # 2-3 sentences, clear, factual
    detailed_explanation: str       # Longer supporting content
    bullet_points: list[str] = []
    source_url: str = ""
    schema_type: str = "FAQPage"   # FAQPage, HowTo, QAPage


class EntityPresence(BaseModel):
    """Tracks where the business entity exists across knowledge sources."""
    platform: str
    url: str = ""
    status: str = "missing"        # present / missing / incomplete / outdated
    nap_consistent: bool = True    # Name, Address, Phone matches
    last_checked: datetime = Field(default_factory=datetime.utcnow)


class MentionOpportunity(BaseModel):
    """A place where the brand should be mentioned for AI discoverability."""
    platform: str
    url: str = ""
    type: str = ""                 # guest_post, directory, forum, qa_answer
    difficulty: str = "medium"     # easy / medium / hard
    impact: str = "high"           # high / medium
    strategy: str = ""


class AIVisibilityScore(BaseModel):
    """How likely a business is to appear in AI recommendations."""
    business_name: str
    answer_readiness: float = 0.0    # 0-10: content structured for AI extraction
    entity_saturation: float = 0.0   # 0-10: presence across knowledge sources
    mention_density: float = 0.0     # 0-10: brand mentions across the web
    content_authority: float = 0.0   # 0-10: backlinks + trust signals
    composite: float = 0.0           # Weighted average
    scored_at: datetime = Field(default_factory=datetime.utcnow)


class LLMSitemap(BaseModel):
    """llms.txt — tells AI crawlers what this site is about."""
    business_name: str
    description: str
    services: list[str] = []
    service_area: list[str] = []
    key_pages: list[dict] = []      # {url, title, description}
    faqs: list[dict] = []           # {question, answer}
