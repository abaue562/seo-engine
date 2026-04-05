"""Script Campaign Orchestrator — builds 21-day campaigns from templates, zero LLM calls.

The phase structure is always the same shape. Claude was just filling in JSON that
could be a lookup table. This generates the same output deterministically.

Phase structure:
  Foundation  (1-3):   Page + GBP — fix the foundation first
  Distribution (4-7):  Blog + Reddit + Social — push the message out
  Amplification (8-14): TikTok + Links + Medium — amplify winners
  Reinforcement (15-21): Update + CTR test + FAQ — cement and convert
"""

from __future__ import annotations

import uuid
import logging

from models.business import BusinessContext
from orchestration.campaign import Campaign, CampaignPhase

log = logging.getLogger(__name__)

# Platform persona assignments — deterministic rotation
PERSONA_MAP = {
    "blog":     "Local Expert",
    "gbp":      "Local Expert",
    "reddit":   "Practical Homeowner",
    "social":   "Design Enthusiast",
    "tiktok":   "Smart Home Tech",
    "medium":   "Design Enthusiast",
    "quora":    "Practical Homeowner",
    "youtube":  "Smart Home Tech",
    "email":    "Local Expert",
}


def _slug(keyword: str) -> str:
    return keyword.lower().replace(" ", "-")


def build_campaign(
    keyword: str,
    business: BusinessContext,
    duration_days: int = 21,
) -> Campaign:
    """Build a complete phased campaign from templates — no LLM.

    Args:
        keyword: Target keyword (e.g. "landscape lighting kelowna")
        business: BusinessContext
        duration_days: Campaign length (default 21)
    """
    position = business.current_rankings.get(keyword, 0)
    slug = _slug(keyword)
    page_url = f"{business.website}/{slug}"
    city = business.primary_city
    svc = business.primary_service
    biz = business.business_name

    # ── Phase 1: Foundation (Days 1-3) ──────────────────────────────
    foundation_actions = [
        {
            "action": f"Optimize page title + H1 for '{keyword}' — include city + service exactly",
            "channel": "blog",
            "persona": PERSONA_MAP["blog"],
            "day": 1,
            "detail": f"Current pos #{position}. Rewrite to: '{keyword.title()} | {biz}'. Update meta description with offer + city.",
        },
        {
            "action": f"Publish GBP post with exact '{keyword}' keyword + CTA",
            "channel": "gbp",
            "persona": PERSONA_MAP["gbp"],
            "day": 2,
            "detail": f"Post 150-word update about {svc} in {city}. Include keyword '{keyword}' naturally. CTA: 'Get a free quote'.",
        },
        {
            "action": f"Add FAQ schema block to {page_url} with 5 PAA questions",
            "channel": "blog",
            "persona": PERSONA_MAP["blog"],
            "day": 3,
            "detail": f"Wrap existing Q&A in FAQPage JSON-LD. Questions: cost, install time, warranty, how it works, why {biz}.",
        },
    ]

    # ── Phase 2: Distribution (Days 4-7) ────────────────────────────
    distribution_actions = [
        {
            "action": f"Publish blog post: '7 {svc} ideas for {city} homeowners'",
            "channel": "blog",
            "persona": PERSONA_MAP["blog"],
            "day": 4,
            "detail": f"1000-word post. Internal link to {page_url}. Use '{keyword}' in H2 and 3x in body. Add HowTo schema.",
        },
        {
            "action": f"Post Reddit r/{city.lower()} — organic homeowner experience story",
            "channel": "reddit",
            "persona": PERSONA_MAP["reddit"],
            "day": 5,
            "detail": f"Post as homeowner sharing {svc} experience. Include subtle {biz} mention. No direct promotion. r/{city.lower()} or r/homeimprovement.",
        },
        {
            "action": f"Instagram + Facebook post: before/after {svc} project photo",
            "channel": "social",
            "persona": PERSONA_MAP["social"],
            "day": 6,
            "detail": f"Use best project photo. Caption focuses on transformation. Location tag: {city}. Link in bio to {page_url}.",
        },
        {
            "action": f"Publish Medium article: 'Why {city} homeowners are switching to {svc}'",
            "channel": "medium",
            "persona": PERSONA_MAP["medium"],
            "day": 7,
            "detail": f"600-word authoritative piece. Link back to {page_url}. Targets people comparing options.",
        },
    ]

    # ── Phase 3: Amplification (Days 8-14) ──────────────────────────
    amplification_actions = [
        {
            "action": f"TikTok script: 'Things you didn't know about {svc} in {city}'",
            "channel": "tiktok",
            "persona": PERSONA_MAP["tiktok"],
            "day": 8,
            "detail": f"30-second hook-driven video. Start with shock/curiosity. End with '{biz} link in bio'. Use trending audio.",
        },
        {
            "action": f"Build 3 directory citations: Yelp + HomeStars + BBB",
            "channel": "directories",
            "persona": "Local Expert",
            "day": 10,
            "detail": f"Consistent NAP: {biz}, {city}. Include '{keyword}' in description. Upload project photos to each.",
        },
        {
            "action": f"Answer Quora question about {svc} with {city} context",
            "channel": "quora",
            "persona": PERSONA_MAP["quora"],
            "day": 11,
            "detail": f"Find or post question: 'Best {svc} company in {city}?' Answer as knowledgeable homeowner. Link to {page_url} as resource.",
        },
        {
            "action": f"Second GBP post: project showcase with keyword",
            "channel": "gbp",
            "persona": PERSONA_MAP["gbp"],
            "day": 13,
            "detail": f"Post specific project: '[Neighbourhood] {svc} install — before + after'. Include {city} location. Link to {page_url}.",
        },
    ]

    # ── Phase 4: Reinforcement (Days 15-21) ─────────────────────────
    reinforcement_actions = [
        {
            "action": f"Update {page_url} — add 300 words + new section based on top PAA",
            "channel": "blog",
            "persona": PERSONA_MAP["blog"],
            "day": 15,
            "detail": f"Add section answering: 'How much does {keyword} cost?' with price range + factors. Freshness signal to Google.",
        },
        {
            "action": f"A/B test title tag on {page_url} — 3 variants for 7 days",
            "channel": "blog",
            "persona": PERSONA_MAP["blog"],
            "day": 16,
            "detail": f"Test: (1) '{keyword.title()} | {biz}' (2) 'Best {svc} in {city}' (3) '{city} {svc} — Free Quote'. Monitor CTR in GSC.",
        },
        {
            "action": f"Request 5 Google reviews from recent {city} customers",
            "channel": "gbp",
            "persona": PERSONA_MAP["gbp"],
            "day": 17,
            "detail": "Send review request via SMS template. Include direct Google review link. Target customers from last 60 days.",
        },
        {
            "action": f"Final TikTok: 'Results after installing {svc} — 30 days later'",
            "channel": "tiktok",
            "persona": PERSONA_MAP["tiktok"],
            "day": 19,
            "detail": f"Show measurable benefit. Social proof format. Link to {page_url} in bio. Reinforce keyword anchor.",
        },
    ]

    phases = [
        CampaignPhase(name="foundation", day_start=1, day_end=3, actions=foundation_actions),
        CampaignPhase(name="distribution", day_start=4, day_end=7, actions=distribution_actions),
        CampaignPhase(name="amplification", day_start=8, day_end=14, actions=amplification_actions),
        CampaignPhase(name="reinforcement", day_start=15, day_end=21, actions=reinforcement_actions),
    ]

    campaign = Campaign(
        campaign_id=uuid.uuid4().hex[:12],
        goal=f"Rank '{keyword}' in top 3",
        keyword=keyword,
        duration_days=duration_days,
        phases=phases,
        status="planned",
    )

    total = sum(len(p.actions) for p in phases)
    log.info("campaign_script.built  keyword=%s  phases=%d  actions=%d", keyword, len(phases), total)
    return campaign
