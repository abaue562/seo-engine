"""Perception Engine — detect narratives, choose strategy, deploy messaging, measure shifts.

Closed-loop system:
  1. Detect what the internet believes about a topic
  2. Score narrative strengths + find gaps
  3. Choose a strategy (which narrative to push)
  4. Generate consistent messaging across channels
  5. Deploy to multi-channel
  6. Measure perception shift (CTR, engagement, query alignment)
  7. Reinforce winners, kill losers
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude

log = logging.getLogger(__name__)


# =====================================================================
# Perception Graph — tracks what the internet "believes"
# =====================================================================

class PerceptionGraph:
    """Weighted graph of narratives — how a topic is perceived."""

    def __init__(self):
        self.nodes: dict[str, float] = defaultdict(float)      # narrative → weight
        self.edges: dict[tuple, float] = defaultdict(float)     # (a, b) → association
        self.sentiment: dict[str, str] = {}                     # narrative → positive/neutral/negative

    def update(self, narrative: str, weight: float = 1.0, sentiment: str = "neutral"):
        self.nodes[narrative] += weight
        self.sentiment[narrative] = sentiment

    def relate(self, a: str, b: str, weight: float = 0.5):
        self.edges[(a, b)] += weight

    def top_narratives(self, k: int = 5) -> list[tuple[str, float]]:
        return sorted(self.nodes.items(), key=lambda x: x[1], reverse=True)[:k]

    def gaps(self, targets: list[str]) -> list[str]:
        """Find target narratives that aren't strongly present."""
        return [t for t in targets if t not in self.nodes or self.nodes[t] < 1.0]

    def to_dict(self) -> dict:
        return {
            "narratives": [
                {"narrative": n, "weight": round(w, 2), "sentiment": self.sentiment.get(n, "neutral")}
                for n, w in self.top_narratives(10)
            ],
            "total_narratives": len(self.nodes),
        }


# =====================================================================
# Strategy Selection
# =====================================================================

STRATEGIES = {
    "cost_transparency": {
        "description": "Counter 'expensive' perception with ROI + value framing",
        "templates": [
            "What {service} actually costs in {city} (and where you save)",
            "How to get premium {service} without overspending",
            "ROI of modern {service} systems — the numbers most companies won't show you",
        ],
    },
    "design_aesthetics": {
        "description": "Push visual transformation narrative",
        "templates": [
            "Why most {city} homes look outdated at night — and how to fix it",
            "Modern {service} that transforms your home's curb appeal instantly",
            "The single upgrade that makes your home look $50K more expensive",
        ],
    },
    "safety_security": {
        "description": "Position lighting as safety/security investment",
        "templates": [
            "The #1 thing police recommend for home security in {city}",
            "Why dark homes are 3x more likely to be targeted",
            "How {service} protects your family (and saves on insurance)",
        ],
    },
    "convenience_tech": {
        "description": "Push smart home / app control narrative",
        "templates": [
            "Control your home lights from your phone — no more climbing ladders",
            "Set it and forget it: {service} that runs itself all year",
            "Why {city} homeowners are ditching seasonal lights forever",
        ],
    },
    "local_authority": {
        "description": "Position as THE local expert",
        "templates": [
            "{city}'s most trusted {service} company — here's why",
            "What {city} homeowners wish they knew about {service}",
            "{reviews} families in {city} chose us. Here's what they say.",
        ],
    },
}


def choose_strategy(graph: PerceptionGraph, service: str) -> str:
    """Choose the best strategy based on current perception landscape."""
    top = graph.top_narratives(5)
    top_labels = [n.lower() for n, _ in top]

    # If cost/expensive dominates negatively → counter with transparency
    if any("cost" in n or "expensive" in n or "price" in n for n in top_labels):
        neg = [n for n in top_labels if graph.sentiment.get(n) == "negative"]
        if neg:
            return "cost_transparency"

    # If safety concerns present → lean into security
    if any("safe" in n or "security" in n or "dark" in n for n in top_labels):
        return "safety_security"

    # Default: design aesthetics (strongest emotional pull)
    return "design_aesthetics"


def generate_messages(strategy: str, service: str, city: str, reviews: int = 0) -> list[str]:
    """Generate messages from strategy templates."""
    config = STRATEGIES.get(strategy, STRATEGIES["design_aesthetics"])
    return [
        t.format(service=service, city=city, reviews=reviews)
        for t in config["templates"]
    ]


# =====================================================================
# Deployment Planning
# =====================================================================

def build_deployment(messages: list[str], target_page: str, keyword: str) -> list[dict]:
    """Turn messages into a multi-channel deployment plan."""
    plan = []
    for i, msg in enumerate(messages):
        plan.append({
            "channel": "blog",
            "title": msg,
            "link_to": target_page,
            "day": i + 1,
        })
        plan.append({
            "channel": "tiktok",
            "hook": msg,
            "cta": f"Search '{keyword}' for more",
            "day": i + 1,
        })
        plan.append({
            "channel": "social",
            "text": msg,
            "link_to": target_page,
            "day": i + 1,
        })
        if i == 0:
            plan.append({
                "channel": "gbp_post",
                "text": msg,
                "day": 1,
            })

    return plan


# =====================================================================
# Measurement
# =====================================================================

def measure_shift(before: dict, after: dict) -> dict:
    """Measure perception shift between two time periods."""
    return {
        "ctr_delta": after.get("ctr", 0) - before.get("ctr", 0),
        "engagement_delta": after.get("time_on_page", 0) - before.get("time_on_page", 0),
        "query_alignment": after.get("aligned_queries", 0) - before.get("aligned_queries", 0),
        "improved": (
            (after.get("ctr", 0) > before.get("ctr", 0)) or
            (after.get("aligned_queries", 0) > before.get("aligned_queries", 0))
        ),
    }


# =====================================================================
# Full Perception Cycle
# =====================================================================

class PerceptionResult(BaseModel):
    keyword: str
    graph: dict = {}
    chosen_strategy: str = ""
    strategy_description: str = ""
    messages: list[str] = []
    deployment_plan: list[dict] = []
    target_narratives: list[str] = []
    narrative_gaps: list[str] = []


async def run_perception_cycle(
    keyword: str,
    business_name: str,
    service: str,
    city: str,
    reviews: int = 0,
    competitors: list[str] | None = None,
) -> PerceptionResult:
    """Run one full perception cycle: detect → score → choose → deploy."""

    # Step 1: Use Claude to detect current narratives
    detect_prompt = f"""Analyze how the internet currently perceives "{keyword}" in {city}.

List 5-7 dominant narratives about this topic. For each:
- The narrative (e.g., "outdoor lighting = luxury upgrade")
- Strength: strong/medium/weak
- Sentiment: positive/neutral/negative

Also identify 2-3 narrative GAPS — angles nobody is pushing.

Return ONLY JSON:
{{
  "narratives": [{{"narrative": "", "strength": "strong|medium|weak", "sentiment": "positive|neutral|negative"}}],
  "gaps": ["untapped narrative angle"]
}}"""

    graph = PerceptionGraph()
    target_narratives = []
    gaps = []

    try:
        raw = call_claude(
            detect_prompt,
            system="You are a narrative analyst. Return ONLY valid JSON.",
            max_tokens=1024,
        )
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        start = raw.find("{")
        if start > 0:
            raw = raw[start:]

        data, _ = json.JSONDecoder().raw_decode(raw)

        for n in data.get("narratives", []):
            weight = {"strong": 3.0, "medium": 2.0, "weak": 1.0}.get(n.get("strength", "medium"), 2.0)
            graph.update(n.get("narrative", ""), weight, n.get("sentiment", "neutral"))

        gaps = data.get("gaps", [])
        target_narratives = [n.get("narrative", "") for n in data.get("narratives", [])[:3]]

    except Exception as e:
        log.error("perception.detect_fail  err=%s", e)

    # Step 2: Choose strategy
    strategy = choose_strategy(graph, service)
    strategy_desc = STRATEGIES.get(strategy, {}).get("description", "")

    # Step 3: Generate messages
    messages = generate_messages(strategy, service, city, reviews)

    # Step 4: Build deployment plan
    target_page = f"https://{business_name.lower().replace(' ', '')}.com/{keyword.replace(' ', '-')}"
    deployment = build_deployment(messages, target_page, keyword)

    log.info("perception.cycle  keyword=%s  strategy=%s  narratives=%d  messages=%d  deploy=%d",
             keyword, strategy, len(graph.nodes), len(messages), len(deployment))

    return PerceptionResult(
        keyword=keyword,
        graph=graph.to_dict(),
        chosen_strategy=strategy,
        strategy_description=strategy_desc,
        messages=messages,
        deployment_plan=deployment,
        target_narratives=target_narratives,
        narrative_gaps=gaps,
    )
