"""Simulation Engine — test strategies before executing them in the real world.

Flow:
  Generate plan → Create scenarios (variants) → Simulate each → Score → Select best → Execute winner

The simulation model estimates ranking movement, CTR changes, and traffic impact
based on historical patterns and action type weights. It's an approximation that
improves over time as real results calibrate the model.

This is the difference between:
  guess → act → hope
  vs
  simulate → choose → act → refine
"""

from __future__ import annotations

import copy
import logging
from datetime import datetime
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)


# =====================================================================
# Simulation State — digital twin of reality
# =====================================================================

class SimState(BaseModel):
    """Simplified model of the world for simulation."""
    rankings: dict[str, float] = {}      # keyword → position (lower = better)
    ctr: dict[str, float] = {}           # page → click-through rate
    traffic: dict[str, int] = {}         # page → monthly visits
    authority: dict[str, float] = {}     # page → authority score (0-100)
    content_depth: dict[str, int] = {}   # page → word count
    freshness: dict[str, int] = {}       # page → days since last update


# =====================================================================
# Action Effects — estimated impact of each action type
# =====================================================================

# These weights are calibrated by real results over time
ACTION_EFFECTS = {
    "content_update": {
        "ranking_boost": 1.5,       # positions gained
        "ctr_boost": 0.005,
        "traffic_multiplier": 1.1,
        "freshness_reset": True,
    },
    "content_creation": {
        "ranking_boost": 2.0,
        "ctr_boost": 0.01,
        "traffic_multiplier": 1.15,
    },
    "title_optimization": {
        "ranking_boost": 0.5,
        "ctr_boost": 0.02,          # Titles have biggest CTR impact
        "traffic_multiplier": 1.2,
    },
    "backlink": {
        "ranking_boost": 2.5,
        "authority_boost": 5,
        "traffic_multiplier": 1.05,
    },
    "internal_link": {
        "ranking_boost": 0.8,
        "authority_boost": 2,
    },
    "schema_markup": {
        "ranking_boost": 0.3,
        "ctr_boost": 0.015,         # Rich snippets boost CTR
    },
    "gbp_optimization": {
        "ranking_boost": 3.0,       # Huge for local
        "traffic_multiplier": 1.25,
    },
    "gbp_post": {
        "ranking_boost": 0.5,
        "freshness_reset": True,
    },
    "signal_burst": {
        "ranking_boost": 2.0,
        "traffic_multiplier": 1.3,
        "ctr_boost": 0.01,
    },
    "social_push": {
        "traffic_multiplier": 1.1,
        "ctr_boost": 0.005,
    },
}


def simulate_action(state: SimState, action: dict) -> SimState:
    """Apply one action's estimated effect to the simulation state."""
    action_type = action.get("type", action.get("action_type", "content_update"))
    target = action.get("target", action.get("page", ""))
    keyword = action.get("keyword", target)

    effects = ACTION_EFFECTS.get(action_type, ACTION_EFFECTS["content_update"])

    # Apply ranking boost
    if keyword in state.rankings and "ranking_boost" in effects:
        current = state.rankings[keyword]
        boost = effects["ranking_boost"]
        # Diminishing returns: harder to move when already high
        if current <= 5:
            boost *= 0.5
        elif current <= 10:
            boost *= 0.8
        state.rankings[keyword] = max(1, current - boost)

    # Apply CTR boost
    if target in state.ctr and "ctr_boost" in effects:
        state.ctr[target] = min(0.15, state.ctr[target] + effects["ctr_boost"])

    # Apply traffic multiplier
    if target in state.traffic and "traffic_multiplier" in effects:
        state.traffic[target] = int(state.traffic[target] * effects["traffic_multiplier"])

    # Apply authority boost
    if target in state.authority and "authority_boost" in effects:
        state.authority[target] = min(100, state.authority[target] + effects["authority_boost"])

    # Reset freshness
    if effects.get("freshness_reset") and target in state.freshness:
        state.freshness[target] = 0

    return state


def simulate_plan(state: SimState, plan: list[dict]) -> SimState:
    """Simulate an entire plan (sequence of actions)."""
    sim = SimState(**copy.deepcopy(state.model_dump()))
    for action in plan:
        sim = simulate_action(sim, action)
    return sim


# =====================================================================
# Scenario Generation — create strategy variants
# =====================================================================

def generate_scenarios(base_plan: list[dict], keyword: str) -> list[tuple[str, list[dict]]]:
    """Generate multiple strategy variants from a base plan."""
    scenarios = [("baseline", base_plan)]

    # Variant: more backlinks
    more_links = list(base_plan) + [
        {"type": "backlink", "target": keyword, "keyword": keyword},
        {"type": "backlink", "target": keyword, "keyword": keyword},
    ]
    scenarios.append(("more_backlinks", more_links))

    # Variant: more content
    more_content = list(base_plan) + [
        {"type": "content_creation", "target": keyword, "keyword": keyword},
        {"type": "content_update", "target": keyword, "keyword": keyword},
    ]
    scenarios.append(("more_content", more_content))

    # Variant: aggressive push (everything)
    aggressive = list(base_plan) + [
        {"type": "backlink", "target": keyword, "keyword": keyword},
        {"type": "content_update", "target": keyword, "keyword": keyword},
        {"type": "signal_burst", "target": keyword, "keyword": keyword},
        {"type": "title_optimization", "target": keyword, "keyword": keyword},
    ]
    scenarios.append(("aggressive_push", aggressive))

    # Variant: CTR focus
    ctr_focus = list(base_plan) + [
        {"type": "title_optimization", "target": keyword, "keyword": keyword},
        {"type": "schema_markup", "target": keyword, "keyword": keyword},
        {"type": "social_push", "target": keyword, "keyword": keyword},
    ]
    scenarios.append(("ctr_focus", ctr_focus))

    return scenarios


# =====================================================================
# Scoring — evaluate simulated outcomes
# =====================================================================

def score_simulation(original: SimState, simulated: SimState, keyword: str) -> dict:
    """Score a simulated outcome vs the original state."""
    ranking_before = original.rankings.get(keyword, 20)
    ranking_after = simulated.rankings.get(keyword, 20)
    ranking_gain = ranking_before - ranking_after

    target = keyword  # simplified
    ctr_before = original.ctr.get(target, 0.02)
    ctr_after = simulated.ctr.get(target, 0.02)
    ctr_gain = ctr_after - ctr_before

    traffic_before = original.traffic.get(target, 100)
    traffic_after = simulated.traffic.get(target, 100)
    traffic_gain = traffic_after - traffic_before

    # Composite score
    composite = (
        ranking_gain * 0.50 +
        (ctr_gain * 100) * 0.25 +    # Normalize CTR to comparable scale
        (traffic_gain / max(traffic_before, 1)) * 0.25 * 10
    )

    return {
        "ranking_gain": round(ranking_gain, 1),
        "predicted_position": round(ranking_after, 1),
        "ctr_gain": round(ctr_gain, 4),
        "traffic_gain": traffic_gain,
        "composite_score": round(composite, 2),
    }


# =====================================================================
# Main Simulation Runner
# =====================================================================

class SimulationResult(BaseModel):
    keyword: str
    scenarios: list[dict] = Field(default_factory=list)
    best_scenario: str = ""
    best_score: float = 0.0
    recommendation: str = ""


def run_simulation(
    keyword: str,
    current_position: int,
    current_ctr: float = 0.025,
    current_traffic: int = 100,
    current_authority: float = 25,
    base_plan: list[dict] | None = None,
) -> SimulationResult:
    """Run full simulation: generate scenarios, simulate each, pick winner."""

    # Build current state
    state = SimState(
        rankings={keyword: float(current_position)},
        ctr={keyword: current_ctr},
        traffic={keyword: current_traffic},
        authority={keyword: current_authority},
        content_depth={keyword: 800},
        freshness={keyword: 30},
    )

    # Default base plan if none provided
    if not base_plan:
        base_plan = [
            {"type": "content_update", "target": keyword, "keyword": keyword},
            {"type": "title_optimization", "target": keyword, "keyword": keyword},
            {"type": "internal_link", "target": keyword, "keyword": keyword},
        ]

    # Generate and simulate scenarios
    scenarios = generate_scenarios(base_plan, keyword)
    results = []

    for name, plan in scenarios:
        simulated = simulate_plan(state, plan)
        scores = score_simulation(state, simulated, keyword)
        results.append({
            "scenario": name,
            "actions": len(plan),
            **scores,
        })

    # Pick best
    results.sort(key=lambda r: r["composite_score"], reverse=True)
    best = results[0]

    # Risk assessment
    if best["scenario"] == "aggressive_push":
        rec = f"Aggressive push recommended: predicted #{best['predicted_position']:.0f} (+{best['ranking_gain']:.1f} positions). Higher risk but highest reward."
    elif best["scenario"] == "more_backlinks":
        rec = f"Link building focus recommended: predicted #{best['predicted_position']:.0f}. Authority is the main gap."
    elif best["scenario"] == "ctr_focus":
        rec = f"CTR optimization recommended: predicted +{best['ctr_gain']*100:.1f}% CTR. Already ranking well — clicks are the bottleneck."
    elif best["scenario"] == "more_content":
        rec = f"Content depth recommended: predicted #{best['predicted_position']:.0f}. Pages need more substance to compete."
    else:
        rec = f"Baseline plan is sufficient: predicted #{best['predicted_position']:.0f}."

    log.info("simulation.done  keyword=%s  best=%s  score=%.2f  predicted=#%.0f",
             keyword, best["scenario"], best["composite_score"], best["predicted_position"])

    return SimulationResult(
        keyword=keyword,
        scenarios=results,
        best_scenario=best["scenario"],
        best_score=best["composite_score"],
        recommendation=rec,
    )


# =====================================================================
# Model Calibration — adjust weights based on real vs predicted
# =====================================================================

def calibrate_model(predicted: dict, actual: dict) -> dict:
    """Compare prediction vs reality, return adjustment factors."""
    adjustments = {}

    for key in ["ranking_gain", "ctr_gain", "traffic_gain"]:
        pred = predicted.get(key, 0)
        real = actual.get(key, 0)
        if pred != 0:
            accuracy = real / pred
            adjustments[key] = round(accuracy, 2)

    log.info("simulation.calibrate  adjustments=%s", adjustments)
    return adjustments
