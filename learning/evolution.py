"""Self-Evolution Engine — the system rewrites its own strategies based on results.

Meta-learning loop:
  Action → Outcome → Pattern → Strategy Mutation → Prompt Evolution → Next Action

This is not just "learning from results" — it's:
  - Mutating strategy parameters (aggressiveness, content depth, speed bias)
  - Evolving prompts (adding/removing instructions based on what works)
  - Running experiments (A/B testing strategies, not just content)
  - Auto-killing failures and promoting winners
  - Adjusting its own weights and thresholds
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from data.storage.database import Database
from learning.patterns import PatternMemory, ActionPattern

log = logging.getLogger(__name__)


def _get_real_ctr_change(business_id: str) -> float:
    """Pull real CTR change from GA4 if configured, else return 0."""
    try:
        from data.connectors.ga4 import GA4Connector
        ga4 = GA4Connector()
        if not ga4.is_configured():
            return 0.0
        signals = ga4.get_behavioral_signals(days=30)
        return signals.ctr_change
    except Exception:
        return 0.0


def _get_real_conversion_rate(business_id: str) -> float:
    """Pull real conversion rate from GA4 if configured, else return 0.03."""
    try:
        from data.connectors.ga4 import GA4Connector
        ga4 = GA4Connector()
        if not ga4.is_configured():
            return 0.03
        signals = ga4.get_behavioral_signals(days=30)
        return signals.conversion_rate if signals.conversion_rate > 0 else 0.03
    except Exception:
        return 0.03


# =====================================================================
# Strategy Mutation
# =====================================================================

class StrategyParams(BaseModel):
    """Mutable strategy parameters that the system adjusts."""
    aggressiveness: float = 5.0        # 1-10: how aggressive to be
    content_depth: int = 1200          # Target word count for pages
    link_velocity: int = 3             # Links per month target
    update_frequency_days: int = 7     # How often to update pages
    ctr_test_threshold: float = 0.035  # Below this CTR, trigger test
    burst_threshold: int = 8           # Position must be <= this for burst
    min_confidence: float = 6.0        # Minimum confidence to execute
    speed_weight: float = 0.30         # Scoring weight for speed
    impact_weight: float = 0.35        # Scoring weight for impact
    last_mutated: datetime = Field(default_factory=datetime.utcnow)


def mutate_strategy(params: StrategyParams, performance: dict) -> StrategyParams:
    """Mutate strategy parameters based on performance metrics."""
    ranking_gain = performance.get("avg_ranking_gain", 0)
    success_rate = performance.get("success_rate", 0.5)
    ctr_change = performance.get("ctr_change", 0)

    # Not enough ranking movement → be more aggressive
    if ranking_gain < 2:
        params.aggressiveness = min(10, params.aggressiveness + 0.5)
        params.content_depth = min(2000, params.content_depth + 200)
        params.link_velocity = min(10, params.link_velocity + 1)
        log.info("mutation.more_aggressive  agg=%.1f  depth=%d  links=%d",
                 params.aggressiveness, params.content_depth, params.link_velocity)

    # Good movement → maintain or slightly reduce to save resources
    elif ranking_gain >= 4:
        params.aggressiveness = max(3, params.aggressiveness - 0.3)
        log.info("mutation.maintain  agg=%.1f  good_progress", params.aggressiveness)

    # Low success rate → raise confidence threshold (be more selective)
    if success_rate < 0.4:
        params.min_confidence = min(8, params.min_confidence + 0.5)
        log.info("mutation.raise_confidence  min_conf=%.1f", params.min_confidence)
    elif success_rate > 0.7:
        params.min_confidence = max(4, params.min_confidence - 0.3)

    # CTR not improving → lower threshold to trigger more tests
    if ctr_change <= 0:
        params.ctr_test_threshold = min(0.05, params.ctr_test_threshold + 0.005)

    # Pages stagnating → update more frequently
    if ranking_gain == 0:
        params.update_frequency_days = max(3, params.update_frequency_days - 1)
    elif ranking_gain >= 3:
        params.update_frequency_days = min(14, params.update_frequency_days + 1)

    params.last_mutated = datetime.utcnow()
    return params


# =====================================================================
# Prompt Evolution
# =====================================================================

class PromptModifier(BaseModel):
    """A learned addition/removal for prompts."""
    instruction: str
    trigger: str                       # What performance signal triggered this
    added_at: datetime = Field(default_factory=datetime.utcnow)
    performance_since: float = 0.0     # Has it helped?
    active: bool = True


def evolve_prompts(base_prompt: str, performance: dict, modifiers: list[PromptModifier]) -> tuple[str, list[PromptModifier]]:
    """Evolve the system prompt by adding/removing instructions based on results."""
    new_modifiers = list(modifiers)

    # Low CTR → add curiosity instruction
    if performance.get("ctr_change", 0) < 0:
        exists = any(m.instruction.startswith("Make titles") for m in new_modifiers if m.active)
        if not exists:
            new_modifiers.append(PromptModifier(
                instruction="Make titles more curiosity-driven and emotionally engaging. Use power words.",
                trigger="ctr_declining",
            ))

    # Rankings stagnant → add local specificity
    if performance.get("avg_ranking_gain", 0) == 0:
        exists = any("local" in m.instruction.lower() for m in new_modifiers if m.active)
        if not exists:
            new_modifiers.append(PromptModifier(
                instruction="Increase local signals: mention specific neighborhoods, local landmarks, and regional climate factors.",
                trigger="ranking_stagnant",
            ))

    # Content not converting → add CTA instruction
    if performance.get("conversion_rate", 0) < 0.02:
        exists = any("CTA" in m.instruction for m in new_modifiers if m.active)
        if not exists:
            new_modifiers.append(PromptModifier(
                instruction="Every page section must include a micro-CTA or engagement hook. Don't wait until the end.",
                trigger="low_conversion",
            ))

    # Kill modifiers that haven't helped after 3 cycles
    for m in new_modifiers:
        if m.active and m.performance_since < -0.1:
            m.active = False
            log.info("prompt_evolution.killed  instruction=%s", m.instruction[:50])

    # Build evolved prompt
    active_mods = [m for m in new_modifiers if m.active]
    if active_mods:
        additions = "\n".join(f"- {m.instruction}" for m in active_mods)
        evolved = f"{base_prompt}\n\nLEARNED RULES (from performance data):\n{additions}"
    else:
        evolved = base_prompt

    return evolved, new_modifiers


# =====================================================================
# Experimentation Engine
# =====================================================================

class Experiment(BaseModel):
    """A strategy A/B test."""
    experiment_id: str = ""
    name: str
    variant_a: dict = {}               # Strategy config A
    variant_b: dict = {}               # Strategy config B
    results_a: dict = {}
    results_b: dict = {}
    winner: str = ""                   # "a" or "b" or ""
    status: str = "running"            # running / completed
    created_at: datetime = Field(default_factory=datetime.utcnow)


def create_experiment(name: str, current_params: StrategyParams) -> Experiment:
    """Create an A/B test by mutating one parameter."""
    import uuid
    variant_a = current_params.model_dump()
    variant_b = current_params.model_dump()

    # Mutate one thing in variant B
    if "content" in name.lower():
        variant_b["content_depth"] = variant_a["content_depth"] + 300
    elif "aggressive" in name.lower():
        variant_b["aggressiveness"] = min(10, variant_a["aggressiveness"] + 2)
    elif "speed" in name.lower():
        variant_b["speed_weight"] = min(0.5, variant_a["speed_weight"] + 0.1)

    return Experiment(
        experiment_id=uuid.uuid4().hex[:8],
        name=name,
        variant_a=variant_a,
        variant_b=variant_b,
    )


def evaluate_experiment(exp: Experiment) -> str:
    """Determine winner based on results."""
    score_a = exp.results_a.get("ranking_gain", 0) + exp.results_a.get("traffic_gain", 0) * 0.1
    score_b = exp.results_b.get("ranking_gain", 0) + exp.results_b.get("traffic_gain", 0) * 0.1

    if score_b > score_a * 1.1:  # B must beat A by 10%
        exp.winner = "b"
    elif score_a > score_b * 1.1:
        exp.winner = "a"
    else:
        exp.winner = "a"  # Tie goes to control

    exp.status = "completed"
    log.info("experiment.result  name=%s  winner=%s  a=%.1f  b=%.1f",
             exp.name, exp.winner, score_a, score_b)
    return exp.winner


# =====================================================================
# System Health Monitor
# =====================================================================

class SystemHealth(BaseModel):
    """Overall system performance metrics."""
    success_rate: float = 0.0
    avg_ranking_gain: float = 0.0
    avg_traffic_change: float = 0.0
    error_rate: float = 0.0
    total_actions: int = 0
    total_campaigns: int = 0
    active_experiments: int = 0
    prompt_modifiers_active: int = 0
    strategy_params: dict = {}
    last_evolved: datetime | None = None
    status: str = "healthy"            # healthy / degrading / critical


def assess_health(
    patterns: list[ActionPattern],
    params: StrategyParams,
    experiments: list[Experiment],
    modifiers: list[PromptModifier],
) -> SystemHealth:
    """Assess overall system health."""
    health = SystemHealth()

    if patterns:
        health.success_rate = sum(p.success_rate for p in patterns) / len(patterns)
        health.avg_ranking_gain = sum(p.avg_performance for p in patterns) / len(patterns)
        health.total_actions = sum(p.times_used for p in patterns)

    health.active_experiments = sum(1 for e in experiments if e.status == "running")
    health.prompt_modifiers_active = sum(1 for m in modifiers if m.active)
    health.strategy_params = params.model_dump()
    health.last_evolved = params.last_mutated

    if health.success_rate < 0.3:
        health.status = "critical"
    elif health.success_rate < 0.5:
        health.status = "degrading"
    else:
        health.status = "healthy"

    return health


# =====================================================================
# Autonomous Evolution Cycle
# =====================================================================

async def self_evolve(
    db: Database,
    business_id: str,
    params: StrategyParams,
    base_prompt: str,
    modifiers: list[PromptModifier],
) -> tuple[StrategyParams, str, list[PromptModifier], SystemHealth]:
    """Run one self-evolution cycle: assess → mutate → evolve → report."""

    # Get patterns
    pattern_memory = PatternMemory(db)
    patterns = await pattern_memory.get_all_patterns()

    # Calculate performance
    performance = {
        "avg_ranking_gain": sum(p.avg_performance for p in patterns) / max(len(patterns), 1),
        "success_rate": sum(p.success_rate for p in patterns) / max(len(patterns), 1),
        "ctr_change": _get_real_ctr_change(business_id),
        "conversion_rate": _get_real_conversion_rate(business_id),
    }

    # Mutate strategy
    new_params = mutate_strategy(params, performance)

    # Evolve prompts
    evolved_prompt, new_modifiers = evolve_prompts(base_prompt, performance, modifiers)

    # Assess health
    health = assess_health(patterns, new_params, [], new_modifiers)

    log.info("self_evolve.done  health=%s  success_rate=%.2f  mods=%d",
             health.status, health.success_rate, health.prompt_modifiers_active)

    return new_params, evolved_prompt, new_modifiers, health
