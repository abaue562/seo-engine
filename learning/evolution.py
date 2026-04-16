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
from pathlib import Path
from pydantic import BaseModel, Field

from data.storage.database import Database
from learning.patterns import PatternMemory, ActionPattern

_STRATEGY_PARAMS_PATH = Path("data/storage/strategy_params.json")
_EVOLVED_PROMPTS_PATH = Path("data/storage/evolved_prompts.json")

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


# =====================================================================
# StrategyEvolution class — called by LearningEngine.monthly_cycle()
# =====================================================================

def _load_params_dict() -> dict:
    """Load strategy params from JSON file."""
    if _STRATEGY_PARAMS_PATH.exists():
        try:
            return json.loads(_STRATEGY_PARAMS_PATH.read_text())
        except Exception:
            pass
    return {
        "aggressiveness": 5.0,
        "content_weight": 0.5,
        "link_weight": 0.5,
        "impact_weight": {},
        "last_updated": datetime.utcnow().isoformat(),
    }


def _save_params_dict(params: dict) -> None:
    _STRATEGY_PARAMS_PATH.parent.mkdir(parents=True, exist_ok=True)
    params["last_updated"] = datetime.utcnow().isoformat()
    _STRATEGY_PARAMS_PATH.write_text(json.dumps(params, indent=2, default=str))


def _clamp(value: float, lo: float = 0.1, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


class StrategyEvolution:
    """
    Applies mutations to strategy parameters based on real performance data
    and optionally queries Claude to suggest prompt improvements.
    """

    def __init__(self, patterns: PatternMemory | None = None, db: Database | None = None):
        self.db = db or Database()
        self.patterns = patterns or PatternMemory(self.db)

    async def evolve(self, performance_data: dict) -> dict:
        """
        Run one evolution cycle.

        Args:
            performance_data: dict with keys:
                avg_rank_improvement    – avg positions gained per week (positive = better)
                success_rate            – 0.0-1.0
                content_avg_performance – avg performance score for content tasks
                link_avg_performance    – avg performance score for link/authority tasks
                (other keys tolerated and passed to Claude)

        Returns:
            evolution_report dict with 'mutations', 'prompt_updates', 'health'.
        """
        log.info("evolution.start  perf=%s", performance_data)

        # ------------------------------------------------------------------
        # 1. Load current strategy params
        # ------------------------------------------------------------------
        params = _load_params_dict()

        mutations: dict[str, str] = {}

        # ------------------------------------------------------------------
        # 2. Apply mutations based on performance
        # ------------------------------------------------------------------
        avg_rank_improvement = float(performance_data.get("avg_rank_improvement", 0))
        content_perf = float(performance_data.get("content_avg_performance", 0))
        link_perf = float(performance_data.get("link_avg_performance", 0))

        # Aggressiveness — stored on 0-1 scale internally (legacy StrategyParams used 1-10,
        # but we normalise here to the 0-1 clamp range the spec requests).
        old_agg = float(params.get("aggressiveness", 5.0))
        # Normalise to 0-1 if stored in 1-10 scale
        if old_agg > 1.0:
            old_agg = old_agg / 10.0

        if avg_rank_improvement > 3:
            new_agg = _clamp(old_agg + 0.05)
            log.info("evolution.aggressiveness_up  %.3f -> %.3f", old_agg, new_agg)
            mutations["aggressiveness"] = f"+0.05 ({old_agg:.3f} -> {new_agg:.3f}) — good rank improvement"
        elif avg_rank_improvement < 1:
            new_agg = _clamp(old_agg - 0.05)
            log.info("evolution.aggressiveness_down  %.3f -> %.3f", old_agg, new_agg)
            mutations["aggressiveness"] = f"-0.05 ({old_agg:.3f} -> {new_agg:.3f}) — slow rank improvement"
        else:
            new_agg = old_agg

        params["aggressiveness"] = round(new_agg * 10, 3)  # Persist back in 1-10 scale

        # Content vs link weight
        old_cw = _clamp(float(params.get("content_weight", 0.5)))
        old_lw = _clamp(float(params.get("link_weight", 0.5)))

        if content_perf > link_perf:
            new_cw = _clamp(old_cw + 0.05)
            new_lw = _clamp(old_lw - 0.05)
            mutations["content_weight"] = f"+0.05 ({old_cw:.3f} -> {new_cw:.3f})"
            mutations["link_weight"] = f"-0.05 ({old_lw:.3f} -> {new_lw:.3f})"
            log.info("evolution.content_up  cw=%.3f  lw=%.3f", new_cw, new_lw)
        elif link_perf > content_perf:
            new_cw = _clamp(old_cw - 0.05)
            new_lw = _clamp(old_lw + 0.05)
            mutations["content_weight"] = f"-0.05 ({old_cw:.3f} -> {new_cw:.3f})"
            mutations["link_weight"] = f"+0.05 ({old_lw:.3f} -> {new_lw:.3f})"
            log.info("evolution.link_up  cw=%.3f  lw=%.3f", new_cw, new_lw)
        else:
            new_cw, new_lw = old_cw, old_lw

        params["content_weight"] = round(new_cw, 3)
        params["link_weight"] = round(new_lw, 3)

        # ------------------------------------------------------------------
        # 3. Save updated params
        # ------------------------------------------------------------------
        _save_params_dict(params)

        # ------------------------------------------------------------------
        # 4. Ask Claude to suggest prompt improvements (if API available)
        # ------------------------------------------------------------------
        prompt_updates: dict = {}
        try:
            prompt_updates = await self._ask_claude_for_prompt_suggestions(performance_data, params)
        except Exception as exc:
            log.debug("evolution.claude_skip  err=%s", exc)

        # ------------------------------------------------------------------
        # 5. Assess system health from patterns
        # ------------------------------------------------------------------
        try:
            all_patterns = await self.patterns.get_all_patterns()
            strategy_params_obj = StrategyParams(
                aggressiveness=float(params.get("aggressiveness", 5.0)),
            )
            health = assess_health(all_patterns, strategy_params_obj, [], [])
            health_dict = {"status": health.status, "success_rate": health.success_rate}
        except Exception:
            health_dict = {"status": "unknown"}

        report = {
            "evolved_at": datetime.utcnow().isoformat(),
            "mutations": mutations,
            "prompt_updates": prompt_updates,
            "health": health_dict,
            "params_snapshot": params,
        }

        log.info("evolution.done  mutations=%d  health=%s",
                 len(mutations), health_dict.get("status"))
        return report

    async def _ask_claude_for_prompt_suggestions(
        self,
        performance_data: dict,
        current_params: dict,
    ) -> dict:
        """
        Call Claude (via API or CLI) to suggest prompt improvements.
        Saves results to data/storage/evolved_prompts.json.
        """
        from config.settings import ANTHROPIC_API_KEY

        # Build the meta-prompt
        perf_summary = json.dumps(performance_data, indent=2, default=str)
        params_summary = json.dumps({
            k: v for k, v in current_params.items()
            if k != "last_updated"
        }, indent=2, default=str)

        prompt = (
            "You are an SEO strategy meta-optimizer. "
            "Based on the following 30-day performance data and current strategy parameters, "
            "suggest 3 specific improvements to the system prompts used by the SEO brain "
            "to generate better tasks. Each suggestion should be a concrete instruction "
            "addition of 1-2 sentences.\n\n"
            f"PERFORMANCE DATA:\n{perf_summary}\n\n"
            f"CURRENT STRATEGY PARAMS:\n{params_summary}\n\n"
            "Return a JSON object with key 'suggestions' containing a list of "
            "instruction strings. Example: "
            '{"suggestions": ["Instruction 1.", "Instruction 2.", "Instruction 3."]}'
        )

        raw_response = ""

        if ANTHROPIC_API_KEY:
            try:
                import anthropic
                client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                msg = client.messages.create(
                    model="claude-haiku-4-5",
                    max_tokens=512,
                    messages=[{"role": "user", "content": prompt}],
                )
                raw_response = msg.content[0].text if msg.content else ""
            except Exception as exc:
                log.debug("evolution.api_fail  err=%s", exc)
                # Fall through to CLI

        if not raw_response:
            try:
                from core.claude import call_claude
                raw_response = call_claude(prompt)
            except Exception as exc:
                log.debug("evolution.cli_fail  err=%s", exc)
                return {}

        # Parse JSON from response
        suggestions = []
        try:
            clean = raw_response.strip()
            if "```" in clean:
                for part in clean.split("```"):
                    if part.strip().startswith("{") or part.strip().startswith("json"):
                        clean = part.strip().lstrip("json").strip()
                        break
            start = clean.find("{")
            end = clean.rfind("}")
            if start >= 0 and end > start:
                parsed = json.loads(clean[start:end + 1])
                suggestions = parsed.get("suggestions", [])
        except Exception as exc:
            log.debug("evolution.parse_fail  err=%s", exc)

        if not suggestions:
            return {}

        # Build evolved prompt modifiers
        modifiers = [
            {
                "instruction": s,
                "trigger": "monthly_evolution",
                "added_at": datetime.utcnow().isoformat(),
                "active": True,
            }
            for s in suggestions[:5]
        ]

        # Save to evolved_prompts.json
        evolved_data: dict = {"updated_at": datetime.utcnow().isoformat(), "modifiers": modifiers}
        try:
            _EVOLVED_PROMPTS_PATH.parent.mkdir(parents=True, exist_ok=True)
            existing: dict = {}
            if _EVOLVED_PROMPTS_PATH.exists():
                existing = json.loads(_EVOLVED_PROMPTS_PATH.read_text())
            existing_mods = existing.get("modifiers", [])
            # Append new modifiers (dedup by instruction text)
            existing_instructions = {m.get("instruction") for m in existing_mods}
            for m in modifiers:
                if m["instruction"] not in existing_instructions:
                    existing_mods.append(m)
            evolved_data["modifiers"] = existing_mods[-20:]  # Keep last 20
            _EVOLVED_PROMPTS_PATH.write_text(json.dumps(evolved_data, indent=2, default=str))
            log.info("evolution.prompts_saved  count=%d", len(modifiers))
        except Exception as exc:
            log.warning("evolution.prompts_save_fail  err=%s", exc)

        return {"suggestions_added": len(modifiers), "modifiers": modifiers}
