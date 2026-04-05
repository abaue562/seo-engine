"""Multi-Agent Orchestrator v2 — uses unified Claude caller (CLI or API).

Flow:
  INPUT → Data Agent → Analysis Agent → Strategy Agent(s) → Execution Agent → Scoring → OUTPUT
"""

from __future__ import annotations

import json
import uuid
import logging
from dataclasses import dataclass, field

from core.claude import call_claude, call_claude_json
from core.agents.prompts import (
    DATA_AGENT_SYSTEM,
    ANALYSIS_AGENT_SYSTEM,
    STRATEGY_AGENT_SYSTEM,
    STRATEGY_AGENT_CONSERVATIVE_SYSTEM,
    EXECUTION_AGENT_SYSTEM,
)
from core.scoring.engine import score_and_rank
from models.business import BusinessContext
from models.task import SEOTask, TaskBatch, ImpactLevel, TaskType, ExecutionMode

log = logging.getLogger(__name__)


@dataclass
class PipelineLog:
    """Full trace of every agent's input/output for debugging."""
    run_id: str = ""
    data_agent_output: dict = field(default_factory=dict)
    analysis_agent_output: dict = field(default_factory=dict)
    strategy_agent_output: dict = field(default_factory=dict)
    strategy_conservative_output: dict | None = None
    execution_agent_output: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "data_agent_output": self.data_agent_output,
            "analysis_agent_output": self.analysis_agent_output,
            "strategy_agent_output": self.strategy_agent_output,
            "strategy_conservative_output": self.strategy_conservative_output,
            "execution_agent_output": self.execution_agent_output,
        }


class AgentOrchestrator:
    """Chains 4 specialized Claude agents into a full SEO decision pipeline."""

    def __init__(self, model: str | None = None):
        self.model = model

    def _call_agent(self, system: str, user_content: str) -> str:
        return call_claude(user_content, system=system, model=self.model)

    def _call_agent_json(self, system: str, user_content: str, agent_name: str) -> dict | list:
        result = call_claude_json(user_content, system=system, model=self.model)
        if not result:
            log.error("%s.empty_response", agent_name)
        return result

    async def run(
        self,
        business: BusinessContext,
        input_type: str = "FULL",
        disagreement_mode: bool = False,
    ) -> tuple[TaskBatch, PipelineLog]:
        """Run the full 4-agent pipeline."""
        run_id = uuid.uuid4().hex[:12]
        pipeline_log = PipelineLog(run_id=run_id)

        log.info("orchestrator.start  run=%s  type=%s  biz=%s  disagree=%s",
                 run_id, input_type, business.business_name, disagreement_mode)

        # --- Agent 1: Data ---
        data_input = (
            f"Extract and normalize all SEO data from this business context.\n"
            f"Focus on: {input_type}\n\n{business.to_prompt_block()}"
        )
        data_output = self._call_agent_json(DATA_AGENT_SYSTEM, data_input, "data_agent")
        pipeline_log.data_agent_output = data_output if isinstance(data_output, dict) else {}
        log.info("orchestrator.data_done  run=%s", run_id)

        # --- Agent 2: Analysis ---
        analysis_input = (
            f"Analyze this structured SEO data and identify gaps, weaknesses, "
            f"opportunities, and competitor insights.\n\n{json.dumps(data_output, indent=2)}"
        )
        analysis_output = self._call_agent_json(ANALYSIS_AGENT_SYSTEM, analysis_input, "analysis_agent")
        pipeline_log.analysis_agent_output = analysis_output if isinstance(analysis_output, dict) else {}
        log.info("orchestrator.analysis_done  run=%s", run_id)

        # --- Agent 3: Strategy ---
        strategy_input = (
            f"Based on this analysis, decide what actually matters. "
            f"Max 5 high-leverage decisions.\n\n{json.dumps(analysis_output, indent=2)}"
        )
        strategy_output = self._call_agent_json(STRATEGY_AGENT_SYSTEM, strategy_input, "strategy_agent")
        pipeline_log.strategy_agent_output = strategy_output if isinstance(strategy_output, dict) else {}
        log.info("orchestrator.strategy_done  run=%s", run_id)

        # --- Agent 3B: Conservative strategy (disagreement mode) ---
        if disagreement_mode:
            conservative_output = self._call_agent_json(
                STRATEGY_AGENT_CONSERVATIVE_SYSTEM, strategy_input, "strategy_conservative"
            )
            pipeline_log.strategy_conservative_output = conservative_output if isinstance(conservative_output, dict) else {}
            strategy_output = self._merge_strategies(strategy_output, conservative_output)

        # --- Agent 4: Execution ---
        decisions_json = json.dumps(strategy_output, indent=2)
        execution_input = (
            f"Convert these strategic decisions into executable SEO tasks.\n"
            f"Include specific content where possible.\n"
            f"Score each task 1-10 on impact, ease, speed, confidence.\n\n"
            f"Business: {business.business_name} ({business.website})\n\n{decisions_json}"
        )
        execution_output = self._call_agent_json(EXECUTION_AGENT_SYSTEM, execution_input, "execution_agent")
        if isinstance(execution_output, dict):
            execution_output = [execution_output]
        pipeline_log.execution_agent_output = execution_output
        log.info("orchestrator.execution_done  run=%s  raw_tasks=%d", run_id, len(execution_output))

        # --- Parse + Score + Filter + Rank ---
        tasks = self._parse_execution_output(execution_output)
        ranked, filtered_count = score_and_rank(tasks)
        log.info("orchestrator.done  run=%s  kept=%d  filtered=%d", run_id, len(ranked), filtered_count)

        batch = TaskBatch(
            input_type=input_type,
            tasks=ranked,
            business_name=business.business_name,
            run_id=run_id,
            filtered_count=filtered_count,
        )
        return batch, pipeline_log

    @staticmethod
    def _merge_strategies(aggressive: dict, conservative: dict) -> dict:
        all_decisions = []
        seen_focuses = set()
        for source in [aggressive, conservative]:
            if not isinstance(source, dict):
                continue
            for d in source.get("decisions", []):
                focus = d.get("focus", "").lower().strip()
                if focus and focus not in seen_focuses:
                    seen_focuses.add(focus)
                    all_decisions.append(d)
        return {"decisions": all_decisions[:7]}

    @staticmethod
    def _parse_execution_output(data: list) -> list[SEOTask]:
        tasks: list[SEOTask] = []
        for item in data:
            try:
                impact_raw = item.get("impact", "medium").lower().strip()
                if impact_raw not in ("high", "medium", "low"):
                    impact_raw = "medium"
                type_raw = item.get("type", "WEBSITE").upper().strip()
                if type_raw not in ("GBP", "WEBSITE", "CONTENT", "AUTHORITY"):
                    type_raw = "WEBSITE"
                mode_raw = item.get("execution_mode", "MANUAL").upper().strip()
                if mode_raw not in ("AUTO", "MANUAL", "ASSISTED"):
                    mode_raw = "MANUAL"

                tasks.append(SEOTask(
                    action=item.get("action", ""),
                    target=item.get("target", ""),
                    why=item.get("why", ""),
                    impact=ImpactLevel(impact_raw),
                    estimated_result=item.get("estimated_result", ""),
                    time_to_result=item.get("time_to_result", ""),
                    execution=item.get("execution", ""),
                    type=TaskType(type_raw),
                    execution_mode=ExecutionMode(mode_raw),
                    impact_score=float(item.get("impact_score", 0)),
                    ease_score=float(item.get("ease_score", 0)),
                    speed_score=float(item.get("speed_score", 0)),
                    confidence_score=float(item.get("confidence_score", 0)),
                ))
            except Exception as e:
                log.warning("orchestrator.task_skip  err=%s", e)
        return tasks
