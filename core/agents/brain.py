"""SEO Brain v3 — uses unified Claude caller (CLI or API)."""

from __future__ import annotations

import json
import uuid
import logging

from core.claude import call_claude, call_claude_json
from core.prompts.system import MASTER_SYSTEM_PROMPT, build_agent_prompt
from core.scoring.engine import score_and_rank
from models.business import BusinessContext
from models.task import SEOTask, TaskBatch, ImpactLevel, TaskType, ExecutionMode

log = logging.getLogger(__name__)


class SEOBrain:
    """Stateless brain — give it a business context + input type, get scored tasks."""

    def __init__(self, model: str | None = None):
        self.model = model

    async def analyze(
        self,
        business: BusinessContext,
        input_type: str = "FULL",
        max_actions: int = 5,
    ) -> TaskBatch:
        """Run full analysis cycle → scored, filtered, ranked tasks."""
        run_id = uuid.uuid4().hex[:12]
        log.info("brain.analyze  run=%s  type=%s  biz=%s", run_id, input_type, business.business_name)

        prompt = (
            "Here is the permanent business context. Use this as memory. "
            "Never ask for it again.\n\n"
            + business.to_prompt_block()
            + "\n\n---\n\n"
            + build_agent_prompt(input_type, max_actions)
        )

        # Try up to 2 times if we get bad output
        tasks = []
        for attempt in range(2):
            raw = call_claude(prompt, system=MASTER_SYSTEM_PROMPT, model=self.model)
            log.info("brain.raw  run=%s  attempt=%d  len=%d  start=%s",
                     run_id, attempt + 1, len(raw), raw[:80].replace("\n", " "))

            # Check for CLI errors
            if "Error:" in raw and len(raw) < 100:
                log.warning("brain.cli_error  run=%s  raw=%s", run_id, raw)
                continue

            # Extract JSON from response (handle fences, text before/after)
            clean = raw.strip()
            if "```" in clean:
                # Extract content between first ``` and last ```
                parts = clean.split("```")
                for part in parts:
                    part = part.strip()
                    if part.startswith("json"):
                        part = part[4:].strip()
                    if part.startswith("["):
                        clean = part
                        break

            # Try to find JSON array in the response
            if not clean.startswith("["):
                start = clean.find("[")
                if start >= 0:
                    clean = clean[start:]
                    # Find matching closing bracket
                    depth = 0
                    for i, c in enumerate(clean):
                        if c == "[":
                            depth += 1
                        elif c == "]":
                            depth -= 1
                            if depth == 0:
                                clean = clean[:i+1]
                                break

            tasks = self._parse_tasks(clean)
            if tasks:
                break
            log.warning("brain.parse_empty  run=%s  attempt=%d", run_id, attempt + 1)

        log.info("brain.parsed  run=%s  tasks=%d", run_id, len(tasks))

        ranked, filtered_count = score_and_rank(tasks)
        log.info("brain.scored  run=%s  kept=%d  filtered=%d", run_id, len(ranked), filtered_count)

        return TaskBatch(
            input_type=input_type,
            tasks=ranked,
            business_name=business.business_name,
            run_id=run_id,
            filtered_count=filtered_count,
        )

    @staticmethod
    def _get(item: dict, *keys, default="") -> str:
        """Try multiple possible key names, return first non-empty value."""
        for key in keys:
            val = item.get(key, "")
            if val:
                return str(val)
        return default

    @staticmethod
    def _parse_tasks(raw_json: str) -> list[SEOTask]:
        """Parse Claude's JSON output into typed task objects. Handles varied key names."""
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            log.error("brain.parse_fail  raw=%s", raw_json[:200])
            return []

        if isinstance(data, dict):
            # Might be wrapped: {"tasks": [...]} or {"results": [...]}
            if "tasks" in data:
                data = data["tasks"]
            elif "results" in data:
                data = data["results"]
            elif "actions" in data:
                data = data["actions"]
            else:
                data = [data]

        if not isinstance(data, list):
            return []

        tasks: list[SEOTask] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            try:
                # Try multiple possible key names for each field
                action = SEOBrain._get(item, "action", "task", "title", "name", "task_action")
                target = SEOBrain._get(item, "target", "page", "url", "target_page", "target_url")
                why = SEOBrain._get(item, "why", "reason", "reasoning", "rationale", "justification")
                result = SEOBrain._get(item, "estimated_result", "expected_result", "result", "outcome", "expected_outcome")
                time = SEOBrain._get(item, "time_to_result", "timeline", "time", "timeframe", "estimated_time")
                execution = SEOBrain._get(item, "execution", "steps", "execution_steps", "how", "implementation")

                # If execution is a list, join it
                exec_raw = item.get("execution", item.get("steps", ""))
                if isinstance(exec_raw, list):
                    execution = "\n".join(f"{i+1}. {s}" for i, s in enumerate(exec_raw))

                # If action is still empty, try to derive from other fields
                if not action and why:
                    action = why[:100]

                impact_raw = str(item.get("impact", item.get("impact_level", "medium"))).lower().strip()
                if impact_raw not in ("high", "medium", "low"):
                    impact_raw = "medium"

                type_raw = str(item.get("type", item.get("task_type", item.get("category", "WEBSITE")))).upper().strip()
                if type_raw in ("PRIMARY", "SUPPORTING", "EXPERIMENTAL"):
                    type_raw = "WEBSITE"  # These are roles, not types
                if type_raw not in ("GBP", "WEBSITE", "CONTENT", "AUTHORITY"):
                    type_raw = "WEBSITE"

                mode_raw = str(item.get("execution_mode", item.get("mode", "ASSISTED"))).upper().strip()
                if mode_raw not in ("AUTO", "MANUAL", "ASSISTED"):
                    mode_raw = "ASSISTED"

                tasks.append(SEOTask(
                    action=action,
                    target=target,
                    why=why,
                    impact=ImpactLevel(impact_raw),
                    estimated_result=result,
                    time_to_result=time,
                    execution=execution,
                    type=TaskType(type_raw),
                    execution_mode=ExecutionMode(mode_raw),
                    impact_score=float(item.get("impact_score", 0)),
                    ease_score=float(item.get("ease_score", 0)),
                    speed_score=float(item.get("speed_score", 0)),
                    confidence_score=float(item.get("confidence_score", 0)),
                ))
            except Exception as e:
                log.warning("brain.task_skip  err=%s  item=%s", e, str(item)[:100])

        return tasks
