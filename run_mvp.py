"""
SEO Engine MVP — Run the full system end-to-end from the command line.

Usage:
  python run_mvp.py                        # Quick single-brain analysis
  python run_mvp.py --mode orchestrate     # Full multi-agent pipeline
  python run_mvp.py --mode execute         # Analyze + execute top task
  python run_mvp.py --mode shadow          # Full run in shadow mode (no real actions)

Uses Claude CLI by default (your Claude Code subscription). Falls back to API if CLI not found.
"""

import asyncio
import argparse
import json
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# Import after logging setup
from core.claude import get_mode
from models.business import BusinessContext
from models.task import SEOTask


# --- Demo business (replace with real data) ---
DEMO_BUSINESS = BusinessContext(
    business_name="Demo Plumbing Co",
    website="https://demoplumbing.com",
    gbp_url="",
    years_active=8,
    primary_service="Plumbing",
    secondary_services=["Drain Cleaning", "Water Heater Repair"],
    primary_city="Austin",
    service_areas=["Austin", "Round Rock", "Cedar Park"],
    target_customer="Homeowners",
    avg_job_value=350,
    primary_keywords=["plumber austin", "emergency plumber austin", "drain cleaning austin"],
    current_rankings={"plumber austin": 11, "emergency plumber austin": 15},
    missing_keywords=["water heater repair austin", "24 hour plumber austin"],
    reviews_count=127,
    rating=4.7,
    monthly_traffic=2400,
    gbp_views=8500,
    competitors=["ABC Plumbing", "Pro Drain Solutions", "Austin Rooter"],
)


def print_tasks(tasks: list[SEOTask]) -> None:
    """Pretty-print scored tasks."""
    print("\n" + "=" * 70)
    print(f"  TOP {len(tasks)} ACTIONS (ranked by score)")
    print("=" * 70)

    for t in tasks:
        print(f"\n  #{t.priority_rank}  [{t.type.value}] [{t.execution_mode.value}]  Score: {t.total_score:.1f}")
        print(f"  ACTION: {t.action}")
        print(f"  TARGET: {t.target}")
        print(f"  WHY:    {t.why}")
        print(f"  IMPACT: {t.impact.value}  |  Result: {t.estimated_result}  |  Time: {t.time_to_result}")
        print(f"  SCORES: impact={t.impact_score} ease={t.ease_score} speed={t.speed_score} conf={t.confidence_score}")
        print(f"  EXEC:   {t.execution[:120]}...")


async def run_analyze(business: BusinessContext) -> list[SEOTask]:
    """Single-brain mode: 1 Claude call."""
    from core.agents.brain import SEOBrain
    brain = SEOBrain()
    log.info("Running single-brain analysis...")
    batch = await brain.analyze(business, input_type="FULL", max_actions=5)
    log.info("Analysis complete: %d tasks, %d filtered", len(batch.tasks), batch.filtered_count)
    return batch.tasks


async def run_orchestrate(business: BusinessContext) -> list[SEOTask]:
    """Multi-agent mode: 4 Claude calls."""
    from core.agents.orchestrator import AgentOrchestrator
    orch = AgentOrchestrator()
    log.info("Running multi-agent orchestration...")
    batch, plog = await orch.run(business, input_type="FULL")
    log.info("Orchestration complete: %d tasks, %d filtered", len(batch.tasks), batch.filtered_count)
    log.info("Pipeline stages: data → analysis → strategy → execution")
    return batch.tasks


async def run_execute(business: BusinessContext, shadow: bool = False) -> None:
    """Analyze + execute the top task."""
    from core.agents.brain import SEOBrain
    from execution.router import ExecutionRouter
    from data.storage.database import Database

    brain = SEOBrain()
    db = Database()
    executor = ExecutionRouter(db, shadow_mode=shadow)

    log.info("Running analysis...")
    batch = await brain.analyze(business, input_type="FULL", max_actions=5)

    if not batch.tasks:
        log.warning("No tasks generated.")
        return

    print_tasks(batch.tasks)

    top_task = batch.tasks[0]
    mode_label = "SHADOW" if shadow else "LIVE"
    log.info(f"\n{'=' * 70}")
    log.info(f"  EXECUTING TOP TASK ({mode_label} MODE)")
    log.info(f"{'=' * 70}")
    log.info(f"  Action: {top_task.action}")
    log.info(f"  Type: {top_task.type.value}  Mode: {top_task.execution_mode.value}")

    result = await executor.execute_task(top_task, business, "demo-001")

    print(f"\n  RESULT: {result.status.value}")
    if result.output:
        print(f"  OUTPUT: {json.dumps(result.output, indent=2, default=str)[:500]}")


async def main():
    parser = argparse.ArgumentParser(description="SEO Engine MVP")
    parser.add_argument("--mode", choices=["analyze", "orchestrate", "execute", "shadow"],
                        default="analyze", help="Run mode")
    args = parser.parse_args()

    business = DEMO_BUSINESS

    mode = get_mode()
    log.info("Claude mode: %s %s", mode.upper(), "(CLI — uses your Claude Code subscription)" if mode == "cli" else "(API — uses ANTHROPIC_API_KEY)")

    if args.mode == "analyze":
        tasks = await run_analyze(business)
        print_tasks(tasks)

    elif args.mode == "orchestrate":
        tasks = await run_orchestrate(business)
        print_tasks(tasks)

    elif args.mode == "execute":
        await run_execute(business, shadow=False)

    elif args.mode == "shadow":
        await run_execute(business, shadow=True)

    print("\n" + "=" * 70)
    print("  MVP RUN COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
