"""Autonomous Scheduler — runs the SEO engine on a loop without human input.

Usage:
  python scheduler.py                    # Run once now
  python scheduler.py --loop             # Run every 24h
  python scheduler.py --loop --hours 12  # Run every 12h
  python scheduler.py --shadow           # Shadow mode (no real execution)

Uses Claude CLI — no API key needed.
"""

import asyncio
import argparse
import logging
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

from strategy.autonomous import AutonomousRunner, AutonomousConfig
from models.business import BusinessContext


# --- Configure your business here ---
BUSINESS = BusinessContext(
    business_name="Demo Plumbing Co",
    website="https://demoplumbing.com",
    primary_service="Plumbing",
    primary_city="Austin",
    service_areas=["Austin", "Round Rock", "Cedar Park"],
    secondary_services=["Drain Cleaning", "Water Heater Repair"],
    primary_keywords=["plumber austin", "emergency plumber austin", "drain cleaning austin"],
    competitors=["ABC Plumbing", "Pro Drain Solutions", "Austin Rooter"],
    reviews_count=127,
    rating=4.7,
    years_active=8,
    target_customer="Homeowners",
    avg_job_value=350,
    monthly_traffic=2400,
)

BUSINESS_ID = "demo-plumbing"


async def run_cycle(shadow: bool = False) -> None:
    config = AutonomousConfig(
        business_id=BUSINESS_ID,
        business=BUSINESS,
        shadow_mode=shadow,
        auto_execute=not shadow,
        max_auto_executions_per_day=3,
        min_confidence_for_auto=7.0,
        min_score_for_auto=7.0,
    )

    runner = AutonomousRunner(config)

    log.info("=" * 60)
    log.info("AUTONOMOUS CYCLE START  shadow=%s", shadow)
    log.info("=" * 60)

    result = await runner.run_cycle()

    log.info("=" * 60)
    log.info("CYCLE COMPLETE")
    log.info("  Data refreshed: %s", result.data_refreshed)
    log.info("  Events detected: %d", result.events_detected)
    log.info("  Tasks generated: %d", result.tasks_generated)
    log.info("  Tasks filtered: %d", result.tasks_filtered)
    log.info("  Auto executed: %d", result.auto_executed)
    log.info("  Queued for approval: %d", result.queued_for_approval)
    log.info("  Skipped: %d", result.skipped)
    log.info("=" * 60)


async def main():
    parser = argparse.ArgumentParser(description="SEO Engine Autonomous Scheduler")
    parser.add_argument("--loop", action="store_true", help="Run continuously on schedule")
    parser.add_argument("--hours", type=int, default=24, help="Hours between runs (default: 24)")
    parser.add_argument("--shadow", action="store_true", help="Shadow mode — analyze but don't execute")
    args = parser.parse_args()

    if args.loop:
        log.info("Starting autonomous loop (every %dh, shadow=%s)", args.hours, args.shadow)
        while True:
            try:
                await run_cycle(shadow=args.shadow)
            except Exception as e:
                log.error("Cycle failed: %s", e)
            log.info("Next run in %d hours...", args.hours)
            await asyncio.sleep(args.hours * 3600)
    else:
        await run_cycle(shadow=args.shadow)


if __name__ == "__main__":
    asyncio.run(main())
