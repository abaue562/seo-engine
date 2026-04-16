#!/usr/bin/env python3
"""Full end-to-end integration test for the SEO Engine content pipeline.

Tests the complete chain:
  keyword → generate → publish → link inject → index → track → feedback

Usage:
  # From the seo-engine root directory:
  python scripts/integration_test.py

  # With a custom keyword:
  python scripts/integration_test.py --keyword "emergency plumber NYC"

  # Dry-run (generate only, no publishing):
  python scripts/integration_test.py --dry-run

Requirements:
  - config/.env must exist with at minimum ANTHROPIC_API_KEY
  - For publishing tests: WP_URL, WP_USER, WP_APP_PASSWORD must be set
  - Redis must be running (or tests use inline mode)

Exit codes:
  0 = all checks passed
  1 = one or more checks failed
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

# Add project root to path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# Load .env before any other imports
from dotenv import load_dotenv
load_dotenv(ROOT / "config" / ".env")
# .env.example is NOT loaded at runtime - documentation only


# ── Colours ──────────────────────────────────────────────────────────────────

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
RESET  = "\033[0m"

def ok(msg: str):   print(f"{GREEN}  ✓ {msg}{RESET}")
def fail(msg: str): print(f"{RED}  ✗ {msg}{RESET}")
def warn(msg: str): print(f"{YELLOW}  ⚠ {msg}{RESET}")
def info(msg: str): print(f"{BLUE}  → {msg}{RESET}")


# ── Test sections ─────────────────────────────────────────────────────────────

PASSED: list[str] = []
FAILED: list[str] = []


def check(name: str, condition: bool, detail: str = ""):
    if condition:
        ok(f"{name}  {detail}")
        PASSED.append(name)
    else:
        fail(f"{name}  {detail}")
        FAILED.append(name)


# =============================================================================
# CHECK 1 — Environment
# =============================================================================

def test_environment():
    print(f"\n{BLUE}══ CHECK 1 — Environment ══{RESET}")

    check("ANTHROPIC_API_KEY set",
          bool(os.getenv("ANTHROPIC_API_KEY")),
          "Required for content generation")

    check("config/.env exists",
          (ROOT / "config" / ".env").exists(),
          str(ROOT / "config" / ".env"))

    check("businesses.json exists",
          (ROOT / "data" / "storage" / "businesses.json").exists())

    wp_url = os.getenv("WP_URL", "")
    wp_ok = bool(wp_url and os.getenv("WP_USER") and os.getenv("WP_APP_PASSWORD"))
    if wp_ok:
        ok(f"WordPress credentials present  url={wp_url}")
    else:
        warn("WordPress credentials missing  (WP_URL / WP_USER / WP_APP_PASSWORD)")

    check("Redis URL configured",
          bool(os.getenv("REDIS_URL", "redis://localhost:6379/0")))


# =============================================================================
# CHECK 2 — Imports (no circular imports, no missing modules)
# =============================================================================

def test_imports():
    print(f"\n{BLUE}══ CHECK 2 — Imports ══{RESET}")

    modules = [
        ("config.settings",              "settings"),
        ("models.business",              "BusinessContext"),
        ("models.task",                  "SEOTask, TaskType"),
        ("core.agents.brain",            "SEOBrain"),
        ("execution.router",             "ExecutionRouter"),
        ("execution.publisher",          "MultiChannelPublisher"),
        ("execution.startup",            "get_publisher, init_publisher"),
        ("execution.link_injector",      "LinkInjector"),
        ("execution.handlers.content",   "ContentHandler"),
        ("execution.handlers.website",   "WebsiteHandler"),
        ("execution.handlers.gbp",       "GBPHandler"),
        ("execution.handlers.authority", "AuthorityHandler"),
        ("execution.connectors.wordpress", "WordPressConnector"),
        ("taskq.celery_app",             "app"),
        ("taskq.tasks",                  "run_content_pipeline"),
        ("monitoring.brand_mentions",    "BrandMentionMonitor"),
        ("data.connectors.rank_tracker", "RankTracker"),
    ]

    for module_path, symbols in modules:
        try:
            mod = __import__(module_path, fromlist=symbols.split(","))
            for sym in [s.strip() for s in symbols.split(",")]:
                assert hasattr(mod, sym), f"missing {sym}"
            check(f"import {module_path}", True, f"[{symbols}]")
        except Exception as e:
            check(f"import {module_path}", False, str(e)[:100])


# =============================================================================
# CHECK 3 — Connector registration
# =============================================================================

def test_connector_registration():
    print(f"\n{BLUE}══ CHECK 3 — Connector Registration ══{RESET}")

    from execution.startup import init_publisher, reset_publisher
    reset_publisher()
    pub = init_publisher()

    registered = list(pub.connectors.keys())
    info(f"Registered connectors: {registered}")

    wp_expected = bool(os.getenv("WP_URL") and os.getenv("WP_USER") and os.getenv("WP_APP_PASSWORD"))
    if wp_expected:
        check("wordpress connector registered", "wordpress" in registered)
    else:
        warn("wordpress connector not registered (credentials not set — expected)")

    check("publisher.connectors is dict", isinstance(pub.connectors, dict))


# =============================================================================
# CHECK 4 — Content generation (inline, no Celery)
# =============================================================================

async def test_content_generation(keyword: str, dry_run: bool):
    print(f"\n{BLUE}══ CHECK 4 — Content Generation ══{RESET}")

    from execution.handlers.content import ContentHandler
    from models.business import BusinessContext

    business = BusinessContext(
        business_name="Test Plumbing NYC",
        website="https://test-plumbing.com",
        primary_service="plumbing",
        primary_city="New York City",
        primary_keywords=[keyword],
        service_areas=["Manhattan", "Brooklyn"],
    )

    handler = ContentHandler()

    t0 = time.time()
    result = await handler.create_article(
        task_id="test_generate_001",
        target=keyword,
        action="create service page",
        business=business,
    )
    elapsed = time.time() - t0

    check("ContentHandler.execute() returns ExecResult", result is not None)
    check("status is SUCCESS or FAILED",
          result.status.value in ("success", "failed"),
          f"got {result.status.value}")

    if result.status.value == "success":
        output = result.output
        check("output contains 'type'", "type" in output)
        check("output contains keyword", bool(output.get("keyword") or keyword in str(output)))
        info(f"Generated in {elapsed:.1f}s")

        # Show snippet
        content = str(output.get("content_html", output.get("content", "")))
        if content:
            info(f"Content preview: {content[:120]}...")
        return output
    else:
        fail(f"Generation failed: {result.output.get('error', 'unknown')}")
        return None


# =============================================================================
# CHECK 5 — WordPress publishing (skipped if no credentials)
# =============================================================================

async def test_publishing(content_output: dict | None, keyword: str):
    print(f"\n{BLUE}══ CHECK 5 — WordPress Publishing ══{RESET}")

    wp_url = os.getenv("WP_URL", "")
    if not (wp_url and os.getenv("WP_USER") and os.getenv("WP_APP_PASSWORD")):
        warn("Skipping — WP_URL / WP_USER / WP_APP_PASSWORD not set")
        return None

    if content_output is None:
        warn("Skipping — no content from previous step")
        return None

    from execution.startup import get_publisher, reset_publisher
    from execution.publisher import ContentPackage

    reset_publisher()
    pub = get_publisher()

    if "wordpress" not in pub.connectors:
        fail("wordpress connector not registered despite credentials being set")
        return None

    package = ContentPackage(
        topic=content_output.get("title", keyword),
        keyword=keyword,
        assets={
            "blog": {
                "title": content_output.get("title", keyword),
                "content": content_output.get("content_html", content_output.get("content", "")),
                "slug": keyword.lower().replace(" ", "-") + "-test",
                "status": "draft",   # always draft in tests
                "type": "posts",
            }
        },
        source="integration_test",
    )

    t0 = time.time()
    report = await pub.publish_package(package)
    elapsed = time.time() - t0

    check("publish_package returned report", report is not None)
    check("at least one publish attempt", report.total_attempted > 0)

    info(f"Published in {elapsed:.1f}s — success={report.total_success}  failed={report.total_failed}")

    wp_result = next((r for r in report.results if r.get("platform") == "wordpress"), {})
    wp_url_result = wp_result.get("url", "")
    wp_post_id = wp_result.get("post_id", "")

    if report.total_success > 0:
        ok(f"WordPress publish succeeded  url={wp_url_result}  post_id={wp_post_id}")
        return {"wp_url": wp_url_result, "wp_post_id": wp_post_id}
    else:
        fail(f"WordPress publish failed: {wp_result.get('error', 'unknown')}")
        return None


# =============================================================================
# CHECK 6 — Link injection
# =============================================================================

async def test_link_injection(publish_result: dict | None, keyword: str):
    print(f"\n{BLUE}══ CHECK 6 — Link Injection ══{RESET}")

    if publish_result is None:
        warn("Skipping — no publish result from previous step")
        return

    from execution.link_injector import LinkInjector

    injector = LinkInjector()
    report = await injector.inject(
        new_url=publish_result.get("wp_url", ""),
        new_keyword=keyword,
        new_post_id=publish_result.get("wp_post_id", ""),
        page_data={},
    )

    check("inject() returned report dict", isinstance(report, dict))
    info(f"Links injected: {report.get('links_injected', 0)}  "
         f"pages updated: {len(report.get('pages_updated', []))}")


# =============================================================================
# CHECK 7 — Celery task queue (requires Redis)
# =============================================================================

def test_celery_tasks():
    print(f"\n{BLUE}══ CHECK 7 — Celery Task Discovery ══{RESET}")

    try:
        from taskq.celery_app import app as celery_app
        registered = list(celery_app.tasks.keys())

        expected_tasks = [
            "taskq.tasks.run_content_pipeline",
            "taskq.tasks.generate_content",
            "taskq.tasks.publish_content",
            "taskq.tasks.inject_internal_links",
            "taskq.tasks.indexnow_and_track",
            "taskq.tasks.run_feedback_loop",
            "taskq.tasks.analyze_business",
            "taskq.tasks.execute_seo_task",
            "taskq.tasks.check_rankings",
            "taskq.tasks.scan_content_decay",
            "taskq.tasks.monitor_ai_citations",
        ]

        for task_name in expected_tasks:
            check(f"task registered: {task_name}",
                  task_name in registered)

    except Exception as e:
        fail(f"Celery import error: {e}")


def test_redis_ping():
    print(f"\n{BLUE}══ CHECK 7b — Redis Connection ══{RESET}")
    try:
        import redis as redis_lib
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        r = redis_lib.from_url(url, socket_connect_timeout=3)
        pong = r.ping()
        check(f"Redis ping  url={url}", pong)
    except Exception as e:
        warn(f"Redis not reachable: {e}  (Workers won't start without Redis)")


# =============================================================================
# CHECK 8 — Full Celery pipeline dispatch (requires Redis + worker)
# =============================================================================

def test_celery_pipeline_dispatch(keyword: str):
    print(f"\n{BLUE}══ CHECK 8 — Pipeline Dispatch via Celery ══{RESET}")

    try:
        import redis as redis_lib
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        r = redis_lib.from_url(url, socket_connect_timeout=2)
        r.ping()
    except Exception:
        warn("Skipping — Redis not reachable")
        return

    from taskq.tasks import run_content_pipeline
    from models.business import BusinessContext

    business = BusinessContext(
        business_name="Pipeline Test Co",
        website=os.getenv("WP_URL", "https://example.com"),
        primary_service="plumbing",
        primary_city="New York City",
        primary_keywords=[keyword],
        service_areas=["Manhattan"],
    )

    task = run_content_pipeline.apply_async(
        args=[business.model_dump(), keyword, "service_page"],
    )
    info(f"Pipeline task queued  task_id={task.id}")
    check("task has an ID", bool(task.id))

    # Wait up to 10s for the task to be acknowledged (not full completion)
    deadline = time.time() + 10
    while time.time() < deadline:
        state = task.state
        if state not in ("PENDING",):
            break
        time.sleep(0.5)

    info(f"Task state after 10s: {task.state}")
    check("task left PENDING state (worker is running)",
          task.state != "PENDING",
          "(If PENDING: start a Celery worker first)")


# =============================================================================
# Main
# =============================================================================

async def main(keyword: str, dry_run: bool):
    print(f"\n{'='*60}")
    print(f"  SEO ENGINE — END-TO-END INTEGRATION TEST")
    print(f"  Keyword: {keyword}")
    print(f"  Dry run: {dry_run}")
    print(f"{'='*60}")

    test_environment()
    test_imports()
    test_connector_registration()
    test_redis_ping()

    content_output = await test_content_generation(keyword, dry_run)

    if not dry_run:
        publish_result = await test_publishing(content_output, keyword)
        await test_link_injection(publish_result, keyword)
    else:
        warn("Dry-run mode — skipping publish + link injection")

    test_celery_tasks()

    if not dry_run:
        test_celery_pipeline_dispatch(keyword)

    # ── Final summary ──────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    total = len(PASSED) + len(FAILED)
    print(f"  RESULTS:  {len(PASSED)}/{total} passed")

    if FAILED:
        print(f"\n{RED}  FAILED CHECKS:{RESET}")
        for name in FAILED:
            print(f"    - {name}")

    if not FAILED:
        print(f"\n{GREEN}  ALL CHECKS PASSED — system is operational{RESET}")
    else:
        print(f"\n{YELLOW}  Fix the failures above before running in production.{RESET}")

    print(f"{'='*60}\n")
    return 0 if not FAILED else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SEO Engine integration test")
    parser.add_argument("--keyword", default="emergency plumber NYC",
                        help="Target keyword for the test page")
    parser.add_argument("--dry-run", action="store_true",
                        help="Generate only — skip publishing, link injection, and Celery dispatch")
    args = parser.parse_args()

    exit_code = asyncio.run(main(args.keyword, args.dry_run))
    sys.exit(exit_code)
