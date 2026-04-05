"""Full Integration Test — tests every system component and reports errors/bottlenecks."""

import asyncio
import sys
import time
import os
import json

sys.stdout.reconfigure(encoding="utf-8")

RESULTS = []
ERRORS = []
BOTTLENECKS = []


def test(name, status, detail="", time_s=0):
    icon = "PASS" if status == "pass" else "FAIL" if status == "fail" else "WARN"
    RESULTS.append({"name": name, "status": status, "detail": detail, "time": time_s})
    if status == "fail":
        ERRORS.append(f"{name}: {detail}")
    if time_s > 60:
        BOTTLENECKS.append(f"{name}: {time_s:.0f}s (slow)")
    print(f"  [{icon}] {name} ({time_s:.1f}s)" + (f" — {detail[:80]}" if detail else ""))


async def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    total_start = time.time()

    print("=" * 70)
    print("  FULL INTEGRATION TEST — ALL COMPONENTS")
    print("=" * 70)

    # ---- 1. Core imports ----
    print("\n[1] CORE IMPORTS")
    t = time.time()
    try:
        from models.business import BusinessContext
        from models.task import SEOTask, TaskRole
        from core.claude import call_claude, get_mode
        from core.scoring.engine import score_task, score_and_rank
        test("core_imports", "pass", f"Claude mode: {get_mode()}", time.time() - t)
    except Exception as e:
        test("core_imports", "fail", str(e), time.time() - t)
        return

    # ---- 2. Claude CLI ----
    print("\n[2] CLAUDE CLI")
    t = time.time()
    try:
        result = call_claude("Say OK", system="Reply with only OK", max_tokens=10)
        if "OK" in result.upper() or len(result) < 50:
            test("claude_cli", "pass", f"Response: {result[:30]}", time.time() - t)
        else:
            test("claude_cli", "warn", f"Unexpected: {result[:50]}", time.time() - t)
    except Exception as e:
        test("claude_cli", "fail", str(e), time.time() - t)

    # ---- 3. Scoring engine ----
    print("\n[3] SCORING ENGINE")
    t = time.time()
    try:
        from models.task import ImpactLevel, TaskType, ExecutionMode
        task = SEOTask(
            action="Test", target="/", why="Test", impact=ImpactLevel.HIGH,
            estimated_result="Test", time_to_result="7 days", execution="Test",
            impact_score=8, ease_score=7, speed_score=9, confidence_score=8,
        )
        scored = score_task(task)
        assert scored.total_score > 0
        assert scored.role in (TaskRole.PRIMARY, TaskRole.SUPPORTING, TaskRole.EXPERIMENTAL)
        test("scoring_engine", "pass", f"Score={scored.total_score} Role={scored.role.value}", time.time() - t)
    except Exception as e:
        test("scoring_engine", "fail", str(e), time.time() - t)

    # ---- 4. Simulation engine ----
    print("\n[4] SIMULATION ENGINE")
    t = time.time()
    try:
        from simulation.engine import run_simulation
        sim = run_simulation("test keyword", 10)
        assert len(sim.scenarios) > 0
        assert sim.best_scenario != ""
        test("simulation", "pass", f"Best: {sim.best_scenario}, Score: {sim.best_score}", time.time() - t)
    except Exception as e:
        test("simulation", "fail", str(e), time.time() - t)

    # ---- 5. Self-evolution ----
    print("\n[5] SELF-EVOLUTION")
    t = time.time()
    try:
        from learning.evolution import StrategyParams, mutate_strategy
        params = StrategyParams()
        new_p = mutate_strategy(params, {"avg_ranking_gain": 1, "success_rate": 0.4, "ctr_change": -0.01})
        assert new_p.aggressiveness != params.aggressiveness or new_p.content_depth != params.content_depth
        test("self_evolution", "pass", f"Mutated: agg={new_p.aggressiveness}", time.time() - t)
    except Exception as e:
        test("self_evolution", "fail", str(e), time.time() - t)

    # ---- 6. Page renderer ----
    print("\n[6] PAGE RENDERER")
    t = time.time()
    try:
        from execution.renderers.page_renderer import render_page
        html = render_page(
            {"hero": {"headline": "Test", "subheadline": "Test", "cta": "Click"}, "sections": []},
            {"business_name": "Test", "primary_city": "Test", "primary_service": "Test"},
        )
        assert "<html" in html
        assert len(html) > 500
        test("page_renderer", "pass", f"HTML length: {len(html)}", time.time() - t)
    except Exception as e:
        test("page_renderer", "fail", str(e), time.time() - t)

    # ---- 7. Database ----
    print("\n[7] DATABASE")
    t = time.time()
    try:
        from data.storage.database import Database
        db = Database()
        assert db.is_remote is False  # Local mode
        test("database", "pass", "Local JSON mode", time.time() - t)
    except Exception as e:
        test("database", "fail", str(e), time.time() - t)

    # ---- 8. Browser-use ----
    print("\n[8] BROWSER-USE (AI form automation)")
    t = time.time()
    try:
        import browser_use
        test("browser_use_import", "pass", f"Version installed", time.time() - t)
    except ImportError as e:
        test("browser_use_import", "fail", str(e), time.time() - t)

    # ---- 9. Playwright ----
    print("\n[9] PLAYWRIGHT")
    t = time.time()
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto("https://example.com", timeout=10000)
            title = await page.title()
            await browser.close()
        test("playwright", "pass", f"Loaded example.com: {title}", time.time() - t)
    except Exception as e:
        test("playwright", "fail", str(e), time.time() - t)

    # ---- 10. PRAW (Reddit) ----
    print("\n[10] PRAW (Reddit)")
    t = time.time()
    try:
        import praw
        configured = bool(os.getenv("REDDIT_CLIENT_ID"))
        if configured:
            test("praw", "pass", "Configured + imported", time.time() - t)
        else:
            test("praw", "warn", "Imported but not configured (add REDDIT_CLIENT_ID to .env)", time.time() - t)
    except ImportError as e:
        test("praw", "fail", str(e), time.time() - t)

    # ---- 11. Publishing connectors ----
    print("\n[11] PUBLISHING CONNECTORS")
    t = time.time()
    connectors = {
        "WordPress": bool(os.getenv("WP_URL")),
        "Medium": bool(os.getenv("MEDIUM_TOKEN")),
        "Blogger": bool(os.getenv("BLOGGER_BLOG_ID")),
        "WordPress.com": bool(os.getenv("WP_COM_TOKEN")),
        "Tumblr": bool(os.getenv("TUMBLR_TOKEN")),
        "Pinterest": bool(os.getenv("PINTEREST_TOKEN")),
        "Reddit": bool(os.getenv("REDDIT_CLIENT_ID")),
    }
    connected = sum(1 for v in connectors.values() if v)
    for name, status in connectors.items():
        s = "pass" if status else "warn"
        test(f"connector_{name.lower()}", s, "Connected" if status else "Needs API key", 0)
    test("connectors_total", "warn" if connected == 0 else "pass",
         f"{connected}/{len(connectors)} connected", time.time() - t)

    # ---- 12. Core Analysis (Claude call) ----
    print("\n[12] CORE ANALYSIS (Claude call)")
    t = time.time()
    try:
        from core.agents.brain import SEOBrain
        business = BusinessContext(
            business_name="Test Co", website="https://test.com",
            primary_service="Plumbing", primary_city="Austin",
            primary_keywords=["plumber austin"],
        )
        brain = SEOBrain()
        batch = await brain.analyze(business, max_actions=3)
        if len(batch.tasks) > 0:
            test("core_analysis", "pass", f"{len(batch.tasks)} tasks generated", time.time() - t)
        else:
            test("core_analysis", "warn", "0 tasks — Claude may have returned unexpected format", time.time() - t)
    except Exception as e:
        test("core_analysis", "fail", str(e), time.time() - t)

    # ---- 13. AIC Engine (Claude call) ----
    print("\n[13] AIC ENGINE (Claude call)")
    t = time.time()
    try:
        from aic.engine import AICEngine
        aic = AICEngine()
        result = await aic.generate("plumber austin", business)
        hooks = len(result.attention.hooks)
        sections = len(result.conversion.sections)
        if hooks > 0 or sections > 0:
            test("aic_engine", "pass", f"Hooks: {hooks}, Sections: {sections}", time.time() - t)
        else:
            test("aic_engine", "warn", "Empty result", time.time() - t)
    except Exception as e:
        test("aic_engine", "fail", str(e), time.time() - t)

    # ---- 14. Web Poster (find targets) ----
    print("\n[14] WEB POSTER (find third-party sites)")
    t = time.time()
    try:
        from execution.connectors.external.web_poster import WebPoster
        poster = WebPoster()
        targets = await poster.find_targets("plumber austin", "Test Co", "Plumbing", "Austin")
        if len(targets) > 0:
            test("web_poster_find", "pass", f"Found {len(targets)} sites", time.time() - t)
        else:
            test("web_poster_find", "warn", "0 targets found", time.time() - t)
    except Exception as e:
        test("web_poster_find", "fail", str(e), time.time() - t)

    # ---- 15. Persona System (Claude call) ----
    print("\n[15] PERSONA SYSTEM (Claude call)")
    t = time.time()
    try:
        from personas.system import PersonaSystem
        ps = PersonaSystem()
        pc = await ps.generate_campaign("plumber austin", "Test Co", "Austin", max_personas=2)
        if pc.personas_used > 0:
            test("personas", "pass", f"{pc.personas_used} personas, channels: {pc.channels_covered}", time.time() - t)
        else:
            test("personas", "warn", "0 persona outputs", time.time() - t)
    except Exception as e:
        test("personas", "fail", str(e), time.time() - t)

    # ---- 16. Campaign Orchestrator (Claude call) ----
    print("\n[16] CAMPAIGN ORCHESTRATOR (Claude call)")
    t = time.time()
    try:
        from orchestration.campaign import CampaignOrchestrator
        orch = CampaignOrchestrator()
        campaign = await orch.create_campaign("plumber austin", business)
        s = orch.campaign_summary(campaign)
        if s["total_actions"] > 0:
            test("campaign", "pass", f"{s['total_phases']} phases, {s['total_actions']} actions", time.time() - t)
        else:
            test("campaign", "warn", "Used fallback plan (Claude timed out)", time.time() - t)
    except Exception as e:
        test("campaign", "fail", str(e), time.time() - t)

    # ---- 17. API Server ----
    print("\n[17] API SERVER")
    t = time.time()
    try:
        import httpx
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get("http://localhost:8901/health")
            data = resp.json()
        if data.get("status") == "ok":
            test("api_server", "pass", f"v{data.get('version')} mode={data.get('claude_mode')}", time.time() - t)
        else:
            test("api_server", "warn", f"Unexpected: {data}", time.time() - t)
    except Exception as e:
        test("api_server", "fail", f"Not running? {e}", time.time() - t)

    # ---- SUMMARY ----
    total = time.time() - total_start
    passed = sum(1 for r in RESULTS if r["status"] == "pass")
    warned = sum(1 for r in RESULTS if r["status"] == "warn")
    failed = sum(1 for r in RESULTS if r["status"] == "fail")

    print("\n" + "=" * 70)
    print(f"  INTEGRATION TEST COMPLETE")
    print(f"  Time: {total:.0f}s ({total/60:.1f} min)")
    print(f"  Results: {passed} passed, {warned} warnings, {failed} failed")
    print("=" * 70)

    if ERRORS:
        print(f"\n  ERRORS ({len(ERRORS)}):")
        for e in ERRORS:
            print(f"    X {e[:100]}")

    if BOTTLENECKS:
        print(f"\n  BOTTLENECKS ({len(BOTTLENECKS)}):")
        for b in BOTTLENECKS:
            print(f"    ! {b}")

    # Additional bottleneck analysis
    print("\n  MISSING CREDENTIALS (limits automation):")
    for r in RESULTS:
        if r["status"] == "warn" and "connector" in r["name"]:
            print(f"    - {r['name'].replace('connector_', '')}: {r['detail']}")

    print("\n  SPEED ANALYSIS:")
    slow = [r for r in RESULTS if r["time"] > 30]
    if slow:
        for r in sorted(slow, key=lambda x: x["time"], reverse=True):
            print(f"    {r['name']}: {r['time']:.0f}s")
    else:
        print("    All components under 30s")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
