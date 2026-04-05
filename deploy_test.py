"""FULL SYSTEM DEPLOYMENT TEST — Blend Bright Lights

Tests every layer with real execution:
  1. Simulation (predict rankings)
  2. Core Analysis (SEO tasks)
  3. AIC Funnel (attention + intent + conversion)
  4. Page Generation (HTML pages)
  5. Campaign Orchestrator (21-day plan)
  6. Persona Content (multi-voice)
  7. Web Poster (find third-party sites)
  8. Auto-Signup (register on 1 directory with CAPTCHA solving)
  9. Disposable Email (verify it works)
  10. Self-Evolution (mutate strategy)

Reports every error and bottleneck.
"""

import asyncio
import json
import sys
import time
import os

sys.stdout.reconfigure(encoding="utf-8")
os.chdir(os.path.dirname(os.path.abspath(__file__)))

BIZ = {
    "business_name": "Blend Bright Lights",
    "website": "https://blendbright lights.com",
    "primary_service": "Permanent Lighting",
    "secondary_services": ["Roof Line Lights", "Landscape Lighting", "Christmas Lights"],
    "primary_city": "Kelowna",
    "service_areas": ["Kelowna", "West Kelowna", "Lake Country", "Vancouver"],
    "target_customer": "Homeowners",
    "avg_job_value": 3000,
    "primary_keywords": ["permanent lights kelowna", "landscape lighting kelowna", "roof line lights kelowna", "christmas lights kelowna"],
    "current_rankings": {"landscape lighting kelowna": 9, "permanent lights kelowna": 12, "roof line lights kelowna": 15, "christmas lights kelowna": 18},
    "missing_keywords": ["outdoor lighting kelowna", "perm lights kelowna"],
    "reviews_count": 45,
    "rating": 4.9,
    "years_active": 5,
    "monthly_traffic": 800,
    "gbp_views": 3200,
    "competitors": ["Gemstone Lights", "Trimlight Kelowna", "Okanagan Lighting"],
}

ERRORS = []
RESULTS = {}


def log_result(name, status, detail="", t=0):
    icon = "PASS" if status == "pass" else "FAIL" if status == "fail" else "WARN"
    RESULTS[name] = {"status": status, "detail": detail, "time": t}
    if status == "fail":
        ERRORS.append(f"{name}: {detail}")
    print(f"  [{icon}] {name} ({t:.1f}s)" + (f" -- {detail[:100]}" if detail else ""))


async def main():
    total_start = time.time()
    from models.business import BusinessContext
    business = BusinessContext(**BIZ)

    print("=" * 70)
    print("  FULL DEPLOYMENT TEST -- BLEND BRIGHT LIGHTS")
    print("=" * 70)

    # ---- 1. SIMULATION ----
    print("\n[1/10] SIMULATION")
    t = time.time()
    try:
        from simulation.engine import run_simulation
        results = {}
        for kw, pos in BIZ["current_rankings"].items():
            sim = run_simulation(kw, pos, current_authority=25)
            results[kw] = {"predicted": sim.scenarios[0]["predicted_position"], "strategy": sim.best_scenario}
            print(f"    {kw} #{pos} -> #{sim.scenarios[0]['predicted_position']:.0f} ({sim.best_scenario})")
        log_result("simulation", "pass", f"{len(results)} keywords simulated", time.time() - t)
    except Exception as e:
        log_result("simulation", "fail", str(e), time.time() - t)

    # ---- 2. CORE ANALYSIS ----
    print("\n[2/10] CORE ANALYSIS")
    t = time.time()
    try:
        from core.agents.brain import SEOBrain
        brain = SEOBrain()
        batch = await brain.analyze(business, max_actions=4)
        for task in batch.tasks:
            action = task.action or task.why[:60]
            print(f"    #{task.priority_rank} [{task.type.value}] {action[:70]}")
        log_result("core_analysis", "pass" if batch.tasks else "warn", f"{len(batch.tasks)} tasks", time.time() - t)
    except Exception as e:
        log_result("core_analysis", "fail", str(e), time.time() - t)

    # ---- 3. AIC FUNNEL ----
    print("\n[3/10] AIC ENGINE")
    t = time.time()
    try:
        from aic.engine import AICEngine
        aic = AICEngine()
        aic_result = await aic.generate("permanent lights kelowna", business)
        hooks = len(aic_result.attention.hooks)
        sections = len(aic_result.conversion.sections)
        print(f"    Hooks: {hooks}")
        for h in aic_result.attention.hooks:
            print(f"      [{h.get('type','')}] {h.get('text','')}")
        if aic_result.attention.tiktok_script.get("hook"):
            print(f"    TikTok: {aic_result.attention.tiktok_script['hook'][:80]}")
        print(f"    Page: {aic_result.conversion.title} ({sections} sections)")
        log_result("aic_engine", "pass" if hooks > 0 else "warn", f"{hooks} hooks, {sections} sections", time.time() - t)
    except Exception as e:
        log_result("aic_engine", "fail", str(e), time.time() - t)

    # ---- 4. PAGE GENERATION ----
    print("\n[4/10] PAGE GENERATION")
    t = time.time()
    try:
        from execution.renderers.generate import PageGenerator
        gen = PageGenerator()
        path = await gen.generate_and_save("permanent lights kelowna", BIZ, "deploy_test_pages")
        size = os.path.getsize(path)
        print(f"    Generated: {path} ({size:,} bytes)")
        log_result("page_generation", "pass", f"{size:,} bytes", time.time() - t)
    except Exception as e:
        log_result("page_generation", "fail", str(e), time.time() - t)

    # ---- 5. CAMPAIGN ORCHESTRATOR ----
    print("\n[5/10] CAMPAIGN ORCHESTRATOR")
    t = time.time()
    try:
        from orchestration.campaign import CampaignOrchestrator
        orch = CampaignOrchestrator()
        campaign = await orch.create_campaign("landscape lighting kelowna", business)
        s = orch.campaign_summary(campaign)
        print(f"    Goal: {s['goal']}")
        print(f"    {s['total_phases']} phases, {s['total_actions']} actions, {s['duration_days']} days")
        for p in s.get("phases", []):
            print(f"      {p['name']:20s} days {p['days']} | {p['actions']} actions")
        log_result("campaign", "pass" if s["total_actions"] > 0 else "warn", f"{s['total_actions']} actions", time.time() - t)
    except Exception as e:
        log_result("campaign", "fail", str(e), time.time() - t)

    # ---- 6. PERSONA CONTENT ----
    print("\n[6/10] PERSONA SYSTEM")
    t = time.time()
    try:
        from personas.system import PersonaSystem
        ps = PersonaSystem()
        pc = await ps.generate_campaign("permanent lights kelowna", BIZ["business_name"], BIZ["primary_city"], max_personas=2)
        print(f"    Personas: {pc.personas_used}, Channels: {pc.channels_covered}")
        for o in pc.outputs:
            print(f"      [{o.persona_name}] [{o.channel}] {o.title[:50]}")
        log_result("personas", "pass" if pc.personas_used > 0 else "warn", f"{pc.personas_used} voices", time.time() - t)
    except Exception as e:
        log_result("personas", "fail", str(e), time.time() - t)

    # ---- 7. WEB POSTER (find sites) ----
    print("\n[7/10] WEB POSTER (find third-party sites)")
    t = time.time()
    found_targets = []
    found_submissions = []
    try:
        from execution.connectors.external.web_poster import WebPoster
        poster = WebPoster()
        found_targets = await poster.find_targets("permanent lights kelowna", BIZ["business_name"], BIZ["primary_service"], BIZ["primary_city"])
        print(f"    Sites found: {len(found_targets)}")
        for tgt in found_targets[:5]:
            print(f"      [{tgt.authority}] {tgt.name} ({tgt.url})")

        if found_targets:
            found_submissions = await poster.prepare_submissions(found_targets[:2], BIZ["business_name"], BIZ["primary_service"], BIZ["primary_city"], BIZ["website"])
            print(f"    Submissions prepared: {len(found_submissions)}")

        log_result("web_poster", "pass" if found_targets else "warn", f"{len(found_targets)} sites", time.time() - t)
    except Exception as e:
        log_result("web_poster", "fail", str(e), time.time() - t)

    # ---- 8. DISPOSABLE EMAIL + LIVE SIGNUP ----
    print("\n[8/10] DISPOSABLE EMAIL + LIVE SIGNUP")
    t = time.time()
    signup_engine = None
    live_email = ""
    try:
        from execution.connectors.external.auto_signup import AutoSignupEngine
        signup_engine = AutoSignupEngine()
        live_email, email_pw = await signup_engine.create_email()
        is_real = "@" in live_email and "example" not in live_email
        print(f"    Email: {live_email} ({'REAL inbox' if is_real else 'FAKE - fallback'})")
        log_result("disposable_email", "pass" if is_real else "warn", live_email, time.time() - t)
    except Exception as e:
        log_result("disposable_email", "fail", str(e), time.time() - t)

    # ---- 9. AUTO-SIGNUP on a REAL directory ----
    print("\n[9/10] LIVE AUTO-SIGNUP (real directory)")
    t = time.time()
    if found_targets and signup_engine:
        # Pick the safest target — avoid strict sites like Yelp/BBB
        safe_sites = ["manta", "hotfrog", "brownbook", "cylex", "foursquare", "yellowpages"]
        test_target = None
        for tgt in found_targets:
            url_lower = tgt.url.lower()
            if any(s in url_lower for s in safe_sites):
                test_target = tgt
                break
        if not test_target:
            test_target = found_targets[-1]

        print(f"    Target: {test_target.name} ({test_target.url})")
        print(f"    Email: {live_email}")
        try:
            # Use the SAME engine so it reuses the real email
            result = await signup_engine.auto_register(
                site_url=test_target.url,
                site_name=test_target.name,
                business_name=BIZ["business_name"],
                website=BIZ["website"],
                city=BIZ["primary_city"],
                service=BIZ["primary_service"],
                phone="(250) 555-0199",
                description=f"{BIZ['business_name']} provides professional permanent lighting installation in {BIZ['primary_city']}, BC. Rated 4.9/5 with 45+ reviews.",
            )
            print(f"    Status: {result.status}")
            print(f"    Email used: {result.email_used}")
            print(f"    Form submitted: {result.status in ('form_filled', 'verified', 'confirmation_received')}")
            if result.confirmation_received:
                print(f"    CONFIRMATION EMAIL RECEIVED!")
            if result.verification_completed:
                print(f"    VERIFICATION COMPLETED!")
            if result.error:
                print(f"    Error: {result.error[:150]}")
            log_result("auto_signup", "pass" if result.status in ("verified", "form_filled", "confirmation_received") else "warn", f"{result.status} on {test_target.name}", time.time() - t)
        except Exception as e:
            log_result("auto_signup", "fail", str(e), time.time() - t)
    else:
        log_result("auto_signup", "warn", "No targets or signup engine", time.time() - t)

    # ---- 10. SELF-EVOLUTION ----
    print("\n[10/10] SELF-EVOLUTION")
    t = time.time()
    try:
        from learning.evolution import StrategyParams, mutate_strategy
        params = StrategyParams()
        new_p = mutate_strategy(params, {"avg_ranking_gain": 1.5, "success_rate": 0.45, "ctr_change": -0.01})
        print(f"    Aggressiveness: {params.aggressiveness} -> {new_p.aggressiveness}")
        print(f"    Content depth: {params.content_depth} -> {new_p.content_depth}")
        print(f"    Link velocity: {params.link_velocity} -> {new_p.link_velocity}")
        log_result("self_evolution", "pass", "Strategy mutated", time.time() - t)
    except Exception as e:
        log_result("self_evolution", "fail", str(e), time.time() - t)

    # ---- SUMMARY ----
    total = time.time() - total_start
    passed = sum(1 for r in RESULTS.values() if r["status"] == "pass")
    warned = sum(1 for r in RESULTS.values() if r["status"] == "warn")
    failed = sum(1 for r in RESULTS.values() if r["status"] == "fail")

    print("\n" + "=" * 70)
    print(f"  DEPLOYMENT TEST COMPLETE")
    print(f"  Time: {total:.0f}s ({total/60:.1f} min)")
    print(f"  Results: {passed} passed, {warned} warnings, {failed} failed")
    print("=" * 70)

    if ERRORS:
        print(f"\n  ERRORS:")
        for e in ERRORS:
            print(f"    X {e[:120]}")

    print(f"\n  GENERATED ASSETS:")
    print(f"    Pages: deploy_test_pages/permanent-lights-kelowna.html")
    print(f"    Campaign: {RESULTS.get('campaign',{}).get('detail','')}")
    print(f"    Persona content: {RESULTS.get('personas',{}).get('detail','')}")
    print(f"    Third-party sites: {RESULTS.get('web_poster',{}).get('detail','')}")

    print(f"\n  SLOW COMPONENTS:")
    for name, r in sorted(RESULTS.items(), key=lambda x: x[1]["time"], reverse=True):
        if r["time"] > 10:
            print(f"    {name}: {r['time']:.0f}s")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
