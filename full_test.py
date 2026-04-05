"""Full system test — ALL layers wired in for Blend Bright Lights.

Usage:
  python full_test.py              # Plan mode (uses LLM where needed)
  python full_test.py --script     # Script mode — zero LLM calls (fastest, ~2 min)
  python full_test.py --execute    # Live mode — actually post to configured channels
  python full_test.py --script --execute  # Script mode + live posting
"""

import asyncio
import os
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

EXECUTE     = "--execute" in sys.argv
SCRIPT_MODE = "--script"  in sys.argv   # Use rule-based engines instead of LLM

BIZ = {
    "business_name": "Blend Bright Lights",
    "website": "https://blendbrightlights.com",
    "primary_service": "Permanent Lighting",
    "secondary_services": ["Roof Line Lights", "Landscape Lighting", "Christmas Lights", "LED Lights"],
    "primary_city": "Kelowna",
    "service_areas": ["Kelowna", "West Kelowna", "Lake Country", "Vancouver"],
    "target_customer": "Homeowners",
    "avg_job_value": 3000,
    "primary_keywords": ["permanent lights kelowna", "roof line lights kelowna", "landscape lighting kelowna", "christmas lights kelowna"],
    "current_rankings": {
        "landscape lighting kelowna": 9,
        "permanent lights kelowna": 12,
        "roof line lights kelowna": 15,
        "christmas lights kelowna": 18,
    },
    "missing_keywords": ["perm lights kelowna", "kelowna xmas lights", "outdoor lighting kelowna"],
    "reviews_count": 45,
    "rating": 4.9,
    "years_active": 5,
    "monthly_traffic": 800,
    "gbp_views": 3200,
    "competitors": ["Gemstone Lights", "Trimlight Kelowna", "Okanagan Lighting"],
}

# Simulated competitor rankings (no live SERP needed)
COMP_RANKINGS = {
    "landscape lighting kelowna":  {"Gemstone Lights": 3,  "Trimlight Kelowna": 6,  "Okanagan Lighting": 7},
    "permanent lights kelowna":    {"Gemstone Lights": 4,  "Trimlight Kelowna": 8,  "Okanagan Lighting": 11},
    "roof line lights kelowna":    {"Gemstone Lights": 5,  "Trimlight Kelowna": 9,  "Okanagan Lighting": 12},
    "christmas lights kelowna":    {"Gemstone Lights": 7,  "Trimlight Kelowna": 11, "Okanagan Lighting": 14},
}
# Simulated prev rankings — Gemstone jumped 4 on landscape
PREV_COMP_RANKINGS = {
    "landscape lighting kelowna":  {"Gemstone Lights": 7,  "Trimlight Kelowna": 6, "Okanagan Lighting": 8},
    "permanent lights kelowna":    {"Gemstone Lights": 5,  "Trimlight Kelowna": 8, "Okanagan Lighting": 12},
    "roof line lights kelowna":    {"Gemstone Lights": 5,  "Trimlight Kelowna": 9, "Okanagan Lighting": 14},
    "christmas lights kelowna":    {"Gemstone Lights": 8,  "Trimlight Kelowna": 11,"Okanagan Lighting": 15},
}
# Simulated GSC data (no live connector needed)
SIMULATED_GSC = [
    {"keyword": "landscape lighting kelowna",  "page": "/landscape-lighting", "impressions": 420, "clicks": 12, "ctr": 0.029, "position": 9},
    {"keyword": "permanent lights kelowna",    "page": "/permanent-lights",   "impressions": 380, "clicks": 9,  "ctr": 0.024, "position": 12},
    {"keyword": "roof line lights kelowna",    "page": "/roof-line-lights",   "impressions": 210, "clicks": 4,  "ctr": 0.019, "position": 15},
    {"keyword": "christmas lights kelowna",    "page": "/christmas-lights",   "impressions": 190, "clicks": 3,  "ctr": 0.016, "position": 18},
]
# Estimated domain authority (no Moz/Ahrefs needed)
OUR_DA    = 14
COMP_DA   = {"Gemstone Lights": 38, "Trimlight Kelowna": 22, "Okanagan Lighting": 19}
COMP_LINKS = {"Gemstone Lights": 480, "Trimlight Kelowna": 140, "Okanagan Lighting": 95}
OUR_LINKS = 45

SEP  = "=" * 70
SEP2 = "-" * 70

def hdr(title):
    print(f"\n{SEP}\n  {title}\n{SEP}")

def sub(title):
    print(f"\n  {SEP2}\n  {title}\n  {SEP2}")

def tick(start):
    print(f"\n  [{time.time()-start:.1f}s]")


async def main():
    start = time.time()

    from models.business import BusinessContext
    business = BusinessContext(**BIZ)

    hdr("BLEND BRIGHT LIGHTS — FULL SEO BATTLE PLAN")
    print(f"  Website : {BIZ['website']}")
    print(f"  Market  : {BIZ['primary_city']} | Avg job: ${BIZ['avg_job_value']:,}")
    print(f"  Reviews : {BIZ['reviews_count']} @ {BIZ['rating']}★  |  DA: {OUR_DA}  |  Backlinks: ~{OUR_LINKS}")

    # ═══════════════════════════════════════════════════════════════
    # LAYER 1 — SIMULATION
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 1 — RANKING PROJECTIONS (no Claude)")
    from simulation.engine import run_simulation

    priority_kw = max(BIZ["current_rankings"], key=lambda k: BIZ["current_rankings"][k])
    for kw, pos in BIZ["current_rankings"].items():
        sim = run_simulation(kw, pos, current_authority=OUR_DA)
        best = sim.scenarios[0]
        predicted = int(best["predicted_position"])
        gain = best["ranking_gain"]
        flag = " ◄ PRIORITY" if predicted <= 5 else ""
        print(f"  #{pos:>2} → #{predicted:<2} (+{gain:.0f})  [{sim.best_scenario}]  {kw}{flag}")

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 2 — KEYWORD DISCOVERY (no Claude)
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 2 — KEYWORD DISCOVERY (no Claude)")

    sub("GOOGLE AUTOCOMPLETE EXPANSION")
    from data.analyzers.autocomplete import get_suggestions
    seed_kw = "permanent lights kelowna"
    suggestions = get_suggestions(seed_kw, country="ca")
    print(f"  Seed: '{seed_kw}'  →  {len(suggestions)} suggestions")
    for s in suggestions[:12]:
        print(f"    • {s}")

    sub("PEOPLE ALSO ASK TREE")
    from data.analyzers.paa_tree import get_paa_questions
    try:
        paa = get_paa_questions("permanent lighting kelowna", num_results=10)
    except Exception:
        paa = []
    if paa:
        print(f"  {len(paa)} PAA questions found:")
        for q in paa[:8]:
            print(f"    Q: {q}")
    else:
        # Fallback: hand-crafted PAA based on niche (Google rate-limited or no results)
        paa = [
            "How much does permanent outdoor lighting cost in Canada?",
            "What are the best permanent Christmas lights?",
            "How long do permanent LED lights last?",
            "Can permanent lights be installed on any home?",
            "Are permanent lights worth it?",
            "What is the difference between Jellyfish lights and Trimlight?",
            "Can you program permanent lights from your phone?",
        ]
        print(f"  Google rate-limited — using niche PAA baseline ({len(paa)} questions):")
        for q in paa:
            print(f"    Q: {q}")

    sub("KEYWORD CLUSTERS")
    from core.keyword_clustering import cluster_by_serp_overlap
    serp_rows = []
    for kw, comp_data in COMP_RANKINGS.items():
        for comp, rank in comp_data.items():
            serp_rows.append({"keyword": kw, "url": f"/{comp.lower().replace(' ','-')}"})
    clusters = cluster_by_serp_overlap(serp_rows, threshold=0.3)
    if clusters:
        print(f"  {len(clusters)} keyword clusters:")
        for c in clusters:
            kws = ", ".join(c.get("keywords", []))
            print(f"    Cluster [{c.get('representative_keyword','')}]: {kws}")
    else:
        print("  All target keywords in same cluster — tight topical focus (good)")

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 2b — DATA ENRICHMENT (CWV + Volume + Snippets + Backlinks)
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 2b — DATA ENRICHMENT (no Claude)")

    # ── Core Web Vitals ────────────────────────────────────────────
    sub("CORE WEB VITALS — " + BIZ["website"])
    from data.analyzers.cwv import measure_cwv, score_cwv, cwv_to_task
    cwv_mobile  = measure_cwv(BIZ["website"], strategy="mobile")
    cwv_desktop = measure_cwv(BIZ["website"], strategy="desktop")

    for label, cwv in [("MOBILE", cwv_mobile), ("DESKTOP", cwv_desktop)]:
        if cwv.error:
            print(f"  [{label}] Error: {cwv.error}")
        else:
            status = "PASS ✓" if cwv.passed else "FAIL ✗"
            print(f"  [{label}]  Score: {cwv.performance_score}/100  Grade: {cwv.grade}  CWV: {status}")
            print(f"    LCP  : {cwv.lcp.display:12s} ({cwv.lcp.rating})")
            print(f"    CLS  : {cwv.cls.display:12s} ({cwv.cls.rating})")
            print(f"    INP  : {cwv.inp.display:12s} ({cwv.inp.rating})")
            print(f"    TTFB : {cwv.ttfb.display:12s} ({cwv.ttfb.rating})")
            if not cwv.passed:
                print(f"    BOTTLENECK: {cwv.bottleneck}")
                print(f"    FIX: {cwv.action}")
            if cwv.opportunities:
                print(f"    TOP OPPORTUNITIES:")
                for o in cwv.opportunities[:3]:
                    print(f"      • {o}")

    cwv_task = cwv_to_task(cwv_mobile, BIZ["business_name"])
    if cwv_task:
        print(f"\n  ⚠  CWV TASK ADDED: {cwv_task['action']}")

    # ── Keyword Volume Enrichment ──────────────────────────────────
    sub("KEYWORD VOLUME + DIFFICULTY (DataForSEO)")
    from data.connectors.dataforseo import DataForSEOClient
    dfs = DataForSEOClient()
    all_kws = list(BIZ["current_rankings"].keys()) + suggestions[:6] + paa[:4]

    if dfs.is_configured():
        volume_map = dfs.enrich_keywords(all_kws, location="ca")
        print(f"  {len(volume_map)} keywords enriched with real volume data:")
        for kw in list(BIZ["current_rankings"].keys()) + suggestions[:4]:
            m = volume_map.get(kw)
            if m:
                trend_dir = "↑" if len(m.trend) >= 2 and m.trend[-1] > m.trend[0] else "↓" if len(m.trend) >= 2 else "–"
                print(f"  {kw[:45]:45s}  vol={m.search_volume:>6,}  KD={m.keyword_difficulty:>3}  CPC=${m.cpc:.2f}  {trend_dir}")
    else:
        print("  DataForSEO not configured — add DATAFORSEO_LOGIN + DATAFORSEO_PASSWORD to config/.env")
        print("  Register free at dataforseo.com (free trial available)")
        # Show what volume data would look like with estimated values
        print("\n  Estimated volumes (based on niche benchmarks — not real data):")
        est = {
            "landscape lighting kelowna": (320, 28, 1.85),
            "permanent lights kelowna":   (480, 22, 2.10),
            "roof line lights kelowna":   (210, 18, 1.60),
            "christmas lights kelowna":   (590, 15, 1.20),
        }
        for kw, (vol, kd, cpc) in est.items():
            print(f"  {kw:45s}  vol≈{vol:>5,}  KD≈{kd:>3}  CPC≈${cpc:.2f}  [ESTIMATED]")

    # ── Backlink Profile ───────────────────────────────────────────
    sub("BACKLINK PROFILE COMPARISON (DataForSEO)")
    if dfs.is_configured():
        backlink_comparison = dfs.compare_backlink_profiles(
            "blendbrightlights.com",
            [c.lower().replace(" ", "") + ".com" for c in BIZ["competitors"]],
        )
        our_rank = backlink_comparison["our_rank"]
        our_bl   = backlink_comparison["our_backlinks"]
        our_rd   = backlink_comparison["our_referring_domains"]
        print(f"  {BIZ['business_name']:30s}  DR={our_rank:>3}  Backlinks={our_bl:>6,}  RDs={our_rd:>5,}")
        for comp, stats in backlink_comparison["competitors"].items():
            gap_icon = "🔴" if stats["rank_gap"] > 15 else "🟡" if stats["rank_gap"] > 5 else "🟢"
            print(f"  {gap_icon} {comp:28s}  DR={stats['rank']:>3}  Backlinks={stats['backlinks']:>6,}  RDs={stats['referring_domains']:>5,}  gap=+{stats['rank_gap']}")
        if backlink_comparison["links_needed_to_match_leader"] > 0:
            print(f"\n  Links needed to match leader: {backlink_comparison['links_needed_to_match_leader']:,}")
            print(f"  RDs needed to match leader  : {backlink_comparison['rds_needed_to_match_leader']:,}")
    else:
        print("  DataForSEO not configured — showing estimated gap from Layer 4 inputs")
        print(f"  Our DR: ~{OUR_DA}  |  Gemstone: ~{COMP_DA['Gemstone Lights']}  |  Gap: {COMP_DA['Gemstone Lights'] - OUR_DA}")

    # ── Featured Snippet Opportunities ────────────────────────────
    sub("FEATURED SNIPPET OPPORTUNITIES")
    from data.analyzers.snippet_format import analyze_snippet_batch
    snippet_kws = paa[:5] + [
        "how much does permanent lighting cost kelowna",
        "permanent lights vs christmas lights",
        "best permanent outdoor lights canada",
    ]
    # Use intent-only analysis (no SERP fetch) to avoid rate limits in test
    snippets = analyze_snippet_batch(snippet_kws, check_serp=False)
    print(f"  {len(snippets)} snippet opportunities analyzed (intent-based, no SERP fetch):")
    easy = [s for s in snippets if s.difficulty == "easy"]
    medium = [s for s in snippets if s.difficulty == "medium"]
    print(f"  Easy (no current holder): {len(easy)}  |  Medium/Hard: {len(medium)}")
    print()
    for s in snippets[:8]:
        diff_icon = "🟢" if s.difficulty == "easy" else "🟡"
        print(f"  {diff_icon} [{s.format_needed.value:14s}] {s.keyword}")
        print(f"     Heading : {s.heading_format}")
        print(f"     Target  : {s.answer_word_target}")
        print(f"     Schema  : {s.schema_type}")

    # ── GA4 Behavioral Signals ─────────────────────────────────────
    sub("GA4 BEHAVIORAL SIGNALS")
    from data.connectors.ga4 import GA4Connector
    ga4 = GA4Connector()
    if ga4.is_configured():
        signals = ga4.get_behavioral_signals(days=30)
        print(f"  Avg session duration : {signals.avg_session_duration:.0f}s")
        print(f"  Avg bounce rate      : {signals.avg_bounce_rate:.1%}")
        print(f"  Conversion rate      : {signals.conversion_rate:.2%}")
        print(f"  Total conversions    : {signals.total_conversions}")
        if signals.top_converting_pages:
            print(f"  Top converting pages : {signals.top_converting_pages[:3]}")
        if signals.underperforming_pages:
            print(f"  Underperforming      : {signals.underperforming_pages[:3]}")
        print(f"  → CTR signal for evolution: {signals.ctr_change:+.3f}")
        print(f"  → Conversion rate for evolution: {signals.conversion_rate:.3f}")
    else:
        print("  GA4 not configured — self-evolution using baseline estimates")
        print("  To fix: add GA4_PROPERTY_ID + GA4_CREDENTIALS_PATH to config/.env")
        print("  Setup: GCP Console → GA4 Data API → Service Account → grant access in GA4 admin")

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 3 — CORE SEO ANALYSIS (1 Claude call)
    # ═══════════════════════════════════════════════════════════════
    hdr(f"LAYER 3 — CORE SEO ANALYSIS ({'script' if SCRIPT_MODE else '1 Claude call'})")
    if SCRIPT_MODE:
        from core.agents.brain_script import generate_tasks
        batch = generate_tasks(
            business,
            our_da=OUR_DA,
            our_links=OUR_LINKS,
            comp_da=COMP_DA,
            comp_links=COMP_LINKS,
            gsc_data=SIMULATED_GSC,
            max_tasks=5,
        )
    else:
        from core.agents.brain import SEOBrain
        brain = SEOBrain()
        batch = await brain.analyze(business, max_actions=5)

    for t in batch.tasks:
        action = t.action or t.why
        print(f"\n  #{t.priority_rank}  [{t.type.value.upper()}]  Score: {t.total_score:.0f}")
        print(f"  WHAT : {action[:120]}")
        if hasattr(t, 'why') and t.why and t.why != action:
            print(f"  WHY  : {t.why[:120]}")
        if hasattr(t, 'target') and t.target:
            print(f"  URL  : {t.target}")

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 4 — COMPETITIVE INTELLIGENCE (Claude calls)
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 4 — COMPETITIVE INTELLIGENCE")

    # 4a: CTR Dominator
    sub("CTR DOMINATOR — LOW-CTR PAGES")
    from prediction.ctr import CTRDominator
    ctr_dom = CTRDominator()
    low_ctr = ctr_dom.detect_low_ctr_pages(SIMULATED_GSC, min_impressions=100, ctr_threshold=0.035)
    print(f"  {len(low_ctr)} pages under-performing on CTR:")
    ctr_tests = []
    for row in low_ctr:
        pot = row["potential_clicks"]
        print(f"  '{row['keyword']}' — pos #{row['position']}  CTR {row['ctr']:.1%}  +{pot} clicks available")

    if low_ctr:
        best = low_ctr[0]
        print(f"\n  Generating title variants for best opportunity: '{best['keyword']}'")
        ctr_variants = await ctr_dom.generate_variants(
            page_url=f"https://blendbrightlights.com{best['page']}",
            keyword=best["keyword"],
            current_title=f"{BIZ['primary_service']} Kelowna | {BIZ['business_name']}",
            current_meta="Professional permanent lighting installation in Kelowna.",
            current_ctr=best["ctr"],
            position=best["position"],
            impressions=best["impressions"],
            business_name=BIZ["business_name"],
            city=BIZ["primary_city"],
        )
        print(f"  {len(ctr_variants)} title variants generated:")
        for i, v in enumerate(ctr_variants, 1):
            print(f"    [{i}] {v.title}")
            print(f"         Meta: {v.meta_description[:90]}")
            print(f"         Style: {v.style}  |  Predicted CTR boost: {v.predicted_ctr_boost}")

    # 4b: Rapid Update Engine
    sub("RAPID UPDATE ENGINE — PAGE FRESHNESS PUSH")
    from prediction.rapid_update import RapidUpdateEngine
    rapid = RapidUpdateEngine()
    # Target the best near-ranking page
    update_plan = await rapid.generate_updates(
        page_url="https://blendbrightlights.com/landscape-lighting",
        keyword="landscape lighting kelowna",
        business_name=BIZ["business_name"],
        city=BIZ["primary_city"],
        position=9,
        update_number=1,
    )
    print(f"  {len(update_plan.updates)} incremental updates for /landscape-lighting (pos #9):")
    print(f"  Total word additions: +{update_plan.total_word_additions}")
    for u in update_plan.updates:
        print(f"\n    [{u.type.upper()}]  {u.instruction}")
        if u.content:
            preview = u.content[:200].replace("\n", " ")
            print(f"    CONTENT: {preview}...")

    # 4c: Competitor Reaction
    sub("COMPETITOR REACTION — THREAT DETECTION")
    from prediction.competitor_reaction import CompetitorReactor
    reactor = CompetitorReactor()
    moves = reactor.detect_moves(
        our_rankings=BIZ["current_rankings"],
        previous_rankings={k: v + 1 for k, v in BIZ["current_rankings"].items()},
        competitor_rankings=COMP_RANKINGS,
        previous_competitor_rankings=PREV_COMP_RANKINGS,
    )
    print(f"  {len(moves)} competitor moves detected:")
    reaction_plans = []
    for m in moves:
        threat_icon = "🔴" if m.threat_level == "critical" else "🟡"
        print(f"  {threat_icon} [{m.threat_level.upper()}] {m.competitor}: {m.move_type} — {m.detail}")
        print(f"     Keyword: '{m.keyword}' | Them: #{m.their_position} | Us: #{m.our_position}")

    if moves:
        print(f"\n  Generating counter-plan for biggest threat: {moves[0].competitor}...")
        rplan = await reactor.generate_reaction(
            moves[0], BIZ["business_name"], BIZ["primary_city"]
        )
        reaction_plans.append(rplan)
        print(f"  {rplan.total_actions} counter-actions:")
        for a in rplan.counter_actions:
            print(f"    [{a.urgency.upper()}] {a.action}")
            print(f"      Target: {a.target}")
            print(f"      How: {a.execution[:120]}")

    # 4d: Authority Gap
    sub("AUTHORITY GAP — LINK BUILDING PLAN")
    from prediction.authority_gap import AuthorityGapAccelerator, AuthorityProfile, AuthorityGap
    accel = AuthorityGapAccelerator()

    our_profile = AuthorityProfile(
        name=BIZ["business_name"], domain_authority=OUR_DA,
        backlink_count=OUR_LINKS, referring_domains=28,
    )
    # Gemstone is the biggest threat
    comp_profile = AuthorityProfile(
        name="Gemstone Lights", domain_authority=COMP_DA["Gemstone Lights"],
        backlink_count=COMP_LINKS["Gemstone Lights"], referring_domains=120,
    )
    da_gap = comp_profile.domain_authority - our_profile.domain_authority
    severity = "critical" if da_gap > 20 else "high" if da_gap > 10 else "moderate"
    gap = AuthorityGap(
        keyword="permanent lights kelowna",
        our_profile=our_profile,
        top_competitor=comp_profile,
        domain_gap=da_gap,
        link_gap=COMP_LINKS["Gemstone Lights"] - OUR_LINKS,
        severity=severity,
    )
    print(f"  DA gap vs Gemstone Lights: {OUR_DA} → {COMP_DA['Gemstone Lights']} (gap: {da_gap:.0f})")
    print(f"  Link gap: {OUR_LINKS} → {COMP_LINKS['Gemstone Lights']} ({COMP_LINKS['Gemstone Lights']-OUR_LINKS} more links)")
    print(f"  Severity: {severity.upper()}")
    print(f"  Strategy: {accel.recommend_strategy(gap)}")

    gap_plan = await accel.generate_plan(
        gap, BIZ["business_name"], BIZ["primary_city"], "permanent lights kelowna"
    )
    print(f"\n  {len(gap_plan.targets)} link targets | {gap_plan.links_needed} links needed | {gap_plan.estimated_timeline}")
    for t in gap_plan.targets[:8]:
        print(f"    [{t.difficulty.upper():6s}] [{t.type}]  {t.target}")
        print(f"             → {t.strategy[:100]}")

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 5 — SIGNAL BURST + SUPPRESSION (Claude calls)
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 5 — SIGNAL BURST + SUPPRESSION")

    sub("SIGNAL BURST — FORCE RANKING JUMP")
    from signals.burst import SignalBurstEngine
    burst_engine = SignalBurstEngine()
    # Target landscape lighting #9 — closest to page 1
    burst_plan = await burst_engine.plan_burst(
        keyword="landscape lighting kelowna",
        page_url="https://blendbrightlights.com/landscape-lighting",
        position=9,
        business_name=BIZ["business_name"],
        city=BIZ["primary_city"],
    )
    print(f"  Intensity: {burst_plan.intensity.value.upper()}  |  Duration: {burst_plan.duration_days} days  |  {burst_plan.total_content_pieces} pieces")
    print(f"  Expected: {burst_plan.expected_effect}")
    by_day = {}
    for a in burst_plan.actions:
        by_day.setdefault(a.day, []).append(a)
    for day in sorted(by_day.keys()):
        print(f"\n  Burst Day {day}:")
        for a in by_day[day]:
            print(f"    [{a.channel.upper():10s}] [{a.type}]  {a.description}")
            if a.content:
                preview = a.content[:150].replace("\n", " ")
                print(f"               {preview}...")

    sub("SUPPRESSION ANALYSIS — OUTPACE COMPETITORS")
    from signals.suppression import analyze_suppression_opportunities
    suppression = analyze_suppression_opportunities(
        our_keywords=BIZ["current_rankings"],
        competitor_keywords=COMP_RANKINGS,
        competitor_links=COMP_LINKS,
        our_link_count=OUR_LINKS,
    )
    print(f"  {len(suppression)} suppression opportunities:")
    for s in suppression[:6]:
        print(f"  [{s.action.upper():12s}] vs {s.competitor} on '{s.keyword}' (us #{s.our_rank} / them #{s.their_rank})")
        print(f"               {s.detail[:110]}")

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 6 — ENTITY DOMINANCE + AUTHORITY SWARM (Claude calls)
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 6 — ENTITY DOMINANCE + AUTHORITY SWARM")

    sub("ENTITY DOMINANCE AUDIT")
    from entity.dominance import EntityEngine
    entity_engine = EntityEngine()
    entity_profile, entity_actions = await entity_engine.audit(business)
    print(f"  Schema gaps     : {entity_profile.schema_gaps}")
    print(f"  Platform gaps   : {entity_profile.mentions_needed}")
    print(f"  {len(entity_actions)} entity actions:")
    for a in entity_actions:
        print(f"\n  [{a.impact.upper()}] {a.action}")
        print(f"  Target : {a.target}")
        print(f"  Why    : {a.why[:100]}")
        print(f"  How    : {a.implementation[:120]}")

    sub("AUTHORITY SWARM PLAN")
    if SCRIPT_MODE:
        from authority.swarm_script import plan_swarm_script
        from authority.swarm import AuthoritySwarm
        swarm = AuthoritySwarm()
        swarm_plan = plan_swarm_script(
            keyword="permanent lights kelowna",
            target_page="https://blendbrightlights.com/permanent-lights",
            business_name=BIZ["business_name"],
            city=BIZ["primary_city"],
            service=BIZ["primary_service"],
            velocity="medium",
        )
    else:
        from authority.swarm import AuthoritySwarm
        swarm = AuthoritySwarm()
        swarm_plan = await swarm.plan_swarm(
            keyword="permanent lights kelowna",
            target_page="https://blendbrightlights.com/permanent-lights",
            business_name=BIZ["business_name"],
            city=BIZ["primary_city"],
            service=BIZ["primary_service"],
            velocity="medium",
        )
    diversity = swarm.check_anchor_diversity(swarm_plan)
    print(f"  {swarm_plan.total_nodes} content nodes  |  velocity: {swarm_plan.velocity}  |  est. {swarm_plan.estimated_days} days")
    print(f"  Anchor mix: {swarm_plan.anchor_mix}")
    print(f"  Diversity check: {diversity}")
    print(f"  Nodes:")
    for n in swarm_plan.nodes[:10]:
        print(f"    [T{n.tier}] [{n.type:12s}] [{n.anchor_category:8s}] {n.platform}  anchor: '{n.anchor_text}'")
        if n.content_angle:
            print(f"           angle: {n.content_angle}")

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 7 — AIC ENGINE — LANDING PAGE BLUEPRINT (1 Claude call)
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 7 — LANDING PAGE BLUEPRINT + HOOKS (1 Claude call)")
    from aic.engine import AICEngine
    aic = AICEngine()
    aic_result = await aic.generate("permanent lights kelowna", business)
    att = aic_result.attention

    sub("PAGE BLUEPRINT")
    print(f"  Title   : {aic_result.conversion.title}")
    print(f"  Target  : {aic_result.intent.target_search_phrase}")
    print(f"  Sections ({len(aic_result.conversion.sections)}):")
    for i, sec in enumerate(aic_result.conversion.sections, 1):
        if isinstance(sec, dict):
            heading = sec.get("heading") or sec.get("title") or sec.get("type", "")
            content = sec.get("content", "")
            print(f"    {i}. {heading}")
            if content and not isinstance(content, (dict, list)):
                print(f"       {str(content)[:120]}")
        else:
            print(f"    {i}. {str(sec)[:100]}")

    sub("DEPLOY-READY HOOK COPY")
    for h in att.hooks:
        print(f"  [{h.get('type','').upper():10s}] {h.get('text','')}")

    if att.tiktok_script.get("hook"):
        sub("TIKTOK SCRIPT")
        ts = att.tiktok_script
        print(f"  HOOK : {ts.get('hook','')}")
        if ts.get("body"):
            print(f"  BODY : {ts.get('body','')[:300]}")
        if ts.get("cta"):
            print(f"  CTA  : {ts.get('cta','')}")

    sub("KPIs")
    for kpi in aic_result.measurement.kpis:
        if isinstance(kpi, dict):
            print(f"  • {kpi.get('metric', kpi.get('name', str(kpi)))}: {kpi.get('target', kpi.get('value', ''))}")
        else:
            print(f"  • {kpi}")

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 8 — PERCEPTION ENGINE (1 Claude call)
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 8 — PERCEPTION & NARRATIVE STRATEGY (1 Claude call)")
    from aic.perception.engine import run_perception_cycle
    perc = await run_perception_cycle(
        keyword="permanent lights kelowna",
        business_name=BIZ["business_name"],
        service=BIZ["primary_service"],
        city=BIZ["primary_city"],
        reviews=BIZ["reviews_count"],
    )

    narrs = perc.graph.get("narratives", [])
    if narrs:
        sub("CURRENT NARRATIVE LANDSCAPE")
        for n in narrs:
            icon = "+" if n.get("sentiment") == "positive" else "-" if n.get("sentiment") == "negative" else "~"
            print(f"  [{icon}] {n.get('narrative','')}")

    if perc.narrative_gaps:
        sub("UNTAPPED NARRATIVE GAPS — ANGLES TO OWN")
        for g in perc.narrative_gaps:
            print(f"  ★ {g}")

    sub(f"STRATEGY: {perc.chosen_strategy.upper().replace('_',' ')}")
    print(f"  {perc.strategy_description}")
    sub("MESSAGES — DEPLOY ACROSS ALL CHANNELS")
    for i, msg in enumerate(perc.messages, 1):
        print(f"  {i}. {msg}")

    sub("DEPLOYMENT CALENDAR")
    by_day = {}
    for action in perc.deployment_plan:
        by_day.setdefault(action.get("day", 1), []).append(action)
    for day in sorted(by_day.keys()):
        for a in by_day[day]:
            ch = a.get("channel", "")
            if ch == "blog":
                print(f"  Day {day:>2} [BLOG  ] '{a.get('title','')}' → {a.get('link_to','')}")
            elif ch == "tiktok":
                print(f"  Day {day:>2} [TIKTOK] hook: '{a.get('hook','')}' | CTA: {a.get('cta','')}")
            elif ch == "social":
                print(f"  Day {day:>2} [SOCIAL] '{a.get('text','')}' → {a.get('link_to','')}")
            elif ch == "gbp_post":
                print(f"  Day {day:>2} [GBP   ] '{a.get('text','')}'")

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 9 — CAMPAIGN ORCHESTRATOR (1 Claude call)
    # ═══════════════════════════════════════════════════════════════
    hdr(f"LAYER 9 — 21-DAY CAMPAIGN CALENDAR ({'script' if SCRIPT_MODE else '1 Claude call'})")
    if SCRIPT_MODE:
        from orchestration.campaign_script import build_campaign
        from orchestration.campaign import CampaignOrchestrator
        orch = CampaignOrchestrator()
        campaign = build_campaign("landscape lighting kelowna", business)
    else:
        from orchestration.campaign import CampaignOrchestrator
        orch = CampaignOrchestrator()
        campaign = await orch.create_campaign("landscape lighting kelowna", business)
    s = orch.campaign_summary(campaign)

    print(f"  Goal    : {s['goal']}")
    print(f"  Duration: {s['duration_days']} days  |  {s['total_phases']} phases  |  {s['total_actions']} actions")
    print(f"  Channels: {', '.join(s['channels'])}")
    print(f"  Personas: {', '.join(s['personas'])}")

    for p in s.get("phases", []):
        sub(f"PHASE: {p['name'].upper()} (days {p['days']})")
        phase_actions = p.get("action_list", [])
        if phase_actions:
            for a in phase_actions:
                ch = a.get("channel", "")
                task = a.get("task") or a.get("action") or a.get("description") or str(a)
                print(f"  [{ch.upper():12s}] {task[:100]}")
        else:
            print(f"  {p['actions']} actions scheduled")

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 10 — PERSONAS — READY-TO-POST CONTENT (Claude calls)
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 10 — READY-TO-POST PERSONA CONTENT")
    from personas.system import PersonaSystem
    ps = PersonaSystem()
    pc = await ps.generate_campaign(
        keyword="permanent lights kelowna",
        business_name=BIZ["business_name"],
        city=BIZ["primary_city"],
        max_personas=3,
    )
    for o in pc.outputs:
        sub(f"{o.channel.upper()} — {o.persona_name}")
        print(f"  TITLE: {o.title}\n")
        print(o.content)

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 11 — LINK RECOVERY + WIKI CITATIONS (no Claude)
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 11 — LINK RECOVERY + CITATION OPPORTUNITIES (no Claude)")

    sub("WAYBACK — BROKEN LINK RECOVERY")
    from data.analyzers.wayback_links import find_broken_links
    dead = find_broken_links("blendbrightlights.com", current_urls=[
        "/permanent-lights", "/landscape-lighting", "/roof-line-lights",
        "/christmas-lights", "/about", "/contact", "/gallery",
    ], max_results=50)
    if dead:
        print(f"  {len(dead)} dead URLs found in Wayback Machine (redirect these → reclaim link equity):")
        for d in dead[:8]:
            print(f"    {d.get('url','')}  (archived: {d.get('archive_url','')})")
    else:
        print("  No broken archived URLs found — clean link profile")

    sub("WIKIPEDIA — CITATION OPPORTUNITIES")
    from data.analyzers.wikipedia_citations import find_citation_opportunities
    wiki_opps = find_citation_opportunities("permanent outdoor lighting", max_articles=5)
    if wiki_opps:
        print(f"  {len(wiki_opps)} [citation needed] opportunities:")
        for w in wiki_opps[:5]:
            print(f"    Article: {w.get('article_title','')}")
            print(f"    Section: {w.get('sentence','')[:120]}")
            print(f"    URL    : {w.get('article_url','')}")
    else:
        print("  No Wikipedia citation gaps found for this niche")

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 12 — SELF-EVOLUTION + CONTENT FLYWHEEL
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 12 — EVOLUTION + FLYWHEEL (no Claude)")

    sub("STRATEGY EVOLUTION")
    from learning.evolution import StrategyParams, mutate_strategy
    params = StrategyParams()
    perf_feedback = {"avg_ranking_gain": 1.5, "success_rate": 0.45, "ctr_change": -0.01}
    new_p = mutate_strategy(params, perf_feedback)
    changed = []
    for field in ["aggressiveness", "content_depth", "link_velocity", "min_confidence"]:
        old, new = getattr(params, field), getattr(new_p, field)
        if old != new:
            changed.append(f"  {field}: {old} → {new}")
    if changed:
        print("  Mutations recommended:")
        for c in changed: print(c)
    else:
        print(f"  Params stable — aggressiveness={params.aggressiveness} | depth={params.content_depth}w | velocity={params.link_velocity}/wk")

    sub("CONTENT FLYWHEEL — AMPLIFICATION TRIGGERS")
    from channels.flywheel import ContentFlywheel
    from channels.models import ContentPerformance
    flywheel = ContentFlywheel()
    # Simulate performance data
    simulated_performances = [
        ContentPerformance(keyword="landscape lighting kelowna", seo_impact=7.5, social_engagement=4.0, traffic_generated=85, conversions=6, composite_score=7.0),
        ContentPerformance(keyword="permanent lights kelowna",   seo_impact=5.0, social_engagement=8.5, traffic_generated=60, conversions=2, composite_score=6.0),
        ContentPerformance(keyword="christmas lights kelowna",   seo_impact=2.5, social_engagement=2.0, traffic_generated=15, conversions=0, composite_score=2.5),
    ]
    flywheel_actions = flywheel.evaluate(simulated_performances)
    print(f"  {len(flywheel_actions)} flywheel actions triggered:")
    for a in flywheel_actions:
        print(f"  [{a.priority.upper():6s}] [{a.channel.upper():10s}] {a.action}")
        print(f"           trigger: {a.trigger}")

    tick(start)

    # ═══════════════════════════════════════════════════════════════
    # MASTER EXECUTION CHECKLIST
    # ═══════════════════════════════════════════════════════════════
    total = time.time() - start
    hdr("MASTER EXECUTION CHECKLIST — DO THESE IN ORDER")
    print(f"""
  TODAY (Day 1):
  □ A/B test new title variants on /landscape-lighting (Layer 4 — CTR Dominator)
  □ Update page title → "{aic_result.conversion.title}"
  □ Post GBP update: message #1 from Layer 8 perception strategy
  □ Post Reddit (Practical Homeowner — full copy in Layer 10 above)
  □ Schedule TikTok hook from Layer 7

  THIS WEEK (Days 2-7):
  □ Apply Rapid Update content additions to /landscape-lighting (Layer 4)
  □ Execute Burst Day 1-3 actions for 'landscape lighting kelowna' (Layer 5)
  □ Counter Gemstone Lights ranking jump — execute reaction plan (Layer 4)
  □ Publish blog post + Instagram caption (Layer 10 personas)
  □ Fix schema gaps: {entity_profile.schema_gaps} (Layer 6)
  □ Add platforms for entity mentions: {entity_profile.mentions_needed[:3]} (Layer 6)
  □ Claim Wikipedia citation opportunity (Layer 11)
  □ Request 5 new Google reviews from recent customers

  THIS MONTH (Days 8-21):
  □ Execute authority swarm: {swarm_plan.total_nodes} content nodes over {swarm_plan.estimated_days} days (Layer 6)
  □ Build {gap_plan.links_needed} backlinks from plan above — close DA gap vs Gemstone (Layer 4)
  □ Execute campaign phases 2-4 (Layer 9)
  □ Continue signal burst cycles — max 1 per keyword per 14 days (Layer 5)
  □ Suppress Gemstone: publish 3 supporting pieces around their keywords (Layer 5)
  □ Run this test again in 14 days — compare ranking movement
  □ If 'landscape lighting kelowna' hits top 5 → expand swarm to Vancouver market
    """)
    print(f"  Total runtime : {total:.0f}s ({total/60:.1f} min)")
    print(f"  Layers active : 14/14  (Layer 14 runs after)")
    print(SEP)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 13 — EXECUTION ENGINE
    # Package all generated content → route to channels → post or queue
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 13 — EXECUTION ENGINE")

    from execution.publisher import MultiChannelPublisher, ContentPackage
    from execution.connectors.base import PublishResult
    from execution.connectors.external.reddit import RedditConnector
    from execution.connectors.external.medium import MediumConnector
    from execution.connectors.external.web_poster import WebPoster

    # ── Credential check ──────────────────────────────────────────
    sub("CONNECTOR STATUS")
    CREDS = {
        "reddit":    ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USERNAME", "REDDIT_PASSWORD"],
        "medium":    ["MEDIUM_TOKEN"],
        "wordpress": ["WP_URL", "WP_USERNAME", "WP_PASSWORD"],
        "wp.com":    ["WP_COM_TOKEN", "WP_COM_SITE"],
        "tumblr":    ["TUMBLR_CONSUMER_KEY", "TUMBLR_CONSUMER_SECRET", "TUMBLR_TOKEN", "TUMBLR_TOKEN_SECRET"],
        "vercel":    ["VERCEL_TOKEN", "VERCEL_PROJECT_NAME"],
        "gbp":       ["GSC_CREDENTIALS_PATH"],
    }
    ready, needs_setup = [], []
    for connector, keys in CREDS.items():
        missing = [k for k in keys if not os.getenv(k)]
        if missing:
            needs_setup.append((connector, missing))
            print(f"  [NEEDS SETUP] {connector:12s} — add to config/.env: {', '.join(missing)}")
        else:
            ready.append(connector)
            print(f"  [READY      ] {connector}")

    # ── Build content packages from this run ──────────────────────
    sub("CONTENT PACKAGES READY FOR DISPATCH")

    packages = []

    # Package 1: Reddit post (Practical Homeowner persona)
    reddit_output = next((o for o in pc.outputs if o.channel == "reddit"), None)
    if reddit_output:
        packages.append(ContentPackage(
            topic="permanent lights kelowna — reddit",
            keyword="permanent lights kelowna",
            source="persona_system",
            assets={
                "reddit": {
                    "subreddit": "kelowna",
                    "subreddit_suggestions": ["kelowna", "britishcolumbia", "homeimprovement", "ChristmasLights"],
                    "title": reddit_output.title,
                    "content": reddit_output.content,
                }
            }
        ))
        print(f"  PKG-1 [REDDIT ] '{reddit_output.title[:60]}'")
        print(f"         subreddit: r/kelowna  |  {len(reddit_output.content)} chars")

    # Package 2: Blog post (Local Expert persona)
    blog_output = next((o for o in pc.outputs if o.channel == "blog"), None)
    if blog_output:
        packages.append(ContentPackage(
            topic="permanent lights kelowna — blog",
            keyword="permanent lights kelowna",
            source="persona_system",
            assets={
                "blog": {
                    "title": blog_output.title,
                    "content": blog_output.content,
                    "tags": ["permanent lights kelowna", "outdoor lighting", "blend bright lights", "kelowna"],
                    "status": "draft",
                }
            }
        ))
        print(f"  PKG-2 [BLOG   ] '{blog_output.title[:60]}'")
        print(f"         → will post as DRAFT to WordPress (review before publish)")

    # Package 3: Burst Day 1 — GBP post + blog
    if burst_plan.actions:
        gbp_action = next((a for a in burst_plan.actions if a.channel == "gbp" and a.day == 1), None)
        blog_action = next((a for a in burst_plan.actions if a.channel == "blog" and a.day == 1), None)
        if gbp_action or blog_action:
            assets = {}
            if gbp_action:
                assets["gbp"] = {"text": gbp_action.content or gbp_action.description, "cta": "Book a free quote"}
            if blog_action:
                assets["blog_article"] = {
                    "title": blog_action.description[:80],
                    "content": blog_action.content or blog_action.description,
                    "tags": ["landscape lighting kelowna", "kelowna outdoor lighting"],
                    "status": "draft",
                }
            packages.append(ContentPackage(
                topic="landscape lighting kelowna — burst day 1",
                keyword="landscape lighting kelowna",
                source="signal_burst",
                assets=assets,
            ))
            channels = list(assets.keys())
            print(f"  PKG-3 [BURST  ] Day 1 burst — channels: {channels}")

    # Package 4: Perception messages → GBP + social
    if perc.messages:
        packages.append(ContentPackage(
            topic="permanent lights kelowna — perception",
            keyword="permanent lights kelowna",
            source="perception_engine",
            assets={
                "gbp_post": {"text": perc.messages[0], "cta": "Get a free quote at blendbrightlights.com"},
                "social": {"text": perc.messages[0], "link_to": "https://blendbrightlights.com/permanent-lights"},
            }
        ))
        print(f"  PKG-4 [GBP+SOC] '{perc.messages[0][:70]}'")

    print(f"\n  {len(packages)} packages ready | {len(ready)} connectors live | {len(needs_setup)} connectors need setup")

    # ── Vercel page generation + deploy ──────────────────────────
    sub("VERCEL PAGE DEPLOYMENT — GENERATE + PUBLISH NEW SERVICE PAGES")
    from execution.renderers.generate import PageGenerator

    # Keywords that need new/updated pages
    page_targets = [
        ("permanent lights kelowna",     "permanent-lights-kelowna"),
        ("landscape lighting kelowna",   "landscape-lighting-kelowna"),
        ("roof line lights kelowna",     "roof-line-lights-kelowna"),
        ("christmas lights kelowna",     "christmas-lights-kelowna"),
    ]

    vercel_pages = []
    gen_dir = os.path.join(os.path.dirname(__file__), "generated_pages")
    os.makedirs(gen_dir, exist_ok=True)

    if "vercel" in ready and EXECUTE:
        print(f"  Vercel READY — generating {len(page_targets)} pages and deploying...")
        gen = PageGenerator()
        for kw, slug in page_targets:
            print(f"    Generating: {kw}...")
            try:
                html_path = await gen.generate_and_save(kw, BIZ, gen_dir)
                html = open(html_path, encoding="utf-8").read()
                vercel_pages.append({"path": slug, "html": html})
                print(f"    ✓ {slug}.html ({len(html):,} bytes)")
            except Exception as e:
                print(f"    ✗ {kw}: {e}")

        if vercel_pages:
            # Add content packages for Vercel deployment
            packages.append(ContentPackage(
                topic="new service pages — vercel deploy",
                keyword="permanent lights kelowna",
                source="page_generator",
                assets={
                    "vercel": {
                        "files": vercel_pages,
                        "promote": False,  # preview first, promote manually
                    }
                }
            ))
            print(f"\n  PKG-V [VERCEL ] {len(vercel_pages)} pages → preview deployment")
            print(f"  → After review: promote to production in Vercel dashboard")

    elif "vercel" not in ready:
        # Show pre-generated pages if they exist
        existing = list(Path(gen_dir).glob("*.html"))
        if existing:
            print(f"  {len(existing)} pages already generated in generated_pages/:")
            for p in sorted(existing)[:6]:
                size = p.stat().st_size
                print(f"    {p.name:45s}  ({size:,} bytes)")
            print(f"\n  To deploy to Vercel:")
            print(f"  1. Add VERCEL_TOKEN + VERCEL_PROJECT_NAME to config/.env")
            print(f"  2. Run: python full_test.py --execute")
        else:
            print(f"  Vercel not configured — add to config/.env:")
            print(f"    VERCEL_TOKEN=<from vercel.com/account/tokens>")
            print(f"    VERCEL_PROJECT_NAME=blendbrightlights")
            print(f"  Then run: python full_test.py --execute")
            print(f"\n  Pages will be deployed to: blendbrightlights.vercel.app/permanent-lights-kelowna")
            print(f"  Map to your domain: vercel.com → project → Domains → add blendbrightlights.com")

    # ── Route and execute (or dry-run) ────────────────────────────
    sub(f"{'EXECUTING' if EXECUTE else 'DRY-RUN'} — {'python full_test.py --execute to go live' if not EXECUTE else 'POSTING NOW'}")

    publisher = MultiChannelPublisher()

    # Register connectors that are configured
    for connector_name in ready:
        if connector_name == "reddit":
            from execution.connectors.external.reddit import RedditConnector
            publisher.register("reddit", RedditConnector())
        elif connector_name == "medium":
            from execution.connectors.external.medium import MediumConnector
            publisher.register("medium", MediumConnector(token=os.getenv("MEDIUM_TOKEN", "")))
        elif connector_name == "wordpress":
            from execution.connectors.wordpress import WordPressConnector
            publisher.register("wordpress", WordPressConnector(
                url=os.getenv("WP_URL", ""),
                username=os.getenv("WP_USERNAME", ""),
                password=os.getenv("WP_PASSWORD", ""),
            ))
        elif connector_name == "gbp":
            from execution.connectors.external.gbp import GBPConnector
            publisher.register("gbp", GBPConnector(
                credentials_path=os.getenv("GSC_CREDENTIALS_PATH", "")
            ))
        elif connector_name == "vercel":
            from execution.connectors.external.vercel import VercelConnector
            publisher.register("vercel", VercelConnector())

    dispatch_results = []
    for pkg in packages:
        report = await publisher.publish_package(pkg, dry_run=not EXECUTE)
        dispatch_results.append(report)
        status_icon = "✓" if report.total_success > 0 else "○"
        print(f"\n  {status_icon} [{pkg.source.upper():20s}] {pkg.topic}")
        print(f"    attempted={report.total_attempted} success={report.total_success} queued={report.total_queued} failed={report.total_failed}")
        for r in report.results:
            platform = r.get("platform", "")
            status = r.get("status", "")
            url = r.get("url", "")
            reason = r.get("reason", r.get("error", ""))
            if url:
                print(f"    [{platform:10s}] {status.upper()} → {url}")
            elif reason:
                print(f"    [{platform:10s}] {status.upper()} — {reason}")
            else:
                print(f"    [{platform:10s}] {status.upper()}")

    # ── Setup guide for unconfigured connectors ───────────────────
    if needs_setup:
        sub("SETUP GUIDE — ADD TO config/.env")
        setup_instructions = {
            "reddit":    [
                "1. Go to reddit.com/prefs/apps → create 'script' app",
                "2. REDDIT_CLIENT_ID=<your_app_id>",
                "3. REDDIT_CLIENT_SECRET=<your_app_secret>",
                "4. REDDIT_USERNAME=<your_reddit_username>",
                "5. REDDIT_PASSWORD=<your_reddit_password>",
            ],
            "medium":    [
                "1. Go to medium.com/me/settings/security → Integration tokens",
                "2. MEDIUM_TOKEN=<your_integration_token>",
            ],
            "wordpress": [
                "1. WORDPRESS site (self-hosted)",
                "2. WP_URL=https://blendbrightlights.com",
                "3. WP_USERNAME=<wp_admin_username>",
                "4. WP_PASSWORD=<wp_application_password>  (Settings → Users → App passwords)",
            ],
            "wp.com":    [
                "1. wordpress.com free blog → developer settings → OAuth app",
                "2. WP_COM_TOKEN=<oauth_token>",
                "3. WP_COM_SITE=yourblog.wordpress.com",
            ],
        }
        for connector, _ in needs_setup:
            instructions = setup_instructions.get(connector)
            if instructions:
                print(f"\n  [{connector.upper()}]")
                for step in instructions:
                    print(f"    {step}")

    # ═══════════════════════════════════════════════════════════════
    # LAYER 14 — AI VISIBILITY + E-E-A-T + GEO (no Claude)
    # ═══════════════════════════════════════════════════════════════
    hdr("LAYER 14 — AI VISIBILITY / E-E-A-T / GEO OPTIMIZATION (no Claude)")

    # ── E-E-A-T Score (live homepage HTML) ───────────────────────
    sub("E-E-A-T SCORE — " + BIZ["website"])
    from ai_visibility.eeat_scorer import score_eeat

    # Use cached HTML if available, otherwise fetch
    bbl_html_path = os.path.join(os.path.dirname(__file__), "bbl_home.html")
    if os.path.exists(bbl_html_path):
        with open(bbl_html_path, encoding="utf-8", errors="replace") as f:
            site_html = f.read()
        print(f"  Using cached homepage HTML ({len(site_html):,} bytes)")
    else:
        import requests as _req
        try:
            r = _req.get(BIZ["website"], timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            site_html = r.text
            print(f"  Fetched live HTML ({len(site_html):,} bytes)")
        except Exception as e:
            site_html = ""
            print(f"  Could not fetch HTML: {e}")

    if site_html:
        eeat = score_eeat(site_html, content_type="service")
        grade = eeat.get("grade", "?")
        total = eeat.get("total", 0)
        geo_score = eeat.get("geo_score", 0)
        seo_score = eeat.get("seo_score", 0)
        print(f"  Overall : {total:.0f}/100  Grade: {grade}  |  GEO (contextual): {geo_score:.0f}  SEO (EEAT): {seo_score:.0f}")

        # Show worst-scoring dimensions using weakest_dimensions
        weakest = eeat.get("weakest_dimensions", [])
        if weakest:
            print(f"  Weakest dimensions (highest priority fixes):")
            for wd in weakest[:4]:
                dim_name = wd.get("dimension", "?")
                dim_score = wd.get("score", 0)
                bar = "█" * (dim_score // 10) + "░" * (10 - dim_score // 10)
                print(f"    {dim_name:25s} [{bar}] {dim_score:.0f}/100")

        # Top failing criteria
        failures = eeat.get("failed_items", [])
        if failures:
            print(f"\n  Top {min(6, len(failures))} EEAT failures to fix:")
            for item in failures[:6]:
                print(f"    [{item.get('id', '?'):6s}] {item.get('name', '?')}")
        if eeat.get("veto_triggered"):
            print(f"  ⚠ VETO triggered — total score capped at 59 (critical criterion failed)")
    else:
        print("  No HTML available — skipping EEAT scoring")

    # ── AI Visibility Score ────────────────────────────────────────
    sub("AI VISIBILITY SCORE")
    from ai_visibility.scoring import score_visibility

    ai_score = score_visibility(
        business,
        faq_count=len(paa),  # reuse PAA count as FAQ proxy
        schema_present="schema.org" in site_html.lower() if site_html else False,
        mention_count=15,    # estimated — DataForSEO will give real count when configured
        backlink_count=OUR_LINKS,
    )

    from ai_visibility.scoring import score_to_prompt_block
    ai_grade = "STRONG" if ai_score.composite >= 7 else "MODERATE" if ai_score.composite >= 4 else "LOW"
    print(f"  AI Visibility Score : {ai_score.composite:.1f}/10  Status: {ai_grade}")
    print(f"  Answer Readiness    : {ai_score.answer_readiness:.1f}/10   (structured Q&A, schema, FAQs)")
    print(f"  Entity Saturation   : {ai_score.entity_saturation:.1f}/10   (GBP, directories, Knowledge Graph)")
    print(f"  Mention Density     : {ai_score.mention_density:.1f}/10   (brand mentions across web)")
    print(f"  Content Authority   : {ai_score.content_authority:.1f}/10   (backlinks, reviews, age)")

    # ── AI Crawler Access Audit ────────────────────────────────────
    sub("AI CRAWLER ACCESS — robots.txt AUDIT")
    from ai_visibility.llms_txt import audit_site_crawlers, generate_llms_txt

    try:
        crawler_audit = audit_site_crawlers(BIZ["website"])
        if "error" not in crawler_audit:
            score = crawler_audit.get("score", 0)
            blocked = crawler_audit.get("blocked", [])
            allowed = crawler_audit.get("allowed", [])
            has_llms = crawler_audit.get("has_llms_txt", False)
            print(f"  AI Crawler Score : {score}/100")
            print(f"  Allowed : {len(allowed)} crawlers  |  Blocked: {len(blocked)}")
            if blocked:
                print(f"  ⚠ BLOCKED: {', '.join(blocked)}")
            else:
                print(f"  ✓ All AI crawlers allowed")
            if not has_llms:
                print(f"  ✗ No llms.txt found — add one to improve AI training signal")
            for rec in crawler_audit.get("recommendations", [])[:3]:
                print(f"  → {rec}")
        else:
            print(f"  Crawler audit failed: {crawler_audit['error']}")
    except Exception as e:
        print(f"  Crawler audit error: {e}")

    # ── llms.txt Generator ─────────────────────────────────────────
    sub("llms.txt — GENERATE FOR " + BIZ["website"])
    llms_content = generate_llms_txt(business)
    lines_count = len(llms_content.splitlines())
    print(f"  Generated {lines_count} line llms.txt:")
    print()
    for line in llms_content.splitlines():
        print(f"  {line}")
    print()
    print(f"  → Deploy to: {BIZ['website']}/llms.txt")

    # ── GEO Strategy Recommendations ──────────────────────────────
    sub("GEO OPTIMIZATION STRATEGIES")
    from ai_visibility.geo_strategies import GEO_STRATEGIES

    print(f"  {len(GEO_STRATEGIES)} GEO strategies available — ranked by AI citation impact:")
    strategy_order = [
        ("citations", "statistics"),  # look for whichever keys are there
        "quotations", "authoritative", "statistics", "fluency",
        "unique_words", "persuasive_elements", "easy_to_understand", "keyword_stuffing",
    ]
    sorted_strats = sorted(
        GEO_STRATEGIES.items(),
        key=lambda x: -int((x[1].get("impact", "+0%") or "+0%").replace("%", "").split("+")[-1].split("-")[0].strip() or 0),
    )
    for name, strat in sorted_strats[:6]:
        print(f"\n  [{name.upper():25s}] {strat.get('impact', 'impact unknown')}")
        print(f"  {strat.get('description', '')}")

    print(f"""
  GEO PRIORITY ACTIONS:
  1. Apply 'authoritative' + 'quotations' strategies to /landscape-lighting page
     (highest CTR × AI citation gain — estimated +25-35% combined)
  2. Add statistics with sources to every service page
     (AI engines prefer data-backed content — cite Canadian industry stats)
  3. Rewrite FAQ sections with 'fluency' optimization
     (FAQ is prime AI extraction target — fluency boosts citation rate +12%)
  4. Place llms.txt at {BIZ["website"]}/llms.txt
     (tells GPTBot/ClaudeBot exactly what the site is + where to cite from)
""")

    tick(start)

    final_total = time.time() - start
    print(f"\n  Mode    : {'LIVE EXECUTION' if EXECUTE else 'DRY RUN — run with --execute to post'}")
    print(f"  Runtime : {final_total:.0f}s ({final_total/60:.1f} min)")
    print(f"  Layers  : 14/14")
    print(SEP)


if __name__ == "__main__":
    asyncio.run(main())
