"""
FIRST RANKING WIN — Focused attack on ONE keyword.

This script:
  1. Runs aggressive analysis for your business
  2. Identifies the best keyword to attack (position 5-15)
  3. Generates the full "Ranking Push Stack":
     - Content upgrade
     - Title/meta CTR optimization
     - Internal link suggestions
     - GBP post targeting the keyword
     - SERP hijack cluster plan
  4. Outputs everything as ready-to-deploy content

Usage:
  python ranking_win.py                    # Run with demo business
  python ranking_win.py --business mine    # Edit BUSINESS below with your real data

Then: take the output and apply it to your actual website.
"""

import asyncio
import json
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

from core.claude import call_claude, get_mode
from core.agents.brain import SEOBrain
from core.scoring.engine import score_and_rank
from prediction.ctr import CTRDominator
from prediction.serp_hijack import SERPHijacker
from prediction.rapid_update import RapidUpdateEngine
from execution.handlers.gbp import GBPHandler
from execution.handlers.website import WebsiteHandler
from models.business import BusinessContext


# =====================================================================
# YOUR BUSINESS — EDIT THIS WITH YOUR REAL DATA
# =====================================================================

BUSINESS = BusinessContext(
    business_name="Blend Bright Lights",
    website="https://blendbright.com",
    gbp_url="",
    years_active=5,
    primary_service="Landscape Lighting",
    secondary_services=["Outdoor Lighting", "Holiday Lighting", "Residential Lighting"],
    primary_city="Kelowna",
    service_areas=["Kelowna", "West Kelowna", "Lake Country", "Peachland"],
    target_customer="Homeowners",
    avg_job_value=2500,
    primary_keywords=[
        "landscape lighting kelowna",
        "outdoor lighting kelowna",
        "residential lighting kelowna",
    ],
    current_rankings={
        "landscape lighting kelowna": 9,
        "outdoor lighting kelowna": 12,
    },
    missing_keywords=[
        "christmas lighting kelowna",
        "garden lighting kelowna",
        "patio lighting kelowna",
    ],
    reviews_count=45,
    rating=4.9,
    monthly_traffic=800,
    gbp_views=3200,
    competitors=["Okanagan Lighting", "BC Outdoor Lights", "Kelowna Landscape Pros"],
)

# The ONE keyword to attack
TARGET_KEYWORD = "landscape lighting kelowna"
TARGET_POSITION = 9
TARGET_PAGE = f"{BUSINESS.website}/landscape-lighting-kelowna"

BUSINESS_ID = "blendbright"


# =====================================================================
# RANKING WIN EXECUTION
# =====================================================================

def banner(text: str):
    print(f"\n{'='*70}")
    print(f"  {text}")
    print(f"{'='*70}\n")


async def main():
    banner(f"RANKING WIN PLAN — {TARGET_KEYWORD}")
    print(f"  Business: {BUSINESS.business_name}")
    print(f"  Target: {TARGET_KEYWORD} (currently #{TARGET_POSITION})")
    print(f"  Goal: Top 3 within 14-21 days")
    print(f"  Claude mode: {get_mode().upper()}")
    print()

    results = {}

    # ---- STEP 1: Aggressive Analysis ----
    banner("STEP 1 — AGGRESSIVE ANALYSIS")
    brain = SEOBrain()
    batch = await brain.analyze(BUSINESS, input_type="FULL", max_actions=5)
    print(f"  Tasks generated: {len(batch.tasks)} (filtered: {batch.filtered_count})")
    for t in batch.tasks:
        print(f"  #{t.priority_rank} [{t.type.value}] {t.action}")
        print(f"     Score: {t.total_score:.1f} | Impact: {t.impact_score} | Speed: {t.speed_score} | Confidence: {t.confidence_score}")
        print(f"     Result: {t.estimated_result}")
        print()
    results["analysis"] = [t.model_dump() for t in batch.tasks]

    # ---- STEP 2: CTR Optimization ----
    banner("STEP 2 — CTR TITLE VARIANTS")
    ctr = CTRDominator()
    variants = await ctr.generate_variants(
        page_url=TARGET_PAGE,
        keyword=TARGET_KEYWORD,
        current_title=f"Landscape Lighting Kelowna | {BUSINESS.business_name}",
        current_meta="Professional landscape lighting services in Kelowna.",
        current_ctr=0.025,
        position=TARGET_POSITION,
        impressions=400,
        business_name=BUSINESS.business_name,
        city=BUSINESS.primary_city,
        reviews=BUSINESS.reviews_count,
    )
    for i, v in enumerate(variants):
        print(f"  Variant {i+1} [{v.style}]:")
        print(f"    Title: {v.title}")
        print(f"    Meta:  {v.meta_description}")
        print(f"    Boost: {v.predicted_ctr_boost}")
        print()
    results["ctr_variants"] = [v.model_dump() for v in variants]

    # ---- STEP 3: SERP Hijack Cluster ----
    banner("STEP 3 — SERP HIJACK CLUSTER")
    hijacker = SERPHijacker()
    cluster = await hijacker.plan_cluster(
        keyword=TARGET_KEYWORD,
        business_name=BUSINESS.business_name,
        service=BUSINESS.primary_service,
        city=BUSINESS.primary_city,
        current_position=TARGET_POSITION,
    )
    if cluster.main_page:
        print(f"  Main page: {cluster.main_page.title}")
        print(f"    Slug: /{cluster.main_page.url_slug}")
        print(f"    Words: {cluster.main_page.word_count_target}")
    for p in cluster.supporting_pages:
        print(f"  Support: {p.title}")
        print(f"    Slug: /{p.url_slug} | Angle: {p.angle}")
    if cluster.authority_page:
        print(f"  Authority: {cluster.authority_page.title}")
        print(f"    Slug: /{cluster.authority_page.url_slug}")
    print(f"  Internal links: {len(cluster.link_plan)}")
    for link in cluster.link_plan[:5]:
        print(f"    /{link.from_slug} -> /{link.to_slug} [{link.anchor_text}]")
    results["serp_cluster"] = cluster.model_dump()

    # ---- STEP 4: Rapid Update Plan ----
    banner("STEP 4 — RAPID UPDATE PLAN")
    updater = RapidUpdateEngine()
    update_plan = await updater.generate_updates(
        page_url=TARGET_PAGE,
        keyword=TARGET_KEYWORD,
        business_name=BUSINESS.business_name,
        city=BUSINESS.primary_city,
        position=TARGET_POSITION,
        update_number=1,
    )
    for u in update_plan.updates:
        print(f"  [{u.type}] {u.instruction}")
        if u.content:
            preview = u.content[:200].replace("\n", " ")
            print(f"    Content: {preview}...")
        print()
    results["rapid_updates"] = update_plan.model_dump()

    # ---- STEP 5: GBP Post ----
    banner("STEP 5 — GBP POST (targeting keyword)")
    gbp = GBPHandler()
    gbp_result = await gbp.create_post("win-gbp", BUSINESS)
    if gbp_result.status.value == "success":
        post = gbp_result.output
        print(f"  Post text: {post.get('text', post.get('content', {}).get('text', ''))[:300]}")
        print(f"  CTA: {post.get('cta', post.get('content', {}).get('cta', ''))}")
    results["gbp_post"] = gbp_result.output

    # ---- STEP 6: Service Page Content ----
    banner("STEP 6 — SERVICE PAGE CONTENT")
    web = WebsiteHandler()
    page_result = await web.create_page("win-page", TARGET_KEYWORD, f"Build {TARGET_KEYWORD} page", BUSINESS)
    if page_result.status.value == "success":
        page = page_result.output
        print(f"  Title: {page.get('title', '')}")
        print(f"  Meta: {page.get('meta_description', '')}")
        print(f"  H1: {page.get('h1', '')}")
        if page.get("faqs"):
            print(f"  FAQs: {len(page['faqs'])}")
            for faq in page["faqs"][:2]:
                print(f"    Q: {faq.get('question', '')}")
    results["service_page"] = page_result.output

    # ---- SAVE RESULTS ----
    banner("COMPLETE — RANKING WIN PLAN SAVED")
    output_path = "ranking_win_output.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"  Full output saved to: {output_path}")
    print()

    # ---- ACTION CHECKLIST ----
    banner("14-DAY ACTION CHECKLIST")
    print("  DAY 1-2:")
    print("    [ ] Apply best CTR title variant to your page")
    print("    [ ] Update/create service page with generated content")
    print("    [ ] Add FAQ schema from generated FAQs")
    print("    [ ] Publish GBP post")
    print()
    print("  DAY 2-3:")
    print("    [ ] Add 3-5 internal links to target page from other pages")
    print("    [ ] Apply rapid update #1 (content additions)")
    print("    [ ] Submit URL to Google Search Console for re-indexing")
    print()
    print("  DAY 3-7:")
    print("    [ ] Create 1 supporting blog post from SERP cluster")
    print("    [ ] Build 2-3 backlinks to target page")
    print("    [ ] Publish 2nd GBP post")
    print()
    print("  DAY 7-10:")
    print("    [ ] Apply rapid update #2")
    print("    [ ] Create 2nd supporting blog post")
    print("    [ ] Check GSC for ranking movement")
    print()
    print("  DAY 10-14:")
    print("    [ ] Apply rapid update #3")
    print("    [ ] Publish 3rd GBP post")
    print("    [ ] Measure: position, CTR, impressions")
    print()
    print("  SUCCESS CRITERIA:")
    print(f"    Position #{TARGET_POSITION} -> #5 or better")
    print(f"    If not moving by day 10: add +2 backlinks, +300 words content")
    print()
    print(f"  All generated content is in: {output_path}")
    print(f"  Copy-paste ready. Go execute.")


if __name__ == "__main__":
    asyncio.run(main())
