"""Script SEO Brain — generates ranked tasks from rules + data, zero LLM calls.

Replaces SEOBrain.analyze() for cases where speed matters or Claude is unavailable.
Uses the same scoring engine and SEOTask model as the LLM brain.

Task generation logic:
  - ranking gap → page optimization priority
  - DA/link gap → authority building priority
  - CTR below threshold → title/meta optimization
  - missing schema → technical priority
  - review count → GBP priority
"""

from __future__ import annotations

import uuid
import logging

from core.scoring.engine import score_and_rank
from models.business import BusinessContext
from models.task import SEOTask, TaskBatch, ImpactLevel, TaskType, ExecutionMode

log = logging.getLogger(__name__)


# Position buckets → action urgency
def _pos_urgency(pos: int) -> tuple[str, ImpactLevel, float]:
    """Return (action_verb, impact_level, confidence) based on current position."""
    if pos <= 3:
        return "defend", ImpactLevel.MEDIUM, 6.0
    if pos <= 5:
        return "push_to_top3", ImpactLevel.HIGH, 8.0
    if pos <= 10:
        return "push_to_page1", ImpactLevel.HIGH, 7.5
    if pos <= 20:
        return "optimize", ImpactLevel.MEDIUM, 6.5
    return "build_presence", ImpactLevel.LOW, 5.0


def _da_gap_severity(our_da: int, their_da: int) -> str:
    gap = their_da - our_da
    if gap > 20: return "critical"
    if gap > 10: return "high"
    if gap > 5:  return "moderate"
    return "low"


def generate_tasks(
    business: BusinessContext,
    our_da: int = 0,
    our_links: int = 0,
    comp_da: dict[str, int] | None = None,
    comp_links: dict[str, int] | None = None,
    gsc_data: list[dict] | None = None,
    ctr_threshold: float = 0.035,
    max_tasks: int = 5,
) -> TaskBatch:
    """Generate scored SEO tasks from business data — no LLM needed.

    Args:
        business: BusinessContext with rankings, keywords, etc.
        our_da: Our domain authority estimate
        our_links: Our backlink count
        comp_da: {competitor_name: da} dict
        comp_links: {competitor_name: link_count} dict
        gsc_data: List of GSC rows [{keyword, position, impressions, ctr, page}]
        ctr_threshold: CTR below this triggers a title test task
        max_tasks: Max tasks to return
    """
    comp_da = comp_da or {}
    comp_links = comp_links or {}
    gsc_data = gsc_data or []
    run_id = uuid.uuid4().hex[:8]

    tasks: list[SEOTask] = []

    # ── 1. Ranking gap tasks (one per keyword, prioritized by position) ──
    rankings = business.current_rankings or {}
    sorted_kws = sorted(rankings.items(), key=lambda x: x[1])  # lowest pos first

    for kw, pos in sorted_kws[:4]:
        verb, impact, conf = _pos_urgency(pos)

        # Find matching GSC row for impressions
        gsc_row = next((r for r in gsc_data if r.get("keyword") == kw), {})
        impressions = gsc_row.get("impressions", 200)

        # Higher impressions = higher impact
        if impressions > 500:
            impact = ImpactLevel.HIGH
            conf = min(9.0, conf + 0.5)
        elif impressions < 100:
            impact = ImpactLevel.MEDIUM
            conf = max(4.0, conf - 1.0)

        # Page slug from GSC or infer from keyword
        page = gsc_row.get("page", f"/{kw.lower().replace(' ', '-')}")

        if verb == "push_to_page1" or verb == "push_to_top3":
            tasks.append(SEOTask(
                action=f"Optimize {page} for '{kw}' — current pos #{pos}, target top 3",
                target=f"{business.website}{page}",
                why=f"At pos #{pos} with {impressions} impressions — on-page optimization has highest ROI for this position range. Competitors at #{pos-3} likely have stronger title + H1 + content depth.",
                impact=impact,
                estimated_result=f"Move from #{pos} to #{max(1, pos-4)} within 21 days with content depth expansion + internal links",
                time_to_result="2-3 weeks",
                execution=f"1. Rewrite H1 to include exact '{kw}' + city. 2. Expand content to 1200+ words with FAQ section. 3. Add LocalBusiness schema with service + areaServed. 4. Build 2 internal links from high-DA pages.",
                type=TaskType.WEBSITE,
                execution_mode=ExecutionMode.MANUAL,
                confidence_score=conf,
                impact_score=9.0 if pos <= 10 else 7.0,
                speed_score=8.0,
                ease_score=7.0,
            ))

        elif verb == "optimize":
            tasks.append(SEOTask(
                action=f"Create dedicated page for '{kw}' — not yet ranking on page 1",
                target=f"{business.website}{page}",
                why=f"At #{pos}, this keyword is on page 2. A dedicated page with semantic depth typically pulls page-2 keywords to page 1 within 30 days.",
                impact=impact,
                estimated_result=f"Move from #{pos} to top 10 within 30 days",
                time_to_result="3-4 weeks",
                execution=f"1. Build standalone page at {page}. 2. 1000+ words targeting '{kw}'. 3. Include 5 PAA-style FAQ answers. 4. Add HowTo or FAQPage schema. 5. Internal link from homepage + top-ranking pages.",
                type=TaskType.WEBSITE,
                execution_mode=ExecutionMode.MANUAL,
                confidence_score=conf,
                impact_score=7.0,
                speed_score=7.0,
                ease_score=6.0,
            ))

    # ── 2. CTR optimization tasks ──
    low_ctr = [r for r in gsc_data if r.get("ctr", 1) < ctr_threshold and r.get("impressions", 0) >= 100]
    if low_ctr:
        worst = sorted(low_ctr, key=lambda x: x.get("ctr", 0))[0]
        kw = worst.get("keyword", "primary keyword")
        ctr = worst.get("ctr", 0)
        pos = worst.get("position", 10)
        pot_clicks = int(worst.get("impressions", 200) * (ctr_threshold - ctr))
        tasks.append(SEOTask(
            action=f"A/B test title + meta for '{kw}' — CTR {ctr:.1%} vs {ctr_threshold:.1%} target",
            target=f"{business.website}{worst.get('page', '/')}",
            why=f"At pos #{pos:.0f} with {ctr:.1%} CTR ({worst.get('impressions',200)} impressions), you're leaving ~{pot_clicks} clicks/month on the table. Each 1% CTR lift = free traffic equivalent to ranking 1 position higher.",
            impact=ImpactLevel.HIGH,
            estimated_result=f"+{pot_clicks} clicks/month — no ranking change needed",
            time_to_result="1-2 weeks",
            execution=f"1. Test 3 title variants: curiosity | urgency | benefit-led. 2. Use power words + location. 3. Rewrite meta to include specific offer or proof point. 4. Monitor in GSC for 14 days.",
            type=TaskType.WEBSITE,
            execution_mode=ExecutionMode.MANUAL,
            confidence_score=8.0,
            impact_score=8.5,
            speed_score=9.0,
            ease_score=9.0,
        ))

    # ── 3. GBP optimization ──
    gbp_views = getattr(business, "gbp_views", 0)
    monthly_traffic = getattr(business, "monthly_traffic", 0)
    if gbp_views > monthly_traffic * 2:
        # GBP is driving more views than website — GBP optimization is high-value
        tasks.append(SEOTask(
            action=f"Optimize Google Business Profile — {gbp_views:,} GBP views vs {monthly_traffic:,} website visits",
            target=f"https://business.google.com — {business.business_name}",
            why=f"GBP drives {gbp_views:,} views vs {monthly_traffic:,} site visits — GBP is your primary discovery channel. Adding keywords to services, posting 3x/week, and getting 10 more reviews will push into local pack top 3.",
            impact=ImpactLevel.HIGH,
            estimated_result="Enter local pack top 3 for primary keyword — adds 40-80 calls/month",
            time_to_result="3-4 weeks",
            execution="1. Add all services with keyword-rich descriptions. 2. Upload 10 project photos with geo-tagged filenames. 3. Post GBP update 3x/week for 30 days. 4. Request 10 new reviews from recent customers. 5. Add full FAQ to Q&A section.",
            type=TaskType.GBP,
            execution_mode=ExecutionMode.MANUAL,
            confidence_score=8.0,
            impact_score=9.0,
            speed_score=7.0,
            ease_score=8.0,
        ))

    # ── 4. Authority / link building ──
    if comp_da:
        leader_name = max(comp_da, key=comp_da.get)
        leader_da = comp_da[leader_name]
        leader_links = comp_links.get(leader_name, 200)
        da_gap = leader_da - our_da
        link_gap = leader_links - our_links
        severity = _da_gap_severity(our_da, leader_da)

        if da_gap > 5:
            tasks.append(SEOTask(
                action=f"Close DA gap vs {leader_name} — build {min(link_gap, 30)} backlinks in 30 days",
                target=business.website,
                why=f"{leader_name} has DA {leader_da} vs your DA {our_da} (gap: {da_gap}). Google uses domain authority as a tiebreaker — at equal content quality, higher-DA domain wins. {link_gap} link gap means systematic citation building needed.",
                impact=ImpactLevel.HIGH if severity == "critical" else ImpactLevel.MEDIUM,
                estimated_result=f"+{min(5, da_gap//4)} DA points in 60 days — directly translates to ranking lift across all keywords",
                time_to_result="2-3 months",
                execution="1. Claim all free citations: Yelp, BBB, HomeStars, Houzz, Yellow Pages Canada. 2. Submit to 10 local directories. 3. Request 3 supplier/partner backlinks. 4. Publish 1 linkable asset (cost guide, comparison table). 5. Guest post on 1 local home improvement blog.",
                type=TaskType.AUTHORITY,
                execution_mode=ExecutionMode.MANUAL,
                confidence_score=7.0,
                impact_score=8.0 if severity in ("critical", "high") else 6.0,
                speed_score=4.0,
                ease_score=6.0,
            ))

    # ── 5. Review acquisition ──
    reviews = getattr(business, "reviews_count", 0)
    rating = getattr(business, "rating", 0)
    if reviews < 50:
        tasks.append(SEOTask(
            action=f"Get {50 - reviews} more Google reviews — currently at {reviews} (target: 50+)",
            target=f"https://business.google.com — {business.business_name}",
            why=f"Reviews directly influence local pack rankings. {business.business_name} has {reviews} reviews vs top competitors who typically have 80-200+. Each review adds E-E-A-T signal and improves map pack CTR.",
            impact=ImpactLevel.MEDIUM,
            estimated_result=f"Push into local pack top 3 — {50-reviews} reviews can add 20-40 map pack impressions/day",
            time_to_result="2-4 weeks",
            execution=f"1. Send review request SMS/email to last 30 customers. 2. Add QR code to invoice/receipt. 3. Use short Google review link in follow-up messages. 4. Reply to all existing reviews within 24h to show activity.",
            type=TaskType.GBP,
            execution_mode=ExecutionMode.MANUAL,
            confidence_score=8.5,
            impact_score=7.0,
            speed_score=7.0,
            ease_score=9.0,
        ))

    # ── 6. Schema / technical ──
    primary_kw = business.primary_keywords[0] if business.primary_keywords else business.primary_service
    tasks.append(SEOTask(
        action=f"Add LocalBusiness + Service + FAQPage schema to all service pages",
        target=business.website,
        why="Google's entity understanding depends on structured data. Missing schema = weaker entity signals = lower rankings for competitive keywords. Adding AggregateRating schema also generates star ratings in SERP, boosting CTR by 15-30%.",
        impact=ImpactLevel.MEDIUM,
        estimated_result="Star ratings in SERP within 2 weeks, knowledge panel eligibility within 60 days",
        time_to_result="1-2 weeks",
        execution=f"1. Add LocalBusiness JSON-LD to all pages (name, url, phone, address, areaServed). 2. Add Service schema blocks for each service. 3. Add AggregateRating with {reviews} reviews @ {rating}★. 4. Add FAQPage schema to any page with Q&A. 5. Validate at schema.org/validator.",
        type=TaskType.WEBSITE,
        execution_mode=ExecutionMode.MANUAL,
        confidence_score=8.0,
        impact_score=7.0,
        speed_score=8.0,
        ease_score=7.0,
    ))

    # Score + rank all tasks
    ranked, filtered = score_and_rank(tasks)

    log.info("brain_script.done  run=%s  generated=%d  filtered=%d  final=%d",
             run_id, len(tasks), filtered, len(ranked))

    return TaskBatch(
        input_type="SCRIPT",
        tasks=ranked[:max_tasks],
        business_name=business.business_name,
        run_id=run_id,
        filtered_count=filtered,
    )
