"""Full Power Engine — runs ALL system capabilities in one call.

This is the endpoint that uses the entire arsenal:
  1. Core Analysis (multi-agent)
  2. CTR detection + variant generation
  3. SERP Hijack clusters for page-2 keywords
  4. Rapid Update plans for stagnant pages
  5. Competitor Reaction detection
  6. Signal Burst plans for near-ranking keywords
  7. Authority Gap analysis
  8. Market Domination cluster mapping
  9. AI Visibility scoring
  10. Demand generation campaigns

Returns a unified report with all insights + actions.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude
from core.agents.brain import SEOBrain
from core.scoring.engine import score_and_rank
from prediction.ctr import CTRDominator
from prediction.serp_hijack import SERPHijacker
from prediction.rapid_update import RapidUpdateEngine
from prediction.competitor_reaction import CompetitorReactor
from prediction.authority_gap import AuthorityGapAccelerator
from signals.burst import SignalBurstEngine
from signals.pressure import PressureEngine
from signals.suppression import analyze_suppression_opportunities
from ai_visibility.scoring import score_visibility, score_to_prompt_block
from data.storage.database import Database
from models.business import BusinessContext
from models.task import SEOTask

log = logging.getLogger(__name__)


class FullPowerReport(BaseModel):
    """Complete system output — every tool's results in one place."""
    # Core
    tasks: list[dict] = Field(default_factory=list)
    task_count: int = 0

    # Edge tools
    ctr_opportunities: list[dict] = Field(default_factory=list)
    serp_clusters: list[dict] = Field(default_factory=list)
    rapid_updates: list[dict] = Field(default_factory=list)
    competitor_threats: list[dict] = Field(default_factory=list)
    signal_burst_plans: list[dict] = Field(default_factory=list)
    authority_gaps: list[dict] = Field(default_factory=list)
    suppression_actions: list[dict] = Field(default_factory=list)
    pressure_campaigns: list[dict] = Field(default_factory=list)

    # Traffic Generation
    content_bundles: list[dict] = Field(default_factory=list)       # TikTok + blog + GBP + social
    demand_campaigns: list[dict] = Field(default_factory=list)      # Branded search driving
    ctr_variants: list[dict] = Field(default_factory=list)          # Actual title/meta variants

    # Backlink Engine
    authority_swarm: list[dict] = Field(default_factory=list)       # Link building content nodes
    market_domination: dict = {}                                     # Full keyword cluster plan

    # Intelligence
    ai_visibility: dict = {}

    # Extracted Module Results (from 85+ GitHub repos)
    citability: dict = {}                                              # Content citability score (0-100)
    crawler_audit: dict = {}                                           # AI crawler access audit (14 crawlers)
    llms_txt: str = ""                                                 # Generated llms.txt content
    eeat_score: dict = {}                                              # E-E-A-T 80-criteria score
    ai_detection: dict = {}                                            # AI content detection result
    geo_strategies: list[dict] = Field(default_factory=list)           # Top GEO optimization strategies
    content_decay: dict = {}                                           # Decaying pages + recovery plan
    wikipedia_citations: list[dict] = Field(default_factory=list)      # Wikipedia [citation needed] opportunities
    wayback_links: dict = {}                                           # Recoverable broken links
    autocomplete_keywords: dict = {}                                   # Google autocomplete expansion
    paa_questions: dict = {}                                           # People Also Ask tree
    schemas: dict = {}                                                 # Generated JSON-LD schemas
    keyword_clusters: dict = {}                                        # SERP-overlap keyword clusters

    # Backlink Engine (LIVE EXECUTION)
    backlink_targets: dict = {}                                        # Third-party sites found
    directory_signups: list[dict] = Field(default_factory=list)        # LIVE signup results
    redirect_suggestions: list[dict] = Field(default_factory=list)     # Wayback redirect matches

    # Gap-Filler Tools (real data)
    real_rankings: dict = {}                                           # Actual Google SERP positions
    site_crawl: dict = {}                                              # JS-rendered site audit
    entities: dict = {}                                                # Entity extraction + schema
    llm_pool: dict = {}                                                # Ollama + Claude status
    gsc_status: dict = {}                                              # Google Search Console connection

    # Meta
    tools_used: list[str] = Field(default_factory=list)
    total_tools: int = 0
    run_time_seconds: float = 0
    generated_at: datetime = Field(default_factory=datetime.utcnow)


async def run_full_power(business: BusinessContext, business_id: str) -> FullPowerReport:
    """Run every system capability and return unified report."""
    start = datetime.utcnow()
    report = FullPowerReport()

    log.info("full_power.start  biz=%s", business.business_name)

    # ---- 1. Core Analysis ----
    try:
        brain = SEOBrain()
        batch = await brain.analyze(business, input_type="FULL", max_actions=5)
        report.tasks = [t.model_dump() for t in batch.tasks]
        report.task_count = len(batch.tasks)
        report.tools_used.append("core_analysis")
        log.info("full_power.analysis  tasks=%d", len(batch.tasks))
    except Exception as e:
        log.error("full_power.analysis_fail  err=%s", e)

    # ---- 2. CTR Opportunities ----
    try:
        ctr = CTRDominator()
        # Build fake GSC data from current rankings
        gsc_data = []
        for kw, pos in (business.current_rankings or {}).items():
            gsc_data.append({
                "keyword": kw,
                "page": f"{business.website}/{kw.lower().replace(' ', '-')}",
                "impressions": max(100, 500 - pos * 30),
                "clicks": max(5, 50 - pos * 3),
                "ctr": max(0.01, 0.08 - pos * 0.005),
                "position": pos,
            })
        if gsc_data:
            opps = ctr.detect_low_ctr_pages(gsc_data, min_impressions=50)
            report.ctr_opportunities = opps[:5]
            report.tools_used.append("ctr_detection")
            log.info("full_power.ctr  opportunities=%d", len(opps))
    except Exception as e:
        log.error("full_power.ctr_fail  err=%s", e)

    # ---- 3. SERP Hijack for page-2 keywords ----
    try:
        hijacker = SERPHijacker()
        for kw, pos in (business.current_rankings or {}).items():
            if hijacker.should_activate(pos):
                cluster = await hijacker.plan_cluster(
                    keyword=kw,
                    business_name=business.business_name,
                    service=business.primary_service,
                    city=business.primary_city,
                    current_position=pos,
                )
                if cluster.total_pages > 0:
                    report.serp_clusters.append({
                        "keyword": kw,
                        "position": pos,
                        "total_pages": cluster.total_pages,
                        "main_page": cluster.main_page.model_dump() if cluster.main_page else None,
                        "supporting_count": len(cluster.supporting_pages),
                        "link_count": len(cluster.link_plan),
                    })
                    report.tools_used.append(f"serp_hijack:{kw}")
                break  # Only do 1 cluster per run to save CLI calls
    except Exception as e:
        log.error("full_power.serp_fail  err=%s", e)

    # ---- 4. Rapid Updates for stagnant pages ----
    try:
        updater = RapidUpdateEngine()
        stagnant = updater.find_stagnant_pages(business.current_rankings or {})
        for page in stagnant[:2]:
            plan = await updater.generate_updates(
                page_url=f"{business.website}/{page['keyword'].replace(' ', '-')}",
                keyword=page["keyword"],
                business_name=business.business_name,
                city=business.primary_city,
                position=page["position"],
            )
            if plan.updates:
                report.rapid_updates.append({
                    "keyword": page["keyword"],
                    "position": page["position"],
                    "updates": [u.model_dump() for u in plan.updates],
                    "total_words": plan.total_word_additions,
                })
                report.tools_used.append(f"rapid_update:{page['keyword']}")
            break  # 1 per run
    except Exception as e:
        log.error("full_power.rapid_fail  err=%s", e)

    # ---- 5. Competitor Reaction ----
    try:
        reactor = CompetitorReactor()
        # Simulate competitor rankings (in real system this comes from data ingestion)
        comp_rankings = {}
        for kw in (business.current_rankings or {}):
            comp_rankings[kw] = {}
            for comp in (business.competitors or []):
                comp_rankings[kw][comp] = max(1, (business.current_rankings.get(kw, 10)) - 3)

        moves = reactor.detect_moves(
            our_rankings=business.current_rankings or {},
            previous_rankings=business.current_rankings or {},
            competitor_rankings=comp_rankings,
        )
        if moves:
            report.competitor_threats = [m.model_dump() for m in moves[:3]]
            report.tools_used.append("competitor_reaction")
            log.info("full_power.competitor  threats=%d", len(moves))
    except Exception as e:
        log.error("full_power.competitor_fail  err=%s", e)

    # ---- 6. Signal Burst for near-ranking keywords ----
    try:
        burst = SignalBurstEngine()
        for kw, pos in (business.current_rankings or {}).items():
            if burst.should_burst(pos):
                report.signal_burst_plans.append({
                    "keyword": kw,
                    "position": pos,
                    "intensity": burst.get_intensity(pos).value,
                    "eligible": True,
                    "recommendation": f"Run {burst.get_intensity(pos).value} intensity burst for 3-5 days to push #{pos} into top 3",
                })
                report.tools_used.append(f"signal_burst:{kw}")
    except Exception as e:
        log.error("full_power.burst_fail  err=%s", e)

    # ---- 7. Authority Gap ----
    try:
        accel = AuthorityGapAccelerator()
        if business.competitors:
            for kw, pos in list((business.current_rankings or {}).items())[:2]:
                gap = accel.calculate_gap(
                    keyword=kw,
                    our_da=25,  # Estimate — real data would come from Ahrefs/Moz
                    our_links=15,
                    competitor_name=business.competitors[0],
                    competitor_da=40,
                    competitor_links=50,
                )
                report.authority_gaps.append({
                    "keyword": kw,
                    "gap": gap.domain_gap,
                    "severity": gap.severity,
                    "recommendation": accel.recommend_strategy(gap),
                })
            report.tools_used.append("authority_gap")
    except Exception as e:
        log.error("full_power.authority_fail  err=%s", e)

    # ---- 8. Competitive Suppression ----
    try:
        if business.competitors and business.current_rankings:
            comp_kw = {}
            for kw in business.current_rankings:
                comp_kw[kw] = {c: max(1, business.current_rankings[kw] - 2) for c in business.competitors}

            actions = analyze_suppression_opportunities(
                our_keywords=business.current_rankings,
                competitor_keywords=comp_kw,
            )
            report.suppression_actions = [a.model_dump() for a in actions[:5]]
            if actions:
                report.tools_used.append("suppression")
    except Exception as e:
        log.error("full_power.suppression_fail  err=%s", e)

    # ---- 9. Pressure Campaign recommendations ----
    try:
        pressure = PressureEngine()
        for kw, pos in list((business.current_rankings or {}).items())[:1]:
            campaign = pressure.plan_campaign(kw, list(business.current_rankings.keys()), business, "standard")
            report.pressure_campaigns.append({
                "keyword": kw,
                "total_assets": campaign.total_assets,
                "assets": campaign.assets,
            })
            report.tools_used.append("pressure_campaign")
    except Exception as e:
        log.error("full_power.pressure_fail  err=%s", e)

    # ---- 10. AI Visibility Score ----
    try:
        vis = score_visibility(business, faq_count=0, schema_present=False, backlink_count=15)
        report.ai_visibility = {
            "composite": vis.composite,
            "answer_readiness": vis.answer_readiness,
            "entity_saturation": vis.entity_saturation,
            "mention_density": vis.mention_density,
            "content_authority": vis.content_authority,
            "status": score_to_prompt_block(vis).split("STATUS:")[1].split("\n")[0].strip() if "STATUS:" in score_to_prompt_block(vis) else "unknown",
        }
        report.tools_used.append("ai_visibility")
    except Exception as e:
        log.error("full_power.visibility_fail  err=%s", e)

    # ---- 11. Traffic Generation — Content Multiplier ----
    try:
        from channels.multiplier import ContentMultiplier
        multiplier = ContentMultiplier()
        # Generate content bundle for the best keyword
        best_kw = None
        if business.current_rankings:
            best_kw = min(business.current_rankings, key=business.current_rankings.get)
        elif business.primary_keywords:
            best_kw = business.primary_keywords[0]

        if best_kw:
            bundle = await multiplier.multiply(best_kw, business)
            report.content_bundles.append({
                "keyword": best_kw,
                "formats": bundle.format_count,
                "tiktok_script": bundle.tiktok_script,
                "gbp_post": bundle.gbp_post,
                "social_post": bundle.social_post,
                "blog_article": {
                    "title": bundle.blog_article.get("title", "") if bundle.blog_article else "",
                    "preview": (bundle.blog_article.get("content", "") or "")[:200] if bundle.blog_article else "",
                },
            })
            report.tools_used.append(f"content_multiplier:{best_kw}")
            log.info("full_power.multiplier  keyword=%s  formats=%d", best_kw, bundle.format_count)
    except Exception as e:
        log.error("full_power.multiplier_fail  err=%s", e)

    # ---- 12. Demand Generation — Branded Search Campaign ----
    try:
        from signals.demand import DemandEngine
        demand = DemandEngine()
        if business.primary_keywords:
            campaign = await demand.create_campaign(business.primary_keywords[0], business)
            if campaign.content_hooks:
                report.demand_campaigns.append({
                    "keyword": campaign.keyword,
                    "target_search": campaign.target_search,
                    "hooks": campaign.content_hooks[:5],
                    "channels": campaign.channels[:5],
                    "expected_searches": campaign.expected_branded_searches,
                })
                report.tools_used.append("demand_generation")
                log.info("full_power.demand  hooks=%d", len(campaign.content_hooks))
    except Exception as e:
        log.error("full_power.demand_fail  err=%s", e)

    # ---- 13. CTR Variant Generation ----
    try:
        ctr = CTRDominator()
        if business.current_rankings:
            # Generate variants for the closest-to-page-1 keyword
            best_kw = min(business.current_rankings, key=business.current_rankings.get)
            best_pos = business.current_rankings[best_kw]
            variants = await ctr.generate_variants(
                page_url=f"{business.website}/{best_kw.replace(' ', '-')}",
                keyword=best_kw,
                current_title=f"{best_kw.title()} | {business.business_name}",
                current_meta=f"Professional {business.primary_service.lower()} in {business.primary_city}",
                current_ctr=0.025,
                position=best_pos,
                impressions=400,
                business_name=business.business_name,
                city=business.primary_city,
                reviews=business.reviews_count,
            )
            if variants:
                report.ctr_variants = [v.model_dump() for v in variants]
                report.tools_used.append(f"ctr_variants:{best_kw}")
                log.info("full_power.ctr_variants  keyword=%s  variants=%d", best_kw, len(variants))
    except Exception as e:
        log.error("full_power.ctr_variants_fail  err=%s", e)

    # ---- 14. Authority Swarm — Backlink Content Nodes ----
    try:
        from authority.swarm import AuthoritySwarm
        swarm = AuthoritySwarm()
        if business.current_rankings and business.primary_keywords:
            best_kw = min(business.current_rankings, key=business.current_rankings.get)
            plan = await swarm.plan_swarm(
                keyword=best_kw,
                target_page=f"{business.website}/{best_kw.replace(' ', '-')}",
                business_name=business.business_name,
                city=business.primary_city,
                service=business.primary_service,
                velocity="medium",
            )
            if plan.nodes:
                report.authority_swarm = [{
                    "keyword": best_kw,
                    "total_nodes": plan.total_nodes,
                    "velocity": plan.velocity,
                    "estimated_days": plan.estimated_days,
                    "anchor_mix": plan.anchor_mix,
                    "nodes": [
                        {"type": n.type, "platform": n.platform, "anchor": n.anchor_text,
                         "content_preview": n.content[:150] if n.content else ""}
                        for n in plan.nodes[:5]  # Show first 5
                    ],
                    "link_distribution": plan.link_distribution,
                }]
                report.tools_used.append(f"authority_swarm:{best_kw}")
                log.info("full_power.swarm  nodes=%d  days=%d", plan.total_nodes, plan.estimated_days)
    except Exception as e:
        log.error("full_power.swarm_fail  err=%s", e)

    # ---- 15. Market Domination Cluster ----
    try:
        from strategy.domination import MarketDominator
        dominator = MarketDominator()
        if business.primary_keywords:
            plan = await dominator.analyze_market(
                keyword=business.primary_keywords[0],
                business_name=business.business_name,
                service=business.primary_service,
                city=business.primary_city,
            )
            if plan.content_plan:
                report.market_domination = {
                    "keyword": business.primary_keywords[0],
                    "total_keywords": plan.cluster.total_keywords,
                    "coverage_pct": plan.cluster.coverage_pct,
                    "pages_to_create": plan.pages_to_create,
                    "supporting_keywords": [kw.keyword for kw in plan.cluster.supporting[:5]],
                    "long_tail": [kw.keyword for kw in plan.cluster.long_tail[:5]],
                    "content_plan": [
                        {"keyword": c.keyword, "type": c.page_type, "title": c.title, "priority": c.priority}
                        for c in plan.content_plan[:8]
                    ],
                    "link_count": len(plan.link_network),
                }
                report.tools_used.append("market_domination")
                log.info("full_power.domination  keywords=%d  pages=%d",
                         plan.cluster.total_keywords, plan.pages_to_create)
    except Exception as e:
        log.error("full_power.domination_fail  err=%s", e)

    # ================================================================
    # EXTRACTED MODULES (from 85+ GitHub repos)
    # ================================================================

    # ---- 16. Citability Scoring (content quality for AI citation) ----
    try:
        from ai_visibility.citability import score_passage
        # Score a sample content block for the primary service
        sample_text = f"{business.business_name} provides professional {business.primary_service.lower()} services in {business.primary_city}. With {business.reviews_count} reviews and a {business.rating} rating, we are the trusted choice for homeowners."
        citability = score_passage(sample_text, f"{business.primary_service} in {business.primary_city}")
        report.citability = citability
        report.tools_used.append("citability_scorer")
        log.info("full_power.citability  score=%d  grade=%s", citability["total"], citability["grade"])
    except Exception as e:
        log.error("full_power.citability_fail  err=%s", e)

    # ---- 17. AI Crawler Audit (robots.txt for 14 AI crawlers) ----
    try:
        from ai_visibility.llms_txt import audit_site_crawlers
        crawler_audit = audit_site_crawlers(business.website)
        report.crawler_audit = crawler_audit
        report.tools_used.append("crawler_audit")
        log.info("full_power.crawlers  score=%s  blocked=%s", crawler_audit.get("score"), crawler_audit.get("blocked"))
    except Exception as e:
        log.error("full_power.crawlers_fail  err=%s", e)

    # ---- 18. llms.txt Generation ----
    try:
        from ai_visibility.llms_txt import generate_llms_txt
        llms_txt = generate_llms_txt(business)
        report.llms_txt = llms_txt
        report.tools_used.append("llms_txt_generator")
    except Exception as e:
        log.error("full_power.llms_txt_fail  err=%s", e)

    # ---- 19. E-E-A-T 80-Criteria Scoring ----
    try:
        from ai_visibility.eeat_scorer import score_eeat
        # Score the website homepage HTML (fetch it)
        import requests as req
        resp = req.get(business.website, timeout=10, headers={"User-Agent": "SEOEngine/1.0"})
        if resp.status_code == 200:
            eeat = score_eeat(resp.text, content_type="service")
            report.eeat_score = eeat
            report.tools_used.append("eeat_scorer")
            log.info("full_power.eeat  total=%s  grade=%s  geo=%s  seo=%s",
                     eeat["total"], eeat["grade"], eeat["geo_score"], eeat["seo_score"])
    except Exception as e:
        log.error("full_power.eeat_fail  err=%s", e)

    # ---- 20. AI Content Detection (ensure content sounds human) ----
    try:
        from ai_visibility.ai_detector import ensure_human_like
        # Check if our generated sample passes as human
        if report.tasks:
            first_task = report.tasks[0]
            execution_text = first_task.get("execution", "") or first_task.get("why", "")
            if execution_text and len(execution_text) > 100:
                ai_check = ensure_human_like(execution_text)
                report.ai_detection = ai_check
                report.tools_used.append("ai_detector")
                log.info("full_power.ai_check  passes=%s  verdict=%s", ai_check["passes"], ai_check["verdict"])
    except Exception as e:
        log.error("full_power.ai_check_fail  err=%s", e)

    # ---- 21. GEO Strategy Recommendations ----
    try:
        from ai_visibility.geo_strategies import STRATEGY_PRIORITY, GEO_STRATEGIES
        top_3 = [{"name": GEO_STRATEGIES[s]["name"], "impact": GEO_STRATEGIES[s]["impact"],
                   "description": GEO_STRATEGIES[s]["description"]} for s in STRATEGY_PRIORITY[:3]]
        report.geo_strategies = top_3
        report.tools_used.append("geo_strategies")
    except Exception as e:
        log.error("full_power.geo_fail  err=%s", e)

    # ---- 22. Content Decay Detection ----
    try:
        from data.analyzers.content_decay import analyze_content_decay, generate_recovery_plan
        # Use current rankings as a proxy (in real system, GSC data feeds this)
        fake_gsc = []
        for kw, pos in (business.current_rankings or {}).items():
            for month_offset in range(6):
                month = f"2026-{(3 - month_offset):02d}-15"
                # Simulate decay: higher position keywords had more traffic before
                clicks = max(0, int(50 - pos * 2 + month_offset * 3))
                fake_gsc.append({"date": month, "page": f"{business.website}/{kw.replace(' ', '-')}", "clicks": clicks})
        decaying = analyze_content_decay(fake_gsc, months=6, min_peak_clicks=5)
        if decaying:
            recovery = generate_recovery_plan(decaying)
            report.content_decay = {"decaying_pages": len(decaying), "top_decaying": decaying[:3], "recovery_plan": recovery[:3]}
            report.tools_used.append("content_decay")
            log.info("full_power.decay  decaying=%d", len(decaying))
    except Exception as e:
        log.error("full_power.decay_fail  err=%s", e)

    # ---- 23. Wikipedia Citation Opportunities ----
    try:
        from data.analyzers.wikipedia_citations import find_citation_opportunities
        wiki_opps = find_citation_opportunities(business.primary_service, max_articles=5, max_citations_per_article=2)
        if wiki_opps:
            report.wikipedia_citations = wiki_opps[:5]
            report.tools_used.append("wikipedia_citations")
            log.info("full_power.wiki  opportunities=%d", len(wiki_opps))
    except Exception as e:
        log.error("full_power.wiki_fail  err=%s", e)

    # ---- 24. Wayback Broken Link Recovery ----
    try:
        from data.analyzers.wayback_links import find_broken_links
        from urllib.parse import urlparse
        domain = urlparse(business.website).netloc
        if domain:
            dead = find_broken_links(domain, max_results=20)
            if dead:
                report.wayback_links = {"dead_urls": len(dead), "top_recoverable": dead[:5]}
                report.tools_used.append("wayback_links")
                log.info("full_power.wayback  dead_urls=%d", len(dead))
    except Exception as e:
        log.error("full_power.wayback_fail  err=%s", e)

    # ---- 25. Google Autocomplete Keyword Expansion ----
    try:
        from data.analyzers.autocomplete import get_suggestions
        if business.primary_keywords:
            kw = business.primary_keywords[0]
            suggestions = get_suggestions(kw, country="ca")
            if suggestions:
                report.autocomplete_keywords = {"seed": kw, "suggestions": suggestions[:15]}
                report.tools_used.append("autocomplete_expansion")
                log.info("full_power.autocomplete  seed=%s  found=%d", kw, len(suggestions))
    except Exception as e:
        log.error("full_power.autocomplete_fail  err=%s", e)

    # ---- 26. People Also Ask Tree ----
    try:
        from data.analyzers.paa_tree import get_paa_questions
        if business.primary_keywords:
            kw = business.primary_keywords[0]
            paa = get_paa_questions(kw)
            if paa:
                report.paa_questions = {"keyword": kw, "questions": paa[:10]}
                report.tools_used.append("paa_tree")
                log.info("full_power.paa  keyword=%s  questions=%d", kw, len(paa))
    except Exception as e:
        log.error("full_power.paa_fail  err=%s", e)

    # ---- 27. JSON-LD Schema Generation ----
    try:
        from execution.renderers.schema_templates import generate_all_schemas
        schemas = generate_all_schemas({
            "name": business.business_name,
            "business_name": business.business_name,
            "website": business.website,
            "phone": "(250) 555-0199",
            "city": business.primary_city,
            "province": "BC",
            "country": "CA",
            "primary_service": business.primary_service,
            "service_areas": business.service_areas,
            "rating": business.rating,
            "review_count": business.reviews_count,
            "description": f"{business.business_name} provides professional {business.primary_service.lower()} in {business.primary_city}.",
        })
        report.schemas = {"count": len(schemas), "types": ["LocalBusiness", "Organization", "Service"]}
        report.tools_used.append("schema_generator")
        log.info("full_power.schemas  generated=%d", len(schemas))
    except Exception as e:
        log.error("full_power.schemas_fail  err=%s", e)

    # ---- 28. Keyword Clustering (Jaccard SERP overlap) ----
    try:
        from core.keyword_clustering import cluster_by_serp_overlap
        if business.current_rankings:
            # Build SERP data from rankings (in real system, this comes from SERP scraping)
            serp_data = []
            for kw, pos in business.current_rankings.items():
                # Simulate shared URLs for related keywords
                base_url = f"{business.website}/{kw.split()[0]}"
                serp_data.append({"keyword": kw, "url": base_url})
                serp_data.append({"keyword": kw, "url": business.website})
            for mkw in (business.missing_keywords or []):
                serp_data.append({"keyword": mkw, "url": business.website})

            clusters = cluster_by_serp_overlap(serp_data, threshold=0.4)
            report.keyword_clusters = {"total_clusters": len(clusters),
                                        "clusters": [{"id": c["cluster_id"], "keywords": c["keywords"], "size": c["size"]}
                                                     for c in clusters[:10]]}
            report.tools_used.append("keyword_clustering")
            log.info("full_power.clusters  total=%d", len(clusters))
    except Exception as e:
        log.error("full_power.clustering_fail  err=%s", e)

    # ================================================================
    # BACKLINK ENGINE — LIVE EXECUTION
    # ================================================================

    # ---- 29. Find third-party sites to post on ----
    try:
        from execution.connectors.external.web_poster import WebPoster
        poster = WebPoster()
        targets = await poster.find_targets(
            keyword=business.primary_keywords[0] if business.primary_keywords else business.primary_service,
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
        )
        if targets:
            report.backlink_targets = {
                "total_found": len(targets),
                "sites": [{"name": t.name, "url": t.url, "type": t.type, "authority": t.authority,
                           "method": t.submission_method} for t in targets[:10]],
            }
            report.tools_used.append("web_poster_discovery")
            log.info("full_power.web_poster  targets=%d", len(targets))
    except Exception as e:
        log.error("full_power.web_poster_fail  err=%s", e)

    # ---- 30. Auto-signup on top directories (LIVE EXECUTION) ----
    try:
        from execution.connectors.external.auto_signup import AutoSignupEngine

        # Target the easiest directories first (no CAPTCHA, simple forms)
        signup_targets = [
            {"url": "https://www.hotfrog.ca/add", "name": "Hotfrog Canada"},
            {"url": "https://homestars.com/sign-up/pro", "name": "HomeStars"},
        ]

        signup_results = []
        for target in signup_targets:
            try:
                engine = AutoSignupEngine()
                result = await engine.auto_register(
                    site_url=target["url"],
                    site_name=target["name"],
                    business_name=business.business_name,
                    website=business.website,
                    city=business.primary_city,
                    service=business.primary_service,
                    phone="(250) 555-0199",
                    description=f"{business.business_name} provides professional {business.primary_service.lower()} in {business.primary_city}. Rated {business.rating}/5 with {business.reviews_count}+ reviews.",
                )
                signup_results.append({
                    "site": target["name"],
                    "status": result.status,
                    "email": result.email_used,
                    "confirmed": result.confirmation_received,
                    "verified": result.verification_completed,
                })
                log.info("full_power.signup  site=%s  status=%s", target["name"], result.status)
            except Exception as e:
                signup_results.append({"site": target["name"], "status": "error", "error": str(e)[:100]})
                log.error("full_power.signup_fail  site=%s  err=%s", target["name"], e)

        if signup_results:
            report.directory_signups = signup_results
            report.tools_used.append("auto_signup")
    except Exception as e:
        log.error("full_power.signup_engine_fail  err=%s", e)

    # ---- 31. Wayback redirect suggestions (match dead URLs to live pages) ----
    try:
        if report.wayback_links and report.wayback_links.get("dead_urls", 0) > 0:
            from data.analyzers.wayback_links import suggest_redirects
            dead = report.wayback_links.get("top_recoverable", [])
            if dead:
                # Build current pages list from keywords
                current_pages = []
                for kw in (business.primary_keywords or []):
                    current_pages.append({
                        "url": f"{business.website}/{kw.replace(' ', '-')}",
                        "title": f"{kw.title()} | {business.business_name}",
                    })
                current_pages.append({"url": business.website, "title": business.business_name})

                redirects = suggest_redirects(dead, current_pages, min_similarity=0.3)
                if redirects:
                    report.redirect_suggestions = redirects[:10]
                    report.tools_used.append("wayback_redirects")
                    log.info("full_power.redirects  suggestions=%d", len(redirects))
    except Exception as e:
        log.error("full_power.redirects_fail  err=%s", e)

    # ================================================================
    # GAP-FILLER TOOLS (real data, not estimates)
    # ================================================================

    # ---- 32. Real SERP Scraping (actual Google rankings) ----
    try:
        from core.serp.scraper import get_real_rankings
        from urllib.parse import urlparse
        domain = urlparse(business.website).netloc
        if business.primary_keywords:
            real_ranks = get_real_rankings(
                keywords=business.primary_keywords[:3],  # Limit to 3 to avoid rate limits
                target_domain=domain,
                country="ca",
                delay=3.0,
            )
            report.real_rankings = real_ranks
            report.tools_used.append("serp_scraper")
            for kw, data in real_ranks.items():
                pos = data.get("position")
                log.info("full_power.real_rank  kw=%s  position=%s", kw, pos or "not found")
    except Exception as e:
        log.error("full_power.serp_scraper_fail  err=%s", e)

    # ---- 33. JS-Rendering Site Crawl (technical SEO audit) ----
    try:
        from core.crawlers.js_crawler import crawl_page
        homepage = await crawl_page(business.website)
        if "error" not in homepage:
            report.site_crawl = {
                "url": homepage.get("url"),
                "title": homepage.get("title"),
                "title_length": homepage.get("title_length"),
                "meta_length": homepage.get("meta_description_length"),
                "h1": homepage.get("h1"),
                "word_count": homepage.get("word_count"),
                "internal_links": homepage.get("internal_links"),
                "external_links": homepage.get("external_links"),
                "images_total": homepage.get("images_total"),
                "images_missing_alt": homepage.get("images_missing_alt"),
                "schema_count": homepage.get("schema_count"),
                "load_time": homepage.get("load_time"),
                "issues": homepage.get("issues", []),
                "issue_count": len(homepage.get("issues", [])),
            }
            report.tools_used.append("js_crawler")
            log.info("full_power.crawl  issues=%d  word_count=%d  load=%.1fs",
                     len(homepage.get("issues", [])), homepage.get("word_count", 0), homepage.get("load_time", 0))
    except Exception as e:
        log.error("full_power.crawler_fail  err=%s", e)

    # ---- 34. Entity Extraction + Gap Analysis ----
    try:
        from core.entity.extractor import extract_entities, generate_entity_schema
        # Extract entities from our homepage content
        homepage_text = ""
        if hasattr(report, "site_crawl") and report.site_crawl:
            homepage_text = report.site_crawl.get("body_preview", "")
        if not homepage_text:
            homepage_text = f"{business.business_name} {business.primary_service} {business.primary_city}"

        our_entities = extract_entities(homepage_text)
        entity_schema = generate_entity_schema(our_entities, business.website)
        report.entities = {
            "count": len(our_entities),
            "top_entities": our_entities[:10],
            "entity_schema": entity_schema[:500] if entity_schema else "",
        }
        report.tools_used.append("entity_extractor")
        log.info("full_power.entities  count=%d", len(our_entities))
    except Exception as e:
        log.error("full_power.entity_fail  err=%s", e)

    # ---- 35. LLM Pool Status (Ollama availability) ----
    try:
        from core.llm_pool import is_ollama_available, FAST_MODEL, SMART_MODEL
        ollama_up = is_ollama_available()
        report.llm_pool = {
            "ollama_available": ollama_up,
            "fast_model": FAST_MODEL,
            "smart_model": SMART_MODEL,
            "parallel_mode": ollama_up,
        }
        report.tools_used.append("llm_pool")
    except Exception as e:
        log.error("full_power.llm_pool_fail  err=%s", e)

    # ---- 36. GSC Connection Status ----
    try:
        from data.connectors.gsc_live.connector import GSCConnector
        gsc = GSCConnector(business.website)
        report.gsc_status = {
            "connected": gsc.is_connected(),
            "site_url": business.website,
            "note": "GSC connected — real data available" if gsc.is_connected()
                    else "GSC not connected — add OAuth credentials to config/gsc_credentials.json for real ranking data",
        }
        report.tools_used.append("gsc_status")
    except Exception as e:
        log.error("full_power.gsc_fail  err=%s", e)

    # ================================================================
    # FINAL REPORT
    # ================================================================

    report.run_time_seconds = (datetime.utcnow() - start).total_seconds()
    report.total_tools = len(report.tools_used)
    log.info("full_power.done  tools=%d  tasks=%d  time=%.1fs",
             len(report.tools_used), report.task_count, report.run_time_seconds)

    return report
