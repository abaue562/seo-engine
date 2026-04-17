"""Self-hosted SERP, crawl, and keyword intelligence API routes."""
from __future__ import annotations
from fastapi import APIRouter, Query
from pydantic import BaseModel

router = APIRouter(prefix="/crawl", tags=["crawl"])


class SerpRequest(BaseModel):
    keyword: str
    location: str = ""
    num_results: int = 10


class RankCheckRequest(BaseModel):
    business_id: str
    keyword: str
    your_domain: str
    location: str = ""


class RankSweepRequest(BaseModel):
    business_id: str
    keywords: list[str]
    your_domain: str
    location: str = ""


class CrawlRequest(BaseModel):
    domain: str
    max_pages: int = 10


class KeywordRequest(BaseModel):
    keyword: str
    business_id: str = ""
    location: str = ""


class OpportunityRequest(BaseModel):
    business_id: str
    niche: str
    location: str = ""
    limit: int = 30


class CompetitorSuiteRequest(BaseModel):
    business_id: str


@router.post("/serp")
def scrape_serp(body: SerpRequest):
    from core.serp_scraper import scrape_serp as _scrape
    result = _scrape(body.keyword, body.location, body.num_results)
    return result


@router.get("/serp/suggestions")
def keyword_suggestions(keyword: str = Query(...), location: str = Query("")):
    from core.serp_scraper import get_keyword_suggestions
    return {"keyword": keyword, "suggestions": get_keyword_suggestions(keyword, location)}


@router.post("/serp/difficulty")
def keyword_difficulty(body: KeywordRequest):
    from core.serp_scraper import estimate_keyword_difficulty
    return estimate_keyword_difficulty(body.keyword)


@router.post("/rank/check")
def check_ranking(body: RankCheckRequest):
    from core.serp_scraper import check_keyword_ranking
    return check_keyword_ranking(body.business_id, body.keyword, body.your_domain, body.location)


@router.post("/rank/sweep")
def rank_sweep(body: RankSweepRequest):
    from core.serp_scraper import run_rank_tracking_sweep
    return run_rank_tracking_sweep(body.business_id, body.keywords, body.your_domain, body.location)


@router.get("/rank/history")
def rank_history(business_id: str = Query(...), keyword: str = Query(""), limit: int = Query(30)):
    from core.serp_scraper import get_ranking_history
    return {"history": get_ranking_history(business_id, keyword, limit)}


@router.get("/rank/competitors")
def serp_competitors(keyword: str = Query(...), your_domain: str = Query(...), location: str = Query("")):
    from core.serp_scraper import get_serp_competitors
    return {"competitors": get_serp_competitors(keyword, your_domain, location)}


@router.post("/domain/crawl")
def crawl_domain(body: CrawlRequest):
    from core.backlink_crawler import crawl_domain as _crawl
    return _crawl(body.domain, body.max_pages)


@router.get("/domain/authority")
def domain_authority(domain: str = Query(...), refresh: bool = Query(False)):
    from core.backlink_crawler import compute_domain_authority, get_domain_authority
    if refresh:
        return compute_domain_authority(domain)
    return {"domain": domain, "da_score": get_domain_authority(domain)}


@router.post("/domain/competitor-suite")
def competitor_suite(body: CompetitorSuiteRequest):
    from core.backlink_crawler import crawl_competitor_suite
    return crawl_competitor_suite(body.business_id)


@router.post("/keyword/research")
def keyword_research(body: KeywordRequest):
    from core.keyword_intel import research_keyword
    return research_keyword(body.keyword, body.business_id, body.location)


@router.post("/keyword/opportunities")
def keyword_opportunities(body: OpportunityRequest):
    from core.keyword_intel import get_keyword_opportunities
    return {"opportunities": get_keyword_opportunities(body.business_id, body.niche, body.location, body.limit)}


@router.post("/keyword/trending")
def trending_keywords(body: dict):
    from core.keyword_intel import get_trending_keywords
    return {"trending": get_trending_keywords(body.get("niche", ""), body.get("location", ""))}
