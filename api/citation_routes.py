"""
Citation content API routes — LLM citation layer (Gap #5).
"""
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

router = APIRouter(prefix="/citation", tags=["citation"])


class FactIn(BaseModel):
    business_id: str
    category: str
    claim: str
    value: str
    unit: str = ""
    source: str = "market_analysis"
    methodology: str = ""
    confidence: str = "estimated"
    keywords: list[str] = []


@router.post("/facts")
def add_fact_endpoint(body: FactIn):
    from core.citable_data import add_fact
    return add_fact(
        business_id=body.business_id,
        category=body.category,
        claim=body.claim,
        value=body.value,
        unit=body.unit,
        source=body.source,
        methodology=body.methodology,
        confidence=body.confidence,
        keywords=body.keywords,
    )


@router.get("/facts")
def get_facts_endpoint(
    business_id: str = Query(...),
    category: str = Query(""),
    limit: int = Query(50),
):
    from core.citable_data import get_facts
    return get_facts(business_id, category, limit)


@router.post("/facts/generate")
def generate_facts_endpoint(business_id: str = Query(...)):
    from core.citable_data import generate_local_facts
    facts = generate_local_facts(business_id)
    return {"business_id": business_id, "generated": len(facts), "facts": facts}


@router.post("/facts/cite/{fact_id}")
def record_citation_endpoint(fact_id: str):
    from core.citable_data import record_citation
    record_citation(fact_id)
    return {"ok": True, "fact_id": fact_id}


@router.post("/generate/cost-guide")
def generate_cost_guide_endpoint(
    business_id: str = Query(...),
    service: str = Query(...),
    location: str = Query(...),
):
    from core.citable_data import get_facts
    from core.citation_content import generate_cost_guide
    facts = get_facts(business_id, category="pricing", limit=20)
    result = generate_cost_guide(business_id, service, location, facts)
    return result


@router.post("/generate/stats-page")
def generate_stats_page_endpoint(
    business_id: str = Query(...),
    topic: str = Query(...),
    location: str = Query(...),
):
    from core.citable_data import get_facts
    from core.citation_content import generate_stats_page
    facts = get_facts(business_id, limit=30)
    result = generate_stats_page(business_id, topic, location, facts)
    return result


@router.post("/generate/faq-hub")
def generate_faq_hub_endpoint(
    business_id: str = Query(...),
    service: str = Query(...),
    location: str = Query(...),
):
    from core.citable_data import get_facts
    from core.citation_content import generate_faq_hub
    facts = get_facts(business_id, limit=30)
    result = generate_faq_hub(business_id, service, location, facts)
    return result


@router.post("/generate/local-study")
def generate_local_study_endpoint(
    business_id: str = Query(...),
    service: str = Query(...),
    location: str = Query(...),
):
    from core.citable_data import get_facts
    from core.citation_content import generate_local_study
    facts = get_facts(business_id, limit=30)
    result = generate_local_study(business_id, service, location, facts)
    return result


@router.post("/sweep")
def citation_sweep_endpoint(business_id: str = Query(...)):
    from core.citation_content import run_citation_content_sweep
    results = run_citation_content_sweep(business_id)
    return {"business_id": business_id, "pages": results}


@router.get("/pages")
def get_citation_pages_endpoint(
    business_id: str = Query(...),
    page_type: str = Query(""),
):
    from core.citation_content import get_citation_pages
    return get_citation_pages(business_id, page_type)


@router.get("/pages/{page_id}")
def get_citation_page_html_endpoint(page_id: str):
    from core.citation_content import get_citation_page_html
    html = get_citation_page_html(page_id)
    if html is None:
        raise HTTPException(status_code=404, detail="Page not found")
    return {"page_id": page_id, "html": html}


@router.get("/score/{page_id}")
def score_citation_page_endpoint(page_id: str):
    from core.citation_content import get_citation_page_html, _score_citation_readiness
    html = get_citation_page_html(page_id)
    if html is None:
        raise HTTPException(status_code=404, detail="Page not found")
    score = _score_citation_readiness(html)
    return {"page_id": page_id, "citation_score": score}
