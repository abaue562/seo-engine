"""
Parasite SEO API routes (Gap #7).
"""
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter(prefix="/parasite", tags=["parasite"])


class GenerateIn(BaseModel):
    business_id: str
    platform: str
    service: str
    location: str
    business_name: str = ""
    domain: str = ""


@router.post("/generate")
def generate_page(body: GenerateIn):
    from core.parasite_seo import generate_parasite_content
    from core.citable_data import get_facts
    facts = get_facts(body.business_id, limit=15)
    return generate_parasite_content(
        business_id=body.business_id,
        platform=body.platform,
        service=body.service,
        location=body.location,
        facts=facts,
        business_name=body.business_name,
        domain=body.domain,
    )


@router.post("/publish/{page_id}")
def publish_page(page_id: str):
    from core.parasite_seo import publish_parasite_page
    return publish_parasite_page(page_id)


@router.post("/sweep")
def parasite_sweep(
    business_id: str = Query(...),
    platforms: str = Query("github_pages,medium,devto,reddit,quora"),
):
    from core.parasite_seo import run_parasite_sweep
    platform_list = [p.strip() for p in platforms.split(",") if p.strip()]
    results = run_parasite_sweep(business_id, platform_list)
    return {"business_id": business_id, "pages": results}


@router.get("/pages")
def list_pages(
    business_id: str = Query(...),
    platform: str = Query(""),
    status: str = Query(""),
):
    from core.parasite_seo import get_parasite_pages
    return get_parasite_pages(business_id, platform, status)


@router.get("/pages/{page_id}/content")
def get_page_content(page_id: str):
    import sqlite3
    c = sqlite3.connect("data/storage/seo_engine.db")
    c.row_factory = sqlite3.Row
    row = c.execute("SELECT * FROM parasite_pages WHERE id=?", [page_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Page not found")
    return dict(row)


@router.post("/rankings/check")
def check_rankings(business_id: str = Query(...)):
    from core.parasite_seo import check_parasite_rankings
    return check_parasite_rankings(business_id)


@router.get("/stats")
def parasite_stats(business_id: str = Query(...)):
    from core.parasite_seo import get_parasite_stats
    return get_parasite_stats(business_id)


@router.get("/platforms")
def list_platforms():
    from core.parasite_seo import PLATFORMS
    return PLATFORMS
