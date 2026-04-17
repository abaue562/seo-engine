import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

log = logging.getLogger(__name__)
router = APIRouter(prefix="/geo", tags=["geo"])

class OptimizeRequest(BaseModel):
    business_id: str
    keyword: str
    intent: str = "informational"
    html: str
    page_url: str = ""

class DeployLlmsTxtRequest(BaseModel):
    business_id: str

@router.post("/optimize")
async def optimize_for_geo(req: OptimizeRequest):
    from core.geo_optimizer import optimize_for_geo
    from core.speakable_schema import inject_speakable_into_html
    from core.entity_chains import auto_enrich_content

    biz_context = {}
    try:
        import json
        from pathlib import Path
        all_biz = json.loads(Path("data/storage/businesses.json").read_text())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
        biz_context = next((b for b in biz_list if b.get("id") == req.business_id or b.get("business_id") == req.business_id), {})
    except Exception:
        pass

    geo_result = optimize_for_geo(req.html, req.keyword, req.intent, biz_context, req.business_id)
    html_with_speakable = inject_speakable_into_html(geo_result["html"], req.page_url or "")
    entity_result = auto_enrich_content(html_with_speakable, req.keyword, biz_context, req.business_id)

    return {
        "html": entity_result["html"],
        "geo_score": geo_result["geo_score"],
        "geo_elements": geo_result["geo_elements"],
        "entity_chains": entity_result["entity_chains"],
        "words_added": geo_result["words_added"],
    }

@router.get("/score")
async def geo_score(url: str):
    from core.geo_optimizer import score_geo_readiness
    import requests as req_lib
    try:
        resp = req_lib.get(url, timeout=15)
        score = score_geo_readiness(resp.text)
        return {"url": url, **score}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@router.get("/citation-gaps")
async def citation_gaps(business_id: str, limit: int = 20):
    from core.ai_answer_monitor import get_citation_gaps
    gaps = get_citation_gaps(business_id, limit)
    return {"business_id": business_id, "gaps": gaps, "count": len(gaps)}

@router.get("/citation-wins")
async def citation_wins(business_id: str):
    from core.ai_answer_monitor import get_citation_wins
    wins = get_citation_wins(business_id)
    return {"business_id": business_id, "wins": wins, "count": len(wins)}

@router.get("/llms-txt")
async def get_llms_txt(business_id: str):
    from core.llms_txt_builder import build_llms_txt
    content = build_llms_txt(business_id)
    return {"business_id": business_id, "content": content, "length": len(content)}

@router.post("/deploy-llms-txt")
async def deploy_llms_txt_endpoint(req: DeployLlmsTxtRequest):
    from core.llms_txt_builder import deploy_llms_txt
    ok = deploy_llms_txt(req.business_id)
    return {"status": "deployed" if ok else "failed", "business_id": req.business_id}

@router.get("/entity-chains")
async def entity_chains(business_id: str, keyword: str):
    from core.entity_chains import auto_enrich_content
    import sqlite3
    conn = sqlite3.connect("data/storage/seo_engine.db")
    row = conn.execute("SELECT url FROM published_urls WHERE business_id=? AND keyword=? LIMIT 1", [business_id, keyword]).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="No published URL found for this keyword")
    try:
        import requests as req_lib
        html = req_lib.get(row[0], timeout=15).text
        result = auto_enrich_content(html, keyword, {}, business_id)
        return {"keyword": keyword, "entity_chains": result["entity_chains"], "entity_schema": result["entity_schema"]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
