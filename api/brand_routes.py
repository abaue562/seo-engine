"""Brand entity and Knowledge Graph API routes."""
from __future__ import annotations
from fastapi import APIRouter, Query
from pydantic import BaseModel

router = APIRouter(prefix="/brand", tags=["brand"])


class EntityIn(BaseModel):
    business_id: str
    entity_name: str
    entity_type: str = "LocalBusiness"
    description: str = ""
    founding_year: int = 0
    location: str = ""
    address: str = ""
    phone: str = ""
    email: str = ""
    website: str = ""
    logo_url: str = ""


class SameAsIn(BaseModel):
    business_id: str
    platform: str
    url: str
    verified: bool = False


class InjectSchemaIn(BaseModel):
    html: str
    business_id: str


@router.post("/entity")
def upsert_entity(body: EntityIn):
    from core.brand_entity import upsert_brand_entity
    entity = upsert_brand_entity(
        business_id=body.business_id,
        entity_name=body.entity_name,
        entity_type=body.entity_type,
        description=body.description,
        founding_year=body.founding_year,
        location=body.location,
        address=body.address,
        phone=body.phone,
        email=body.email,
        website=body.website,
        logo_url=body.logo_url,
    )
    return {"status": "ok", "entity": entity}


@router.get("/entity")
def get_entity(business_id: str = Query(...)):
    from core.brand_entity import get_brand_entity
    return get_brand_entity(business_id)


@router.post("/same-as")
def add_same_as(body: SameAsIn):
    from core.brand_entity import add_same_as as _add
    _add(body.business_id, body.platform, body.url, body.verified)
    return {"status": "ok"}


@router.get("/same-as")
def list_same_as(business_id: str = Query(...)):
    from core.brand_entity import get_same_as
    return {"same_as": get_same_as(business_id)}


@router.post("/schema/build")
def build_schema(body: dict):
    from core.brand_entity import build_organization_schema
    schema = build_organization_schema(body.get("business_id", ""))
    return {"schema": schema}


@router.post("/schema/inject")
def inject_schema(body: InjectSchemaIn):
    from core.brand_entity import inject_organization_schema
    html = inject_organization_schema(body.html, body.business_id)
    return {"html": html}


@router.post("/kg/publish")
def publish_kg(body: dict):
    from core.brand_entity import publish_to_knowledge_graph
    result = publish_to_knowledge_graph(body.get("business_id", ""))
    return result


@router.get("/kg/query")
def query_kg(business_id: str = Query(...)):
    from core.brand_entity import get_brand_entity
    from core.aion_bridge import aion
    entity = get_brand_entity(business_id)
    if not entity:
        return {"nodes": [], "edges": []}
    return aion.knowledge_query(entity["entity_name"])


@router.post("/mentions/find")
def find_mentions(body: dict):
    from core.brand_entity import find_entity_mentions, get_brand_entity
    biz_id = body.get("business_id", "")
    entity = get_brand_entity(biz_id)
    brand_name = body.get("brand_name", entity.get("entity_name", ""))
    mentions = find_entity_mentions(biz_id, brand_name)
    return {"mentions": mentions, "count": len(mentions)}


@router.get("/score")
def entity_score(business_id: str = Query(...)):
    from core.brand_entity import score_entity_strength
    return score_entity_strength(business_id)


@router.post("/sweep")
def entity_sweep(body: dict):
    from core.brand_entity import run_entity_sweep
    return run_entity_sweep(body.get("business_id", ""))


@router.get("/wikidata")
def check_wikidata(brand_name: str = Query(...)):
    from core.brand_entity import check_wikidata_presence
    qid = check_wikidata_presence(brand_name)
    return {"wikidata_qid": qid, "found": qid is not None,
            "url": f"https://www.wikidata.org/wiki/{qid}" if qid else None}
