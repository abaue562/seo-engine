"""Backlink acquisition API routes."""
from __future__ import annotations
from fastapi import APIRouter, Query
from pydantic import BaseModel

router = APIRouter(prefix="/backlinks", tags=["backlinks"])


class ProspectIn(BaseModel):
    business_id: str
    opportunity_type: str
    target_url: str
    domain_rating: int = 0
    page_title: str = ""
    contact_email: str = ""
    contact_name: str = ""
    anchor_context: str = ""
    your_page_to_link: str = ""
    pitch_angle: str = ""


class OutreachIn(BaseModel):
    business_id: str
    prospect_id: str
    template: str = "initial"
    sender_name: str
    sender_email: str
    your_domain: str
    your_page: str = ""


class QueueRunIn(BaseModel):
    business_id: str
    sender_name: str
    sender_email: str
    your_domain: str


class AcquiredIn(BaseModel):
    business_id: str
    source_url: str
    target_url: str
    anchor_text: str = ""
    domain_rating: int = 0
    is_dofollow: bool = True


@router.post("/prospects")
def add_prospect(body: ProspectIn):
    from core.backlink_prospector import add_prospect as _add
    p = _add(
        business_id=body.business_id,
        opportunity_type=body.opportunity_type,
        target_url=body.target_url,
        domain_rating=body.domain_rating,
        page_title=body.page_title,
        contact_email=body.contact_email,
        contact_name=body.contact_name,
        anchor_context=body.anchor_context,
        your_page_to_link=body.your_page_to_link,
        pitch_angle=body.pitch_angle,
    )
    return {"status": "ok", "prospect": p}


@router.get("/prospects")
def list_prospects(business_id: str = Query(...), status: str = Query("new"), limit: int = Query(50)):
    from core.backlink_prospector import get_prospects
    return {"prospects": get_prospects(business_id, status=status, limit=limit)}


@router.post("/prospects/sweep")
def prospect_sweep(body: dict):
    from core.backlink_prospector import run_prospect_sweep
    result = run_prospect_sweep(body.get("business_id", ""))
    return {"status": "ok", **result}


@router.post("/prospects/{prospect_id}/status")
def update_status(prospect_id: str, body: dict):
    from core.backlink_prospector import update_prospect_status
    update_prospect_status(prospect_id, body.get("status", "new"))
    return {"status": "ok"}


@router.post("/outreach/send")
def send_outreach(body: OutreachIn):
    from core.backlink_outreach import send_outreach as _send
    result = _send(
        business_id=body.business_id,
        prospect_id=body.prospect_id,
        template=body.template,
        sender_name=body.sender_name,
        sender_email=body.sender_email,
        your_domain=body.your_domain,
        your_page=body.your_page,
    )
    return result


@router.post("/outreach/queue")
def run_queue(body: QueueRunIn):
    from core.backlink_outreach import run_outreach_queue
    result = run_outreach_queue(
        business_id=body.business_id,
        sender_name=body.sender_name,
        sender_email=body.sender_email,
        your_domain=body.your_domain,
    )
    return {"status": "ok", **result}


@router.get("/outreach/stats")
def outreach_stats(business_id: str = Query(...)):
    from core.backlink_outreach import get_outreach_stats
    return get_outreach_stats(business_id)


@router.post("/acquired")
def record_acquired(body: AcquiredIn):
    from core.backlink_prospector import record_acquired_backlink
    record_acquired_backlink(
        business_id=body.business_id,
        source_url=body.source_url,
        target_url=body.target_url,
        anchor_text=body.anchor_text,
        domain_rating=body.domain_rating,
        is_dofollow=body.is_dofollow,
    )
    return {"status": "ok"}


@router.get("/acquired/health")
def check_health(business_id: str = Query(...)):
    from core.backlink_prospector import check_backlink_health
    return check_backlink_health(business_id)


@router.get("/stats")
def backlink_stats(business_id: str = Query(...)):
    from core.backlink_prospector import get_backlink_stats
    return get_backlink_stats(business_id)
