"""
Conversion layer API routes — call tracking, CTA, lead capture (Gap #6).
"""
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

router = APIRouter(prefix="/conversion", tags=["conversion"])


# ── Call Tracking ──────────────────────────────────────────────────────────────

class TrackingNumberIn(BaseModel):
    business_id: str
    number: str
    label: str = ""
    source: str = "organic"
    medium: str = ""
    campaign: str = ""


class CallLogIn(BaseModel):
    business_id: str
    tracking_number: str
    caller_number: str = ""
    source: str = ""
    medium: str = ""
    campaign: str = ""
    duration_sec: int = 0
    answered: bool = False
    recording_url: str = ""
    notes: str = ""


@router.post("/numbers")
def add_tracking_number(body: TrackingNumberIn):
    from core.call_tracking import add_tracking_number
    return add_tracking_number(**body.dict())


@router.get("/numbers")
def get_tracking_numbers(
    business_id: str = Query(...),
    source: str = Query(""),
):
    from core.call_tracking import get_tracking_numbers
    return get_tracking_numbers(business_id, source)


@router.post("/calls")
def log_call(body: CallLogIn):
    from core.call_tracking import log_call
    return log_call(**body.dict())


@router.get("/calls/stats")
def get_call_stats(
    business_id: str = Query(...),
    days: int = Query(30),
):
    from core.call_tracking import get_call_stats
    return get_call_stats(business_id, days)


# ── Conversion Events ──────────────────────────────────────────────────────────

class ConversionEventIn(BaseModel):
    business_id: str
    event_type: str
    source: str = ""
    medium: str = ""
    campaign: str = ""
    page_url: str = ""
    value: float = 0.0
    metadata: dict = {}


@router.post("/event")
def log_conversion_event(body: ConversionEventIn):
    from core.call_tracking import log_conversion
    return log_conversion(
        business_id=body.business_id,
        event_type=body.event_type,
        source=body.source,
        medium=body.medium,
        campaign=body.campaign,
        page_url=body.page_url,
        value=body.value,
        metadata=body.metadata,
    )


@router.get("/events/stats")
def get_conversion_stats(
    business_id: str = Query(...),
    days: int = Query(30),
):
    from core.call_tracking import get_conversion_stats
    return get_conversion_stats(business_id, days)


# ── CTA Optimizer ──────────────────────────────────────────────────────────────

class CTAGenerateIn(BaseModel):
    business_id: str
    page_id: str = ""
    service: str
    location: str
    intent: str = ""
    business_name: str = ""
    phone: str = ""


@router.post("/cta/generate")
def generate_cta_variants(body: CTAGenerateIn):
    from core.cta_optimizer import generate_cta_variants
    return generate_cta_variants(
        business_id=body.business_id,
        page_id=body.page_id,
        service=body.service,
        location=body.location,
        intent=body.intent,
        business_name=body.business_name,
        phone=body.phone,
    )


@router.get("/cta/performance")
def get_cta_performance(business_id: str = Query(...)):
    from core.cta_optimizer import get_cta_performance
    return get_cta_performance(business_id)


@router.post("/cta/optimize")
def auto_optimize_cta(business_id: str = Query(...)):
    from core.cta_optimizer import auto_optimize_cta
    return auto_optimize_cta(business_id)


@router.post("/cta/click/{variant_id}")
def track_cta_click(variant_id: str):
    from core.cta_optimizer import track_cta_click
    track_cta_click(variant_id)
    return {"ok": True}


@router.post("/cta/convert/{variant_id}")
def track_cta_conversion(variant_id: str):
    from core.cta_optimizer import track_cta_conversion
    track_cta_conversion(variant_id)
    return {"ok": True}


# ── Lead Capture ───────────────────────────────────────────────────────────────

class LeadIn(BaseModel):
    business_id: str
    name: str = ""
    email: str = ""
    phone: str = ""
    service: str = ""
    message: str = ""
    source: str = "organic"
    medium: str = ""
    campaign: str = ""
    page_url: str = ""
    metadata: dict = {}
    # optional qualifier fields passed through
    project_type: str = ""
    urgency: str = ""
    home_age: str = ""


@router.post("/lead")
def submit_lead(body: LeadIn):
    from core.lead_capture import save_lead, notify_lead, push_to_crm
    import json

    meta = dict(body.metadata)
    for field in ["project_type", "urgency", "home_age"]:
        val = getattr(body, field, "")
        if val:
            meta[field] = val

    result = save_lead(
        business_id=body.business_id,
        name=body.name,
        email=body.email,
        phone=body.phone,
        service=body.service,
        message=body.message,
        source=body.source,
        medium=body.medium,
        campaign=body.campaign,
        page_url=body.page_url,
        metadata=meta,
    )

    # async notification — best effort
    try:
        import os
        notify_email = os.environ.get("LEAD_NOTIFY_EMAIL", "")
        if notify_email:
            lead_data = {**body.dict(), **result}
            notify_lead(body.business_id, lead_data, notify_email)
    except Exception:
        pass

    # push to CRM best effort
    try:
        push_to_crm(body.business_id, result["id"], {**body.dict(), **result})
    except Exception:
        pass

    return {"ok": True, "lead_id": result["id"], "qualified_score": result["qualified_score"]}


@router.get("/leads")
def get_leads(
    business_id: str = Query(...),
    status: str = Query(""),
    days: int = Query(30),
    limit: int = Query(50),
):
    from core.lead_capture import get_leads
    return get_leads(business_id, status, days, limit)


@router.get("/leads/stats")
def get_lead_stats(
    business_id: str = Query(...),
    days: int = Query(30),
):
    from core.lead_capture import get_lead_stats
    return get_lead_stats(business_id, days)


@router.patch("/leads/{lead_id}/status")
def update_lead_status(lead_id: str, status: str = Query(...)):
    from core.lead_capture import update_lead_status
    update_lead_status(lead_id, status)
    return {"ok": True, "lead_id": lead_id, "status": status}


@router.get("/leads/form")
def get_lead_form_html(
    business_id: str = Query(...),
    service: str = Query(...),
    location: str = Query(...),
    form_type: str = Query("quote"),
    phone: str = Query(""),
):
    from core.lead_capture import build_lead_form
    html = build_lead_form(business_id, service, location, form_type, phone)
    return {"html": html}


# ── Dashboard ──────────────────────────────────────────────────────────────────

@router.get("/dashboard")
def conversion_dashboard(
    business_id: str = Query(...),
    days: int = Query(30),
):
    from core.call_tracking import get_call_stats, get_conversion_stats
    from core.lead_capture import get_lead_stats
    from core.cta_optimizer import get_cta_performance

    return {
        "business_id": business_id,
        "days": days,
        "calls": get_call_stats(business_id, days),
        "conversions": get_conversion_stats(business_id, days),
        "leads": get_lead_stats(business_id, days),
        "cta_performance": get_cta_performance(business_id),
    }
