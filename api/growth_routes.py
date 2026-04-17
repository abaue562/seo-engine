import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

log = logging.getLogger(__name__)
router = APIRouter(prefix="/growth", tags=["growth"])

class ConvertReferralRequest(BaseModel):
    code: str
    referee_business_id: str

class MilestoneRequest(BaseModel):
    business_id: str
    milestone: str

class InviteUserRequest(BaseModel):
    tenant_id: str
    email: str
    role: str = "viewer"
    invited_by: str = ""

@router.get("/health-score")
async def health_score(business_id: str):
    from core.health_score import compute_health_score
    return compute_health_score(business_id)

@router.get("/at-risk")
async def at_risk_tenants(limit: int = 20):
    from core.health_score import get_at_risk_tenants
    return {"tenants": get_at_risk_tenants(limit)}

@router.get("/referral")
async def referral_stats(business_id: str):
    from core.referral import get_referral_stats, create_referral_code
    stats = get_referral_stats(business_id)
    if not stats.get("code"):
        stats["code"] = create_referral_code(business_id)
    return stats

@router.post("/referral/convert")
async def convert_referral(req: ConvertReferralRequest):
    from core.referral import record_conversion
    ok = record_conversion(req.code, req.referee_business_id)
    if not ok:
        raise HTTPException(status_code=400, detail="Invalid or already-used referral code")
    return {"status": "converted"}

@router.get("/expansion-triggers")
async def expansion_triggers(business_id: str):
    from core.expansion import detect_expansion_triggers
    triggers = detect_expansion_triggers(business_id)
    return {"business_id": business_id, "triggers": triggers, "count": len(triggers)}

@router.get("/plan")
async def plan_details(business_id: str):
    from core.pricing import get_tenant_plan, get_plan, calculate_overages
    from core.tenant_users import check_seat_limit
    import sqlite3
    plan_name = get_tenant_plan(business_id)
    plan = get_plan(plan_name)
    conn = sqlite3.connect("data/storage/seo_engine.db")
    pages = conn.execute("SELECT COUNT(*) FROM published_urls WHERE business_id=? AND status='live'", [business_id]).fetchone()[0]
    keywords = conn.execute("SELECT COUNT(DISTINCT keyword) FROM ranking_history WHERE business_id=?", [business_id]).fetchone()[0]
    conn.close()
    overages = calculate_overages(business_id, {"pages_per_month": pages, "keywords": keywords})
    seats = check_seat_limit(business_id)
    return {"business_id": business_id, "plan_name": plan_name, "plan": plan, "usage": {"pages": pages, "keywords": keywords}, "overages": overages, "seats": seats}

@router.get("/funnel")
async def funnel_stage(business_id: str):
    from core.onboarding_funnel import get_funnel_stage
    return get_funnel_stage(business_id)

@router.post("/funnel/milestone")
async def record_funnel_milestone(req: MilestoneRequest):
    from core.onboarding_funnel import record_milestone
    record_milestone(req.business_id, req.milestone)
    return {"status": "recorded"}

@router.get("/users")
async def tenant_users(business_id: str):
    from core.tenant_users import get_tenant_users
    return {"users": get_tenant_users(business_id)}

@router.post("/users/invite")
async def invite_user(req: InviteUserRequest):
    from core.tenant_users import invite_user
    try:
        token = invite_user(req.tenant_id, req.email, req.role, req.invited_by)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not token:
        raise HTTPException(status_code=400, detail="Seat limit reached")
    return {"status": "invited", "token": token}

@router.get("/case-study")
async def case_study(business_id: str):
    from core.case_study import check_case_study_eligible, draft_case_study
    eligibility = check_case_study_eligible(business_id)
    if not eligibility["eligible"]:
        return {"eligible": False, "eligibility": eligibility}
    result = draft_case_study(business_id)
    return {"eligible": True, **result}
