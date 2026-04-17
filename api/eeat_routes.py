"""E-E-A-T API routes — author management, trust signals, policy pages."""
from __future__ import annotations
import json
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

router = APIRouter(prefix="/eeat", tags=["eeat"])


class AuthorIn(BaseModel):
    business_id: str
    name: str
    title: str = ""
    bio: str = ""
    expertise: list[str] = []
    credentials: list[str] = []
    photo_url: str = ""
    linkedin_url: str = ""
    twitter_url: str = ""
    website_url: str = ""
    is_default: bool = False


class AuthorAssignIn(BaseModel):
    business_id: str
    content_url: str
    author_id: str


class EEATOptimizeIn(BaseModel):
    html: str
    business_id: str
    content_url: str = ""
    breadcrumbs: list[dict] = []
    faqs: list[dict] = []
    reviews: list[dict] = []
    review_count: int = 0
    avg_rating: float = 0.0
    business_name: str = ""


class PolicyIn(BaseModel):
    business_name: str
    domain: str
    contact_email: str
    state: str = "BC, Canada"
    founding_year: int = 2020
    location: str = ""
    description: str = ""
    review_count: int = 0
    avg_rating: float = 0.0
    team_members: list[dict] = []


@router.post("/authors")
def upsert_author(body: AuthorIn):
    from core.author_profiles import upsert_author as _upsert
    author = _upsert(
        business_id=body.business_id,
        name=body.name,
        title=body.title,
        bio=body.bio,
        expertise=body.expertise,
        credentials=body.credentials,
        photo_url=body.photo_url,
        linkedin_url=body.linkedin_url,
        twitter_url=body.twitter_url,
        website_url=body.website_url,
        is_default=body.is_default,
    )
    return {"status": "ok", "author": author}


@router.get("/authors")
def list_authors(business_id: str = Query(...)):
    from core.author_profiles import list_authors as _list
    return {"authors": _list(business_id)}


@router.get("/authors/default")
def get_default_author(business_id: str = Query(...)):
    from core.author_profiles import get_default_author as _get
    author = _get(business_id)
    if not author:
        raise HTTPException(404, "No author found for this tenant")
    return {"author": author}


@router.post("/authors/assign")
def assign_author(body: AuthorAssignIn):
    from core.author_profiles import assign_author as _assign
    _assign(body.business_id, body.content_url, body.author_id)
    return {"status": "ok"}


@router.post("/optimize")
def optimize_eeat(body: EEATOptimizeIn):
    from core.eeat_pipeline import run_eeat_pipeline
    result = run_eeat_pipeline(
        html=body.html,
        business_id=body.business_id,
        content_url=body.content_url,
        breadcrumbs=body.breadcrumbs or None,
        faqs=body.faqs or None,
        reviews=body.reviews or None,
        review_count=body.review_count,
        avg_rating=body.avg_rating,
        business_name=body.business_name,
    )
    return {
        "html": result["html"],
        "score_before": result["score_before"]["total"],
        "score_after": result["score_after"]["total"],
        "passing": result["score_after"]["passing"],
        "missing": result["score_after"]["missing"],
    }


@router.post("/score")
def score_eeat(body: dict):
    from core.eeat_pipeline import score_eeat as _score
    html = body.get("html", "")
    result = _score(html)
    return result


@router.post("/policies/privacy")
def generate_privacy(body: PolicyIn):
    from core.trust_signals import generate_privacy_policy
    html = generate_privacy_policy(body.business_name, body.domain, body.contact_email)
    return {"html": html}


@router.post("/policies/terms")
def generate_terms(body: PolicyIn):
    from core.trust_signals import generate_terms_of_service
    html = generate_terms_of_service(body.business_name, body.domain, body.contact_email, body.state)
    return {"html": html}


@router.post("/policies/editorial")
def generate_editorial(body: PolicyIn):
    from core.trust_signals import generate_editorial_policy
    html = generate_editorial_policy(body.business_name, body.domain)
    return {"html": html}


@router.post("/policies/about")
def generate_about(body: PolicyIn):
    from core.trust_signals import generate_about_page
    html = generate_about_page(
        business_name=body.business_name,
        domain=body.domain,
        founding_year=body.founding_year,
        location=body.location,
        description=body.description,
        team_members=body.team_members or None,
        review_count=body.review_count,
        avg_rating=body.avg_rating,
    )
    return {"html": html}
