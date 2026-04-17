"""Onboarding wizard API routes."""
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional
import os

router = APIRouter(prefix="/onboarding", tags=["onboarding"])


class OnboardingProfile(BaseModel):
    business_id: str
    business_name: str
    location: str
    niche: str = "home services"
    services: List[str] = []
    phone: str = ""
    domain: str = ""
    owner_name: str = ""
    bio: str = ""
    role: str = "Owner"
    homepage_html: str = ""


@router.get("")
@router.get("/")
def serve_onboarding_html():
    path = "static/onboarding.html"
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    raise HTTPException(status_code=404, detail="onboarding.html not found")


@router.post("/start")
def start_onboarding(profile: OnboardingProfile):
    from taskq.tasks import run_onboarding_task
    result = run_onboarding_task.delay(profile.model_dump())
    return {"job_id": result.id, "status": "queued"}


@router.get("/status/{job_id}")
def get_status(job_id: str):
    from core.onboarding_orchestrator import get_job_status
    status = get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    return status


@router.get("/jobs/{business_id}")
def list_jobs(business_id: str):
    from core.onboarding_orchestrator import list_jobs
    return list_jobs(business_id)
