"""Credential vault API routes."""
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter(prefix="/credentials", tags=["credentials"])


class SetCredentialIn(BaseModel):
    business_id: str
    platform: str
    key_name: str
    value: str


@router.get("/platforms")
def list_platforms(business_id: str = Query(...)):
    from core.credential_vault import list_platforms
    return list_platforms(business_id)


@router.get("/platform/{platform}")
def get_platform_creds(platform: str, business_id: str = Query(...)):
    from core.credential_vault import get_platform_credentials
    return get_platform_credentials(business_id, platform)


@router.post("/set")
def set_credential(body: SetCredentialIn):
    from core.credential_vault import set_credential
    return set_credential(body.business_id, body.platform, body.key_name, body.value)


@router.delete("/{platform}/{key_name}")
def delete_credential(platform: str, key_name: str, business_id: str = Query(...)):
    from core.credential_vault import delete_credential
    return delete_credential(business_id, platform, key_name)


@router.get("/available-platforms")
def available_platforms():
    from core.credential_vault import PLATFORM_KEYS
    return [
        {"platform": p, "required_keys": keys}
        for p, keys in PLATFORM_KEYS.items()
    ]
