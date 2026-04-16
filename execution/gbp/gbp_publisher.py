"""Google Business Profile post automation.

Two modes:
1. API mode: Posts directly via Google My Business API (requires OAuth2 credentials)
2. Draft mode: Generates post content and saves to disk for manual copy-paste

GBP post types supported:
- STANDARD (general update)
- EVENT (seasonal promotions)
- OFFER (discounts/specials)
"""
from __future__ import annotations
import logging
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class GBPPost:
    summary: str          # Main post text (up to 1500 chars)
    call_to_action: str = "CALL"  # BOOK, CALL, LEARN_MORE, ORDER, SHOP, SIGN_UP
    cta_url: str = ""
    post_type: str = "STANDARD"   # STANDARD, EVENT, OFFER
    event_title: str = ""
    offer_coupon: str = ""
    photo_url: str = ""


class GBPPublisher:
    """Posts to Google Business Profile via API or saves drafts."""

    def __init__(self, account_id: str = "", location_id: str = "",
                 credentials_path: str = ""):
        self.account_id = account_id
        self.location_id = location_id
        self.credentials_path = credentials_path
        self._has_credentials = bool(account_id and location_id and credentials_path)

    def post(self, post: GBPPost) -> dict:
        """Publish a GBP post. Falls back to draft if no credentials."""
        if self._has_credentials:
            return self._post_via_api(post)
        else:
            return self._save_draft(post)

    def _post_via_api(self, post: GBPPost) -> dict:
        try:
            from google.oauth2.credentials import Credentials
            from googleapiclient.discovery import build

            creds = Credentials.from_authorized_user_file(self.credentials_path)
            service = build("mybusinesspostings", "v1", credentials=creds)

            body = {
                "languageCode": "en-US",
                "summary": post.summary,
                "callToAction": {
                    "actionType": post.call_to_action,
                    "url": post.cta_url,
                },
                "topicType": post.post_type,
            }
            if post.event_title:
                body["event"] = {
                    "title": post.event_title,
                    "schedule": {
                        "startDate": {"year": datetime.now().year, "month": datetime.now().month, "day": datetime.now().day},
                        "endDate": {"year": datetime.now().year, "month": datetime.now().month + 1, "day": 1},
                    }
                }

            name = f"accounts/{self.account_id}/locations/{self.location_id}"
            result = service.locations().localPosts().create(parent=name, body=body).execute()
            log.info("gbp.post_ok  name=%s", result.get("name"))
            return {"status": "posted", "name": result.get("name")}

        except Exception as e:
            log.warning("gbp.api_fail  err=%s  falling_back_to_draft", e)
            return self._save_draft(post)

    def _save_draft(self, post: GBPPost) -> dict:
        drafts_dir = Path("data/storage/gbp_drafts")
        drafts_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        draft_file = drafts_dir / f"gbp_post_{ts}.json"
        draft = {
            "summary": post.summary,
            "call_to_action": post.call_to_action,
            "cta_url": post.cta_url,
            "post_type": post.post_type,
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "status": "draft_ready_for_manual_post",
            "instructions": "Copy summary text and post manually at business.google.com",
        }
        draft_file.write_text(json.dumps(draft, indent=2))
        log.info("gbp.draft_saved  path=%s", draft_file)
        return {"status": "draft", "path": str(draft_file), "summary_preview": post.summary[:100]}
