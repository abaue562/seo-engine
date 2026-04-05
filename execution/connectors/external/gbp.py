"""GBP Connector — queues Google Business Profile posts locally for copy-paste posting.

Google Business Profile API requires manual OAuth setup. This connector:
  1. Saves posts to gbp_queue/ folder with full content
  2. Prints a direct link to your GBP post page
  3. Returns status "queued" so the execution engine tracks them

To use the full GBP API:
  - See google-auth library setup
  - Requires OAuth flow at: https://mybusiness.googleapis.com/v4/accounts/{account_id}/locations/{location_id}/localPosts
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

from execution.connectors.base import Connector, PublishResult

log = logging.getLogger(__name__)

GBP_QUEUE_DIR = Path(__file__).parents[4] / "gbp_queue"


class GBPConnector(Connector):
    platform = "gbp"

    def __init__(self, credentials_path: str = ""):
        self.credentials_path = credentials_path
        GBP_QUEUE_DIR.mkdir(exist_ok=True)

    async def publish(self, payload: dict) -> PublishResult:
        """Queue a GBP post for manual posting."""
        text = payload.get("text", "")
        cta = payload.get("cta", "")
        full_text = f"{text}\n\n{cta}".strip() if cta else text

        if not full_text:
            return PublishResult(platform="gbp", status="failed", error="No text provided")

        # Save to queue folder with timestamp
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = GBP_QUEUE_DIR / f"gbp_post_{ts}.txt"

        post_data = {
            "queued_at": datetime.utcnow().isoformat(),
            "text": full_text,
            "char_count": len(full_text),
            "post_url": "https://business.google.com/u/0/dashboard/l/",
            "instructions": [
                "1. Go to business.google.com",
                "2. Click 'Posts' in the left sidebar",
                "3. Click 'Add update'",
                "4. Paste the text below",
                "5. Add a photo if available",
                "6. Click 'Publish'",
            ],
        }

        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"=== GBP POST — {ts} ===\n\n")
            f.write(full_text)
            f.write(f"\n\n=== INSTRUCTIONS ===\n")
            for step in post_data["instructions"]:
                f.write(f"{step}\n")

        log.info("gbp.queued  file=%s  chars=%d", filename.name, len(full_text))

        return PublishResult(
            platform="gbp",
            status="queued",
            url=f"file://{filename}",
            post_id=ts,
        )
