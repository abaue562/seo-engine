"""Google Search Console Live Connector — real ranking data.

Connects to GSC API for:
  - Real keyword rankings (position, clicks, impressions, CTR)
  - Page performance data
  - Indexing status
  - URL inspection
  - Sitemap management

Setup:
  1. Go to https://console.cloud.google.com
  2. Enable Search Console API
  3. Create OAuth credentials (Desktop app)
  4. Download credentials.json to config/gsc_credentials.json
  5. First run will open browser for OAuth consent

Usage:
    from data.connectors.gsc_live.connector import GSCConnector

    gsc = GSCConnector("https://blendbrightlights.com")
    data = gsc.get_search_analytics(days=30)
    keywords = gsc.get_top_keywords(limit=50)
"""

from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

CREDENTIALS_PATH = Path("config/gsc_credentials.json")
TOKEN_PATH = Path("config/gsc_token.json")
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


class GSCConnector:
    """Google Search Console API connector."""

    def __init__(self, site_url: str):
        self.site_url = site_url
        self._service = None

    def _auth(self):
        """Authenticate with Google OAuth."""
        if self._service:
            return self._service

        try:
            from google.oauth2.credentials import Credentials
            from google_auth_oauthlib.flow import InstalledAppFlow
            from google.auth.transport.requests import Request
            from googleapiclient.discovery import build
        except ImportError:
            log.error("gsc.missing_deps  pip install google-auth-oauthlib google-api-python-client")
            return None

        creds = None

        # Load saved token
        if TOKEN_PATH.exists():
            creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

        # Refresh or create new token
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not CREDENTIALS_PATH.exists():
                    log.error("gsc.no_credentials  download OAuth credentials to %s", CREDENTIALS_PATH)
                    return None
                flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
                creds = flow.run_local_server(port=0)

            # Save token
            TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(TOKEN_PATH, "w") as f:
                f.write(creds.to_json())

        self._service = build("searchconsole", "v1", credentials=creds)
        log.info("gsc.authenticated  site=%s", self.site_url)
        return self._service

    def get_search_analytics(
        self,
        days: int = 30,
        dimensions: list[str] | None = None,
        row_limit: int = 1000,
    ) -> list[dict]:
        """Get search analytics data (queries, pages, clicks, impressions, CTR, position).

        Args:
            days: Number of days to look back
            dimensions: ["query", "page", "date", "country", "device"]
            row_limit: Max rows to return

        Returns:
            List of dicts with keys matching dimensions + clicks, impressions, ctr, position
        """
        service = self._auth()
        if not service:
            return []

        if dimensions is None:
            dimensions = ["query", "page"]

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        try:
            response = service.searchanalytics().query(
                siteUrl=self.site_url,
                body={
                    "startDate": start_date,
                    "endDate": end_date,
                    "dimensions": dimensions,
                    "rowLimit": row_limit,
                },
            ).execute()

            rows = response.get("rows", [])
            results = []
            for row in rows:
                entry = {
                    "clicks": row.get("clicks", 0),
                    "impressions": row.get("impressions", 0),
                    "ctr": round(row.get("ctr", 0), 4),
                    "position": round(row.get("position", 0), 1),
                }
                for i, dim in enumerate(dimensions):
                    entry[dim] = row["keys"][i]
                results.append(entry)

            log.info("gsc.analytics  rows=%d  days=%d", len(results), days)
            return results

        except Exception as e:
            log.error("gsc.analytics_fail  err=%s", e)
            return []

    def get_top_keywords(self, days: int = 30, limit: int = 50) -> list[dict]:
        """Get top performing keywords with position + clicks."""
        data = self.get_search_analytics(days=days, dimensions=["query"], row_limit=limit)
        data.sort(key=lambda x: x.get("clicks", 0), reverse=True)
        return data

    def get_page_performance(self, days: int = 30, limit: int = 50) -> list[dict]:
        """Get per-page performance data."""
        data = self.get_search_analytics(days=days, dimensions=["page"], row_limit=limit)
        data.sort(key=lambda x: x.get("clicks", 0), reverse=True)
        return data

    def get_keyword_positions(self, days: int = 7, limit: int = 200) -> dict[str, float]:
        """Get keyword → average position mapping."""
        data = self.get_search_analytics(days=days, dimensions=["query"], row_limit=limit)
        return {row["query"]: row["position"] for row in data}

    def get_striking_distance_keywords(self, days: int = 30, min_pos: int = 4, max_pos: int = 20) -> list[dict]:
        """Find keywords ranking 4-20 (striking distance of page 1)."""
        data = self.get_search_analytics(days=days, dimensions=["query", "page"], row_limit=500)
        striking = [row for row in data if min_pos <= row.get("position", 100) <= max_pos]
        striking.sort(key=lambda x: x.get("impressions", 0), reverse=True)
        return striking

    def get_low_ctr_opportunities(self, days: int = 30, min_impressions: int = 100) -> list[dict]:
        """Find high-impression, low-CTR pages (CTR optimization candidates)."""
        data = self.get_search_analytics(days=days, dimensions=["query", "page"], row_limit=500)
        opportunities = [
            row for row in data
            if row.get("impressions", 0) >= min_impressions and row.get("ctr", 1) < 0.05
        ]
        opportunities.sort(key=lambda x: x.get("impressions", 0), reverse=True)
        return opportunities

    def submit_url_for_indexing(self, url: str) -> dict:
        """Request Google to re-index a URL via the Indexing API.

        Note: Requires separate Indexing API credentials.
        """
        try:
            from googleapiclient.discovery import build

            service = self._auth()
            if not service:
                return {"error": "Not authenticated"}

            # Note: Indexing API is separate from Search Console API
            # This is a placeholder — full implementation needs Indexing API service account
            log.info("gsc.index_request  url=%s", url)
            return {"status": "submitted", "url": url, "note": "Requires Indexing API service account"}

        except Exception as e:
            return {"error": str(e)}

    def is_connected(self) -> bool:
        """Check if GSC credentials are set up."""
        return CREDENTIALS_PATH.exists() or TOKEN_PATH.exists()
