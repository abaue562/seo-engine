"""GSC Performance API — queries, pages, CTR, position data.

Pulls the Search Analytics report (not just URL Inspection / Coverage).
GSC has a ~2 day reporting lag; we pull the last 3 days to stay current.

Usage:
    from data.connectors.gsc_performance import GSCPerformanceConnector
    gsc = GSCPerformanceConnector("https://blendbrightlights.com")
    data = gsc.get_top_queries(days=30, limit=50)
    opps = gsc.get_opportunities()
"""
from __future__ import annotations
import logging, os
from datetime import datetime, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

CREDENTIALS_PATH = Path("config/gsc_credentials.json")
TOKEN_PATH = Path("config/gsc_token.json")
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


class GSCPerformanceConnector:
    def __init__(self, site_url: str):
        self.site_url = site_url
        self._service = None

    def _auth(self):
        if self._service:
            return self._service
        try:
            from google.oauth2.credentials import Credentials
            from google_auth_oauthlib.flow import InstalledAppFlow
            from google.auth.transport.requests import Request
            from googleapiclient.discovery import build

            creds = None
            if TOKEN_PATH.exists():
                creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    if not CREDENTIALS_PATH.exists():
                        log.error("gsc_perf.no_credentials  path=%s", CREDENTIALS_PATH)
                        return None
                    flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
                    creds = flow.run_local_server(port=0)
                TOKEN_PATH.write_text(creds.to_json())
            self._service = build("searchconsole", "v1", credentials=creds)
            return self._service
        except Exception as e:
            log.error("gsc_perf.auth_fail  err=%s", e)
            return None

    def _date_range(self, days: int) -> tuple[str, str]:
        end = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")
        start = (datetime.utcnow() - timedelta(days=days + 2)).strftime("%Y-%m-%d")
        return start, end

    def _query(self, body: dict) -> list[dict]:
        svc = self._auth()
        if not svc:
            return []
        try:
            resp = svc.searchanalytics().query(siteUrl=self.site_url, body=body).execute()
            return resp.get("rows", [])
        except Exception as e:
            log.error("gsc_perf.query_fail  err=%s", e)
            return []

    def get_top_queries(self, days: int = 30, limit: int = 50) -> list[dict]:
        """Top queries by clicks in the date range."""
        start, end = self._date_range(days)
        rows = self._query({
            "startDate": start, "endDate": end,
            "dimensions": ["query"],
            "rowLimit": limit,
            "orderBy": [{"fieldName": "clicks", "sortOrder": "DESCENDING"}],
        })
        return [{"query": r["keys"][0], "clicks": r["clicks"],
                 "impressions": r["impressions"], "ctr": round(r["ctr"] * 100, 2),
                 "position": round(r["position"], 1)} for r in rows]

    def get_top_pages(self, days: int = 30, limit: int = 50) -> list[dict]:
        """Top pages by clicks."""
        start, end = self._date_range(days)
        rows = self._query({
            "startDate": start, "endDate": end,
            "dimensions": ["page"],
            "rowLimit": limit,
            "orderBy": [{"fieldName": "clicks", "sortOrder": "DESCENDING"}],
        })
        return [{"page": r["keys"][0], "clicks": r["clicks"],
                 "impressions": r["impressions"], "ctr": round(r["ctr"] * 100, 2),
                 "position": round(r["position"], 1)} for r in rows]

    def get_opportunities(self, days: int = 30) -> list[dict]:
        """Queries ranking 4-20 with > 100 impressions — easy wins for optimization."""
        start, end = self._date_range(days)
        rows = self._query({
            "startDate": start, "endDate": end,
            "dimensions": ["query", "page"],
            "rowLimit": 200,
            "dimensionFilterGroups": [{"filters": [
                {"dimension": "position", "operator": "greaterThan", "expression": "3"},
            ]}],
        })
        opps = []
        for r in rows:
            pos = r["position"]
            impr = r["impressions"]
            if 3 < pos <= 20 and impr >= 100:
                opps.append({
                    "query": r["keys"][0], "page": r["keys"][1],
                    "position": round(pos, 1), "impressions": impr,
                    "clicks": r["clicks"], "ctr": round(r["ctr"] * 100, 2),
                    "opportunity_score": round(impr * (1 / pos), 1),
                })
        opps.sort(key=lambda x: x["opportunity_score"], reverse=True)
        return opps[:50]

    def get_losing_queries(self, days: int = 14) -> list[dict]:
        """Queries that lost position week over week."""
        start_prev, end_prev = self._date_range(days * 2)
        start_curr, end_curr = self._date_range(days)
        prev_rows = {r["keys"][0]: r["position"] for r in self._query(
            {"startDate": start_prev, "endDate": end_prev, "dimensions": ["query"], "rowLimit": 200})}
        curr_rows = self._query(
            {"startDate": start_curr, "endDate": end_curr, "dimensions": ["query"], "rowLimit": 200})
        losers = []
        for r in curr_rows:
            q = r["keys"][0]
            curr_pos = r["position"]
            prev_pos = prev_rows.get(q)
            if prev_pos and curr_pos > prev_pos + 1:
                losers.append({
                    "query": q, "current_position": round(curr_pos, 1),
                    "prev_position": round(prev_pos, 1),
                    "delta": round(curr_pos - prev_pos, 1),
                    "impressions": r["impressions"], "clicks": r["clicks"],
                })
        losers.sort(key=lambda x: x["delta"], reverse=True)
        return losers[:30]
