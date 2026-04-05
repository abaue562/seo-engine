"""Google Analytics 4 connector — real behavioral signal data.

Provides: session duration, bounce rate, scroll depth, conversions,
page-level engagement metrics — feeds the self-evolution learning loop.

Setup (two options):

Option A — Service Account (recommended for server use):
  1. GCP Console → APIs → Enable Google Analytics Data API
  2. Create Service Account → download JSON key
  3. In GA4: Admin → Property → Property Access Management → Add service account email
  4. Add to config/.env:
     GA4_PROPERTY_ID=123456789
     GA4_CREDENTIALS_PATH=config/ga4_credentials.json

Option B — OAuth (for personal use):
  1. GCP Console → OAuth credentials (Desktop app)
  2. GA4_CLIENT_ID=xxx  GA4_CLIENT_SECRET=xxx  GA4_REFRESH_TOKEN=xxx

Usage:
    from data.connectors.ga4 import GA4Connector

    ga4 = GA4Connector()
    metrics = ga4.get_page_metrics(days=30)
    signals = ga4.get_behavioral_signals(page_path="/permanent-lights")
    kw_perf = ga4.get_keyword_performance()
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import requests
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

GA4_API_BASE = "https://analyticsdata.googleapis.com/v1beta"
OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"
SCOPE = "https://www.googleapis.com/auth/analytics.readonly"


class PageMetrics(BaseModel):
    """Behavioral metrics for a single page."""
    page_path: str
    sessions: int = 0
    avg_session_duration: float = 0.0    # seconds
    bounce_rate: float = 0.0             # 0-1
    scroll_depth: float = 0.0            # 0-100 average %
    conversions: int = 0
    conversion_rate: float = 0.0
    entrances: int = 0
    exits: int = 0
    pageviews: int = 0
    new_users: int = 0


class BehavioralSignals(BaseModel):
    """Aggregated behavioral signals for the evolution engine."""
    # Overall site signals
    avg_session_duration: float = 0.0
    avg_bounce_rate: float = 0.0
    avg_pages_per_session: float = 0.0
    total_conversions: int = 0
    overall_conversion_rate: float = 0.0

    # Per-page signals
    page_metrics: list[PageMetrics] = Field(default_factory=list)

    # Keyword-level signals (from GSC x GA4 join — estimated)
    top_converting_pages: list[str] = Field(default_factory=list)
    underperforming_pages: list[str] = Field(default_factory=list)  # high traffic, low conversion

    # For self-evolution input
    ctr_change: float = 0.0           # Estimated from session data change
    conversion_rate: float = 0.0      # Actual sitewide conversion rate

    period_days: int = 30
    fetched_at: str = ""


class GA4Connector:
    """Google Analytics 4 Data API connector."""

    def __init__(
        self,
        property_id: str = "",
        credentials_path: str = "",
    ):
        self.property_id = property_id or os.getenv("GA4_PROPERTY_ID", "")
        self.credentials_path = credentials_path or os.getenv("GA4_CREDENTIALS_PATH", "config/ga4_credentials.json")
        self._access_token: str = ""
        self._token_expiry: float = 0.0

    def is_configured(self) -> bool:
        return bool(self.property_id) and (
            os.path.exists(self.credentials_path) or
            os.getenv("GA4_REFRESH_TOKEN")
        )

    def _get_access_token(self) -> str:
        """Get/refresh OAuth access token."""
        import time
        if self._access_token and time.time() < self._token_expiry - 60:
            return self._access_token

        # Try service account first
        if os.path.exists(self.credentials_path):
            try:
                import google.auth.transport.requests
                from google.oauth2 import service_account
                creds = service_account.Credentials.from_service_account_file(
                    self.credentials_path,
                    scopes=[SCOPE],
                )
                creds.refresh(google.auth.transport.requests.Request())
                self._access_token = creds.token
                import time as t
                self._token_expiry = t.time() + 3600
                return self._access_token
            except ImportError:
                log.warning("ga4.missing_google_auth  pip install google-auth")
            except Exception as e:
                log.error("ga4.service_account_fail  err=%s", e)

        # Try OAuth refresh token
        refresh_token = os.getenv("GA4_REFRESH_TOKEN", "")
        client_id = os.getenv("GA4_CLIENT_ID", "")
        client_secret = os.getenv("GA4_CLIENT_SECRET", "")

        if refresh_token and client_id:
            try:
                resp = requests.post(OAUTH_TOKEN_URL, data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": client_id,
                    "client_secret": client_secret,
                }, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                self._access_token = data["access_token"]
                import time as t
                self._token_expiry = t.time() + data.get("expires_in", 3600)
                return self._access_token
            except Exception as e:
                log.error("ga4.oauth_refresh_fail  err=%s", e)

        return ""

    def _run_report(
        self,
        dimensions: list[str],
        metrics: list[str],
        date_range_days: int = 30,
        dimension_filter: dict | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Run a GA4 Data API report."""
        if not self.is_configured():
            log.warning("ga4.not_configured")
            return []

        token = self._get_access_token()
        if not token:
            log.error("ga4.no_token")
            return []

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=date_range_days)).strftime("%Y-%m-%d")

        body: dict = {
            "dateRanges": [{"startDate": start_date, "endDate": end_date}],
            "dimensions": [{"name": d} for d in dimensions],
            "metrics": [{"name": m} for m in metrics],
            "limit": limit,
            "orderBys": [{"metric": {"metricName": metrics[0]}, "desc": True}],
        }
        if dimension_filter:
            body["dimensionFilter"] = dimension_filter

        try:
            resp = requests.post(
                f"{GA4_API_BASE}/properties/{self.property_id}:runReport",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=body,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error("ga4.report_fail  err=%s", e)
            return []

        dim_headers = [h["name"] for h in data.get("dimensionHeaders", [])]
        met_headers = [h["name"] for h in data.get("metricHeaders", [])]

        rows = []
        for row in data.get("rows", []):
            record: dict = {}
            for i, dim in enumerate(row.get("dimensionValues", [])):
                if i < len(dim_headers):
                    record[dim_headers[i]] = dim.get("value", "")
            for i, met in enumerate(row.get("metricValues", [])):
                if i < len(met_headers):
                    try:
                        record[met_headers[i]] = float(met.get("value", 0))
                    except ValueError:
                        record[met_headers[i]] = 0.0
            rows.append(record)

        return rows

    # ── Public API ─────────────────────────────────────────────────

    def get_page_metrics(self, days: int = 30, limit: int = 50) -> list[PageMetrics]:
        """Get per-page behavioral metrics."""
        rows = self._run_report(
            dimensions=["pagePath"],
            metrics=[
                "sessions",
                "averageSessionDuration",
                "bounceRate",
                "conversions",
                "screenPageViews",
                "newUsers",
                "entrances",
            ],
            date_range_days=days,
            limit=limit,
        )

        pages = []
        for row in rows:
            sessions = int(row.get("sessions", 0))
            conversions = int(row.get("conversions", 0))
            pages.append(PageMetrics(
                page_path=row.get("pagePath", ""),
                sessions=sessions,
                avg_session_duration=round(row.get("averageSessionDuration", 0), 1),
                bounce_rate=round(row.get("bounceRate", 0), 3),
                conversions=conversions,
                conversion_rate=round(conversions / sessions, 3) if sessions else 0.0,
                pageviews=int(row.get("screenPageViews", 0)),
                new_users=int(row.get("newUsers", 0)),
                entrances=int(row.get("entrances", 0)),
            ))

        log.info("ga4.page_metrics  pages=%d  days=%d", len(pages), days)
        return pages

    def get_behavioral_signals(self, days: int = 30) -> BehavioralSignals:
        """Get aggregated behavioral signals for the self-evolution engine."""
        pages = self.get_page_metrics(days=days, limit=100)

        if not pages:
            return BehavioralSignals(period_days=days, fetched_at=datetime.now().isoformat())

        total_sessions = sum(p.sessions for p in pages) or 1
        total_conversions = sum(p.conversions for p in pages)

        avg_duration = sum(p.avg_session_duration * p.sessions for p in pages) / total_sessions
        avg_bounce = sum(p.bounce_rate * p.sessions for p in pages) / total_sessions
        overall_cr = total_conversions / total_sessions

        # Identify top converting pages
        top_converting = sorted(pages, key=lambda p: p.conversion_rate, reverse=True)
        underperforming = [
            p for p in pages
            if p.sessions > 50 and p.conversion_rate < overall_cr * 0.3
        ]

        # Estimate CTR change: session change vs previous period
        # (Rough proxy — ideally compare two periods)
        ctr_proxy = overall_cr / 0.03 - 1.0  # deviation from 3% baseline

        signals = BehavioralSignals(
            avg_session_duration=round(avg_duration, 1),
            avg_bounce_rate=round(avg_bounce, 3),
            total_conversions=total_conversions,
            overall_conversion_rate=round(overall_cr, 4),
            page_metrics=pages,
            top_converting_pages=[p.page_path for p in top_converting[:5]],
            underperforming_pages=[p.page_path for p in underperforming[:5]],
            ctr_change=round(ctr_proxy, 3),
            conversion_rate=round(overall_cr, 4),
            period_days=days,
            fetched_at=datetime.now().isoformat(),
        )

        log.info("ga4.signals  sessions=%d  conversions=%d  cr=%.2f%%  avg_duration=%.0fs",
                 total_sessions, total_conversions, overall_cr * 100, avg_duration)
        return signals

    def get_keyword_performance(self, days: int = 30) -> list[dict]:
        """Get landing page performance — proxy for keyword-level behavioral signals."""
        rows = self._run_report(
            dimensions=["landingPage", "sessionDefaultChannelGroup"],
            metrics=["sessions", "conversions", "averageSessionDuration", "bounceRate"],
            date_range_days=days,
            dimension_filter={
                "filter": {
                    "fieldName": "sessionDefaultChannelGroup",
                    "stringFilter": {"matchType": "EXACT", "value": "Organic Search"},
                }
            },
            limit=50,
        )

        return [
            {
                "page": row.get("landingPage", ""),
                "organic_sessions": int(row.get("sessions", 0)),
                "conversions": int(row.get("conversions", 0)),
                "avg_duration": round(row.get("averageSessionDuration", 0), 1),
                "bounce_rate": round(row.get("bounceRate", 0), 3),
            }
            for row in rows
        ]
