"""Google Search Console connector — pulls real ranking + traffic data.

Requires OAuth2 credentials (Desktop app) stored at GSC_CREDENTIALS_PATH.
First run will open a browser for auth; subsequent runs use the saved token.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import BaseModel

from config.settings import GSC_CREDENTIALS_PATH, GSC_TOKEN_PATH

log = logging.getLogger(__name__)


class GSCRow(BaseModel):
    keyword: str
    page: str
    clicks: int
    impressions: int
    ctr: float
    position: float


class GSCData(BaseModel):
    site_url: str
    rows: list[GSCRow] = []
    fetched_at: datetime = datetime.utcnow()
    start_date: str = ""
    end_date: str = ""


def _get_service():
    """Build authenticated GSC service."""
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
    creds = None
    token_path = Path(GSC_TOKEN_PATH)

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(GSC_CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json())

    return build("searchconsole", "v1", credentials=creds)


async def fetch_gsc(
    site_url: str,
    days_back: int = 30,
    row_limit: int = 1000,
) -> GSCData:
    """Pull query + page performance from GSC."""
    end = datetime.utcnow().date()
    start = end - timedelta(days=days_back)

    log.info("gsc.fetch  site=%s  range=%s→%s", site_url, start, end)

    service = _get_service()
    response = service.searchanalytics().query(
        siteUrl=site_url,
        body={
            "startDate": str(start),
            "endDate": str(end),
            "dimensions": ["query", "page"],
            "rowLimit": row_limit,
        },
    ).execute()

    rows = []
    for r in response.get("rows", []):
        rows.append(GSCRow(
            keyword=r["keys"][0],
            page=r["keys"][1],
            clicks=r.get("clicks", 0),
            impressions=r.get("impressions", 0),
            ctr=round(r.get("ctr", 0), 4),
            position=round(r.get("position", 0), 1),
        ))

    log.info("gsc.fetched  rows=%d", len(rows))

    return GSCData(
        site_url=site_url,
        rows=rows,
        fetched_at=datetime.utcnow(),
        start_date=str(start),
        end_date=str(end),
    )


def gsc_to_rankings(data: GSCData) -> dict[str, float]:
    """Convert GSC data to keyword → avg position map for BusinessContext."""
    rankings = {}
    for row in data.rows:
        if row.keyword not in rankings or row.position < rankings[row.keyword]:
            rankings[row.keyword] = row.position
    return {k: round(v) for k, v in rankings.items()}


def gsc_to_prompt_block(data: GSCData, top_n: int = 20) -> str:
    """Render GSC data as a context block for agents."""
    sorted_rows = sorted(data.rows, key=lambda r: r.clicks, reverse=True)[:top_n]
    lines = [
        f"GOOGLE SEARCH CONSOLE DATA ({data.start_date} → {data.end_date}):",
        f"Total queries tracked: {len(data.rows)}",
        "",
        "Top performing queries:",
    ]
    for r in sorted_rows:
        lines.append(f"  [{r.keyword}] pos={r.position} clicks={r.clicks} imp={r.impressions} ctr={r.ctr:.1%}")

    return "\n".join(lines)
