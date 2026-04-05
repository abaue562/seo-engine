"""DataForSEO API connector — keyword volume, SERP data, backlinks.

DataForSEO provides real keyword search volume, CPC, difficulty, SERP features,
and backlink profiles. Uses HTTP Basic Auth.

Setup:
  1. Register at dataforseo.com (free trial available)
  2. Add to config/.env:
     DATAFORSEO_LOGIN=your@email.com
     DATAFORSEO_PASSWORD=your_api_password

Pricing (approx):
  - Keywords Data: $0.0005/keyword (~$0.50 per 1,000)
  - SERP: $0.0006/request
  - Backlinks: $0.003/domain profile

Usage:
    from data.connectors.dataforseo import DataForSEOClient

    client = DataForSEOClient()
    volumes = client.get_keyword_volumes(["permanent lights kelowna", "landscape lighting kelowna"])
    serp = client.get_serp("permanent lights kelowna", location_code=2124)
    backlinks = client.get_backlink_profile("gemstonebyoutdoor.com")
"""

from __future__ import annotations

import json
import logging
import os
import time
from base64 import b64encode

import requests
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

BASE_URL = "https://api.dataforseo.com/v3"

# DataForSEO location codes
LOCATIONS = {
    "ca": 2124,     # Canada
    "us": 2840,     # United States
    "gb": 2826,     # United Kingdom
    "au": 2036,     # Australia
}

# Language codes
LANGUAGES = {
    "en": "en",
    "fr": "fr",
}


class KeywordMetrics(BaseModel):
    keyword: str
    search_volume: int = 0           # Monthly search volume
    cpc: float = 0.0                 # Cost per click (USD)
    competition: float = 0.0         # 0-1 advertiser competition
    competition_level: str = ""      # LOW / MEDIUM / HIGH
    keyword_difficulty: int = 0      # 0-100 SEO difficulty estimate
    trend: list[int] = Field(default_factory=list)  # 12-month search volume trend
    intent: str = ""                 # informational / navigational / commercial / transactional
    related: list[str] = Field(default_factory=list)


class SERPFeatures(BaseModel):
    has_featured_snippet: bool = False
    has_ai_overview: bool = False
    has_paa: bool = False
    has_local_pack: bool = False
    has_image_pack: bool = False
    has_video: bool = False
    has_knowledge_panel: bool = False
    snippet_type: str = ""           # paragraph / list / table / video / none
    snippet_url: str = ""
    snippet_text: str = ""


class BacklinkProfile(BaseModel):
    domain: str
    domain_rank: int = 0             # 0-100 domain authority equivalent
    backlinks_total: int = 0
    referring_domains: int = 0
    referring_ips: int = 0
    dofollow_links: int = 0
    nofollow_links: int = 0
    anchor_distribution: dict[str, float] = Field(default_factory=dict)  # anchor → % share
    top_anchors: list[str] = Field(default_factory=list)
    top_referring_domains: list[dict] = Field(default_factory=list)
    toxic_score: float = 0.0


class DataForSEOClient:
    """DataForSEO API client — keyword volume, SERP, backlinks."""

    def __init__(self, login: str = "", password: str = ""):
        self.login = login or os.getenv("DATAFORSEO_LOGIN", "")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD", "")
        self._session = None

    def _is_configured(self) -> bool:
        return bool(self.login and self.password)

    def _headers(self) -> dict:
        creds = b64encode(f"{self.login}:{self.password}".encode()).decode()
        return {
            "Authorization": f"Basic {creds}",
            "Content-Type": "application/json",
        }

    def _post(self, endpoint: str, payload: list[dict]) -> dict:
        if not self._is_configured():
            log.warning("dataforseo.not_configured  endpoint=%s", endpoint)
            return {"status_code": 0, "tasks": []}
        try:
            resp = requests.post(
                f"{BASE_URL}/{endpoint}",
                headers=self._headers(),
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.error("dataforseo.request_fail  endpoint=%s  err=%s", endpoint, e)
            return {"status_code": 0, "tasks": [], "error": str(e)}

    # ── Keyword Volume ─────────────────────────────────────────────

    def get_keyword_volumes(
        self,
        keywords: list[str],
        location_code: int = LOCATIONS["ca"],
        language_code: str = "en",
    ) -> list[KeywordMetrics]:
        """Get real search volumes and metrics for a list of keywords.

        Uses Google Ads Keywords Data endpoint — same data Google Keyword Planner uses.
        Cost: ~$0.0005/keyword.
        """
        payload = [{
            "keywords": keywords[:700],  # max 700 per request
            "location_code": location_code,
            "language_code": language_code,
            "include_serp_info": True,
            "include_adult_keywords": False,
        }]

        data = self._post("keywords_data/google_ads/keywords_for_keywords/live", payload)

        results = []
        for task in data.get("tasks", []):
            for item in task.get("result", []) or []:
                for kw_data in item.get("items", []) or []:
                    kw = kw_data.get("keyword", "")
                    monthly = kw_data.get("search_volume", 0) or 0
                    trend = [
                        m.get("search_volume", 0) or 0
                        for m in (kw_data.get("monthly_searches", []) or [])[-12:]
                    ]
                    comp = kw_data.get("competition", 0.0) or 0.0
                    comp_level = "HIGH" if comp > 0.66 else "MEDIUM" if comp > 0.33 else "LOW"

                    results.append(KeywordMetrics(
                        keyword=kw,
                        search_volume=monthly,
                        cpc=round(kw_data.get("cpc", 0.0) or 0.0, 2),
                        competition=round(comp, 2),
                        competition_level=comp_level,
                        keyword_difficulty=kw_data.get("keyword_difficulty", 0) or 0,
                        trend=trend,
                    ))

        log.info("dataforseo.volumes  keywords=%d  results=%d", len(keywords), len(results))
        return results

    def enrich_keywords(
        self,
        keywords: list[str],
        location: str = "ca",
    ) -> dict[str, KeywordMetrics]:
        """Enrich a keyword list with volume data. Returns dict keyed by keyword."""
        metrics = self.get_keyword_volumes(keywords, location_code=LOCATIONS.get(location, LOCATIONS["ca"]))
        return {m.keyword: m for m in metrics}

    # ── SERP Analysis ──────────────────────────────────────────────

    def get_serp(
        self,
        keyword: str,
        location_code: int = LOCATIONS["ca"],
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 10,
    ) -> dict:
        """Get live SERP results with featured snippet detection.

        Returns organic results + SERP features including snippet type/format.
        Cost: ~$0.0006/request.
        """
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth,
        }]

        data = self._post("serp/google/organic/live/advanced", payload)

        organic = []
        features = SERPFeatures()

        for task in data.get("tasks", []):
            for result in task.get("result", []) or []:
                for item in result.get("items", []) or []:
                    item_type = item.get("type", "")

                    if item_type == "organic":
                        organic.append({
                            "position": item.get("rank_absolute", 0),
                            "title": item.get("title", ""),
                            "url": item.get("url", ""),
                            "domain": item.get("domain", ""),
                            "description": item.get("description", ""),
                            "is_featured_snippet": item.get("is_featured_snippet", False),
                        })

                    elif item_type == "featured_snippet":
                        features.has_featured_snippet = True
                        features.snippet_url = item.get("url", "")
                        features.snippet_text = item.get("description", "")[:300]
                        # Detect snippet format
                        desc = item.get("description", "")
                        if item.get("table"):
                            features.snippet_type = "table"
                        elif "\n" in desc and any(c.isdigit() for c in desc[:5]):
                            features.snippet_type = "numbered_list"
                        elif "\n" in desc and desc.strip().startswith(("•", "-", "*")):
                            features.snippet_type = "bullet_list"
                        else:
                            features.snippet_type = "paragraph"

                    elif item_type == "people_also_ask":
                        features.has_paa = True

                    elif item_type == "local_pack":
                        features.has_local_pack = True

                    elif item_type == "images":
                        features.has_image_pack = True

                    elif item_type in ("video", "video_carousel"):
                        features.has_video = True

                    elif item_type == "knowledge_graph":
                        features.has_knowledge_panel = True

                    elif item_type == "ai_overview":
                        features.has_ai_overview = True

        return {
            "keyword": keyword,
            "organic": organic,
            "features": features.model_dump(),
            "total_results": len(organic),
        }

    # ── Backlink Profile ───────────────────────────────────────────

    def get_backlink_profile(self, domain: str) -> BacklinkProfile:
        """Get complete backlink profile for a domain.

        Includes: total backlinks, referring domains, anchor distribution,
        dofollow/nofollow split, top referring domains.
        Cost: ~$0.003/request.
        """
        payload = [{
            "target": domain,
            "limit": 1000,
            "include_subdomains": True,
        }]

        data = self._post("backlinks/summary/live", payload)
        profile = BacklinkProfile(domain=domain)

        for task in data.get("tasks", []):
            for result in task.get("result", []) or []:
                profile.domain_rank = result.get("rank", 0) or 0
                profile.backlinks_total = result.get("backlinks", 0) or 0
                profile.referring_domains = result.get("referring_domains", 0) or 0
                profile.referring_ips = result.get("referring_ips", 0) or 0
                profile.dofollow_links = result.get("dofollow", 0) or 0
                profile.nofollow_links = result.get("nofollow", 0) or 0

        log.info("dataforseo.backlinks  domain=%s  total=%d  rd=%d",
                 domain, profile.backlinks_total, profile.referring_domains)
        return profile

    def get_anchor_distribution(self, domain: str) -> dict[str, float]:
        """Get anchor text distribution for a domain. Returns anchor → percentage."""
        payload = [{
            "target": domain,
            "limit": 100,
            "order_by": ["backlinks,desc"],
        }]

        data = self._post("backlinks/anchors/live", payload)
        anchors: dict[str, int] = {}

        for task in data.get("tasks", []):
            for result in task.get("result", []) or []:
                for item in result.get("items", []) or []:
                    anchor = (item.get("anchor", "") or "").lower().strip()
                    count = item.get("backlinks", 0) or 0
                    if anchor:
                        anchors[anchor] = count

        total = sum(anchors.values()) or 1
        return {a: round(c / total, 3) for a, c in sorted(anchors.items(), key=lambda x: -x[1])[:20]}

    def compare_backlink_profiles(
        self,
        our_domain: str,
        competitor_domains: list[str],
    ) -> dict:
        """Compare our backlink profile against competitors.

        Returns gap analysis: who links to competitors but not to us.
        """
        our_profile = self.get_backlink_profile(our_domain)
        comp_profiles = {d: self.get_backlink_profile(d) for d in competitor_domains}

        biggest_threat = max(comp_profiles.items(), key=lambda x: x[1].domain_rank, default=(None, BacklinkProfile(domain="")))

        return {
            "our_domain": our_domain,
            "our_rank": our_profile.domain_rank,
            "our_backlinks": our_profile.backlinks_total,
            "our_referring_domains": our_profile.referring_domains,
            "competitors": {
                d: {
                    "rank": p.domain_rank,
                    "backlinks": p.backlinks_total,
                    "referring_domains": p.referring_domains,
                    "rank_gap": p.domain_rank - our_profile.domain_rank,
                    "link_gap": p.backlinks_total - our_profile.backlinks_total,
                    "rd_gap": p.referring_domains - our_profile.referring_domains,
                }
                for d, p in comp_profiles.items()
            },
            "biggest_threat": biggest_threat[0] if biggest_threat[0] else "",
            "links_needed_to_match_leader": max(0, biggest_threat[1].backlinks_total - our_profile.backlinks_total),
            "rds_needed_to_match_leader": max(0, biggest_threat[1].referring_domains - our_profile.referring_domains),
        }

    def is_configured(self) -> bool:
        return self._is_configured()
