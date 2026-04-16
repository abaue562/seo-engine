"""DataForSEO API connector — keyword volume, SERP data, backlinks.

DataForSEO provides real keyword search volume, CPC, difficulty, SERP features,
and backlink profiles. Uses HTTP Basic Auth.

Docs: https://docs.dataforseo.com/v3/

Setup:
  1. Register at dataforseo.com (free trial available)
  2. Add to config/.env:
     DATAFORSEO_LOGIN=your@email.com
     DATAFORSEO_PASSWORD=your_api_password

Pricing (approx):
  - Keywords Data: $0.0005/keyword (~$0.50 per 1,000)
  - SERP: $0.0006/request
  - Backlinks: $0.003/domain profile

Env vars required:
  DATAFORSEO_LOGIN
  DATAFORSEO_PASSWORD

Usage:
    from data.connectors.dataforseo import DataForSEOClient

    client = DataForSEOClient()
    volumes = client.keyword_data_live(["permanent lights kelowna", "landscape lighting kelowna"])
    serp    = client.serp_live("permanent lights kelowna", location_code=2124)
    ranked  = client.ranked_keywords("gemstonebyoutdoor.com")
"""

from __future__ import annotations

import base64
import logging
import os
from typing import Optional

import httpx
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

# DataForSEO location codes
LOCATIONS = {
    "ca": 2124,   # Canada
    "us": 2840,   # United States
    "gb": 2826,   # United Kingdom
    "au": 2036,   # Australia
}

# Language codes
LANGUAGES = {
    "en": "en",
    "fr": "fr",
}


# ── Pydantic models (kept from original for downstream compatibility) ──────────

class KeywordMetrics(BaseModel):
    keyword: str
    search_volume: int = 0
    cpc: float = 0.0
    competition: float = 0.0
    competition_level: str = ""      # LOW / MEDIUM / HIGH
    keyword_difficulty: int = 0      # 0-100
    trend: list[int] = Field(default_factory=list)   # 12-month trend
    intent: str = ""
    related: list[str] = Field(default_factory=list)


class SERPFeatures(BaseModel):
    has_featured_snippet: bool = False
    has_ai_overview: bool = False
    has_paa: bool = False
    has_local_pack: bool = False
    has_image_pack: bool = False
    has_video: bool = False
    has_knowledge_panel: bool = False
    snippet_type: str = ""
    snippet_url: str = ""
    snippet_text: str = ""


class BacklinkProfile(BaseModel):
    domain: str
    domain_rank: int = 0
    backlinks_total: int = 0
    referring_domains: int = 0
    referring_ips: int = 0
    dofollow_links: int = 0
    nofollow_links: int = 0
    anchor_distribution: dict[str, float] = Field(default_factory=dict)
    top_anchors: list[str] = Field(default_factory=list)
    top_referring_domains: list[dict] = Field(default_factory=list)
    toxic_score: float = 0.0


# ── Client ─────────────────────────────────────────────────────────────────────

class DataForSEOClient:
    """DataForSEO API client.

    Authenticates via HTTP Basic Auth.  All methods make synchronous httpx
    calls and return plain dicts / lists so they compose easily with other
    connectors.  Pydantic model helpers are kept for backward compatibility.
    """

    BASE_URL = "https://api.dataforseo.com/v3"

    def __init__(self, login: str = "", password: str = ""):
        _login    = login    or os.getenv("DATAFORSEO_LOGIN", "")
        _password = password or os.getenv("DATAFORSEO_PASSWORD", "")
        credentials = base64.b64encode(f"{_login}:{_password}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/json",
        }
        self._configured = bool(_login and _password)
        self.client = httpx.Client(timeout=30)

    # ── Low-level helper ───────────────────────────────────────────────────────

    def _post(self, endpoint: str, data: list) -> dict:
        """Authenticated POST to DataForSEO.  Raises ValueError on API errors.

        DataForSEO envelope:
          {"status_code": 20000, "tasks": [{"status_code": 20000, "result": [...]}]}
        status_code 20000 == OK.  Any other code is an error.
        """
        if not self._configured:
            log.warning("dataforseo.not_configured  endpoint=%s", endpoint)
            return {"status_code": 0, "tasks": []}

        url = f"{self.BASE_URL}/{endpoint}"
        try:
            resp = self.client.post(url, headers=self.headers, json=data)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            log.error("dataforseo.http_error  endpoint=%s  status=%s", endpoint, exc.response.status_code)
            raise ValueError(f"DataForSEO HTTP {exc.response.status_code}: {exc.response.text[:200]}") from exc
        except httpx.RequestError as exc:
            log.error("dataforseo.request_error  endpoint=%s  err=%s", endpoint, exc)
            raise ValueError(f"DataForSEO request failed: {exc}") from exc

        body = resp.json()
        api_status = body.get("status_code", 0)
        if api_status != 20000:
            msg = body.get("status_message", "unknown error")
            log.error("dataforseo.api_error  endpoint=%s  code=%s  msg=%s", endpoint, api_status, msg)
            raise ValueError(f"DataForSEO API error {api_status}: {msg}")

        return body

    # ── Keyword Data ───────────────────────────────────────────────────────────

    def keyword_data_live(
        self,
        keywords: list[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> list[dict]:
        """Get search volume, CPC, competition, and keyword difficulty for keywords.

        Endpoint: POST /keywords_data/google_ads/search_volume/live
        Cost: ~$0.0005/keyword.

        Returns list of:
          {keyword, search_volume, cpc, competition, keyword_difficulty, monthly_searches}
        """
        payload = [{
            "keywords": keywords[:700],   # DataForSEO max per task
            "location_code": location_code,
            "language_code": language_code,
            "include_serp_info": True,
            "include_adult_keywords": False,
        }]

        try:
            body = self._post("keywords_data/google_ads/search_volume/live", payload)
        except ValueError as exc:
            log.error("dataforseo.keyword_data_live  err=%s", exc)
            return []

        results: list[dict] = []
        for task in body.get("tasks", []):
            task_status = task.get("status_code", 0)
            if task_status != 20000:
                log.warning("dataforseo.task_error  code=%s  msg=%s",
                            task_status, task.get("status_message", ""))
                continue
            for item in task.get("result", []) or []:
                kw = item.get("keyword", "")
                monthly_raw = item.get("monthly_searches") or []
                monthly_searches = [
                    {"year": m.get("year"), "month": m.get("month"), "search_volume": m.get("search_volume", 0)}
                    for m in monthly_raw
                ]
                comp = item.get("competition", 0.0) or 0.0
                results.append({
                    "keyword": kw,
                    "search_volume": item.get("search_volume") or 0,
                    "cpc": round(item.get("cpc") or 0.0, 2),
                    "competition": round(comp, 4),
                    "keyword_difficulty": item.get("keyword_difficulty") or 0,
                    "monthly_searches": monthly_searches,
                })

        log.info("dataforseo.keyword_data_live  in=%d  out=%d", len(keywords), len(results))
        return results

    def bulk_keyword_data(
        self,
        keywords: list[str],
        location_code: int = 2840,
    ) -> list[dict]:
        """Batch keyword data — chunks keywords into 100-keyword batches.

        Combines results from multiple keyword_data_live calls.
        Returns the same structure as keyword_data_live.
        """
        all_results: list[dict] = []
        batch_size = 100

        for i in range(0, len(keywords), batch_size):
            batch = keywords[i : i + batch_size]
            log.info("dataforseo.bulk_keyword_data  batch=%d/%d  size=%d",
                     i // batch_size + 1, -(-len(keywords) // batch_size), len(batch))
            batch_results = self.keyword_data_live(batch, location_code=location_code)
            all_results.extend(batch_results)

        log.info("dataforseo.bulk_keyword_data  total_keywords=%d  total_results=%d",
                 len(keywords), len(all_results))
        return all_results

    # ── SERP ───────────────────────────────────────────────────────────────────

    def serp_live(
        self,
        keyword: str,
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
    ) -> dict:
        """Get live SERP results for a keyword.

        Endpoint: POST /serp/google/organic/live/advanced
        Cost: ~$0.0006/request.

        Returns:
          {keyword, items: [{type, rank_absolute, url, title, description}], ...}
        """
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": 10,
        }]

        try:
            body = self._post("serp/google/organic/live/advanced", payload)
        except ValueError as exc:
            log.error("dataforseo.serp_live  keyword=%s  err=%s", keyword, exc)
            return {"keyword": keyword, "items": [], "error": str(exc)}

        items: list[dict] = []
        for task in body.get("tasks", []):
            if task.get("status_code") != 20000:
                continue
            for result in task.get("result", []) or []:
                for item in result.get("items", []) or []:
                    items.append({
                        "type": item.get("type", ""),
                        "rank_absolute": item.get("rank_absolute"),
                        "url": item.get("url", ""),
                        "title": item.get("title", ""),
                        "description": item.get("description", ""),
                        "domain": item.get("domain", ""),
                        "is_featured_snippet": item.get("is_featured_snippet", False),
                    })

        log.info("dataforseo.serp_live  keyword=%s  items=%d", keyword, len(items))
        return {"keyword": keyword, "items": items}

    def serp_features(self, keyword: str, location_code: int = 2840) -> list[str]:
        """Return SERP feature types present for a keyword.

        E.g. ["featured_snippet", "people_also_ask", "local_pack", "images"]
        Uses serp_live and extracts unique item type values.
        """
        serp = self.serp_live(keyword, location_code=location_code)
        feature_types: list[str] = []
        seen: set[str] = set()
        for item in serp.get("items", []):
            t = item.get("type", "")
            if t and t != "organic" and t not in seen:
                seen.add(t)
                feature_types.append(t)
        return feature_types

    # ── Ranked Keywords ────────────────────────────────────────────────────────

    def ranked_keywords(
        self,
        target_domain: str,
        location_code: int = 2840,
    ) -> list[dict]:
        """Get all keywords a domain ranks for in Google organic.

        Endpoint: POST /dataforseo_labs/google/ranked_keywords/live
        Cost: ~$0.003/request.

        Returns list of:
          {keyword, rank_absolute, url, search_volume}
        """
        payload = [{
            "target": target_domain,
            "location_code": location_code,
            "language_code": "en",
            "limit": 1000,
            "include_serp_info": True,
        }]

        try:
            body = self._post("dataforseo_labs/google/ranked_keywords/live", payload)
        except ValueError as exc:
            log.error("dataforseo.ranked_keywords  domain=%s  err=%s", target_domain, exc)
            return []

        results: list[dict] = []
        for task in body.get("tasks", []):
            if task.get("status_code") != 20000:
                continue
            for result in task.get("result", []) or []:
                for item in result.get("items", []) or []:
                    kw_data   = item.get("keyword_data", {}) or {}
                    kw_info   = kw_data.get("keyword_info", {}) or {}
                    rank_info = item.get("ranked_serp_element", {}) or {}
                    serp_el   = rank_info.get("serp_item", {}) or {}
                    results.append({
                        "keyword":       kw_data.get("keyword", ""),
                        "rank_absolute": serp_el.get("rank_absolute"),
                        "url":           serp_el.get("url", ""),
                        "search_volume": kw_info.get("search_volume") or 0,
                    })

        log.info("dataforseo.ranked_keywords  domain=%s  results=%d", target_domain, len(results))
        return results

    # ── Domain Intersection ────────────────────────────────────────────────────

    def domain_intersection(
        self,
        target: str,
        competitor: str,
        location_code: int = 2840,
    ) -> list[dict]:
        """Keywords both domains rank for — competitive gap analysis.

        Endpoint: POST /dataforseo_labs/google/domain_intersection/live

        Returns list of:
          {keyword, target_rank, competitor_rank, search_volume}
        """
        payload = [{
            "target1": target,
            "target2": competitor,
            "location_code": location_code,
            "language_code": "en",
            "limit": 1000,
            "include_serp_info": True,
        }]

        try:
            body = self._post("dataforseo_labs/google/domain_intersection/live", payload)
        except ValueError as exc:
            log.error("dataforseo.domain_intersection  target=%s  competitor=%s  err=%s",
                      target, competitor, exc)
            return []

        results: list[dict] = []
        for task in body.get("tasks", []):
            if task.get("status_code") != 20000:
                continue
            for result in task.get("result", []) or []:
                for item in result.get("items", []) or []:
                    kw_data  = item.get("keyword_data", {}) or {}
                    kw_info  = kw_data.get("keyword_info", {}) or {}
                    # target1 and target2 rank info are nested lists
                    t1_items = item.get("first_domain_serp_element", []) or []
                    t2_items = item.get("second_domain_serp_element", []) or []
                    t1_rank  = t1_items[0].get("rank_absolute") if t1_items else None
                    t2_rank  = t2_items[0].get("rank_absolute") if t2_items else None
                    results.append({
                        "keyword":          kw_data.get("keyword", ""),
                        "target_rank":      t1_rank,
                        "competitor_rank":  t2_rank,
                        "search_volume":    kw_info.get("search_volume") or 0,
                    })

        log.info("dataforseo.domain_intersection  target=%s  competitor=%s  results=%d",
                 target, competitor, len(results))
        return results

    # ── Backlinks ──────────────────────────────────────────────────────────────

    def backlinks_summary(self, target: str) -> dict:
        """Domain backlink summary.

        Endpoint: POST /backlinks/summary/live
        Cost: ~$0.003/request.

        Returns:
          {total_count, referring_domains, rank, backlinks: [{url_from, url_to, anchor, domain_from_rank}]}
        """
        payload = [{
            "target": target,
            "include_subdomains": True,
            "limit": 100,
        }]

        try:
            body = self._post("backlinks/summary/live", payload)
        except ValueError as exc:
            log.error("dataforseo.backlinks_summary  target=%s  err=%s", target, exc)
            return {"target": target, "total_count": 0, "referring_domains": 0, "rank": 0, "backlinks": []}

        summary: dict = {
            "target": target,
            "total_count": 0,
            "referring_domains": 0,
            "rank": 0,
            "backlinks": [],
        }

        for task in body.get("tasks", []):
            if task.get("status_code") != 20000:
                continue
            for result in task.get("result", []) or []:
                summary["total_count"]       = result.get("backlinks", 0) or 0
                summary["referring_domains"] = result.get("referring_domains", 0) or 0
                summary["rank"]              = result.get("rank", 0) or 0
                # top backlinks items if present
                for item in result.get("items", []) or []:
                    summary["backlinks"].append({
                        "url_from":         item.get("url_from", ""),
                        "url_to":           item.get("url_to", ""),
                        "anchor":           item.get("anchor", ""),
                        "domain_from_rank": item.get("domain_from_rank", 0),
                    })

        log.info("dataforseo.backlinks_summary  target=%s  total=%d  rd=%d",
                 target, summary["total_count"], summary["referring_domains"])
        return summary

    # ── Legacy helpers kept for backward compatibility ─────────────────────────

    def get_keyword_volumes(
        self,
        keywords: list[str],
        location_code: int = LOCATIONS["ca"],
        language_code: str = "en",
    ) -> list[KeywordMetrics]:
        """Legacy method — wraps keyword_data_live and returns KeywordMetrics models."""
        raw = self.keyword_data_live(keywords, location_code=location_code, language_code=language_code)
        results: list[KeywordMetrics] = []
        for item in raw:
            comp = item.get("competition", 0.0)
            comp_level = "HIGH" if comp > 0.66 else "MEDIUM" if comp > 0.33 else "LOW"
            trend = [
                m.get("search_volume", 0) or 0
                for m in (item.get("monthly_searches") or [])[-12:]
            ]
            results.append(KeywordMetrics(
                keyword=item["keyword"],
                search_volume=item.get("search_volume", 0),
                cpc=item.get("cpc", 0.0),
                competition=comp,
                competition_level=comp_level,
                keyword_difficulty=item.get("keyword_difficulty", 0),
                trend=trend,
            ))
        return results

    def enrich_keywords(
        self,
        keywords: list[str],
        location: str = "ca",
    ) -> dict[str, KeywordMetrics]:
        """Enrich keyword list with volume data. Returns dict keyed by keyword."""
        metrics = self.get_keyword_volumes(keywords, location_code=LOCATIONS.get(location, LOCATIONS["ca"]))
        return {m.keyword: m for m in metrics}

    def get_serp(
        self,
        keyword: str,
        location_code: int = LOCATIONS["ca"],
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 10,
    ) -> dict:
        """Legacy method — wraps serp_live with full feature detection."""
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth,
        }]

        try:
            body = self._post("serp/google/organic/live/advanced", payload)
        except ValueError:
            return {"keyword": keyword, "organic": [], "features": SERPFeatures().model_dump(), "total_results": 0}

        organic: list[dict] = []
        features = SERPFeatures()

        for task in body.get("tasks", []):
            for result in task.get("result", []) or []:
                for item in result.get("items", []) or []:
                    item_type = item.get("type", "")
                    if item_type == "organic":
                        organic.append({
                            "position":           item.get("rank_absolute", 0),
                            "title":              item.get("title", ""),
                            "url":                item.get("url", ""),
                            "domain":             item.get("domain", ""),
                            "description":        item.get("description", ""),
                            "is_featured_snippet": item.get("is_featured_snippet", False),
                        })
                    elif item_type == "featured_snippet":
                        features.has_featured_snippet = True
                        features.snippet_url  = item.get("url", "")
                        features.snippet_text = item.get("description", "")[:300]
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
            "keyword":       keyword,
            "organic":       organic,
            "features":      features.model_dump(),
            "total_results": len(organic),
        }

    def get_backlink_profile(self, domain: str) -> BacklinkProfile:
        """Legacy method — wraps backlinks_summary and returns BacklinkProfile model."""
        summary = self.backlinks_summary(domain)
        return BacklinkProfile(
            domain=domain,
            domain_rank=summary.get("rank", 0),
            backlinks_total=summary.get("total_count", 0),
            referring_domains=summary.get("referring_domains", 0),
        )

    def get_anchor_distribution(self, domain: str) -> dict[str, float]:
        """Get anchor text distribution. Returns anchor → percentage."""
        payload = [{
            "target": domain,
            "limit": 100,
            "order_by": ["backlinks,desc"],
        }]
        try:
            body = self._post("backlinks/anchors/live", payload)
        except ValueError as exc:
            log.error("dataforseo.anchors  domain=%s  err=%s", domain, exc)
            return {}

        anchors: dict[str, int] = {}
        for task in body.get("tasks", []):
            for result in task.get("result", []) or []:
                for item in result.get("items", []) or []:
                    anchor = (item.get("anchor", "") or "").lower().strip()
                    count  = item.get("backlinks", 0) or 0
                    if anchor:
                        anchors[anchor] = count

        total = sum(anchors.values()) or 1
        return {a: round(c / total, 3) for a, c in sorted(anchors.items(), key=lambda x: -x[1])[:20]}

    def compare_backlink_profiles(
        self,
        our_domain: str,
        competitor_domains: list[str],
    ) -> dict:
        """Compare backlink profile against competitors."""
        our_profile   = self.get_backlink_profile(our_domain)
        comp_profiles = {d: self.get_backlink_profile(d) for d in competitor_domains}

        biggest_threat = max(
            comp_profiles.items(),
            key=lambda x: x[1].domain_rank,
            default=(None, BacklinkProfile(domain="")),
        )

        return {
            "our_domain":           our_domain,
            "our_rank":             our_profile.domain_rank,
            "our_backlinks":        our_profile.backlinks_total,
            "our_referring_domains": our_profile.referring_domains,
            "competitors": {
                d: {
                    "rank":             p.domain_rank,
                    "backlinks":        p.backlinks_total,
                    "referring_domains": p.referring_domains,
                    "rank_gap":         p.domain_rank - our_profile.domain_rank,
                    "link_gap":         p.backlinks_total - our_profile.backlinks_total,
                    "rd_gap":           p.referring_domains - our_profile.referring_domains,
                }
                for d, p in comp_profiles.items()
            },
            "biggest_threat": biggest_threat[0] if biggest_threat[0] else "",
            "links_needed_to_match_leader": max(
                0, biggest_threat[1].backlinks_total - our_profile.backlinks_total
            ),
            "rds_needed_to_match_leader": max(
                0, biggest_threat[1].referring_domains - our_profile.referring_domains
            ),
        }

    def is_configured(self) -> bool:
        return self._configured
