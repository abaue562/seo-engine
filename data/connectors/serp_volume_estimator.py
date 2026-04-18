"""Self-hosted SERP volume + keyword difficulty estimator.

Replaces DataForSEO keyword data with signal-based estimation from live SERP.
Zero cost — uses core/serp_scraper.py (Bing via Firecrawl :3002) + Google
Autocomplete (public, no auth).

Signals used for difficulty estimation:
  - total_results      → proxy for competition breadth
  - ad_count           → commercial intent + paid competition
  - top10_gov_edu      → authoritative domain density (harder to displace)
  - featured_snippet   → SERP is dominated (harder to get click share)
  - paa_count          → topic demand depth
  - exact_title_match  → how many top results target keyword exactly
  - autocomplete_pos   → position in autocomplete = popularity signal

Volume tiers:
  high   → estimated 5000+ searches/month
  medium → 500–5000
  low    → 50–500
  micro  → <50
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Optional

import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
_CACHE_TTL = 86400 * 3
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

_GOV_EDU = re.compile(r'\.(gov|edu|ac\.uk|gov\.uk)$', re.I)
_AD_SIGNALS = ['ad\xb7', '/ad/', 'sponsored', 'advertisement']


def _cache_key(keyword: str, location: str) -> str:
    return "sve:" + hashlib.sha256((keyword + ":" + location).encode()).hexdigest()[:16]


def _autocomplete_position(keyword: str) -> int:
    try:
        q = urllib.parse.quote_plus(keyword.split()[0])
        url = "https://suggestqueries.google.com/complete/search?client=firefox&q=" + q
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())
            suggestions = [s.lower() for s in data[1]] if len(data) > 1 else []
            kw_lower = keyword.lower()
            for i, s in enumerate(suggestions[:10], 1):
                if kw_lower in s or s in kw_lower:
                    return i
    except Exception:
        pass
    return 11


def _score_difficulty(signals: dict) -> int:
    score = 0
    rc = signals.get("result_count", 0)
    if rc > 100_000_000:
        score += 30
    elif rc > 10_000_000:
        score += 22
    elif rc > 1_000_000:
        score += 14
    elif rc > 100_000:
        score += 6

    score += min(signals.get("ad_count", 0) * 5, 20)
    score += min(signals.get("top10_gov_edu", 0) * 4, 20)
    if signals.get("featured_snippet"):
        score += 10
    score += min(signals.get("paa_count", 0) * 2, 10)
    score += min(signals.get("exact_title_match", 0) * 2, 10)
    return min(score, 100)


def _score_volume(signals: dict) -> tuple:
    rc = signals.get("result_count", 0)
    ads = signals.get("ad_count", 0)
    paa = signals.get("paa_count", 0)
    ac_pos = signals.get("autocomplete_position", 11)

    v = 0
    if rc > 100_000_000:
        v += 40
    elif rc > 10_000_000:
        v += 28
    elif rc > 1_000_000:
        v += 16
    elif rc > 100_000:
        v += 6

    v += min(ads * 6, 24)
    v += min(paa * 3, 18)
    if ac_pos <= 3:
        v += 18
    elif ac_pos <= 7:
        v += 10
    elif ac_pos <= 10:
        v += 4

    # 7-tier refined volume (calibrated for local service markets)
    if v >= 90:
        return "ultra_high", 50000
    if v >= 75:
        return "very_high", 10000
    if v >= 60:
        return "high", 3000
    if v >= 48:
        return "medium_high", 1000
    if v >= 35:
        return "medium", 300
    if v >= 20:
        return "low", 75
    return "micro", 15


class SERPVolumeEstimator:
    """Estimate keyword volume + difficulty purely from SERP signals. No paid API."""

    def estimate(self, keyword: str, location: str = "") -> dict:
        ck = _cache_key(keyword, location)
        cached = _redis.get(ck)
        if cached:
            log.debug("serp_volume_estimator.cache_hit  keyword=%s", keyword)
            return json.loads(cached)

        from core.serp_scraper import scrape_serp
        serp = scrape_serp(keyword, location=location, num_results=50)
        organic = serp.get("organic", [])
        paa = serp.get("paa", [])

        gov_edu = sum(1 for r in organic[:10] if _GOV_EDU.search(r.get("domain", "")))
        kw_words = set(keyword.lower().split())
        exact_title = sum(
            1 for r in organic[:10]
            if kw_words.issubset(set(r.get("title", "").lower().split()))
        )
        ad_count = sum(
            1 for r in organic[:10]
            if any(sig in (r.get("snippet", "") + r.get("title", "")).lower()
                   for sig in _AD_SIGNALS)
        )
        featured_snippet = bool(
            organic and organic[0].get("position") == 1
            and len(organic[0].get("snippet", "")) > 60
        )
        result_count = len(organic) * 1_000_000 if len(organic) >= 10 else len(organic) * 100_000
        ac_pos = _autocomplete_position(keyword)

        signals = {
            "result_count": result_count,
            "ad_count": ad_count,
            "top10_gov_edu": gov_edu,
            "featured_snippet": featured_snippet,
            "paa_count": len(paa),
            "exact_title_match": exact_title,
            "autocomplete_position": ac_pos,
        }

        difficulty = _score_difficulty(signals)
        volume_tier, est_monthly = _score_volume(signals)

        if ad_count >= 3:
            commercial_intent = "transactional"
        elif ad_count >= 1:
            commercial_intent = "commercial"
        elif len(paa) >= 4:
            commercial_intent = "informational"
        else:
            commercial_intent = "navigational"

        if difficulty >= 70:
            competition_level = "very_high"
        elif difficulty >= 50:
            competition_level = "high"
        elif difficulty >= 30:
            competition_level = "medium"
        else:
            competition_level = "low"

        serp_features = []
        if featured_snippet:
            serp_features.append("featured_snippet")
        if paa:
            serp_features.append("people_also_ask")
        if ad_count > 0:
            serp_features.append("ads")
        if gov_edu > 0:
            serp_features.append("authoritative_domains")

        result = {
            "keyword": keyword,
            "volume_tier": volume_tier,
            "estimated_monthly_searches": est_monthly,
            "keyword_difficulty": difficulty,
            "commercial_intent": commercial_intent,
            "competition_level": competition_level,
            "featured_snippet": featured_snippet,
            "paa_count": len(paa),
            "ad_count": ad_count,
            "result_count": result_count,
            "autocomplete_position": ac_pos,
            "serp_features": serp_features,
            "top_domains": [r.get("domain") for r in organic[:5]],
            "estimated_at": datetime.utcnow().isoformat(),
        }

        _redis.setex(ck, _CACHE_TTL, json.dumps(result))
        log.info(
            "serp_volume_estimator.done  keyword=%s  tier=%s  kd=%d  intent=%s",
            keyword, volume_tier, difficulty, commercial_intent,
        )
        return result

    def batch_estimate(self, keywords: list, location: str = "") -> list:
        import time
        results = []
        for i, kw in enumerate(keywords):
            if i > 0:
                time.sleep(1.5)
            results.append(self.estimate(kw, location=location))
        results.sort(key=lambda r: r["estimated_monthly_searches"], reverse=True)
        return results

    def prioritise(self, keywords: list, location: str = "", max_difficulty: int = 60) -> list:
        estimates = self.batch_estimate(keywords, location=location)
        filtered = [e for e in estimates if e["keyword_difficulty"] <= max_difficulty]
        for e in filtered:
            kd = max(e["keyword_difficulty"], 1)
            e["opportunity_score"] = round(e["estimated_monthly_searches"] / kd, 1)
        filtered.sort(key=lambda e: e["opportunity_score"], reverse=True)
        return filtered
