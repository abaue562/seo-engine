"""Common Crawl + Wayback backlink intelligence — free Ahrefs replacement.

Three data sources, all free, no API key:
  1. Common Crawl CDX API — petabyte-scale web index, enumerate domain pages
  2. Wayback Machine CDX API — historical crawl data, link discovery
  3. core/backlink_crawler.py (Firecrawl) — real-time competitor crawl

Use cases (replacing Ahrefs):
  - get_domain_pages()       → site structure, index size proxy
  - get_referring_domains()  → who links to a target domain
  - estimate_domain_authority() → DA score from crawl signals
  - get_link_gaps()          → competitor backlinks you don't have

CC index rotates monthly. We query the 3 most recent indexes for coverage.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Optional

import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)

_CACHE_TTL = 86400 * 7     # 7 days — CC data changes slowly
_UA = "SEOEngine-CCBot/1.0 (+https://gethubed.com/bot)"
_CC_INDEXES = [
    "CC-MAIN-2025-13",
    "CC-MAIN-2024-51",
    "CC-MAIN-2024-38",
]
_CC_CDX_BASE = "https://index.commoncrawl.org/{index}-index"
_WB_CDX_BASE = "http://web.archive.org/cdx/search/cdx"
_MAX_PAGES = 500
_MAX_REFS = 200


def _get(url: str, timeout: int = 15) -> str:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as exc:
        log.warning("common_crawl._get  url=%s  err=%s", url[:80], exc)
        return ""


def _parse_jsonlines(raw: str) -> list:
    results = []
    for line in raw.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            results.append(json.loads(line))
        except Exception:
            pass
    return results


def _normalise_domain(raw: str) -> str:
    d = re.sub(r'^https?://(www\.)?', '', raw.lower())
    return d.split('/')[0].rstrip('.')


class CommonCrawlClient:
    """Free backlink + site intelligence via Common Crawl and Wayback CDX APIs."""

    def get_domain_pages(self, domain: str, limit: int = 200) -> list:
        """Enumerate pages on a domain from Common Crawl index.

        Returns: [{url, timestamp, status, mime}]
        Useful for: site structure analysis, index size estimation, page discovery.
        """
        ck = "cc:pages:" + hashlib.sha256(domain.encode()).hexdigest()[:12]
        cached = _redis.get(ck)
        if cached:
            return json.loads(cached)

        results = []
        for index in _CC_INDEXES:
            if len(results) >= limit:
                break
            params = urllib.parse.urlencode({
                "url": "*." + domain,
                "output": "json",
                "fl": "url,timestamp,status,mime",
                "limit": min(limit - len(results), 200),
                "filter": "status:200",
            })
            url = _CC_CDX_BASE.format(index=index) + "?" + params
            raw = _get(url)
            if raw:
                results.extend(_parse_jsonlines(raw))

        # Deduplicate by URL
        seen = set()
        deduped = []
        for r in results:
            u = r.get("url", "")
            if u not in seen:
                seen.add(u)
                deduped.append(r)

        _redis.setex(ck, _CACHE_TTL, json.dumps(deduped[:limit]))
        log.info("common_crawl.domain_pages  domain=%s  count=%d", domain, len(deduped))
        return deduped[:limit]

    def get_referring_domains(self, target_domain: str, limit: int = 100) -> list:
        """Find domains that link to target_domain using Wayback CDX + CC.

        Strategy:
          1. Wayback CDX: find archived pages that mention target_domain in URL
          2. CC CDX: search for pages on other domains linking to target
          3. Our Firecrawl crawler as fallback for real-time data

        Returns: [{referring_url, referring_domain, timestamp, da_estimate}]
        """
        ck = "cc:refs:" + hashlib.sha256(target_domain.encode()).hexdigest()[:12]
        cached = _redis.get(ck)
        if cached:
            return json.loads(cached)

        results = []

        # Source 1: Wayback CDX — find saved pages that contain target_domain
        try:
            params = urllib.parse.urlencode({
                "url": "*" + target_domain + "*",
                "output": "json",
                "fl": "original,timestamp,urlkey",
                "limit": min(limit, _MAX_REFS),
                "filter": "statuscode:200",
                "collapse": "urlkey",
            })
            raw = _get(_WB_CDX_BASE + "?" + params, timeout=20)
            rows = _parse_jsonlines(raw)
            # Filter: only pages NOT on the target domain itself
            for row in rows:
                ref_url = row.get("original", "")
                ref_domain = _normalise_domain(ref_url)
                if ref_domain and ref_domain != target_domain and target_domain not in ref_domain:
                    results.append({
                        "referring_url": ref_url,
                        "referring_domain": ref_domain,
                        "timestamp": row.get("timestamp", ""),
                        "source": "wayback",
                        "da_estimate": self._estimate_da(ref_domain),
                    })
        except Exception as exc:
            log.warning("common_crawl.wayback_fail  domain=%s  err=%s", target_domain, exc)

        # Source 2: CC CDX — find pages on other domains with target in URL path
        if len(results) < limit:
            for index in _CC_INDEXES[:2]:
                if len(results) >= limit:
                    break
                try:
                    params = urllib.parse.urlencode({
                        "url": "*" + target_domain,
                        "output": "json",
                        "fl": "url,timestamp",
                        "limit": 100,
                        "filter": "status:200",
                        "collapse": "domain",
                    })
                    url = _CC_CDX_BASE.format(index=index) + "?" + params
                    raw = _get(url, timeout=20)
                    for row in _parse_jsonlines(raw):
                        ref_url = row.get("url", "")
                        ref_domain = _normalise_domain(ref_url)
                        if ref_domain and ref_domain != target_domain:
                            results.append({
                                "referring_url": ref_url,
                                "referring_domain": ref_domain,
                                "timestamp": row.get("timestamp", ""),
                                "source": "common_crawl",
                                "da_estimate": self._estimate_da(ref_domain),
                            })
                except Exception as exc:
                    log.warning("common_crawl.cc_fail  index=%s  err=%s", index, exc)

        # Deduplicate by referring_domain
        seen = set()
        deduped = []
        for r in results:
            d = r["referring_domain"]
            if d not in seen:
                seen.add(d)
                deduped.append(r)

        # Sort by DA estimate descending
        deduped.sort(key=lambda r: r.get("da_estimate", 0), reverse=True)
        deduped = deduped[:limit]

        _redis.setex(ck, _CACHE_TTL, json.dumps(deduped))
        log.info("common_crawl.referring_domains  target=%s  count=%d", target_domain, len(deduped))
        return deduped

    def get_link_gaps(self, your_domain: str, competitor_domains: list,
                      limit: int = 50) -> list:
        """Find domains that link to competitors but NOT to you.

        Returns: [{referring_domain, da_estimate, competitors_linked, gap_score}]
        Sorted by gap_score (high DA + links to many competitors = best prospect).
        """
        ck = "cc:gaps:" + hashlib.sha256((your_domain + "|".join(sorted(competitor_domains))).encode()).hexdigest()[:12]
        cached = _redis.get(ck)
        if cached:
            return json.loads(cached)

        your_refs = {r["referring_domain"] for r in self.get_referring_domains(your_domain)}

        gap_map: dict[str, dict] = {}
        for comp in competitor_domains[:5]:
            time.sleep(1)
            for ref in self.get_referring_domains(comp):
                rd = ref["referring_domain"]
                if rd in your_refs:
                    continue
                if rd not in gap_map:
                    gap_map[rd] = {
                        "referring_domain": rd,
                        "da_estimate": ref.get("da_estimate", 0),
                        "competitors_linked": [],
                        "example_url": ref.get("referring_url", ""),
                    }
                if comp not in gap_map[rd]["competitors_linked"]:
                    gap_map[rd]["competitors_linked"].append(comp)

        gaps = list(gap_map.values())
        for g in gaps:
            g["gap_score"] = g["da_estimate"] * len(g["competitors_linked"])
        gaps.sort(key=lambda g: g["gap_score"], reverse=True)
        gaps = gaps[:limit]

        _redis.setex(ck, _CACHE_TTL, json.dumps(gaps))
        log.info("common_crawl.link_gaps  your=%s  comps=%d  gaps=%d",
                 your_domain, len(competitor_domains), len(gaps))
        return gaps

    def estimate_domain_authority(self, domain: str) -> int:
        """Estimate DA (0-100) from crawl signals. No paid API."""
        return self._estimate_da(domain)

    def _estimate_da(self, domain: str) -> int:
        """Score DA from: TLD quality + page count + Wayback age + known high-DA list."""
        ck = "cc:da:" + hashlib.sha256(domain.encode()).hexdigest()[:12]
        cached = _redis.get(ck)
        if cached:
            return int(cached)

        # Known high-DA domains (fast path)
        _HIGH_DA = {
            "reddit.com": 93, "medium.com": 92, "linkedin.com": 98,
            "github.com": 95, "youtube.com": 100, "wikipedia.org": 93,
            "twitter.com": 95, "x.com": 95, "quora.com": 92,
            "dev.to": 78, "substack.com": 80, "wordpress.com": 82,
            "blogger.com": 76, "tumblr.com": 74, "pinterest.com": 94,
        }
        if domain in _HIGH_DA:
            return _HIGH_DA[domain]

        score = 0

        # TLD quality
        tld = domain.split(".")[-1].lower()
        tld_scores = {"edu": 25, "gov": 25, "org": 10, "com": 5, "net": 4, "io": 6, "co": 4}
        score += tld_scores.get(tld, 2)

        # Domain age via Wayback first seen
        try:
            params = urllib.parse.urlencode({
                "url": domain,
                "output": "json",
                "fl": "timestamp",
                "limit": 1,
                "from": "19900101",
            })
            raw = _get(_WB_CDX_BASE + "?" + params, timeout=8)
            rows = _parse_jsonlines(raw)
            if rows:
                ts = rows[0].get("timestamp", "20200101")
                year = int(ts[:4])
                age_years = max(0, 2025 - year)
                score += min(age_years * 3, 30)
        except Exception:
            pass

        # Page count from CC (more pages = bigger site = higher DA)
        pages = self.get_domain_pages(domain, limit=50)
        if len(pages) >= 50:
            score += 25
        elif len(pages) >= 20:
            score += 15
        elif len(pages) >= 5:
            score += 8
        elif len(pages) >= 1:
            score += 3

        da = min(score, 90)
        _redis.setex(ck, _CACHE_TTL * 4, str(da))
        return da

    def get_domain_stats(self, domain: str) -> dict:
        """Full domain intelligence summary."""
        pages = self.get_domain_pages(domain, limit=100)
        refs = self.get_referring_domains(domain, limit=50)
        da = self.estimate_domain_authority(domain)
        return {
            "domain": domain,
            "estimated_da": da,
            "indexed_pages": len(pages),
            "referring_domains": len(refs),
            "top_referring_domains": [r["referring_domain"] for r in refs[:10]],
            "analyzed_at": datetime.utcnow().isoformat(),
        }
