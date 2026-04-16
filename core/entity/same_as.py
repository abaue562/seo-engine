"""
sameAs entity cross-reference manager.

The sameAs property in schema.org is the most reliable signal
for Google's Knowledge Graph entity confidence scoring.
This system maintains a registry of external entity URLs per business
and injects them into all schema instances.

Usage:
    registry = SameAsRegistry()
    registry.add_entity(business_id="plumber-nyc-joe", platform="google_business", url="https://g.co/...")
    same_as = registry.get_same_as_urls(business_id="plumber-nyc-joe")
    schema = registry.inject_same_as(schema_dict, business_id)
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

log = logging.getLogger(__name__)

KNOWN_PLATFORMS = {
    "google_business": {"authority": 10, "prefix": "https://www.google.com/maps/"},
    "facebook": {"authority": 8, "prefix": "https://www.facebook.com/"},
    "linkedin": {"authority": 9, "prefix": "https://www.linkedin.com/"},
    "twitter": {"authority": 7, "prefix": "https://twitter.com/"},
    "instagram": {"authority": 7, "prefix": "https://www.instagram.com/"},
    "yelp": {"authority": 8, "prefix": "https://www.yelp.com/"},
    "bbb": {"authority": 9, "prefix": "https://www.bbb.org/"},
    "wikipedia": {"authority": 10, "prefix": "https://en.wikipedia.org/"},
    "wikidata": {"authority": 10, "prefix": "https://www.wikidata.org/"},
    "crunchbase": {"authority": 8, "prefix": "https://www.crunchbase.com/"},
    "houzz": {"authority": 7, "prefix": "https://www.houzz.com/"},
    "angi": {"authority": 7, "prefix": "https://www.angi.com/"},
    "thumbtack": {"authority": 7, "prefix": "https://www.thumbtack.com/"},
}

# Alternate URL prefixes that are still valid for a given platform
_PLATFORM_ALT_PREFIXES: dict[str, list[str]] = {
    "google_business": [
        "https://maps.google.com/",
        "https://g.co/",
        "https://goo.gl/maps/",
        "https://www.google.com/search?q=",
    ],
    "twitter": ["https://x.com/"],
    "yelp": ["https://yelp.com/"],
    "linkedin": ["https://linkedin.com/"],
    "bbb": ["https://bbb.org/"],
    "angi": ["https://www.angieslist.com/", "https://angieslist.com/"],
}


def _url_matches_platform(url: str, platform: str) -> bool:
    """Check whether url is a plausible URL for the given platform."""
    info = KNOWN_PLATFORMS.get(platform)
    if not info:
        return False
    primary = info["prefix"]
    if url.startswith(primary):
        return True
    for alt in _PLATFORM_ALT_PREFIXES.get(platform, []):
        if url.startswith(alt):
            return True
    return False


class SameAsRegistry:
    STORAGE_PATH = Path("data/storage/entity_registry")

    def __init__(self):
        self.storage = self.STORAGE_PATH
        self.storage.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Core CRUD
    # ------------------------------------------------------------------

    def add_entity(self, business_id: str, platform: str, url: str) -> bool:
        """Add an external entity URL for a business.

        Validates URL matches expected platform prefix.
        Returns True if added, False if invalid.
        """
        if platform not in KNOWN_PLATFORMS:
            log.warning("same_as.unknown_platform  platform=%s", platform)
            return False

        if not _url_matches_platform(url, platform):
            log.warning(
                "same_as.url_mismatch  platform=%s  url=%s  expected_prefix=%s",
                platform, url, KNOWN_PLATFORMS[platform]["prefix"],
            )
            return False

        registry = self.load_registry(business_id)
        entries = registry.setdefault("entities", {})

        entries[platform] = {
            "url": url,
            "platform": platform,
            "authority": KNOWN_PLATFORMS[platform]["authority"],
            "added_at": datetime.utcnow().isoformat(),
            "verified": True,
        }
        registry["updated_at"] = datetime.utcnow().isoformat()
        self.save_registry(business_id, registry)
        log.info("same_as.added  biz=%s  platform=%s", business_id, platform)
        return True

    def get_same_as_urls(self, business_id: str, min_authority: int = 7) -> list[str]:
        """Return all validated sameAs URLs for a business, sorted by authority score.

        Filters to only platforms with authority >= min_authority.
        """
        registry = self.load_registry(business_id)
        entries = registry.get("entities", {})

        qualified = [
            (info["authority"], info["url"])
            for info in entries.values()
            if info.get("authority", 0) >= min_authority and info.get("url")
        ]
        # Sort descending by authority
        qualified.sort(key=lambda x: x[0], reverse=True)
        return [url for _, url in qualified]

    # ------------------------------------------------------------------
    # Schema injection
    # ------------------------------------------------------------------

    def inject_same_as(self, schema: dict, business_id: str) -> dict:
        """Add sameAs array to schema dict. Returns updated schema.

        Only adds if there are verified URLs. Merges with existing sameAs if present.
        """
        urls = self.get_same_as_urls(business_id)
        if not urls:
            return schema

        updated = dict(schema)
        existing = updated.get("sameAs", [])
        if isinstance(existing, str):
            existing = [existing]
        # Merge, deduplicate, preserve order
        merged = list(existing)
        for url in urls:
            if url not in merged:
                merged.append(url)
        updated["sameAs"] = merged
        log.debug("same_as.injected  biz=%s  count=%d", business_id, len(merged))
        return updated

    def inject_same_as_all_schemas(self, schema_list: list[dict], business_id: str) -> list[dict]:
        """Inject sameAs into every schema object in a list."""
        return [self.inject_same_as(schema, business_id) for schema in schema_list]

    # ------------------------------------------------------------------
    # Auto-discovery
    # ------------------------------------------------------------------

    def auto_discover(self, business_name: str, business_id: str, location: str = "") -> dict:
        """Attempt to auto-discover entity URLs by constructing likely profile URLs.

        Returns {discovered: list, failed: list}
        Constructs and validates URLs for: Yelp, BBB, LinkedIn, Facebook
        by searching known URL patterns.
        """
        try:
            import httpx
            client = httpx.Client(timeout=10, follow_redirects=True, headers={
                "User-Agent": "Mozilla/5.0 (compatible; SEOEngine/1.0; entity-discovery)"
            })
        except ImportError:
            try:
                import requests as _req

                class _FakeClient:
                    def get(self, url, **kwargs):
                        return _req.get(url, **kwargs)
                client = _FakeClient()
            except ImportError:
                log.warning("same_as.auto_discover: no HTTP client available")
                return {"discovered": [], "failed": [], "error": "no HTTP client"}

        slug = re.sub(r"[^a-z0-9]+", "-", business_name.lower()).strip("-")
        loc_slug = re.sub(r"[^a-z0-9]+", "-", location.lower()).strip("-") if location else ""

        candidates = {
            "yelp": f"https://www.yelp.com/biz/{slug}" + (f"-{loc_slug}" if loc_slug else ""),
            "bbb": f"https://www.bbb.org/us/{loc_slug or 'us'}/{slug}",
            "linkedin": f"https://www.linkedin.com/company/{slug}",
            "facebook": f"https://www.facebook.com/{slug}",
            "houzz": f"https://www.houzz.com/professionals/{slug}",
            "angi": f"https://www.angi.com/companylist/us/{loc_slug or 'us'}/{slug}.htm",
        }

        discovered = []
        failed = []

        for platform, url in candidates.items():
            try:
                resp = client.get(url, timeout=8)
                if resp.status_code in (200, 301, 302):
                    # Verify the redirected URL still belongs to the platform
                    final_url = str(getattr(resp, "url", url))
                    if _url_matches_platform(final_url, platform) or _url_matches_platform(url, platform):
                        self.add_entity(business_id, platform, url)
                        discovered.append({"platform": platform, "url": url, "status": resp.status_code})
                        log.info("same_as.discovered  biz=%s  platform=%s", business_id, platform)
                    else:
                        failed.append({"platform": platform, "url": url, "reason": "redirect_mismatch"})
                else:
                    failed.append({"platform": platform, "url": url, "reason": f"http_{resp.status_code}"})
            except Exception as e:
                failed.append({"platform": platform, "url": url, "reason": str(e)})

        return {"discovered": discovered, "failed": failed}

    # ------------------------------------------------------------------
    # Entity strength scoring
    # ------------------------------------------------------------------

    def get_entity_strength_score(self, business_id: str) -> dict:
        """Score entity strength 0-100 based on platform coverage and authority.

        Returns {score, platforms_count, has_wikipedia, has_knowledge_panel_signals, grade}
        """
        registry = self.load_registry(business_id)
        entries = registry.get("entities", {})

        if not entries:
            return {
                "score": 0,
                "platforms_count": 0,
                "has_wikipedia": False,
                "has_knowledge_panel_signals": False,
                "grade": "F",
            }

        has_wikipedia = "wikipedia" in entries or "wikidata" in entries
        has_google = "google_business" in entries
        has_bbb = "bbb" in entries
        has_linkedin = "linkedin" in entries

        # Base score: weighted authority sum (max theoretical ~100 for full coverage)
        authority_sum = sum(
            info.get("authority", 5) for info in entries.values()
        )
        # Normalize: full coverage (all 13 platforms) with max authority (10) = 130
        base = min(50, int((authority_sum / 130) * 50))

        bonus = 0
        if has_wikipedia:
            bonus += 30
        if has_google:
            bonus += 20
        if has_bbb:
            bonus += 10
        if has_linkedin:
            bonus += 5

        score = min(100, base + bonus)

        # Knowledge panel signals: Wikipedia + Google Business = strong signal
        has_kp_signals = has_wikipedia or (has_google and has_bbb)

        if score >= 80:
            grade = "A"
        elif score >= 65:
            grade = "B"
        elif score >= 50:
            grade = "C"
        elif score >= 35:
            grade = "D"
        else:
            grade = "F"

        return {
            "score": score,
            "platforms_count": len(entries),
            "has_wikipedia": has_wikipedia,
            "has_knowledge_panel_signals": has_kp_signals,
            "grade": grade,
            "platforms": list(entries.keys()),
        }

    # ------------------------------------------------------------------
    # Storage helpers
    # ------------------------------------------------------------------

    def load_registry(self, business_id: str) -> dict:
        """Load entity registry for business from storage."""
        path = self.storage / f"{business_id}.json"
        if not path.exists():
            return {"business_id": business_id, "entities": {}, "created_at": datetime.utcnow().isoformat()}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("same_as.load_error  biz=%s  err=%s", business_id, e)
            return {"business_id": business_id, "entities": {}}

    def save_registry(self, business_id: str, registry: dict) -> None:
        """Save entity registry to storage."""
        path = self.storage / f"{business_id}.json"
        path.write_text(json.dumps(registry, indent=2, ensure_ascii=False), encoding="utf-8")
        log.debug("same_as.saved  biz=%s", business_id)
