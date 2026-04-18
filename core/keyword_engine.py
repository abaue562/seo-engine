"""Enterprise keyword engine — seed expansion, cannibalization detection, intent routing.

Handles:
  - Dense seed generation for any tenant/niche (50-200 seeds per business)
  - Cannibalization detection across published pages
  - Intent-based content template routing
  - Volume tier refinement with 7 buckets (not 4)

All methods are tenant-aware: pass business_id, get scaled output.

Usage:
    from core.keyword_engine import expand_seeds, detect_cannibalization, route_by_intent
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Optional

import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
DB_PATH = "data/storage/seo_engine.db"

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


# ── Business loader ───────────────────────────────────────────────────────────

def _load_business(business_id: str) -> dict:
    try:
        raw = json.loads(Path("data/storage/businesses.json").read_text())
        biz_list = raw if isinstance(raw, list) else list(raw.values())
        for b in biz_list:
            if b.get("id") == business_id or b.get("business_id") == business_id:
                return b
    except Exception:
        pass
    return {}


# ── Seed expansion ────────────────────────────────────────────────────────────

_MODIFIERS = {
    "local": ["near me", "in {city}", "{city}", "{city} bc", "{city} canada",
              "{province}", "kelowna", "okanagan", "vernon bc", "penticton"],
    "intent_commercial": ["best", "top rated", "professional", "affordable", "cheap",
                          "cost", "price", "quote", "estimate", "company", "service"],
    "intent_transactional": ["hire", "book", "call", "get quote", "same day",
                              "emergency", "24 hour", "licensed", "insured"],
    "intent_informational": ["how to", "what is", "why", "tips", "guide", "diy",
                              "when to", "signs you need", "how often", "benefits of"],
    "long_tail_prefix": ["local", "professional", "certified", "experienced", "trusted",
                          "residential", "commercial", "affordable"],
    "long_tail_suffix": ["services", "contractor", "company", "specialist", "experts",
                          "installation", "repair", "maintenance", "cleaning", "removal"],
    "seasonal": ["spring", "fall", "winter", "annual", "seasonal"],
}

_SERVICE_EXPANSIONS: dict[str, list[str]] = {
    "gutter": [
        "gutter cleaning", "gutter repair", "gutter guards", "gutter installation",
        "eavestroughs cleaning", "downspout cleaning", "leaf guard installation",
        "gutter protection", "eavestroughs repair", "gutter replacement",
        "clogged gutters", "overflowing gutters", "gutter inspection",
    ],
    "roof": [
        "roof moss removal", "moss treatment roof", "roof cleaning",
        "roof rejuvenation", "roof maintenance", "algae removal roof",
        "lichen removal roof", "zinc strip installation", "soft wash roof",
        "roof soft washing", "asphalt shingle cleaning",
    ],
    "window": [
        "window cleaning", "window washing", "pressure washing windows",
        "exterior window cleaning", "window squeegee service", "screen cleaning",
        "solar panel cleaning", "skylight cleaning", "storefront window cleaning",
    ],
    "lighting": [
        "landscape lighting", "outdoor lighting", "permanent holiday lights",
        "LED landscape lights", "pathway lighting", "driveway lighting",
        "tree uplighting", "string lights installation", "architectural lighting",
        "security lighting installation", "low voltage lighting",
    ],
    "exterior": [
        "exterior cleaning", "pressure washing", "house washing",
        "driveway cleaning", "patio cleaning", "deck cleaning",
        "fence cleaning", "concrete cleaning", "soft washing service",
    ],
}


def _google_suggest(query: str) -> list[str]:
    cache_key = f"suggest:{hashlib.md5(query.encode()).hexdigest()[:12]}"
    cached = _redis.get(cache_key)
    if cached:
        return json.loads(cached)
    try:
        q = urllib.parse.quote_plus(query)
        url = f"https://suggestqueries.google.com/complete/search?client=firefox&q={q}"
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())
            results = data[1][:10] if len(data) > 1 else []
            _redis.setex(cache_key, 86400, json.dumps(results))
            return results
    except Exception:
        return []


def expand_seeds(
    business_id: str,
    target_count: int = 100,
    use_suggest: bool = True,
) -> list[dict]:
    """Generate dense keyword seeds for a business.

    Produces 50-200 seeds depending on target_count.
    Uses service expansions + modifiers + Google Suggest.
    Deduplicates against existing keyword_intel table.

    Args:
        business_id:  Tenant identifier.
        target_count: Target number of new seeds to return.
        use_suggest:  Whether to call Google Suggest API.

    Returns:
        List of {keyword, source, priority} dicts — highest priority first.
    """
    biz = _load_business(business_id)
    if not biz:
        log.warning("expand_seeds.no_biz  business_id=%s", business_id)
        return []

    city = biz.get("city", "Kelowna")
    province = biz.get("province", "BC")
    services = biz.get("services", [])
    if isinstance(services, str):
        try:
            services = json.loads(services)
        except Exception:
            services = [services]

    # Existing keywords to avoid duplicating
    try:
        conn = sqlite3.connect(DB_PATH)
        existing = {r[0].lower() for r in conn.execute(
            "SELECT keyword FROM keyword_intel WHERE business_id=?", [business_id]
        ).fetchall()}
        conn.close()
    except Exception:
        existing = set()

    seeds = []
    seen = set(existing)

    def _add(kw: str, source: str, priority: int):
        kw = kw.strip().lower()
        if kw and kw not in seen and len(kw) > 3:
            seen.add(kw)
            seeds.append({"keyword": kw, "source": source, "priority": priority})

    # 1. Service-specific expansions
    for svc in services:
        svc_lower = svc.lower()
        for key, variants in _SERVICE_EXPANSIONS.items():
            if key in svc_lower or svc_lower in key:
                for v in variants:
                    _add(v, "service_expansion", 90)
                    _add(f"{v} {city.lower()}", "service_local", 95)

    # 2. Modifier matrix
    base_services = [s.lower() for s in services[:5]]
    for svc in base_services:
        for mod in _MODIFIERS["local"]:
            loc = mod.replace("{city}", city.lower()).replace("{province}", province.lower())
            _add(f"{svc} {loc}", "local_modifier", 85)
        for mod in _MODIFIERS["intent_transactional"]:
            _add(f"{mod} {svc} {city.lower()}", "transactional", 80)
        for mod in _MODIFIERS["intent_commercial"]:
            _add(f"best {svc} {city.lower()}", "commercial", 75)
            _add(f"{svc} {mod}", "commercial", 70)
        for mod in _MODIFIERS["intent_informational"]:
            _add(f"{mod} {svc}", "informational", 60)

    # 3. Long-tail combinations
    for svc in base_services:
        for pre in _MODIFIERS["long_tail_prefix"]:
            _add(f"{pre} {svc} {city.lower()}", "long_tail", 65)
        for suf in _MODIFIERS["long_tail_suffix"]:
            _add(f"{svc} {suf} {city.lower()}", "long_tail", 65)

    # 4. Google Suggest for top services
    if use_suggest:
        for svc in base_services[:3]:
            suggestions = _google_suggest(f"{svc} {city.lower()}")
            for s in suggestions:
                _add(s, "google_suggest", 88)

    seeds.sort(key=lambda x: x["priority"], reverse=True)
    result = seeds[:target_count]
    log.info("expand_seeds.done  biz=%s  new_seeds=%d  target=%d",
             business_id, len(result), target_count)
    return result


# ── Cannibalization detection ─────────────────────────────────────────────────

def detect_cannibalization(business_id: str) -> list[dict]:
    """Find keyword cannibalization across published pages.

    Two pages targeting the same keyword will split click share and
    confuse Google about which page to rank.

    Returns:
        List of {keyword, urls, severity} dicts for each cannibalized keyword.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        # Check keyword_rankings table
        rows = conn.execute(
            """
            SELECT keyword, url FROM keyword_rankings
            WHERE business_id=? AND url IS NOT NULL AND url != ''
            """,
            [business_id],
        ).fetchall()
        conn.close()
    except Exception:
        rows = []

    # Also check parasite_pages
    parasite_kws: list[tuple] = []
    try:
        conn = sqlite3.connect(DB_PATH)
        parasite_kws = conn.execute(
            "SELECT keyword, url FROM parasite_pages WHERE business_id=? AND keyword IS NOT NULL",
            [business_id],
        ).fetchall()
        conn.close()
    except Exception:
        pass

    # Group by normalized keyword
    from collections import defaultdict
    kw_to_urls: dict[str, list[str]] = defaultdict(list)
    for kw, url in list(rows) + list(parasite_kws):
        if kw and url:
            # Normalize: lowercase, strip location suffixes for comparison
            normalized = re.sub(r'\b(kelowna|bc|canada|okanagan|vernon|penticton)\b', '', kw.lower()).strip()
            normalized = re.sub(r'\s+', ' ', normalized).strip()
            if normalized:
                kw_to_urls[normalized].append(url)

    issues = []
    for kw, urls in kw_to_urls.items():
        unique_urls = list(dict.fromkeys(urls))  # deduplicate preserving order
        if len(unique_urls) >= 2:
            severity = "high" if len(unique_urls) >= 3 else "medium"
            issues.append({
                "keyword": kw,
                "urls": unique_urls,
                "page_count": len(unique_urls),
                "severity": severity,
                "recommendation": f"Consolidate pages or add canonical to secondary URLs pointing at primary.",
            })

    issues.sort(key=lambda x: x["page_count"], reverse=True)
    log.info("detect_cannibalization.done  biz=%s  issues=%d", business_id, len(issues))
    return issues


# ── Intent → content template router ─────────────────────────────────────────

CONTENT_TEMPLATES = {
    "transactional": {
        "structure": ["hero_cta", "trust_signals", "service_details", "local_proof", "faq", "cta_footer"],
        "tone": "direct, action-oriented",
        "cta": "Book a Free Quote",
        "schema_types": ["local_business", "service", "review", "breadcrumb"],
        "word_count_target": 800,
        "faq_count": 3,
    },
    "commercial": {
        "structure": ["comparison_intro", "benefits", "pricing_signals", "reviews", "faq", "cta"],
        "tone": "consultative, value-focused",
        "cta": "Compare Services",
        "schema_types": ["local_business", "service", "review", "breadcrumb"],
        "word_count_target": 1200,
        "faq_count": 5,
    },
    "informational": {
        "structure": ["direct_answer", "key_takeaways", "detailed_guide", "howto_steps", "faq", "soft_cta"],
        "tone": "educational, expert",
        "cta": "Need a Professional?",
        "schema_types": ["local_business", "howto", "faq", "breadcrumb"],
        "word_count_target": 1800,
        "faq_count": 8,
    },
    "navigational": {
        "structure": ["brand_hero", "service_overview", "contact", "locations"],
        "tone": "welcoming, clear",
        "cta": "Find Us",
        "schema_types": ["local_business", "breadcrumb"],
        "word_count_target": 600,
        "faq_count": 2,
    },
}


def route_by_intent(keyword: str, intent: Optional[str] = None) -> dict:
    """Return the content template spec for a keyword's intent.

    If intent not provided, infers it from keyword signals.
    Returns template dict with structure, schema_types, word_count_target.
    """
    if not intent:
        from core.intent_classifier import classify_intent
        probs = classify_intent(keyword)
        intent = probs.get("mixed_primary", "informational")

    template = CONTENT_TEMPLATES.get(intent, CONTENT_TEMPLATES["informational"]).copy()
    template["intent"] = intent
    template["keyword"] = keyword
    log.debug("route_by_intent  kw=%s  intent=%s  wc=%d",
              keyword, intent, template["word_count_target"])
    return template


# ── Refined volume tiers (7 buckets, not 4) ──────────────────────────────────

VOLUME_TIERS_REFINED = {
    # tier_name: (min_score, est_monthly_low, est_monthly_high)
    "ultra_high":  (90, 50_000, 500_000),
    "very_high":   (75, 10_000, 50_000),
    "high":        (60, 3_000,  10_000),
    "medium_high": (48, 1_000,  3_000),
    "medium":      (35, 300,    1_000),
    "low":         (20, 50,     300),
    "micro":       (0,  5,      50),
}


def classify_volume_tier(score: int) -> tuple[str, int, int]:
    """Map a volume score (0-100) to a refined tier.

    Returns (tier_name, est_low, est_high).
    More granular than the original 4-tier system — critical for competitive
    keyword selection where 1k/mo and 10k/mo require very different strategies.
    """
    for tier_name, (min_score, low, high) in VOLUME_TIERS_REFINED.items():
        if score >= min_score:
            return tier_name, low, high
    return "micro", 5, 50


def opportunity_score(volume_score: int, difficulty: int, intent: str) -> float:
    """Score a keyword's ranking opportunity.

    Combines volume, difficulty, and intent multiplier.
    Higher = better opportunity.
    """
    intent_mult = {
        "transactional": 2.0,
        "commercial": 1.6,
        "informational": 1.0,
        "navigational": 0.5,
    }.get(intent, 1.0)

    # Protect against division by zero
    kd_factor = max(difficulty, 1)
    score = (volume_score / kd_factor) * intent_mult * 100
    return round(score, 2)


# ── PAA → content content queue wiring ───────────────────────────────────────

def build_paa_content_queue(business_id: str, top_n: int = 5) -> list[dict]:
    """Convert PAA tree data into content generation jobs.

    Takes the top PAA questions for a business's keywords,
    classifies intent, and returns a prioritized content queue.

    Args:
        business_id: Tenant identifier.
        top_n:       Max content jobs to queue per run.

    Returns:
        List of {keyword, question, intent, template, priority} dicts.
    """
    biz = _load_business(business_id)
    services = biz.get("services", [])
    if isinstance(services, str):
        try:
            services = json.loads(services)
        except Exception:
            services = [services]

    city = biz.get("city", "Kelowna")

    paa_cache_dir = Path("data/storage/paa_cache")
    if not paa_cache_dir.exists():
        log.info("build_paa_content_queue.no_cache  biz=%s", business_id)
        return []

    # Collect all cached PAA questions
    all_questions: list[tuple[str, str]] = []
    for cache_file in paa_cache_dir.glob("*.json"):
        try:
            data = json.loads(cache_file.read_text())
            seed = data.get("keyword", "")
            questions = data.get("questions", [])
            for q in questions:
                if isinstance(q, str):
                    all_questions.append((seed, q))
                elif isinstance(q, dict):
                    all_questions.append((seed, q.get("question", "")))
        except Exception:
            continue

    if not all_questions:
        log.info("build_paa_content_queue.empty_cache  biz=%s", business_id)
        return []

    # Filter to questions relevant to this business
    svc_terms = set()
    for s in services:
        svc_terms.update(s.lower().split())
    svc_terms.update([city.lower(), "kelowna", "okanagan", "bc"])

    relevant = [
        (seed, q) for seed, q in all_questions
        if any(t in q.lower() for t in svc_terms)
        and len(q) > 15
    ]

    # Deduplicate and score
    seen_q: set[str] = set()
    queue: list[dict] = []

    # Check existing keyword_intel to avoid re-queueing already-researched questions
    try:
        conn = sqlite3.connect(DB_PATH)
        done_kws = {r[0].lower() for r in conn.execute(
            "SELECT keyword FROM keyword_intel WHERE business_id=?", [business_id]
        ).fetchall()}
        conn.close()
    except Exception:
        done_kws = set()

    for seed, q in relevant:
        q_lower = q.lower().rstrip("?")
        if q_lower in seen_q or q_lower in done_kws:
            continue
        seen_q.add(q_lower)

        from core.intent_classifier import classify_intent
        probs = classify_intent(q)
        intent = probs.get("mixed_primary", "informational")
        template = route_by_intent(q, intent)

        # Score by intent value
        priority = {
            "transactional": 90,
            "commercial": 80,
            "informational": 70,
            "navigational": 50,
        }.get(intent, 60)

        queue.append({
            "keyword": q_lower,
            "question": q,
            "seed": seed,
            "intent": intent,
            "template": template,
            "priority": priority,
            "business_id": business_id,
        })

    queue.sort(key=lambda x: x["priority"], reverse=True)
    result = queue[:top_n]
    log.info("build_paa_content_queue.done  biz=%s  total_paa=%d  relevant=%d  queued=%d",
             business_id, len(all_questions), len(relevant), len(result))
    return result
