"""
Self-hosted keyword intelligence — volume estimation, difficulty, intent,
trending signals, and related keyword discovery.
No paid keyword API needed. Uses: Google Autocomplete, Bing Autosuggest,
AION Twitter Intel, GSC data, and SERP signal analysis.
"""
from __future__ import annotations
import hashlib
import json
import logging
import re
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Optional

import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
_DB = "data/storage/seo_engine.db"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS keyword_intel (
            id              TEXT PRIMARY KEY,
            keyword         TEXT NOT NULL UNIQUE,
            volume_estimate INTEGER DEFAULT 0,
            difficulty      INTEGER DEFAULT 50,
            intent          TEXT DEFAULT 'informational',
            trending_score  INTEGER DEFAULT 0,
            related         TEXT DEFAULT '[]',
            gsc_clicks      INTEGER DEFAULT 0,
            gsc_impressions INTEGER DEFAULT 0,
            sources         TEXT DEFAULT '[]',
            updated_at      TEXT
        );
    """)
    c.commit()
    return c


# ── Volume estimation ─────────────────────────────────────────────────────────

def _autocomplete_depth(keyword: str) -> int:
    """
    Use autocomplete suggestion count as a volume proxy.
    More autocomplete variants = higher search volume signal.
    """
    encoded = urllib.parse.quote_plus(keyword)
    total = 0
    prefixes = [keyword, keyword + " a", keyword + " b", keyword + " how", keyword + " what"]
    for prefix in prefixes[:3]:
        try:
            req = urllib.request.Request(
                f"https://suggestqueries.google.com/complete/search?client=firefox&q={urllib.parse.quote_plus(prefix)}",
                headers={"User-Agent": _UA}
            )
            data = json.loads(urllib.request.urlopen(req, timeout=6).read())
            total += len(data[1])
        except Exception:
            pass
    return total  # 0-30 range typically


def _gsc_volume(keyword: str, business_id: str = "") -> dict:
    """Pull GSC impressions/clicks as volume signal if available."""
    if not business_id:
        return {}
    try:
        with _conn() as c:
            # Check ranking_history table (populated by GSC connector)
            row = c.execute("""
                SELECT impressions, clicks FROM ranking_history
                WHERE business_id=? AND keyword=? ORDER BY checked_at DESC LIMIT 1
            """, [business_id, keyword]).fetchone()
        if row:
            return {"gsc_impressions": row[0] or 0, "gsc_clicks": row[1] or 0}
    except Exception:
        pass
    return {}


def estimate_volume(keyword: str, business_id: str = "") -> int:
    """
    Estimate monthly search volume (0–100,000 range).
    Combines: autocomplete depth signal, GSC impressions (if available), Twitter trending.
    Bucketed to avoid false precision: 10, 50, 100, 500, 1K, 5K, 10K, 50K, 100K.
    """
    cache_key = f"kv:{hashlib.sha256(keyword.encode()).hexdigest()[:12]}"
    cached = _redis.get(cache_key)
    if cached:
        return int(cached)

    BUCKETS = [10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000]

    depth = _autocomplete_depth(keyword)
    gsc = _gsc_volume(keyword, business_id)
    gsc_imp = gsc.get("gsc_impressions", 0)

    # Score 0-30 based on autocomplete depth
    if gsc_imp > 0:
        # GSC is ground truth — use it directly
        est = gsc_imp * 10  # rough: impressions * 10 ≈ monthly volume
    else:
        # Map depth (0-30) → estimated volume
        ratio = min(depth / 30, 1.0)
        est = int(BUCKETS[int(ratio * (len(BUCKETS) - 1))])

    # Snap to nearest bucket
    bucket = min(BUCKETS, key=lambda b: abs(b - est))
    _redis.setex(cache_key, 86400 * 7, str(bucket))
    return bucket


# ── Trending signals ──────────────────────────────────────────────────────────

def get_trending_keywords(niche: str, location: str = "") -> list[dict]:
    """
    Pull trending keywords from AION Twitter Intel + Google Trends autocomplete.
    No API key needed.
    """
    trending = []

    # Twitter Intel signals
    try:
        from core.aion_bridge import aion
        signals = aion.twitter_signals(limit=50)
        for sig in (signals or []):
            text = sig.get("text", "") or sig.get("content", "") or str(sig)
            if niche.lower() in text.lower():
                keywords_in_signal = re.findall(r'\b[a-z][a-z\s]{3,30}(?:service|guide|tip|cost|price|best|how|why|what)\b', text, re.I)
                for kw in keywords_in_signal[:2]:
                    trending.append({"keyword": kw.strip(), "source": "twitter_intel", "score": sig.get("engagement", 1)})
    except Exception:
        log.exception("get_trending_keywords: twitter_intel failed")

    # Google Trends proxy via autocomplete with "2025" or "near me" modifiers
    trend_queries = [f"{niche} 2025", f"{niche} near me", f"best {niche}", f"{niche} cost"]
    for q in trend_queries:
        try:
            req = urllib.request.Request(
                f"https://suggestqueries.google.com/complete/search?client=firefox&q={urllib.parse.quote_plus(q)}",
                headers={"User-Agent": _UA}
            )
            data = json.loads(urllib.request.urlopen(req, timeout=6).read())
            for term in data[1][:3]:
                trending.append({"keyword": term, "source": "autocomplete_trend", "score": 5})
        except Exception:
            pass

    # Dedupe
    seen: set[str] = set()
    out = []
    for t in sorted(trending, key=lambda x: x.get("score", 0), reverse=True):
        if t["keyword"].lower() not in seen:
            seen.add(t["keyword"].lower())
            out.append(t)

    log.info("get_trending_keywords  niche=%s  found=%d", niche, len(out))
    return out[:30]


# ── Full keyword research ─────────────────────────────────────────────────────

def research_keyword(keyword: str, business_id: str = "", location: str = "") -> dict:
    """
    Full keyword intelligence: volume estimate, difficulty, intent, suggestions, trending score.
    Stores result in SQLite for reuse.
    """
    from core.serp_scraper import get_keyword_suggestions, estimate_keyword_difficulty
    from core.intent_classifier import classify_intent

    # Check cache
    with _conn() as c:
        row = c.execute("SELECT * FROM keyword_intel WHERE keyword=?", [keyword]).fetchone()
        if row:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(row["updated_at"])).days
            if age < 7:
                d = dict(row)
                d["related"] = json.loads(d.get("related") or "[]")
                d["sources"] = json.loads(d.get("sources") or "[]")
                return d

    volume = estimate_volume(keyword, business_id)
    difficulty_data = estimate_keyword_difficulty(keyword)
    difficulty = difficulty_data.get("difficulty", 50)
    suggestions = get_keyword_suggestions(keyword, location)[:20]

    # Intent from SERP signals
    try:
        serp_snapshot = difficulty_data  # has organic results embedded
        intent = classify_intent(keyword, serp_snapshot)
        intent_str = intent.get("primary", "informational") if isinstance(intent, dict) else str(intent)
    except Exception:
        intent_str = "informational"

    # Trending score from autocomplete freshness
    trending_score = min(_autocomplete_depth(keyword) * 3, 30)

    now = datetime.now(timezone.utc).isoformat()
    kid = hashlib.sha256(keyword.encode()).hexdigest()[:12]
    gsc = _gsc_volume(keyword, business_id)

    with _conn() as c:
        c.execute("""
            INSERT INTO keyword_intel
                (id,keyword,volume_estimate,difficulty,intent,trending_score,related,
                 gsc_clicks,gsc_impressions,sources,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(keyword) DO UPDATE SET
                volume_estimate=excluded.volume_estimate, difficulty=excluded.difficulty,
                intent=excluded.intent, trending_score=excluded.trending_score,
                related=excluded.related, gsc_clicks=excluded.gsc_clicks,
                gsc_impressions=excluded.gsc_impressions, updated_at=excluded.updated_at
        """, [kid, keyword, volume, difficulty, intent_str, trending_score,
              json.dumps(suggestions), gsc.get("gsc_clicks", 0), gsc.get("gsc_impressions", 0),
              json.dumps(["bing_serp", "google_autocomplete"]), now])

    result = {
        "keyword": keyword,
        "volume_estimate": volume,
        "difficulty": difficulty,
        "intent": intent_str,
        "trending_score": trending_score,
        "related": suggestions,
        "gsc_clicks": gsc.get("gsc_clicks", 0),
        "gsc_impressions": gsc.get("gsc_impressions", 0),
        "sources": ["bing_serp", "google_autocomplete"],
        "updated_at": now,
    }
    log.info("research_keyword  kw=%s  vol=%d  diff=%d  intent=%s", keyword, volume, difficulty, intent_str)
    return result


def get_keyword_opportunities(business_id: str, niche: str, location: str = "", limit: int = 30) -> list[dict]:
    """
    Find low-difficulty, decent-volume keywords you haven't targeted yet.
    Combines trending signals + suggestions from seed keywords.
    """
    # Get existing targeted keywords
    with sqlite3.connect(_DB) as c:
        existing = set(
            r[0] for r in c.execute(
                "SELECT DISTINCT keyword FROM keyword_rankings WHERE business_id=?", [business_id]
            ).fetchall()
        )

    # Get suggestions from niche seed
    from core.serp_scraper import get_keyword_suggestions
    seeds = get_keyword_suggestions(f"{niche} {location}".strip())
    trending = [t["keyword"] for t in get_trending_keywords(niche, location)]
    all_candidates = list(dict.fromkeys(seeds + trending))[:60]

    opportunities = []
    for kw in all_candidates:
        if kw.lower() in (e.lower() for e in existing):
            continue
        intel = research_keyword(kw, business_id, location)
        opp_score = intel["volume_estimate"] - (intel["difficulty"] * 10) + intel["trending_score"] * 5
        if opp_score > 0:
            intel["opportunity_score"] = opp_score
            opportunities.append(intel)

    opportunities.sort(key=lambda x: x.get("opportunity_score", 0), reverse=True)
    log.info("get_keyword_opportunities  biz=%s  found=%d", business_id, len(opportunities))
    return opportunities[:limit]
