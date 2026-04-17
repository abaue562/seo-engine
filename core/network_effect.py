import hashlib, json, logging, sqlite3
from typing import Dict, List, Optional

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

def _volume_bucket(volume: int) -> str:
    if volume < 100: return "nano"
    if volume < 1000: return "low"
    if volume < 10000: return "medium"
    return "high"

def build_cohort_fingerprint(keyword: str, industry_code: str, location_tier: str,
                              intent: str, volume: int) -> Dict:
    volume_bucket = _volume_bucket(volume)
    from core.signal_layer import get_cohort_fingerprint
    fingerprint = get_cohort_fingerprint(industry_code, location_tier, intent, volume_bucket)
    return {"fingerprint": fingerprint, "industry_code": industry_code,
            "location_tier": location_tier, "intent": intent, "volume_bucket": volume_bucket}

def get_patterns_for_brief(keyword: str, industry_code: str, location_tier: str,
                            intent: str, volume: int = 500, top_n: int = 5) -> Dict:
    cohort = build_cohort_fingerprint(keyword, industry_code, location_tier, intent, volume)
    try:
        from core.signal_layer import get_cohort_patterns
        patterns = get_cohort_patterns(cohort["fingerprint"])[:top_n]
    except Exception:
        patterns = []

    tenant_count = max((p.get("tenant_count", 0) for p in patterns), default=0)
    if patterns and tenant_count >= 20:
        msg = f"Using patterns learned from {tenant_count}+ similar keywords across tenants in {industry_code} ({location_tier} markets)"
    elif patterns:
        msg = "Using platform-wide patterns (cohort data accumulating)"
    else:
        msg = "No cohort data yet — using platform defaults"

    return {"patterns": patterns, "cohort": cohort, "tenant_count": tenant_count, "provenance_message": msg}

def inject_patterns_into_brief(brief_dict: dict, pattern_result: dict) -> dict:
    enriched = dict(brief_dict)
    patterns = pattern_result.get("patterns", [])
    if not patterns:
        return enriched
    top = patterns[0]
    enriched["platform_pattern_guidance"] = {
        "top_pattern": top.get("pattern_key"),
        "expected_confidence": top.get("confidence"),
        "expected_rank_at_90d": top.get("avg_rank_at_90d"),
        "provenance": pattern_result.get("provenance_message"),
    }
    log.debug("network_effect.patterns_injected  pattern=%s  confidence=%.2f", top.get("pattern_key"), top.get("confidence", 0))
    return enriched

def measure_network_lift(cohort_fingerprint: str) -> Dict:
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute("""
            SELECT strftime('%Y-%m', p.generated_at) as month,
                   AVG(o.rank_median) as avg_rank,
                   COUNT(*) as sample
            FROM content_provenance p
            JOIN content_outcomes o ON p.content_id = o.content_id AND o.snapshot_days = 90
            WHERE p.cohort_fingerprint = ? AND o.rank_median IS NOT NULL
            GROUP BY month ORDER BY month
        """, [cohort_fingerprint]).fetchall()
        conn.close()
        if len(rows) < 2:
            return {"status": "insufficient_data", "periods": len(rows)}
        first_avg = rows[0][1]
        last_avg = rows[-1][1]
        lift = round(first_avg - last_avg, 1)  # positive = improvement (lower rank number = better)
        return {"cohort": cohort_fingerprint[:8], "periods": len(rows), "first_period_avg_rank": round(first_avg, 1), "latest_period_avg_rank": round(last_avg, 1), "lift": lift, "improving": lift > 0}
    except Exception as exc:
        conn.close()
        log.exception("network_effect.measure_lift_error")
        return {"status": "error", "error": str(exc)}
