import logging, sqlite3
from typing import Dict, List

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

PATTERN_DESCRIPTIONS = {
    "faq_schema": "Include FAQ schema markup with 5+ questions",
    "howto_schema": "Use HowTo schema for step-by-step content",
    "localbusiness_schema": "Add LocalBusiness schema with NAP data",
    "long_form_1500+": "Target 1,500+ words for comprehensive coverage",
    "medium_form_800-1200": "Target 800-1,200 words — ideal for local intent",
    "short_form_400-800": "Concise 400-800 words for transactional pages",
    "h2_heavy": "Use 4+ H2 subheadings for strong structure",
    "paa_targeting": "Answer 3+ People Also Ask questions explicitly",
    "local_pack_optimized": "Include city/region name in H1 and first paragraph",
    "video_script": "Add video script / YouTube embed",
    "table_of_contents": "Include jump-links table of contents",
    "comparison_table": "Use comparison table for commercial intent keywords",
}

def format_pattern_action(pattern_key: str) -> str:
    return PATTERN_DESCRIPTIONS.get(pattern_key, f"Apply pattern: {pattern_key.replace('_', ' ')}")

def get_cluster_playbook(cohort_fingerprint: str, limit: int = 5) -> List[Dict]:
    try:
        from core.signal_layer import get_cohort_patterns
        patterns = get_cohort_patterns(cohort_fingerprint)[:limit]
    except Exception:
        patterns = []
    return [{"pattern_key": p["pattern_key"], "action": format_pattern_action(p["pattern_key"]),
             "confidence": p["confidence"], "avg_rank_at_90d": p.get("avg_rank_at_90d"),
             "tenant_count": p["tenant_count"]} for p in patterns]

def get_benchmark(business_id: str, cohort_fingerprint: str) -> Dict:
    conn = sqlite3.connect(DB_PATH)
    tenant_avg = conn.execute("""
        SELECT AVG(o.rank_median)
        FROM content_provenance p
        JOIN content_outcomes o ON p.content_id = o.content_id AND o.snapshot_days = 90
        WHERE p.business_id = ? AND o.rank_median IS NOT NULL
    """, [business_id]).fetchone()[0]
    cohort_avg = conn.execute("""
        SELECT AVG(o.rank_median)
        FROM content_provenance p
        JOIN content_outcomes o ON p.content_id = o.content_id AND o.snapshot_days = 90
        WHERE p.cohort_fingerprint = ? AND o.rank_median IS NOT NULL
    """, [cohort_fingerprint]).fetchone()[0]
    conn.close()
    if not tenant_avg or not cohort_avg:
        return {"status": "insufficient_data"}
    above_avg = tenant_avg < cohort_avg  # lower rank number = better
    diff = round(cohort_avg - tenant_avg, 1)
    return {"tenant_avg_rank": round(tenant_avg, 1), "cohort_avg_rank": round(cohort_avg, 1),
            "above_average": above_avg, "rank_difference": diff,
            "message": f"Your content ranks {abs(diff)} positions {'better' if above_avg else 'worse'} than similar businesses"}

def get_playbook(business_id: str, industry_code: str = "", location_tier: str = "") -> Dict:
    try:
        from core.signal_layer import get_cohort_fingerprint
        from core.network_effect import build_cohort_fingerprint
        cohort = build_cohort_fingerprint("", industry_code, location_tier, "informational", 500)
        fingerprint = cohort["fingerprint"]
    except Exception:
        fingerprint = ""

    patterns = get_cluster_playbook(fingerprint)
    benchmark = get_benchmark(business_id, fingerprint)
    recommendations = []
    for p in patterns[:3]:
        if p["confidence"] > 0.6:
            recommendations.append({"action": p["action"], "expected_rank": p.get("avg_rank_at_90d"), "confidence": p["confidence"]})

    log.info("playbook.generated  biz=%s  patterns=%d", business_id, len(patterns))
    return {"business_id": business_id, "industry_code": industry_code, "location_tier": location_tier,
            "top_patterns": patterns, "benchmark": benchmark, "recommendations": recommendations,
            "cohort_fingerprint": fingerprint}
