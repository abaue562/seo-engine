import logging, sqlite3
from typing import Dict, List

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

def is_new_tenant(business_id: str, threshold_pages: int = 10) -> bool:
    conn = sqlite3.connect(DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM published_urls WHERE business_id=? AND status='live'", [business_id]).fetchone()[0]
    conn.close()
    return count < threshold_pages

def get_tenant_page_count(business_id: str) -> int:
    conn = sqlite3.connect(DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM published_urls WHERE business_id=? AND status='live'", [business_id]).fetchone()[0]
    conn.close()
    return count

def get_cold_start_patterns(industry_code: str, location_tier: str, intent: str, volume: int = 500) -> Dict:
    try:
        from core.network_effect import build_cohort_fingerprint
        from core.signal_layer import get_cohort_patterns
        cohort = build_cohort_fingerprint("", industry_code, location_tier, intent, volume)
        patterns = get_cohort_patterns(cohort["fingerprint"], min_confidence=0.5)
        for p in patterns:
            ci_width = max(0.1, 0.3 - (p["tenant_count"] / 1000))
            p["confidence_lower"] = round(max(0, p["confidence"] - ci_width), 3)
            p["confidence_upper"] = round(min(1, p["confidence"] + ci_width), 3)
        cohort_tenant_count = max((p["tenant_count"] for p in patterns), default=0)
        return {"patterns": patterns, "cohort": cohort, "cohort_tenant_count": cohort_tenant_count, "is_cold_start": True}
    except Exception as exc:
        log.exception("pattern_transfer.cold_start_error")
        return {"patterns": [], "cohort": {}, "cohort_tenant_count": 0, "is_cold_start": True}

def blend_patterns(business_id: str, cohort_patterns: List[Dict], tenant_patterns: List[Dict]) -> List[Dict]:
    tenant_pages = get_tenant_page_count(business_id)
    tenant_weight = tenant_pages / (tenant_pages + 20)  # Bayesian: starts near 0, approaches 1 with 20+ pages
    cohort_weight = 1 - tenant_weight

    merged = {}
    for p in cohort_patterns:
        key = p["pattern_key"]
        merged[key] = {"pattern_key": key, "confidence": p["confidence"] * cohort_weight, "source": "cohort"}
    for p in tenant_patterns:
        key = p["pattern_key"]
        if key in merged:
            merged[key]["confidence"] += p["confidence"] * tenant_weight
            merged[key]["source"] = "blended"
        else:
            merged[key] = {"pattern_key": key, "confidence": p["confidence"] * tenant_weight, "source": "tenant"}

    result = sorted(merged.values(), key=lambda x: x["confidence"], reverse=True)
    log.debug("pattern_transfer.blended  biz=%s  tenant_weight=%.2f  patterns=%d", business_id, tenant_weight, len(result))
    return result

def get_transfer_message(tenant_page_count: int, cohort_tenant_count: int) -> str:
    if tenant_page_count < 5:
        return f"Using patterns from {cohort_tenant_count}+ similar businesses — your first pages are informed by what's worked for them."
    if tenant_page_count < 20:
        return f"Blending your {tenant_page_count} pages' data with {cohort_tenant_count}+ cohort patterns."
    return f"Primarily using your own performance data ({tenant_page_count} pages), validated against {cohort_tenant_count}+ similar businesses."
