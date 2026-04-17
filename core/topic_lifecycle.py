import logging, sqlite3
from typing import Dict, List, Optional

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

LIFECYCLE_STRATEGIES = {
    "emerging": {"publish_priority": "high", "content_depth": "moderate", "differentiation": "low", "note": "Land early, earn authority"},
    "growing": {"publish_priority": "high", "content_depth": "standard", "differentiation": "moderate", "note": "Standard pipeline"},
    "mature": {"publish_priority": "medium", "content_depth": "comprehensive", "differentiation": "high", "note": "Long-form, unique angle required"},
    "declining": {"publish_priority": "low", "content_depth": "standard", "differentiation": "low", "note": "Consolidate existing, avoid new investment"},
}

def classify_lifecycle(keyword: str, volume_history: Optional[List[int]] = None, serp_churn_rate: Optional[float] = None, competitor_coverage: Optional[int] = None) -> str:
    score = 0  # higher = more mature

    if volume_history and len(volume_history) >= 3:
        recent = volume_history[-3:]
        older = volume_history[:-3]
        if older:
            trend = (sum(recent) / len(recent)) - (sum(older) / len(older))
            if trend > 500:
                score -= 2  # emerging/growing
            elif trend < -200:
                score += 3  # declining
            else:
                score += 1  # mature

    if serp_churn_rate is not None:
        if serp_churn_rate > 0.4:
            score -= 1  # high churn = emerging
        elif serp_churn_rate < 0.1:
            score += 1  # low churn = mature

    if competitor_coverage is not None:
        if competitor_coverage < 3:
            score -= 2  # few competitors = emerging
        elif competitor_coverage > 10:
            score += 2  # many competitors = mature

    kw_lower = keyword.lower()
    emerging_signals = ["2024", "2025", "2026", "new", "latest", "ai-powered", "smart"]
    if any(s in kw_lower for s in emerging_signals):
        score -= 1

    if score <= -2:
        return "emerging"
    elif score <= 0:
        return "growing"
    elif score <= 2:
        return "mature"
    else:
        return "declining"

def get_strategy_for_lifecycle(state: str) -> Dict:
    return LIFECYCLE_STRATEGIES.get(state, LIFECYCLE_STRATEGIES["growing"])

def batch_classify(business_id: str) -> List[Dict]:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT keyword, MIN(position) as best_pos, COUNT(*) as checks
        FROM ranking_history WHERE business_id=? GROUP BY keyword
    """, [business_id]).fetchall()
    conn.close()

    results = []
    for keyword, best_pos, checks in rows:
        competitor_proxy = max(0, 10 - (best_pos or 10)) if best_pos else 5
        state = classify_lifecycle(keyword, competitor_coverage=competitor_proxy)
        strategy = get_strategy_for_lifecycle(state)
        results.append({"keyword": keyword, "lifecycle": state, "best_position": best_pos, **strategy})

    log.info("topic_lifecycle.batch  biz=%s  classified=%d", business_id, len(results))
    return results
