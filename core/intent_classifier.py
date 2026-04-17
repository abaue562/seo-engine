import re, logging
from typing import Dict, Optional, List

log = logging.getLogger(__name__)

COMMERCIAL_SIGNALS = [r'\bbest\b', r'\btop\b', r'\breview', r'\bbuy\b', r'\bprice', r'\bcost\b', r'\bvs\b', r'\bcompare\b', r'\bcheap\b']
INFORMATIONAL_SIGNALS = [r'\bhow\b', r'\bwhat\b', r'\bwhy\b', r'\bwhen\b', r'\bguide\b', r'\btips\b', r'\blearn\b', r'\bexplain\b']
TRANSACTIONAL_SIGNALS = [r'\bnear me\b', r'\bfor sale\b', r'\border\b', r'\bhire\b', r'\bservice\b', r'\binstall\b', r'\brepair\b']
NAVIGATIONAL_SIGNALS = [r'\blogin\b', r'\bsign in\b', r'\bwebsite\b', r'\bofficial\b']

def _count_signals(keyword: str, patterns: List[str]) -> int:
    kw = keyword.lower()
    return sum(1 for p in patterns if re.search(p, kw))

def classify_intent(keyword: str, serp_snapshot: Optional[dict] = None) -> Dict[str, float]:
    scores = {
        "informational": _count_signals(keyword, INFORMATIONAL_SIGNALS) * 2.0,
        "commercial": _count_signals(keyword, COMMERCIAL_SIGNALS) * 2.0,
        "transactional": _count_signals(keyword, TRANSACTIONAL_SIGNALS) * 2.0,
        "navigational": _count_signals(keyword, NAVIGATIONAL_SIGNALS) * 2.0,
    }

    if serp_snapshot:
        paa_count = serp_snapshot.get("paa_count", 0)
        ad_count = serp_snapshot.get("ad_count", 0)
        has_local_pack = serp_snapshot.get("has_local_pack", False)
        scores["informational"] += paa_count * 0.5
        scores["commercial"] += ad_count * 0.8
        if has_local_pack:
            scores["transactional"] += 1.5

    total = sum(scores.values()) or 1.0
    probs = {k: round(v / total, 3) for k, v in scores.items()}

    sorted_by_prob = sorted(probs.items(), key=lambda x: x[1], reverse=True)
    probs["mixed_primary"] = sorted_by_prob[0][0]
    probs["mixed_secondary"] = sorted_by_prob[1][0] if sorted_by_prob[1][1] > 0.2 else None

    log.debug("intent_classifier  keyword=%s  primary=%s", keyword, probs["mixed_primary"])
    return probs

def blend_templates(intent_probs: Dict[str, float], templates: Dict[str, str]) -> str:
    primary = intent_probs.get("mixed_primary", "informational")
    secondary = intent_probs.get("mixed_secondary")
    base = templates.get(primary, templates.get("informational", ""))
    if secondary and secondary != primary and intent_probs.get(secondary, 0) > 0.25:
        addon = templates.get(secondary, "")
        if addon and addon not in base:
            base = base + "\n\n" + addon
    return base
