"""E-E-A-T Content Scorer — 80-criteria rubric across 8 dimensions.

Based on aaron-he-zhu/seo-geo-claude-skills CORE-EEAT benchmark.
Scores content on Experience, Expertise, Authority, Trust +
Contextual Clarity, Organization, Referenceability, Exclusivity.

Usage:
    from ai_visibility.eeat_scorer import score_eeat, EEAT_CRITERIA

    result = score_eeat(html_content, content_type="blog")
    print(f"Total: {result['total']}/100  GEO: {result['geo_score']}  SEO: {result['seo_score']}")
"""

from __future__ import annotations

import re
import logging

log = logging.getLogger(__name__)


# =====================================================================
# 80 Criteria (10 per dimension, 8 dimensions)
# Each criterion: name, description, detection patterns
# Score: Pass=10, Partial=5, Fail=0
# =====================================================================

CORE_CRITERIA = {
    "C": {  # Contextual Clarity
        "name": "Contextual Clarity",
        "items": {
            "C01": {"name": "Intent Alignment", "check": "title promise matches content delivery", "patterns": []},
            "C02": {"name": "Direct Answer", "check": "core answer in first 150 words", "patterns": [r"(?:is|are|means|refers to|defined as)"]},
            "C03": {"name": "Query Coverage", "check": "covers >=3 query variants", "patterns": []},
            "C04": {"name": "Definition First", "check": "key terms defined on first use", "patterns": [r"\b\w+\s+is\s+(?:a|an|the)\s"]},
            "C05": {"name": "Topic Scope", "check": "states what is and isn't covered", "patterns": [r"(?:this (?:article|guide|post) (?:covers|explains|focuses))", r"(?:we (?:won't|will not) cover)"]},
            "C06": {"name": "Audience Targeting", "check": "states target audience", "patterns": [r"(?:this (?:is for|article is for)|(?:if you're a|whether you're))"]},
            "C07": {"name": "Semantic Coherence", "check": "logical flow between paragraphs", "patterns": []},
            "C08": {"name": "Use Case Mapping", "check": "decision framework: when to choose A vs B", "patterns": [r"(?:when to|if you need|choose .+ when|vs\.?|versus)"]},
            "C09": {"name": "FAQ Coverage", "check": "structured FAQ section", "patterns": [r"(?:FAQ|frequently asked|common questions)", r"(?:FAQPage|Question|Answer)"]},
            "C10": {"name": "Semantic Closure", "check": "conclusion answers opening + next steps", "patterns": [r"(?:in (?:conclusion|summary)|to (?:summarize|wrap up)|next steps|key takeaways)"]},
        },
    },
    "O": {  # Organization
        "name": "Organization",
        "items": {
            "O01": {"name": "Heading Hierarchy", "check": "H1>H2>H3, no level skipping", "patterns": [r"<h[1-6]"]},
            "O02": {"name": "Summary Box", "check": "TL;DR or Key Takeaways section", "patterns": [r"(?:TL;DR|key takeaways|summary|at a glance|quick summary)"]},
            "O03": {"name": "Data Tables", "check": "comparisons in tables", "patterns": [r"<table"]},
            "O04": {"name": "List Formatting", "check": "parallel items use lists", "patterns": [r"<(?:ul|ol)"]},
            "O05": {"name": "Schema Markup", "check": "appropriate JSON-LD", "patterns": [r"application/ld\+json", r"schema\.org"]},
            "O06": {"name": "Section Chunking", "check": "single topic per section, 3-5 sentence paragraphs", "patterns": []},
            "O07": {"name": "Visual Hierarchy", "check": "key concepts bolded/highlighted", "patterns": [r"<(?:strong|b|mark)"]},
            "O08": {"name": "Anchor Navigation", "check": "table of contents with jump links", "patterns": [r"(?:table of contents|#\w+)", r"href=\"#"]},
            "O09": {"name": "Information Density", "check": "no filler, consistent terminology", "patterns": []},
            "O10": {"name": "Multimedia Structure", "check": "images/videos with captions", "patterns": [r"<(?:figure|figcaption|video|img[^>]+alt=)"]},
        },
    },
    "R": {  # Referenceability
        "name": "Referenceability",
        "items": {
            "R01": {"name": "Data Precision", "check": ">=5 precise numbers with units", "patterns": [r"\d+(?:\.\d+)?%", r"\$[\d,]+", r"\d+\s*(?:ms|seconds|minutes|hours|days|months|years)"]},
            "R02": {"name": "Citation Density", "check": ">=1 external citation per 500 words", "patterns": [r"(?:according to|per|cited by|source:|reference:)"]},
            "R03": {"name": "Source Hierarchy", "check": "primary sources first, >=3 tier 1-2 sources", "patterns": [r"(?:Gartner|Forrester|McKinsey|Harvard|Stanford|MIT|Google|Microsoft)"]},
            "R04": {"name": "Evidence-Claim Mapping", "check": "every claim backed by evidence", "patterns": []},
            "R05": {"name": "Methodology Transparency", "check": "sample size, steps documented", "patterns": [r"(?:methodology|sample size|we (?:tested|analyzed|surveyed)\s+\d+)"]},
            "R06": {"name": "Timestamp & Versioning", "check": "last updated <1 year", "patterns": [r"(?:updated|last modified|published)\s*:?\s*(?:20[2-9]\d)"]},
            "R07": {"name": "Entity Precision", "check": "full names, no 'a company'", "patterns": [r"[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+"]},
            "R08": {"name": "Internal Link Graph", "check": "descriptive anchor texts in topic clusters", "patterns": [r"<a\s+href"]},
            "R09": {"name": "HTML Semantics", "check": "semantic tags for articles, figures, time", "patterns": [r"<(?:article|figure|time|section|aside|nav)"]},
            "R10": {"name": "Content Consistency", "check": "data self-consistent, no broken links", "patterns": []},
        },
    },
    "E": {  # Exclusivity
        "name": "Exclusivity",
        "items": {
            "E01": {"name": "Original Data", "check": "first-party surveys/experiments", "patterns": [r"(?:our (?:research|study|data|analysis|survey|findings))"]},
            "E02": {"name": "Novel Framework", "check": "named, citable original framework", "patterns": [r"(?:we (?:call|named|developed|created)\s+(?:this|the|our))"]},
            "E03": {"name": "Primary Research", "check": "original experiments with process", "patterns": [r"(?:we (?:tested|conducted|ran|performed))"]},
            "E04": {"name": "Contrarian View", "check": "challenges consensus with evidence", "patterns": [r"(?:contrary to|unlike (?:popular|common)|most people (?:think|believe))"]},
            "E05": {"name": "Proprietary Visuals", "check": ">=2 original infographics/charts", "patterns": [r"<(?:svg|canvas)", r"(?:infographic|chart|diagram|illustration)"]},
            "E06": {"name": "Gap Filling", "check": "covers questions competitors don't", "patterns": []},
            "E07": {"name": "Practical Tools", "check": "downloadable templates/calculators", "patterns": [r"(?:download|template|calculator|checklist|worksheet)"]},
            "E08": {"name": "Depth Advantage", "check": "deeper than competing content", "patterns": []},
            "E09": {"name": "Synthesis Value", "check": "cross-domain knowledge combination", "patterns": []},
            "E10": {"name": "Forward Insights", "check": "data-backed predictions", "patterns": [r"(?:predict|forecast|trend|by 20[2-9]\d|in the (?:coming|next))"]},
        },
    },
}

EEAT_CRITERIA = {
    "Exp": {  # Experience
        "name": "Experience",
        "items": {
            "Exp01": {"name": "First-Person Narrative", "check": "contains 'I tested' or 'We found'", "patterns": [r"(?:I (?:tested|tried|used|found|discovered)|we (?:found|tested|tried))"]},
            "Exp02": {"name": "Sensory Details", "check": ">=10 sensory words", "patterns": [r"(?:smooth|heavy|bright|warm|cold|loud|soft|sharp|rough|crisp)"]},
            "Exp03": {"name": "Process Documentation", "check": "step-by-step with timeline", "patterns": [r"(?:step \d+|first,|second,|third,|finally,|after \d+)"]},
            "Exp04": {"name": "Tangible Proof", "check": ">=2 original photos/screenshots", "patterns": [r"(?:screenshot|photo|image)[^>]*(?:original|our|my)"]},
            "Exp05": {"name": "Usage Duration", "check": "states 'after X months of use'", "patterns": [r"(?:after \d+ (?:months?|years?|weeks?) of (?:use|using|testing))"]},
            "Exp06": {"name": "Problems Encountered", "check": ">=2 real problems + solutions", "patterns": [r"(?:problem|issue|challenge|difficulty|downside|drawback)"]},
            "Exp07": {"name": "Before/After Comparison", "check": "shows change or improvement", "patterns": [r"(?:before|after|improvement|increased|decreased|changed)"]},
            "Exp08": {"name": "Quantified Metrics", "check": "measurable experience data", "patterns": [r"\d+(?:\.\d+)?%?\s*(?:improvement|increase|decrease|faster|slower|better)"]},
            "Exp09": {"name": "Repeated Testing", "check": "multiple tests or long-term tracking", "patterns": [r"(?:multiple (?:tests|trials)|over (?:time|months|weeks)|long.?term)"]},
            "Exp10": {"name": "Limitations Acknowledged", "check": "states testing limitations", "patterns": [r"(?:limitation|caveat|only tested|didn't test|note that)"]},
        },
    },
    "Ept": {  # Expertise
        "name": "Expertise",
        "items": {
            "Ept01": {"name": "Author Identity", "check": "byline + bio (>30 words)", "patterns": [r"(?:written by|author|byline)", r"(?:bio|about the author)"]},
            "Ept02": {"name": "Credentials Display", "check": "degrees, certs, years experience", "patterns": [r"(?:certified|licensed|degree|Ph\.?D|MBA|\d+ years (?:of )?experience)"]},
            "Ept03": {"name": "Professional Vocabulary", "check": "accurate industry jargon", "patterns": []},
            "Ept04": {"name": "Technical Depth", "check": "actionable parameters and thresholds", "patterns": [r"\d+(?:\.\d+)?\s*(?:px|em|rem|ms|fps|dpi|Hz|kHz|MHz|GHz|MB|GB|TB)"]},
            "Ept05": {"name": "Methodology Rigor", "check": "reproducible analysis method", "patterns": [r"(?:methodology|approach|method|we measured|we calculated)"]},
            "Ept06": {"name": "Edge Case Awareness", "check": ">=2 exceptions discussed", "patterns": [r"(?:exception|edge case|doesn't apply|caveat|however|unless)"]},
            "Ept07": {"name": "Historical Context", "check": "field evolution knowledge", "patterns": [r"(?:historically|in the past|originally|evolved|used to be)"]},
            "Ept08": {"name": "Reasoning Transparency", "check": "'We chose A over B because...'", "patterns": [r"(?:we chose|we (?:decided|opted|selected)|because|reason (?:is|was))"]},
            "Ept09": {"name": "Cross-domain Integration", "check": "connects knowledge across fields", "patterns": []},
            "Ept10": {"name": "Editorial Process", "check": "'Reviewed by' or 'Fact-checked by'", "patterns": [r"(?:reviewed by|fact.?checked|edited by|verified by)"]},
        },
    },
    "A": {  # Authority
        "name": "Authority",
        "items": {
            "A01": {"name": "Backlink Profile", "check": "cited by authoritative sites", "patterns": []},
            "A02": {"name": "Media Mentions", "check": "'Featured in' with media logos", "patterns": [r"(?:featured in|as seen (?:in|on)|press|media)"]},
            "A03": {"name": "Industry Awards", "check": "relevant awards or recognition", "patterns": [r"(?:award|winner|recognized|nominated|best of)"]},
            "A04": {"name": "Publishing Record", "check": "talks, publications, patents", "patterns": [r"(?:published|conference|patent|paper|presentation|spoke at)"]},
            "A05": {"name": "Brand Recognition", "check": "brand has search volume", "patterns": []},
            "A06": {"name": "Social Proof", "check": "authentic testimonials", "patterns": [r"(?:testimonial|review|customer said|client feedback|\d+\s*(?:stars?|rating))"]},
            "A07": {"name": "Knowledge Graph Presence", "check": "Wikipedia or Knowledge Panel", "patterns": [r"(?:wikipedia|knowledge (?:panel|graph))"]},
            "A08": {"name": "Entity Consistency", "check": "brand info consistent across web", "patterns": []},
            "A09": {"name": "Partnership Signals", "check": "partnerships with authoritative orgs", "patterns": [r"(?:partner|partnership|certified (?:by|partner)|authorized)"]},
            "A10": {"name": "Community Standing", "check": "active in professional communities", "patterns": [r"(?:community|forum|contributor|member|board|advisory)"]},
        },
    },
    "T": {  # Trust
        "name": "Trust",
        "items": {
            "T01": {"name": "Legal Compliance", "check": "Privacy Policy + Terms present", "patterns": [r"(?:privacy policy|terms of service|terms and conditions)"]},
            "T02": {"name": "Contact Transparency", "check": "address or >=2 contact methods", "patterns": [r"(?:contact|address|phone|email|call us)"]},
            "T03": {"name": "Security Standards", "check": "HTTPS, no warnings", "patterns": [r"https://"]},
            "T04": {"name": "Disclosure Statements", "check": "affiliate links disclosed (VETO)", "patterns": [r"(?:affiliate|sponsored|disclosure|paid partnership)", r"(?:we may (?:earn|receive))"]},
            "T05": {"name": "Editorial Policy", "check": "content standards published", "patterns": [r"(?:editorial (?:policy|guidelines|standards)|content (?:policy|guidelines))"]},
            "T06": {"name": "Correction Policy", "check": "corrections page or changelog", "patterns": [r"(?:correction|errata|changelog|updated|revision)"]},
            "T07": {"name": "Ad Experience", "check": "ads <30%, no intrusive popups", "patterns": []},
            "T08": {"name": "Risk Disclaimers", "check": "YMYL disclaimers present", "patterns": [r"(?:disclaimer|not (?:medical|financial|legal) advice|consult (?:a|your))"]},
            "T09": {"name": "Review Authenticity", "check": "reviews show authenticity signals", "patterns": [r"(?:verified (?:purchase|buyer|customer)|real (?:customer|user))"]},
            "T10": {"name": "Customer Support", "check": "return policy, complaint channels", "patterns": [r"(?:return policy|refund|complaint|support|help (?:center|desk))"]},
        },
    },
}

# Content-type weights for each dimension
CONTENT_TYPE_WEIGHTS = {
    "blog": {"C": 0.25, "O": 0.10, "R": 0.10, "E": 0.20, "Exp": 0.10, "Ept": 0.10, "A": 0.05, "T": 0.10},
    "how-to": {"C": 0.20, "O": 0.20, "R": 0.10, "E": 0.05, "Exp": 0.05, "Ept": 0.20, "A": 0.05, "T": 0.15},
    "comparison": {"C": 0.10, "O": 0.20, "R": 0.25, "E": 0.10, "Exp": 0.05, "Ept": 0.15, "A": 0.05, "T": 0.10},
    "landing": {"C": 0.20, "O": 0.10, "R": 0.05, "E": 0.05, "Exp": 0.05, "Ept": 0.05, "A": 0.25, "T": 0.25},
    "faq": {"C": 0.25, "O": 0.25, "R": 0.15, "E": 0.05, "Exp": 0.05, "Ept": 0.10, "A": 0.05, "T": 0.10},
    "service": {"C": 0.15, "O": 0.15, "R": 0.10, "E": 0.10, "Exp": 0.10, "Ept": 0.10, "A": 0.15, "T": 0.15},
}

# Veto criteria — if these fail, cap total at "Low" (max 59)
VETO_ITEMS = ["T04", "C01", "R10"]


def _check_criterion(text: str, html: str, criterion: dict) -> int:
    """Check a single criterion. Returns 10 (pass), 5 (partial), or 0 (fail)."""
    patterns = criterion.get("patterns", [])
    if not patterns:
        return 5  # Can't auto-check — default to partial

    combined = text + " " + html
    matches = 0
    for pattern in patterns:
        if re.search(pattern, combined, re.IGNORECASE):
            matches += 1

    if matches >= len(patterns):
        return 10  # All patterns found = pass
    elif matches > 0:
        return 5   # Some patterns found = partial
    return 0       # No patterns = fail


def score_eeat(html: str, content_type: str = "blog") -> dict:
    """Score content on the 80-criteria EEAT rubric.

    Args:
        html: Full HTML content of the page
        content_type: One of: blog, how-to, comparison, landing, faq, service

    Returns:
        Dict with dimension scores, GEO score, SEO score, total, grade, and per-item details
    """
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
    except ImportError:
        text = re.sub(r"<[^>]+>", " ", html)

    all_criteria = {**CORE_CRITERIA, **EEAT_CRITERIA}
    dimension_scores = {}
    item_details = {}
    veto_triggered = False

    for dim_key, dim in all_criteria.items():
        dim_total = 0
        for item_key, item in dim["items"].items():
            score = _check_criterion(text, html, item)
            dim_total += score
            item_details[item_key] = {
                "name": item["name"],
                "score": score,
                "status": "pass" if score == 10 else "partial" if score == 5 else "fail",
                "dimension": dim_key,
            }
            if item_key in VETO_ITEMS and score == 0:
                veto_triggered = True

        dimension_scores[dim_key] = dim_total  # 0-100

    # GEO Score (CORE dimensions)
    geo_dims = ["C", "O", "R", "E"]
    geo_score = sum(dimension_scores.get(d, 0) for d in geo_dims) / len(geo_dims)

    # SEO Score (EEAT dimensions)
    seo_dims = ["Exp", "Ept", "A", "T"]
    seo_score = sum(dimension_scores.get(d, 0) for d in seo_dims) / len(seo_dims)

    # Total (simple average)
    total = (geo_score + seo_score) / 2

    # Weighted total (by content type)
    weights = CONTENT_TYPE_WEIGHTS.get(content_type, CONTENT_TYPE_WEIGHTS["blog"])
    weighted = sum(dimension_scores.get(d, 0) * w for d, w in weights.items())

    # Apply veto cap
    if veto_triggered:
        total = min(total, 59)
        weighted = min(weighted, 59)

    # Grade
    if total >= 90: grade = "Excellent"
    elif total >= 75: grade = "Good"
    elif total >= 60: grade = "Medium"
    elif total >= 40: grade = "Low"
    else: grade = "Poor"

    # Find weakest dimensions
    sorted_dims = sorted(dimension_scores.items(), key=lambda x: x[1])
    weakest = [{"dimension": all_criteria[d]["name"], "score": s} for d, s in sorted_dims[:3]]

    # Failed items
    failed = [{"id": k, **v} for k, v in item_details.items() if v["score"] == 0]

    return {
        "total": round(total, 1),
        "weighted": round(weighted, 1),
        "grade": grade,
        "geo_score": round(geo_score, 1),
        "seo_score": round(seo_score, 1),
        "dimensions": dimension_scores,
        "content_type": content_type,
        "veto_triggered": veto_triggered,
        "weakest_dimensions": weakest,
        "failed_items": failed[:10],
        "items_checked": len(item_details),
    }
