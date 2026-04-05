"""Citability Scorer — scores content blocks on how likely AI models are to cite them.

Based on research from geo-seo-claude (MIT license).
5 dimensions, 0-100 composite score.

Usage:
    from ai_visibility.citability import score_passage, analyze_page, CitabilityGrade

    result = score_passage("Your content block here...")
    print(f"Score: {result['total']}/100  Grade: {result['grade']}")

    page_result = analyze_page("<html>...</html>")
    print(f"Average: {page_result['average_score']}  Grade: {page_result['grade']}")
"""

from __future__ import annotations

import re
import logging
from enum import Enum

log = logging.getLogger(__name__)


class CitabilityGrade(str, Enum):
    A = "A"  # >= 80: Highly Citable
    B = "B"  # >= 65: Good Citability
    C = "C"  # >= 50: Moderate
    D = "D"  # >= 35: Low
    F = "F"  # < 35: Poor


def grade_from_score(score: float) -> CitabilityGrade:
    if score >= 80:
        return CitabilityGrade.A
    if score >= 65:
        return CitabilityGrade.B
    if score >= 50:
        return CitabilityGrade.C
    if score >= 35:
        return CitabilityGrade.D
    return CitabilityGrade.F


GRADE_LABELS = {
    CitabilityGrade.A: "Highly Citable",
    CitabilityGrade.B: "Good Citability",
    CitabilityGrade.C: "Moderate Citability",
    CitabilityGrade.D: "Low Citability",
    CitabilityGrade.F: "Poor Citability",
}


# =====================================================================
# Dimension 1: Answer Block Quality (max 30 points)
# =====================================================================

DEFINITION_PATTERNS = [
    re.compile(r"\b\w+\s+is\s+(?:a|an|the)\s", re.IGNORECASE),
    re.compile(r"\b\w+\s+refers?\s+to\s", re.IGNORECASE),
    re.compile(r"\b\w+\s+means?\s", re.IGNORECASE),
    re.compile(r"\b\w+\s+(?:can be |are )?defined\s+as\s", re.IGNORECASE),
    re.compile(r"\bin\s+(?:simple|other)\s+(?:terms|words)\s*,", re.IGNORECASE),
]

EARLY_ANSWER_PATTERNS = [
    re.compile(r"\b(?:is|are|was|were|means?|refers?)\b", re.IGNORECASE),
    re.compile(r"\d+%"),
    re.compile(r"\$[\d,]+"),
    re.compile(r"\d+\s+(?:million|billion|thousand)", re.IGNORECASE),
]

QUOTABLE_CLAIM = re.compile(
    r"(?:according to|research shows|studies?\s+(?:show|indicate|suggest|found)|data\s+(?:shows|indicates|suggests))",
    re.IGNORECASE,
)


def _score_answer_quality(text: str, heading: str = "") -> int:
    score = 0

    # Definition patterns (+15, one-time)
    for pat in DEFINITION_PATTERNS:
        if pat.search(text):
            score += 15
            break

    # Early answer: check first 60 words
    words = text.split()
    first_60 = " ".join(words[:60])
    for pat in EARLY_ANSWER_PATTERNS:
        if pat.search(first_60):
            score += 15
            break

    # Question-based heading (+10)
    if heading.strip().endswith("?"):
        score += 10

    # Clear sentence structure (+0-10)
    sentences = re.split(r"[.!?]+", text)
    sentences = [s.strip() for s in sentences if s.strip()]
    if sentences:
        good_len = sum(1 for s in sentences if 5 <= len(s.split()) <= 25)
        ratio = good_len / len(sentences)
        score += int(ratio * 10)

    # Quotable claim (+10)
    if QUOTABLE_CLAIM.search(text):
        score += 10

    return min(score, 30)


# =====================================================================
# Dimension 2: Self-Containment (max 25 points)
# =====================================================================

PRONOUNS = re.compile(r"\b(?:it|they|them|their|this|that|these|those|he|she|his|her)\b", re.IGNORECASE)
NAMED_ENTITIES = re.compile(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b")


def _score_self_containment(text: str) -> int:
    score = 0
    wc = len(text.split())

    # Word count scoring
    if 134 <= wc <= 167:
        score += 10
    elif 100 <= wc <= 200:
        score += 7
    elif 80 <= wc <= 250:
        score += 4
    elif wc < 30 or wc > 400:
        score += 0
    else:
        score += 2

    # Pronoun density
    pronoun_count = len(PRONOUNS.findall(text))
    ratio = pronoun_count / max(wc, 1)
    if ratio < 0.02:
        score += 8
    elif ratio < 0.04:
        score += 5
    elif ratio < 0.06:
        score += 3

    # Named entities
    entities = NAMED_ENTITIES.findall(text)
    # Filter out common words that start sentences
    entity_count = len([e for e in entities if len(e) > 2])
    if entity_count >= 3:
        score += 7
    elif entity_count >= 1:
        score += 4

    return min(score, 25)


# =====================================================================
# Dimension 3: Structural Readability (max 20 points)
# =====================================================================

TRANSITIONS = re.compile(
    r"(?:first|second|third|finally|additionally|moreover|furthermore)",
    re.IGNORECASE,
)
NUMBERED_ITEMS = re.compile(r"(?:\d+[\.\)]\s|\b(?:step|tip|point)\s+\d+)", re.IGNORECASE)


def _score_readability(text: str) -> int:
    score = 0

    # Average sentence length
    sentences = re.split(r"[.!?]+", text)
    sentences = [s.strip() for s in sentences if s.strip()]
    if sentences:
        avg_len = sum(len(s.split()) for s in sentences) / len(sentences)
        if 10 <= avg_len <= 20:
            score += 8
        elif 8 <= avg_len <= 25:
            score += 5
        else:
            score += 2

    # Transitional phrases (+4)
    if TRANSITIONS.search(text):
        score += 4

    # Numbered items (+4)
    if NUMBERED_ITEMS.search(text):
        score += 4

    # Paragraph breaks (+4)
    if "\n" in text:
        score += 4

    return min(score, 20)


# =====================================================================
# Dimension 4: Statistical Density (max 15 points)
# =====================================================================

PERCENTAGES = re.compile(r"\d+(?:\.\d+)?%")
DOLLAR_AMOUNTS = re.compile(r"\$[\d,]+(?:\.\d+)?(?:\s*(?:million|billion|M|B|K))?")
NUMBERS_WITH_CONTEXT = re.compile(
    r"\b\d+(?:,\d{3})*(?:\.\d+)?\s+(?:users|customers|pages|sites|companies|businesses|people|percent|times|x\b)",
    re.IGNORECASE,
)
YEAR_REFS = re.compile(r"\b20(?:2[3-6]|1\d)\b")
NAMED_SOURCES = [
    re.compile(r"(?:according to|per|from|by)\s+[A-Z]"),
    re.compile(r"(?:Gartner|Forrester|McKinsey|Harvard|Stanford|MIT|Google|Microsoft|OpenAI|Anthropic)"),
    re.compile(r"\([A-Z][a-z]+(?:\s+\d{4})?\)"),
]


def _score_statistical_density(text: str) -> int:
    score = 0

    # Percentages (+3 each, max +6)
    pct_count = len(PERCENTAGES.findall(text))
    score += min(pct_count * 3, 6)

    # Dollar amounts (+3 each, max +5)
    dollar_count = len(DOLLAR_AMOUNTS.findall(text))
    score += min(dollar_count * 3, 5)

    # Numbers with context (+2 each, max +4)
    num_count = len(NUMBERS_WITH_CONTEXT.findall(text))
    score += min(num_count * 2, 4)

    # Year references (+2)
    if YEAR_REFS.search(text):
        score += 2

    # Named sources (+2 each)
    for pat in NAMED_SOURCES:
        if pat.search(text):
            score += 2

    return min(score, 15)


# =====================================================================
# Dimension 5: Uniqueness Signals (max 10 points)
# =====================================================================

ORIGINAL_DATA = re.compile(
    r"(?:our\s+(?:research|study|data|analysis|survey|findings)|we\s+(?:found|discovered|analyzed|surveyed|measured))",
    re.IGNORECASE,
)
CASE_STUDY = re.compile(
    r"(?:case study|for example|for instance|in practice|real-world|hands-on)",
    re.IGNORECASE,
)
TOOL_MENTIONS = re.compile(r"(?:using|with|via|through)\s+[A-Z][a-z]+")


def _score_uniqueness(text: str) -> int:
    score = 0

    if ORIGINAL_DATA.search(text):
        score += 5
    if CASE_STUDY.search(text):
        score += 3
    if TOOL_MENTIONS.search(text):
        score += 2

    return min(score, 10)


# =====================================================================
# Public API
# =====================================================================

def score_passage(text: str, heading: str = "") -> dict:
    """Score a single content block for AI citability (0-100).

    Returns dict with sub-scores, total, grade, and label.
    """
    scores = {
        "answer_quality": _score_answer_quality(text, heading),
        "self_containment": _score_self_containment(text),
        "readability": _score_readability(text),
        "statistical_density": _score_statistical_density(text),
        "uniqueness": _score_uniqueness(text),
    }

    total = sum(scores.values())
    grade = grade_from_score(total)

    return {
        **scores,
        "total": total,
        "grade": grade.value,
        "label": GRADE_LABELS[grade],
        "word_count": len(text.split()),
        "optimal_length": 134 <= len(text.split()) <= 167,
    }


def analyze_page(html: str) -> dict:
    """Analyze all content blocks on a page for citability.

    Strips nav/footer/header/scripts, splits at headings, scores each block.
    Returns average score, top/bottom blocks, grade distribution.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return {"error": "beautifulsoup4 not installed"}

    soup = BeautifulSoup(html, "html.parser")

    # Remove non-content elements
    for tag in soup.find_all(["script", "style", "nav", "footer", "header", "aside", "form"]):
        tag.decompose()

    # Split content at headings
    blocks = []
    current_heading = ""
    current_text = []

    for el in soup.find_all(["h1", "h2", "h3", "h4", "p", "li", "div", "span"]):
        if el.name in ("h1", "h2", "h3", "h4"):
            # Save previous block
            if current_text:
                text = " ".join(current_text).strip()
                if len(text.split()) >= 20:
                    blocks.append({"heading": current_heading, "text": text})
            current_heading = el.get_text(strip=True)
            current_text = []
        else:
            t = el.get_text(strip=True)
            if t and len(t) > 10:
                current_text.append(t)

    # Don't forget last block
    if current_text:
        text = " ".join(current_text).strip()
        if len(text.split()) >= 20:
            blocks.append({"heading": current_heading, "text": text})

    if not blocks:
        return {"error": "No content blocks found", "blocks": 0}

    # Score each block
    scored = []
    for block in blocks:
        result = score_passage(block["text"], block["heading"])
        result["heading"] = block["heading"]
        result["preview"] = block["text"][:100] + "..." if len(block["text"]) > 100 else block["text"]
        scored.append(result)

    # Sort by score
    scored.sort(key=lambda x: x["total"], reverse=True)

    # Stats
    avg = sum(b["total"] for b in scored) / len(scored)
    grade = grade_from_score(avg)

    grade_dist = {g.value: 0 for g in CitabilityGrade}
    for b in scored:
        grade_dist[b["grade"]] += 1

    optimal_count = sum(1 for b in scored if b["optimal_length"])

    return {
        "blocks_analyzed": len(scored),
        "average_score": round(avg, 1),
        "grade": grade.value,
        "label": GRADE_LABELS[grade],
        "grade_distribution": grade_dist,
        "optimal_length_blocks": optimal_count,
        "top_5": scored[:5],
        "bottom_5": scored[-5:] if len(scored) > 5 else [],
    }
