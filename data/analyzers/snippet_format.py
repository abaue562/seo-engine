"""Featured Snippet Format Analyzer — detect snippet format and generate winning content.

Google shows different snippet formats for different query types:
  - Paragraph  → "What is X?" / "How does X work?"  — 40-60 word direct answer
  - Numbered list → "How to X" / "Steps to X"        — numbered H2/H3 with short items
  - Bullet list   → "Best X for Y" / "Types of X"    — bullet points with brief descriptions
  - Table         → "X vs Y" / "Comparison of X"     — markdown/HTML table
  - Video         → "How to X" with visual need       — YouTube embed + transcript

To win a snippet you must:
  1. Match the exact format Google is showing for that query
  2. Place the answer immediately after a heading that matches the query
  3. Keep paragraph answers 40-60 words, list items 15-30 words
  4. Use schema markup (FAQPage for Q&A, HowTo for steps)

Usage:
    from data.analyzers.snippet_format import analyze_snippet_opportunity, SnippetOpportunity

    opp = analyze_snippet_opportunity("how much do permanent lights cost in kelowna")
    print(f"Format needed: {opp.format_needed}")
    print(f"Winning template: {opp.content_template}")
"""

from __future__ import annotations

import re
import logging
import time
from enum import Enum

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

GOOGLE_URL = "https://www.google.com/search"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-CA,en;q=0.9",
}


class SnippetFormat(str, Enum):
    PARAGRAPH    = "paragraph"
    NUMBERED_LIST = "numbered_list"
    BULLET_LIST  = "bullet_list"
    TABLE        = "table"
    VIDEO        = "video"
    NONE         = "none"      # No snippet currently — opportunity to be first


class SnippetOpportunity(BaseModel):
    """Analysis of a keyword's snippet opportunity."""
    keyword: str
    has_snippet: bool = False
    format_needed: SnippetFormat = SnippetFormat.NONE
    current_holder: str = ""          # Domain currently holding the snippet
    current_snippet_text: str = ""    # Existing snippet text

    # Winning content requirements
    answer_word_target: str = ""      # e.g. "40-60 words"
    heading_format: str = ""          # Exact H2 text to use
    content_template: str = ""        # Template for the winning content
    schema_type: str = ""             # FAQPage / HowTo / None

    # Query intent signals
    query_intent: str = ""            # informational / transactional / navigational
    action_word: str = ""             # how / what / why / best / vs / list

    # Position 0 difficulty
    difficulty: str = "medium"        # easy / medium / hard
    notes: str = ""


# ── Query intent detection ──────────────────────────────────────────────

def _detect_intent(keyword: str) -> tuple[str, str, SnippetFormat]:
    """Detect query intent and expected snippet format."""
    kw = keyword.lower().strip()

    how_long = re.search(r"^how (long|often|many|much|far|well)", kw)
    how_to = re.search(r"^how (to|do|does|can|should)", kw)
    what   = re.search(r"^what (is|are|does|do|causes)", kw)
    why    = re.search(r"^why ", kw)
    best   = re.search(r"^(best|top|cheapest|fastest|easiest)\b", kw)
    vs     = re.search(r"\bvs\.?\b|versus\b|compare\b|difference between\b", kw)
    cost   = re.search(r"(cost|price|how much|expensive|cheap)", kw)
    steps  = re.search(r"(steps|process|guide|tutorial|install|setup|configure)", kw)
    list_q = re.search(r"(types of|kinds of|examples of|list of|ideas for)", kw)
    who    = re.search(r"^who\b", kw)

    if how_to and steps:
        return "informational", "how_to_steps", SnippetFormat.NUMBERED_LIST
    elif how_long and cost:
        return "informational", "cost_explanation", SnippetFormat.PARAGRAPH
    elif how_long:
        # "How long/often/many" → paragraph answer, not a how-to list
        return "informational", "how_long", SnippetFormat.PARAGRAPH
    elif how_to and cost:
        return "informational", "cost_explanation", SnippetFormat.PARAGRAPH
    elif how_to:
        return "informational", "how_to", SnippetFormat.NUMBERED_LIST
    elif what or why or who:
        return "informational", "definition", SnippetFormat.PARAGRAPH
    elif vs:
        return "informational", "comparison", SnippetFormat.TABLE
    elif best:
        return "commercial", "best_list", SnippetFormat.BULLET_LIST
    elif list_q:
        return "informational", "list", SnippetFormat.BULLET_LIST
    elif cost:
        return "informational", "cost", SnippetFormat.PARAGRAPH
    else:
        return "commercial", "general", SnippetFormat.PARAGRAPH


# ── Content templates ──────────────────────────────────────────────────

TEMPLATES = {
    SnippetFormat.PARAGRAPH: """## {heading}

{direct_answer_40_60_words}

[Continue with detailed explanation, examples, and supporting evidence below this block.]

```schema
{{
  "@type": "FAQPage",
  "mainEntity": [{{
    "@type": "Question",
    "name": "{heading}",
    "acceptedAnswer": {{"@type": "Answer", "text": "{direct_answer_40_60_words}"}}
  }}]
}}
```""",

    SnippetFormat.NUMBERED_LIST: """## {heading}

1. **{step_1}** — brief description (15-20 words max per item)
2. **{step_2}** — brief description
3. **{step_3}** — brief description
4. **{step_4}** — brief description
5. **{step_5}** — brief description

[Expand each step with detail after this block — Google only shows the list in the snippet.]

```schema
{{
  "@type": "HowTo",
  "name": "{heading}",
  "step": [
    {{"@type": "HowToStep", "name": "{step_1}", "text": "..."}},
    {{"@type": "HowToStep", "name": "{step_2}", "text": "..."}}
  ]
}}
```""",

    SnippetFormat.BULLET_LIST: """## {heading}

- **Option 1**: brief description (15-25 words)
- **Option 2**: brief description
- **Option 3**: brief description
- **Option 4**: brief description
- **Option 5**: brief description

[Detail and comparison below — Google only shows the bullet list in the snippet.]""",

    SnippetFormat.TABLE: """## {heading}

| Feature | {option_a} | {option_b} |
|---------|-----------|-----------|
| Cost    | $X        | $Y        |
| Install | Hours     | Hours     |
| Warranty| X years   | Y years   |
| App     | Yes/No    | Yes/No    |
| Annual Fee | None/Yes | None/Yes |

[Full comparison details below — Google renders the table directly in the snippet.]""",
}

HEADING_TEMPLATES = {
    "how_to_steps":       "How to {keyword_cleaned}",
    "cost_explanation":   "How much does {keyword_cleaned} cost?",
    "how_to":             "How to {keyword_cleaned}",
    "how_long":           "{keyword_cleaned}",   # use full question as heading
    "definition":         "What is {keyword_cleaned}?",
    "comparison":         "{keyword_cleaned}: comparison",
    "best_list":          "Best {keyword_cleaned}",
    "list":               "{keyword_cleaned}",
    "cost":               "How much does {keyword_cleaned} cost?",
    "general":            "{keyword_cleaned}: overview",
}

ANSWER_TARGETS = {
    SnippetFormat.PARAGRAPH:     "40-60 words — direct answer, no preamble",
    SnippetFormat.NUMBERED_LIST: "5-8 items, 15-25 words each, bold item names",
    SnippetFormat.BULLET_LIST:   "5-8 bullets, 15-25 words each, bold item names",
    SnippetFormat.TABLE:         "3-6 rows, 3-5 columns, concise cell values",
    SnippetFormat.VIDEO:         "Video embed + 50-word text description below",
}

SCHEMA_TYPES = {
    SnippetFormat.PARAGRAPH:     "FAQPage",
    SnippetFormat.NUMBERED_LIST: "HowTo",
    SnippetFormat.BULLET_LIST:   "ItemList",
    SnippetFormat.TABLE:         "Table (no specific schema — use clean HTML table)",
    SnippetFormat.VIDEO:         "VideoObject",
}


# ── SERP detection ────────────────────────────────────────────────────

def _fetch_snippet_from_serp(keyword: str, country: str = "ca") -> tuple[bool, str, str, SnippetFormat]:
    """Fetch SERP and extract existing featured snippet info.

    Returns: (has_snippet, holder_domain, snippet_text, detected_format)
    """
    time.sleep(2.0)
    params = {"q": keyword, "num": 5, "gl": country, "hl": "en"}

    try:
        resp = requests.get(GOOGLE_URL, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        log.warning("snippet.serp_fail  keyword=%s  err=%s", keyword, e)
        return False, "", "", SnippetFormat.NONE

    # Check for featured snippet block
    snippet_block = (
        soup.find("div", class_=re.compile("kp-blk|featured-snippet|xpdopen|ifM9O")) or
        soup.find("div", attrs={"data-attrid": re.compile("snippet|description")})
    )

    if not snippet_block:
        return False, "", "", SnippetFormat.NONE

    text = snippet_block.get_text(separator=" ", strip=True)[:300]

    # Detect format from DOM structure
    if snippet_block.find("table"):
        fmt = SnippetFormat.TABLE
    elif snippet_block.find("ol"):
        fmt = SnippetFormat.NUMBERED_LIST
    elif snippet_block.find("ul"):
        fmt = SnippetFormat.BULLET_LIST
    else:
        fmt = SnippetFormat.PARAGRAPH

    # Extract domain
    holder = ""
    link = snippet_block.find("a", href=True)
    if link:
        href = link.get("href", "")
        m = re.search(r"https?://(?:www\.)?([^/]+)", href)
        if m:
            holder = m.group(1)

    return True, holder, text, fmt


# ── Main interface ─────────────────────────────────────────────────────

def analyze_snippet_opportunity(
    keyword: str,
    country: str = "ca",
    check_serp: bool = True,
) -> SnippetOpportunity:
    """Analyze a keyword's featured snippet opportunity.

    Args:
        keyword: Target keyword to analyze
        country: Country code for SERP
        check_serp: Whether to fetch live SERP (False = intent-only analysis)

    Returns:
        SnippetOpportunity with format requirements and content template
    """
    intent, action_word, inferred_format = _detect_intent(keyword)
    opp = SnippetOpportunity(keyword=keyword, query_intent=intent, action_word=action_word)

    actual_format = inferred_format
    has_snippet = False
    holder = ""
    snippet_text = ""

    if check_serp:
        has_snippet, holder, snippet_text, serp_format = _fetch_snippet_from_serp(keyword, country)
        if has_snippet and serp_format != SnippetFormat.NONE:
            actual_format = serp_format  # Trust what we see over inference

    opp.has_snippet = has_snippet
    opp.format_needed = actual_format
    opp.current_holder = holder
    opp.current_snippet_text = snippet_text
    opp.answer_word_target = ANSWER_TARGETS.get(actual_format, "40-60 words")
    opp.schema_type = SCHEMA_TYPES.get(actual_format, "FAQPage")

    # Build heading — strip question-word phrases so templates don't double them
    # Apply longest-match first to avoid partial stripping
    _PREFIX_PATTERNS = [
        # "how much does/do X cost" → strip whole question phrase, keep just X
        (r"^how\s+much\s+(?:does?|do|did|will|would|should)\s+", ""),
        (r"^how\s+(?:long|often|far|well|many|much)\s+(?:does?|do|did|will|is|are)\s+", ""),
        (r"^how\s+(?:to|do|does|can|should|did)\s+", ""),
        (r"^what\s+(?:is|are|does|do|causes?)\s+", ""),
        (r"^why\s+(?:is|are|do|does)\s+", ""),
        (r"^who\s+(?:is|are)\s+", ""),
        (r"^(?:can|are|is|best)\s+", ""),
    ]
    kw_clean = keyword.strip()
    for pattern, repl in _PREFIX_PATTERNS:
        kw_clean, n = re.subn(pattern, repl, kw_clean, flags=re.IGNORECASE)
        if n:
            break  # only apply first matching strip
    kw_clean = kw_clean.strip().rstrip("?").strip()

    # Strip trailing "cost [in] <word>" when heading template ends with "cost?"
    # e.g. kw_clean="permanent lighting cost kelowna" + "How much does X cost?" → doubled
    heading_tmpl = HEADING_TEMPLATES.get(action_word, "{keyword_cleaned}")
    if heading_tmpl.endswith("cost?"):
        kw_clean = re.sub(r"\s+cost(?:\s+(?:in\s+)?\S+)?$", "", kw_clean, flags=re.IGNORECASE).strip()

    # For "how_long" and bare {keyword_cleaned} templates, use the full question title-cased
    if action_word == "how_long":
        opp.heading_format = keyword.strip().rstrip("?").strip().capitalize() + "?"
    elif "{keyword_cleaned}" == heading_tmpl:
        opp.heading_format = keyword.capitalize()
    else:
        opp.heading_format = heading_tmpl.format(keyword_cleaned=kw_clean)

    # Build content template
    tmpl = TEMPLATES.get(actual_format, TEMPLATES[SnippetFormat.PARAGRAPH])
    opp.content_template = tmpl.format(
        heading=opp.heading_format,
        keyword_cleaned=kw_clean,
        direct_answer_40_60_words=f"[Write a {opp.answer_word_target} direct answer here. No preamble. Start with the answer.]",
        step_1="First step name", step_2="Second step name", step_3="Third step name",
        step_4="Fourth step name", step_5="Fifth step name",
        option_a="Option A", option_b="Option B",
    )

    # Difficulty assessment
    if has_snippet and holder:
        # Someone holds it — harder to steal
        opp.difficulty = "hard" if snippet_text and len(snippet_text) > 100 else "medium"
        opp.notes = f"Snippet held by {holder}. Match their format exactly but with better/longer answer."
    else:
        # No snippet — first mover advantage
        opp.difficulty = "easy"
        opp.notes = "No snippet exists — first to publish in correct format will win position 0."

    log.info("snippet.analyzed  keyword=%s  has_snippet=%s  format=%s  holder=%s  difficulty=%s",
             keyword, has_snippet, actual_format.value, holder, opp.difficulty)
    return opp


def analyze_snippet_batch(
    keywords: list[str],
    country: str = "ca",
    check_serp: bool = True,
    delay: float = 3.0,
) -> list[SnippetOpportunity]:
    """Analyze multiple keywords. Applies delay between SERP fetches."""
    results = []
    for i, kw in enumerate(keywords):
        if i > 0 and check_serp:
            time.sleep(delay)
        opp = analyze_snippet_opportunity(kw, country=country, check_serp=check_serp)
        results.append(opp)

    # Sort: easy first, then medium, then hard
    order = {"easy": 0, "medium": 1, "hard": 2}
    return sorted(results, key=lambda x: order.get(x.difficulty, 1))


def snippet_to_prompt(opp: SnippetOpportunity) -> str:
    """Convert snippet opportunity to a Claude prompt for content generation."""
    return f"""Generate content to win the featured snippet for: "{opp.keyword}"

FORMAT REQUIRED: {opp.format_needed.value}
HEADING TO USE (exact): {opp.heading_format}
ANSWER TARGET: {opp.answer_word_target}
SCHEMA TYPE: {opp.schema_type}
DIFFICULTY: {opp.difficulty}

{"CURRENT HOLDER: " + opp.current_holder if opp.current_holder else "No current snippet — be first."}
{"CURRENT SNIPPET: " + opp.current_snippet_text[:200] if opp.current_snippet_text else ""}

TEMPLATE:
{opp.content_template}

Generate the actual content following this template exactly. The answer block must be placed immediately after the H2 heading with no preamble."""
