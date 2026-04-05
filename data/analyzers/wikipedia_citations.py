"""Wikipedia Citation Finder — finds [citation needed] opportunities for link building.

Based on searchsolved/search-solved-public-seo (MIT license).
Finds Wikipedia articles with [citation needed] tags related to your niche,
giving you legitimate opportunities to add citations (and backlinks).

Usage:
    from data.analyzers.wikipedia_citations import find_citation_opportunities

    opps = find_citation_opportunities("permanent lighting")
    for opp in opps:
        print(f"{opp['article_url']}: {opp['sentence']}")
"""

from __future__ import annotations

import re
import logging

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

WIKI_SEARCH_API = "https://en.wikipedia.org/w/api.php"
WIKI_HEADERS = {"User-Agent": "SEOEngine/1.0 (research tool; contact@blendbrightlights.com)"}


def find_citation_opportunities(
    keyword: str,
    max_articles: int = 10,
    max_citations_per_article: int = 5,
) -> list[dict]:
    """Find Wikipedia [citation needed] tags related to a keyword.

    Args:
        keyword: Topic to search for
        max_articles: Maximum Wikipedia articles to check
        max_citations_per_article: Max citation opportunities per article

    Returns:
        List of dicts: article_title, article_url, sentence, section_url
    """
    # Search Wikipedia for relevant articles
    params = {
        "action": "query",
        "list": "search",
        "srsearch": keyword,
        "srlimit": max_articles,
        "format": "json",
    }

    try:
        resp = requests.get(WIKI_SEARCH_API, params=params, headers=WIKI_HEADERS, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("query", {}).get("search", [])
    except Exception as e:
        log.error("wikipedia.search_fail  keyword=%s  err=%s", keyword, e)
        return []

    opportunities = []

    for result in results:
        title = result["title"]
        article_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"

        try:
            page_resp = requests.get(article_url, headers=WIKI_HEADERS, timeout=10)
            soup = BeautifulSoup(page_resp.text, "html.parser")

            # Find [citation needed] tags
            citations = soup.find_all(class_="noprint Inline-Template Template-Fact")

            for citation in citations[:max_citations_per_article]:
                # Get parent paragraph
                parent = citation.find_parent("p")
                if not parent:
                    continue

                # Extract the sentence containing the citation needed tag
                full_text = parent.get_text(strip=True)
                sentences = re.split(r'(?<=[.!?]) +', full_text)

                # Find which sentence contains "[citation needed]"
                target_sentence = ""
                for sentence in sentences:
                    if "citation needed" in sentence.lower() or len(sentence) > 20:
                        target_sentence = sentence.replace("[citation needed]", "").strip()
                        break

                if not target_sentence:
                    target_sentence = full_text[:200]

                opportunities.append({
                    "article_title": title,
                    "article_url": article_url,
                    "sentence": target_sentence[:300],
                    "full_paragraph": full_text[:500],
                    "relevance": _score_relevance(keyword, target_sentence),
                })

        except Exception as e:
            log.debug("wikipedia.page_fail  title=%s  err=%s", title, e)

    # Sort by relevance
    opportunities.sort(key=lambda x: x["relevance"], reverse=True)
    log.info("wikipedia.found  keyword=%s  opportunities=%d", keyword, len(opportunities))
    return opportunities


def _score_relevance(keyword: str, text: str) -> float:
    """Score how relevant a citation opportunity is to the keyword."""
    keyword_words = set(keyword.lower().split())
    text_words = set(text.lower().split())
    overlap = keyword_words & text_words
    if not keyword_words:
        return 0.0
    return len(overlap) / len(keyword_words)
