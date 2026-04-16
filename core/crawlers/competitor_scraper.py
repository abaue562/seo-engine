"""Competitor content scraper — uses AION Firecrawl + Brain for content briefs.

Replaces manual BeautifulSoup scraping with AION's JS-aware Firecrawl service.
Analyzes competitor pages and generates structured content briefs via AION Brain.

Usage:
    from core.crawlers.competitor_scraper import CompetitorScraper

    scraper = CompetitorScraper()
    brief = scraper.generate_brief(
        keyword="best link building tools",
        competitor_urls=["https://ahrefs.com/blog/link-building-tools/", ...]
    )
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class CompetitorPage:
    url: str
    title: str = ""
    word_count: int = 0
    headings: list[str] = field(default_factory=list)
    markdown: str = ""
    language: str = "en"
    error: str = ""


@dataclass
class ContentBrief:
    keyword: str
    recommended_word_count: int = 0
    recommended_title: str = ""
    suggested_h2s: list[str] = field(default_factory=list)
    suggested_h3s: list[str] = field(default_factory=list)
    faq_questions: list[str] = field(default_factory=list)
    competitor_summary: str = ""
    content_gaps: list[str] = field(default_factory=list)
    youtube_insights: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)


class CompetitorScraper:
    """Scrapes competitor pages via AION Firecrawl and generates content briefs."""

    def scrape_page(self, url: str) -> CompetitorPage:
        """Scrape a single competitor URL. Returns CompetitorPage with markdown."""
        try:
            from core.aion_bridge import aion
            meta = aion.firecrawl_scrape_meta(url)
            if "error" in meta:
                return CompetitorPage(url=url, error=meta["error"])

            md = meta.get("markdown", "")
            title = meta.get("title", "")
            word_count = len(md.split())

            # Extract headings from markdown
            headings = []
            for line in md.splitlines():
                if line.startswith("#"):
                    clean = re.sub(r"^#+\s*", "", line).strip()
                    if clean:
                        headings.append(clean)

            return CompetitorPage(
                url=url,
                title=title,
                word_count=word_count,
                headings=headings[:20],
                markdown=md[:8000],  # cap to avoid huge payloads
                language=meta.get("language", "en"),
            )
        except Exception as e:
            log.warning("competitor_scraper.scrape_fail  url=%s  err=%s", url, e)
            return CompetitorPage(url=url, error=str(e))

    def scrape_serp_competitors(
        self, keyword: str, competitor_urls: list[str], max_pages: int = 5
    ) -> list[CompetitorPage]:
        """Scrape up to max_pages competitor URLs."""
        pages = []
        for url in competitor_urls[:max_pages]:
            page = self.scrape_page(url)
            if not page.error:
                pages.append(page)
                log.info(
                    "competitor_scraper.scraped  url=%s  words=%d  headings=%d",
                    url, page.word_count, len(page.headings),
                )
        return pages

    def generate_brief(
        self,
        keyword: str,
        competitor_urls: list[str],
        max_competitors: int = 4,
        include_youtube: bool = True,
    ) -> ContentBrief:
        """Generate a full content brief for a keyword.

        1. Scrapes competitor pages via Firecrawl
        2. Searches YouTube for video insights
        3. Calls AION Brain to synthesize a structured brief
        4. Returns ContentBrief dataclass
        """
        from core.aion_bridge import aion

        brief = ContentBrief(keyword=keyword, sources=competitor_urls[:max_competitors])

        # Step 1: Scrape competitors
        pages = self.scrape_serp_competitors(keyword, competitor_urls, max_competitors)

        if not pages:
            log.warning("competitor_scraper.no_pages  keyword=%s", keyword)
            return brief

        # Step 2: Compute recommended word count (avg + 20%)
        avg_words = sum(p.word_count for p in pages) / len(pages)
        brief.recommended_word_count = int(avg_words * 1.2)

        # Collect all competitor headings
        all_headings = []
        for p in pages:
            all_headings.extend(p.headings)

        # Step 3: YouTube research
        yt_snippets: list[str] = []
        if include_youtube:
            videos = aion.youtube_search(keyword, max_results=3)
            for v in videos:
                snippet = f"[{v['views']:,} views] {v['title']} by {v['channel']}"
                yt_snippets.append(snippet)
                brief.youtube_insights.append(snippet)

        # Step 4: Generate brief via AION Brain
        competitor_context = "\n\n".join(
            f"URL: {p.url}\nTitle: {p.title}\nWords: {p.word_count}\n"
            f"Headings: {'; '.join(p.headings[:8])}\n"
            f"Content preview: {p.markdown[:600]}"
            for p in pages
        )

        youtube_context = ""
        if yt_snippets:
            youtube_context = f"\n\nTop YouTube videos on this topic:\n" + "\n".join(yt_snippets)

        system = (
            "You are an expert SEO content strategist. "
            "Analyze competitor content and generate a detailed content brief. "
            "Respond with valid JSON only."
        )

        prompt = f"""Analyze these competitor pages for the keyword: "{keyword}"

{competitor_context}{youtube_context}

Generate a JSON content brief with exactly these keys:
{{
  "recommended_title": "SEO-optimized H1 title",
  "suggested_h2s": ["H2 heading 1", "H2 heading 2", "H2 heading 3", "H2 heading 4", "H2 heading 5"],
  "suggested_h3s": ["H3 subheading 1", "H3 subheading 2", "H3 subheading 3"],
  "faq_questions": ["FAQ Q1?", "FAQ Q2?", "FAQ Q3?", "FAQ Q4?", "FAQ Q5?"],
  "content_gaps": ["Topic competitors missed 1", "Topic competitors missed 2", "Topic competitors missed 3"],
  "competitor_summary": "2-3 sentence summary of competitor content quality and gaps"
}}"""

        result = aion.brain_json(prompt, system=system, model="claude-max", max_tokens=1500)

        if result and isinstance(result, dict):
            brief.recommended_title = result.get("recommended_title", "")
            brief.suggested_h2s = result.get("suggested_h2s", [])
            brief.suggested_h3s = result.get("suggested_h3s", [])
            brief.faq_questions = result.get("faq_questions", [])
            brief.content_gaps = result.get("content_gaps", [])
            brief.competitor_summary = result.get("competitor_summary", "")
            log.info(
                "competitor_scraper.brief_ok  keyword=%s  h2s=%d  faqs=%d",
                keyword, len(brief.suggested_h2s), len(brief.faq_questions),
            )
        else:
            log.warning("competitor_scraper.brain_fail  keyword=%s", keyword)

        return brief

    def brief_to_dict(self, brief: ContentBrief) -> dict:
        """Convert ContentBrief to a JSON-serializable dict."""
        return {
            "keyword": brief.keyword,
            "recommended_word_count": brief.recommended_word_count,
            "recommended_title": brief.recommended_title,
            "suggested_h2s": brief.suggested_h2s,
            "suggested_h3s": brief.suggested_h3s,
            "faq_questions": brief.faq_questions,
            "competitor_summary": brief.competitor_summary,
            "content_gaps": brief.content_gaps,
            "youtube_insights": brief.youtube_insights,
            "sources": brief.sources,
        }
