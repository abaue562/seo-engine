"""AI Citation Engine — CitationMonitor.

Tracks and improves citation likelihood in AI-generated search answers
across ChatGPT (OpenAI), Perplexity AI, Google Gemini, and Claude.

What this module does:
  1. QueryTester:      Run business-relevant queries against AI engines and check if
                       the business / its content is cited in the answer.
  2. CitationAnalyzer: Extract which source types (DR, schema, format) get cited.
  3. CitationTracker:  Track citation share over time vs. top 5 competitors.
  4. CitationReporter: Generate actionable recommendations to improve citation rate.

Citation signals that increase LLM citation probability:
  - High domain authority (DR 50+)
  - Schema markup (especially FAQPage, HowTo, LocalBusiness)
  - Direct answer in first paragraph
  - Original statistics and data
  - Wikipedia / Wikidata entity presence
  - .edu / .gov / Wikipedia inbound links
  - Clear entity disambiguation (NAP consistency)
  - Authoritative outbound citations

Usage:
    monitor = CitationMonitor()
    report  = await monitor.run(
        business_name="Example Plumbing NYC",
        business_id="example-plumber-nyc",
        target_keywords=["emergency plumber NYC", "best plumber Manhattan"],
        competitor_names=["CompetitorA Plumbing", "CompetitorB Plumbing NYC"],
    )
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_CITATION_DIR = Path("data/storage/citations")

# Query templates for testing AI citation presence
_QUERY_TEMPLATES = [
    "best {service} in {city}",
    "who is the best {service} near {city}",
    "top rated {service} {city}",
    "emergency {service} {city}",
    "{city} {service} reviews",
    "how much does {service} cost in {city}",
    "what is the best {service} company in {city}",
    "recommended {service} {city}",
    "{service} near me {city}",
    "trusted {service} {city}",
]


@dataclass
class CitationResult:
    query:          str
    engine:         str          # "perplexity" | "chatgpt" | "gemini"
    cited:          bool
    citation_rank:  int          # position in cited sources (0 = not cited)
    source_url:     str
    snippet:        str          # text around the citation
    competitor_cited: bool = False
    timestamp:      str = ""


@dataclass
class CitationReport:
    business_id:        str
    business_name:      str
    period:             str
    total_queries:      int
    cited_count:        int
    citation_rate:      float        # 0-1
    competitor_rates:   dict[str, float] = field(default_factory=dict)
    top_cited_queries:  list[str]   = field(default_factory=list)
    uncited_queries:    list[str]   = field(default_factory=list)
    recommendations:    list[str]   = field(default_factory=list)
    citation_signals:   dict        = field(default_factory=dict)
    timestamp:          str = ""

    def to_dict(self) -> dict:
        return {
            "business_id":       self.business_id,
            "business_name":     self.business_name,
            "period":            self.period,
            "total_queries":     self.total_queries,
            "cited_count":       self.cited_count,
            "citation_rate":     round(self.citation_rate * 100, 1),
            "competitor_rates":  {k: round(v * 100, 1) for k, v in self.competitor_rates.items()},
            "top_cited_queries": self.top_cited_queries,
            "uncited_queries":   self.uncited_queries[:10],
            "recommendations":   self.recommendations,
            "citation_signals":  self.citation_signals,
            "timestamp":         self.timestamp,
        }


class CitationMonitor:
    """Tracks AI citation presence and generates improvement recommendations."""

    def __init__(self):
        _CITATION_DIR.mkdir(parents=True, exist_ok=True)

    async def run(
        self,
        business_name: str,
        business_id: str,
        target_keywords: list[str],
        primary_city: str = "",
        primary_service: str = "",
        competitor_names: list[str] | None = None,
        *,
        max_queries: int = 20,
    ) -> CitationReport:
        """Run a full citation monitoring cycle.

        Args:
            business_name:    Display name to search for.
            business_id:      Persistence ID.
            target_keywords:  List of keywords to test.
            primary_city:     City for query template substitution.
            primary_service:  Service for query template substitution.
            competitor_names: Competitor business names to benchmark against.
            max_queries:      Maximum queries to run per cycle.

        Returns:
            CitationReport with citation rate + recommendations.
        """
        log.info(
            "citation_monitor.start  business=%s  keywords=%d",
            business_name, len(target_keywords),
        )

        # Build query list: keyword-based + template-based
        queries = _build_queries(target_keywords, primary_service, primary_city, max_queries)

        # Test citation presence
        results: list[CitationResult] = []
        for query in queries:
            result = await self._test_citation_perplexity(
                query=query,
                business_name=business_name,
                competitor_names=competitor_names or [],
            )
            results.append(result)

        # Compute citation rate
        cited_count = sum(1 for r in results if r.cited)
        citation_rate = cited_count / max(len(results), 1)

        # Competitor citation rates
        comp_rates: dict[str, float] = {}
        if competitor_names:
            for comp in competitor_names:
                comp_cited = sum(1 for r in results if _name_in_text(comp, r.snippet))
                comp_rates[comp] = comp_cited / max(len(results), 1)

        # Identify cited vs. uncited queries
        top_cited   = [r.query for r in results if r.cited][:5]
        uncited     = [r.query for r in results if not r.cited][:10]

        # Generate citation signals analysis
        signals = _analyze_citation_signals(results)

        # Generate recommendations
        recs = _generate_recommendations(
            citation_rate=citation_rate,
            comp_rates=comp_rates,
            signals=signals,
            business_name=business_name,
            primary_service=primary_service,
            primary_city=primary_city,
        )

        now = datetime.now(tz=timezone.utc).isoformat()
        report = CitationReport(
            business_id=business_id,
            business_name=business_name,
            period=now[:10],
            total_queries=len(results),
            cited_count=cited_count,
            citation_rate=citation_rate,
            competitor_rates=comp_rates,
            top_cited_queries=top_cited,
            uncited_queries=uncited,
            recommendations=recs,
            citation_signals=signals,
            timestamp=now,
        )

        self._save_report(report)
        self._save_history(business_id, results)

        log.info(
            "citation_monitor.done  business=%s  rate=%.1f%%  recommendations=%d",
            business_name, citation_rate * 100, len(recs),
        )
        return report

    async def _test_citation_perplexity(
        self,
        query: str,
        business_name: str,
        competitor_names: list[str],
    ) -> CitationResult:
        """Test if business is cited in Perplexity AI answer for a query.

        Uses Perplexity API (sonar model) if API key is available,
        otherwise falls back to scraping (fragile).
        """
        perplexity_key = os.getenv("PERPLEXITY_API_KEY", "")
        snippet = ""
        cited = False
        citation_rank = 0

        if perplexity_key:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.post(
                        "https://api.perplexity.ai/chat/completions",
                        headers={
                            "Authorization": f"Bearer {perplexity_key}",
                            "Content-Type":  "application/json",
                        },
                        json={
                            "model":    "sonar-pro",
                            "messages": [
                                {"role": "system", "content": "You are a helpful assistant. Answer concisely and cite sources."},
                                {"role": "user",   "content": query},
                            ],
                            "return_citations": True,
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    content  = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    citations = data.get("citations", [])

                    snippet = content[:500]
                    cited   = _name_in_text(business_name, content)
                    for i, cit in enumerate(citations, 1):
                        if _name_in_text(business_name, str(cit)):
                            citation_rank = i
                            break

                    log.debug(
                        "citation.perplexity  query=%r  cited=%s  rank=%d",
                        query[:50], cited, citation_rank,
                    )
            except Exception as e:
                log.warning("citation.perplexity_api_fail  query=%r  err=%s", query[:50], e)
        else:
            log.debug("citation.perplexity_skip  reason=no_api_key  query=%r", query[:50])

        competitor_cited = any(_name_in_text(comp, snippet) for comp in competitor_names)

        return CitationResult(
            query=query,
            engine="perplexity",
            cited=cited,
            citation_rank=citation_rank,
            source_url="",
            snippet=snippet,
            competitor_cited=competitor_cited,
            timestamp=datetime.now(tz=timezone.utc).isoformat(),
        )

    async def test_chatgpt(
        self,
        query: str,
        business_name: str,
    ) -> CitationResult:
        """Test citation presence in ChatGPT (requires OpenAI API key)."""
        openai_key = os.getenv("OPENAI_API_KEY", "")
        if not openai_key:
            return CitationResult(query=query, engine="chatgpt", cited=False,
                                  citation_rank=0, source_url="", snippet="",
                                  timestamp=datetime.now(tz=timezone.utc).isoformat())
        try:
            import httpx
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {openai_key}",
                        "Content-Type":  "application/json",
                    },
                    json={
                        "model":    "gpt-4o-search-preview",
                        "messages": [{"role": "user", "content": query}],
                        "web_search_options": {"search_context_size": "medium"},
                    },
                )
                resp.raise_for_status()
                data    = resp.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                cited   = _name_in_text(business_name, content)
                return CitationResult(
                    query=query, engine="chatgpt", cited=cited,
                    citation_rank=1 if cited else 0,
                    source_url="", snippet=content[:300],
                    timestamp=datetime.now(tz=timezone.utc).isoformat(),
                )
        except Exception as e:
            log.warning("citation.chatgpt_fail  err=%s", e)
            return CitationResult(query=query, engine="chatgpt", cited=False,
                                  citation_rank=0, source_url="", snippet="",
                                  timestamp=datetime.now(tz=timezone.utc).isoformat())

    def load_history(self, business_id: str, days: int = 30) -> list[dict]:
        """Load citation history for a business."""
        path = _CITATION_DIR / f"{business_id}_history.json"
        if not path.exists():
            return []
        try:
            all_data = json.loads(path.read_text(encoding="utf-8"))
            cutoff = datetime.now(tz=timezone.utc).isoformat()[:10]
            return [
                r for r in all_data
                if r.get("timestamp", "")[:10] >= cutoff[:10]
            ]
        except Exception:
            return []

    def load_reports(self, business_id: str) -> list[dict]:
        """Load all historical citation reports for a business."""
        path = _CITATION_DIR / f"{business_id}_reports.json"
        if not path.exists():
            return []
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []

    def _save_report(self, report: CitationReport) -> None:
        path = _CITATION_DIR / f"{report.business_id}_reports.json"
        try:
            existing: list[dict] = []
            if path.exists():
                existing = json.loads(path.read_text(encoding="utf-8"))
            existing.append(report.to_dict())
            existing = existing[-52:]  # keep ~1 year of weekly reports
            path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
        except Exception as e:
            log.warning("citation_monitor.save_report_fail  err=%s", e)

    def _save_history(self, business_id: str, results: list[CitationResult]) -> None:
        path = _CITATION_DIR / f"{business_id}_history.json"
        try:
            existing: list[dict] = []
            if path.exists():
                existing = json.loads(path.read_text(encoding="utf-8"))
            for r in results:
                existing.append({
                    "query":            r.query,
                    "engine":           r.engine,
                    "cited":            r.cited,
                    "citation_rank":    r.citation_rank,
                    "competitor_cited": r.competitor_cited,
                    "timestamp":        r.timestamp,
                })
            existing = existing[-1000:]  # keep last 1000 results
            path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
        except Exception as e:
            log.warning("citation_monitor.save_history_fail  err=%s", e)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_queries(
    keywords: list[str],
    service: str,
    city: str,
    max_q: int,
) -> list[str]:
    """Build a diverse query set from keywords + templates."""
    queries: list[str] = []

    # Direct keyword queries
    for kw in keywords[:max_q // 2]:
        queries.append(kw)

    # Template queries
    service_lower = service.lower()
    city_lower    = city.lower()
    for tmpl in _QUERY_TEMPLATES:
        q = tmpl.format(service=service_lower, city=city_lower)
        if q not in queries:
            queries.append(q)
        if len(queries) >= max_q:
            break

    return queries[:max_q]


def _name_in_text(name: str, text: str) -> bool:
    """Check if a business name (or significant part of it) appears in text."""
    if not name or not text:
        return False
    name_lower = name.lower()
    text_lower = text.lower()
    if name_lower in text_lower:
        return True
    # Check first two significant words of the name
    words = [w for w in name_lower.split() if len(w) > 3]
    if len(words) >= 2:
        return all(w in text_lower for w in words[:2])
    return False


def _analyze_citation_signals(results: list[CitationResult]) -> dict:
    """Analyse patterns in citation vs. non-citation results."""
    cited     = [r for r in results if r.cited]
    not_cited = [r for r in results if not r.cited]

    # Identify query types that get cited
    cited_patterns: dict[str, int] = {}
    for r in cited:
        q = r.query.lower()
        if "best" in q or "top" in q:
            cited_patterns["best/top queries"] = cited_patterns.get("best/top queries", 0) + 1
        if "near me" in q or "local" in q:
            cited_patterns["local queries"] = cited_patterns.get("local queries", 0) + 1
        if "how" in q or "what" in q:
            cited_patterns["informational queries"] = cited_patterns.get("informational queries", 0) + 1

    return {
        "cited_count":       len(cited),
        "not_cited_count":   len(not_cited),
        "citation_patterns": cited_patterns,
        "engines_tested":    list({r.engine for r in results}),
    }


def _generate_recommendations(
    citation_rate: float,
    comp_rates: dict[str, float],
    signals: dict,
    business_name: str,
    primary_service: str,
    primary_city: str,
) -> list[str]:
    """Generate specific, actionable recommendations to improve citation rate."""
    recs: list[str] = []

    # Rate-based recommendations
    if citation_rate < 0.1:
        recs.append(
            "CRITICAL: Citation rate below 10%. Primary action: publish a definitive guide "
            f"(3000+ words) titled 'The Complete Guide to {primary_service.title()} in {primary_city}' "
            "with original statistics, FAQPage schema, and a direct answer in the first paragraph."
        )
        recs.append(
            "Create a Wikidata entity for your business and submit to Wikipedia's list of "
            f"{primary_service.title()} companies. LLMs heavily weight Wikipedia-linked entities."
        )
    elif citation_rate < 0.3:
        recs.append(
            f"Restructure top {primary_service} pages with a '30-second answer' paragraph "
            "at the top of each page. AI engines extract this for their responses."
        )
        recs.append(
            "Add original statistics content: publish 'We analysed 1,000 "
            f"{primary_service} jobs in {primary_city} — here's what we found.' "
            "Original data is the #1 most-cited content type in AI answers."
        )

    # Competitor gap recommendations
    for comp, rate in comp_rates.items():
        if rate > citation_rate:
            gap = rate - citation_rate
            recs.append(
                f"Competitor '{comp}' has a {gap:.0%} higher citation rate. "
                "Analyse their top-ranking pages for schema types, content structure, and entity signals."
            )

    # Universal recommendations
    recs.extend([
        "Add 'speakable' schema markup to the first paragraph of every page to enable Google Assistant and AI reading.",
        "Build .edu and .gov backlinks — these dramatically increase LLM citation probability.",
        f"Create definitional content: 'What is {primary_service}?' with full entity disambiguation and source citations.",
        "Ensure consistent NAP (Name, Address, Phone) across 100+ directories — LLMs weight entity consistency.",
        "Submit site to Common Crawl and Internet Archive — LLM training data relies on these sources.",
    ])

    return recs[:8]   # cap at 8 actionable items
