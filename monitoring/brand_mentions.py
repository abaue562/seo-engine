"""
Brand mention and AI citation monitoring.

Monitors when the business is mentioned in:
- AI search responses (Perplexity, ChatGPT)
- Web search results
- News articles
- Social media

Usage:
    monitor = BrandMentionMonitor()
    mentions = monitor.scan_web_mentions(brand="Joe's Plumbing", location="NYC")
    ai_mentions = monitor.check_perplexity_citation(brand="Joe's Plumbing", queries=["best plumber NYC"])
"""
from __future__ import annotations

import os
import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional
from urllib.parse import quote_plus, urlparse

log = logging.getLogger(__name__)

# Sentiment word lists
_POSITIVE_WORDS = frozenset([
    "great", "excellent", "recommend", "best", "love", "amazing", "helpful",
    "professional", "outstanding", "fantastic", "wonderful", "superb", "top",
    "reliable", "trustworthy", "responsive", "quality", "expert", "efficient",
    "honest", "fair", "friendly", "knowledgeable", "satisfied", "impressed",
    "highly", "five stars", "5 stars", "5-star",
])

_NEGATIVE_WORDS = frozenset([
    "terrible", "worst", "avoid", "scam", "bad", "never", "unprofessional",
    "rude", "awful", "horrible", "disappointed", "waste", "overpriced", "late",
    "unreliable", "poor", "incompetent", "dishonest", "fraud", "broken", "failed",
    "no-show", "never showed", "one star", "1 star", "1-star",
])

_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


@dataclass
class Mention:
    source: str          # 'perplexity', 'google', 'bing', 'news', 'reddit', etc.
    url: str
    snippet: str         # Text snippet containing the mention
    sentiment: str       # 'positive', 'neutral', 'negative'
    has_link: bool       # Whether mention includes a link back
    discovered_at: str   # ISO timestamp
    query_used: str      # What query triggered the discovery


def _get_http_client():
    """Return httpx.Client or fallback requests wrapper."""
    try:
        import httpx
        return httpx.Client(
            timeout=30,
            follow_redirects=True,
            headers={"User-Agent": _DEFAULT_UA},
        )
    except ImportError:
        try:
            import requests

            class _RequestsWrapper:
                def __init__(self):
                    self._session = requests.Session()
                    self._session.headers["User-Agent"] = _DEFAULT_UA

                def get(self, url, **kwargs):
                    return self._session.get(url, **kwargs)

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    self._session.close()

            return _RequestsWrapper()
        except ImportError:
            return None


def _html_to_text(html: str) -> str:
    """Strip HTML tags, return plain text."""
    try:
        from bs4 import BeautifulSoup
        return BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    except ImportError:
        return re.sub(r"<[^>]+>", " ", html)


def _extract_snippets(text: str, brand: str, context_chars: int = 200) -> list[str]:
    """Find all occurrences of brand in text and return surrounding snippets."""
    snippets = []
    brand_lower = brand.lower()
    text_lower = text.lower()
    start = 0
    while True:
        idx = text_lower.find(brand_lower, start)
        if idx == -1:
            break
        left = max(0, idx - context_chars)
        right = min(len(text), idx + len(brand) + context_chars)
        snippets.append(text[left:right].strip())
        start = idx + 1
    return snippets


class BrandMentionMonitor:
    STORAGE_PATH = Path("data/storage/brand_mentions")

    def __init__(self):
        self.storage = self.STORAGE_PATH
        self.storage.mkdir(parents=True, exist_ok=True)
        self.brandmentions_api_key = os.getenv("BRANDMENTIONS_API_KEY", "")

    # ------------------------------------------------------------------
    # Main entry point (called by Celery task monitor_ai_citations)
    # ------------------------------------------------------------------

    async def check(self, business_name: str, business_id: str | None = None) -> list[dict]:
        """Aggregate all mention sources for a brand and return a list of mention dicts.

        This is the primary method called by the Celery task `monitor_ai_citations`.

        Args:
            business_name: Display name of the business to search for.
            business_id:   Optional identifier for storage namespacing.

        Returns:
            List of mention dicts (serialised Mention objects).
        """
        # Run perplexity + web scan synchronously (they use blocking httpx.Client)
        import asyncio

        loop = asyncio.get_event_loop()
        perplexity_mentions: list[Mention] = await loop.run_in_executor(
            None, self.check_perplexity_citation, business_name
        )
        web_mentions: list[Mention] = await loop.run_in_executor(
            None, self.scan_web_mentions, business_name
        )

        # API-based check (non-blocking if key missing)
        api_mentions: list[Mention] = []
        if self.brandmentions_api_key:
            api_mentions = await loop.run_in_executor(
                None, self.check_via_brandmentions_api, business_name
            )

        all_mentions = perplexity_mentions + web_mentions + api_mentions

        # Deduplicate by URL
        seen: set[str] = set()
        deduped: list[Mention] = []
        for m in all_mentions:
            if m.url not in seen:
                seen.add(m.url)
                deduped.append(m)

        # Persist to storage
        brand_id = business_id or re.sub(r"[^a-z0-9]+", "-", business_name.lower()).strip("-")
        if deduped:
            self.save_mentions(brand_id, deduped)

        log.info(
            "brand_monitor.check_done  brand=%s  total=%d  ai=%d  web=%d  api=%d",
            business_name, len(deduped),
            len(perplexity_mentions), len(web_mentions), len(api_mentions),
        )

        from dataclasses import asdict
        return [asdict(m) for m in deduped]

    # ------------------------------------------------------------------
    # AI Citation Monitoring
    # ------------------------------------------------------------------

    def check_perplexity_citation(self, brand: str, queries: list[str] | None = None) -> list[Mention]:
        """Check if brand appears in Perplexity search results.

        Uses Playwright browser automation (core.browser_llm) instead of httpx scraping.
        Gets the fully JS-rendered answer, not just the static HTML skeleton.
        No PERPLEXITY_API_KEY required.
        """
        if queries is None:
            queries = [
                f"{brand} reviews",
                f"is {brand} good",
                f"best {brand.split()[0]} services near me",
            ]

        from core.browser_llm import call_perplexity_sync

        mentions = []
        for query in queries:
            try:
                answer_text, citations = call_perplexity_sync(query, wait_seconds=8.0)
                if not answer_text:
                    continue

                snippets = _extract_snippets(answer_text, brand)
                search_url = f"https://www.perplexity.ai/search?q={quote_plus(query)}"
                has_link = any(brand.lower().replace(" ", "") in url.lower().replace("-", "") for url in citations)

                for snippet in snippets:
                    sentiment = self.analyze_sentiment(snippet, brand)
                    mentions.append(Mention(
                        source="perplexity",
                        url=search_url,
                        snippet=snippet[:500],
                        sentiment=sentiment,
                        has_link=has_link,
                        discovered_at=datetime.utcnow().isoformat(),
                        query_used=query,
                    ))
                    log.info(
                        "brand_monitor.perplexity_hit  brand=%s  query=%s  sentiment=%s",
                        brand, query, sentiment,
                    )
            except Exception as e:
                log.warning("brand_monitor.perplexity_error  query=%s  err=%s", query, e)

        return mentions

    # ------------------------------------------------------------------
    # Web SERP Scraping
    # ------------------------------------------------------------------

    def scan_web_mentions(self, brand: str, location: str = "") -> list[Mention]:
        """Scan Google and Bing for brand mentions.

        Uses scraping (no API key needed) to find brand mentions in SERP snippets.
        Returns deduplicated list of Mention objects.
        """
        search_term = f'"{brand}"' + (f" {location}" if location else "")
        mentions = []
        seen_urls: set[str] = set()

        client = _get_http_client()
        if client is None:
            log.warning("brand_monitor.no_http_client")
            return mentions

        search_engines = [
            {
                "name": "google",
                "url": f"https://www.google.com/search?q={quote_plus(search_term)}&num=20",
                "snippet_pattern": re.compile(
                    r'<div[^>]*class="[^"]*(?:VwiC3b|yXK7lf|s3v9rd)[^"]*"[^>]*>(.*?)</div>',
                    re.DOTALL | re.IGNORECASE,
                ),
                "url_pattern": re.compile(
                    r'<a href="/url\?q=(https?://[^&"]+)', re.IGNORECASE
                ),
            },
            {
                "name": "bing",
                "url": f"https://www.bing.com/search?q={quote_plus(search_term)}&count=20",
                "snippet_pattern": re.compile(
                    r'<p[^>]*class="[^"]*b_lineclamp[^"]*"[^>]*>(.*?)</p>',
                    re.DOTALL | re.IGNORECASE,
                ),
                "url_pattern": re.compile(
                    r'<cite[^>]*>(https?://[^<]+)</cite>', re.IGNORECASE
                ),
            },
        ]

        with client if hasattr(client, "__enter__") else _NullContext(client) as c:
            for engine in search_engines:
                try:
                    resp = c.get(engine["url"], timeout=20)
                    if resp.status_code != 200:
                        continue

                    html = resp.text
                    text = _html_to_text(html)

                    # Extract result URLs
                    result_urls = engine["url_pattern"].findall(html)

                    # Extract snippets containing the brand
                    raw_snippets = engine["snippet_pattern"].findall(html)
                    brand_lower = brand.lower()

                    for i, raw in enumerate(raw_snippets):
                        snippet_text = _html_to_text(raw)
                        if brand_lower not in snippet_text.lower():
                            continue

                        result_url = result_urls[i] if i < len(result_urls) else engine["url"]
                        if result_url in seen_urls:
                            continue
                        seen_urls.add(result_url)

                        sentiment = self.analyze_sentiment(snippet_text, brand)
                        mentions.append(Mention(
                            source=engine["name"],
                            url=result_url,
                            snippet=snippet_text[:500],
                            sentiment=sentiment,
                            has_link=True,  # SERP results always link
                            discovered_at=datetime.utcnow().isoformat(),
                            query_used=search_term,
                        ))

                    log.info(
                        "brand_monitor.serp_scan  engine=%s  brand=%s  found=%d",
                        engine["name"], brand, len([m for m in mentions if m.source == engine["name"]]),
                    )
                except Exception as e:
                    log.warning("brand_monitor.serp_error  engine=%s  err=%s", engine["name"], e)

        return mentions

    # ------------------------------------------------------------------
    # BrandMentions API
    # ------------------------------------------------------------------

    def check_via_brandmentions_api(self, brand: str) -> list[Mention]:
        """Use BrandMentions.com API if key is configured.

        GET https://api.brandmentions.com/api/v2/mentions
        Returns empty list if API key not configured.
        """
        if not self.brandmentions_api_key:
            log.debug("brand_monitor.api_key_not_set  skipping_brandmentions_api")
            return []

        client = _get_http_client()
        if client is None:
            return []

        mentions = []
        try:
            with client if hasattr(client, "__enter__") else _NullContext(client) as c:
                resp = c.get(
                    "https://api.brandmentions.com/api/v2/mentions",
                    params={
                        "token": self.brandmentions_api_key,
                        "q": brand,
                        "limit": 50,
                    },
                    timeout=20,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for item in data.get("mentions", []):
                        url = item.get("url", "")
                        snippet = item.get("snippet", item.get("title", ""))
                        sentiment_raw = item.get("sentiment", "neutral")
                        # Normalize to our three values
                        if "positive" in sentiment_raw:
                            sentiment = "positive"
                        elif "negative" in sentiment_raw:
                            sentiment = "negative"
                        else:
                            sentiment = self.analyze_sentiment(snippet, brand)

                        mentions.append(Mention(
                            source=item.get("source", "brandmentions"),
                            url=url,
                            snippet=snippet[:500],
                            sentiment=sentiment,
                            has_link=bool(url),
                            discovered_at=datetime.utcnow().isoformat(),
                            query_used=brand,
                        ))
                else:
                    log.warning("brand_monitor.api_error  status=%d", resp.status_code)
        except Exception as e:
            log.warning("brand_monitor.brandmentions_api_exception  err=%s", e)

        return mentions

    # ------------------------------------------------------------------
    # Unlinked mentions
    # ------------------------------------------------------------------

    def get_unlinked_mentions(self, brand: str, domain: str) -> list[Mention]:
        """Find mentions that don't link back to the domain — link building opportunities.

        Returns mentions where has_link=False.
        """
        all_mentions = self.scan_web_mentions(brand)
        all_mentions += self.check_via_brandmentions_api(brand)

        domain_clean = domain.lower().replace("https://", "").replace("http://", "").rstrip("/")
        unlinked = []

        client = _get_http_client()

        for mention in all_mentions:
            if mention.has_link:
                # Verify the link actually points to our domain by fetching page
                try:
                    if client is not None:
                        with client if hasattr(client, "__enter__") else _NullContext(client) as c:
                            resp = c.get(mention.url, timeout=10)
                            page_text = resp.text.lower()
                            if domain_clean not in page_text:
                                mention.has_link = False
                                unlinked.append(mention)
                except Exception:
                    pass  # Conservative: if we can't check, don't flag as unlinked
            else:
                unlinked.append(mention)

        log.info("brand_monitor.unlinked  brand=%s  count=%d", brand, len(unlinked))
        return unlinked

    # ------------------------------------------------------------------
    # Sentiment analysis
    # ------------------------------------------------------------------

    def analyze_sentiment(self, text: str, brand: str) -> str:
        """Simple lexicon-based sentiment analysis for brand mentions.

        Positive keywords: great, excellent, recommend, best, love, amazing, helpful, professional
        Negative keywords: terrible, worst, avoid, scam, bad, never, unprofessional, rude
        Returns 'positive', 'negative', or 'neutral'
        """
        text_lower = text.lower()
        words_and_phrases = re.split(r"\W+", text_lower)
        word_set = set(words_and_phrases)
        full_text = text_lower  # for multi-word phrases

        positive_hits = sum(1 for w in _POSITIVE_WORDS if w in full_text)
        negative_hits = sum(1 for w in _NEGATIVE_WORDS if w in full_text)

        if positive_hits > negative_hits:
            return "positive"
        elif negative_hits > positive_hits:
            return "negative"
        return "neutral"

    # ------------------------------------------------------------------
    # Citation report
    # ------------------------------------------------------------------

    def generate_citation_report(self, brand: str, domain: str) -> dict:
        """Full citation report: AI mentions, web mentions, unlinked mentions, sentiment.

        Returns {brand, total_mentions, ai_citations, web_mentions, unlinked_opportunities,
                 avg_sentiment, top_sources, recommendations}
        """
        log.info("brand_monitor.report_start  brand=%s", brand)

        ai_mentions = self.check_perplexity_citation(brand)
        web_mentions = self.scan_web_mentions(brand)
        api_mentions = self.check_via_brandmentions_api(brand)

        all_mentions = ai_mentions + web_mentions + api_mentions

        # Deduplicate by URL
        seen: set[str] = set()
        deduped: list[Mention] = []
        for m in all_mentions:
            if m.url not in seen:
                seen.add(m.url)
                deduped.append(m)

        # Unlinked opportunities (from web + api, already deduplicated)
        domain_clean = domain.lower().replace("https://", "").replace("http://", "").rstrip("/")
        unlinked = [m for m in deduped if not m.has_link]

        # Sentiment breakdown
        sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
        for m in deduped:
            sentiment_counts[m.sentiment] = sentiment_counts.get(m.sentiment, 0) + 1

        total = len(deduped)
        if total > 0:
            avg_sentiment = max(sentiment_counts, key=lambda k: sentiment_counts[k])
        else:
            avg_sentiment = "neutral"

        # Top sources by frequency
        source_counts: dict[str, int] = {}
        for m in deduped:
            source_counts[m.source] = source_counts.get(m.source, 0) + 1
        top_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        # Recommendations
        recommendations = []
        if len(ai_mentions) == 0:
            recommendations.append(
                "No AI citations found — create more E-E-A-T content and build backlinks "
                "to increase Perplexity/ChatGPT visibility."
            )
        if len(unlinked) > 0:
            recommendations.append(
                f"Found {len(unlinked)} unlinked mentions — reach out to earn backlinks "
                "from these existing brand references."
            )
        if sentiment_counts.get("negative", 0) > sentiment_counts.get("positive", 0):
            recommendations.append(
                "Negative sentiment dominates — prioritize review response strategy "
                "and reputation repair content."
            )
        if total < 5:
            recommendations.append(
                "Low mention volume — invest in PR outreach, guest posts, and press releases "
                "to build brand citation coverage."
            )

        # Save report snapshot
        brand_id = re.sub(r"[^a-z0-9]+", "-", brand.lower()).strip("-")
        self.save_mentions(brand_id, deduped)

        return {
            "brand": brand,
            "domain": domain,
            "generated_at": datetime.utcnow().isoformat(),
            "total_mentions": total,
            "ai_citations": len(ai_mentions),
            "web_mentions": len(web_mentions),
            "api_mentions": len(api_mentions),
            "unlinked_opportunities": len(unlinked),
            "sentiment": sentiment_counts,
            "avg_sentiment": avg_sentiment,
            "top_sources": [{"source": s, "count": c} for s, c in top_sources],
            "recommendations": recommendations,
            "mentions_sample": [asdict(m) for m in deduped[:10]],
        }

    # ------------------------------------------------------------------
    # Storage
    # ------------------------------------------------------------------

    def save_mentions(self, brand_id: str, mentions: list[Mention]) -> None:
        """Append new mentions to storage, deduplicating by URL."""
        path = self.storage / f"{brand_id}.json"
        existing: list[dict] = []
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                existing = []

        existing_urls = {m.get("url") for m in existing}
        new_records = [asdict(m) for m in mentions if m.url not in existing_urls]
        combined = existing + new_records

        path.write_text(json.dumps(combined, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info("brand_monitor.saved  brand=%s  new=%d  total=%d", brand_id, len(new_records), len(combined))

    def load_mentions(self, brand_id: str, days: int = 30) -> list[Mention]:
        """Load mentions from last N days."""
        path = self.storage / f"{brand_id}.json"
        if not path.exists():
            return []

        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        try:
            records = json.loads(path.read_text(encoding="utf-8"))
            mentions = []
            for r in records:
                if r.get("discovered_at", "") >= cutoff:
                    try:
                        mentions.append(Mention(**r))
                    except Exception:
                        pass
            return mentions
        except Exception as e:
            log.warning("brand_monitor.load_error  brand=%s  err=%s", brand_id, e)
            return []


class _NullContext:
    """Simple context manager wrapper for objects that don't support 'with'."""
    def __init__(self, obj):
        self._obj = obj

    def __enter__(self):
        return self._obj

    def __exit__(self, *args):
        pass
