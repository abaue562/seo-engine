"""AION v21 integration bridge — plugs SEO engine into VPS AI infrastructure.

Available AION services (all on localhost):
  Brain        :9082  — OpenAI-compatible LLM router (groq, mistral, claude-max)
  Memory       :9190  — Semantic/episodic memory store + vector recall
  Knowledge    :9091  — Knowledge graph (nodes, edges, query)
  YouTube      :9240  — Video search + transcript extraction
  Firecrawl    :3002  — Web scraping to clean markdown (replaces BeautifulSoup)
  Email Sender :9280  — Transactional email sending
  Outbound     :9301  — Lead management (add leads with score + source)
  Research     :9250  — Signal feed (HackerNews, Reddit, news)

Usage:
    from core.aion_bridge import aion

    # LLM call via AION Brain (cheaper than direct Claude API)
    text = aion.brain_complete("Summarize this content: ...")

    # Scrape competitor URL to clean markdown
    md = aion.firecrawl_scrape("https://competitor.com/target-page")

    # Store SEO analysis for later recall
    aion.memory_store("Keyword analysis: 'best link building tools'...", tags=["keyword"])

    # Find YouTube videos on a topic
    videos = aion.youtube_search("local SEO tips", max_results=5)

    # Get trending signals from research aggregator
    signals = aion.get_signals(limit=20)
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

log = logging.getLogger(__name__)

# Base URLs — all services run on VPS localhost
_BRAIN_URL     = os.getenv("AION_BRAIN_URL",     "http://localhost:9082")
_MEMORY_URL    = os.getenv("AION_MEMORY_URL",    "http://localhost:9190")
_KNOWLEDGE_URL = os.getenv("AION_KNOWLEDGE_URL", "http://localhost:9091")
_YOUTUBE_URL   = os.getenv("AION_YOUTUBE_URL",   "http://localhost:9240")
_FIRECRAWL_URL = os.getenv("AION_FIRECRAWL_URL", "http://localhost:3002")
_EMAIL_URL     = os.getenv("AION_EMAIL_URL",     "http://localhost:9280")
_OUTBOUND_URL  = os.getenv("AION_OUTBOUND_URL",  "http://localhost:9301")
_RESEARCH_URL  = os.getenv("AION_RESEARCH_URL",  "http://localhost:9250")

_TIMEOUT = int(os.getenv("AION_TIMEOUT", "20"))


def _post(base: str, path: str, data: dict, timeout: int = _TIMEOUT) -> dict:
    url = f"{base}{path}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")[:300]
        log.warning("aion.post_error  url=%s  code=%d  detail=%s", url, e.code, detail)
        return {"error": e.code, "detail": detail}
    except Exception as exc:
        log.warning("aion.post_fail  url=%s  err=%s", url, exc)
        return {"error": str(exc)}


def _get(base: str, path: str, timeout: int = _TIMEOUT) -> Any:
    url = f"{base}{path}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as exc:
        log.warning("aion.get_fail  url=%s  err=%s", url, exc)
        return {"error": str(exc)}


def _post_qs(base: str, path: str, params: dict, timeout: int = _TIMEOUT) -> Any:
    """POST with query-string params (for AION services that use ?param= style)."""
    qs = urllib.parse.urlencode(params)
    url = f"{base}{path}?{qs}"
    req = urllib.request.Request(url, data=b"", method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")[:300]
        log.warning("aion.postqs_error  url=%s  code=%d", url, e.code)
        return {"error": e.code, "detail": detail}
    except Exception as exc:
        log.warning("aion.postqs_fail  url=%s  err=%s", url, exc)
        return {"error": str(exc)}


class AIONBridge:
    """Unified interface to all AION v21 services."""

    # ------------------------------------------------------------------
    # Brain — LLM routing (claude-max, groq, mistral, ollama)
    # ------------------------------------------------------------------

    def brain_complete(
        self,
        prompt: str,
        system: str = "",
        model: str = "claude-max",
        max_tokens: int = 2048,
    ) -> str:
        """Call AION Brain for LLM completion. Returns empty string on error.

        Model options: 'claude-max' (Claude subscription), 'groq' (fast/free),
                       'mistral', 'openai', 'ollama' (local).
        Brain applies semantic caching — repeat prompts served from cache.
        """
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        result = _post(_BRAIN_URL, "/v1/chat/completions", {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
        }, timeout=120)

        if "error" in result:
            log.warning("aion.brain_fail  model=%s", model)
            return ""

        # Brain wraps the raw provider response under 'raw'
        try:
            text = result["raw"]["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError):
            try:
                text = result["choices"][0]["message"]["content"]
            except Exception:
                log.warning("aion.brain_parse_fail  result=%s", str(result)[:200])
                return ""

        log.info(
            "aion.brain_ok  model=%s  provider=%s  tokens=%d  latency_ms=%.0f",
            model,
            result.get("provider", "?"),
            result.get("tokens_used", 0),
            result.get("latency_ms", 0),
        )
        return text

    def brain_json(
        self,
        prompt: str,
        system: str = "",
        model: str = "groq",
        max_tokens: int = 2048,
    ) -> dict | list:
        """Call Brain and parse the response as JSON. Returns {} on failure."""
        raw = self.brain_complete(prompt, system, model, max_tokens)
        if not raw:
            return {}
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        start = next((i for i, ch in enumerate(raw) if ch in "{["), -1)
        if start > 0:
            raw = raw[start:]
        try:
            data, _ = json.JSONDecoder().raw_decode(raw)
            return data
        except json.JSONDecodeError:
            log.warning("aion.brain_json_fail  preview=%s", raw[:200])
            return {}

    # ------------------------------------------------------------------
    # Firecrawl — JS-aware web scraping to markdown
    # ------------------------------------------------------------------

    def firecrawl_scrape(self, url: str, timeout: int = 30) -> str:
        """Scrape a URL and return clean markdown. Empty string on failure.

        Handles JS-rendered pages. Replaces BeautifulSoup in competitor analysis.
        """
        result = _post(_FIRECRAWL_URL, "/v1/scrape",
                       {"url": url, "formats": ["markdown"]}, timeout=timeout)
        if result.get("success"):
            md = result.get("data", {}).get("markdown", "")
            log.info("aion.firecrawl_ok  url=%s  chars=%d", url, len(md))
            return md
        log.warning("aion.firecrawl_fail  url=%s  err=%s", url, str(result)[:150])
        return ""

    def firecrawl_scrape_meta(self, url: str) -> dict:
        """Scrape a URL and return markdown + metadata dict."""
        result = _post(_FIRECRAWL_URL, "/v1/scrape",
                       {"url": url, "formats": ["markdown"]}, timeout=30)
        if result.get("success"):
            data = result.get("data", {})
            meta = data.get("metadata", {})
            return {
                "markdown": data.get("markdown", ""),
                "title": meta.get("title", ""),
                "language": meta.get("language", ""),
                "status_code": meta.get("statusCode", 0),
                "source_url": meta.get("sourceURL", url),
            }
        return {"error": result.get("error", "scrape failed"), "url": url}

    # ------------------------------------------------------------------
    # Memory — semantic store + vector recall
    # ------------------------------------------------------------------

    def memory_store(
        self,
        content: str,
        tier: str = "semantic",
        agent_id: str = "seo-engine",
        tags: list[str] | None = None,
    ) -> bool:
        """Store content in AION memory. Content must be >100 chars to pass filter.

        Tiers: 'working' | 'episodic' | 'semantic' | 'procedural'
        Returns True if stored successfully.
        """
        result = _post(_MEMORY_URL, "/store", {
            "tier": tier,
            "content": content,
            "agent_id": agent_id,
            "tags": tags or [],
        })
        stored = bool(result.get("stored", False))
        if not stored:
            log.debug("aion.memory_skip  reason=%s", result.get("reason", "?"))
        return stored

    def memory_recall(
        self,
        query: str,
        tier: str = "all",
        agent_id: str = "seo-engine",
        limit: int = 5,
    ) -> list[dict]:
        """Recall memories relevant to a query. Returns list of memory dicts."""
        result = _post(_MEMORY_URL, "/recall", {
            "query": query,
            "tier": tier,
            "agent_id": agent_id,
            "limit": limit,
        })
        return result.get("results", [])

    # ------------------------------------------------------------------
    # Knowledge graph
    # ------------------------------------------------------------------

    def knowledge_query(self, query: str, limit: int = 10) -> dict:
        """Query the knowledge graph. Returns {nodes, edges, subgraph}."""
        return _post(_KNOWLEDGE_URL, "/query", {"query": query, "limit": limit})

    def knowledge_add_node(
        self,
        label: str,
        node_type: str,
        properties: dict | None = None,
    ) -> dict:
        """Add a node to the knowledge graph (keyword, topic, competitor, entity)."""
        return _post(_KNOWLEDGE_URL, "/nodes", {
            "label": label,
            "type": node_type,
            "properties": properties or {},
        })

    def knowledge_add_edge(
        self,
        source_id: str,
        target_id: str,
        relation: str,
        weight: float = 1.0,
    ) -> dict:
        """Add a relationship edge between knowledge graph nodes."""
        return _post(_KNOWLEDGE_URL, "/edges", {
            "source_id": source_id,
            "target_id": target_id,
            "relation": relation,
            "weight": weight,
        })

    # ------------------------------------------------------------------
    # YouTube — video research + transcripts
    # ------------------------------------------------------------------

    def youtube_search(self, query: str, max_results: int = 5) -> list[dict]:
        """Search YouTube. Returns list of {video_id, title, channel, duration, views, url}."""
        result = _post_qs(_YOUTUBE_URL, "/search",
                          {"query": query, "max_results": max_results})
        return result.get("videos", []) if isinstance(result, dict) else []

    def youtube_transcript(self, video_id: str) -> str:
        """Get transcript for a video by ID. Returns empty string on failure."""
        url = f"{_YOUTUBE_URL}/transcript?video_id={urllib.parse.quote(video_id)}"
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                data = json.loads(r.read())
                return data.get("transcript", "") or data.get("text", "")
        except Exception as exc:
            log.warning("aion.youtube_transcript_fail  video=%s  err=%s", video_id, exc)
            return ""

    def youtube_research(self, topic: str, max_videos: int = 3) -> list[dict]:
        """Search YouTube and retrieve transcripts. Returns enriched video list.

        Use for: FAQ enrichment, topic gap analysis, competitor content research.
        """
        videos = self.youtube_search(topic, max_results=max_videos)
        results = []
        for v in videos:
            transcript = self.youtube_transcript(v["video_id"])
            results.append({
                "title": v["title"],
                "channel": v["channel"],
                "url": v["url"],
                "duration_s": v.get("duration", 0),
                "views": v.get("view_count", 0),
                "transcript": transcript,
            })
        log.info(
            "aion.youtube_research  topic=%s  videos=%d  with_transcript=%d",
            topic, len(results), sum(1 for r in results if r["transcript"]),
        )
        return results

    # ------------------------------------------------------------------
    # Email Sender
    # ------------------------------------------------------------------

    def send_email(
        self,
        to_email: str,
        subject: str,
        body_html: str,
        body_text: str = "",
    ) -> bool:
        """Send transactional email via AION Email Sender. Returns True on success."""
        result = _post_qs(_EMAIL_URL, "/send", {
            "to_email": to_email,
            "subject": subject,
            "body_html": body_html,
            "body_text": body_text,
        })
        success = bool(result.get("success") or result.get("queued"))
        if not success:
            log.warning("aion.email_fail  to=%s  result=%s", to_email, str(result)[:150])
        return success

    # ------------------------------------------------------------------
    # Outbound Engine — lead management
    # ------------------------------------------------------------------

    def add_lead(
        self,
        email: str,
        name: str = "",
        company: str = "",
        source: str = "seo-engine",
        score: float = 0.0,
    ) -> bool:
        """Add a lead to AION Outbound Engine. Returns True on success."""
        result = _post_qs(_OUTBOUND_URL, "/add-lead", {
            "email": email,
            "name": name,
            "company": company,
            "source": source,
            "score": score,
        })
        return "error" not in result

    # ------------------------------------------------------------------
    # Research Aggregator — trending signals
    # ------------------------------------------------------------------

    def get_signals(self, source: str | None = None, limit: int = 20) -> list[dict]:
        """Get trending signals from AION Research Aggregator.

        Sources: 'hackernews', 'reddit', 'news' (or None for all).
        Returns list of {content, url, source, score, signal_type, created_at}.
        Use for: content calendar topics, trending query discovery.
        """
        path = f"/signals?limit={limit}"
        if source:
            path += f"&source={urllib.parse.quote(source)}"
        result = _get(_RESEARCH_URL, path)
        return result if isinstance(result, list) else []

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def health(self) -> dict[str, bool]:
        """Check which AION services are reachable. Returns {name: bool}."""
        # Standard /health endpoints
        checks = {
            "brain":     (_BRAIN_URL,     "/health"),
            "memory":    (_MEMORY_URL,    "/health"),
            "knowledge": (_KNOWLEDGE_URL, "/health"),
            "youtube":   (_YOUTUBE_URL,   "/health"),
            "email":     (_EMAIL_URL,     "/health"),
            "outbound":  (_OUTBOUND_URL,  "/health"),
            "research":  (_RESEARCH_URL,  "/health"),
        }
        status = {
            name: "error" not in _get(base, path, timeout=5)
            for name, (base, path) in checks.items()
        }

        # Firecrawl uses a different health check (no /health route)
        try:
            result = _post(_FIRECRAWL_URL, "/v1/scrape",
                           {"url": "https://example.com", "formats": ["markdown"]},
                           timeout=10)
            status["firecrawl"] = bool(result.get("success"))
        except Exception:
            status["firecrawl"] = False

        return status


# Global singleton
aion = AIONBridge()
