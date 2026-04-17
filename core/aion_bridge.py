"""AION v21 integration bridge — plugs SEO engine into VPS AI infrastructure.

Available AION services (all on localhost):
  Brain           :9082  — OpenAI-compatible LLM router (claude-max, groq, mistral, ollama)
  Memory          :9190  — Semantic/episodic memory store + vector recall
  Knowledge       :9091  — Knowledge graph (nodes, edges, query)
  YouTube         :9240  — Video search + transcript extraction
  Firecrawl       :3002  — JS-aware web scraping to clean markdown
  Email Sender    :9280  — Amazon SES transactional email (aion@gethubed.com)
  Outbound Engine :9301  — Lead management pipeline
  Research        :9250  — Signal feed (HackerNews, Reddit, Google Trends)
  Twitter Intel   :8195  — 4,192 market signals, 299 opportunities, real-time trends
  GPT-Researcher  :8170  — Autonomous deep research (Cerebras/Qwen backend)
  Listmonk        :9001  — Email campaign platform (drip sequences)
  Ollama          :11434 — Local LLM inference (qwen3:8b, nomic-embed-text, glm-4.7-flash)

Usage:
    from core.aion_bridge import aion

    # Embeddings via nomic-embed-text (768-dim, free, local)
    vec = aion.embed("best SEO strategies for local business")

    # LLM call via AION Brain (routes to claude-max, groq, mistral)
    text = aion.brain_complete("Summarize this content: ...")

    # Scrape competitor URL to clean markdown (Playwright-backed)
    md = aion.firecrawl_scrape("https://competitor.com/target-page")

    # Twitter Intel market signals
    signals = aion.twitter_signals(limit=20)

    # Deep research via GPT-Researcher
    report = aion.gpt_research("best link building tactics 2024")

    # Listmonk drip campaign
    aion.listmonk_add_subscriber("prospect@example.com", "John Smith")
"""

from __future__ import annotations

import base64
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

log = logging.getLogger(__name__)

# Base URLs — all services run on VPS localhost
_BRAIN_URL        = os.getenv("AION_BRAIN_URL",        "http://localhost:9082")
_MEMORY_URL       = os.getenv("AION_MEMORY_URL",       "http://localhost:9190")
_KNOWLEDGE_URL    = os.getenv("AION_KNOWLEDGE_URL",    "http://localhost:9091")
_YOUTUBE_URL      = os.getenv("AION_YOUTUBE_URL",      "http://localhost:9240")
_FIRECRAWL_URL    = os.getenv("AION_FIRECRAWL_URL",    "http://localhost:3002")
_EMAIL_URL        = os.getenv("AION_EMAIL_URL",        "http://localhost:9280")
_OUTBOUND_URL     = os.getenv("AION_OUTBOUND_URL",     "http://localhost:9301")
_RESEARCH_URL     = os.getenv("AION_RESEARCH_URL",     "http://localhost:9250")
_TWITTER_URL      = os.getenv("AION_TWITTER_URL",      "http://localhost:8195")
_GPT_RESEARCHER_URL = os.getenv("AION_GPT_RESEARCHER_URL", "http://localhost:8170")
_LISTMONK_URL     = os.getenv("AION_LISTMONK_URL",     "http://localhost:9001")
_LISTMONK_USER    = os.getenv("AION_LISTMONK_USER",    "admin")
_LISTMONK_PASS    = os.getenv("AION_LISTMONK_PASS",    "AionAdmin2026!")
_OLLAMA_URL       = os.getenv("OLLAMA_URL",            "http://localhost:11434")

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
            "name": label,       # AION Knowledge API uses 'name' + 'node_type'
            "node_type": node_type,
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
            "source": source_id,
            "target": target_id,
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
    # Ollama — local embeddings + inference
    # ------------------------------------------------------------------

    def embed(self, text: str, model: str = "nomic-embed-text") -> list[float]:
        """Generate a 768-dim embedding vector via Ollama nomic-embed-text.

        Free, local, no API cost. Used for:
        - Internal link semantic similarity (replaces TF-IDF)
        - Duplicate content detection (replaces SimHash)
        - Keyword cluster grouping
        - Qdrant vector storage
        Returns empty list on failure.
        """
        result = _post(_OLLAMA_URL, "/api/embeddings", {
            "model": model,
            "prompt": text,
        }, timeout=30)
        vec = result.get("embedding", [])
        if vec:
            log.debug("aion.embed_ok  model=%s  dims=%d", model, len(vec))
        else:
            log.warning("aion.embed_fail  model=%s  result=%s", model, str(result)[:100])
        return vec

    def embed_batch(self, texts: list[str], model: str = "nomic-embed-text") -> list[list[float]]:
        """Embed multiple texts. Returns list of vectors (empty list on failure)."""
        return [self.embed(t, model) for t in texts]

    def cosine_similarity(self, vec_a: list[float], vec_b: list[float]) -> float:
        """Compute cosine similarity between two embedding vectors."""
        if not vec_a or not vec_b or len(vec_a) != len(vec_b):
            return 0.0
        dot = sum(a * b for a, b in zip(vec_a, vec_b))
        norm_a = sum(a * a for a in vec_a) ** 0.5
        norm_b = sum(b * b for b in vec_b) ** 0.5
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def ollama_complete(self, prompt: str, model: str = "qwen3:8b", system: str = "") -> str:
        """Call Ollama local LLM for cheap inference. Returns empty string on failure.

        Models available: qwen3:8b (fast), glm-4.7-flash (multilingual)
        Use for: bulk classification, template expansion, data enrichment.
        Free — no API cost, runs on VPS GPU/CPU.
        """
        payload: dict = {"model": model, "prompt": prompt, "stream": False}
        if system:
            payload["system"] = system
        result = _post(_OLLAMA_URL, "/api/generate", payload, timeout=120)
        text = result.get("response", "")
        if text:
            log.info("aion.ollama_ok  model=%s  chars=%d", model, len(text))
        return text

    # ------------------------------------------------------------------
    # Twitter Intel — market signals + opportunities
    # ------------------------------------------------------------------

    def twitter_signals(
        self,
        limit: int = 50,
        query: str | None = None,
    ) -> list[dict]:
        """Get market signals from Twitter Intel (4,192 signals collected).

        Returns list of {source, content, url, score, comments, query, author}
        Sources: hackernews_show, hackernews_ask, google_trends, twitter, reddit
        Use for: trending topic discovery, keyword seed generation, competitor mentions.
        """
        path = f"/api/v1/signals?limit={limit}"
        if query:
            path += f"&query={urllib.parse.quote(query)}"
        result = _get(_TWITTER_URL, path)
        signals = result.get("signals", []) if isinstance(result, dict) else []
        log.info("aion.twitter_signals  count=%d", len(signals))
        return signals

    def twitter_opportunities(self, limit: int = 50) -> list[dict]:
        """Get business opportunities found by Twitter Intel (299 found so far).

        Returns list of {problem, score, decision, status, created_at}
        Use for: content topic ideation, keyword gap discovery.
        """
        result = _get(_TWITTER_URL, "/api/v1/memory")
        if isinstance(result, dict):
            return result.get("opportunities", [])[:limit]
        return []

    # ------------------------------------------------------------------
    # GPT-Researcher — autonomous deep research
    # ------------------------------------------------------------------

    def gpt_research(
        self,
        query: str,
        report_type: str = "outline",
        timeout: int = 120,
    ) -> str:
        """Run autonomous deep research via GPT-Researcher (Cerebras/Qwen backend).

        report_type options: 'outline' | 'research_report' | 'resource_report'
        'outline' is fastest — returns structured topic outline.
        Use for: content brief enrichment, competitor gap analysis.
        Returns research text or empty string on failure.
        """
        result = _post(_GPT_RESEARCHER_URL, "/api/v1/research/sync", {
            "query": query,
            "report_type": report_type,
        }, timeout=timeout)
        if "detail" in result and "Set SMART_LLM" in str(result.get("detail", "")):
            log.warning("aion.gpt_researcher_config_error — backend misconfigured, using Brain fallback")
            return self.brain_complete(
                f"Research the following topic and provide a detailed outline: {query}",
                model="groq",
                max_tokens=2000,
            )
        text = result.get("report", "") or result.get("output", "") or result.get("result", "")
        if text:
            log.info("aion.gpt_research_ok  query=%s  chars=%d", query[:50], len(text))
        return text

    # ------------------------------------------------------------------
    # Listmonk — email campaigns and drip sequences
    # ------------------------------------------------------------------

    def _listmonk_headers(self) -> dict:
        """Build Basic Auth headers for Listmonk API."""
        creds = base64.b64encode(
            f"{_LISTMONK_USER}:{_LISTMONK_PASS}".encode()
        ).decode()
        return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}

    def _listmonk_post(self, path: str, data: dict, timeout: int = 15) -> dict:
        url = f"{_LISTMONK_URL}/api{path}"
        body = json.dumps(data).encode()
        headers = self._listmonk_headers()
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")[:300]
            log.warning("aion.listmonk_post_error  path=%s  code=%d  detail=%s", path, e.code, detail)
            return {"error": e.code, "detail": detail}
        except Exception as exc:
            log.warning("aion.listmonk_post_fail  path=%s  err=%s", path, exc)
            return {"error": str(exc)}

    def _listmonk_get(self, path: str, timeout: int = 10) -> Any:
        url = f"{_LISTMONK_URL}/api{path}"
        headers = self._listmonk_headers()
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except Exception as exc:
            log.warning("aion.listmonk_get_fail  path=%s  err=%s", path, exc)
            return {"error": str(exc)}

    def listmonk_get_lists(self) -> list[dict]:
        """Get all mailing lists in Listmonk."""
        result = self._listmonk_get("/lists")
        data = result.get("data", {})
        return data.get("results", []) if isinstance(data, dict) else []

    def listmonk_create_list(self, name: str, list_type: str = "private") -> dict:
        """Create a new mailing list. type: 'public' | 'private'"""
        return self._listmonk_post("/lists", {
            "name": name,
            "type": list_type,
            "optin": "single",
            "tags": ["seo-engine"],
        })

    def listmonk_add_subscriber(
        self,
        email: str,
        name: str = "",
        list_ids: list[int] | None = None,
        attribs: dict | None = None,
    ) -> dict:
        """Add a subscriber to Listmonk. Optionally add to specific list IDs."""
        payload: dict = {
            "email": email,
            "name": name or email.split("@")[0],
            "status": "enabled",
            "lists": list_ids or [],
            "attribs": attribs or {},
            "preconfirm_subscriptions": True,
        }
        return self._listmonk_post("/subscribers", payload)

    def listmonk_create_campaign(
        self,
        name: str,
        subject: str,
        body: str,
        list_ids: list[int],
        from_email: str = "aion@gethubed.com",
        campaign_type: str = "regular",
    ) -> dict:
        """Create an email campaign in Listmonk.

        campaign_type: 'regular' | 'optin'
        Returns campaign dict with id for sending.
        """
        return self._listmonk_post("/campaigns", {
            "name": name,
            "subject": subject,
            "lists": list_ids,
            "from_email": from_email,
            "body": body,
            "content_type": "richtext",
            "type": campaign_type,
            "tags": ["seo-engine"],
        })

    def listmonk_send_campaign(self, campaign_id: int) -> bool:
        """Start sending a Listmonk campaign. Returns True on success."""
        url = f"{_LISTMONK_URL}/api/campaigns/{campaign_id}/status"
        body = json.dumps({"status": "running"}).encode()
        headers = self._listmonk_headers()
        req = urllib.request.Request(url, data=body, headers=headers, method="PUT")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                result = json.loads(r.read())
                return result.get("data", {}).get("status") == "running"
        except Exception as exc:
            log.warning("aion.listmonk_send_fail  campaign=%d  err=%s", campaign_id, exc)
            return False

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def health(self) -> dict[str, bool]:
        """Check which AION services are reachable. Returns {name: bool}."""
        checks = {
            "brain":          (_BRAIN_URL,          "/health"),
            "memory":         (_MEMORY_URL,         "/health"),
            "knowledge":      (_KNOWLEDGE_URL,      "/health"),
            "youtube":        (_YOUTUBE_URL,        "/health"),
            "email":          (_EMAIL_URL,          "/health"),
            "outbound":       (_OUTBOUND_URL,       "/health"),
            "research":       (_RESEARCH_URL,       "/health"),
            "twitter_intel":  (_TWITTER_URL,        "/health"),
            "gpt_researcher": (_GPT_RESEARCHER_URL, "/health"),
            "ollama":         (_OLLAMA_URL,         "/api/tags"),
        }
        status = {
            name: "error" not in _get(base, path, timeout=5)
            for name, (base, path) in checks.items()
        }

        # Firecrawl has no /health — probe with a lightweight scrape
        try:
            result = _post(_FIRECRAWL_URL, "/v1/scrape",
                           {"url": "https://example.com", "formats": ["markdown"]},
                           timeout=10)
            status["firecrawl"] = bool(result.get("success"))
        except Exception:
            status["firecrawl"] = False

        # Listmonk — check with auth
        try:
            result = self._listmonk_get("/lists", timeout=5)
            status["listmonk"] = "error" not in result
        except Exception:
            status["listmonk"] = False

        return status


# Global singleton
aion = AIONBridge()
