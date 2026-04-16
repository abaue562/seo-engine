"""Semantic similarity-based internal linking.

PRIMARY: Ollama nomic-embed-text (768-dim real embeddings, free, local)
FALLBACK: TF-IDF cosine similarity (stdlib only, no ML dependencies)

The embedding-based approach produces dramatically better link suggestions
because nomic-embed-text understands semantic meaning, not just term overlap.

Key features:
- nomic-embed-text embeddings via Ollama (primary, free, local)
- TF-IDF cosine similarity fallback (if Ollama unavailable)
- Pillar page detection and enforcement
- Orphan page detection
- Authority flow prioritization (more links flow to pillar pages)
"""

from __future__ import annotations

import logging
import math
import random
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data.db import SEODatabase

log = logging.getLogger(__name__)

# Check if Ollama embeddings are available at startup
_OLLAMA_AVAILABLE: bool | None = None  # None = not yet checked


def _check_ollama() -> bool:
    global _OLLAMA_AVAILABLE
    if _OLLAMA_AVAILABLE is None:
        try:
            from core.aion_bridge import aion
            vec = aion.embed("test")
            _OLLAMA_AVAILABLE = len(vec) > 0
        except Exception:
            _OLLAMA_AVAILABLE = False
        log.info("semantic_linker.ollama_available=%s", _OLLAMA_AVAILABLE)
    return _OLLAMA_AVAILABLE

_STOPWORDS = {
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "has", "have", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "that", "this", "these", "those", "it", "its",
    "we", "you", "he", "she", "they", "our", "your", "their", "my", "his",
    "her", "how", "what", "when", "where", "why", "who", "which", "can",
    "not", "no", "so", "if", "as", "up", "out", "into", "than", "then",
    "also", "all", "any", "each", "more", "most", "other", "some", "such",
}


@dataclass
class PageNode:
    url: str
    slug: str
    keyword: str
    title: str
    content_snippet: str = ""  # first 200 chars of body text
    page_type: str = "cluster"  # pillar | cluster | service | faq
    word_count: int = 0
    inbound_link_count: int = 0
    outbound_link_count: int = 0
    business_id: str = ""


@dataclass
class LinkRecommendation:
    source_url: str
    target_url: str
    source_slug: str
    target_slug: str
    anchor_text: str
    similarity_score: float
    reason: str
    priority: int  # 1-10


class SemanticLinker:
    """Embedding-based internal link recommender with pillar enforcement.

    Uses Ollama nomic-embed-text when available (recommended), falls back
    to TF-IDF for environments without Ollama.
    """

    def __init__(self, db: "SEODatabase" = None):
        self.db = db

    # ----------------------------------------------------------------
    # Embedding-based similarity (primary — nomic-embed-text)
    # ----------------------------------------------------------------

    def _embed_texts(self, texts: list[str]) -> list[list[float]] | None:
        """Get embeddings for a list of texts. Returns None if Ollama unavailable."""
        if not _check_ollama():
            return None
        try:
            from core.aion_bridge import aion
            vecs = [aion.embed(t) for t in texts]
            if any(len(v) == 0 for v in vecs):
                return None
            return vecs
        except Exception as e:
            log.warning("semantic_linker.embed_fail  err=%s", e)
            return None

    def _vec_cosine(self, a: list[float], b: list[float]) -> float:
        """Cosine similarity between two float vectors."""
        dot = sum(x * y for x, y in zip(a, b))
        na = sum(x * x for x in a) ** 0.5
        nb = sum(x * x for x in b) ** 0.5
        return dot / (na * nb) if na and nb else 0.0

    # ----------------------------------------------------------------
    # TF-IDF fallback
    # ----------------------------------------------------------------

    def _tokenize(self, text: str) -> list[str]:
        text = text.lower()
        tokens = re.findall(r"[a-z0-9]+", text)
        return [t for t in tokens if len(t) >= 3 and t not in _STOPWORDS]

    def _compute_tfidf(self, documents: list[str]) -> list[dict[str, float]]:
        """Compute TF-IDF vectors for a list of documents."""
        tokenized = [self._tokenize(doc) for doc in documents]
        N = len(documents)

        # Document frequency
        df: dict[str, int] = {}
        for tokens in tokenized:
            for term in set(tokens):
                df[term] = df.get(term, 0) + 1

        # TF-IDF per document
        vectors: list[dict[str, float]] = []
        for tokens in tokenized:
            total = len(tokens) or 1
            tf: dict[str, float] = {}
            for token in tokens:
                tf[token] = tf.get(token, 0) + 1 / total

            tfidf: dict[str, float] = {}
            for term, freq in tf.items():
                idf = math.log(N / (1 + df.get(term, 0)))
                tfidf[term] = freq * idf

            vectors.append(tfidf)
        return vectors

    def _cosine_similarity(self, vec1: dict, vec2: dict) -> float:
        """Cosine similarity between two TF-IDF vectors."""
        if not vec1 or not vec2:
            return 0.0
        dot = sum(vec1.get(t, 0) * vec2.get(t, 0) for t in vec1)
        norm1 = math.sqrt(sum(v * v for v in vec1.values()))
        norm2 = math.sqrt(sum(v * v for v in vec2.values()))
        if norm1 == 0 or norm2 == 0:
            return 0.0
        return dot / (norm1 * norm2)

    # ----------------------------------------------------------------
    # Link graph building
    # ----------------------------------------------------------------

    def build_link_graph(self, pages: list[PageNode]) -> list[LinkRecommendation]:
        """Compute similarity between all page pairs and recommend links.

        Uses nomic-embed-text embeddings (primary) or TF-IDF (fallback).
        Embedding-based similarity is dramatically more accurate for semantic linking.
        """
        if len(pages) < 2:
            return []

        texts = [f"{p.keyword} {p.title} {p.content_snippet}" for p in pages]

        # Try embedding-based approach first
        embed_vecs = self._embed_texts(texts)
        using_embeddings = embed_vecs is not None

        if using_embeddings:
            log.info("semantic_linker.using_embeddings  pages=%d  dims=%d",
                     len(pages), len(embed_vecs[0]) if embed_vecs else 0)
        else:
            log.info("semantic_linker.using_tfidf  pages=%d", len(pages))
            tfidf_vecs = self._compute_tfidf(texts)

        # Similarity threshold — embeddings use 0.65+, TF-IDF uses 0.25+
        sim_threshold = 0.65 if using_embeddings else 0.25

        recommendations: list[LinkRecommendation] = []

        for i, page_a in enumerate(pages):
            page_recs: list[LinkRecommendation] = []
            for j, page_b in enumerate(pages):
                if i == j:
                    continue
                if page_a.url == page_b.url:
                    continue

                if using_embeddings:
                    sim = self._vec_cosine(embed_vecs[i], embed_vecs[j])
                else:
                    sim = self._cosine_similarity(tfidf_vecs[i], tfidf_vecs[j])

                if sim < sim_threshold:
                    continue

                # Determine priority
                if page_a.page_type == "pillar" and page_b.page_type == "cluster":
                    priority = 10
                    reason = "pillar → cluster enforcement"
                elif page_a.page_type == "cluster" and page_b.page_type == "pillar":
                    priority = 8
                    reason = "cluster → pillar (authority flow)"
                elif page_a.page_type == "cluster" and page_b.page_type == "cluster":
                    priority = 5
                    reason = f"cluster cross-link (similarity {sim:.2f})"
                elif page_b.page_type == "service":
                    priority = 7
                    reason = "link to service page"
                else:
                    priority = 4
                    reason = f"semantic similarity {sim:.2f}"

                anchor = self.anchor_from_keyword(
                    page_b.keyword,
                    page_b.page_type,
                )
                page_recs.append(
                    LinkRecommendation(
                        source_url=page_a.url,
                        target_url=page_b.url,
                        source_slug=page_a.slug,
                        target_slug=page_b.slug,
                        anchor_text=anchor,
                        similarity_score=round(sim, 3),
                        reason=reason,
                        priority=priority,
                    )
                )

            # Keep top 5 per page, sorted by priority then similarity
            page_recs.sort(key=lambda r: (r.priority, r.similarity_score), reverse=True)
            recommendations.extend(page_recs[:5])

        return recommendations

    def detect_orphans(self, pages: list[PageNode]) -> list[PageNode]:
        """Return pages with zero inbound links."""
        return [p for p in pages if p.inbound_link_count == 0]

    def enforce_pillar_links(self, pages: list[PageNode]) -> list[LinkRecommendation]:
        """Ensure every cluster links to its pillar and pillar links to clusters."""
        pillars = [p for p in pages if p.page_type == "pillar" or p.word_count > 2000]
        clusters = [p for p in pages if p not in pillars]
        missing: list[LinkRecommendation] = []

        for cluster in clusters:
            # Find best matching pillar by keyword overlap
            best_pillar = None
            best_score = 0.0
            cv = self._compute_tfidf([cluster.keyword])[0]
            for pillar in pillars:
                pv = self._compute_tfidf([pillar.keyword])[0]
                score = self._cosine_similarity(cv, pv)
                if score > best_score:
                    best_score = score
                    best_pillar = pillar

            if best_pillar and best_score > 0.1:
                missing.append(
                    LinkRecommendation(
                        source_url=cluster.url,
                        target_url=best_pillar.url,
                        source_slug=cluster.slug,
                        target_slug=best_pillar.slug,
                        anchor_text=self.anchor_from_keyword(best_pillar.keyword, "pillar"),
                        similarity_score=best_score,
                        reason="missing cluster→pillar link",
                        priority=10,
                    )
                )

        # Pillar → all clusters
        for pillar in pillars:
            for cluster in clusters:
                missing.append(
                    LinkRecommendation(
                        source_url=pillar.url,
                        target_url=cluster.url,
                        source_slug=pillar.slug,
                        target_slug=cluster.slug,
                        anchor_text=self.anchor_from_keyword(cluster.keyword, "cluster"),
                        similarity_score=0.0,
                        reason="pillar→cluster enforcement",
                        priority=10,
                    )
                )

        return missing

    def get_authority_flow_plan(self, pages: list[PageNode]) -> dict:
        """Return a report on authority flow and linking health."""
        pillars = [p for p in pages if p.page_type == "pillar" or p.word_count > 2000]
        orphans = self.detect_orphans(pages)
        cluster_count = len([p for p in pages if p.page_type == "cluster"])
        pillar_count = len(pillars)
        coverage = pillar_count / max(len(pages), 1)

        money_pages = sorted(pages, key=lambda p: p.inbound_link_count, reverse=True)[:5]
        return {
            "total_pages": len(pages),
            "pillar_count": pillar_count,
            "cluster_count": cluster_count,
            "orphan_count": len(orphans),
            "orphan_urls": [o.url for o in orphans[:10]],
            "pillar_coverage": round(coverage, 3),
            "money_pages": [p.url for p in money_pages],
            "recommended_link_targets": [p.url for p in pillars],
        }

    def anchor_from_keyword(self, keyword: str, page_type: str, brand_name: str = "") -> str:
        """Generate appropriate anchor text for a link."""
        r = random.random()
        words = keyword.split()

        if r < 0.40:  # 40% full keyword
            return keyword

        if r < 0.75 and len(words) > 1:  # 35% partial
            return " ".join(words[: max(1, len(words) - 1)])

        if r < 0.90:  # 15% exact (same as full for now)
            return keyword

        # 10% generic
        generics = ["learn more", "read our guide", "see details", "find out more", "click here"]
        return random.choice(generics)
