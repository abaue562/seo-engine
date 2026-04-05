"""Semantic Internal Linker — BERT-based internal linking recommendations.

Uses SentenceTransformers to encode page content into embeddings,
then finds semantically similar pages that should link to each other.

This is our own implementation inspired by BERTlinker's approach
(which is closed-source SaaS), built on the same open-source primitives:
PolyFuzz + SentenceTransformers.

Usage:
    from core.semantic_linker import SemanticLinker

    linker = SemanticLinker()
    pages = [
        {"url": "/permanent-lights", "title": "Permanent Lights Kelowna", "content": "..."},
        {"url": "/landscape-lighting", "title": "Landscape Lighting Kelowna", "content": "..."},
    ]
    suggestions = linker.suggest_links(pages, min_similarity=0.4)
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class LinkSuggestion:
    source_url: str
    source_title: str
    target_url: str
    target_title: str
    anchor_text: str
    similarity: float
    reason: str


class SemanticLinker:
    """Generate internal linking suggestions using sentence embeddings."""

    def __init__(self, model_name: str = "paraphrase-MiniLM-L3-v2"):
        self.model_name = model_name
        self._model = None

    def _load_model(self):
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._model = SentenceTransformer(self.model_name, device="cpu")
                log.info("semantic_linker.model_loaded  model=%s", self.model_name)
            except ImportError:
                log.error("semantic_linker.missing_dep  pip install sentence-transformers")
                raise
        return self._model

    def suggest_links(
        self,
        pages: list[dict],
        min_similarity: float = 0.4,
        max_suggestions_per_page: int = 5,
    ) -> list[LinkSuggestion]:
        """Find pages that should link to each other based on semantic similarity.

        Args:
            pages: List of dicts with keys: url, title, content (or h1, description)
            min_similarity: Minimum cosine similarity threshold (0-1)
            max_suggestions_per_page: Max outgoing link suggestions per page

        Returns:
            List of LinkSuggestion objects sorted by similarity (highest first)
        """
        if len(pages) < 2:
            return []

        model = self._load_model()

        # Build text representations for each page
        texts = []
        for page in pages:
            text = f"{page.get('title', '')} {page.get('h1', '')} {page.get('content', page.get('description', ''))}"
            texts.append(text[:512])  # Limit to 512 chars for efficiency

        # Encode all pages
        embeddings = model.encode(texts, show_progress_bar=False)

        # Compute pairwise similarity
        from sklearn.metrics.pairwise import cosine_similarity
        sim_matrix = cosine_similarity(embeddings)

        suggestions = []
        for i, source in enumerate(pages):
            page_suggestions = []
            for j, target in enumerate(pages):
                if i == j:
                    continue
                sim = float(sim_matrix[i][j])
                if sim < min_similarity:
                    continue

                # Find best anchor text (keyword overlap)
                anchor = self._find_anchor_text(source, target)

                page_suggestions.append(LinkSuggestion(
                    source_url=source["url"],
                    source_title=source.get("title", ""),
                    target_url=target["url"],
                    target_title=target.get("title", ""),
                    anchor_text=anchor,
                    similarity=round(sim, 3),
                    reason=f"Semantic similarity: {sim:.0%}",
                ))

            # Keep top N per page
            page_suggestions.sort(key=lambda x: x.similarity, reverse=True)
            suggestions.extend(page_suggestions[:max_suggestions_per_page])

        suggestions.sort(key=lambda x: x.similarity, reverse=True)
        log.info("semantic_linker.suggestions  pages=%d  links=%d", len(pages), len(suggestions))
        return suggestions

    def suggest_links_fuzzy(
        self,
        pages: list[dict],
        keywords: list[dict] | None = None,
        min_similarity: float = 0.5,
    ) -> list[LinkSuggestion]:
        """Use PolyFuzz for fuzzy matching between page titles/H1s.

        Faster than full content embedding — matches title-to-title.
        Optionally matches keywords to pages for anchor text suggestions.
        """
        try:
            from polyfuzz import PolyFuzz
            from polyfuzz.models import SentenceEmbeddings
            from sentence_transformers import SentenceTransformer
        except ImportError:
            log.error("semantic_linker.missing_dep  pip install polyfuzz sentence-transformers")
            return []

        model = SentenceTransformer(self.model_name, device="cpu")
        distance_model = SentenceEmbeddings(model)
        matcher = PolyFuzz(distance_model)

        # Match page titles to each other
        titles = [p.get("title", p.get("h1", p["url"])) for p in pages]
        matcher.fit(titles)
        matcher.group(link_min_similarity=min_similarity)

        matches = matcher.get_matches()
        suggestions = []

        for _, row in matches.iterrows():
            source_title = row["From"]
            target_title = row["To"]
            sim = row["Similarity"]

            if source_title == target_title or sim < min_similarity:
                continue

            source = next((p for p in pages if p.get("title", "") == source_title), None)
            target = next((p for p in pages if p.get("title", "") == target_title), None)

            if source and target:
                suggestions.append(LinkSuggestion(
                    source_url=source["url"],
                    source_title=source_title,
                    target_url=target["url"],
                    target_title=target_title,
                    anchor_text=target_title,
                    similarity=round(sim, 3),
                    reason=f"Fuzzy title match: {sim:.0%}",
                ))

        suggestions.sort(key=lambda x: x.similarity, reverse=True)
        return suggestions

    @staticmethod
    def _find_anchor_text(source: dict, target: dict) -> str:
        """Find the best anchor text for linking source → target."""
        target_title = target.get("title", "")
        # Use target title as default anchor
        if target_title:
            # Shorten if too long
            words = target_title.split()
            if len(words) > 6:
                return " ".join(words[:6])
            return target_title
        return target.get("url", "").split("/")[-1].replace("-", " ").title()
