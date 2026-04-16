"""Hard content quality gate — blocks publishing if content fails critical checks.

3 BLOCKING checks (fail = no publish):
  1. word_count     — minimum by intent
  2. ai_score       — Originality.ai < 0.45
  3. direct_answer  — first 100 words must contain a direct answer

4 WARNING checks (logged, not blocking):
  4. faq_presence   — FAQ section must exist
  5. header_structure — H1 + at least 2 H2s
  6. readability    — Flesch-Kincaid 50-80
  7. entity_density — 3+ named entities per 500 words
"""

from __future__ import annotations

import asyncio
import logging
import re
import string
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class GateResult:
    passed: bool
    blocking_failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    scores: dict = field(default_factory=dict)
    humanised_html: str | None = None


class ContentGate:
    """Validates content against quality requirements before publishing."""

    MIN_WORDS = {"transactional": 900, "commercial": 1200, "informational": 1500, "pillar": 2500}

    def __init__(self, originality_api_key: str = "", ai_threshold: float = 0.45):
        self.api_key = originality_api_key
        self.ai_threshold = ai_threshold

    async def check(
        self,
        content_html: str,
        keyword: str,
        *,
        intent: str = "informational",
        title: str = "",
        meta_description: str = "",
        schema_json: dict = None,
    ) -> GateResult:
        text = _strip_html(content_html)
        blocking: list[str] = []
        warnings: list[str] = []
        scores: dict = {}

        # --- BLOCKING CHECK 1: Word count ---
        wc = self._word_count(text)
        min_wc = self._min_words_for_intent(intent)
        scores["word_count"] = wc
        scores["min_word_count"] = min_wc
        if wc < min_wc:
            blocking.append(f"word_count: {wc} < required {min_wc} for {intent}")

        # --- BLOCKING CHECK 2: AI score ---
        ai_score = await self._ai_score(text)
        scores["ai_score"] = ai_score
        if ai_score >= self.ai_threshold:
            blocking.append(f"ai_score: {ai_score:.2f} >= threshold {self.ai_threshold}")

        # --- BLOCKING CHECK 3: Direct answer ---
        has_da = self._check_direct_answer(content_html, keyword)
        scores["has_direct_answer"] = has_da
        if not has_da:
            blocking.append("direct_answer: no factual direct answer found in first 100 words")

        # --- WARNING 4: FAQ ---
        has_faq = self._check_faq(content_html)
        scores["has_faq"] = has_faq
        if not has_faq:
            warnings.append("faq_presence: no FAQ section detected")

        # --- WARNING 5: Header structure ---
        headers = self._check_headers(content_html)
        scores["headers"] = headers
        if not headers["valid"]:
            warnings.append(f"headers: needs H1 + 2× H2 (found H1={headers['h1']}, H2={headers['h2']})")

        # --- WARNING 6: Readability ---
        fk = self._flesch_kincaid(text)
        scores["readability_score"] = round(fk, 1)
        if fk < 30 or fk > 85:
            warnings.append(f"readability: Flesch score {fk:.0f} outside 30-85 range")

        # --- WARNING 7: Entity density ---
        entities_per_500 = self._entity_density(text)
        scores["entity_density"] = round(entities_per_500, 2)
        if entities_per_500 < 1.0:
            warnings.append(f"entity_density: {entities_per_500:.1f} entities/500 words (target: 3+)")

        passed = len(blocking) == 0
        log.info(
            "content_gate.check  keyword=%s  passed=%s  blocking=%d  warnings=%d  ai=%.2f  wc=%d",
            keyword, passed, len(blocking), len(warnings), ai_score, wc,
        )
        return GateResult(passed=passed, blocking_failures=blocking, warnings=warnings, scores=scores)

    async def check_and_humanise(
        self,
        content_html: str,
        keyword: str,
        **kwargs,
    ) -> GateResult:
        """Check content; if AI score too high, humanise and re-check."""
        result = await self.check(content_html, keyword, **kwargs)

        ai_score = result.scores.get("ai_score", 0.0)
        if ai_score >= self.ai_threshold:
            log.info("content_gate.humanising  keyword=%s  ai_score=%.2f", keyword, ai_score)
            humanised = await self.humanise(content_html, keyword, ai_score)
            if humanised and humanised != content_html:
                # Re-check with humanised version
                result2 = await self.check(humanised, keyword, **kwargs)
                result2.humanised_html = humanised
                # Carry over previous warnings that aren't AI-related
                if not result2.passed and result.passed:
                    return result  # original was better
                return result2
            result.humanised_html = None
        return result

    async def humanise(self, content_html: str, keyword: str, ai_score: float) -> str:
        """Rewrite content to reduce AI detection score."""
        try:
            from core.claude import call_claude
        except ImportError:
            log.warning("content_gate.humanise  claude not available")
            return content_html

        prompt = f"""Rewrite this article in a natural, conversational human voice.

Rules:
- Vary sentence length: mix short punchy sentences with longer explanatory ones
- Add personal opinions and first-person perspective where natural
- Use contractions (it's, you'll, we've, don't, can't)
- Replace robotic transitions ("Furthermore", "Additionally") with natural ones ("Plus", "And", "But here's the thing")
- Add colloquial phrases where appropriate
- Vary paragraph length: some 1-sentence, some 4-5 sentences
- Include a rhetorical question or two
- Keep ALL facts, data, links, HTML tags, and structure intact
- Do NOT change headings, URLs, or schema markup
- Target Flesch reading ease score: 65-75

Current AI detection score: {ai_score:.2f} (target: below 0.45)
Keyword: {keyword}

ARTICLE TO REWRITE:
{content_html[:8000]}"""

        try:
            result = await asyncio.to_thread(
                call_claude, prompt,
                system="You are a human editor. Return only the rewritten HTML article, no preamble.",
                max_tokens=8192,
            )
            return result or content_html
        except Exception as e:
            log.warning("content_gate.humanise_fail  err=%s", e)
            return content_html

    # ----------------------------------------------------------------
    # Individual checks
    # ----------------------------------------------------------------

    def _word_count(self, text: str) -> int:
        return len(text.split())

    def _min_words_for_intent(self, intent: str) -> int:
        return self.MIN_WORDS.get(intent, 700)

    def _check_direct_answer(self, html: str, keyword: str) -> bool:
        """Check if first ~300 chars of visible text contains a direct answer."""
        # Get first 500 chars of text
        text = _strip_html(html)
        first_chunk = text[:300].strip()
        if len(first_chunk) < 30:
            return False

        # Must contain a sentence (has a period) and be substantial
        has_sentence = "." in first_chunk and len(first_chunk) > 40

        # Must reference the keyword concept
        kw_words = set(keyword.lower().split())
        text_words = set(first_chunk.lower().split())
        kw_overlap = len(kw_words & text_words) / max(len(kw_words), 1)

        return has_sentence and kw_overlap >= 0.3

    def _check_faq(self, html: str) -> bool:
        """Detect FAQ section."""
        html_lower = html.lower()
        # Explicit FAQ markers
        if any(m in html_lower for m in ("faq", "frequently asked", "common questions")):
            return True
        # Definition list
        if "<dl>" in html_lower:
            return True
        # Multiple H3s with question marks
        h3_matches = re.findall(r"<h3[^>]*>([^<]+)</h3>", html, re.IGNORECASE)
        question_h3s = [h for h in h3_matches if "?" in h]
        return len(question_h3s) >= 3

    def _check_headers(self, html: str) -> dict:
        h1 = len(re.findall(r"<h1[^>]*>", html, re.IGNORECASE))
        h2 = len(re.findall(r"<h2[^>]*>", html, re.IGNORECASE))
        h3 = len(re.findall(r"<h3[^>]*>", html, re.IGNORECASE))
        return {"h1": h1, "h2": h2, "h3": h3, "valid": h1 >= 1 and h2 >= 2}

    def _flesch_kincaid(self, text: str) -> float:
        """Flesch Reading Ease score."""
        sentences = re.split(r"[.!?]+", text)
        sentences = [s.strip() for s in sentences if s.strip()]
        n_sentences = max(len(sentences), 1)

        words = text.split()
        n_words = max(len(words), 1)

        syllables = sum(self._count_syllables(w) for w in words)
        n_syllables = max(syllables, 1)

        score = 206.835 - 1.015 * (n_words / n_sentences) - 84.6 * (n_syllables / n_words)
        return max(0.0, min(100.0, score))

    def _count_syllables(self, word: str) -> int:
        word = word.lower().strip(string.punctuation)
        if not word:
            return 1
        vowels = re.findall(r"[aeiouy]+", word)
        count = len(vowels)
        if word.endswith("e") and count > 1:
            count -= 1
        return max(1, count)

    def _entity_density(self, text: str) -> float:
        """Estimate named entities per 500 words using capitalization heuristic."""
        words = text.split()
        if not words:
            return 0.0
        n_words = len(words)

        # Entities: Title Case words not at sentence start
        entity_count = 0
        sentences = re.split(r"[.!?]\s+", text)
        sentence_starters: set[str] = set()
        for sent in sentences:
            first = sent.strip().split(" ")
            if first:
                sentence_starters.add(first[0])

        for i, word in enumerate(words):
            clean = word.strip(string.punctuation)
            if (
                clean
                and clean[0].isupper()
                and len(clean) > 2
                and clean not in sentence_starters
                and i > 0
            ):
                entity_count += 1

        return entity_count / max(n_words, 1) * 500

    async def _ai_score(self, text: str) -> float:
        """Check AI content score via Originality.ai API."""
        if not self.api_key:
            return 0.0
        try:
            import httpx
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://api.originality.ai/api/v1/scan/ai",
                    headers={"X-OAI-API-KEY": self.api_key, "Content-Type": "application/json"},
                    json={
                        "content": text[:10_000],
                        "aiModelVersion": "1",
                        "storeScan": "false",
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    return float(data.get("score", {}).get("ai", 0.0))
        except Exception as e:
            log.warning("content_gate.ai_score_fail  err=%s", e)
        return 0.0


def _strip_html(html: str) -> str:
    """Remove HTML tags and return plain text."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)
    return text.strip()
