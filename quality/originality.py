"""
Content originality and AI-detection checking via Originality.ai API.
Docs: https://originality.ai/api-documentation

Falls back to compression-ratio method if API key not set.

Usage:
    checker = OriginalityChecker()
    result = checker.check(content="...")
    # result: {ai_score: 0.0-1.0, original_score: 0.0-1.0, verdict: 'original'|'ai'|'mixed', method: 'api'|'heuristic'}
"""
import os
import hashlib
import lzma
import zlib
import logging
from typing import Optional

import httpx

log = logging.getLogger(__name__)


class OriginalityChecker:
    API_URL = "https://api.originality.ai/api/v1/scan/ai"

    def __init__(self):
        self.api_key   = os.getenv("ORIGINALITY_API_KEY", "")
        self.threshold = float(os.getenv("AI_SCORE_THRESHOLD", "0.8"))
        self.client    = httpx.Client(timeout=30)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check(self, content: str, title: str = "") -> dict:
        """Check content for AI generation.

        Uses Originality.ai API when a key is set; falls back to a
        compression-ratio heuristic otherwise.

        Returns:
            {
                ai_score:       float,   # 0.0 = human, 1.0 = AI
                original_score: float,   # 1.0 - ai_score
                verdict:        str,     # "original" | "ai" | "mixed"
                method:         str,     # "api" | "heuristic"
                flagged:        bool,    # True if ai_score >= threshold
            }
        """
        if self.api_key:
            try:
                return self._check_via_api(content, title)
            except httpx.HTTPStatusError as exc:
                log.warning("originality.api_http_error  status=%d  fallback=heuristic",
                            exc.response.status_code)
            except httpx.RequestError as exc:
                log.warning("originality.api_request_error  err=%s  fallback=heuristic", exc)

        return self._check_heuristic(content)

    def check_batch(self, contents: list[str]) -> list[dict]:
        """Check multiple pieces of content. Returns a list of result dicts
        in the same order as the input list."""
        return [self.check(c) for c in contents]

    def is_acceptable(self, content: str) -> bool:
        """Quick gate — returns True when the content passes the originality threshold
        (i.e. ai_score is below the configured threshold)."""
        result = self.check(content)
        return not result.get("flagged", False)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _check_via_api(self, content: str, title: str) -> dict:
        """POST to Originality.ai API and normalise the response.

        Request:
            POST /api/v1/scan/ai
            X-OAI-API-KEY: <key>
            {"content": <str>, "title": <str>, "aiModelVersion": "1"}

        Response:
            {"score": {"ai": 0.99, "original": 0.01}, "credits_used": 1}
        """
        resp = self.client.post(
            self.API_URL,
            headers={"X-OAI-API-KEY": self.api_key, "Content-Type": "application/json"},
            json={"content": content, "title": title, "aiModelVersion": "1"},
        )
        resp.raise_for_status()
        data = resp.json()

        score = data.get("score", {})
        ai_score       = float(score.get("ai", 0.0))
        original_score = float(score.get("original", 1.0 - ai_score))

        verdict = self._verdict(ai_score)
        flagged = ai_score >= self.threshold

        log.info("originality.api  ai_score=%.2f  verdict=%s  flagged=%s",
                 ai_score, verdict, flagged)

        return {
            "ai_score":       ai_score,
            "original_score": original_score,
            "verdict":        verdict,
            "method":         "api",
            "flagged":        flagged,
        }

    def _check_heuristic(self, content: str) -> dict:
        """Compression-ratio heuristic for AI detection (no API key required).

        AI text tends to be highly structured and repetitive, making it more
        compressible than authentic human writing.

        Strategy:
          - Compress with both lzma and zlib; average the ratios.
          - ratio = compressed_size / original_size
          - ratio < 0.45  →  highly compressible  →  likely AI
          - ratio > 0.65  →  less compressible     →  likely human
          - middle zone   →  mixed / uncertain

        The resulting ai_score is mapped from the ratio so that callers get
        the same dict shape as from the real API.
        """
        if not content:
            return {
                "ai_score": 0.0, "original_score": 1.0,
                "verdict": "original", "method": "heuristic", "flagged": False,
            }

        raw = content.encode("utf-8")
        original_size = len(raw)
        if original_size == 0:
            return {
                "ai_score": 0.0, "original_score": 1.0,
                "verdict": "original", "method": "heuristic", "flagged": False,
            }

        lzma_size  = len(lzma.compress(raw, preset=6))
        zlib_size  = len(zlib.compress(raw, level=6))
        ratio      = ((lzma_size / original_size) + (zlib_size / original_size)) / 2

        # Map compression ratio → ai_score
        # Lower ratio = more compressible = more AI-like
        # Calibrated range: ratio 0.20 → ai=1.0, ratio 0.70 → ai=0.0
        LOW, HIGH = 0.20, 0.70
        ai_score = max(0.0, min(1.0, (HIGH - ratio) / (HIGH - LOW)))

        original_score = 1.0 - ai_score
        verdict = self._verdict(ai_score)
        flagged = ai_score >= self.threshold

        log.debug("originality.heuristic  ratio=%.3f  ai_score=%.2f  verdict=%s",
                  ratio, ai_score, verdict)

        return {
            "ai_score":       round(ai_score, 4),
            "original_score": round(original_score, 4),
            "verdict":        verdict,
            "method":         "heuristic",
            "flagged":        flagged,
        }

    def _verdict(self, ai_score: float) -> str:
        """Classify score into a human-readable verdict."""
        if ai_score >= 0.80:
            return "ai"
        if ai_score >= 0.40:
            return "mixed"
        return "original"
