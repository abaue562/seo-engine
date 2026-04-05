"""AI Content Detector — detects AI-generated text using compression ratios.

Based on thinkst/zippy (MIT license, <200 lines, 50x faster than RoBERTa).
No ML model needed — pure algorithmic detection.

Core insight: AI text compresses better with other AI text because LLMs
produce statistically similar patterns. Human text is more varied.

Usage:
    from ai_visibility.ai_detector import detect_ai_content, AiDetectionResult

    result = detect_ai_content("Some text to check")
    print(f"{result.verdict}: {result.confidence:.1%} confidence")
    # "AI: 34.2% confidence" or "Human: 15.6% confidence"
"""

from __future__ import annotations

import lzma
import zlib
import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)

# Known AI-generated text corpus (seed for compression comparison)
# This is a sample — for production, use a larger corpus
AI_PRELUDE = """Artificial intelligence has transformed the way we interact with technology.
Machine learning algorithms can process vast amounts of data to identify patterns
and make predictions. Natural language processing enables computers to understand
and generate human language with remarkable accuracy. Deep learning neural networks
have achieved breakthroughs in image recognition, speech synthesis, and text generation.
The rapid advancement of AI technology continues to reshape industries from healthcare
to finance, creating new opportunities while also raising important ethical questions.
As these systems become more sophisticated, the distinction between human and machine
generated content becomes increasingly difficult to determine. Large language models
trained on massive datasets can produce coherent, contextually appropriate text that
closely mirrors human writing patterns. The implications of this technology extend
far beyond simple text generation, encompassing creative writing, code generation,
scientific research, and many other domains. Understanding how to detect and
differentiate AI-generated content from human-written text has become a critical
challenge in the modern information landscape."""


@dataclass
class AiDetectionResult:
    verdict: str          # "AI" or "Human"
    confidence: float     # 0.0 to 1.0
    method: str           # "lzma", "zlib", or "ensemble"
    details: dict = None


def _compression_ratio(data: bytes, method: str = "lzma") -> float:
    """Calculate compression ratio for given bytes."""
    if method == "lzma":
        compressed = lzma.compress(data, preset=4)
    elif method == "zlib":
        compressed = zlib.compress(data, level=6)
    else:
        raise ValueError(f"Unknown method: {method}")

    return len(compressed) / len(data)


def _score_text(sample: str, prelude: str = AI_PRELUDE, method: str = "lzma") -> tuple[str, float]:
    """Score a text sample against the AI prelude.

    Returns (verdict, confidence) where:
        - verdict is "AI" or "Human"
        - confidence is a float (higher = more certain)
    """
    prelude_bytes = prelude.encode("utf-8")
    sample_bytes = sample.encode("utf-8")

    # Compression ratio of prelude alone
    prelude_ratio = _compression_ratio(prelude_bytes, method)

    # Compression ratio of prelude + sample together
    combined_ratio = _compression_ratio(prelude_bytes + sample_bytes, method)

    # If combined compresses BETTER (lower ratio) than prelude alone,
    # the sample is similar to AI text → AI detected
    delta = prelude_ratio - combined_ratio

    if delta > 0:
        return ("AI", abs(delta * 100))
    else:
        return ("Human", abs(delta * 100))


def _score_chunked(sample: str, chunk_size: int = 1500, method: str = "lzma") -> tuple[str, float]:
    """Score long text by chunking and averaging."""
    if len(sample) <= chunk_size:
        return _score_text(sample, method=method)

    chunks = [sample[i:i + chunk_size] for i in range(0, len(sample), chunk_size)]
    ai_scores = []
    human_scores = []

    for chunk in chunks:
        if len(chunk.strip()) < 50:
            continue
        verdict, confidence = _score_text(chunk, method=method)
        if verdict == "AI":
            ai_scores.append(confidence)
        else:
            human_scores.append(confidence)

    total = len(ai_scores) + len(human_scores)
    if total == 0:
        return ("Human", 0.0)

    ai_avg = sum(ai_scores) / len(ai_scores) if ai_scores else 0
    human_avg = sum(human_scores) / len(human_scores) if human_scores else 0

    if len(ai_scores) > len(human_scores):
        return ("AI", ai_avg)
    elif len(human_scores) > len(ai_scores):
        return ("Human", human_avg)
    else:
        # Tie — go by confidence
        return ("AI", ai_avg) if ai_avg > human_avg else ("Human", human_avg)


def detect_ai_content(text: str, method: str = "ensemble") -> AiDetectionResult:
    """Detect whether text is AI-generated or human-written.

    Args:
        text: Text to analyze
        method: "lzma", "zlib", or "ensemble" (default)

    Returns:
        AiDetectionResult with verdict, confidence, and details
    """
    if len(text.strip()) < 50:
        return AiDetectionResult(verdict="Human", confidence=0.0, method=method,
                                  details={"note": "Text too short for reliable detection"})

    if method == "ensemble":
        # Run both methods and combine
        lzma_verdict, lzma_conf = _score_chunked(text, method="lzma")
        zlib_verdict, zlib_conf = _score_chunked(text, method="zlib")

        # Weighted average (LZMA is more reliable)
        lzma_weight = 0.6
        zlib_weight = 0.4

        lzma_score = lzma_conf if lzma_verdict == "AI" else -lzma_conf
        zlib_score = zlib_conf if zlib_verdict == "AI" else -zlib_conf

        combined = lzma_score * lzma_weight + zlib_score * zlib_weight

        verdict = "AI" if combined > 0 else "Human"
        confidence = abs(combined)

        return AiDetectionResult(
            verdict=verdict,
            confidence=min(confidence, 1.0),
            method="ensemble",
            details={
                "lzma": {"verdict": lzma_verdict, "confidence": lzma_conf},
                "zlib": {"verdict": zlib_verdict, "confidence": zlib_conf},
                "combined_score": combined,
            },
        )
    else:
        verdict, confidence = _score_chunked(text, method=method)
        return AiDetectionResult(
            verdict=verdict,
            confidence=min(confidence, 1.0),
            method=method,
        )


def ensure_human_like(text: str, threshold: float = 0.5) -> dict:
    """Check if content passes as human-written.

    Returns dict with passes (bool), score, and recommendation.
    """
    result = detect_ai_content(text)

    passes = result.verdict == "Human" or result.confidence < threshold

    recommendation = ""
    if not passes:
        recommendation = (
            "Content detected as AI-generated. To improve: "
            "vary sentence length, add personal anecdotes, "
            "use colloquial language, include specific local references, "
            "and break predictable patterns."
        )

    return {
        "passes": passes,
        "verdict": result.verdict,
        "confidence": result.confidence,
        "recommendation": recommendation,
    }
