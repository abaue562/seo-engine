import hashlib, json, logging
from dataclasses import dataclass
from typing import Optional
import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)

JUDGE_RUBRIC = ["specificity", "accuracy", "depth", "originality", "intent_match", "eeat"]
PASS_THRESHOLD = 3  # any dimension below this → reject

@dataclass
class JudgeResult:
    passed: bool
    scores: dict  # {dimension: int 1-5}
    reasoning: dict  # {dimension: str}
    overall: float
    fix_instructions: str

def judge_content(content_html: str, keyword: str, intent: str, business_id: str = "", sample_rate: float = 0.2) -> Optional[JudgeResult]:
    """Run LLM-as-judge on content. Returns None if not sampled."""
    import random
    if random.random() > sample_rate:
        return None

    content_hash = hashlib.sha256(content_html.encode()).hexdigest()[:16]
    cache_key = f"judge:{content_hash}"
    cached = _redis.get(cache_key)
    if cached:
        data = json.loads(cached)
        return JudgeResult(**data)

    prompt = f"""You are a senior SEO content quality judge. Evaluate the following content on 6 dimensions.

KEYWORD: {keyword}
INTENT: {intent}

CONTENT:
{content_html[:4000]}

Score each dimension 1-5 (1=poor, 5=excellent). Return JSON only:
{{
  "scores": {{"specificity": N, "accuracy": N, "depth": N, "originality": N, "intent_match": N, "eeat": N}},
  "reasoning": {{"specificity": "...", "accuracy": "...", "depth": "...", "originality": "...", "intent_match": "...", "eeat": "..."}},
  "fix_instructions": "Concise list of what needs fixing if any dimension < 3"
}}"""

    try:
        from core.llm_gateway import LLMGateway
        gw = LLMGateway(business_id=business_id)
        raw = gw.generate(prompt, complexity="smart")
        data = json.loads(raw.strip().lstrip("```json").rstrip("```"))
        scores = data["scores"]
        passed = all(v >= PASS_THRESHOLD for v in scores.values())
        overall = sum(scores.values()) / len(scores)
        result = JudgeResult(
            passed=passed,
            scores=scores,
            reasoning=data.get("reasoning", {}),
            overall=round(overall, 2),
            fix_instructions=data.get("fix_instructions", ""),
        )
        _redis.setex(cache_key, 86400 * 7, json.dumps(result.__dict__))
        log.info("llm_judge  keyword=%s  passed=%s  overall=%.1f", keyword, passed, overall)
        return result
    except Exception as exc:
        log.exception("llm_judge.error  keyword=%s", keyword)
        return None

def record_judge_outcome(content_hash: str, rank_at_90d: Optional[int]):
    """Feed judge result + outcome back into platform_content_patterns."""
    key = f"judge_outcome:{content_hash}"
    _redis.setex(key, 86400 * 120, json.dumps({"rank_at_90d": rank_at_90d}))
