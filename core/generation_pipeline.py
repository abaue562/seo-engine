import logging, re
from typing import Optional, Dict
import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)

class GenerationPipeline:
    def __init__(self, business_id: str = ""):
        self.business_id = business_id
        self.cost_tokens = 0

    def _pass1_gate(self, content: str) -> tuple[bool, str]:
        h2_count = len(re.findall(r'<h2', content, re.I))
        word_count = len(content.split())
        if h2_count < 2:
            return False, f"Too few H2s: {h2_count} (need >=2)"
        if word_count < 200:
            return False, f"Too short: {word_count} words"
        return True, ""

    def _pass2_gate(self, content: str) -> tuple[bool, str]:
        sentences = [s.strip() for s in re.split(r'[.!?]', content) if len(s.strip()) > 20]
        if not sentences:
            return True, ""
        generic_patterns = [r'\bis important\b', r'\bplays a (key|crucial|vital) role\b', r'\bin today\'s world\b', r'\bit is worth noting\b']
        generic_count = sum(1 for s in sentences if any(re.search(p, s, re.I) for p in generic_patterns))
        ratio = generic_count / len(sentences)
        if ratio > 0.4:
            return False, f"Too many generic sentences: {ratio:.0%}"
        return True, ""

    def _pass3_gate(self, content: str) -> tuple[bool, str]:
        if 'application/ld+json' in content:
            return True, ""
        return False, "Missing structured data / schema markup"

    def run(self, keyword: str, intent: str, brief: str, generator_fn) -> Optional[Dict]:
        content = None
        for attempt in range(2):
            content = generator_fn(keyword, intent, brief, pass_num=1)
            self.cost_tokens += len(content.split()) * 2
            ok, reason = self._pass1_gate(content)
            if ok:
                break
            log.warning("generation_pipeline.pass1_fail  attempt=%d  reason=%s", attempt, reason)
            brief = brief + f"\n\nFIX REQUIRED: {reason}"
            content = None

        if content is None:
            _redis.incr(f"pipeline:pass1_fail:{self.business_id}")
            log.error("generation_pipeline.pass1_abort  keyword=%s", keyword)
            return None

        content = generator_fn(keyword, intent, brief, pass_num=2, draft=content)
        self.cost_tokens += len(content.split()) * 2
        ok, reason = self._pass2_gate(content)
        if not ok:
            log.warning("generation_pipeline.pass2_fail  reason=%s", reason)

        content = generator_fn(keyword, intent, brief, pass_num=3, draft=content)
        self.cost_tokens += len(content.split())
        ok, _ = self._pass3_gate(content)
        stages_passed = 3 if ok else 2

        try:
            from core.llm_judge import judge_content
            result = judge_content(content, keyword, intent, self.business_id)
            if result and not result.passed:
                log.warning("generation_pipeline.judge_fail  keyword=%s  overall=%.1f", keyword, result.overall)
                stages_passed = max(stages_passed - 1, 0)
        except Exception:
            pass

        _redis.incrby(f"pipeline:tokens:{self.business_id}", self.cost_tokens)
        return {"content": content, "stages_passed": stages_passed, "cost_tokens": self.cost_tokens}


def run_pass5_eeat(html: str, business_id: str, content_url: str = "", business_name: str = "") -> str:
    """Pass 5: E-E-A-T injection (author bio, FAQ schema, trust signals)."""
    try:
        from core.eeat_pipeline import run_eeat_pipeline
        result = run_eeat_pipeline(
            html=html,
            business_id=business_id,
            content_url=content_url,
            business_name=business_name,
        )
        return result["html"]
    except Exception:
        import logging
        logging.getLogger(__name__).exception("pass5_eeat failed")
        return html
