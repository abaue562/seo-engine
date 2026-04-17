"""LLM Gateway -- budget caps + circuit breaker for all LLM calls."""
from __future__ import annotations
import logging, os, time
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

_DEFAULT_DAILY_BUDGET_CENTS = int(os.getenv("LLM_DAILY_BUDGET_CENTS", "500"))
_GLOBAL_HOURLY_BUDGET_CENTS = int(os.getenv("GLOBAL_LLM_BUDGET_HOURLY_CENTS", "5000"))
_CIRCUIT_ERROR_THRESHOLD = float(os.getenv("LLM_CIRCUIT_ERROR_THRESHOLD", "0.20"))
_CIRCUIT_WINDOW_SECS = int(os.getenv("LLM_CIRCUIT_WINDOW_SECS", "60"))
_CIRCUIT_COOLDOWN_SECS = int(os.getenv("LLM_CIRCUIT_COOLDOWN_SECS", "120"))
_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_COST_FAST = 1
_COST_SMART = 2
_COST_COMPLEX = 300


class _CircuitBreaker:
    def __init__(self, provider: str, r):
        self._r = r
        self._provider = provider
        self._ok_key = f"llm:circuit:{provider}:ok"
        self._err_key = f"llm:circuit:{provider}:err"
        self._open_key = f"llm:circuit:{provider}:open_until"

    def is_open(self) -> bool:
        try:
            until = self._r.get(self._open_key)
            return bool(until and float(until) > time.time())
        except Exception:
            return False

    def record_success(self) -> None:
        try:
            p = self._r.pipeline()
            p.incr(self._ok_key)
            p.expire(self._ok_key, _CIRCUIT_WINDOW_SECS)
            p.execute()
        except Exception:
            pass

    def record_failure(self) -> None:
        try:
            p = self._r.pipeline()
            p.incr(self._err_key)
            p.expire(self._err_key, _CIRCUIT_WINDOW_SECS)
            p.execute()
            err = int(self._r.get(self._err_key) or 0)
            ok = int(self._r.get(self._ok_key) or 0)
            total = err + ok
            if total >= 5 and err / total >= _CIRCUIT_ERROR_THRESHOLD:
                open_until = time.time() + _CIRCUIT_COOLDOWN_SECS
                self._r.set(self._open_key, open_until, ex=_CIRCUIT_COOLDOWN_SECS + 10)
                log.warning(
                    "llm_gateway.circuit_open  provider=%s  err_rate=%.0f%%",
                    self._provider, 100 * err / total,
                )
        except Exception:
            pass

    def reset(self) -> None:
        try:
            self._r.delete(self._ok_key, self._err_key, self._open_key)
        except Exception:
            pass


class LLMGateway:
    """Single entry point for all LLM calls with budget caps and circuit breaking."""

    def __init__(self):
        self._redis = None
        self._circuits: dict[str, _CircuitBreaker] = {}

    def _get_redis(self):
        if self._redis is None:
            try:
                import redis
                self._redis = redis.from_url(_REDIS_URL, decode_responses=True, socket_timeout=2)
                self._redis.ping()
            except Exception as e:
                log.debug("llm_gateway.redis_unavailable  err=%s", e)
                self._redis = None
        return self._redis

    def _circuit(self, provider: str) -> Optional[_CircuitBreaker]:
        r = self._get_redis()
        if r is None:
            return None
        if provider not in self._circuits:
            self._circuits[provider] = _CircuitBreaker(provider, r)
        return self._circuits[provider]

    def _check_budget(self, business_id: str, est_microcents: int) -> tuple[bool, str]:
        r = self._get_redis()
        if r is None:
            return True, "redis_unavailable"
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        hour = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H")
        biz_key = f"llm:budget:biz:{business_id}:{today}"
        global_key = f"llm:budget:global:{hour}"
        try:
            p = r.pipeline()
            p.get(biz_key)
            p.get(global_key)
            biz_spend, global_spend = p.execute()
            biz_spend = int(biz_spend or 0)
            global_spend = int(global_spend or 0)
            biz_lim = _DEFAULT_DAILY_BUDGET_CENTS * 100
            glb_lim = _GLOBAL_HOURLY_BUDGET_CENTS * 100
            if biz_spend + est_microcents > biz_lim:
                return False, f"biz_budget_exceeded biz={business_id}"
            if global_spend + est_microcents > glb_lim:
                return False, "global_budget_exceeded"
            return True, "ok"
        except Exception as e:
            log.debug("llm_gateway.budget_check_fail  err=%s", e)
            return True, "error_allow"

    def _record_spend(self, business_id: str, cost_mc: int, provider: str, tokens: int) -> None:
        r = self._get_redis()
        if r is None:
            return
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        hour = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H")
        try:
            p = r.pipeline()
            p.incrby(f"llm:budget:biz:{business_id}:{today}", cost_mc)
            p.expire(f"llm:budget:biz:{business_id}:{today}", 90000)
            p.incrby(f"llm:budget:global:{hour}", cost_mc)
            p.expire(f"llm:budget:global:{hour}", 7200)
            p.execute()
        except Exception:
            pass

    def generate(
        self,
        prompt: str,
        *,
        business_id: str = "global",
        complexity: str = "smart",
        max_tokens: int = 2048,
        system: str = "",
    ) -> str:
        """Generate text via the best available LLM within budget and circuit limits."""
        cost_map = {"fast": _COST_FAST, "smart": _COST_SMART, "complex": _COST_COMPLEX}
        est_cost = max_tokens * cost_map.get(complexity, _COST_SMART)
        allowed, reason = self._check_budget(business_id, est_cost)
        if not allowed:
            log.warning("llm_gateway.budget_blocked  biz=%s  reason=%s", business_id, reason)
            return ""
        if complexity == "complex":
            return self._call_complex(prompt, system, max_tokens, business_id, est_cost)
        if complexity == "fast":
            return self._call_ollama(prompt, "qwen3:14b", max_tokens, business_id, _COST_FAST * max_tokens)
        return self._call_ollama(prompt, "gemma3:12b", max_tokens, business_id, _COST_SMART * max_tokens)

    def _call_ollama(self, prompt, model, max_tokens, business_id, est_cost) -> str:
        provider = f"ollama:{model}"
        cb = self._circuit(provider)
        if cb and cb.is_open():
            log.warning("llm_gateway.circuit_skip  provider=%s", provider)
            return ""
        try:
            from core.llm_pool import call_ollama
            result = call_ollama(prompt, model=model, max_tokens=max_tokens)
            if cb:
                if result:
                    cb.record_success()
                else:
                    cb.record_failure()
            if result:
                est_tokens = int(len(result.split()) * 1.3)
                # Use est_cost to record proportional actual cost (not hardcoded COST_FAST)
                per_token_cost = est_cost / max_tokens if max_tokens else _COST_FAST
                self._record_spend(business_id, int(est_tokens * per_token_cost), provider, est_tokens)
            return result
        except Exception as e:
            log.warning("llm_gateway.ollama_fail  err=%s", e)
            if cb:
                cb.record_failure()
            return ""

    def _call_complex(self, prompt, system, max_tokens, business_id, est_cost) -> str:
        cb = self._circuit("claude")
        if cb and cb.is_open():
            log.warning("llm_gateway.claude_circuit_open  falling_back")
            return self._call_ollama(prompt, "gemma3:12b", max_tokens, business_id, _COST_SMART * max_tokens)
        try:
            from core.llm_pool import call_claude
            result = call_claude(prompt, system=system, max_tokens=max_tokens)
            if cb:
                if result:
                    cb.record_success()
                else:
                    cb.record_failure()
            if result:
                est_tokens = int(len(result.split()) * 1.3)
                self._record_spend(business_id, est_tokens * _COST_COMPLEX, "claude", est_tokens)
            return result
        except Exception as e:
            log.warning("llm_gateway.claude_fail  err=%s  falling_back", e)
            if cb:
                cb.record_failure()
            return self._call_ollama(prompt, "gemma3:12b", max_tokens, business_id, _COST_SMART * max_tokens)

    def get_spend_summary(self, business_id: str) -> dict:
        r = self._get_redis()
        if r is None:
            return {"error": "redis_unavailable"}
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        hour = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H")
        biz_spend = int(r.get(f"llm:budget:biz:{business_id}:{today}") or 0)
        global_spend = int(r.get(f"llm:budget:global:{hour}") or 0)
        biz_lim = _DEFAULT_DAILY_BUDGET_CENTS * 100
        return {
            "business_id": business_id,
            "today": today,
            "biz_microcents_spent": biz_spend,
            "biz_budget_microcents": biz_lim,
            "biz_utilization_pct": round(100 * biz_spend / biz_lim, 1) if biz_lim else 0,
            "global_hour_microcents_spent": global_spend,
            "global_hour_budget_microcents": _GLOBAL_HOURLY_BUDGET_CENTS * 100,
        }

    def pause_business(self, business_id: str, hours: int = 24) -> None:
        r = self._get_redis()
        if r:
            today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
            r.set(
                f"llm:budget:biz:{business_id}:{today}",
                _DEFAULT_DAILY_BUDGET_CENTS * 100 + 1,
                ex=hours * 3600,
            )
            log.info("llm_gateway.business_paused  biz=%s  hours=%d", business_id, hours)

    def reset_circuit(self, provider: str) -> None:
        cb = self._circuit(provider)
        if cb:
            cb.reset()
        log.info("llm_gateway.circuit_reset  provider=%s", provider)


# Module-level singleton
gateway = LLMGateway()
