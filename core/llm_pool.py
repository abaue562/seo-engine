"""LLM Pool — parallel Claude CLI + local Ollama for speed.

Routes calls to the fastest available LLM:
  - Claude CLI for complex tasks (analysis, strategy)
  - Local Ollama (qwen3:14b / gemma3:12b) for simple tasks (rewrites, scoring, formatting)

Usage:
    from core.llm_pool import call_fast, call_smart, call_parallel

    # Fast (local Ollama) — 1-3 seconds
    result = call_fast("Rewrite this title for CTR: ...")

    # Smart (Claude CLI) — 30-60 seconds but highest quality
    result = call_smart("Analyze this competitor and generate strategy...")

    # Parallel — send to both, return first response
    result = await call_parallel("Generate 3 title variants for...")
"""

from __future__ import annotations

import asyncio
import logging
import json
from concurrent.futures import ThreadPoolExecutor

import requests

log = logging.getLogger(__name__)

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_CHAT_URL = "http://localhost:11434/api/chat"

# Model routing
FAST_MODEL = "qwen3:14b"       # Local, fast, good for rewrites/formatting
SMART_MODEL = "gemma3:12b"     # Local, good reasoning
VISION_MODEL = "qwen3-vl:8b"  # Local, can see screenshots

_executor = ThreadPoolExecutor(max_workers=4)


def call_ollama(prompt: str, model: str = FAST_MODEL, max_tokens: int = 2048, temperature: float = 0.3) -> str:
    """Call local Ollama model. Returns response text. 1-5 seconds."""
    try:
        resp = requests.post(OLLAMA_URL, json={
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "num_predict": max_tokens,
                "temperature": temperature,
            },
        }, timeout=120)
        resp.raise_for_status()
        return resp.json().get("response", "").strip()
    except Exception as e:
        log.error("ollama.fail  model=%s  err=%s", model, e)
        return ""


def call_ollama_chat(messages: list[dict], model: str = FAST_MODEL, max_tokens: int = 2048) -> str:
    """Call Ollama with chat format (system + user messages)."""
    try:
        resp = requests.post(OLLAMA_CHAT_URL, json={
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {"num_predict": max_tokens},
        }, timeout=120)
        resp.raise_for_status()
        return resp.json().get("message", {}).get("content", "").strip()
    except Exception as e:
        log.error("ollama_chat.fail  model=%s  err=%s", model, e)
        return ""


def call_fast(prompt: str, max_tokens: int = 2048) -> str:
    """Fast local call — qwen3:14b. Good for rewrites, formatting, simple tasks."""
    return call_ollama(prompt, model=FAST_MODEL, max_tokens=max_tokens)


def call_smart(prompt: str, max_tokens: int = 4096) -> str:
    """Smart local call — gemma3:12b. Better reasoning."""
    return call_ollama(prompt, model=SMART_MODEL, max_tokens=max_tokens)


def call_claude(prompt: str, system: str = "", max_tokens: int = 4096) -> str:
    """Call Claude CLI (highest quality, slowest). Falls back to Ollama if CLI unavailable."""
    try:
        from core.claude import call_claude as _claude
        return _claude(prompt, system=system, max_tokens=max_tokens)
    except Exception as e:
        log.warning("claude.unavailable  falling back to ollama  err=%s", e)
        full_prompt = f"{system}\n\n{prompt}" if system else prompt
        return call_smart(full_prompt, max_tokens=max_tokens)


async def call_parallel(prompt: str, system: str = "") -> str:
    """Send to both Claude CLI and Ollama in parallel, return first response.

    If Ollama finishes first (likely), return that.
    If Claude finishes first (unlikely but possible), return that.
    """
    loop = asyncio.get_event_loop()

    async def _ollama():
        return await loop.run_in_executor(_executor, call_smart, prompt)

    async def _claude():
        full = f"{system}\n\n{prompt}" if system else prompt
        return await loop.run_in_executor(_executor, call_claude, prompt, system)

    # Race both
    tasks = [asyncio.create_task(_ollama()), asyncio.create_task(_claude())]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    # Cancel the slower one
    for task in pending:
        task.cancel()

    result = done.pop().result()
    log.info("llm_pool.parallel  result_len=%d", len(result))
    return result


def route_call(prompt: str, complexity: str = "auto", system: str = "") -> str:
    """Auto-route to the best LLM based on task complexity.

    complexity:
        "simple" → Ollama fast (rewrites, formatting, scoring)
        "medium" → Ollama smart (analysis, comparisons)
        "complex" → Claude CLI (strategy, multi-step reasoning)
        "auto" → Estimate from prompt length and content
    """
    if complexity == "auto":
        word_count = len(prompt.split())
        has_json = "json" in prompt.lower() or "return only" in prompt.lower()
        has_analysis = any(k in prompt.lower() for k in ["analyze", "strategy", "compare", "evaluate", "competitor"])

        if word_count < 100 and not has_analysis:
            complexity = "simple"
        elif has_analysis or word_count > 500:
            complexity = "complex"
        else:
            complexity = "medium"

    if complexity == "simple":
        return call_fast(prompt)
    elif complexity == "medium":
        return call_smart(prompt)
    else:
        return call_claude(prompt, system=system)


def is_ollama_available() -> bool:
    """Check if Ollama is running."""
    try:
        resp = requests.get("http://localhost:11434/api/tags", timeout=3)
        return resp.status_code == 200
    except Exception:
        return False
