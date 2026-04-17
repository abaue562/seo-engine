"""Browser-based LLM caller — Perplexity and Grok via Playwright.

Routes queries through actual browser sessions instead of paid APIs.
Same function signatures as core.claude — drop-in replacements.

Perplexity: Navigates perplexity.ai/search, waits for answer, extracts text + citations.
Grok:       Navigates x.com/i/grok, types prompt, waits for streamed response.

Session persistence: stores browser auth state in data/storage/browser_sessions/
so login only happens once per provider.

Usage:
    from core.browser_llm import call_perplexity, call_grok

    # Ask Perplexity a search question -- returns (answer_text, citations_list)
    answer, citations = await call_perplexity("best LED lighting contractor Kelowna BC")

    # Ask Grok a question -- returns response text
    response = await call_grok("What do people say about Blend Bright Lights?")

    # Sync wrappers (for Celery tasks / non-async callers)
    answer, citations = call_perplexity_sync("best LED lighting Kelowna")
    response = call_grok_sync("Who is the best gutter guard installer in Kelowna?")
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

log = logging.getLogger(__name__)

_SESSIONS_DIR = Path("data/storage/browser_sessions")
_PERPLEXITY_SESSION = _SESSIONS_DIR / "perplexity.json"
_GROK_SESSION       = _SESSIONS_DIR / "grok.json"
_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Browser launch helpers
# ---------------------------------------------------------------------------

def _browser_kwargs() -> dict:
    """Return standard stealth-friendly Playwright launch kwargs."""
    return {
        "headless": True,
        "args": [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--window-size=1280,900",
        ],
    }


async def _new_stealth_page(playwright_instance, session_path: Path | None = None):
    """Launch Chromium with stored session state.

    Returns (browser, context, page).
    """
    storage_state = None
    if session_path and session_path.exists():
        storage_state = str(session_path)

    browser = await playwright_instance.chromium.launch(**_browser_kwargs())
    ctx_kwargs: dict[str, Any] = {
        "user_agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        "viewport": {"width": 1280, "height": 900},
        "locale": "en-US",
        "timezone_id": "America/Vancouver",
    }
    if storage_state:
        ctx_kwargs["storage_state"] = storage_state

    ctx  = await browser.new_context(**ctx_kwargs)
    page = await ctx.new_page()

    # Remove navigator.webdriver fingerprint
    await page.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return browser, ctx, page


async def _save_session(ctx, session_path: Path) -> None:
    """Persist browser cookies/localStorage to disk."""
    try:
        state = await ctx.storage_state()
        session_path.write_text(json.dumps(state))
        log.info("browser_llm.session_saved  path=%s", session_path)
    except Exception as e:
        log.warning("browser_llm.session_save_fail  err=%s", e)


# ---------------------------------------------------------------------------
# Perplexity -- browser-based search, no API key required
# ---------------------------------------------------------------------------

async def call_perplexity(
    query: str,
    wait_seconds: float = 8.0,
) -> tuple[str, list[str]]:
    """Ask Perplexity a question via browser automation. No API key needed.

    Priority order:
      1. Authenticated Playwright browser (uses perplexity.json session cookies)
         — bypasses Cloudflare entirely when logged in
      2. AION GPT-Researcher / Groq Brain fallback (no browser needed)

    Args:
        query:        The search query / question.
        wait_seconds: Seconds to wait for streaming answer to complete.

    Returns:
        (answer_text, citations)
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("browser_llm.playwright_missing  pip install playwright && playwright install chromium")
        return await _perplexity_via_research(query)

    log.info("browser_llm.perplexity_start  query=%r", query[:80])

    # --- Method 1: Authenticated Playwright browser with session cookies ---
    if not _PERPLEXITY_SESSION.exists():
        log.info("browser_llm.perplexity_no_session  skipping_to_research_fallback")
        return await _perplexity_via_research(query)

    async with async_playwright() as pw:
        browser, ctx, page = await _new_stealth_page(pw, _PERPLEXITY_SESSION)
        try:
            # Apply stealth patches
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                pass

            url = "https://www.perplexity.ai/search?q=" + quote_plus(query) + "&focus=web"
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(wait_seconds)

            # Check for Cloudflare challenge — fall back to research if still blocked
            body_text = await page.evaluate("() => document.body.innerText")
            if "security verification" in body_text.lower() or "verify you are human" in body_text.lower():
                log.warning("browser_llm.perplexity_cf_block  falling_back_to_research")
                await browser.close()
                return await _perplexity_via_research(query)

            # Wait for answer containers
            try:
                await page.wait_for_selector(
                    '[data-testid="answer-content"], .prose, [class*="AnswerBody"], [class*="answer"]',
                    timeout=10_000,
                )
            except Exception:
                pass

            # Extract main answer — try targeted selectors then fall back to body text
            answer_text = ""
            for selector in [
                '[data-testid="answer-content"]',
                ".prose",
                '[class*="AnswerBody"]',
                '[class*="answer"]',
                "main",
            ]:
                try:
                    el = await page.query_selector(selector)
                    if el:
                        txt = (await el.inner_text()).strip()
                        if len(txt) > 100:
                            answer_text = txt
                            break
                except Exception:
                    continue

            if not answer_text:
                answer_text = body_text[:3000]

            # Extract external citation links
            citations: list[str] = []
            try:
                import re
                hrefs = await page.evaluate(
                    "() => Array.from(document.querySelectorAll('a[href^=\"http\"]'))"
                    ".map(a => a.href)"
                )
                seen: set[str] = set()
                for href in hrefs:
                    if "perplexity.ai" not in href and href not in seen:
                        seen.add(href)
                        citations.append(href)
                        if len(citations) >= 10:
                            break
            except Exception as ce:
                log.debug("browser_llm.perplexity_citations_fail  err=%s", ce)

            await _save_session(ctx, _PERPLEXITY_SESSION)
            log.info(
                "browser_llm.perplexity_done  method=browser  chars=%d  citations=%d",
                len(answer_text), len(citations),
            )
            return (answer_text, citations)

        except Exception as e:
            log.error("browser_llm.perplexity_browser_fail  err=%s", e)
            return await _perplexity_via_research(query)
        finally:
            try:
                await browser.close()
            except Exception:
                pass


async def _perplexity_via_research(query: str) -> tuple[str, list[str]]:
    """Fallback chain when Perplexity browser is blocked.

    Tries in order:
      1. AION GPT-Researcher (real web research)
      2. AION Brain via Groq (handles all known response shapes)
      3. Local Ollama via call_smart (always available)
    """
    import re

    # 1. GPT-Researcher (pass correct provider:model format to fix 500 error)
    try:
        import urllib.request as _ur
        _body = json.dumps({
            "query": query,
            "report_type": "outline",
            "smart_llm_model": "openai:qwen-3-235b-a22b-instruct-2507",
            "fast_llm_model":  "openai:qwen-3-235b-a22b-instruct-2507",
        }).encode()
        _req = _ur.Request(
            "http://localhost:8170/api/v1/research/sync",
            data=_body,
            headers={"Content-Type": "application/json"},
        )
        with _ur.urlopen(_req, timeout=60) as _r:
            _data = json.loads(_r.read())
        report = _data.get("report", "") or _data.get("output", "") or _data.get("result", "")
        if report and len(report) > 100:
            log.info("browser_llm.perplexity_gpt_researcher_ok  chars=%d", len(report))
            citations = re.findall(r'https?://[^\s\)\"\'\]]+', report)
            citations = [c for c in citations if "perplexity.ai" not in c][:10]
            return (report, citations)
        log.debug("browser_llm.perplexity_gpt_researcher_empty  detail=%s", str(_data)[:200])
    except Exception as e:
        log.debug("browser_llm.perplexity_gpt_researcher_fail  err=%s", e)

    # 2. AION Brain (Groq) -- handle all response shapes
    try:
        from core.aion_bridge import _post, _BRAIN_URL
        prompt = f"Answer this question in detail: {query}"
        result = _post(_BRAIN_URL, "/v1/chat/completions", {
            "model": "groq",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 1000,
        }, timeout=30)
        text = (
            result.get("content")
            or ((result.get("raw") or {}).get("choices") or [{}])[0].get("message", {}).get("content", "")
            or ((result.get("choices") or [{}])[0]).get("message", {}).get("content", "")
            or ""
        )
        if text and len(text) > 50:
            log.info("browser_llm.perplexity_brain_ok  chars=%d", len(text))
            return (text, [])
    except Exception as e:
        log.debug("browser_llm.perplexity_brain_fail  err=%s", e)

    # 3. Local Ollama (always available)
    try:
        from core.llm_pool import call_smart
        prompt = f"Answer in detail, mention relevant local businesses if known: {query}"
        text = call_smart(prompt, max_tokens=1000)
        if text and len(text) > 50:
            log.info("browser_llm.perplexity_ollama_ok  chars=%d", len(text))
            return (text, [])
    except Exception as e:
        log.debug("browser_llm.perplexity_ollama_fail  err=%s", e)

    log.error("browser_llm.perplexity_all_fallbacks_failed  query=%r", query[:60])
    return ("", [])


def call_perplexity_sync(query: str, wait_seconds: float = 8.0) -> tuple[str, list[str]]:
    """Synchronous wrapper for Celery tasks / non-async callers."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(asyncio.run, call_perplexity(query, wait_seconds))
                return fut.result(timeout=60)
        return loop.run_until_complete(call_perplexity(query, wait_seconds))
    except Exception as e:
        log.error("browser_llm.perplexity_sync_fail  err=%s", e)
        return ("", [])


# ---------------------------------------------------------------------------
# Grok -- x.com/i/grok via browser session
# ---------------------------------------------------------------------------

async def call_grok(
    prompt: str,
    wait_seconds: float = 20.0,
    session_path: Path | None = None,
) -> str:
    """Ask Grok a question via x.com/i/grok browser automation.

    Requires X/Twitter cookies saved in the browser session file.
    First run: call setup_grok_session() to log in and persist cookies.

    Args:
        prompt:       The question or task for Grok.
        wait_seconds: Seconds to wait for streaming response to complete.
        session_path: Override default session file path.

    Returns:
        Grok's response as plain text. Empty string on failure.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("browser_llm.playwright_missing")
        return ""

    spath = session_path or _GROK_SESSION
    if not spath.exists():
        log.warning(
            "browser_llm.grok_no_session  path=%s  "
            "call setup_grok_session() once to save Twitter login cookies",
            spath,
        )
        return ""

    log.info("browser_llm.grok_start  prompt_chars=%d", len(prompt))

    async with async_playwright() as pw:
        browser, ctx, page = await _new_stealth_page(pw, spath)
        try:
            await page.goto("https://x.com/i/grok", wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(3)

            # Find the prompt input
            textarea = None
            for sel in [
                "textarea[placeholder]",
                '[data-testid="tweetTextarea_0"]',
                '[role="textbox"]',
                'div[contenteditable="true"]',
            ]:
                try:
                    textarea = await page.wait_for_selector(sel, timeout=8_000)
                    if textarea:
                        break
                except Exception:
                    continue

            if not textarea:
                log.error("browser_llm.grok_no_textarea  url=%s", page.url)
                return ""

            await textarea.click()
            await asyncio.sleep(0.5)
            await textarea.fill(prompt)
            await asyncio.sleep(0.5)

            # Submit
            try:
                send_btn = await page.query_selector(
                    '[data-testid="grok-send-button"], button[aria-label="Send"]'
                )
                if send_btn:
                    await send_btn.click()
                else:
                    await textarea.press("Enter")
            except Exception:
                await textarea.press("Enter")

            log.info("browser_llm.grok_waiting  seconds=%.1f", wait_seconds)
            await asyncio.sleep(wait_seconds)

            # Wait for streaming to finish (send button re-enables)
            try:
                await page.wait_for_function(
                    "() => !document.querySelector('[data-testid=\"grok-send-button\"][disabled]')",
                    timeout=int((wait_seconds + 15) * 1000),
                )
            except Exception:
                pass

            # Extract Grok's response from page body text.
            # Grok renders: <nav chrome> ... <user prompt> <grok answer> <suggested follow-ups>
            # We find the user's prompt in the body text and take everything after it.
            response_text = ""
            try:
                body_text = await page.evaluate("() => document.body.innerText")
                # Find the user's prompt in the page text
                prompt_idx = body_text.find(prompt)
                if prompt_idx != -1:
                    after_prompt = body_text[prompt_idx + len(prompt):].strip()
                    # Strip trailing UI noise (suggested questions, buttons)
                    noise_markers = ["Think Harder\n", "Auto\n", "Create Images\n",
                                     "Explore\n", "SuperGrok\n"]
                    cutoff = len(after_prompt)
                    for marker in noise_markers:
                        idx = after_prompt.find(marker)
                        if idx != -1 and idx < cutoff:
                            cutoff = idx
                    response_text = after_prompt[:cutoff].strip()

                # If prompt not found in body, fall back to full body tail
                if not response_text:
                    response_text = body_text[-3000:].strip()
            except Exception as ex:
                log.debug("browser_llm.grok_extract_fail  err=%s", ex)

            await _save_session(ctx, spath)
            log.info("browser_llm.grok_done  chars=%d", len(response_text))
            return response_text

        except Exception as e:
            log.error("browser_llm.grok_fail  err=%s", e)
            return ""
        finally:
            await browser.close()


def call_grok_sync(prompt: str, wait_seconds: float = 20.0) -> str:
    """Synchronous wrapper for Celery tasks / non-async callers."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(asyncio.run, call_grok(prompt, wait_seconds))
                return fut.result(timeout=90)
        return loop.run_until_complete(call_grok(prompt, wait_seconds))
    except Exception as e:
        log.error("browser_llm.grok_sync_fail  err=%s", e)
        return ""


# ---------------------------------------------------------------------------
# Session setup -- run once interactively to log in
# ---------------------------------------------------------------------------

async def setup_grok_session(
    twitter_username: str = "",
    twitter_password: str = "",
) -> bool:
    """Log into x.com and persist cookies for future headless Grok calls.

    Run this once via:
        python -c "from core.browser_llm import setup_grok_session_sync; setup_grok_session_sync()"

    Reads credentials from args or TWITTER_USERNAME / TWITTER_PASSWORD env vars.

    Returns:
        True if login succeeded and session was saved.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("browser_llm.playwright_missing")
        return False

    username = twitter_username or os.getenv("TWITTER_USERNAME", "")
    password = twitter_password or os.getenv("TWITTER_PASSWORD", "")

    if not username or not password:
        log.error(
            "browser_llm.grok_setup_no_creds  "
            "set TWITTER_USERNAME + TWITTER_PASSWORD in config/.env"
        )
        return False

    async with async_playwright() as pw:
        browser, ctx, page = await _new_stealth_page(pw, session_path=None)
        try:
            await page.goto("https://x.com/login", wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(2)

            await page.fill('input[autocomplete="username"], input[name="text"]', username)
            await page.press('input[autocomplete="username"], input[name="text"]', "Enter")
            await asyncio.sleep(2)

            await page.fill('input[name="password"]', password)
            await page.press('input[name="password"]', "Enter")
            await asyncio.sleep(4)

            if "home" in page.url:
                await _save_session(ctx, _GROK_SESSION)
                log.info("browser_llm.grok_session_saved  user=%s", username)
                return True
            else:
                log.error(
                    "browser_llm.grok_login_fail  url=%s  "
                    "may need 2FA -- run locally with headless=False first then copy the session file",
                    page.url,
                )
                return False
        finally:
            await browser.close()


def setup_grok_session_sync(
    twitter_username: str = "",
    twitter_password: str = "",
) -> bool:
    """Synchronous wrapper for setup_grok_session."""
    return asyncio.run(setup_grok_session(twitter_username, twitter_password))


# ---------------------------------------------------------------------------
# Citation helper -- drop-in for citation_monitor._test_citation_perplexity
# ---------------------------------------------------------------------------

async def perplexity_citation_check(
    query: str,
    business_name: str,
    competitor_names: list[str] | None = None,
) -> dict:
    """Check if business_name appears in Perplexity's browser answer. No API key.

    Drop-in replacement for citation_monitor._test_citation_perplexity().

    Returns:
        dict: cited, citation_rank, snippet, competitor_cited, citations, engine
    """
    answer, citations = await call_perplexity(query)
    competitor_names = competitor_names or []

    cited = business_name.lower() in answer.lower()
    citation_rank = 0
    for i, url in enumerate(citations, 1):
        name_slug = business_name.lower().replace(" ", "")
        if name_slug in url.lower().replace("-", "").replace(" ", ""):
            citation_rank = i
            break

    competitor_cited = any(comp.lower() in answer.lower() for comp in competitor_names)

    return {
        "cited":            cited,
        "citation_rank":    citation_rank,
        "snippet":          answer[:500],
        "competitor_cited": competitor_cited,
        "citations":        citations,
        "engine":           "perplexity_browser",
    }
