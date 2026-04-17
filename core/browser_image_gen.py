"""Browser-based image generation via Grok (x.com/i/grok) using saved session.

Same Playwright + session pattern as core/browser_llm.py.
No API key needed — uses the grok.json browser session cookie.

Usage:
    from core.browser_image_gen import generate_image_sync

    path = generate_image_sync(
        prompt="Professional exterior home cleaning service hero image, Kelowna BC",
        filename="bbl_hero",
        business_id="75354f9d-...",
    )
    # Returns: "data/storage/images/75354f9d-.../bbl_hero.png" or ""
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_SESSIONS_DIR  = Path("data/storage/browser_sessions")
_GROK_SESSION  = _SESSIONS_DIR / "grok.json"
_IMAGES_DIR    = Path("data/storage/images")


def _browser_kwargs() -> dict:
    return {
        "headless": True,
        "args": [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--window-size=1280,900",
        ],
    }


async def _new_stealth_page(pw, session_path: Optional[Path] = None):
    storage_state = str(session_path) if session_path and session_path.exists() else None
    browser = await pw.chromium.launch(**_browser_kwargs())
    ctx_kwargs: dict = {
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
    await page.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return browser, ctx, page


async def _save_session(ctx, path: Path) -> None:
    try:
        state = await ctx.storage_state()
        path.write_text(json.dumps(state))
    except Exception as e:
        log.warning("browser_image_gen.session_save_fail  err=%s", e)


async def generate_image(
    prompt: str,
    filename: str = "",
    business_id: str = "default",
    wait_seconds: float = 30.0,
    session_path: Optional[Path] = None,
) -> str:
    """Generate an image via Grok's browser interface.

    Navigates to x.com/i/grok, switches to image mode, submits prompt,
    waits for generation, downloads the result.

    Args:
        prompt:       Image description.
        filename:     Output filename without extension (auto-generated if empty).
        business_id:  Used to organise output under data/storage/images/{business_id}/
        wait_seconds: Max seconds to wait for image to appear.
        session_path: Override default grok.json session path.

    Returns:
        Relative path to saved image file, or "" on failure.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("browser_image_gen.playwright_missing")
        return ""

    spath = session_path or _GROK_SESSION
    if not spath.exists():
        log.warning("browser_image_gen.no_session  run setup_grok_session() first")
        return ""

    out_dir = _IMAGES_DIR / business_id
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = filename or re.sub(r"[^a-z0-9]+", "_", prompt.lower())[:60]
    out_path = out_dir / f"{fname}.png"

    log.info("browser_image_gen.start  prompt=%r  out=%s", prompt[:60], out_path)

    async with async_playwright() as pw:
        browser, ctx, page = await _new_stealth_page(pw, spath)
        try:
            await page.goto("https://x.com/i/grok", wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(3)

            # Click "Create Images" tab/button
            image_mode_clicked = False
            for sel in [
                'button:has-text("Create Images")',
                '[aria-label="Create Images"]',
                'span:has-text("Create Images")',
                '[data-testid*="image"]',
            ]:
                try:
                    el = await page.wait_for_selector(sel, timeout=5_000)
                    if el:
                        await el.click()
                        image_mode_clicked = True
                        log.info("browser_image_gen.image_mode_clicked  sel=%s", sel)
                        await asyncio.sleep(2)
                        break
                except Exception:
                    continue

            if not image_mode_clicked:
                log.warning("browser_image_gen.no_image_tab  trying_prompt_prefix")

            # Build prompt — prefix with /image if no tab found
            full_prompt = prompt if image_mode_clicked else f"Generate an image: {prompt}"

            # Find text input
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
                log.error("browser_image_gen.no_textarea")
                return ""

            await textarea.click()
            await asyncio.sleep(0.5)
            await textarea.fill(full_prompt)
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

            log.info("browser_image_gen.waiting  seconds=%.0f", wait_seconds)

            # Wait for an <img> to appear that isn't a UI element (avatar, icon, etc.)
            img_url = ""
            deadline = time.time() + wait_seconds
            while time.time() < deadline:
                await asyncio.sleep(3)
                try:
                    # Find large generated images (not avatars/icons — those are small)
                    img_srcs: list[str] = await page.evaluate("""
                        () => Array.from(document.querySelectorAll('img'))
                            .filter(img => {
                                const r = img.getBoundingClientRect();
                                return r.width > 200 && r.height > 200 &&
                                       img.src && img.src.startsWith('http') &&
                                       !img.src.includes('profile_images') &&
                                       !img.src.includes('emoji') &&
                                       !img.src.includes('favicon');
                            })
                            .map(img => img.src)
                    """)
                    if img_srcs:
                        img_url = img_srcs[0]
                        log.info("browser_image_gen.image_found  url=%s", img_url[:80])
                        break
                except Exception:
                    continue

            if not img_url:
                # Fallback: screenshot the response area
                log.warning("browser_image_gen.no_img_src  taking_screenshot_fallback")
                try:
                    await page.screenshot(path=str(out_path), full_page=False)
                    log.info("browser_image_gen.screenshot_saved  path=%s", out_path)
                    await _save_session(ctx, spath)
                    return str(out_path)
                except Exception as se:
                    log.error("browser_image_gen.screenshot_fail  err=%s", se)
                    return ""

            # Download the image
            try:
                img_response = await page.request.get(img_url)
                if img_response.ok:
                    out_path.write_bytes(await img_response.body())
                    log.info("browser_image_gen.saved  path=%s  bytes=%d",
                             out_path, out_path.stat().st_size)
                    await _save_session(ctx, spath)
                    return str(out_path)
                else:
                    log.error("browser_image_gen.download_fail  status=%d", img_response.status)
                    return ""
            except Exception as de:
                log.error("browser_image_gen.download_err  err=%s", de)
                return ""

        except Exception as e:
            log.error("browser_image_gen.fail  err=%s", e)
            return ""
        finally:
            try:
                await browser.close()
            except Exception:
                pass


def generate_image_sync(
    prompt: str,
    filename: str = "",
    business_id: str = "default",
    wait_seconds: float = 30.0,
) -> str:
    """Synchronous wrapper — safe to call from Celery tasks."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(
                    asyncio.run,
                    generate_image(prompt, filename, business_id, wait_seconds)
                )
                return fut.result(timeout=wait_seconds + 30)
        return loop.run_until_complete(
            generate_image(prompt, filename, business_id, wait_seconds)
        )
    except Exception as e:
        log.error("browser_image_gen.sync_fail  err=%s", e)
        return ""


def generate_content_images(business: dict, keywords: list[str]) -> list[dict]:
    """Generate hero + supporting images for a batch of content topics.

    Called after content generation to attach images to published pages.

    Args:
        business:  Business dict with name, city, services fields.
        keywords:  List of target keywords to generate images for.

    Returns:
        List of {keyword, path, prompt} dicts.
    """
    name    = business.get("name", "")
    city    = business.get("city", "")
    biz_id  = business.get("business_id") or business.get("id", "default")
    results = []

    for kw in keywords:
        prompt = (
            f"Professional photography style image for '{kw}' service, "
            f"{city} Canada, no text overlays, clean modern look, "
            f"suitable for a business website hero section"
        )
        fname = re.sub(r"[^a-z0-9]+", "_", kw.lower())[:50]
        path  = generate_image_sync(prompt, filename=fname, business_id=biz_id)
        results.append({"keyword": kw, "path": path, "prompt": prompt})
        log.info("browser_image_gen.content_image  kw=%s  path=%s", kw, path or "FAILED")
        # Rate limit: Grok has generation limits
        time.sleep(5)

    return results
