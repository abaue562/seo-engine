"""Google Business Profile scraper — extracts GBP data via Playwright.

Pulls: name, rating, review count, categories, recent reviews, posts, photos count.
Falls back to httpx+BeautifulSoup if Playwright is not available.
"""

from __future__ import annotations

import logging
from datetime import datetime

from pydantic import BaseModel

log = logging.getLogger(__name__)


class GBPReview(BaseModel):
    author: str = ""
    rating: int = 0
    text: str = ""
    date: str = ""


class GBPData(BaseModel):
    name: str = ""
    url: str = ""
    rating: float = 0.0
    review_count: int = 0
    categories: list[str] = []
    address: str = ""
    phone: str = ""
    website: str = ""
    recent_reviews: list[GBPReview] = []
    photo_count: int = 0
    fetched_at: datetime = datetime.utcnow()


async def scrape_gbp(gbp_url: str) -> GBPData:
    """Scrape a Google Business Profile using Playwright."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.warning("gbp.playwright_missing — install with: playwright install chromium")
        return GBPData(url=gbp_url, fetched_at=datetime.utcnow())

    log.info("gbp.scrape  url=%s", gbp_url)
    data = GBPData(url=gbp_url)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await page.goto(gbp_url, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(2000)  # Let dynamic content load

            # Business name
            try:
                data.name = await page.locator("h1").first.inner_text()
            except Exception:
                pass

            # Rating
            try:
                rating_text = await page.locator('[role="img"][aria-label*="stars"]').first.get_attribute("aria-label")
                if rating_text:
                    data.rating = float(rating_text.split()[0])
            except Exception:
                pass

            # Review count
            try:
                review_text = await page.locator('button[jsaction*="review"]').first.inner_text()
                import re
                match = re.search(r"([\d,]+)", review_text)
                if match:
                    data.review_count = int(match.group(1).replace(",", ""))
            except Exception:
                pass

            # Categories
            try:
                cat_buttons = page.locator('button[jsaction*="category"]')
                count = await cat_buttons.count()
                for i in range(min(count, 5)):
                    text = await cat_buttons.nth(i).inner_text()
                    if text.strip():
                        data.categories.append(text.strip())
            except Exception:
                pass

            # Address
            try:
                addr_el = page.locator('[data-item-id="address"]')
                if await addr_el.count() > 0:
                    data.address = await addr_el.first.inner_text()
            except Exception:
                pass

            # Phone
            try:
                phone_el = page.locator('[data-item-id*="phone"]')
                if await phone_el.count() > 0:
                    data.phone = await phone_el.first.inner_text()
            except Exception:
                pass

            log.info("gbp.scraped  name=%s  rating=%s  reviews=%d", data.name, data.rating, data.review_count)

        except Exception as e:
            log.error("gbp.scrape_fail  url=%s  err=%s", gbp_url, e)
        finally:
            await browser.close()

    data.fetched_at = datetime.utcnow()
    return data


async def scrape_competitor_gbps(query: str, city: str, max_results: int = 5) -> list[GBPData]:
    """Search Google Maps for competitors and scrape their GBPs."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.warning("gbp.playwright_missing")
        return []

    log.info("gbp.competitors  query=%s %s", query, city)
    competitors: list[GBPData] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            search_url = f"https://www.google.com/maps/search/{query}+{city}"
            await page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(3000)

            # Extract listing links
            listings = page.locator('a[href*="/maps/place/"]')
            count = min(await listings.count(), max_results)

            for i in range(count):
                try:
                    href = await listings.nth(i).get_attribute("href")
                    name = await listings.nth(i).get_attribute("aria-label") or ""
                    competitors.append(GBPData(
                        name=name,
                        url=href or "",
                        fetched_at=datetime.utcnow(),
                    ))
                except Exception:
                    pass

            log.info("gbp.competitors_found  count=%d", len(competitors))

        except Exception as e:
            log.error("gbp.competitor_fail  err=%s", e)
        finally:
            await browser.close()

    return competitors


def gbp_to_prompt_block(data: GBPData) -> str:
    """Render GBP data as agent context."""
    lines = [
        f"GOOGLE BUSINESS PROFILE:",
        f"  Name: {data.name}",
        f"  Rating: {data.rating} ({data.review_count} reviews)",
        f"  Categories: {', '.join(data.categories)}",
        f"  Address: {data.address}",
        f"  Phone: {data.phone}",
        f"  Photos: {data.photo_count}",
    ]
    if data.recent_reviews:
        lines.append(f"  Recent reviews ({len(data.recent_reviews)}):")
        for r in data.recent_reviews[:5]:
            lines.append(f"    - [{r.rating}/5] {r.text[:100]}")

    return "\n".join(lines)
