"""Chain-of-action image generation pipeline.

Steps:
  1. Load businesses from data/storage/businesses.json
  2. For each business, pull target keywords (primary_keywords + published page titles)
  3. Build a rich image prompt per keyword using Grok (text) for prompt engineering
  4. Generate the image via Grok image mode (browser session)
  5. Save to data/storage/images/{business_id}/{keyword_slug}.png
  6. Update published_urls JSON with image_path field
  7. Log results summary

Run:
    cd /opt/seo-engine && .venv/bin/python run_image_pipeline.py
    or pass --biz-id to target one business:
    .venv/bin/python run_image_pipeline.py --biz-id 75354f9d-e9c4-4c9f-b5ad-900f48ba2988
"""
import argparse
import asyncio
import json
import logging
import re
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s  %(message)s",
)
log = logging.getLogger("image_pipeline")

STORAGE   = Path("data/storage")
BIZ_FILE  = STORAGE / "businesses.json"
IMAGES    = STORAGE / "images"


# ---------------------------------------------------------------------------
# Step 1 — Load businesses
# ---------------------------------------------------------------------------

def load_businesses(biz_id: str = "") -> list[dict]:
    if not BIZ_FILE.exists():
        log.error("businesses.json not found at %s", BIZ_FILE)
        return []
    businesses = json.loads(BIZ_FILE.read_text())
    if biz_id:
        businesses = [b for b in businesses
                      if b.get("id") == biz_id or b.get("business_id") == biz_id]
    return businesses


# ---------------------------------------------------------------------------
# Step 2 — Collect keywords to generate images for
# ---------------------------------------------------------------------------

def get_keywords(business: dict, max_keywords: int = 10) -> list[str]:
    keywords = list(business.get("primary_keywords", []))

    # Pull from published_urls JSON
    biz_id = business.get("id") or business.get("business_id", "")
    for json_file in STORAGE.glob("published_urls_*.json"):
        try:
            items = json.loads(json_file.read_text())
            for item in items:
                kw = item.get("keyword", "")
                if kw and kw not in keywords:
                    keywords.append(kw)
        except Exception:
            pass

    # Skip already-generated keywords
    biz_img_dir = IMAGES / biz_id
    if biz_img_dir.exists():
        existing = {p.stem for p in biz_img_dir.glob("*.png")}
        keywords = [kw for kw in keywords
                    if re.sub(r"[^a-z0-9]+", "_", kw.lower())[:50] not in existing]
        log.info("image_pipeline.skip_existing  remaining=%d", len(keywords))

    return keywords[:max_keywords]


# ---------------------------------------------------------------------------
# Step 3 — Build enriched prompt via Grok text
# ---------------------------------------------------------------------------

def build_image_prompt(keyword: str, business: dict) -> str:
    name = business.get("name", "")
    city = business.get("city", "") or business.get("location", "")
    service = keyword.replace(city, "").replace("BC", "").strip(" ,")

    # Use Grok text to write a better image prompt
    try:
        from core.browser_llm import call_grok_sync
        meta_prompt = (
            f"Write a single concise image generation prompt (max 30 words) for a "
            f"professional business photo representing '{keyword}' for {name} in {city}. "
            f"No text overlays. Photorealistic. Clean white/neutral background or outdoor Canadian setting. "
            f"Output the prompt only, nothing else."
        )
        enriched = call_grok_sync(meta_prompt, wait_seconds=15)
        if enriched and len(enriched) > 15:
            log.info("image_pipeline.prompt_enriched  kw=%s  prompt=%r", keyword, enriched[:80])
            return enriched.strip()
    except Exception as e:
        log.warning("image_pipeline.prompt_enrich_fail  err=%s", e)

    # Fallback: rule-based prompt
    return (
        f"Professional photography, {service} service in {city} Canada, "
        f"clean modern style, no text, suitable for website hero image"
    )


# ---------------------------------------------------------------------------
# Step 4 — Generate image via Grok browser
# ---------------------------------------------------------------------------

def generate_image(prompt: str, keyword: str, biz_id: str) -> str:
    from core.browser_image_gen import generate_image_sync
    fname = re.sub(r"[^a-z0-9]+", "_", keyword.lower())[:50]
    path  = generate_image_sync(prompt, filename=fname, business_id=biz_id, wait_seconds=35)
    return path


# ---------------------------------------------------------------------------
# Step 5 — Update published_urls JSON with image paths
# ---------------------------------------------------------------------------

def update_published_urls(results: list[dict]) -> None:
    kw_to_path = {r["keyword"]: r["path"] for r in results if r.get("path")}
    if not kw_to_path:
        return
    for json_file in STORAGE.glob("published_urls_*.json"):
        try:
            items = json.loads(json_file.read_text())
            changed = False
            for item in items:
                kw = item.get("keyword", "")
                if kw in kw_to_path and not item.get("image_path"):
                    item["image_path"] = kw_to_path[kw]
                    changed = True
            if changed:
                json_file.write_text(json.dumps(items, indent=2))
                log.info("image_pipeline.updated_published_urls  file=%s", json_file.name)
        except Exception as e:
            log.warning("image_pipeline.update_fail  file=%s  err=%s", json_file, e)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run(biz_id: str = "", max_keywords: int = 10) -> None:
    businesses = load_businesses(biz_id)
    if not businesses:
        log.error("No businesses found. Check businesses.json or --biz-id.")
        return

    total_generated = 0
    total_failed    = 0

    for business in businesses:
        name   = business.get("name", "unknown")
        bid    = business.get("id") or business.get("business_id", "default")
        log.info("=== image_pipeline.business  name=%s  id=%s ===", name, bid)

        keywords = get_keywords(business, max_keywords)
        if not keywords:
            log.info("image_pipeline.no_keywords  biz=%s", name)
            continue

        log.info("image_pipeline.keywords  biz=%s  count=%d  list=%s",
                 name, len(keywords), keywords)

        results = []
        for i, kw in enumerate(keywords):
            log.info("image_pipeline.generating  %d/%d  kw=%s", i+1, len(keywords), kw)

            prompt = build_image_prompt(kw, business)
            log.info("image_pipeline.prompt  %r", prompt[:100])

            path = generate_image(prompt, kw, bid)

            if path:
                log.info("image_pipeline.success  kw=%s  path=%s", kw, path)
                total_generated += 1
            else:
                log.warning("image_pipeline.failed  kw=%s", kw)
                total_failed += 1

            results.append({"keyword": kw, "prompt": prompt, "path": path})

            # Rate limit between generations
            if i < len(keywords) - 1:
                log.info("image_pipeline.rate_limit  sleeping_8s")
                time.sleep(8)

        update_published_urls(results)

        # Save generation log
        log_path = IMAGES / bid / "generation_log.json"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        existing_log = []
        if log_path.exists():
            try:
                existing_log = json.loads(log_path.read_text())
            except Exception:
                pass
        existing_log.extend(results)
        log_path.write_text(json.dumps(existing_log, indent=2))

    log.info("=== image_pipeline.done  generated=%d  failed=%d ===",
             total_generated, total_failed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Grok image generation pipeline")
    parser.add_argument("--biz-id", default="", help="Target specific business ID")
    parser.add_argument("--max-keywords", type=int, default=10,
                        help="Max images to generate per business")
    args = parser.parse_args()
    run(biz_id=args.biz_id, max_keywords=args.max_keywords)
