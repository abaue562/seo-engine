"""CTR Domination Engine — generates, tests, and selects winning title/meta variants.

Flow:
  1. Detect low-CTR pages (high impressions, low clicks)
  2. Generate 3 variants (curiosity, urgency, benefit)
  3. Rotate titles on a 7-day schedule
  4. Measure CTR per variant from GSC
  5. Select winner (must beat baseline by 10%+)
  6. Apply winner permanently

This forces ranking movement WITHOUT new backlinks.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from core.claude import call_claude
from data.storage.database import Database

log = logging.getLogger(__name__)

ROTATION_DAYS = 7
MIN_IMPRESSIONS_TO_TEST = 100
CTR_THRESHOLD_LOW = 0.035  # Below 3.5% = underperforming for page 1-2


class CTRVariant(BaseModel):
    title: str
    meta_description: str
    style: str = ""
    predicted_ctr_boost: str = ""


class CTRTest(BaseModel):
    test_id: str = ""
    page_url: str
    keyword: str
    variants: list[CTRVariant] = Field(default_factory=list)
    current_variant: int = 0
    baseline_ctr: float = 0.0
    baseline_impressions: int = 0
    results: dict[str, float] = {}  # "variant_0" -> measured CTR
    winner: int | None = None
    status: str = "planned"        # planned / active / completed
    started_at: datetime = Field(default_factory=datetime.utcnow)
    next_rotation: datetime | None = None


CTR_PROMPT = """You are the CTR Optimization Agent. Your ONLY goal: maximize click-through rate.

Page: {page_url}
Current title: {current_title}
Current meta: {current_meta}
Keyword: {keyword}
Business: {business_name}
City: {city}
Current CTR: {current_ctr}
Current position: {position}
Impressions: {impressions}

Generate 3 RADICALLY different title + meta description variants designed to MAXIMIZE clicks.

Requirements:
- Variant 1: CURIOSITY — create an information gap ("What most {city} homeowners don't know about...")
- Variant 2: URGENCY — time pressure or scarcity ("Limited availability", "Before it's too late")
- Variant 3: BENEFIT — clear value + social proof ("{reviews} reviews", "Save $X", "Same-day")
- All titles UNDER 60 characters (HARD LIMIT)
- All metas UNDER 160 characters
- Keyword must appear naturally in title
- City must appear in title or meta
- Each must be dramatically different from current

Return ONLY JSON:
{{
  "variants": [
    {{"title": "", "meta_description": "", "style": "curiosity", "predicted_ctr_boost": "+X%"}},
    {{"title": "", "meta_description": "", "style": "urgency", "predicted_ctr_boost": "+X%"}},
    {{"title": "", "meta_description": "", "style": "benefit", "predicted_ctr_boost": "+X%"}}
  ],
  "recommended_primary": 0
}}"""


class CTRDominator:
    """Full CTR testing pipeline — detect, generate, rotate, measure, select."""

    def __init__(self, db: Database | None = None):
        self.db = db or Database()

    # ----- Detection -----

    def detect_low_ctr_pages(
        self,
        gsc_data: list[dict],
        min_impressions: int = MIN_IMPRESSIONS_TO_TEST,
        ctr_threshold: float = CTR_THRESHOLD_LOW,
    ) -> list[dict]:
        """Find pages with high impressions but low CTR — biggest CTR opportunities."""
        opportunities = []
        for row in gsc_data:
            impressions = row.get("impressions", 0)
            ctr = row.get("ctr", 0)
            position = row.get("position", 100)

            if impressions >= min_impressions and ctr < ctr_threshold and position <= 20:
                opportunities.append({
                    "keyword": row.get("keyword", ""),
                    "page": row.get("page", ""),
                    "impressions": impressions,
                    "ctr": ctr,
                    "clicks": row.get("clicks", 0),
                    "position": position,
                    "potential_clicks": int(impressions * 0.06) - row.get("clicks", 0),  # 6% target CTR
                })

        opportunities.sort(key=lambda x: x["potential_clicks"], reverse=True)
        log.info("ctr.detect  candidates=%d  above_threshold=%d", len(gsc_data), len(opportunities))
        return opportunities

    # ----- Generation -----

    async def generate_variants(
        self,
        page_url: str,
        keyword: str,
        current_title: str,
        current_meta: str,
        current_ctr: float,
        position: int,
        impressions: int,
        business_name: str,
        city: str,
        reviews: int = 0,
    ) -> list[CTRVariant]:
        """Generate 3 CTR-optimized title/meta variants via Claude."""
        prompt = CTR_PROMPT.format(
            page_url=page_url,
            current_title=current_title or "(no title)",
            current_meta=current_meta or "(no meta description)",
            keyword=keyword,
            business_name=business_name,
            city=city,
            current_ctr=f"{current_ctr:.1%}" if current_ctr else "unknown",
            position=position,
            impressions=impressions,
            reviews=reviews,
        )

        try:
            raw = call_claude(
                prompt,
                system="You are a CTR optimization specialist. Return ONLY valid JSON. No other text.",
                max_tokens=1024,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            data = json.loads(raw)
            variants_data = data.get("variants", data) if isinstance(data, dict) else data
            variants = [CTRVariant(**v) for v in variants_data]
            log.info("ctr.generated  url=%s  variants=%d", page_url, len(variants))
            return variants

        except Exception as e:
            log.error("ctr.generate_fail  url=%s  err=%s", page_url, e)
            return []

    # ----- Test management -----

    def create_test(
        self,
        page_url: str,
        keyword: str,
        variants: list[CTRVariant],
        baseline_ctr: float,
        baseline_impressions: int = 0,
    ) -> CTRTest:
        """Create a new CTR test."""
        import uuid
        test = CTRTest(
            test_id=uuid.uuid4().hex[:12],
            page_url=page_url,
            keyword=keyword,
            variants=variants,
            baseline_ctr=baseline_ctr,
            baseline_impressions=baseline_impressions,
            status="active",
            next_rotation=datetime.utcnow() + timedelta(days=ROTATION_DAYS),
        )
        log.info("ctr.test_created  id=%s  url=%s  variants=%d", test.test_id, page_url, len(variants))
        return test

    def should_rotate(self, test: CTRTest) -> bool:
        """Check if it's time to rotate to the next variant."""
        if test.status != "active":
            return False
        if test.next_rotation and datetime.utcnow() >= test.next_rotation:
            return True
        return False

    def rotate(self, test: CTRTest) -> CTRVariant | None:
        """Advance to next variant. Returns the new active variant, or None if all tested."""
        next_idx = test.current_variant + 1
        if next_idx >= len(test.variants):
            test.status = "completed"
            log.info("ctr.rotation_complete  id=%s  all_variants_tested", test.test_id)
            return None

        test.current_variant = next_idx
        test.next_rotation = datetime.utcnow() + timedelta(days=ROTATION_DAYS)
        log.info("ctr.rotated  id=%s  now_testing=variant_%d", test.test_id, next_idx)
        return test.variants[next_idx]

    def record_result(self, test: CTRTest, variant_index: int, measured_ctr: float) -> None:
        """Record measured CTR for a variant."""
        test.results[f"variant_{variant_index}"] = measured_ctr
        log.info("ctr.recorded  id=%s  variant=%d  ctr=%.2f%%", test.test_id, variant_index, measured_ctr * 100)

    def pick_winner(self, test: CTRTest) -> CTRVariant | None:
        """Select the winning variant. Must beat baseline by 10%+."""
        if not test.results:
            return None

        best_key = max(test.results, key=test.results.get)
        best_ctr = test.results[best_key]
        best_idx = int(best_key.split("_")[1])

        improvement = (best_ctr - test.baseline_ctr) / test.baseline_ctr if test.baseline_ctr > 0 else 0

        if improvement >= 0.10:  # 10% improvement required
            test.winner = best_idx
            test.status = "completed"
            log.info("ctr.winner  id=%s  variant=%d  ctr=%.2f%%  improvement=+%.0f%%",
                     test.test_id, best_idx, best_ctr * 100, improvement * 100)
            return test.variants[best_idx]

        log.info("ctr.no_winner  id=%s  best=%.2f%%  baseline=%.2f%%  improvement=%.0f%%",
                 test.test_id, best_ctr * 100, test.baseline_ctr * 100, improvement * 100)
        return None

    # ----- Persistence -----

    async def save_test(self, test: CTRTest, business_id: str) -> None:
        await self.db.upsert("ctr_tests", {
            "test_id": test.test_id,
            "business_id": business_id,
            **test.model_dump(),
        }, key="test_id")

    async def get_active_tests(self, business_id: str) -> list[CTRTest]:
        rows = await self.db.query("ctr_tests", {"business_id": business_id, "status": "active"})
        return [CTRTest(**r) for r in rows]
