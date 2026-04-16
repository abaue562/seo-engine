"""Topical Authority Engine — TopicalMapBuilder.

Transforms a raw keyword list into a structured topical map:

  PillarTopic
    ├── pillar_keyword (1 page, 2000+ words)
    └── ClusterPage[]  (N pages, 700-1200 words each)

The map drives content calendar generation and ensures all content is
published in topical-authority sequence (pillar first, clusters second).

Usage
-----
    from core.topical.map_builder import TopicalMapBuilder

    builder = TopicalMapBuilder()
    topic_map = await builder.build(
        primary_service="emergency plumber",
        primary_city="New York City",
        keywords=["emergency plumber NYC", "drain cleaning Manhattan", ...],
        max_clusters=15,
    )
    calendar = builder.to_content_calendar(topic_map)

Dependencies
------------
- DataForSEO (optional) for keyword volume + difficulty data
- Claude for semantic grouping if no embedding model is available
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_TOPICAL_MAPS_DIR = Path("data/storage/topical_maps")


@dataclass
class ClusterPage:
    keyword:         str
    slug:            str
    intent:          str          # informational | navigational | commercial | transactional
    page_type:       str          # service_page | blog_post | location_page | faq_page
    target_words:    int = 900
    volume:          int = 0
    difficulty:      int = 0
    priority_rank:   int = 0
    status:          str = "pending"   # pending | generated | published | indexed


@dataclass
class PillarTopic:
    name:          str
    pillar_keyword: str
    slug:          str
    target_words:  int = 2500
    volume:        int = 0
    difficulty:    int = 0
    clusters:      list[ClusterPage] = field(default_factory=list)
    coverage_pct:  float = 0.0        # 0-100%: how many cluster pages are published
    status:        str = "pending"


@dataclass
class TopicalMap:
    business_id:     str
    primary_service: str
    primary_city:    str
    pillars:         list[PillarTopic] = field(default_factory=list)
    total_pages:     int = 0
    coverage_pct:    float = 0.0
    created_at:      str = ""
    updated_at:      str = ""

    def all_keywords(self) -> list[str]:
        kws = [p.pillar_keyword for p in self.pillars]
        for p in self.pillars:
            kws.extend(c.keyword for c in p.clusters)
        return kws

    def pending_pages(self) -> list[dict]:
        """Return all pages not yet published, in priority order."""
        pages = []
        for pillar in self.pillars:
            if pillar.status == "pending":
                pages.append({
                    "keyword":   pillar.pillar_keyword,
                    "page_type": "pillar",
                    "slug":      pillar.slug,
                    "words":     pillar.target_words,
                    "pillar":    pillar.name,
                    "priority":  1,
                })
            for c in pillar.clusters:
                if c.status == "pending":
                    pages.append({
                        "keyword":   c.keyword,
                        "page_type": c.page_type,
                        "intent":    c.intent,
                        "slug":      c.slug,
                        "words":     c.target_words,
                        "pillar":    pillar.name,
                        "priority":  c.priority_rank,
                    })
        return sorted(pages, key=lambda x: x["priority"])

    def coverage_summary(self) -> dict:
        total = sum(1 + len(p.clusters) for p in self.pillars)
        published = sum(
            (1 if p.status == "published" else 0) +
            sum(1 for c in p.clusters if c.status in ("published", "indexed"))
            for p in self.pillars
        )
        return {
            "total_pages":    total,
            "published_pages": published,
            "coverage_pct":   round(published / max(total, 1) * 100, 1),
            "pillars":         len(self.pillars),
            "clusters":        sum(len(p.clusters) for p in self.pillars),
        }


class TopicalMapBuilder:
    """Builds and manages topical authority maps for a business."""

    def __init__(self):
        _TOPICAL_MAPS_DIR.mkdir(parents=True, exist_ok=True)

    async def build(
        self,
        primary_service: str,
        primary_city: str,
        keywords: list[str],
        business_id: str = "",
        max_clusters: int = 15,
        *,
        enrich_from_dataforseo: bool = True,
    ) -> TopicalMap:
        """Build a complete topical map from a keyword list.

        Args:
            primary_service: e.g. "emergency plumbing"
            primary_city:    e.g. "New York City"
            keywords:        List of keyword strings to cluster
            business_id:     ID for persistence
            max_clusters:    Max cluster pages per pillar
            enrich_from_dataforseo: Fetch volume/difficulty data if True

        Returns:
            TopicalMap with PillarTopics and ClusterPages
        """
        log.info(
            "topical_map.build  service=%s  city=%s  keywords=%d",
            primary_service, primary_city, len(keywords),
        )

        # 1. Classify intent for each keyword
        enriched = _classify_intent(keywords, primary_city)

        # 2. Enrich with DataForSEO volume + difficulty
        if enrich_from_dataforseo:
            enriched = await _enrich_with_dataforseo(enriched)

        # 3. Cluster keywords into topical groups via Claude
        clusters = await _cluster_keywords_via_claude(
            keywords=enriched,
            primary_service=primary_service,
            primary_city=primary_city,
            max_groups=max(3, len(keywords) // 5),
        )

        # 4. Build TopicalMap dataclass
        pillars: list[PillarTopic] = []
        rank = 1
        for group in clusters:
            pillar_kw = _pick_pillar(group["keywords"], primary_service, primary_city)
            pillar = PillarTopic(
                name=group["name"],
                pillar_keyword=pillar_kw,
                slug=_to_slug(pillar_kw),
                target_words=2500,
                volume=_volume_for(pillar_kw, enriched),
                difficulty=_difficulty_for(pillar_kw, enriched),
            )
            for kw_info in group["keywords"]:
                kw = kw_info if isinstance(kw_info, str) else kw_info.get("keyword", "")
                if kw == pillar_kw:
                    continue
                intent = _intent_for(kw, enriched)
                cluster = ClusterPage(
                    keyword=kw,
                    slug=_to_slug(kw),
                    intent=intent,
                    page_type=_page_type_for_intent(intent),
                    target_words=_target_words_for_intent(intent),
                    volume=_volume_for(kw, enriched),
                    difficulty=_difficulty_for(kw, enriched),
                    priority_rank=rank,
                )
                pillar.clusters.append(cluster)
                rank += 1
                if len(pillar.clusters) >= max_clusters:
                    break
            # Sort clusters: high volume + low difficulty first
            pillar.clusters.sort(
                key=lambda c: (c.volume * (1 - c.difficulty / 100)),
                reverse=True,
            )
            for i, c in enumerate(pillar.clusters, 1):
                c.priority_rank = i
            pillars.append(pillar)

        now = datetime.now(tz=timezone.utc).isoformat()
        topic_map = TopicalMap(
            business_id=business_id,
            primary_service=primary_service,
            primary_city=primary_city,
            pillars=pillars,
            total_pages=sum(1 + len(p.clusters) for p in pillars),
            created_at=now,
            updated_at=now,
        )
        topic_map.coverage_pct = topic_map.coverage_summary()["coverage_pct"]
        self._save(topic_map)

        log.info(
            "topical_map.built  pillars=%d  total_pages=%d",
            len(pillars), topic_map.total_pages,
        )
        return topic_map

    def to_content_calendar(
        self,
        topic_map: TopicalMap,
        publish_per_week: int = 3,
        start_date: datetime | None = None,
    ) -> list[dict]:
        """Generate a sequenced content calendar from the topical map.

        Pillar pages always come first, then cluster pages in priority order.

        Returns:
            List of dicts with: scheduled_date, keyword, page_type, slug,
                                target_words, pillar, intent
        """
        start = start_date or datetime.now(tz=timezone.utc)
        calendar: list[dict] = []
        day_offset = 0
        items_this_week = 0

        for pillar in topic_map.pillars:
            # Pillar page first
            if pillar.status == "pending":
                pub_date = start + timedelta(days=day_offset)
                calendar.append({
                    "scheduled_date": pub_date.strftime("%Y-%m-%d"),
                    "keyword":        pillar.pillar_keyword,
                    "page_type":      "pillar",
                    "slug":           pillar.slug,
                    "target_words":   pillar.target_words,
                    "pillar":         pillar.name,
                    "intent":         "commercial",
                    "volume":         pillar.volume,
                    "difficulty":     pillar.difficulty,
                })
                items_this_week += 1
                if items_this_week >= publish_per_week:
                    day_offset += 7
                    items_this_week = 0

            # Cluster pages after pillar
            for cluster in pillar.clusters:
                if cluster.status != "pending":
                    continue
                pub_date = start + timedelta(days=day_offset)
                calendar.append({
                    "scheduled_date": pub_date.strftime("%Y-%m-%d"),
                    "keyword":        cluster.keyword,
                    "page_type":      cluster.page_type,
                    "slug":           cluster.slug,
                    "target_words":   cluster.target_words,
                    "pillar":         pillar.name,
                    "intent":         cluster.intent,
                    "volume":         cluster.volume,
                    "difficulty":     cluster.difficulty,
                })
                items_this_week += 1
                if items_this_week >= publish_per_week:
                    day_offset += 7
                    items_this_week = 0

        return calendar

    def mark_published(self, business_id: str, keyword: str) -> bool:
        """Mark a keyword page as published in the persisted topical map."""
        topic_map = self.load(business_id)
        if not topic_map:
            return False
        for pillar in topic_map.pillars:
            if pillar.pillar_keyword == keyword:
                pillar.status = "published"
                self._save(topic_map)
                return True
            for cluster in pillar.clusters:
                if cluster.keyword == keyword:
                    cluster.status = "published"
                    self._save(topic_map)
                    return True
        return False

    def get_gap_report(self, business_id: str) -> dict:
        """Return unpublished pages as a gap report."""
        topic_map = self.load(business_id)
        if not topic_map:
            return {"error": "no topical map found", "business_id": business_id}
        pending = topic_map.pending_pages()
        summary = topic_map.coverage_summary()
        return {
            "business_id":    business_id,
            "summary":        summary,
            "pending_pages":  pending[:20],  # top 20 gaps
            "total_gaps":     len(pending),
        }

    def load(self, business_id: str) -> TopicalMap | None:
        """Load a persisted topical map for a business."""
        path = _TOPICAL_MAPS_DIR / f"{business_id}.json"
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            pillars = []
            for pd in data.get("pillars", []):
                clusters = [ClusterPage(**c) for c in pd.pop("clusters", [])]
                pillar = PillarTopic(**pd, clusters=clusters)
                pillars.append(pillar)
            return TopicalMap(
                business_id=data["business_id"],
                primary_service=data["primary_service"],
                primary_city=data["primary_city"],
                pillars=pillars,
                total_pages=data.get("total_pages", 0),
                coverage_pct=data.get("coverage_pct", 0.0),
                created_at=data.get("created_at", ""),
                updated_at=data.get("updated_at", ""),
            )
        except Exception as e:
            log.warning("topical_map.load_fail  business_id=%s  err=%s", business_id, e)
            return None

    def _save(self, topic_map: TopicalMap) -> None:
        """Persist a topical map as JSON."""
        if not topic_map.business_id:
            return
        path = _TOPICAL_MAPS_DIR / f"{topic_map.business_id}.json"
        data = {
            "business_id":    topic_map.business_id,
            "primary_service": topic_map.primary_service,
            "primary_city":   topic_map.primary_city,
            "total_pages":    topic_map.total_pages,
            "coverage_pct":   topic_map.coverage_pct,
            "created_at":     topic_map.created_at,
            "updated_at":     datetime.now(tz=timezone.utc).isoformat(),
            "pillars": [
                {
                    "name":           p.name,
                    "pillar_keyword": p.pillar_keyword,
                    "slug":           p.slug,
                    "target_words":   p.target_words,
                    "volume":         p.volume,
                    "difficulty":     p.difficulty,
                    "status":         p.status,
                    "coverage_pct":   p.coverage_pct,
                    "clusters": [
                        {
                            "keyword":       c.keyword,
                            "slug":          c.slug,
                            "intent":        c.intent,
                            "page_type":     c.page_type,
                            "target_words":  c.target_words,
                            "volume":        c.volume,
                            "difficulty":    c.difficulty,
                            "priority_rank": c.priority_rank,
                            "status":        c.status,
                        }
                        for c in p.clusters
                    ],
                }
                for p in topic_map.pillars
            ],
        }
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        log.debug("topical_map.saved  path=%s", path)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _classify_intent(keywords: list[str], city: str) -> list[dict]:
    """Classify search intent for each keyword (heuristic + city signals)."""
    out = []
    city_lower = city.lower()
    for kw in keywords:
        kw_low = kw.lower()
        # Heuristic rules
        if any(w in kw_low for w in ["cost", "price", "how much", "fee", "quote", "estimate"]):
            intent = "commercial"
        elif any(w in kw_low for w in ["hire", "near me", "best", "top", "service", "company", city_lower]):
            intent = "transactional"
        elif any(w in kw_low for w in ["what is", "how to", "why", "guide", "tips", "vs", "difference"]):
            intent = "informational"
        elif any(w in kw_low for w in ["buy", "get", "book", "schedule", "call", "emergency"]):
            intent = "transactional"
        else:
            intent = "informational"
        out.append({"keyword": kw, "intent": intent, "volume": 0, "difficulty": 0})
    return out


async def _enrich_with_dataforseo(keywords: list[dict]) -> list[dict]:
    """Fetch volume + difficulty from DataForSEO for each keyword."""
    login    = os.getenv("DATAFORSEO_LOGIN", "")
    password = os.getenv("DATAFORSEO_PASSWORD", "")
    if not login or not password:
        log.debug("topical_map.dataforseo_skip  reason=no_credentials")
        return keywords

    kw_list = [k["keyword"] for k in keywords]
    try:
        import httpx
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.dataforseo.com/v3/keywords_data/google/search_volume/live",
                auth=(login, password),
                json=[{"keywords": kw_list[:100], "location_code": 2840, "language_code": "en"}],
            )
            resp.raise_for_status()
            items = resp.json().get("tasks", [{}])[0].get("result", [{}])[0].get("items", [])
            vol_map = {i["keyword"]: i.get("search_volume", 0) for i in items if "keyword" in i}
    except Exception as e:
        log.warning("topical_map.dataforseo_fail  err=%s", e)
        return keywords

    for k in keywords:
        k["volume"] = vol_map.get(k["keyword"], 0)
    return keywords


async def _cluster_keywords_via_claude(
    keywords: list[dict],
    primary_service: str,
    primary_city: str,
    max_groups: int,
) -> list[dict]:
    """Use Claude to semantically cluster keywords into topical groups."""
    try:
        from core.claude import call_claude

        kw_list_str = "\n".join(f"- {k['keyword']}" for k in keywords)
        prompt = f"""Cluster these SEO keywords for a {primary_service} business in {primary_city} into {max_groups} topical groups.

Keywords:
{kw_list_str}

Rules:
- Each group must have a clear topical theme (e.g. "Emergency Services", "Drain Cleaning", "Pricing & Costs")
- One keyword per group must be the PILLAR keyword (highest volume, broadest topic)
- Remaining keywords are CLUSTER pages supporting the pillar
- Output ONLY valid JSON array:

[
  {{
    "name": "Group Theme Name",
    "pillar_keyword": "the main keyword for this group",
    "keywords": ["kw1", "kw2", ...]
  }}
]"""

        raw = call_claude(prompt, max_tokens=2048)
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        groups = json.loads(raw)
        log.info("topical_map.clustered  groups=%d", len(groups))
        return groups

    except Exception as e:
        log.warning("topical_map.cluster_fail  err=%s  falling_back_to_heuristic", e)
        return _heuristic_cluster(keywords, primary_service, max_groups)


def _heuristic_cluster(keywords: list[dict], primary_service: str, max_groups: int) -> list[dict]:
    """Fallback: cluster by first significant word in keyword."""
    groups: dict[str, list] = {}
    for kw in keywords:
        words = [w for w in kw["keyword"].split() if len(w) > 3]
        key = words[0].capitalize() if words else "General"
        groups.setdefault(key, []).append(kw["keyword"])
    result = []
    for name, kws in list(groups.items())[:max_groups]:
        result.append({"name": name, "keywords": kws})
    return result


def _pick_pillar(keywords: list, primary_service: str, primary_city: str) -> str:
    """Select the pillar keyword from a group — prefer broadest + highest volume."""
    kws = [k if isinstance(k, str) else k.get("keyword", "") for k in keywords]
    # Prefer keywords that contain the primary service
    service_matches = [k for k in kws if primary_service.lower() in k.lower()]
    if service_matches:
        return service_matches[0]
    return kws[0] if kws else primary_service


def _to_slug(keyword: str) -> str:
    """Convert a keyword to a URL slug."""
    import re
    slug = keyword.lower()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'\s+', '-', slug.strip())
    return slug


def _intent_for(keyword: str, enriched: list[dict]) -> str:
    for item in enriched:
        if item["keyword"] == keyword:
            return item.get("intent", "informational")
    return "informational"


def _volume_for(keyword: str, enriched: list[dict]) -> int:
    for item in enriched:
        if item["keyword"] == keyword:
            return item.get("volume", 0)
    return 0


def _difficulty_for(keyword: str, enriched: list[dict]) -> int:
    for item in enriched:
        if item["keyword"] == keyword:
            return item.get("difficulty", 0)
    return 0


def _page_type_for_intent(intent: str) -> str:
    return {
        "transactional": "service_page",
        "commercial":    "service_page",
        "informational": "blog_post",
        "navigational":  "location_page",
    }.get(intent, "blog_post")


def _target_words_for_intent(intent: str) -> int:
    return {
        "transactional": 900,
        "commercial":    1200,
        "informational": 1500,
        "navigational":  700,
    }.get(intent, 900)
