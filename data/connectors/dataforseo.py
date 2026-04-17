import hashlib, json, logging, os, base64
from typing import List, Dict
import redis, requests

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
BASE_URL = "https://api.dataforseo.com"

class DataForSEOClient:
    def __init__(self, login: str = "", password: str = ""):
        self.login = login or os.getenv("DATAFORSEO_LOGIN", "")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD", "")
        self._headers = {
            "Authorization": "Basic " + base64.b64encode(f"{self.login}:{self.password}".encode()).decode(),
            "Content-Type": "application/json",
        }

    def _post(self, endpoint: str, payload: list) -> dict:
        if not self.login:
            log.warning("dataforseo: credentials not set, skipping")
            return {}
        try:
            resp = requests.post(f"{BASE_URL}{endpoint}", headers=self._headers, json=payload, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.exception("dataforseo.post_error  endpoint=%s", endpoint)
            return {}

    def _meter(self, tenant: str = ""):
        from datetime import date
        _redis.incr(f"dataforseo:calls:{tenant}:{date.today().isoformat()}")

    def get_keyword_data(self, keywords: List[str], location_code: int = 2840, tenant: str = "") -> List[Dict]:
        cache_key = f"dfs:kw:{hashlib.sha256('|'.join(sorted(keywords)).encode()).hexdigest()[:16]}"
        cached = _redis.get(cache_key)
        if cached:
            return json.loads(cached)
        items = []
        # Try DataForSEO first
        if self.login and self.password:
            try:
                payload = [{"keywords": keywords[:1000], "location_code": location_code, "language_code": "en"}]
                raw = self._post("/v3/keywords_data/google_ads/search_volume/live", payload)
                for task in raw.get("tasks", []):
                    for result in (task.get("result") or []):
                        items.append({
                            "keyword": result.get("keyword"),
                            "search_volume": result.get("search_volume", 0),
                            "competition": result.get("competition", 0),
                            "cpc": result.get("cpc", 0),
                            "difficulty": result.get("keyword_difficulty", 0),
                        })
            except Exception:
                log.warning("dataforseo.keyword_data: API failed, using self-hosted fallback")
        # Self-hosted fallback
        if not items:
            try:
                from core.keyword_intel import research_keyword, estimate_volume
                from core.serp_scraper import estimate_keyword_difficulty
                for kw in keywords[:50]:
                    vol = estimate_volume(kw, tenant)
                    diff = estimate_keyword_difficulty(kw).get("difficulty", 50)
                    items.append({"keyword": kw, "search_volume": vol, "competition": diff / 100,
                                  "cpc": 0, "difficulty": diff, "source": "self_hosted"})
                log.info("dataforseo.keyword_data: self-hosted fallback  keywords=%d", len(items))
            except Exception:
                log.exception("dataforseo.keyword_data: self-hosted fallback failed")
        if items:
            _redis.setex(cache_key, 86400 * 7, json.dumps(items))
        self._meter(tenant)
        log.info("dataforseo.keyword_data  keywords=%d  results=%d", len(keywords), len(items))
        return items

    def get_serp_snapshot(self, keyword: str, location_code: int = 2840, tenant: str = "") -> Dict:
        cache_key = f"dfs:serp:{hashlib.sha256(f'{keyword}|{location_code}'.encode()).hexdigest()[:16]}"
        cached = _redis.get(cache_key)
        if cached:
            return json.loads(cached)
        payload = [{"keyword": keyword, "location_code": location_code, "language_code": "en", "device": "desktop", "depth": 10}]
        raw = self._post("/v3/serp/google/organic/live/regular", payload)
        result = {}
        for task in raw.get("tasks", []):
            for r in (task.get("result") or []):
                items = []
                paa_count = 0
                ad_count = 0
                has_local_pack = False
                for item in (r.get("items") or []):
                    t = item.get("type", "")
                    if t == "organic":
                        items.append({"rank_group": item.get("rank_group"), "domain": item.get("domain"), "url": item.get("url"), "title": item.get("title"), "description": item.get("description")})
                    elif t == "people_also_ask":
                        paa_count += len(item.get("items", []))
                    elif t == "paid":
                        ad_count += 1
                    elif t == "local_pack":
                        has_local_pack = True
                result = {"keyword": keyword, "items": items, "paa_count": paa_count, "ad_count": ad_count, "has_local_pack": has_local_pack}
        if result:
            _redis.setex(cache_key, 86400, json.dumps(result))
            try:
                from core.serp_cache import set_cached_serp
                set_cached_serp(keyword, str(location_code), result)
            except Exception:
                pass
        # Self-hosted fallback
        if not result:
            try:
                from core.serp_scraper import scrape_serp
                serp_data = scrape_serp(keyword)
                organic = serp_data.get("organic", [])
                result = {
                    "keyword": keyword,
                    "items": [{"rank_group": r["position"], "domain": r["domain"],
                               "url": r["url"], "title": r["title"], "description": r["snippet"]}
                              for r in organic],
                    "paa_count": len(serp_data.get("paa", [])),
                    "ad_count": 0,
                    "has_local_pack": False,
                    "source": "self_hosted_bing",
                }
                if result["items"]:
                    _redis.setex(cache_key, 86400, json.dumps(result))
                log.info("dataforseo.serp_snapshot: self-hosted fallback  keyword=%s  items=%d",
                         keyword, len(result.get("items", [])))
            except Exception:
                log.exception("dataforseo.serp_snapshot: self-hosted fallback failed")
        self._meter(tenant)
        log.info("dataforseo.serp_snapshot  keyword=%s  items=%d", keyword, len(result.get("items", [])))
        return result

    def get_backlink_summary(self, domain: str, tenant: str = "") -> Dict:
        cache_key = f"dfs:bl:{hashlib.sha256(domain.encode()).hexdigest()[:16]}"
        cached = _redis.get(cache_key)
        if cached:
            return json.loads(cached)
        payload = [{"target": domain, "include_subdomains": True}]
        raw = self._post("/v3/backlinks/summary/live", payload)
        result = {}
        for task in raw.get("tasks", []):
            for r in (task.get("result") or []):
                result = {"domain": domain, "backlinks": r.get("backlinks", 0), "referring_domains": r.get("referring_domains", 0), "domain_rank": r.get("rank", 0)}
        if result:
            _redis.setex(cache_key, 86400 * 3, json.dumps(result))
        # Self-hosted fallback
        if not result:
            try:
                from core.backlink_crawler import compute_domain_authority, _conn as _bl_conn
                da_data = compute_domain_authority(domain)
                with _bl_conn() as c:
                    inbound = c.execute(
                        "SELECT COUNT(*), COUNT(DISTINCT source_domain) FROM link_graph WHERE target_domain=?", [domain]
                    ).fetchone()
                result = {
                    "domain": domain,
                    "backlinks": inbound[0] if inbound else 0,
                    "referring_domains": inbound[1] if inbound else 0,
                    "domain_rank": da_data.get("da_score", 0),
                    "source": "self_hosted_crawl",
                }
                log.info("dataforseo.backlink_summary: self-hosted fallback  domain=%s  da=%d",
                         domain, result["domain_rank"])
            except Exception:
                log.exception("dataforseo.backlink_summary: self-hosted fallback failed")
        self._meter(tenant)
        return result
