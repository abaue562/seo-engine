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
        payload = [{"keywords": keywords[:1000], "location_code": location_code, "language_code": "en"}]
        raw = self._post("/v3/keywords_data/google_ads/search_volume/live", payload)
        items = []
        for task in raw.get("tasks", []):
            for result in (task.get("result") or []):
                items.append({
                    "keyword": result.get("keyword"),
                    "search_volume": result.get("search_volume", 0),
                    "competition": result.get("competition", 0),
                    "cpc": result.get("cpc", 0),
                    "difficulty": result.get("keyword_difficulty", 0),
                })
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
        self._meter(tenant)
        return result
