import hashlib, json, logging, os
from typing import Dict, List, Optional
import redis, requests

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
PSI_URL = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
CRUX_URL = "https://chromeuxreport.googleapis.com/v1/records:queryRecord"

class PageSpeedClient:
    def __init__(self):
        self.api_key = os.getenv("GOOGLE_API_KEY", "")

    def get_psi(self, url: str, strategy: str = "mobile") -> Dict:
        cache_key = f"psi:{hashlib.sha256(f'{url}:{strategy}'.encode()).hexdigest()[:16]}"
        cached = _redis.get(cache_key)
        if cached:
            return json.loads(cached)
        if not self.api_key:
            log.warning("pagespeed: GOOGLE_API_KEY not set")
            return {}
        try:
            resp = requests.get(PSI_URL, params={"url": url, "strategy": strategy, "key": self.api_key}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            cats = data.get("lighthouseResult", {}).get("categories", {})
            audits = data.get("lighthouseResult", {}).get("audits", {})
            result = {
                "score": round((cats.get("performance", {}).get("score") or 0) * 100),
                "lcp": audits.get("largest-contentful-paint", {}).get("numericValue", 0),
                "cls": audits.get("cumulative-layout-shift", {}).get("numericValue", 0),
                "fcp": audits.get("first-contentful-paint", {}).get("numericValue", 0),
                "ttfb": audits.get("server-response-time", {}).get("numericValue", 0),
                "passed": (cats.get("performance", {}).get("score") or 0) >= 0.5,
            }
            _redis.setex(cache_key, 86400 * 7, json.dumps(result))
            log.info("pagespeed.psi  url=%s  score=%d", url[:60], result["score"])
            return result
        except Exception as exc:
            log.exception("pagespeed.psi_error  url=%s", url[:60])
            return {}

    def get_crux(self, url: str) -> Dict:
        cache_key = f"crux:{hashlib.sha256(url.encode()).hexdigest()[:16]}"
        cached = _redis.get(cache_key)
        if cached:
            return json.loads(cached)
        if not self.api_key:
            return {}
        try:
            resp = requests.post(f"{CRUX_URL}?key={self.api_key}", json={"url": url, "formFactor": "PHONE"}, timeout=15)
            if resp.status_code == 404:
                return {"data_available": False}
            resp.raise_for_status()
            metrics = resp.json().get("record", {}).get("metrics", {})
            result = {
                "lcp_p75": metrics.get("largest_contentful_paint", {}).get("percentiles", {}).get("p75", 0),
                "cls_p75": metrics.get("cumulative_layout_shift", {}).get("percentiles", {}).get("p75", 0),
                "fid_p75": metrics.get("first_input_delay", {}).get("percentiles", {}).get("p75", 0),
                "data_available": True,
            }
            _redis.setex(cache_key, 86400, json.dumps(result))
            return result
        except Exception as exc:
            log.exception("pagespeed.crux_error  url=%s", url[:60])
            return {}

    def sample_tenant_pages(self, business_id: str, sample_n: int = 5) -> List[Dict]:
        import sqlite3, random
        conn = sqlite3.connect("data/storage/seo_engine.db")
        urls = [r[0] for r in conn.execute("SELECT url FROM published_urls WHERE business_id=? AND status='live'", [business_id]).fetchall()]
        conn.close()
        sample = random.sample(urls, min(sample_n, len(urls)))
        results = []
        for url in sample:
            psi = self.get_psi(url)
            crux = self.get_crux(url)
            results.append({"url": url, "psi": psi, "crux": crux})
        log.info("pagespeed.sample  biz=%s  sampled=%d", business_id, len(results))
        return results
