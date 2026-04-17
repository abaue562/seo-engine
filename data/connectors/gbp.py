import json, logging, os
from typing import List, Dict, Optional
import redis, requests

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
GBP_BASE = "https://mybusiness.googleapis.com/v4"
GBP_INSIGHTS = "https://businessprofileperformance.googleapis.com/v1"

class GBPConnector:
    def __init__(self, access_token: str = ""):
        self.token = access_token or os.getenv("GBP_ACCESS_TOKEN", "")
        self._headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def _get(self, url: str) -> dict:
        if not self.token:
            log.warning("gbp: GBP_ACCESS_TOKEN not set")
            return {}
        try:
            resp = requests.get(url, headers=self._headers, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.exception("gbp.get_error  url=%s", url)
            return {}

    def _post(self, url: str, body: dict) -> dict:
        if not self.token:
            return {}
        try:
            resp = requests.post(url, headers=self._headers, json=body, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.exception("gbp.post_error  url=%s", url)
            return {}

    def get_locations(self, account_id: str) -> List[Dict]:
        cache_key = f"gbp:locations:{account_id}"
        cached = _redis.get(cache_key)
        if cached:
            return json.loads(cached)
        data = self._get(f"{GBP_BASE}/accounts/{account_id}/locations?pageSize=100")
        locations = data.get("locations", [])
        if locations:
            _redis.setex(cache_key, 3600, json.dumps(locations))
        return locations

    def get_reviews(self, account_id: str, location_id: str, page_size: int = 20) -> List[Dict]:
        data = self._get(f"{GBP_BASE}/accounts/{account_id}/locations/{location_id}/reviews?pageSize={page_size}")
        return data.get("reviews", [])

    def reply_to_review(self, account_id: str, location_id: str, review_id: str, reply_text: str) -> bool:
        url = f"{GBP_BASE}/accounts/{account_id}/locations/{location_id}/reviews/{review_id}/reply"
        result = self._post(url, {"comment": reply_text})
        return bool(result)

    def create_post(self, account_id: str, location_id: str, summary: str, url: Optional[str] = None, media_url: Optional[str] = None) -> str:
        body: Dict = {"languageCode": "en", "summary": summary, "topicType": "STANDARD"}
        if url:
            body["callToAction"] = {"actionType": "LEARN_MORE", "url": url}
        if media_url:
            body["media"] = [{"mediaFormat": "PHOTO", "sourceUrl": media_url}]
        result = self._post(f"{GBP_BASE}/accounts/{account_id}/locations/{location_id}/localPosts", body)
        name = result.get("name", "")
        log.info("gbp.create_post  location=%s  name=%s", location_id, name)
        return name

    def get_insights(self, account_id: str, location_id: str, days: int = 30) -> Dict:
        cache_key = f"gbp:insights:{location_id}:{days}"
        cached = _redis.get(cache_key)
        if cached:
            return json.loads(cached)
        from datetime import date, timedelta
        end = date.today().isoformat()
        start = (date.today() - timedelta(days=days)).isoformat()
        url = f"{GBP_INSIGHTS}/locations/{location_id}:getDailyMetricsTimeSeries?dailyMetric=WEBSITE_CLICKS&dailyRange.startDate.year={start[:4]}&dailyRange.startDate.month={start[5:7]}&dailyRange.startDate.day={start[8:]}&dailyRange.endDate.year={end[:4]}&dailyRange.endDate.month={end[5:7]}&dailyRange.endDate.day={end[8:]}"
        data = self._get(url)
        result = {"views": 0, "searches": 0, "actions": data.get("timeSeries", {}).get("datedValues", [])}
        _redis.setex(cache_key, 86400, json.dumps(result))
        return result
