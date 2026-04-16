"""
IndexNow URL submission for instant search engine indexing.
Spec: https://www.indexnow.org/documentation

Key should be stored at: {site_url}/.well-known/{key}.txt
or at: {site_url}/{key}.txt

Usage:
    indexnow = IndexNow(key="your-api-key", host="yoursite.com")
    result = indexnow.submit(urls=["https://yoursite.com/page"])
    result = indexnow.submit_batch(urls=[...])
"""
import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

log = logging.getLogger(__name__)

ENGINES = {
    "indexnow": "https://api.indexnow.org/indexnow",
    "bing":     "https://www.bing.com/indexnow",
    "yandex":   "https://yandex.com/indexnow",
}

# IndexNow batch size limit per spec
_BATCH_SIZE = 10_000


class IndexNow:
    def __init__(self, key: str = None, host: str = None):
        self.key          = key  or os.getenv("INDEXNOW_API_KEY", "")
        self.host         = host or os.getenv("SITE_HOST", "")
        self.key_location = (
            f"https://{self.host}/{self.key}.txt" if self.host and self.key else ""
        )
        self.log_path = Path("data/storage/indexnow_log.json")
        self.client   = httpx.Client(timeout=15)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(self, url: str, engines: list[str] = None) -> dict:
        """Submit a single URL to one or more IndexNow engines.

        Returns:
            {
                url: str,
                submitted_to: list[str],   # engine names that accepted the URL
                failed: list[str],          # engine names that returned errors
                status: "ok" | "error",
                timestamp: str (ISO-8601),
            }
        """
        target_engines = engines or list(ENGINES.keys())
        submitted_to: list[str] = []
        failed: list[str]       = []

        for engine in target_engines:
            endpoint = ENGINES.get(engine)
            if not endpoint:
                log.warning("indexnow.unknown_engine  engine=%s", engine)
                failed.append(engine)
                continue

            params = {
                "url":         url,
                "key":         self.key,
                "keyLocation": self.key_location,
            }

            try:
                resp = self.client.get(endpoint, params=params)
                # IndexNow returns 200 or 202 on success; 400/422 on bad request
                if resp.status_code in (200, 202):
                    submitted_to.append(engine)
                    log.info("indexnow.submitted  engine=%s  url=%s  status=%d",
                             engine, url, resp.status_code)
                else:
                    log.warning("indexnow.rejected  engine=%s  url=%s  status=%d  body=%s",
                                engine, url, resp.status_code, resp.text[:200])
                    failed.append(engine)
            except httpx.RequestError as exc:
                log.error("indexnow.request_error  engine=%s  url=%s  err=%s",
                          engine, url, exc)
                failed.append(engine)

        status = "ok" if submitted_to else "error"
        result = {
            "url":          url,
            "submitted_to": submitted_to,
            "failed":       failed,
            "status":       status,
            "timestamp":    datetime.now(timezone.utc).isoformat(),
        }
        self._append_log(result)
        return result

    def submit_batch(self, urls: list[str], engines: list[str] = None) -> dict:
        """Submit up to 10,000 URLs per call; chunks automatically if needed.

        POST body per IndexNow spec:
            {
                "host":        self.host,
                "key":         self.key,
                "keyLocation": self.key_location,
                "urlList":     [...urls...]
            }

        Returns:
            {
                total_urls:   int,
                submitted_to: list[str],
                failed:       list[str],
                chunk_count:  int,
                timestamp:    str,
            }
        """
        if not urls:
            return {
                "total_urls":   0,
                "submitted_to": [],
                "failed":       [],
                "chunk_count":  0,
                "timestamp":    datetime.now(timezone.utc).isoformat(),
            }

        target_engines = engines or list(ENGINES.keys())
        submitted_to: list[str] = []
        failed_engines: list[str] = []

        # Chunk into batches
        chunks = [urls[i: i + _BATCH_SIZE] for i in range(0, len(urls), _BATCH_SIZE)]

        for engine in target_engines:
            endpoint = ENGINES.get(engine)
            if not endpoint:
                log.warning("indexnow.batch.unknown_engine  engine=%s", engine)
                failed_engines.append(engine)
                continue

            engine_ok = True
            for chunk_idx, chunk in enumerate(chunks):
                body = {
                    "host":        self.host,
                    "key":         self.key,
                    "keyLocation": self.key_location,
                    "urlList":     chunk,
                }
                try:
                    resp = self.client.post(
                        endpoint,
                        json=body,
                        headers={"Content-Type": "application/json; charset=utf-8"},
                    )
                    if resp.status_code in (200, 202):
                        log.info("indexnow.batch.submitted  engine=%s  chunk=%d/%d  urls=%d  status=%d",
                                 engine, chunk_idx + 1, len(chunks), len(chunk), resp.status_code)
                    else:
                        log.warning("indexnow.batch.rejected  engine=%s  chunk=%d  status=%d  body=%s",
                                    engine, chunk_idx + 1, resp.status_code, resp.text[:300])
                        engine_ok = False
                except httpx.RequestError as exc:
                    log.error("indexnow.batch.request_error  engine=%s  chunk=%d  err=%s",
                              engine, chunk_idx + 1, exc)
                    engine_ok = False

            if engine_ok:
                submitted_to.append(engine)
            else:
                failed_engines.append(engine)

        result = {
            "total_urls":   len(urls),
            "submitted_to": submitted_to,
            "failed":       failed_engines,
            "chunk_count":  len(chunks),
            "timestamp":    datetime.now(timezone.utc).isoformat(),
        }
        self._append_log(result)
        return result

    def submit_sitemap(self, sitemap_url: str) -> dict:
        """Ping Google and Bing about a sitemap update via the standard sitemap ping endpoint.

        Returns:
            {
                sitemap_url: str,
                pinged:      list[str],   # engine names that responded 200
                failed:      list[str],
                timestamp:   str,
            }
        """
        ping_endpoints = {
            "google": "https://www.google.com/ping",
            "bing":   "https://www.bing.com/ping",
        }

        pinged: list[str] = []
        failed: list[str] = []

        for engine, base_url in ping_endpoints.items():
            try:
                resp = self.client.get(base_url, params={"sitemap": sitemap_url})
                if resp.status_code == 200:
                    pinged.append(engine)
                    log.info("indexnow.sitemap_ping.ok  engine=%s  sitemap=%s",
                             engine, sitemap_url)
                else:
                    log.warning("indexnow.sitemap_ping.rejected  engine=%s  status=%d",
                                engine, resp.status_code)
                    failed.append(engine)
            except httpx.RequestError as exc:
                log.error("indexnow.sitemap_ping.error  engine=%s  err=%s", engine, exc)
                failed.append(engine)

        result = {
            "sitemap_url": sitemap_url,
            "pinged":      pinged,
            "failed":      failed,
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        }
        self._append_log(result)
        return result

    def generate_key_file_content(self) -> str:
        """Returns the content for the key verification file (just the key itself)."""
        return self.key

    def is_configured(self) -> bool:
        """Return True if both key and host are set."""
        return bool(self.key and self.host)

    def get_submission_log(self, last_n: int = 100) -> list[dict]:
        """Return the last N submission log entries (oldest-first within the slice)."""
        if not self.log_path.exists():
            return []
        try:
            data = json.loads(self.log_path.read_text(encoding="utf-8"))
            if not isinstance(data, list):
                return []
            return data[-last_n:]
        except (json.JSONDecodeError, OSError) as exc:
            log.error("indexnow.log_read_error  path=%s  err=%s", self.log_path, exc)
            return []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _append_log(self, entry: dict) -> None:
        """Append a log entry to the JSON log file, creating directories as needed."""
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            if self.log_path.exists():
                data: list = json.loads(self.log_path.read_text(encoding="utf-8"))
                if not isinstance(data, list):
                    data = []
            else:
                data = []
            data.append(entry)
            self.log_path.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except (OSError, json.JSONDecodeError) as exc:
            log.error("indexnow.log_write_error  err=%s", exc)
