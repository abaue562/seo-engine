"""Base connector interface — all platform connectors implement this."""

from __future__ import annotations
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field
from datetime import datetime


class PublishResult(BaseModel):
    platform: str
    status: str = "success"     # success / failed / queued / draft
    url: str = ""               # Live URL if published
    post_id: str = ""
    error: str = ""
    published_at: datetime = Field(default_factory=datetime.utcnow)


class Connector(ABC):
    """Base class for all platform connectors."""

    platform: str = "unknown"

    @abstractmethod
    async def publish(self, payload: dict) -> PublishResult:
        raise NotImplementedError

    async def verify(self, result: PublishResult) -> bool:
        """Verify the published content is live."""
        if not result.url:
            return result.status == "success"
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(result.url)
                return resp.status_code == 200
        except Exception:
            return False
