"""Tool Abstraction Layer — base class for all platform connectors."""

from __future__ import annotations
from abc import ABC, abstractmethod


class Tool(ABC):
    """Base class for execution tools (WordPress, Webflow, GBP, etc)."""

    @abstractmethod
    async def create_page(self, title: str, content: str, slug: str = "", status: str = "draft") -> dict:
        raise NotImplementedError

    @abstractmethod
    async def update_page(self, page_id: str, data: dict) -> dict:
        raise NotImplementedError

    @abstractmethod
    async def create_post(self, title: str, content: str, status: str = "draft") -> dict:
        raise NotImplementedError

    async def verify(self, url: str, expected_text: str = "") -> bool:
        """Verify a live page contains expected content."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url)
                if expected_text:
                    return expected_text.lower() in resp.text.lower()
                return resp.status_code == 200
        except Exception:
            return False
