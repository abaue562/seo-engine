"""Vercel Connector — deploys generated HTML pages to Vercel hosting.

Setup:
  1. vercel.com/account/tokens → create token
  2. Add to config/.env:
       VERCEL_TOKEN=<your_token>
       VERCEL_PROJECT_NAME=blendbrightlights   (or your project name)
       VERCEL_TEAM_ID=<optional team id>

How it works:
  - Each call to publish() creates a new Vercel deployment
  - Pages are uploaded as static HTML files
  - Vercel returns a preview URL immediately (e.g. project-abc123.vercel.app)
  - To make it live on your domain, the deployment is promoted to production

Page routing:
  /permanent-lights-kelowna.html → blendbrightlights.com/permanent-lights-kelowna
  /landscape-lighting-kelowna.html → blendbrightlights.com/landscape-lighting-kelowna

To wire your custom domain:
  1. vercel.com → project settings → Domains → add blendbrightlights.com
  2. Or use: vercel domains add blendbrightlights.com
"""

from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path
from typing import Any

import httpx

from execution.connectors.base import Connector, PublishResult

log = logging.getLogger(__name__)

VERCEL_API = "https://api.vercel.com"


class VercelConnector(Connector):
    platform = "vercel"

    def __init__(
        self,
        token: str = "",
        project_name: str = "",
        team_id: str = "",
    ):
        self.token = token or os.getenv("VERCEL_TOKEN", "")
        self.project_name = project_name or os.getenv("VERCEL_PROJECT_NAME", "")
        self.team_id = team_id or os.getenv("VERCEL_TEAM_ID", "")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def is_configured(self) -> bool:
        return bool(self.token and self.project_name)

    async def publish(self, payload: dict) -> PublishResult:
        """Deploy a page (or batch of pages) to Vercel.

        Payload schema:
          {
            "files": [                           # list of pages to deploy
              {"path": "permanent-lights-kelowna", "html": "<html>...</html>"},
              ...
            ],
            "promote": True,                     # promote to production (default False = preview only)
            "target": "production"               # "production" | "preview"
          }

        Or single-page shorthand:
          {
            "path": "permanent-lights-kelowna",
            "html": "<html>...",
            "promote": False
          }
        """
        if not self.is_configured():
            return PublishResult(
                platform="vercel",
                status="failed",
                error="VERCEL_TOKEN or VERCEL_PROJECT_NAME not set in config/.env",
            )

        # Normalize to list of files
        if "files" in payload:
            pages = payload["files"]
        else:
            pages = [{"path": payload.get("path", "index"), "html": payload.get("html", "")}]

        promote = payload.get("promote", False)
        target = payload.get("target", "production" if promote else "preview")

        # Build Vercel files array
        vercel_files = []
        for page in pages:
            path = page["path"].strip("/")
            if not path.endswith(".html"):
                path = f"{path}.html"
            html_bytes = page["html"].encode("utf-8")
            vercel_files.append({
                "file": path,
                "data": base64.b64encode(html_bytes).decode("ascii"),
                "encoding": "base64",
            })

        # Add minimal vercel.json for clean URL routing
        vercel_config = {
            "cleanUrls": True,
            "trailingSlash": False,
        }
        vercel_files.append({
            "file": "vercel.json",
            "data": base64.b64encode(json.dumps(vercel_config).encode()).decode("ascii"),
            "encoding": "base64",
        })

        deploy_body: dict[str, Any] = {
            "name": self.project_name,
            "files": vercel_files,
            "projectSettings": {
                "framework": None,  # static
                "outputDirectory": ".",
            },
            "target": target,
        }

        params: dict[str, str] = {}
        if self.team_id:
            params["teamId"] = self.team_id

        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    f"{VERCEL_API}/v13/deployments",
                    headers=self.headers,
                    params=params,
                    json=deploy_body,
                )

                if resp.status_code in (200, 201):
                    data = resp.json()
                    deploy_id = data.get("id", "")
                    deploy_url = data.get("url", "")
                    state = data.get("readyState", "BUILDING")

                    log.info("vercel.deployed  id=%s  url=%s  state=%s  pages=%d",
                             deploy_id, deploy_url, state, len(pages))

                    return PublishResult(
                        platform="vercel",
                        status="success" if state in ("READY", "BUILDING") else "queued",
                        url=f"https://{deploy_url}" if deploy_url else "",
                        post_id=deploy_id,
                    )

                error_body = resp.text[:500]
                log.error("vercel.deploy_fail  status=%d  body=%s", resp.status_code, error_body)
                return PublishResult(
                    platform="vercel",
                    status="failed",
                    error=f"HTTP {resp.status_code}: {error_body}",
                )

        except Exception as e:
            log.error("vercel.error  err=%s", e)
            return PublishResult(platform="vercel", status="failed", error=str(e))

    async def promote_to_production(self, deployment_id: str) -> bool:
        """Promote a preview deployment to production (live domain)."""
        params: dict[str, str] = {}
        if self.team_id:
            params["teamId"] = self.team_id

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.patch(
                    f"{VERCEL_API}/v13/deployments/{deployment_id}",
                    headers=self.headers,
                    params=params,
                    json={"target": "production"},
                )
                return resp.status_code in (200, 201)
        except Exception as e:
            log.error("vercel.promote_fail  id=%s  err=%s", deployment_id, e)
            return False

    async def list_deployments(self, limit: int = 10) -> list[dict]:
        """List recent deployments for this project."""
        params: dict = {"projectId": self.project_name, "limit": limit}
        if self.team_id:
            params["teamId"] = self.team_id

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    f"{VERCEL_API}/v6/deployments",
                    headers=self.headers,
                    params=params,
                )
                if resp.status_code == 200:
                    return resp.json().get("deployments", [])
        except Exception:
            pass
        return []


async def deploy_pages_to_vercel(
    pages: list[dict],
    promote: bool = False,
) -> list[PublishResult]:
    """Convenience function — deploy a list of generated pages to Vercel.

    Args:
        pages: [{"path": "landscape-lighting-kelowna", "html": "<html>..."}]
        promote: If True, goes live on production domain immediately

    Returns:
        List of PublishResult (one per batch — Vercel deploys all files in one shot)

    Usage:
        from execution.connectors.external.vercel import deploy_pages_to_vercel

        pages = [
            {"path": "permanent-lights-kelowna", "html": open("generated_pages/permanent-lights-kelowna.html").read()},
            {"path": "landscape-lighting-kelowna", "html": open("generated_pages/landscape-lighting-kelowna.html").read()},
        ]
        results = await deploy_pages_to_vercel(pages, promote=False)
        for r in results:
            print(f"Preview URL: {r.url}")
    """
    connector = VercelConnector()
    result = await connector.publish({"files": pages, "promote": promote})
    return [result]
