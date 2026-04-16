"""WordPress REST API publisher for SEO Engine.

Supports both:
  - WordPress.com sites via XMLRPC (basic auth — no OAuth required)
  - Self-hosted WordPress via REST API + Application Passwords

Usage:
    from execution.publish.wordpress import WordPressPublisher, WPPost

    pub = WordPressPublisher(
        site_url="https://yourblog.wordpress.com",
        username="user",
        app_password="password",
    )
    result = pub.publish(WPPost(
        title="My Article",
        content="<p>Hello world</p>",
        slug="my-article",
        status="publish",
    ))
    print(result)  # {"id": 5, "url": "https://...", "status": "publish"}
"""
from __future__ import annotations

import base64
import json
import logging
import urllib.error
import urllib.request
import xmlrpc.client
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class WPPost:
    title: str
    content: str
    slug: str = ""
    status: str = "publish"
    focus_keyword: str = ""
    meta_description: str = ""
    excerpt: str = ""
    categories: list = field(default_factory=list)
    tags: list = field(default_factory=list)


class WordPressPublisher:
    """Publishes posts to WordPress via XMLRPC (primary) or REST API (fallback).

    WordPress.com free sites require XMLRPC since the WP REST API via
    public-api.wordpress.com only supports read operations with basic auth.
    Self-hosted WordPress with Application Passwords works via REST.
    """

    def __init__(self, site_url: str, username: str, app_password: str):
        self.site_url = site_url.rstrip("/")
        self.username = username
        self.app_password = app_password
        self.credentials = base64.b64encode(
            f"{username}:{app_password}".encode()
        ).decode()
        self._is_wpcom = "wordpress.com" in site_url

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def publish(self, post: WPPost) -> dict:
        """Publish a post. Returns dict with id, url, status (or error key)."""
        if self._is_wpcom:
            return self._publish_xmlrpc(post)
        return self._publish_rest(post)

    # ------------------------------------------------------------------
    # XMLRPC (WordPress.com free sites)
    # ------------------------------------------------------------------

    def _publish_xmlrpc(self, post: WPPost) -> dict:
        xmlrpc_url = f"{self.site_url}/xmlrpc.php"
        server = xmlrpc.client.ServerProxy(xmlrpc_url)

        post_data = {
            "post_title": post.title,
            "post_content": post.content,
            "post_status": post.status,
            "post_name": post.slug or _slugify(post.title),
            "post_type": "post",
        }
        if post.excerpt:
            post_data["post_excerpt"] = post.excerpt

        try:
            post_id = server.wp.newPost(1, self.username, self.app_password, post_data)
            post_id = str(post_id)
            fetched = server.wp.getPost(1, self.username, self.app_password, post_id)
            url = fetched.get("link", f"{self.site_url}/?p={post_id}")
            log.info("wp.xmlrpc.published  id=%s  url=%s", post_id, url)
            return {
                "id": post_id,
                "url": url,
                "status": fetched.get("post_status", post.status),
            }
        except xmlrpc.client.Fault as e:
            log.error("wp.xmlrpc.fault  code=%s  msg=%s", e.faultCode, e.faultString)
            return {"error": f"XMLRPC Fault {e.faultCode}", "detail": e.faultString}
        except Exception as ex:
            log.error("wp.xmlrpc.error  err=%s", ex)
            return {"error": str(ex)}

    # ------------------------------------------------------------------
    # REST API (self-hosted WordPress with Application Passwords)
    # ------------------------------------------------------------------

    def _publish_rest(self, post: WPPost) -> dict:
        payload: dict = {
            "title": post.title,
            "content": post.content,
            "status": post.status,
            "slug": post.slug or _slugify(post.title),
        }
        if post.excerpt:
            payload["excerpt"] = post.excerpt
        if post.categories:
            payload["categories"] = post.categories
        if post.tags:
            payload["tags"] = post.tags
        if post.focus_keyword or post.meta_description:
            payload["meta"] = {
                "yoast_wpseo_focuskw": post.focus_keyword,
                "yoast_wpseo_metadesc": post.meta_description,
            }

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{self.site_url}/wp-json/wp/v2/posts",
            data=data,
            headers={
                "Authorization": f"Basic {self.credentials}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read())
                post_id = str(result.get("id", ""))
                url = result.get("link", "")
                log.info("wp.rest.published  id=%s  url=%s", post_id, url)
                return {
                    "id": post_id,
                    "url": url,
                    "status": result.get("status"),
                }
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            log.error("wp.rest.fail  code=%s  body=%s", e.code, body[:200])
            return {"error": f"HTTP {e.code}", "detail": body[:200]}
        except Exception as ex:
            log.error("wp.rest.error  err=%s", ex)
            return {"error": str(ex)}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _slugify(text: str) -> str:
    import re
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    return text[:60].strip("-")
