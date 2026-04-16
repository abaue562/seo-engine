"""Connector registration at server / worker startup.

Call ``init_publisher()`` once during FastAPI lifespan or Celery worker startup.
After that, all tasks call ``get_publisher()`` to access the singleton.

Connectors registered:
  wordpress  — WP_URL + WP_USER + WP_APP_PASSWORD
  medium     — MEDIUM_TOKEN
  gbp        — no credentials needed (uses GBP handler directly)

To add a connector:
  1. Implement ``Connector`` subclass in execution/connectors/
  2. Add credentials to config/settings.py + config/.env.example
  3. Add a registration block below.
"""

from __future__ import annotations

import logging
import os

from execution.publisher import MultiChannelPublisher

log = logging.getLogger(__name__)

_publisher: MultiChannelPublisher | None = None


def get_publisher() -> MultiChannelPublisher:
    """Return the global publisher singleton.  Initialises if needed."""
    global _publisher
    if _publisher is None:
        _publisher = init_publisher()
    return _publisher


def init_publisher() -> MultiChannelPublisher:
    """Instantiate MultiChannelPublisher and register every configured connector.

    A connector is only registered when its required env vars are non-empty,
    so missing credentials silently skip that platform rather than crashing.

    Returns:
        A fully configured MultiChannelPublisher instance.
    """
    global _publisher
    pub = MultiChannelPublisher()
    registered: list[str] = []

    # ── WordPress ─────────────────────────────────────────────────────────────
    wp_url = os.getenv("WP_URL", "").rstrip("/")
    wp_user = os.getenv("WP_USER", "")
    wp_password = os.getenv("WP_APP_PASSWORD", "")

    if wp_url and wp_user and wp_password:
        from execution.connectors.wordpress import WordPressConnector
        pub.register("wordpress", WordPressConnector(wp_url, wp_user, wp_password))
        registered.append("wordpress")
    else:
        log.warning(
            "startup.connector_skip  platform=wordpress  "
            "reason=missing WP_URL / WP_USER / WP_APP_PASSWORD"
        )

    # ── Medium ────────────────────────────────────────────────────────────────
    medium_token = os.getenv("MEDIUM_TOKEN", "")
    if medium_token:
        from execution.connectors.external.medium import MediumConnector
        pub.register("medium", MediumConnector(medium_token))
        registered.append("medium")
    else:
        log.info("startup.connector_skip  platform=medium  reason=no MEDIUM_TOKEN")

    # ── Blogger ───────────────────────────────────────────────────────────────
    blogger_id = os.getenv("BLOGGER_BLOG_ID", "")
    if blogger_id:
        try:
            from execution.connectors.external.blogger import BloggerConnector  # type: ignore
            pub.register("blogger", BloggerConnector(blogger_id))
            registered.append("blogger")
        except ImportError:
            log.info("startup.connector_skip  platform=blogger  reason=not installed")

    # ── WordPress.com ─────────────────────────────────────────────────────────
    wp_com_site = os.getenv("WP_COM_SITE", "")
    wp_com_token = os.getenv("WP_COM_TOKEN", "")
    if wp_com_site and wp_com_token:
        try:
            from execution.connectors.external.free_blogs import WPComConnector  # type: ignore
            pub.register("wp_com", WPComConnector(wp_com_site, wp_com_token))
            registered.append("wp_com")
        except ImportError:
            log.info("startup.connector_skip  platform=wp_com  reason=not installed")

    # ── GBP ──────────────────────────────────────────────────────────────────
    gbp_enabled = os.getenv("GBP_ENABLED", "false").lower() == "true"
    if gbp_enabled:
        try:
            from execution.connectors.external.gbp import GBPConnector  # type: ignore
            pub.register("gbp", GBPConnector())
            registered.append("gbp")
        except ImportError:
            log.info("startup.connector_skip  platform=gbp  reason=not installed")

    if registered:
        log.info("startup.connectors_registered  platforms=%s", registered)
    else:
        log.warning(
            "startup.no_connectors_registered  "
            "Set WP_URL + WP_USER + WP_APP_PASSWORD in config/.env to enable publishing."
        )

    _publisher = pub
    return pub


def reset_publisher() -> None:
    """Reset the singleton (used in tests)."""
    global _publisher
    _publisher = None
