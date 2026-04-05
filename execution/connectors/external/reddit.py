"""Reddit connector via PRAW — post to subreddits with adapted content.

Setup:
  1. Create Reddit app at reddit.com/prefs/apps (script type)
  2. Add to config/.env:
     REDDIT_CLIENT_ID=xxx
     REDDIT_CLIENT_SECRET=xxx
     REDDIT_USERNAME=xxx
     REDDIT_PASSWORD=xxx

Posts are value-first, no hard selling. System respects subreddit rules.
"""

from __future__ import annotations

import os
import logging

from execution.connectors.base import Connector, PublishResult

log = logging.getLogger(__name__)


class RedditConnector(Connector):
    platform = "reddit"

    def __init__(self):
        self.client_id = os.getenv("REDDIT_CLIENT_ID", "")
        self.client_secret = os.getenv("REDDIT_CLIENT_SECRET", "")
        self.username = os.getenv("REDDIT_USERNAME", "")
        self.password = os.getenv("REDDIT_PASSWORD", "")
        self._reddit = None

    def _get_reddit(self):
        if self._reddit:
            return self._reddit
        if not self.client_id:
            return None
        import praw
        self._reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            username=self.username,
            password=self.password,
            user_agent="SEOEngine/1.0",
        )
        return self._reddit

    async def publish(self, payload: dict) -> PublishResult:
        """Post to a subreddit."""
        reddit = self._get_reddit()
        if not reddit:
            log.warning("reddit.not_configured")
            return PublishResult(platform="reddit", status="needs_setup",
                                error="Add REDDIT_CLIENT_ID etc to config/.env")

        subreddit = payload.get("subreddit", payload.get("subreddit_suggestion", ""))
        title = payload.get("title", "")
        content = payload.get("content", payload.get("body", ""))

        if not subreddit or not title:
            return PublishResult(platform="reddit", status="failed", error="Missing subreddit or title")

        try:
            sub = reddit.subreddit(subreddit)
            post = sub.submit(title=title, selftext=content)

            log.info("reddit.posted  sub=%s  title=%s  url=%s", subreddit, title[:40], post.url)
            return PublishResult(
                platform="reddit",
                status="success",
                url=post.url,
                post_id=str(post.id),
            )
        except Exception as e:
            log.error("reddit.post_fail  sub=%s  err=%s", subreddit, e)
            return PublishResult(platform="reddit", status="failed", error=str(e))

    @staticmethod
    def is_configured() -> bool:
        return bool(os.getenv("REDDIT_CLIENT_ID"))
