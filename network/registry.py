"""Network Registry — tracks all distribution nodes (owned, semi-owned, external).

Layer 1: Owned (WordPress sites, micro-sites) — full control
Layer 2: Semi-Owned (Medium, Substack) — account control
Layer 3: External (Reddit, Quora, forums) — participation

Each node has identity, reputation, and scheduling rules.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pydantic import BaseModel, Field

from data.storage.database import Database

log = logging.getLogger(__name__)


class NetworkNode(BaseModel):
    """One distribution point in the network."""
    id: str = ""
    layer: str = "external"          # owned / semi_owned / external
    platform: str = ""               # wordpress, medium, reddit, quora, directory
    url: str = ""
    account: str = ""
    # Identity
    tone: str = "professional"       # professional / casual / educational / authority
    style: str = "informative"       # informative / discussion / answer
    # Reputation
    age_days: int = 0
    total_posts: int = 0
    avg_engagement: float = 0.0
    reputation_score: float = 5.0    # 0-10
    # Limits
    max_posts_per_day: int = 1
    posts_today: int = 0
    last_post: datetime | None = None
    # Performance
    total_traffic_driven: int = 0
    total_links_placed: int = 0
    active: bool = True


class NetworkRegistry:
    """Manages all distribution nodes."""

    def __init__(self, db: Database | None = None):
        self.db = db or Database()
        self._nodes: dict[str, NetworkNode] = {}

    def register_node(self, node: NetworkNode) -> None:
        self._nodes[node.id] = node
        log.info("registry.add  id=%s  platform=%s  layer=%s", node.id, node.platform, node.layer)

    def get_active_nodes(self, layer: str | None = None, platform: str | None = None) -> list[NetworkNode]:
        nodes = [n for n in self._nodes.values() if n.active]
        if layer:
            nodes = [n for n in nodes if n.layer == layer]
        if platform:
            nodes = [n for n in nodes if n.platform == platform]
        return nodes

    def can_post(self, node_id: str) -> bool:
        node = self._nodes.get(node_id)
        if not node or not node.active:
            return False
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if node.last_post and node.last_post.strftime("%Y-%m-%d") == today:
            return node.posts_today < node.max_posts_per_day
        return True

    def record_post(self, node_id: str) -> None:
        node = self._nodes.get(node_id)
        if node:
            today = datetime.utcnow().strftime("%Y-%m-%d")
            if not node.last_post or node.last_post.strftime("%Y-%m-%d") != today:
                node.posts_today = 0
            node.posts_today += 1
            node.total_posts += 1
            node.last_post = datetime.utcnow()

    def get_best_nodes_for_campaign(self, count: int = 4) -> list[NetworkNode]:
        """Get the highest-reputation active nodes for a campaign."""
        active = [n for n in self._nodes.values() if n.active and self.can_post(n.id)]
        active.sort(key=lambda n: n.reputation_score, reverse=True)
        return active[:count]

    def get_link_strategy(self, node: NetworkNode) -> str:
        """Determine link strategy based on node reputation and layer."""
        if node.reputation_score < 3:
            return "none"  # Low rep = no links, build trust first
        if node.layer == "external":
            return "soft"  # External = soft mentions only
        if node.layer == "semi_owned":
            return "soft"  # Semi-owned = contextual links
        return "direct"    # Owned = direct links

    def network_summary(self) -> dict:
        return {
            "total_nodes": len(self._nodes),
            "active": len([n for n in self._nodes.values() if n.active]),
            "owned": len([n for n in self._nodes.values() if n.layer == "owned"]),
            "semi_owned": len([n for n in self._nodes.values() if n.layer == "semi_owned"]),
            "external": len([n for n in self._nodes.values() if n.layer == "external"]),
            "total_posts": sum(n.total_posts for n in self._nodes.values()),
            "total_traffic": sum(n.total_traffic_driven for n in self._nodes.values()),
        }
