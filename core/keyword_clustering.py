"""Keyword Clustering — two methods: Jaccard SERP-overlap + Semantic embedding.

Method 1 (Jaccard): Clusters keywords by what Google ranks together.
    If two keywords share the same ranking URLs, they belong together.
    Based on dartseoengineer/keyword-clustering.

Method 2 (Semantic): Clusters keywords by meaning using BERT embeddings.
    Based on TopicGPT (UMAP + HDBSCAN) and evemilano's BERTopic approach.

Usage:
    from core.keyword_clustering import cluster_by_serp_overlap, cluster_by_semantics

    # Method 1: SERP overlap (needs keyword + URL data)
    clusters = cluster_by_serp_overlap(serp_data, threshold=0.6)

    # Method 2: Semantic (just needs keyword list)
    clusters = cluster_by_semantics(keywords, min_cluster_size=3)
"""

from __future__ import annotations

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


# =====================================================================
# Method 1: Jaccard SERP-Overlap Clustering
# =====================================================================

def jaccard_similarity(set1: set, set2: set) -> float:
    """Calculate Jaccard similarity between two sets."""
    if not set1 or not set2:
        return 0.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0


def cluster_by_serp_overlap(
    serp_data: list[dict],
    keyword_col: str = "keyword",
    url_col: str = "url",
    threshold: float = 0.6,
) -> list[dict]:
    """Cluster keywords by Jaccard similarity of their ranking URLs.

    Keywords that rank for the same URLs belong in the same cluster.
    This is Google's own implicit grouping — more accurate than pure NLP.

    Args:
        serp_data: List of dicts with keyword + url (one row per keyword-URL pair)
        keyword_col: Key for keyword field
        url_col: Key for URL field
        threshold: Jaccard similarity threshold (0-1, default 0.6)

    Returns:
        List of dicts: cluster_id, keywords (list), size, representative_keyword
    """
    # Group URLs by keyword
    keyword_urls: dict[str, set] = defaultdict(set)
    for row in serp_data:
        kw = row.get(keyword_col, "")
        url = row.get(url_col, "")
        if kw and url:
            keyword_urls[kw].add(url)

    # Remove keywords with no URLs
    unclustered = [kw for kw, urls in keyword_urls.items() if not urls]
    keyword_urls = {kw: urls for kw, urls in keyword_urls.items() if urls}

    # Greedy clustering
    clusters: list[list[str]] = []
    for keyword, urls in keyword_urls.items():
        placed = False
        for cluster in clusters:
            # Check similarity against any member of the cluster
            for member in cluster:
                if jaccard_similarity(urls, keyword_urls[member]) >= threshold:
                    cluster.append(keyword)
                    placed = True
                    break
            if placed:
                break
        if not placed:
            clusters.append([keyword])

    # Format output
    results = []
    for i, cluster in enumerate(clusters):
        # Representative = keyword with most URLs (broadest)
        representative = max(cluster, key=lambda kw: len(keyword_urls.get(kw, set())))
        results.append({
            "cluster_id": i,
            "keywords": cluster,
            "size": len(cluster),
            "representative": representative,
            "shared_urls": len(set.intersection(*[keyword_urls[kw] for kw in cluster])) if len(cluster) > 1 else 0,
        })

    # Add unclustered
    if unclustered:
        results.append({
            "cluster_id": -1,
            "keywords": unclustered,
            "size": len(unclustered),
            "representative": unclustered[0] if unclustered else "",
            "shared_urls": 0,
        })

    results.sort(key=lambda x: x["size"], reverse=True)
    log.info("serp_clustering.done  keywords=%d  clusters=%d  unclustered=%d",
             len(keyword_urls), len(clusters), len(unclustered))
    return results


# =====================================================================
# Method 2: Semantic Embedding Clustering (UMAP + HDBSCAN)
# =====================================================================

def cluster_by_semantics(
    keywords: list[str],
    model_name: str = "paraphrase-MiniLM-L3-v2",
    min_cluster_size: int = 3,
    n_dims: int = 5,
    min_similarity: float = 0.85,
) -> list[dict]:
    """Cluster keywords by semantic similarity using sentence embeddings.

    Uses SentenceTransformers + UMAP + HDBSCAN (TopicGPT approach).
    Falls back to simple cosine clustering if UMAP/HDBSCAN not available.

    Args:
        keywords: List of keyword strings
        model_name: SentenceTransformer model
        min_cluster_size: Minimum keywords per cluster for HDBSCAN
        n_dims: UMAP dimensions to reduce to
        min_similarity: Minimum similarity for PolyFuzz grouping (fallback)

    Returns:
        List of dicts: cluster_id, keywords, size, label
    """
    if len(keywords) < 2:
        return [{"cluster_id": 0, "keywords": keywords, "size": len(keywords), "label": keywords[0] if keywords else ""}]

    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
    except ImportError:
        log.error("keyword_clustering.missing_dep  pip install sentence-transformers numpy")
        return []

    # Encode keywords
    model = SentenceTransformer(model_name, device="cpu")
    embeddings = model.encode(keywords, show_progress_bar=False)

    # Try UMAP + HDBSCAN (best quality)
    try:
        import umap
        import hdbscan

        # Reduce dimensions
        reducer = umap.UMAP(
            n_components=min(n_dims, len(keywords) - 1),
            n_neighbors=min(15, len(keywords) - 1),
            min_dist=0,
            metric="cosine",
            random_state=42,
        )
        reduced = reducer.fit_transform(embeddings)
        reduced = reduced / np.linalg.norm(reduced, axis=1, keepdims=True)

        # Cluster
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min(min_cluster_size, max(2, len(keywords) // 5)),
            metric="euclidean",
        )
        labels = clusterer.fit_predict(reduced)

        log.info("keyword_clustering.umap_hdbscan  keywords=%d  clusters=%d", len(keywords), len(set(labels)) - (1 if -1 in labels else 0))

    except ImportError:
        # Fallback: simple cosine similarity clustering
        from sklearn.metrics.pairwise import cosine_similarity

        sim_matrix = cosine_similarity(embeddings)
        labels = np.full(len(keywords), -1)
        cluster_id = 0

        for i in range(len(keywords)):
            if labels[i] != -1:
                continue
            labels[i] = cluster_id
            for j in range(i + 1, len(keywords)):
                if labels[j] == -1 and sim_matrix[i][j] >= min_similarity:
                    labels[j] = cluster_id
            cluster_id += 1

        log.info("keyword_clustering.cosine_fallback  keywords=%d  clusters=%d", len(keywords), cluster_id)

    # Format output
    cluster_map: dict[int, list[str]] = defaultdict(list)
    for kw, label in zip(keywords, labels):
        cluster_map[int(label)].append(kw)

    results = []
    for cid, kws in sorted(cluster_map.items()):
        # Auto-label: shortest keyword or most common words
        label = min(kws, key=len) if kws else ""
        results.append({
            "cluster_id": cid,
            "keywords": kws,
            "size": len(kws),
            "label": label,
        })

    results.sort(key=lambda x: x["size"], reverse=True)
    return results
