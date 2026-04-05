"""CausalImpact SEO A/B Testing — measure if changes actually moved rankings.

Uses Google's CausalImpact (Bayesian structural time-series) to determine
if an SEO intervention had a statistically significant effect.

Usage:
    from testing.causal_impact import measure_seo_impact

    result = measure_seo_impact(
        pre_data=[100, 105, 98, 102, 110, 95, 103],  # 7 days before change
        post_data=[115, 120, 125, 118, 130, 128, 135],  # 7 days after change
    )
    print(f"Impact: {result['relative_effect']:.1%}  Significant: {result['significant']}")
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def measure_seo_impact(
    pre_data: list[float],
    post_data: list[float],
    confidence: float = 0.95,
) -> dict:
    """Measure whether an SEO change had a significant impact.

    Uses CausalImpact to build a counterfactual model of "what would
    have happened without the change" and compares to actual results.

    Args:
        pre_data: Time series before the intervention (daily clicks/traffic)
        post_data: Time series after the intervention
        confidence: Confidence level for significance testing

    Returns:
        Dict with: significant (bool), relative_effect, absolute_effect,
        predicted_without_change, actual, p_value
    """
    try:
        from causalimpact import CausalImpact
        import pandas as pd
    except ImportError:
        log.error("causal_impact.missing_dep  pip install causalimpact pandas")
        return {"error": "causalimpact not installed"}

    if len(pre_data) < 5 or len(post_data) < 3:
        return {"error": "Need at least 5 pre-intervention and 3 post-intervention data points"}

    # Build time series
    all_data = pre_data + post_data
    n_pre = len(pre_data)
    n_total = len(all_data)

    data = pd.DataFrame({"y": all_data}, index=pd.date_range("2026-01-01", periods=n_total, freq="D"))

    pre_period = [data.index[0], data.index[n_pre - 1]]
    post_period = [data.index[n_pre], data.index[-1]]

    try:
        ci = CausalImpact(data, pre_period, post_period, alpha=1 - confidence)
        summary = ci.summary_data

        # Extract key metrics
        actual_avg = sum(post_data) / len(post_data)
        predicted_avg = summary.get("average", {}).get("predicted", actual_avg)

        if hasattr(ci, "summary_data") and isinstance(ci.summary_data, dict):
            avg = ci.summary_data.get("average", {})
            abs_effect = avg.get("abs_effect", actual_avg - predicted_avg)
            rel_effect = avg.get("rel_effect", (actual_avg - predicted_avg) / max(predicted_avg, 1))
            p_value = ci.p_value if hasattr(ci, "p_value") else 0.05
        else:
            abs_effect = actual_avg - sum(pre_data) / len(pre_data)
            rel_effect = abs_effect / max(sum(pre_data) / len(pre_data), 1)
            p_value = 0.05

        significant = p_value < (1 - confidence) if isinstance(p_value, float) else False

        return {
            "significant": significant,
            "p_value": round(p_value, 4) if isinstance(p_value, float) else p_value,
            "absolute_effect": round(abs_effect, 2),
            "relative_effect": round(rel_effect, 4),
            "actual_avg": round(actual_avg, 2),
            "predicted_avg": round(predicted_avg, 2) if isinstance(predicted_avg, (int, float)) else predicted_avg,
            "pre_avg": round(sum(pre_data) / len(pre_data), 2),
            "post_avg": round(actual_avg, 2),
            "lift": f"+{rel_effect:.1%}" if rel_effect > 0 else f"{rel_effect:.1%}",
        }

    except Exception as e:
        log.error("causal_impact.fail  err=%s", e)
        # Fallback: simple before/after comparison
        pre_avg = sum(pre_data) / len(pre_data)
        post_avg = sum(post_data) / len(post_data)
        change = (post_avg - pre_avg) / max(pre_avg, 1)

        return {
            "significant": abs(change) > 0.1,  # >10% change = likely significant
            "p_value": "estimated",
            "absolute_effect": round(post_avg - pre_avg, 2),
            "relative_effect": round(change, 4),
            "actual_avg": round(post_avg, 2),
            "predicted_avg": round(pre_avg, 2),
            "pre_avg": round(pre_avg, 2),
            "post_avg": round(post_avg, 2),
            "lift": f"+{change:.1%}" if change > 0 else f"{change:.1%}",
            "method": "simple_comparison (causalimpact failed)",
        }
