"""Behavioral Signal Engine — influences dwell time, click depth, and repeat visits.

Google watches:
  - How long people stay (dwell time)
  - How deep they click (click depth)
  - Whether they come back (repeat visits)

This engine creates internal structures that maximize these signals LEGITIMATELY
through better content, linking, and user experience.
"""

from __future__ import annotations

import logging
from signals.models import BehavioralSignal

log = logging.getLogger(__name__)


def generate_behavioral_signals(
    pages: list[dict],
    keyword: str,
) -> list[BehavioralSignal]:
    """Analyze pages and generate behavioral signal improvements."""
    signals: list[BehavioralSignal] = []

    for page in pages:
        url = page.get("url", "")
        word_count = page.get("word_count", 0)
        internal_links = page.get("internal_links", [])
        has_schema = page.get("has_schema", False)

        # Dwell time: thin content → people leave fast
        if word_count < 800:
            signals.append(BehavioralSignal(
                tactic="content_depth",
                target_page=url,
                expected_effect="dwell_time",
                implementation=f"Expand content to 1200+ words. Add FAQ section, detailed explanations, "
                              f"and embedded media to keep visitors reading longer.",
            ))

        # Click depth: no internal links → dead-end page
        if len(internal_links) < 3:
            signals.append(BehavioralSignal(
                tactic="internal_link_loop",
                target_page=url,
                expected_effect="click_depth",
                implementation=f"Add 3-5 contextual internal links to related service/blog pages. "
                              f"Create a reading path that keeps users clicking deeper into the site.",
            ))

        # Engagement: add interactive elements
        if word_count > 500 and not has_schema:
            signals.append(BehavioralSignal(
                tactic="schema_engagement",
                target_page=url,
                expected_effect="rich_results",
                implementation=f"Add FAQ schema markup to enable rich results in SERPs. "
                              f"Rich results increase CTR by 20-30%.",
            ))

    # Content chain: multi-page reading experience
    if len(pages) >= 3:
        signals.append(BehavioralSignal(
            tactic="content_chain",
            target_page="site-wide",
            expected_effect="repeat_visits",
            implementation=f"Create a 3-part content series around '{keyword}'. "
                          f"Each article links to the next with 'Read Part 2' CTAs. "
                          f"Drives multi-page visits and return traffic.",
        ))

    # CTA optimization for conversions
    signals.append(BehavioralSignal(
        tactic="cta_optimization",
        target_page="all service pages",
        expected_effect="conversions",
        implementation="Add click-to-call buttons above the fold. Add sticky mobile CTA bar. "
                      "Add exit-intent offer. Each conversion is a ranking signal.",
    ))

    log.info("behavioral.signals  keyword=%s  pages=%d  signals=%d",
             keyword, len(pages), len(signals))
    return signals


def signals_to_prompt_block(signals: list[BehavioralSignal]) -> str:
    """Render behavioral signals as agent context."""
    if not signals:
        return "BEHAVIORAL SIGNALS: No improvements identified."

    lines = ["BEHAVIORAL SIGNAL IMPROVEMENTS:"]
    for s in signals:
        lines.append(f"  [{s.expected_effect}] {s.tactic}")
        lines.append(f"    Target: {s.target_page}")
        lines.append(f"    Action: {s.implementation[:120]}")
    return "\n".join(lines)
