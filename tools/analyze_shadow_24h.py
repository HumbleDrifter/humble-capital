#!/usr/bin/env python3
"""Analyze the last 24 hours of shadow satellite rotation logs."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shadow_rotation_report import WINDOW_HOURS, build_shadow_rotation_report, pct_text


def format_rows(rows, key, show_pct=True):
    if not rows:
        return ["  - none"]
    lines = []
    for row in rows:
        label = row.get(key) or "unknown"
        suffix = f" ({row.get('pct', 0.0):.1f}%)" if show_pct else ""
        lines.append(f"  - {label}: {row.get('count', 0)}{suffix}")
    return lines


def main() -> int:
    report = build_shadow_rotation_report(window_hours=WINDOW_HOURS)

    print(f"Shadow Rotation Review ({WINDOW_HOURS}h)")
    print(f"Log file: {report.get('log_path')}")
    print()

    if report.get("cycles_analyzed", 0) <= 0:
        takeaway = (report.get("quick_takeaways") or ["No shadow rotation rows found in the last 24 hours."])[0]
        print(takeaway)
        malformed = report.get("malformed_rows_skipped", 0)
        if malformed:
            print(f"Malformed rows skipped: {malformed}")
        return 0

    print("Core Metrics")
    print(f"  Cycles analyzed: {report.get('cycles_analyzed', 0)}")
    print(
        f"  Empty live-selection cycles: {report.get('empty_live_selection_cycles', 0)} "
        f"({pct_text(report.get('empty_live_selection_cycles', 0), report.get('cycles_analyzed', 0))})"
    )
    print(
        f"  Shadow/live disagreement cycles: {report.get('shadow_live_disagreement_cycles', 0)} "
        f"({pct_text(report.get('shadow_live_disagreement_cycles', 0), report.get('cycles_analyzed', 0))})"
    )
    print(f"  Average overlap count: {report.get('average_overlap_count', 0.0):.2f}")
    print(f"  Average shadow-only count: {report.get('average_shadow_only_count', 0.0):.2f}")
    print(f"  Average live-only count: {report.get('average_live_only_count', 0.0):.2f}")
    if report.get("malformed_rows_skipped", 0):
        print(f"  Malformed rows skipped: {report.get('malformed_rows_skipped', 0)}")
    print()

    print("Market Regime Distribution")
    print("\n".join(format_rows(report.get("market_regime_distribution", []), "regime")))
    print()

    print("Top Shadow Picks")
    print("\n".join(format_rows(report.get("top_shadow_picks", []), "product_id")))
    print()

    print("Live Selections")
    print("\n".join(format_rows(report.get("live_selections", []), "product_id")))
    print()

    print("Blocked High-Ranked Shadow Candidates")
    print("\n".join(format_rows(report.get("blocked_high_ranked_shadow_candidates", []), "product_id")))
    if report.get("blocked_reason_breakdown"):
        print("  Reasons:")
        for line in format_rows(report.get("blocked_reason_breakdown", []), "reason"):
            print(line)
    print()

    print("Already-Held Shadow Candidates")
    print("\n".join(format_rows(report.get("already_held_shadow_candidates", []), "product_id")))
    print()

    print("Active-Universe Shadow Candidates")
    print("\n".join(format_rows(report.get("active_universe_shadow_candidates", []), "product_id")))
    print()

    print("Quick Takeaways")
    for takeaway in report.get("quick_takeaways", []):
        print(f"  - {takeaway}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
