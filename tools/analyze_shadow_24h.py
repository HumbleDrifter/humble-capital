#!/usr/bin/env python3
"""Analyze the last 24 hours of shadow satellite rotation logs."""

from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

WINDOW_HOURS = 24
TOP_N = 8
DEFAULT_SERVER_LOG_PATH = Path("/root/tradingbot/satellite_rotation_shadow.jsonl")
REPO_ROOT = Path(__file__).resolve().parents[1]
REPO_LOG_PATH = REPO_ROOT / "satellite_rotation_shadow.jsonl"


def resolve_log_path() -> Path:
    env_value = os.getenv("SATELLITE_ROTATION_SHADOW_LOG_PATH", "").strip()
    candidates = []
    if env_value:
        candidates.append(Path(env_value))
    candidates.extend([REPO_LOG_PATH, DEFAULT_SERVER_LOG_PATH])
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0] if candidates else REPO_LOG_PATH


def load_recent_cycles(log_path: Path, window_hours: int) -> tuple[list[dict], int]:
    if not log_path.exists():
        return [], 0

    cutoff_ts = int(time.time() - (window_hours * 3600))
    rows: list[dict] = []
    malformed = 0

    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                malformed += 1
                continue

            logged_at = payload.get("logged_at")
            try:
                logged_at = int(logged_at)
            except Exception:
                malformed += 1
                continue

            if logged_at < cutoff_ts:
                continue

            rows.append(payload)

    return rows, malformed


def pct(count: int, total: int) -> str:
    if total <= 0:
        return "0.0%"
    return f"{(count / total) * 100:.1f}%"


def avg(values: list[int]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def summarize_counter(counter: Counter, limit: int = 8, show_pct_total: int | None = None) -> list[str]:
    if not counter:
        return ["  - none"]

    lines = []
    for name, count in counter.most_common(limit):
        suffix = f" ({pct(count, show_pct_total)})" if show_pct_total else ""
        lines.append(f"  - {name}: {count}{suffix}")
    return lines


def derive_blocked_candidates(cycle: dict) -> list[dict]:
    existing = cycle.get("blocked_high_score_candidates")
    if isinstance(existing, list) and existing:
        return existing

    live = set(cycle.get("current_system_selections") or [])
    ranked = cycle.get("ranking_by_net_score") or []
    derived = []
    for row in ranked[:TOP_N]:
        product_id = str(row.get("product_id") or "").strip()
        if not product_id or product_id in live:
            continue

        if bool(row.get("blocked")):
            reason = "blocked"
        elif not bool(row.get("allowed")) and not bool(row.get("held")):
            reason = "not_allowed"
        elif not bool(row.get("active_buy_universe")) and not bool(row.get("held")):
            reason = "not_in_active_universe"
        else:
            reason = "below_current_selection_cutoff"

        derived.append(
            {
                "product_id": product_id,
                "net_score": row.get("net_score"),
                "reason": reason,
            }
        )
    return derived


def build_takeaways(
    cycles_count: int,
    empty_live_cycles: int,
    disagreement_cycles: int,
    average_overlap: float,
    blocked_counter: Counter,
    regime_counter: Counter,
) -> list[str]:
    takeaways = []

    if cycles_count == 0:
        return ["No shadow cycles were found in the last 24 hours."]

    if empty_live_cycles:
        takeaways.append(
            f"Live selections were empty in {empty_live_cycles}/{cycles_count} cycles ({pct(empty_live_cycles, cycles_count)}), which may indicate tight live constraints."
        )

    if disagreement_cycles:
        takeaways.append(
            f"Shadow/live disagreement showed up in {disagreement_cycles}/{cycles_count} cycles ({pct(disagreement_cycles, cycles_count)})."
        )
    else:
        takeaways.append("Shadow and live selections were aligned across the sampled window.")

    if average_overlap < max(1.0, TOP_N / 3):
        takeaways.append("Average overlap between live and shadow selections is low, suggesting meaningful opportunity-cost pressure from current constraints.")

    if blocked_counter:
        top_reason = blocked_counter.most_common(1)[0][0]
        takeaways.append(f"Most blocked high-ranked candidates were excluded because of: {top_reason}.")

    if regime_counter:
        regime, count = regime_counter.most_common(1)[0]
        takeaways.append(f"The dominant regime over the window was {regime} ({pct(count, cycles_count)} of cycles).")

    return takeaways[:4] if takeaways else ["No strong takeaways yet from the sampled window."]


def main() -> int:
    log_path = resolve_log_path()
    cycles, malformed = load_recent_cycles(log_path, WINDOW_HOURS)

    print(f"Shadow Rotation Review ({WINDOW_HOURS}h)")
    print(f"Log file: {log_path}")
    print()

    if not log_path.exists():
        print("No shadow log file found.")
        return 0

    if not cycles:
        print("No shadow rotation rows found in the last 24 hours.")
        if malformed:
            print(f"Malformed rows skipped: {malformed}")
        return 0

    cycles_count = len(cycles)
    empty_live_cycles = 0
    disagreement_cycles = 0
    overlap_counts: list[int] = []
    shadow_only_counts: list[int] = []
    live_only_counts: list[int] = []
    regime_counter: Counter = Counter()
    shadow_pick_counter: Counter = Counter()
    live_pick_counter: Counter = Counter()
    blocked_candidate_counter: Counter = Counter()
    blocked_reason_counter: Counter = Counter()
    held_shadow_counter: Counter = Counter()
    active_universe_shadow_counter: Counter = Counter()

    for cycle in cycles:
        live = set(cycle.get("current_system_selections") or [])
        shadow = set(cycle.get("shadow_top_selection_ids") or [])

        if not live:
            empty_live_cycles += 1

        if live != shadow:
            disagreement_cycles += 1

        overlap_counts.append(len(live & shadow))
        shadow_only_counts.append(len(shadow - live))
        live_only_counts.append(len(live - shadow))

        regime_counter.update([str(cycle.get("market_regime") or "unknown")])
        shadow_pick_counter.update(cycle.get("shadow_top_selection_ids") or [])
        live_pick_counter.update(cycle.get("current_system_selections") or [])

        ranking = cycle.get("ranking_by_net_score") or []
        for row in ranking[:TOP_N]:
            product_id = str(row.get("product_id") or "").strip()
            if not product_id:
                continue
            if bool(row.get("held")):
                held_shadow_counter.update([product_id])
            if bool(row.get("active_buy_universe")):
                active_universe_shadow_counter.update([product_id])

        for row in derive_blocked_candidates(cycle):
            product_id = str(row.get("product_id") or "").strip()
            reason = str(row.get("reason") or "unknown").strip() or "unknown"
            if not product_id:
                continue
            blocked_candidate_counter.update([product_id])
            blocked_reason_counter.update([reason])

    print("Core Metrics")
    print(f"  Cycles analyzed: {cycles_count}")
    print(f"  Empty live-selection cycles: {empty_live_cycles} ({pct(empty_live_cycles, cycles_count)})")
    print(f"  Shadow/live disagreement cycles: {disagreement_cycles} ({pct(disagreement_cycles, cycles_count)})")
    print(f"  Average overlap count: {avg(overlap_counts):.2f}")
    print(f"  Average shadow-only count: {avg(shadow_only_counts):.2f}")
    print(f"  Average live-only count: {avg(live_only_counts):.2f}")
    if malformed:
        print(f"  Malformed rows skipped: {malformed}")
    print()

    print("Market Regime Distribution")
    print("\n".join(summarize_counter(regime_counter, limit=5, show_pct_total=cycles_count)))
    print()

    print("Top Shadow Picks")
    print("\n".join(summarize_counter(shadow_pick_counter, limit=10, show_pct_total=cycles_count)))
    print()

    print("Live Selections")
    print("\n".join(summarize_counter(live_pick_counter, limit=10, show_pct_total=cycles_count)))
    print()

    print("Blocked High-Ranked Shadow Candidates")
    print("\n".join(summarize_counter(blocked_candidate_counter, limit=10, show_pct_total=cycles_count)))
    if blocked_reason_counter:
        print("  Reasons:")
        for line in summarize_counter(blocked_reason_counter, limit=6, show_pct_total=cycles_count):
            print(line)
    print()

    print("Already-Held Shadow Candidates")
    print("\n".join(summarize_counter(held_shadow_counter, limit=10, show_pct_total=cycles_count)))
    print()

    print("Active-Universe Shadow Candidates")
    print("\n".join(summarize_counter(active_universe_shadow_counter, limit=10, show_pct_total=cycles_count)))
    print()

    print("Quick Takeaways")
    for takeaway in build_takeaways(
        cycles_count=cycles_count,
        empty_live_cycles=empty_live_cycles,
        disagreement_cycles=disagreement_cycles,
        average_overlap=avg(overlap_counts),
        blocked_counter=blocked_reason_counter,
        regime_counter=regime_counter,
    ):
        print(f"  - {takeaway}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
