from __future__ import annotations

import json
import os
import time
from collections import Counter
from pathlib import Path

WINDOW_HOURS = 24
TOP_N = 8
# First conservative tuning pass: slightly widen review eligibility based on near-miss monitoring.
SHADOW_ELIGIBILITY_MIN_NET_SCORE = 65.0
SHADOW_ELIGIBILITY_MIN_REGIME_FIT = 50.0
SHADOW_ELIGIBILITY_MAX_OVEREXTENSION_PENALTY = 14.0
SHADOW_ELIGIBLE_CONFIDENCE_BANDS = {"medium", "high"}
SHADOW_ELIGIBLE_LIQUIDITY_BUCKETS = {"high", "medium"}
SHADOW_INELIGIBLE_VOLATILITY_BUCKETS = {"extreme"}
SHADOW_NEAR_MISS_MIN_NET_SCORE = 55.0
DEFAULT_SERVER_LOG_PATH = Path("/root/tradingbot/satellite_rotation_shadow.jsonl")
DEFAULT_SERVER_CONFIG_PATH = Path("/root/tradingbot/asset_config.json")
REPO_ROOT = Path(__file__).resolve().parent
REPO_LOG_PATH = REPO_ROOT / "satellite_rotation_shadow.jsonl"
REPO_CONFIG_PATH = REPO_ROOT / "asset_config.json"


def resolve_log_path(log_path: str | Path | None = None) -> Path:
    if log_path:
        return Path(log_path)

    env_value = os.getenv("SATELLITE_ROTATION_SHADOW_LOG_PATH", "").strip()
    candidates = []
    if env_value:
        candidates.append(Path(env_value))
    candidates.extend([REPO_LOG_PATH, DEFAULT_SERVER_LOG_PATH])
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0] if candidates else REPO_LOG_PATH


def resolve_asset_config_path(config_path: str | Path | None = None) -> Path:
    if config_path:
        return Path(config_path)

    env_value = os.getenv("ASSET_CONFIG_PATH", "").strip()
    candidates = []
    if env_value:
        candidates.append(Path(env_value))
    candidates.extend([REPO_CONFIG_PATH, DEFAULT_SERVER_CONFIG_PATH])
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0] if candidates else REPO_CONFIG_PATH


def load_asset_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}

    try:
        return json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_recent_cycles(log_path: Path, window_hours: int = WINDOW_HOURS) -> tuple[list[dict], int]:
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


def pct_value(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((count / total) * 100.0, 1)


def pct_text(count: int, total: int) -> str:
    return f"{pct_value(count, total):.1f}%"


def avg(values: list[int]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def summarize_counter(counter: Counter, limit: int = 8, show_pct_total: int | None = None, key_name: str = "name") -> list[dict]:
    rows = []
    for name, count in counter.most_common(limit):
        row = {
            key_name: name,
            "count": count,
        }
        if show_pct_total:
            row["pct"] = pct_value(count, show_pct_total)
        rows.append(row)
    return rows


def derive_blocked_candidates(cycle: dict, top_n: int = TOP_N) -> list[dict]:
    existing = cycle.get("blocked_high_score_candidates")
    if isinstance(existing, list) and existing:
        return existing

    live = set(cycle.get("current_system_selections") or [])
    ranked = cycle.get("ranking_by_net_score") or []
    derived = []
    for row in ranked[:top_n]:
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


def evaluate_shadow_candidate(row: dict) -> dict:
    product_id = str(row.get("product_id") or "").strip()
    net_score = row.get("net_score")
    confidence_band = str(row.get("confidence_band") or "").strip().lower()
    liquidity_bucket = str(row.get("liquidity_bucket") or "").strip().lower()
    volatility_bucket = str(row.get("volatility_bucket") or "").strip().lower()
    regime_fit_score = row.get("regime_fit_score")
    overextension_penalty = row.get("overextension_penalty")
    allowed = bool(row.get("allowed"))
    blocked = bool(row.get("blocked"))
    active_buy_universe = bool(row.get("active_buy_universe"))

    net_score_numeric = float(net_score or 0.0)
    regime_fit_numeric = float(regime_fit_score or 0.0)
    overextension_numeric = float(overextension_penalty or 0.0)

    if blocked:
        shadow_block_reason = "blocked_by_policy"
    elif allowed:
        shadow_block_reason = "already_allowed"
    elif not active_buy_universe:
        shadow_block_reason = "not_in_active_universe"
    else:
        shadow_block_reason = "not_allowed"

    if blocked:
        eligibility_reason = "Blocked by policy and excluded from review promotion."
    elif net_score_numeric < SHADOW_ELIGIBILITY_MIN_NET_SCORE:
        eligibility_reason = "Below the review score threshold."
        shadow_block_reason = "low_score"
    elif confidence_band not in SHADOW_ELIGIBLE_CONFIDENCE_BANDS:
        eligibility_reason = "Confidence is still below the review threshold."
        shadow_block_reason = "low_score"
    elif liquidity_bucket not in SHADOW_ELIGIBLE_LIQUIDITY_BUCKETS:
        eligibility_reason = "Liquidity is too weak for review promotion."
        shadow_block_reason = "low_liquidity"
    elif volatility_bucket in SHADOW_INELIGIBLE_VOLATILITY_BUCKETS:
        eligibility_reason = "Volatility is too extreme for a safe allowlist review."
        shadow_block_reason = "extreme_volatility"
    elif overextension_numeric > SHADOW_ELIGIBILITY_MAX_OVEREXTENSION_PENALTY:
        eligibility_reason = "Recent extension risk is still too elevated."
        shadow_block_reason = "overextended"
    elif regime_fit_numeric < SHADOW_ELIGIBILITY_MIN_REGIME_FIT:
        eligibility_reason = "Regime fit is too weak for review promotion."
        shadow_block_reason = "low_regime_fit"
    elif allowed:
        eligibility_reason = "Already allowed in the live review set."
    elif not active_buy_universe:
        eligibility_reason = "Meets review thresholds but is not yet live in the active universe."
    else:
        eligibility_reason = "Meets score, liquidity, volatility, and regime-fit review thresholds."

    shadow_eligible = (
        not blocked
        and not allowed
        and net_score_numeric >= SHADOW_ELIGIBILITY_MIN_NET_SCORE
        and confidence_band in SHADOW_ELIGIBLE_CONFIDENCE_BANDS
        and liquidity_bucket in SHADOW_ELIGIBLE_LIQUIDITY_BUCKETS
        and volatility_bucket not in SHADOW_INELIGIBLE_VOLATILITY_BUCKETS
        and overextension_numeric <= SHADOW_ELIGIBILITY_MAX_OVEREXTENSION_PENALTY
        and regime_fit_numeric >= SHADOW_ELIGIBILITY_MIN_REGIME_FIT
    )

    return {
        "product_id": product_id,
        "net_score": round(net_score_numeric, 2),
        "confidence_band": confidence_band or "unknown",
        "liquidity_bucket": liquidity_bucket or "unknown",
        "volatility_bucket": volatility_bucket or "unknown",
        "active_buy_universe": active_buy_universe,
        "shadow_eligible": shadow_eligible,
        "shadow_eligibility_reason": eligibility_reason,
        "shadow_block_reason": shadow_block_reason,
        "primary_fail_reason": "" if shadow_eligible else shadow_block_reason,
        "fail_explanation": "" if shadow_eligible else eligibility_reason,
    }


def resolve_dynamic_top_n(
    cycles: list[dict],
    configured_max_active: int | None = None,
    config_path: str | Path | None = None,
) -> int:
    if isinstance(configured_max_active, int) and configured_max_active > 0:
        return configured_max_active

    config = load_asset_config(resolve_asset_config_path(config_path))
    configured = config.get("max_active_satellites")
    try:
        configured = int(configured)
    except Exception:
        configured = None
    if configured and configured > 0:
        return configured

    meme_rotation_cfg = config.get("meme_rotation", {}) if isinstance(config.get("meme_rotation"), dict) else {}
    configured_meme = meme_rotation_cfg.get("max_active")
    try:
        configured_meme = int(configured_meme)
    except Exception:
        configured_meme = None
    if configured_meme and configured_meme > 0:
        return configured_meme

    live_counts = [
        len(cycle.get("current_system_selections") or [])
        for cycle in cycles
        if len(cycle.get("current_system_selections") or []) > 0
    ]
    if live_counts:
        return max(live_counts)

    return TOP_N


def build_takeaways(
    cycles_count: int,
    empty_live_cycles: int,
    disagreement_cycles: int,
    average_overlap: float,
    blocked_counter: Counter,
    regime_counter: Counter,
    top_n: int = TOP_N,
) -> list[str]:
    takeaways = []

    if cycles_count == 0:
        return ["No shadow cycles were found in the last 24 hours."]

    if empty_live_cycles:
        takeaways.append(
            f"Live selections were empty in {empty_live_cycles}/{cycles_count} cycles ({pct_text(empty_live_cycles, cycles_count)}), which may indicate tight live constraints."
        )

    if disagreement_cycles:
        takeaways.append(
            f"Shadow/live disagreement showed up in {disagreement_cycles}/{cycles_count} cycles ({pct_text(disagreement_cycles, cycles_count)})."
        )
    else:
        takeaways.append("Shadow and live selections were aligned across the sampled window.")

    if average_overlap < max(1.0, top_n / 3):
        takeaways.append("Average overlap between live and shadow selections is low, suggesting meaningful opportunity-cost pressure from current constraints.")

    if blocked_counter:
        top_reason = blocked_counter.most_common(1)[0][0]
        takeaways.append(f"Most blocked high-ranked candidates were excluded because of: {top_reason}.")

    if regime_counter:
        regime, count = regime_counter.most_common(1)[0]
        takeaways.append(f"The dominant regime over the window was {regime} ({pct_text(count, cycles_count)} of cycles).")

    return takeaways[:4] if takeaways else ["No strong takeaways yet from the sampled window."]


def build_shadow_rotation_report(
    window_hours: int = WINDOW_HOURS,
    top_n: int | None = None,
    log_path: str | Path | None = None,
    configured_max_active: int | None = None,
    config_path: str | Path | None = None,
) -> dict:
    resolved_log_path = resolve_log_path(log_path)
    cycles, malformed = load_recent_cycles(resolved_log_path, window_hours)
    resolved_top_n = resolve_dynamic_top_n(cycles, configured_max_active=configured_max_active, config_path=config_path) if top_n is None else max(1, int(top_n))

    if not resolved_log_path.exists():
        return {
            "ok": True,
            "window_hours": window_hours,
            "top_n": resolved_top_n,
            "log_path": str(resolved_log_path),
            "generated_at": int(time.time()),
            "last_updated_ts": None,
            "cycles_analyzed": 0,
            "malformed_rows_skipped": malformed,
            "empty_live_selection_cycles": 0,
            "empty_live_selection_rate_pct": 0.0,
            "shadow_live_disagreement_cycles": 0,
            "shadow_live_disagreement_rate_pct": 0.0,
            "average_overlap_count": 0.0,
            "average_shadow_only_count": 0.0,
            "average_live_only_count": 0.0,
            "market_regime_distribution": [],
            "top_shadow_picks": [],
            "live_selections": [],
            "blocked_high_ranked_shadow_candidates": [],
            "blocked_reason_breakdown": [],
            "already_held_shadow_candidates": [],
            "active_universe_shadow_candidates": [],
            "shadow_eligible_candidates": [],
            "shadow_near_miss_candidates": [],
            "quick_takeaways": ["No shadow log file found."],
        }

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
    last_updated_ts = None

    for cycle in cycles:
        logged_at = cycle.get("logged_at")
        if isinstance(logged_at, int):
            last_updated_ts = max(last_updated_ts or 0, logged_at)

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
        for row in ranking[:resolved_top_n]:
            product_id = str(row.get("product_id") or "").strip()
            if not product_id:
                continue
            if bool(row.get("held")):
                held_shadow_counter.update([product_id])
            if bool(row.get("active_buy_universe")):
                active_universe_shadow_counter.update([product_id])

        for row in derive_blocked_candidates(cycle, top_n=resolved_top_n):
            product_id = str(row.get("product_id") or "").strip()
            reason = str(row.get("reason") or "unknown").strip() or "unknown"
            if not product_id:
                continue
            blocked_candidate_counter.update([product_id])
            blocked_reason_counter.update([reason])

    average_overlap = round(avg(overlap_counts), 2)
    average_shadow_only = round(avg(shadow_only_counts), 2)
    average_live_only = round(avg(live_only_counts), 2)
    latest_cycle = max(cycles, key=lambda cycle: int(cycle.get("logged_at") or 0)) if cycles else None
    latest_ranking = list((latest_cycle or {}).get("ranking_by_net_score") or [])
    shadow_eligible_candidates = [
        evaluate_shadow_candidate(row)
        for row in latest_ranking
        if str(row.get("product_id") or "").strip()
    ]
    shadow_eligible_candidates = [
        row for row in shadow_eligible_candidates
        if row.get("shadow_eligible")
    ][:resolved_top_n]
    shadow_near_miss_candidates = [
        row for row in [
            evaluate_shadow_candidate(row)
            for row in latest_ranking
            if str(row.get("product_id") or "").strip()
        ]
        if (
            not row.get("shadow_eligible")
            and row.get("primary_fail_reason") not in {"blocked_by_policy", "already_allowed"}
            and float(row.get("net_score", 0.0) or 0.0) >= SHADOW_NEAR_MISS_MIN_NET_SCORE
        )
    ][:resolved_top_n]

    return {
        "ok": True,
        "window_hours": window_hours,
        "top_n": resolved_top_n,
        "log_path": str(resolved_log_path),
        "generated_at": int(time.time()),
        "last_updated_ts": last_updated_ts,
        "cycles_analyzed": cycles_count,
        "malformed_rows_skipped": malformed,
        "empty_live_selection_cycles": empty_live_cycles,
        "empty_live_selection_rate_pct": pct_value(empty_live_cycles, cycles_count),
        "shadow_live_disagreement_cycles": disagreement_cycles,
        "shadow_live_disagreement_rate_pct": pct_value(disagreement_cycles, cycles_count),
        "average_overlap_count": average_overlap,
        "average_shadow_only_count": average_shadow_only,
        "average_live_only_count": average_live_only,
        "market_regime_distribution": summarize_counter(regime_counter, limit=5, show_pct_total=cycles_count, key_name="regime"),
        "top_shadow_picks": summarize_counter(shadow_pick_counter, limit=10, show_pct_total=cycles_count, key_name="product_id"),
        "live_selections": summarize_counter(live_pick_counter, limit=10, show_pct_total=cycles_count, key_name="product_id"),
        "blocked_high_ranked_shadow_candidates": summarize_counter(blocked_candidate_counter, limit=10, show_pct_total=cycles_count, key_name="product_id"),
        "blocked_reason_breakdown": summarize_counter(blocked_reason_counter, limit=6, show_pct_total=cycles_count, key_name="reason"),
        "already_held_shadow_candidates": summarize_counter(held_shadow_counter, limit=10, show_pct_total=cycles_count, key_name="product_id"),
        "active_universe_shadow_candidates": summarize_counter(active_universe_shadow_counter, limit=10, show_pct_total=cycles_count, key_name="product_id"),
        "shadow_eligible_candidates": shadow_eligible_candidates,
        "shadow_near_miss_candidates": shadow_near_miss_candidates,
        "quick_takeaways": build_takeaways(
            cycles_count=cycles_count,
            empty_live_cycles=empty_live_cycles,
            disagreement_cycles=disagreement_cycles,
            average_overlap=average_overlap,
            blocked_counter=blocked_reason_counter,
            regime_counter=regime_counter,
            top_n=resolved_top_n,
        ),
    }
