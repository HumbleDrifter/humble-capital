import os
import time

from regime import get_daily_closes

_RS_CACHE_TTL_SEC = 1800
_RS_CACHE = {}
ASSET_CONFIG_PATH = os.getenv("ASSET_CONFIG_PATH", "/root/tradingbot/asset_config.json")


def _log(message):
    print(f"[core_rotation] {message}")


def _normalize_product_id(product_id):
    return str(product_id or "").upper().strip()


def _safe_float(value, default=0.0):
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _cache_key(product_id, lookback_days):
    return f"{_normalize_product_id(product_id)}|{int(lookback_days)}"


def _cache_get(product_id, lookback_days):
    key = _cache_key(product_id, lookback_days)
    entry = _RS_CACHE.get(key)
    if not isinstance(entry, dict):
        return None
    ts = _safe_float(entry.get("ts"), 0.0)
    if (time.time() - ts) >= _RS_CACHE_TTL_SEC:
        return None
    return _safe_float(entry.get("value"), 0.0)


def _cache_set(product_id, lookback_days, value):
    key = _cache_key(product_id, lookback_days)
    _RS_CACHE[key] = {
        "ts": time.time(),
        "value": _safe_float(value, 0.0),
    }


def _load_core_assets_from_config():
    import json

    try:
        with open(ASSET_CONFIG_PATH, "r", encoding="utf-8") as handle:
            config = json.load(handle) or {}
        core_assets = config.get("core_assets") or {}
        return core_assets if isinstance(core_assets, dict) else {}
    except Exception as exc:
        _log(f"failed to load asset config error={exc}")
        return {}


def get_relative_strength(product_id: str, lookback_days: int = 14) -> float:
    try:
        product_id = _normalize_product_id(product_id)
        lookback_days = max(1, int(lookback_days or 14))

        if not product_id:
            return 0.0

        cached = _cache_get(product_id, lookback_days)
        if cached is not None:
            return cached

        closes = get_daily_closes(product_id, lookback_days + 5)
        if len(closes) < lookback_days + 1:
            _cache_set(product_id, lookback_days, 0.0)
            return 0.0

        start_price = _safe_float(closes[-(lookback_days + 1)], 0.0)
        end_price = _safe_float(closes[-1], 0.0)
        if start_price <= 0:
            _cache_set(product_id, lookback_days, 0.0)
            return 0.0

        rs_return = (end_price - start_price) / start_price
        _cache_set(product_id, lookback_days, rs_return)
        return rs_return
    except Exception as exc:
        _log(f"relative strength failed product_id={product_id} error={exc}")
        return 0.0


def rank_core_assets(core_assets: dict, lookback_days: int = 14) -> list:
    try:
        core_assets = core_assets if isinstance(core_assets, dict) else {}
        ranked = []

        for product_id, config in core_assets.items():
            product_id = _normalize_product_id(product_id)
            cfg = config if isinstance(config, dict) else {}
            base_weight = _safe_float(cfg.get("target_weight"), 0.0)
            ranked.append(
                {
                    "product_id": product_id,
                    "base_weight": base_weight,
                    "rs_return": get_relative_strength(product_id, lookback_days=lookback_days),
                }
            )

        ranked.sort(key=lambda row: (_safe_float(row["rs_return"]), _safe_float(row["base_weight"])), reverse=True)
        for index, row in enumerate(ranked, start=1):
            row["rs_rank"] = index

        return ranked
    except Exception as exc:
        _log(f"rank core assets failed error={exc}")
        return []


def compute_adjusted_weights(core_assets: dict, lookback_days: int = 14, max_tilt_pct: float = 0.30) -> dict:
    try:
        ranked = rank_core_assets(core_assets, lookback_days=lookback_days)
        if not ranked:
            return {}

        max_tilt_pct = max(0.0, _safe_float(max_tilt_pct, 0.30))
        count = len(ranked)
        original_total = sum(_safe_float(row["base_weight"], 0.0) for row in ranked)

        if original_total <= 0:
            return {}

        adjustments = {}
        if count == 1:
            multipliers = [1.0]
        else:
            step = (2.0 * max_tilt_pct) / max(1, count - 1)
            multipliers = [(1.0 + max_tilt_pct) - (index * step) for index in range(count)]

        prelim = []
        for row, multiplier in zip(ranked, multipliers):
            base_weight = _safe_float(row["base_weight"], 0.0)
            prelim_weight = max(0.03, base_weight * multiplier)
            prelim.append(
                {
                    "product_id": row["product_id"],
                    "base_weight": base_weight,
                    "adjusted_weight": prelim_weight,
                    "rs_return": _safe_float(row["rs_return"], 0.0),
                }
            )

        prelim_total = sum(item["adjusted_weight"] for item in prelim)
        if prelim_total <= 0:
            return {}

        normalization = original_total / prelim_total
        for item in prelim:
            normalized = item["adjusted_weight"] * normalization
            normalized = max(0.03, normalized)
            tilt = normalized - item["base_weight"]
            adjustments[item["product_id"]] = {
                "base_weight": round(item["base_weight"], 6),
                "adjusted_weight": round(normalized, 6),
                "rs_return": round(item["rs_return"], 6),
                "tilt": round(tilt, 6),
            }

        adjusted_total = sum(v["adjusted_weight"] for v in adjustments.values())
        if adjusted_total > 0:
            final_norm = original_total / adjusted_total
            for product_id in list(adjustments.keys()):
                row = adjustments[product_id]
                adjusted_weight = max(0.03, _safe_float(row["adjusted_weight"], 0.0) * final_norm)
                row["adjusted_weight"] = round(adjusted_weight, 6)
                row["tilt"] = round(adjusted_weight - _safe_float(row["base_weight"], 0.0), 6)

        return adjustments
    except Exception as exc:
        _log(f"compute adjusted weights failed error={exc}")
        return {}


def get_rotation_recommendation() -> dict:
    try:
        core_assets = _load_core_assets_from_config()
        rankings = rank_core_assets(core_assets, lookback_days=14)
        adjustments = compute_adjusted_weights(core_assets, lookback_days=14, max_tilt_pct=0.30)

        rs_values = [_safe_float(row.get("rs_return"), 0.0) for row in rankings]
        rs_spread = (max(rs_values) - min(rs_values)) if rs_values else 0.0
        stale = rs_spread <= 0.02

        reasoning = []
        if stale:
            reasoning.append("Momentum spread across core assets is narrow; no clear leader right now.")
        elif rankings:
            strongest = rankings[0]
            weakest = rankings[-1]
            reasoning.append(
                f"Strongest core momentum: {strongest['product_id']} ({strongest['rs_return']:.2%}); "
                f"weakest: {weakest['product_id']} ({weakest['rs_return']:.2%})."
            )
            reasoning.append("Suggested weights tilt modestly toward stronger recent momentum while keeping total core exposure unchanged.")

        return {
            "ok": True,
            "lookback_days": 14,
            "max_tilt_pct": 0.30,
            "stale": stale,
            "rankings": rankings,
            "adjustments": adjustments,
            "reasoning": reasoning,
            "generated_at": int(time.time()),
        }
    except Exception as exc:
        _log(f"rotation recommendation failed error={exc}")
        return {
            "ok": False,
            "lookback_days": 14,
            "max_tilt_pct": 0.30,
            "stale": True,
            "rankings": [],
            "adjustments": {},
            "reasoning": [f"Failed to compute recommendation: {exc}"],
            "generated_at": int(time.time()),
        }
