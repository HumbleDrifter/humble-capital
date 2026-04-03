import json
import math
import os
import time
import threading
from pathlib import Path
from dotenv import load_dotenv

from storage import (
    get_all_positions,
    get_asset_state,
    get_portfolio_history_since,
    insert_portfolio_snapshot,
)
from execution import get_best_bid_ask, get_client
from regime import get_market_regime

load_dotenv("/root/tradingbot/.env")

CONFIG_PATH = "/root/tradingbot/asset_config.json"
MEME_ROTATION_PATH = "/root/tradingbot/meme_rotation.json"

STABLECOIN_PRODUCTS = {"USDC-USD", "USDT-USD", "DAI-USD"}
STABLECOIN_CURRENCIES = {"USDC", "USDT", "DAI"}

PORTFOLIO_CACHE_TTL_SEC = float(os.getenv("PORTFOLIO_CACHE_TTL_SEC", "20"))
PORTFOLIO_CACHE_STALE_SEC = float(os.getenv("PORTFOLIO_CACHE_STALE_SEC", "180"))
PORTFOLIO_HISTORY_MIN_INTERVAL_SEC = float(os.getenv("PORTFOLIO_HISTORY_MIN_INTERVAL_SEC", "120"))
PORTFOLIO_HISTORY_MIN_CHANGE_USD = float(os.getenv("PORTFOLIO_HISTORY_MIN_CHANGE_USD", "1.0"))

_PORTFOLIO_CACHE_LOCK = threading.Lock()
_PORTFOLIO_CACHE = {
    "snapshot": None,
    "cached_at": 0.0,
    "last_error": None,
}


def _log_portfolio(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [portfolio] {msg}")


def _default_asset_config():
    return {
        "core_assets": {
            "BTC-USD": {"target_weight": 0.25, "rebalance_band": 0.03},
            "ETH-USD": {"target_weight": 0.15, "rebalance_band": 0.02},
            "XRP-USD": {"target_weight": 0.10, "rebalance_band": 0.02},
        },
        "satellite_mode": "rotation",
        "satellite_defaults": {"max_weight": 0.12},
        "volatility_buckets": {
            "very_high": {"max_weight": 0.05},
            "high": {"max_weight": 0.08},
            "medium": {"max_weight": 0.12},
        },
        "satellite_volatility_map": {},
        "satellite_allowed": [],
        "satellite_blocked": [],
        "satellite_total_target": 0.50,
        "satellite_total_max": 0.50,
        "regime_satellite_caps": {
            "bull": 0.50,
            "neutral": 0.40,
            "risk_off": 0.25,
        },
        "dust_min_value_usd": 2.0,
        "trade_min_value_usd": 10.0,
        "min_cash_reserve": 0.05,
        "core_buy_fraction_of_shortfall": 0.25,
        "drawdown_controls": {
            "warn_level": 0.10,
            "reduce_level": 0.15,
            "freeze_level": 0.20,
        },
        "profit_harvest": {
            "enabled": True,
            "satellite_only": True,
            "cooldown_hours": 24,
            "min_harvest_usd": 15,
            "routes": {
                "BTC-USD": 0.40,
                "ETH-USD": 0.40,
                "CASH": 0.20,
            },
            "tiers": [
                {"gain_pct": 1.0, "trim_pct": 0.25},
                {"gain_pct": 0.5, "trim_pct": 0.20},
                {"gain_pct": 0.25, "trim_pct": 0.15},
            ],
        },
	 "sniper_mode": {
            "enabled": True,
            "min_score": 85,
            "buy_scale": 0.35,
            "allow_in_regimes": ["bull", "neutral"],
            "require_sniper_eligible": True,
            "block_pump_protected": True,
        },
        "meme_rotation": {
            "enabled": True,
            "min_score": 60,
            "max_active": 8,
            "update_source": "ui_or_external_feed",
        },
        "sell_policy_defaults": {
            "core": {
                "preserve_tradable_remainder": True,
                "allow_full_exit_on_small_remainder": False,
                "min_meaningful_sell_usd": 5,
            },
            "satellite_active": {
                "preserve_tradable_remainder": True,
                "allow_full_exit_on_small_remainder": False,
                "min_meaningful_sell_usd": 5,
            },
            "satellite_blocked": {
                "preserve_tradable_remainder": False,
                "allow_full_exit_on_small_remainder": True,
                "min_meaningful_sell_usd": 5,
            },
            "nontradable": {
                "preserve_tradable_remainder": False,
                "allow_full_exit_on_small_remainder": True,
                "min_meaningful_sell_usd": 0,
            },
            "dust": {
                "preserve_tradable_remainder": False,
                "allow_full_exit_on_small_remainder": True,
                "min_meaningful_sell_usd": 0,
            },
            "unknown": {
                "preserve_tradable_remainder": False,
                "allow_full_exit_on_small_remainder": True,
                "min_meaningful_sell_usd": 5,
            },
        },
        "sell_policy_overrides": {},
    }


def load_asset_config():
    path = Path(CONFIG_PATH)
    if not path.exists():
        data = _default_asset_config()
        save_asset_config(data)
        return data
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_asset_config(config):
    Path(CONFIG_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, sort_keys=False)


def default_meme_rotation():
    return {
        "updated_at": int(time.time()),
        "candidates": [
            {"product_id": "DOGE-USD", "score": 90, "enabled": True, "source": "manual"},
            {"product_id": "SHIB-USD", "score": 80, "enabled": True, "source": "manual"},
            {"product_id": "PEPE-USD", "score": 78, "enabled": True, "source": "manual"},
            {"product_id": "BONK-USD", "score": 72, "enabled": True, "source": "manual"},
        ],
    }


def load_meme_rotation():
    path = Path(MEME_ROTATION_PATH)
    if not path.exists():
        data = default_meme_rotation()
        save_meme_rotation(data)
        return data
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_meme_rotation(data):
    Path(MEME_ROTATION_PATH).parent.mkdir(parents=True, exist_ok=True)
    if "updated_at" not in data:
        data["updated_at"] = int(time.time())
    with open(MEME_ROTATION_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=False)


def _to_dict(x):
    return x.to_dict() if hasattr(x, "to_dict") else x


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return 0.0


def get_mid_price(product_id):
    try:
        bid, ask = get_best_bid_ask(product_id)
        return (float(bid) + float(ask)) / 2.0
    except Exception:
        return 0.0


def get_cash_breakdown():
    client = get_client()
    accounts = []
    cursor = None

    while True:
        resp = _to_dict(client.get_accounts(cursor=cursor) if cursor else client.get_accounts())
        accounts.extend(resp.get("accounts", []))
        if not resp.get("has_next"):
            break
        cursor = resp.get("cursor")

    breakdown = {"USD": 0.0, "USDC": 0.0, "USDT": 0.0, "DAI": 0.0}

    for acct in accounts:
        currency = str(acct.get("currency") or "").upper()
        available_balance = acct.get("available_balance") or {}
        total_balance = acct.get("balance") or {}
        available = safe_float(available_balance.get("value", available_balance.get("amount", 0)))
        total = safe_float(total_balance.get("value", total_balance.get("amount", 0)))
        amount = total if total > 0 else available
        if currency in breakdown:
            breakdown[currency] += amount

    breakdown["TOTAL_CASH_EQUIV_USD"] = (
        breakdown["USD"] + breakdown["USDC"] + breakdown["USDT"] + breakdown["DAI"]
    )
    return breakdown


def calculate_exposure(product_id, qty_total, qty_liquid, qty_locked):
    price = get_mid_price(product_id)
    return {
        "product_id": product_id,
        "base_qty_total": qty_total,
        "base_qty_liquid": qty_liquid,
        "base_qty_locked": qty_locked,
        "price_usd": price,
        "value_total_usd": qty_total * price,
        "value_liquid_usd": qty_liquid * price,
        "value_locked_usd": qty_locked * price,
    }


def get_position_values():
    positions = get_all_positions()
    values = {}
    for pos in positions:
        product_id = pos["product_id"]
        if product_id in STABLECOIN_PRODUCTS:
            continue
        total_qty = safe_float(pos.get("base_qty_total", 0))
        liquid_qty = safe_float(pos.get("base_qty_liquid", 0))
        locked_qty = safe_float(pos.get("base_qty_locked", 0))
        if total_qty <= 0:
            continue
        values[product_id] = calculate_exposure(product_id, total_qty, liquid_qty, locked_qty)
    return values


def get_rotation_products(snapshot=None):
    config = load_asset_config() if snapshot is None else snapshot["config"]
    rotation_cfg = config.get("meme_rotation", {})
    if not rotation_cfg.get("enabled", False):
        return []

    rotation = load_meme_rotation()
    blocked = set(config.get("satellite_blocked", []))
    core = set(config.get("core_assets", {}).keys())

    min_score = float(rotation_cfg.get("min_score", 0))
    max_active = int(rotation_cfg.get("max_active", 8))
    entry_score = float(rotation_cfg.get("entry_score", min_score))
    hold_score = float(rotation_cfg.get("hold_score", max(0.0, min_score * 0.5)))

    excluded_products = set(
        str(x).upper().strip()
        for x in rotation_cfg.get("excluded_products", [])
        if str(x).strip()
    )

    valid_products = set(
        str(x).upper().strip()
        for x in config.get("valid_products", [])
        if str(x).strip()
    )

    held_products = set()
    if snapshot is not None:
        trade_min = float(config.get("trade_min_value_usd", 0))
        for product_id, pos in snapshot.get("positions", {}).items():
            product_id = str(product_id or "").upper().strip()
            if not product_id:
                continue
            if product_id in core or product_id in blocked:
                continue
            liquid_value = float(pos.get("value_liquid_usd", 0.0) or 0.0)
            total_value = float(pos.get("value_total_usd", 0.0) or 0.0)
            if max(liquid_value, total_value) >= trade_min:
                held_products.add(product_id)

    ranked = []
    for item in rotation.get("candidates", []):
        product_id = str(item.get("product_id") or "").upper().strip()
        if not product_id:
            continue
        if product_id in blocked or product_id in core or product_id in excluded_products:
            continue
        if not bool(item.get("enabled", True)):
            continue

        score = safe_float(item.get("score", 0))
        source = item.get("source", "manual")

        if valid_products and product_id not in valid_products:
            continue

        ranked.append(
            {
                "product_id": product_id,
                "score": score,
                "source": source,
                "held": product_id in held_products,
                "sniper_eligible": bool(item.get("sniper_eligible", False)),
                "pump_protected": bool(item.get("pump_protected", False)),
            }
        )

    ranked.sort(key=lambda x: x["score"], reverse=True)

    selected = []

    for item in ranked:
        if len(selected) >= max_active:
            break
        if item["held"] and item["score"] >= hold_score:
            selected.append(item)

    selected_ids = {x["product_id"] for x in selected}

    for item in ranked:
        if len(selected) >= max_active:
            break
        if item["product_id"] in selected_ids:
            continue
        if item["score"] < entry_score:
            continue
        selected.append(item)
        selected_ids.add(item["product_id"])

    selected.sort(key=lambda x: x["score"], reverse=True)
    return selected[:max_active]

def get_active_satellite_buy_universe(snapshot):
    config = snapshot["config"]
    mode = str(config.get("satellite_mode", "dynamic")).lower()

    if mode == "dynamic":
        held = []
        for product_id, pos in snapshot["positions"].items():
            if product_id in config.get("core_assets", {}):
                continue
            if product_id in config.get("satellite_blocked", []):
                continue
            if float(pos.get("value_liquid_usd", 0.0) or 0.0) >= float(config["trade_min_value_usd"]):
                held.append(product_id)
        return sorted(set(held + list(config.get("satellite_allowed", []))))

    if mode == "rotation":
        rotation_ids = [x["product_id"] for x in get_rotation_products(snapshot)]
        return sorted(set(rotation_ids + list(config.get("satellite_allowed", []))))

    return sorted(set(config.get("satellite_allowed", [])))


def classify_asset(product_id, snapshot):
    config = snapshot["config"]

    if product_id in config["core_assets"]:
        return "core"
    if product_id in config.get("satellite_blocked", []):
        return "satellite_blocked"

    pos = snapshot["positions"].get(product_id)
    if not pos:
        return "unknown"

    total_value = float(pos.get("value_total_usd", 0.0) or 0.0)
    liquid_value = float(pos.get("value_liquid_usd", 0.0) or 0.0)

    if total_value < float(config["dust_min_value_usd"]):
        return "dust"
    if liquid_value < float(config["trade_min_value_usd"]):
        return "nontradable"

    return "satellite_active"


def is_satellite_buy_eligible(product_id, snapshot):
    config = snapshot["config"]
    product_id = str(product_id or "").upper().strip()

    if not product_id:
        return False

    if product_id in config.get("core_assets", {}):
        return False

    if product_id in config.get("satellite_blocked", []):
        return False

    return product_id in set(get_active_satellite_buy_universe(snapshot))
def get_rotation_candidate_map():
    rotation = load_meme_rotation()
    out = {}
    for item in rotation.get("candidates", []):
        product_id = str(item.get("product_id") or "").upper().strip()
        if not product_id:
            continue
        out[product_id] = item
    return out


def is_sniper_buy_eligible(product_id, snapshot):
    config = snapshot["config"]
    sniper_cfg = config.get("sniper_mode", {})
    if not sniper_cfg.get("enabled", False):
        return False

    product_id = str(product_id or "").upper().strip()
    if not product_id:
        return False

    if not is_satellite_buy_eligible(product_id, snapshot):
        return False

    regime = str(get_market_regime().get("regime", "")).lower()
    allowed_regimes = {
        str(x).lower() for x in sniper_cfg.get("allow_in_regimes", ["bull", "neutral"])
    }
    if regime not in allowed_regimes:
        return False

    candidate = get_rotation_candidate_map().get(product_id, {})
    if not candidate:
        return False

    score = safe_float(candidate.get("score", 0))
    if score < float(sniper_cfg.get("min_score", 85)):
        return False

    if bool(sniper_cfg.get("require_sniper_eligible", True)) and not bool(candidate.get("sniper_eligible", False)):
        return False

    if bool(sniper_cfg.get("block_pump_protected", True)) and bool(candidate.get("pump_protected", False)):
        return False

    return True

def get_daily_closes(product_id, days=25):
    client = get_client()
    end_ts = int(time.time())
    start_ts = end_ts - (days * 24 * 60 * 60)
    try:
        resp = _to_dict(
            client.get_candles(
                product_id=product_id,
                start=str(start_ts),
                end=str(end_ts),
                granularity="ONE_DAY",
            )
        )
    except Exception:
        return []

    candles = sorted(resp.get("candles", []), key=lambda x: int(x["start"]))
    closes = []
    for c in candles:
        try:
            closes.append(float(c["close"]))
        except Exception:
            pass
    return closes


def compute_realized_volatility(product_id, days=20):
    closes = get_daily_closes(product_id, days + 5)
    if len(closes) < days:
        return None

    returns = []
    for i in range(1, len(closes)):
        prev_close = closes[i - 1]
        curr_close = closes[i]
        if prev_close <= 0 or curr_close <= 0:
            continue
        returns.append(math.log(curr_close / prev_close))

    if len(returns) < 5:
        return None

    mean_r = sum(returns) / len(returns)
    variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
    return math.sqrt(variance)


def get_dynamic_volatility_bucket(product_id):
    vol = compute_realized_volatility(product_id)
    if vol is None:
        return "medium"
    if vol >= 0.12:
        return "very_high"
    if vol >= 0.08:
        return "high"
    return "medium"


def get_bucket_multiplier(bucket_name):
    if bucket_name == "very_high":
        return 0.50
    if bucket_name == "high":
        return 0.75
    return 1.0


def get_satellite_volatility_info(product_id, snapshot):
    config = snapshot["config"]
    vol_map = config.get("satellite_volatility_map", {})
    buckets = config.get("volatility_buckets", {})
    override_bucket = vol_map.get(product_id)
    realized_volatility = compute_realized_volatility(product_id)

    if override_bucket and override_bucket in buckets:
        return {
            "realized_volatility": realized_volatility,
            "volatility_bucket": override_bucket,
            "bucket_source": "manual_override",
            "volatility_multiplier": get_bucket_multiplier(override_bucket),
        }

    dynamic_bucket = get_dynamic_volatility_bucket(product_id)
    if dynamic_bucket and dynamic_bucket in buckets:
        return {
            "realized_volatility": realized_volatility,
            "volatility_bucket": dynamic_bucket,
            "bucket_source": "dynamic",
            "volatility_multiplier": get_bucket_multiplier(dynamic_bucket),
        }

    return {
        "realized_volatility": realized_volatility,
        "volatility_bucket": "default",
        "bucket_source": "default",
        "volatility_multiplier": 1.0,
    }


def get_satellite_max_weight(product_id, snapshot):
    config = snapshot["config"]
    buckets = config.get("volatility_buckets", {})
    vol_info = get_satellite_volatility_info(product_id, snapshot)
    bucket = vol_info["volatility_bucket"]
    if bucket in buckets:
        return float(
            buckets[bucket].get(
                "max_weight",
                config.get("satellite_defaults", {}).get("max_weight", 0.12),
            )
        )
    return float(config.get("satellite_defaults", {}).get("max_weight", 0.12))


def get_asset_value(snapshot, product_id):
    return float(snapshot["positions"].get(product_id, {}).get("value_total_usd", 0.0) or 0.0)


def get_asset_weight(snapshot, product_id):
    return float(snapshot["positions"].get(product_id, {}).get("weight_total", 0.0) or 0.0)


def get_core_target_weight(product_id, snapshot):
    return float(snapshot["config"]["core_assets"].get(product_id, {}).get("target_weight", 0.0))


def get_core_band(product_id, snapshot):
    return float(snapshot["config"]["core_assets"].get(product_id, {}).get("rebalance_band", 0.0))


def get_core_shortfall_usd(product_id, snapshot):
    target_value = snapshot["total_value_usd"] * get_core_target_weight(product_id, snapshot)
    return max(0.0, target_value - get_asset_value(snapshot, product_id))


def core_is_underweight(product_id, snapshot):
    return get_asset_weight(snapshot, product_id) < (
        get_core_target_weight(product_id, snapshot) - get_core_band(product_id, snapshot)
    )


def core_is_overweight(product_id, snapshot):
    return get_asset_weight(snapshot, product_id) > (
        get_core_target_weight(product_id, snapshot) + get_core_band(product_id, snapshot)
    )


def _build_portfolio_snapshot():
    config = load_asset_config()
    positions = get_position_values()
    cash_breakdown = get_cash_breakdown()
    usd_cash = float(cash_breakdown["TOTAL_CASH_EQUIV_USD"])
    asset_total = sum(v["value_total_usd"] for v in positions.values())
    total_value = asset_total + usd_cash
    if total_value <= 0:
        total_value = 1e-9

    snapshot = {
        "timestamp": int(time.time()),
        "total_value_usd": total_value,
        "usd_cash": usd_cash,
        "cash_breakdown": cash_breakdown,
        "cash_weight": usd_cash / total_value,
        "positions": positions,
        "config": config,
        "portfolio_peak": total_value,
        "portfolio_drawdown": 0.0,
        "active_satellite_buy_universe": [],
    }

    core_value = 0.0
    satellite_value = 0.0
    blocked_value = 0.0
    dust_value = 0.0
    nontradable_value = 0.0

    for product_id, info in positions.items():
        info["weight_total"] = info["value_total_usd"] / total_value
        info["weight_liquid"] = info["value_liquid_usd"] / total_value
        info["weight_locked"] = info["value_locked_usd"] / total_value
        asset_class = classify_asset(product_id, snapshot)
        info["class"] = asset_class

        if asset_class == "core":
            core_value += info["value_total_usd"]
        elif asset_class == "satellite_active":
            satellite_value += info["value_total_usd"]
        elif asset_class == "satellite_blocked":
            blocked_value += info["value_total_usd"]
        elif asset_class == "dust":
            dust_value += info["value_total_usd"]
        elif asset_class == "nontradable":
            nontradable_value += info["value_total_usd"]

    snapshot["core_value_usd"] = core_value
    snapshot["core_weight"] = core_value / total_value
    snapshot["satellite_value_usd"] = satellite_value
    snapshot["satellite_weight"] = satellite_value / total_value
    snapshot["blocked_value_usd"] = blocked_value
    snapshot["blocked_weight"] = blocked_value / total_value
    snapshot["dust_value_usd"] = dust_value
    snapshot["dust_weight"] = dust_value / total_value
    snapshot["nontradable_value_usd"] = nontradable_value
    snapshot["nontradable_weight"] = nontradable_value / total_value
    snapshot["active_satellite_buy_universe"] = get_active_satellite_buy_universe(snapshot)
    return snapshot


def _cache_age_seconds():
    with _PORTFOLIO_CACHE_LOCK:
        cached_at = float(_PORTFOLIO_CACHE.get("cached_at", 0.0) or 0.0)
    if cached_at <= 0:
        return None
    return max(0.0, time.time() - cached_at)


def _snapshot_age_seconds(snapshot):
    try:
        snapshot_ts = float((snapshot or {}).get("timestamp") or 0.0)
    except Exception:
        snapshot_ts = 0.0

    if snapshot_ts <= 0:
        return None

    return max(0.0, time.time() - snapshot_ts)


def _cache_metadata(source, warning=None):
    age = _cache_age_seconds()
    snapshot = _get_cached_snapshot()
    snapshot_age = _snapshot_age_seconds(snapshot)

    meta = {
        "source": source,
        "ttl_sec": PORTFOLIO_CACHE_TTL_SEC,
        "stale_sec": PORTFOLIO_CACHE_STALE_SEC,
        "cached_at": float(_PORTFOLIO_CACHE.get("cached_at", 0.0) or 0.0),
        "cache_age_sec": round(age, 2) if age is not None else None,
        "snapshot_age_sec": round(snapshot_age, 2) if snapshot_age is not None else None,
        "snapshot_timestamp": (snapshot or {}).get("timestamp"),
        "last_error": _PORTFOLIO_CACHE.get("last_error"),
    }
    if warning:
        meta["warning"] = warning
    return meta


def _cache_is_fresh():
    age = _cache_age_seconds()
    return age is not None and age <= PORTFOLIO_CACHE_TTL_SEC


def _cache_is_stale_usable():
    age = _cache_age_seconds()
    return age is not None and age <= PORTFOLIO_CACHE_STALE_SEC


def _store_portfolio_cache(snapshot):
    with _PORTFOLIO_CACHE_LOCK:
        _PORTFOLIO_CACHE["snapshot"] = snapshot
        _PORTFOLIO_CACHE["cached_at"] = time.time()
        _PORTFOLIO_CACHE["last_error"] = None


def _get_cached_snapshot():
    with _PORTFOLIO_CACHE_LOCK:
        snap = _PORTFOLIO_CACHE.get("snapshot")
        return dict(snap) if isinstance(snap, dict) else snap


def _snapshot_to_history_row(snapshot):
    snapshot = snapshot or {}
    total_value_usd = float(snapshot.get("total_value_usd", 0.0) or 0.0)
    cash_value_usd = float(snapshot.get("usd_cash", 0.0) or 0.0)
    positions_value_usd = max(0.0, total_value_usd - cash_value_usd)

    return {
        "ts": int(snapshot.get("timestamp") or time.time()),
        "total_value_usd": total_value_usd,
        "cash_value_usd": cash_value_usd,
        "positions_value_usd": positions_value_usd,
    }


def build_portfolio_history_analytics(history_rows, source="portfolio_history", note=None):
    rows = []
    for row in history_rows or []:
        rows.append(
            {
                "ts": int(row.get("ts") or 0),
                "total_value_usd": float(row.get("total_value_usd", 0.0) or 0.0),
            }
        )

    rows = [row for row in rows if row["ts"] > 0]
    rows.sort(key=lambda row: row["ts"])

    analytics = {
        "source": source,
        "history_points": len(rows),
        "sufficient_history": len(rows) >= 2,
        "limited_history": len(rows) < 2,
        "start_value_usd": None,
        "end_value_usd": None,
        "pnl_usd": None,
        "pnl_pct": None,
        "peak_value_usd": None,
        "current_drawdown_pct": None,
        "max_drawdown_pct": None,
        "note": note or "",
    }

    if not rows:
        analytics["note"] = analytics["note"] or "No persisted portfolio history is available for this range yet."
        return analytics

    start_value = float(rows[0]["total_value_usd"] or 0.0)
    end_value = float(rows[-1]["total_value_usd"] or 0.0)
    peak_value = max(float(row["total_value_usd"] or 0.0) for row in rows)

    analytics["start_value_usd"] = start_value
    analytics["end_value_usd"] = end_value
    analytics["peak_value_usd"] = peak_value

    if len(rows) < 2:
        analytics["note"] = analytics["note"] or "At least two persisted portfolio snapshots are required for PnL and drawdown analytics."
        return analytics

    pnl_usd = end_value - start_value
    pnl_pct = (pnl_usd / start_value) if start_value > 0 else None
    current_drawdown_pct = ((peak_value - end_value) / peak_value) if peak_value > 0 else None

    running_peak = 0.0
    max_drawdown_pct = 0.0
    for row in rows:
        value = float(row["total_value_usd"] or 0.0)
        running_peak = max(running_peak, value)
        if running_peak > 0:
            max_drawdown_pct = max(max_drawdown_pct, (running_peak - value) / running_peak)

    analytics.update(
        {
            "limited_history": False,
            "sufficient_history": True,
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct,
            "current_drawdown_pct": current_drawdown_pct,
            "max_drawdown_pct": max_drawdown_pct,
        }
    )
    analytics["note"] = analytics["note"] or ""
    return analytics


def build_portfolio_risk_score(snapshot=None, summary=None, history_analytics=None):
    snapshot = snapshot or get_portfolio_snapshot()
    summary = summary or portfolio_summary(snapshot)
    history_analytics = history_analytics if isinstance(history_analytics, dict) else {}

    cfg = snapshot.get("config", {}) or {}

    satellite_weight = float(summary.get("satellite_weight", snapshot.get("satellite_weight", 0.0)) or 0.0)
    cash_weight = float(summary.get("cash_weight", snapshot.get("cash_weight", 0.0)) or 0.0)
    satellite_target = float(cfg.get("satellite_total_target", 0.0) or 0.0)
    satellite_max = float(cfg.get("satellite_total_max", satellite_target or 0.0) or 0.0)
    min_cash_reserve = float(cfg.get("min_cash_reserve", 0.0) or 0.0)
    market_regime = str(summary.get("market_regime", "unknown") or "unknown").lower()

    current_drawdown_pct = history_analytics.get("current_drawdown_pct")
    max_drawdown_pct = history_analytics.get("max_drawdown_pct")
    limited_history = bool(history_analytics.get("limited_history", False))

    notes = []
    components = {}

    # Satellite allocation pressure: 30 points max.
    if satellite_max > 0 and satellite_weight >= satellite_max:
        satellite_component = 30.0
        notes.append("Satellite allocation is at or above the configured maximum.")
    elif satellite_target > 0 and satellite_weight > satellite_target:
        headroom = max(satellite_max - satellite_target, 0.05)
        over_target = max(0.0, satellite_weight - satellite_target)
        satellite_component = 10.0 + min(20.0, (over_target / headroom) * 20.0)
        notes.append("Satellite allocation is running above the configured target.")
    elif satellite_target > 0:
        satellite_component = min(10.0, (satellite_weight / satellite_target) * 10.0)
    else:
        satellite_component = min(12.0, satellite_weight * 20.0)
        notes.append("Satellite target is not configured, so allocation pressure is estimated from live exposure.")
    components["satellite_allocation"] = round(satellite_component, 2)

    # Cash reserve pressure: 25 points max.
    if min_cash_reserve > 0:
        cash_gap = max(0.0, min_cash_reserve - cash_weight)
        cash_component = min(25.0, (cash_gap / min_cash_reserve) * 25.0)
        if cash_weight < min_cash_reserve:
            notes.append("Cash reserve is below the configured minimum.")
    else:
        cash_component = 0.0
        notes.append("Minimum cash reserve is not configured, so cash pressure is neutral.")
    components["cash_reserve"] = round(cash_component, 2)

    # Drawdown pressure uses persisted history when available.
    if current_drawdown_pct is None:
        current_drawdown_component = 0.0
    else:
        current_drawdown_component = min(20.0, (float(current_drawdown_pct) / 0.20) * 20.0)
        if float(current_drawdown_pct) >= 0.10:
            notes.append("Current drawdown is materially elevated versus recent peak equity.")
    components["current_drawdown"] = round(current_drawdown_component, 2)

    if max_drawdown_pct is None:
        max_drawdown_component = 0.0
    else:
        max_drawdown_component = min(15.0, (float(max_drawdown_pct) / 0.30) * 15.0)
        if float(max_drawdown_pct) >= 0.18:
            notes.append("Recent max drawdown suggests the account has been operating through higher volatility.")
    components["max_drawdown"] = round(max_drawdown_component, 2)

    regime_component_map = {
        "bull": 0.0,
        "neutral": 5.0,
        "risk_off": 10.0,
    }
    regime_component = regime_component_map.get(market_regime, 3.0)
    if market_regime == "risk_off":
        notes.append("Market regime is risk-off, which raises the overall risk posture.")
    elif market_regime == "neutral":
        notes.append("Market regime is neutral, so the score keeps some defensive bias.")
    components["market_regime"] = round(regime_component, 2)

    if limited_history:
        notes.append("Drawdown inputs are limited because persisted equity history is still building.")

    score = int(round(
        satellite_component
        + cash_component
        + current_drawdown_component
        + max_drawdown_component
        + regime_component
    ))
    score = max(0, min(100, score))

    if score >= 75:
        band = "High Risk"
    elif score >= 50:
        band = "Elevated Risk"
    elif score >= 25:
        band = "Moderate Risk"
    else:
        band = "Low Risk"

    deduped_notes = []
    for note in notes:
        if note and note not in deduped_notes:
            deduped_notes.append(note)

    return {
        "score": score,
        "band": band,
        "notes": deduped_notes[:4],
        "inputs": {
            "satellite_weight": satellite_weight,
            "satellite_total_target": satellite_target,
            "satellite_total_max": satellite_max,
            "cash_weight": cash_weight,
            "min_cash_reserve": min_cash_reserve,
            "current_drawdown_pct": current_drawdown_pct,
            "max_drawdown_pct": max_drawdown_pct,
            "market_regime": market_regime,
            "limited_history": limited_history,
        },
        "weights": {
            "satellite_allocation": 30,
            "cash_reserve": 25,
            "current_drawdown": 20,
            "max_drawdown": 15,
            "market_regime": 10,
        },
        "components": components,
    }


def persist_current_portfolio_snapshot(snapshot=None):
    snapshot = snapshot or get_portfolio_snapshot()
    row = _snapshot_to_history_row(snapshot)

    try:
        latest_rows = get_portfolio_history_since(limit=1)
        latest = latest_rows[-1] if latest_rows else None

        if latest:
            age_sec = max(0, int(row["ts"]) - int(latest.get("ts") or 0))
            total_delta = abs(float(row["total_value_usd"]) - float(latest.get("total_value_usd", 0.0) or 0.0))
            cash_delta = abs(float(row["cash_value_usd"]) - float(latest.get("cash_value_usd", 0.0) or 0.0))

            if age_sec < PORTFOLIO_HISTORY_MIN_INTERVAL_SEC and max(total_delta, cash_delta) < PORTFOLIO_HISTORY_MIN_CHANGE_USD:
                return False

        insert_portfolio_snapshot(**row)
        return True
    except Exception as exc:
        _log_portfolio(f"portfolio snapshot persistence skipped: {exc}")
        return False


def force_refresh_portfolio_snapshot():
    snapshot = _build_portfolio_snapshot()
    _store_portfolio_cache(snapshot)
    return snapshot


def get_portfolio_snapshot(force_refresh=False):
    if not force_refresh and _cache_is_fresh():
        cached = _get_cached_snapshot()
        if cached:
            cached.setdefault("_cache", {})
            cached["_cache"].update(_cache_metadata("fresh-cache"))
            return cached

    try:
        snapshot = _build_portfolio_snapshot()
        _store_portfolio_cache(snapshot)
        snapshot.setdefault("_cache", {})
        snapshot["_cache"].update(_cache_metadata("live"))
        return snapshot

    except Exception as exc:
        with _PORTFOLIO_CACHE_LOCK:
            _PORTFOLIO_CACHE["last_error"] = str(exc)

        if not force_refresh and _cache_is_stale_usable():
            cached = _get_cached_snapshot()
            if cached:
                cached.setdefault("_cache", {})
                cached["_cache"].update(_cache_metadata("stale-cache", warning=str(exc)))
                _log_portfolio(f"live portfolio refresh failed; serving stale cache: {exc}")
                return cached
        _log_portfolio(f"live portfolio refresh failed without usable cache: {exc}")
        raise


def get_cached_portfolio_state():
    cached = _get_cached_snapshot()
    age = _cache_age_seconds()
    snapshot_age = _snapshot_age_seconds(cached)

    if cached:
        cache_source = "fresh-cache" if _cache_is_fresh() else "stale-cache"
        return {
            "ok": True,
            "timestamp": cached.get("timestamp"),
            "total_value_usd": float(cached.get("total_value_usd", 0.0) or 0.0),
            "usd_cash": float(cached.get("usd_cash", 0.0) or 0.0),
            "cash_weight": float(cached.get("cash_weight", 0.0) or 0.0),
            "core_weight": float(cached.get("core_weight", 0.0) or 0.0),
            "satellite_weight": float(cached.get("satellite_weight", 0.0) or 0.0),
            "active_satellite_buy_universe": cached.get("active_satellite_buy_universe", []),
            "source": cache_source,
            "age_sec": round(age or 0.0, 2),
            "cache_age_sec": round(age or 0.0, 2),
            "snapshot_age_sec": round(snapshot_age, 2) if snapshot_age is not None else None,
            "last_updated_ts": cached.get("timestamp"),
            "cached_at": float(_PORTFOLIO_CACHE.get("cached_at", 0.0) or 0.0),
            "ttl_sec": PORTFOLIO_CACHE_TTL_SEC,
            "stale_sec": PORTFOLIO_CACHE_STALE_SEC,
            "last_error": _PORTFOLIO_CACHE.get("last_error"),
        }

    return {
        "ok": False,
        "source": "empty",
        "age_sec": None,
        "last_error": _PORTFOLIO_CACHE.get("last_error"),
    }


def get_min_cash_reserve_usd(snapshot):
    return snapshot["total_value_usd"] * float(snapshot["config"].get("min_cash_reserve", 0.05))


def get_free_cash_after_reserve(snapshot):
    return max(0.0, snapshot["usd_cash"] - get_min_cash_reserve_usd(snapshot))


def get_core_priority_active(snapshot):
    total_core_target = sum(v["target_weight"] for v in snapshot["config"]["core_assets"].values())
    return snapshot["core_weight"] < total_core_target


def get_deployable_cash_buckets(snapshot):
    free_cash = get_free_cash_after_reserve(snapshot)
    regime_info = get_market_regime()
    regime = regime_info["regime"]

    if free_cash <= 0:
        return {
            "free_cash_usd": 0.0,
            "core_budget_usd": 0.0,
            "satellite_budget_usd": 0.0,
            "regime": regime,
            "regime_info": regime_info,
        }

    if regime == "bull":
        core_split, satellite_split = 0.60, 0.40
    elif regime == "risk_off":
        core_split, satellite_split = 0.90, 0.10
    else:
        core_split, satellite_split = 0.70, 0.30

    if get_core_priority_active(snapshot):
        if regime == "bull":
            core_split, satellite_split = max(core_split, 0.60), min(satellite_split, 0.40)
        elif regime == "risk_off":
            core_split, satellite_split = max(core_split, 0.90), min(satellite_split, 0.10)
        else:
            core_split, satellite_split = max(core_split, 0.70), min(satellite_split, 0.30)

    return {
        "free_cash_usd": free_cash,
        "core_budget_usd": free_cash * core_split,
        "satellite_budget_usd": free_cash * satellite_split,
        "regime": regime,
        "regime_info": regime_info,
    }


def get_core_allocation_budget_usd(snapshot):
    return float(get_deployable_cash_buckets(snapshot)["core_budget_usd"])


def get_satellite_allocation_budget_usd(snapshot):
    return float(get_deployable_cash_buckets(snapshot)["satellite_budget_usd"])


def allowed_core_buy_usd(product_id, snapshot):
    if classify_asset(product_id, snapshot) != "core":
        return 0.0
    if not core_is_underweight(product_id, snapshot):
        return 0.0

    shortfall = get_core_shortfall_usd(product_id, snapshot)
    proposed = shortfall * float(snapshot["config"].get("core_buy_fraction_of_shortfall", 0.25))
    core_budget = get_core_allocation_budget_usd(snapshot)
    return max(0.0, min(proposed, core_budget))

def allowed_satellite_buy_usd(product_id, snapshot, signal_type="SATELLITE_BUY"):
    if not is_satellite_buy_eligible(product_id, snapshot):
        return 0.0

    summary = portfolio_summary(snapshot)
    regime = str(summary.get("market_regime", "neutral")).lower()

    if regime == "risk_off":
        return 0.0

    drawdown = float(snapshot.get("portfolio_drawdown", 0.0) or 0.0)
    freeze_level = float(snapshot["config"].get("drawdown_controls", {}).get("freeze_level", 1.0))
    if drawdown >= freeze_level:
        return 0.0

    total_value = float(snapshot["total_value_usd"] or 0.0)
    if total_value <= 0:
        return 0.0

    satellite_budget = get_satellite_allocation_budget_usd(snapshot)
    regime_caps = snapshot["config"].get("regime_satellite_caps", {})
    satellite_total_max = float(regime_caps.get(regime, snapshot["config"].get("satellite_total_max", 0.50)))
    current_satellite_value = float(snapshot.get("satellite_value_usd", 0.0) or 0.0)
    current_asset_value = get_asset_value(snapshot, product_id)

    allowed_total_satellite_value = total_value * satellite_total_max
    satellite_headroom = max(0.0, allowed_total_satellite_value - current_satellite_value)

    per_asset_max_value = total_value * get_satellite_max_weight(product_id, snapshot)
    asset_headroom = max(0.0, per_asset_max_value - current_asset_value)

    raw_allowed = max(0.0, min(satellite_budget, satellite_headroom, asset_headroom))
    vol_info = get_satellite_volatility_info(product_id, snapshot)
    adjusted_allowed = max(0.0, raw_allowed * float(vol_info["volatility_multiplier"]))

    sniper_cfg = snapshot["config"].get("sniper_mode", {})
    signal_type = str(signal_type or "").upper().strip()

    if signal_type == "SNIPER_BUY" and is_sniper_buy_eligible(product_id, snapshot):
        adjusted_allowed *= float(sniper_cfg.get("buy_scale", 0.35))

    trade_min_value_usd = float(snapshot["config"].get("trade_min_value_usd", 10.0) or 10.0)
    if adjusted_allowed < trade_min_value_usd:
        return 0.0

    return adjusted_allowed

def required_trim_usd(product_id, snapshot):
    total_value = snapshot["total_value_usd"]
    current_value = get_asset_value(snapshot, product_id)
    asset_class = classify_asset(product_id, snapshot)

    if asset_class == "core":
        max_allowed_value = total_value * (
            get_core_target_weight(product_id, snapshot) + get_core_band(product_id, snapshot)
        )
        return max(0.0, current_value - max_allowed_value)

    if asset_class == "satellite_active":
        max_allowed_value = total_value * get_satellite_max_weight(product_id, snapshot)
        return max(0.0, current_value - max_allowed_value)

    return 0.0


def get_profit_harvest_candidates(snapshot):
    cfg = snapshot["config"].get("profit_harvest", {})
    if not cfg.get("enabled", False):
        return []

    cooldown_sec = int(float(cfg.get("cooldown_hours", 24)) * 3600)
    min_harvest_usd = float(cfg.get("min_harvest_usd", 15))
    tiers = sorted(cfg.get("tiers", []), key=lambda x: float(x.get("gain_pct", 0)), reverse=True)
    now = int(time.time())
    out = []

    for product_id, asset in snapshot["positions"].items():
        asset_class = classify_asset(product_id, snapshot)
        if cfg.get("satellite_only", True) and asset_class != "satellite_active":
            continue

        state = get_asset_state(product_id)
        entry = float(state.get("avg_entry_price", 0.0) or 0.0)
        if entry <= 0:
            continue

        price = float(asset.get("price_usd", 0.0) or 0.0)
        if price <= 0:
            continue

        gain_pct = (price - entry) / entry
        chosen_tier = None
        for tier in tiers:
            if gain_pct >= float(tier.get("gain_pct", 0.0) or 0.0):
                chosen_tier = tier
                break
        if not chosen_tier:
            continue

        last_harvest_ts = int(state.get("last_harvest_ts", 0) or 0)
        if cooldown_sec > 0 and last_harvest_ts > 0 and (now - last_harvest_ts) < cooldown_sec:
            continue

        trim_pct = float(chosen_tier.get("trim_pct", 0.0) or 0.0)
        amount_usd = float(asset.get("value_total_usd", 0.0) or 0.0) * trim_pct
        if amount_usd < min_harvest_usd:
            continue

        out.append(
            {
                "product_id": product_id,
                "gain_pct": gain_pct,
                "trim_pct": trim_pct,
                "amount_usd": amount_usd,
                "avg_entry_price": entry,
                "current_price": price,
                "last_harvest_ts": last_harvest_ts,
            }
        )

    out.sort(key=lambda x: x["gain_pct"], reverse=True)
    return out


def portfolio_summary(snapshot=None):
    snapshot = snapshot or get_portfolio_snapshot()

    total_value_usd = float(snapshot.get("total_value_usd", 0.0) or 0.0)
    usd_cash = float(snapshot.get("usd_cash", 0.0) or 0.0)
    core_value_usd = float(snapshot.get("core_value_usd", 0.0) or 0.0)
    satellite_value_usd = float(snapshot.get("satellite_value_usd", 0.0) or 0.0)
    blocked_value_usd = float(snapshot.get("blocked_value_usd", 0.0) or 0.0)
    dust_value_usd = float(snapshot.get("dust_value_usd", 0.0) or 0.0)
    nontradable_value_usd = float(snapshot.get("nontradable_value_usd", 0.0) or 0.0)
    portfolio_drawdown = float(snapshot.get("portfolio_drawdown", 0.0) or 0.0)

    cfg = snapshot.get("config", {}) or {}
    regime_caps = cfg.get("regime_satellite_caps", {}) or {}
    drawdown_controls = cfg.get("drawdown_controls", {}) or {}

    warn_level = float(drawdown_controls.get("warn_level", 0.10) or 0.10)
    reduce_level = float(drawdown_controls.get("reduce_level", 0.15) or 0.15)
    freeze_level = float(drawdown_controls.get("freeze_level", 0.20) or 0.20)

    if portfolio_drawdown >= freeze_level:
        market_regime = "risk_off"
    elif portfolio_drawdown >= reduce_level:
        market_regime = "neutral"
    else:
        market_regime = "bull"

    assets = {}
    for product_id, pos in (snapshot.get("positions", {}) or {}).items():
        assets[product_id] = {
            "product_id": product_id,
            "class": pos.get("class", "unknown"),
            "value_total_usd": float(pos.get("value_total_usd", 0.0) or 0.0),
            "value_liquid_usd": float(pos.get("value_liquid_usd", 0.0) or 0.0),
            "weight_total": float(pos.get("weight_total", 0.0) or 0.0),
            "weight_liquid": float(pos.get("weight_liquid", 0.0) or 0.0),
            "base_qty_total": float(pos.get("base_qty_total", 0.0) or 0.0),
            "base_qty_liquid": float(pos.get("base_qty_liquid", 0.0) or 0.0),
            "price_usd": float(pos.get("price_usd", 0.0) or 0.0),
        }

    return {
        "ok": True,
        "timestamp": snapshot.get("timestamp"),
        "total_value_usd": total_value_usd,
        "usd_cash": usd_cash,
        "cash_weight": float(snapshot.get("cash_weight", 0.0) or 0.0),
        "core_value_usd": core_value_usd,
        "core_weight": float(snapshot.get("core_weight", 0.0) or 0.0),
        "satellite_value_usd": satellite_value_usd,
        "satellite_weight": float(snapshot.get("satellite_weight", 0.0) or 0.0),
        "blocked_value_usd": blocked_value_usd,
        "blocked_weight": float(snapshot.get("blocked_weight", 0.0) or 0.0),
        "dust_value_usd": dust_value_usd,
        "dust_weight": float(snapshot.get("dust_weight", 0.0) or 0.0),
        "nontradable_value_usd": nontradable_value_usd,
        "nontradable_weight": float(snapshot.get("nontradable_weight", 0.0) or 0.0),
        "portfolio_peak": float(snapshot.get("portfolio_peak", total_value_usd) or total_value_usd),
        "portfolio_drawdown": portfolio_drawdown,
        "market_regime": market_regime,
        "regime_satellite_cap": float(
            regime_caps.get(market_regime, cfg.get("satellite_total_max", 0.5)) or 0.5
        ),
        "drawdown_warn_level": warn_level,
        "drawdown_reduce_level": reduce_level,
        "drawdown_freeze_level": freeze_level,
        "assets": assets,
    }
