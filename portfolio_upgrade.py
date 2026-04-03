import json
import math
import os
import time
from pathlib import Path
from dotenv import load_dotenv

from storage import get_all_positions, get_asset_state
from execution import get_best_bid_ask, get_client
from regime import get_market_regime

load_dotenv("/root/tradingbot/.env")

CONFIG_PATH = "/root/tradingbot/asset_config.json"
MEME_ROTATION_PATH = "/root/tradingbot/meme_rotation.json"

STABLECOIN_PRODUCTS = {"USDC-USD", "USDT-USD", "DAI-USD"}
STABLECOIN_CURRENCIES = {"USDC", "USDT", "DAI"}


def load_asset_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def save_asset_config(config):
    Path(CONFIG_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
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
    with open(path, "r") as f:
        return json.load(f)


def save_meme_rotation(data):
    Path(MEME_ROTATION_PATH).parent.mkdir(parents=True, exist_ok=True)
    if "updated_at" not in data:
        data["updated_at"] = int(time.time())
    with open(MEME_ROTATION_PATH, "w") as f:
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

    breakdown["TOTAL_CASH_EQUIV_USD"] = breakdown["USD"] + breakdown["USDC"] + breakdown["USDT"] + breakdown["DAI"]
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

    items = []
    for item in rotation.get("candidates", []):
        product_id = str(item.get("product_id") or "").upper().strip()
        if not product_id or product_id in blocked or product_id in core:
            continue
        if not bool(item.get("enabled", True)):
            continue
        score = safe_float(item.get("score", 0))
        if score < min_score:
            continue
        items.append({
            "product_id": product_id,
            "score": score,
            "source": item.get("source", "manual"),
        })

    items.sort(key=lambda x: x["score"], reverse=True)
    return items[:max_active]


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

    # Any tradable non-core/non-blocked held asset can be monitored and trimmed as a satellite.
    return "satellite_active"


def is_satellite_buy_eligible(product_id, snapshot):
    if classify_asset(product_id, snapshot) != "satellite_active":
        return False
    return product_id in set(get_active_satellite_buy_universe(snapshot))


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
        return float(buckets[bucket].get("max_weight", config.get("satellite_defaults", {}).get("max_weight", 0.12)))
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
    return get_asset_weight(snapshot, product_id) < (get_core_target_weight(product_id, snapshot) - get_core_band(product_id, snapshot))


def core_is_overweight(product_id, snapshot):
    return get_asset_weight(snapshot, product_id) > (get_core_target_weight(product_id, snapshot) + get_core_band(product_id, snapshot))


def get_portfolio_snapshot():
    config = load_asset_config()
    positions = get_position_values()
    cash_breakdown = get_cash_breakdown()
    usd_cash = float(cash_breakdown["TOTAL_CASH_EQUIV_USD"])
    asset_total = sum(v["value_total_usd"] for v in positions.values())
    total_value = asset_total + usd_cash
    if total_value <= 0:
        total_value = 1e-9

    snapshot = {
        "total_value_usd": total_value,
        "asset_total_usd": asset_total,
        "usd_cash": usd_cash,
        "cash_breakdown": cash_breakdown,
        "cash_weight": usd_cash / total_value,
        "positions": positions,
        "config": config,
        "portfolio_peak": total_value,
        "portfolio_drawdown": 0.0,
        "active_satellite_buy_universe": [],
    }

    core_value = satellite_value = blocked_value = dust_value = nontradable_value = 0.0
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
        return {"free_cash_usd": 0.0, "core_budget_usd": 0.0, "satellite_budget_usd": 0.0, "regime": regime, "regime_info": regime_info}

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


def allowed_satellite_buy_usd(product_id, snapshot):
    if not is_satellite_buy_eligible(product_id, snapshot):
        return 0.0

    regime = get_market_regime()["regime"]
    if regime == "risk_off":
        return 0.0

    drawdown = float(snapshot.get("portfolio_drawdown", 0.0) or 0.0)
    freeze_level = float(snapshot["config"].get("drawdown_controls", {}).get("freeze_level", 1.0))
    if drawdown >= freeze_level:
        return 0.0

    total_value = snapshot["total_value_usd"]
    satellite_budget = get_satellite_allocation_budget_usd(snapshot)
    regime_caps = snapshot["config"].get("regime_satellite_caps", {})
    satellite_total_max = float(regime_caps.get(regime, snapshot["config"].get("satellite_total_max", 0.50)))
    current_satellite_value = snapshot["satellite_value_usd"]
    current_asset_value = get_asset_value(snapshot, product_id)
    allowed_total_satellite_value = total_value * satellite_total_max
    satellite_headroom = max(0.0, allowed_total_satellite_value - current_satellite_value)
    per_asset_max_value = total_value * get_satellite_max_weight(product_id, snapshot)
    asset_headroom = max(0.0, per_asset_max_value - current_asset_value)
    raw_allowed = max(0.0, min(satellite_budget, satellite_headroom, asset_headroom))
    vol_info = get_satellite_volatility_info(product_id, snapshot)
    return max(0.0, raw_allowed * float(vol_info["volatility_multiplier"]))


def required_trim_usd(product_id, snapshot):
    total_value = snapshot["total_value_usd"]
    current_value = get_asset_value(snapshot, product_id)
    asset_class = classify_asset(product_id, snapshot)

    if asset_class == "core":
        max_allowed_value = total_value * (get_core_target_weight(product_id, snapshot) + get_core_band(product_id, snapshot))
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
        out.append({
            "product_id": product_id,
            "gain_pct": gain_pct,
            "trim_pct": trim_pct,
            "amount_usd": amount_usd,
            "avg_entry_price": entry,
            "current_price": price,
            "last_harvest_ts": last_harvest_ts,
        })

    out.sort(key=lambda x: x["gain_pct"], reverse=True)
    return out


def portfolio_summary(snapshot):
    budgets = get_deployable_cash_buckets(snapshot)
    rotation = load_meme_rotation()
    active_buy_universe = snapshot.get("active_satellite_buy_universe", [])
    summary = {
        "total_value_usd": snapshot["total_value_usd"],
        "asset_total_usd": snapshot["asset_total_usd"],
        "usd_cash": snapshot["usd_cash"],
        "cash_breakdown": snapshot.get("cash_breakdown", {}),
        "cash_weight": snapshot["cash_weight"],
        "core_weight": snapshot["core_weight"],
        "satellite_weight": snapshot["satellite_weight"],
        "blocked_weight": snapshot["blocked_weight"],
        "dust_weight": snapshot["dust_weight"],
        "nontradable_weight": snapshot["nontradable_weight"],
        "market_regime": budgets["regime"],
        "market_regime_info": budgets["regime_info"],
        "core_priority_active": get_core_priority_active(snapshot),
        "free_cash_usd": budgets["free_cash_usd"],
        "core_budget_usd": budgets["core_budget_usd"],
        "satellite_budget_usd": budgets["satellite_budget_usd"],
        "active_satellite_buy_universe": active_buy_universe,
        "meme_rotation": rotation,
        "profit_harvest_candidates": get_profit_harvest_candidates(snapshot),
        "assets": {},
    }

    harvest_map = {x["product_id"]: x for x in summary["profit_harvest_candidates"]}

    for product_id, info in snapshot["positions"].items():
        state = get_asset_state(product_id)
        entry = {
            "class": info["class"],
            "value_total_usd": info["value_total_usd"],
            "value_liquid_usd": info["value_liquid_usd"],
            "value_locked_usd": info["value_locked_usd"],
            "weight_total": info["weight_total"],
            "weight_liquid": info["weight_liquid"],
            "weight_locked": info["weight_locked"],
            "base_qty_total": info["base_qty_total"],
            "base_qty_liquid": info["base_qty_liquid"],
            "base_qty_locked": info["base_qty_locked"],
            "price_usd": info["price_usd"],
            "is_locked_or_staked": float(info.get("base_qty_locked", 0.0) or 0.0) > 0,
            "avg_entry_price": float(state.get("avg_entry_price", 0.0) or 0.0),
            "last_harvest_ts": int(state.get("last_harvest_ts", 0) or 0),
            "buy_eligible": product_id in set(active_buy_universe),
        }
        if entry["avg_entry_price"] > 0 and entry["price_usd"] > 0:
            entry["unrealized_gain_pct"] = (entry["price_usd"] - entry["avg_entry_price"]) / entry["avg_entry_price"]
        if info["class"] == "core":
            entry["target_weight"] = get_core_target_weight(product_id, snapshot)
            entry["rebalance_band"] = get_core_band(product_id, snapshot)
            entry["underweight"] = core_is_underweight(product_id, snapshot)
            entry["overweight"] = core_is_overweight(product_id, snapshot)
            entry["core_shortfall_usd"] = get_core_shortfall_usd(product_id, snapshot)
            entry["allowed_buy_usd"] = allowed_core_buy_usd(product_id, snapshot)
            entry["required_trim_usd"] = required_trim_usd(product_id, snapshot)
        elif info["class"] == "satellite_active":
            vol_info = get_satellite_volatility_info(product_id, snapshot)
            entry["max_weight"] = get_satellite_max_weight(product_id, snapshot)
            entry["realized_volatility"] = vol_info["realized_volatility"]
            entry["volatility_bucket"] = vol_info["volatility_bucket"]
            entry["bucket_source"] = vol_info["bucket_source"]
            entry["volatility_multiplier"] = vol_info["volatility_multiplier"]
            entry["allowed_buy_usd"] = allowed_satellite_buy_usd(product_id, snapshot)
            entry["required_trim_usd"] = required_trim_usd(product_id, snapshot)
        if product_id in harvest_map:
            entry["harvest_candidate"] = harvest_map[product_id]
        summary["assets"][product_id] = entry
    return summary
