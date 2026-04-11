import json
import math
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import wraps

from flask import Blueprint, jsonify, request, session

from env_runtime import load_runtime_env, preferred_env_path

load_runtime_env(override=True)

from execution import (
    clear_product_cache,
    get_valid_product_ids,
    get_valid_products,
)
from decision_trace import list_decision_traces
from portfolio import (
    build_adaptive_suggestions,
    build_auto_adaptive_recommendation,
    build_portfolio_history_analytics,
    build_portfolio_risk_score,
    get_rotation_products,
    get_portfolio_snapshot,
    normalize_adaptive_suggestions_payload,
    normalize_auto_adaptive_payload,
    normalize_risk_score_payload,
    persist_current_portfolio_snapshot,
    portfolio_summary,
)
from performance import (
    get_daily_pnl,
    get_equity_analytics,
    get_performance_summary,
    get_product_breakdown,
    get_round_trips,
)
from backtester import Backtester, bollinger_bands, ema as bt_ema, rsi as bt_rsi
from brokers.webull_adapter import WebullAdapter
from options.backtester import OptionsBacktester
from options.chain_fetcher import OptionChainFetcher
from options.earnings import EarningsCalendar
from options.screener import OptionsScreener
from portfolio_backtester import PortfolioBacktester
from rebalancer import get_profit_harvest_plan, get_rebalance_plan
from signal_scanner import (
    SignalScanner,
    get_scanner_params,
    get_scanner_state,
    get_signal_log,
    run_scanner_sweep,
    update_scanner_params,
)
from services.config_proposal_service import (
    apply_config_proposal,
    approve_config_proposal,
    evaluate_auto_draft_review_proposals,
    generate_review_proposals,
    reject_config_proposal,
)
from services.satellite_decision_engine import build_satellite_decisions
from shadow_rotation_report import build_shadow_rotation_report
from storage import get_portfolio_history_since
from storage import get_latest_config_proposal_any_status, list_recent_config_proposals

api_bp = Blueprint("api", __name__)

BASE_DIR = str(preferred_env_path().parent.resolve())
ASSET_CONFIG_PATH = os.path.join(BASE_DIR, "asset_config.json")
MEME_ROTATION_PATH = os.path.join(BASE_DIR, "meme_rotation.json")
SATELLITE_ROTATION_SHADOW_LOG_PATH = os.path.join(BASE_DIR, "satellite_rotation_shadow.jsonl")
TRADING_DB_PATH = os.getenv("TRADINGBOT_DB_PATH", os.path.join(BASE_DIR, "trading.db"))

_API_CACHE_LOCK = threading.Lock()
_API_CACHE = {}
_SHADOW_ROTATION_LOG_LOCK = threading.Lock()
_LAST_SHADOW_ROTATION_LOG_KEY = None


def _log_api(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [api] {msg}")


def get_api_secrets():
    load_runtime_env(override=True)

    values = []
    seen = set()

    for s in [
        os.getenv("INTERNAL_API_SECRET", ""),
        os.getenv("STATUS_SECRET", ""),
        os.getenv("WEBHOOK_SHARED_SECRET", ""),
        os.getenv("WEBHOOK_SHARED_SECRETS", ""),
    ]:
        for part in str(s or "").split(","):
            part = part.strip().strip('"').strip("'")
            if part and part not in seen:
                values.append(part)
                seen.add(part)

    return values


# -------------------------------------------------------------------
# Auth helpers
# -------------------------------------------------------------------

def _provided_api_secret():
    return (
        request.args.get("secret")
        or request.headers.get("X-Api-Secret")
        or (request.get_json(silent=True) or {}).get("secret")
        or ""
    ).strip()


def _has_session_user():
    return bool(session.get("user_id"))


def _has_session_admin():
    return bool(session.get("user_id")) and int(session.get("is_admin", 0) or 0) == 1


def _is_api_authorized():
    if _has_session_user():
        return True
    provided = _provided_api_secret()
    return bool(provided and provided in get_api_secrets())


def _is_admin_authorized():
    if _has_session_admin():
        return True
    provided = _provided_api_secret()
    return bool(provided and provided in get_api_secrets())


def require_api_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _is_api_authorized():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


def require_admin_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _is_admin_authorized():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


require_secret = require_api_auth

# -------------------------------------------------------------------
# Compatibility helpers
# -------------------------------------------------------------------

try:
    from portfolio import force_refresh_portfolio_snapshot
except ImportError:
    def force_refresh_portfolio_snapshot():
        return get_portfolio_snapshot()


try:
    from portfolio import get_cached_portfolio_state
except ImportError:
    def get_cached_portfolio_state():
        try:
            snap = get_portfolio_snapshot()
            return {
                "ok": True,
                "timestamp": snap.get("timestamp"),
                "total_value_usd": snap.get("total_value_usd", 0.0),
                "usd_cash": snap.get("usd_cash", 0.0),
                "source": "live-fallback",
            }
        except Exception as exc:
            return {
                "ok": False,
                "error": str(exc),
                "source": "live-fallback",
            }


try:
    from portfolio import load_asset_config
except ImportError:
    def load_asset_config():
        return _load_json_file(ASSET_CONFIG_PATH, {})


try:
    from portfolio import save_asset_config
except ImportError:
    def save_asset_config(data):
        _save_json_file(ASSET_CONFIG_PATH, data)
        return data


try:
    from portfolio import load_meme_rotation
except ImportError:
    def load_meme_rotation():
        return _load_json_file(MEME_ROTATION_PATH, {"candidates": []})


try:
    from portfolio import save_meme_rotation
except ImportError:
    def save_meme_rotation(data):
        _save_json_file(MEME_ROTATION_PATH, data)
        return data


try:
    from portfolio import get_admin_state
except ImportError:
    def get_admin_state():
        cfg = load_asset_config() or {}
        meme_rotation = cfg.get("meme_rotation", {}) or {}
        return {
            "meme_rotation_enabled": bool(meme_rotation.get("enabled", False)),
            "allowed_candidates": list(cfg.get("satellite_allowed", [])),
            "blocked_candidates": list(cfg.get("satellite_blocked", [])),
            "core_assets": cfg.get("core_assets", {}),
            "satellite_mode": cfg.get("satellite_mode", "rotation"),
            "satellite_total_max": cfg.get("satellite_total_max"),
            "satellite_total_target": cfg.get("satellite_total_target"),
            "min_cash_reserve": cfg.get("min_cash_reserve"),
            "trade_min_value_usd": cfg.get("trade_min_value_usd"),
            "max_quote_per_trade_usd": cfg.get("max_quote_per_trade_usd"),
            "max_active_satellites": cfg.get("max_active_satellites"),
            "rotation_cooldown_minutes": cfg.get("rotation_cooldown_minutes"),
            "min_meme_score": cfg.get("min_meme_score"),
        }


try:
    from portfolio import set_meme_rotation_enabled
except ImportError:
    def set_meme_rotation_enabled(enabled):
        cfg = load_asset_config() or {}
        cfg.setdefault("meme_rotation", {})
        cfg["meme_rotation"]["enabled"] = bool(enabled)
        save_asset_config(cfg)
        return bool(enabled)


try:
    from portfolio import allow_satellite_candidate
except ImportError:
    def allow_satellite_candidate(product_id):
        cfg = load_asset_config() or {}
        arr = cfg.setdefault("satellite_allowed", [])
        if product_id not in arr:
            arr.append(product_id)
        blocked = cfg.setdefault("satellite_blocked", [])
        if product_id in blocked:
            blocked.remove(product_id)
        save_asset_config(cfg)
        return sorted(arr)


try:
    from portfolio import block_satellite_candidate
except ImportError:
    def block_satellite_candidate(product_id):
        cfg = load_asset_config() or {}
        arr = cfg.setdefault("satellite_blocked", [])
        if product_id not in arr:
            arr.append(product_id)
        allowed = cfg.setdefault("satellite_allowed", [])
        if product_id in allowed:
            allowed.remove(product_id)
        core_assets = cfg.setdefault("core_assets", {})
        if product_id in core_assets:
            del core_assets[product_id]
        save_asset_config(cfg)
        return sorted(arr)


try:
    from portfolio import remove_allowed_satellite_candidate
except ImportError:
    def remove_allowed_satellite_candidate(product_id):
        cfg = load_asset_config() or {}
        arr = cfg.setdefault("satellite_allowed", [])
        if product_id in arr:
            arr.remove(product_id)
        save_asset_config(cfg)
        return sorted(arr)


try:
    from portfolio import remove_blocked_satellite_candidate
except ImportError:
    def remove_blocked_satellite_candidate(product_id):
        cfg = load_asset_config() or {}
        arr = cfg.setdefault("satellite_blocked", [])
        if product_id in arr:
            arr.remove(product_id)
        save_asset_config(cfg)
        return sorted(arr)


# -------------------------------------------------------------------
# JSON helpers
# -------------------------------------------------------------------

def _load_json_file(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json_file(path, data):
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def _now():
    return time.time()


def _normalize_product_id(value):
    return str(value or "").strip().upper()


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _clamp_score(value, low=0.0, high=100.0):
    return max(low, min(high, float(value)))


def _normalize_score(value, low, high, default=50.0):
    if value is None:
        return float(default)
    low = float(low)
    high = float(high)
    if high <= low:
        return float(default)
    scaled = ((float(value) - low) / (high - low)) * 100.0
    return _clamp_score(scaled)


def _log_normalize_score(value, low, high, default=0.0):
    numeric = _safe_float(value)
    if numeric <= 0:
        return float(default)
    low = max(float(low), 1.0)
    high = max(float(high), low + 1.0)
    scaled = ((math.log10(numeric) - math.log10(low)) / (math.log10(high) - math.log10(low))) * 100.0
    return _clamp_score(scaled)


def _bucket_from_score(score, thresholds):
    numeric = float(score)
    for minimum, label in thresholds:
        if numeric >= minimum:
            return label
    return thresholds[-1][1]


def _normalized_choice(value, allowed, default=""):
    normalized = str(value or "").strip().lower()
    return normalized if normalized in allowed else default


def _trading_db_conn():
    conn = sqlite3.connect(TRADING_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _range_to_start_ts(range_name):
    now = int(time.time())
    mapping = {
        "7d": 7 * 86400,
        "30d": 30 * 86400,
        "90d": 90 * 86400,
        "180d": 180 * 86400,
        "1y": 365 * 86400,
        "all": None,
    }
    seconds = mapping.get((range_name or "30d").lower(), 30 * 86400)
    if seconds is None:
        return None
    return now - seconds


def _normalize_trade_row(row):
    price = _safe_float(row["price"])
    base_size = _safe_float(row["base_size"])
    return {
        "order_id": row["order_id"],
        "product_id": row["product_id"],
        "side": row["side"],
        "base_size": base_size,
        "price": price,
        "notional_usd": base_size * price,
        "status": row["status"],
        "created_at": _safe_int(row["created_at"]),
    }


# -------------------------------------------------------------------
# Cache helpers
# -------------------------------------------------------------------

def _cache_get(name):
    with _API_CACHE_LOCK:
        entry = _API_CACHE.get(name)
        if not entry:
            return None
        return dict(entry)


def _cache_set(name, data, ttl_sec=30.0, stale_sec=300.0):
    entry = {
        "data": data,
        "cached_at": _now(),
        "ttl_sec": float(ttl_sec),
        "stale_sec": float(stale_sec),
    }
    with _API_CACHE_LOCK:
        _API_CACHE[name] = entry
    return entry


def _cache_valid(entry):
    if not entry:
        return False
    age = _now() - float(entry.get("cached_at", 0.0))
    return age <= float(entry.get("ttl_sec", 0.0))


def _cache_stale_ok(entry):
    if not entry:
        return False
    age = _now() - float(entry.get("cached_at", 0.0))
    return age <= float(entry.get("stale_sec", 0.0))


def _cache_meta(entry, source, warning=None):
    cached_at = float((entry or {}).get("cached_at", 0.0) or 0.0)
    age = max(0.0, _now() - cached_at) if cached_at > 0 else None
    meta = {
        "source": source,
        "cached_at": cached_at,
        "age_sec": round(age, 2) if age is not None else None,
        "ttl_sec": float((entry or {}).get("ttl_sec", 0.0) or 0.0),
        "stale_sec": float((entry or {}).get("stale_sec", 0.0) or 0.0),
        "is_stale": source == "stale-cache",
    }
    if warning:
        meta["warning"] = warning
    return meta


def _freshness_from_payload(payload):
    payload = payload if isinstance(payload, dict) else {}
    cache = payload.get("_cache", {}) if isinstance(payload.get("_cache"), dict) else {}
    freshness = {
        "source": cache.get("source") or payload.get("source"),
        "snapshot_timestamp": payload.get("timestamp") or payload.get("last_updated_ts"),
        "cache_age_sec": cache.get("cache_age_sec", cache.get("age_sec", payload.get("cache_age_sec", payload.get("age_sec")))),
        "snapshot_age_sec": cache.get("snapshot_age_sec", payload.get("snapshot_age_sec")),
        "cached_at": cache.get("cached_at", payload.get("cached_at")),
        "ttl_sec": cache.get("ttl_sec", payload.get("ttl_sec")),
        "stale_sec": cache.get("stale_sec", payload.get("stale_sec")),
        "last_error": cache.get("last_error", payload.get("last_error")),
    }
    warning = cache.get("warning", payload.get("warning"))
    if warning:
        freshness["warning"] = warning
    return freshness


def _with_cache(name, builder, ttl_sec=30.0, stale_sec=300.0):
    existing = _cache_get(name)

    if _cache_valid(existing):
        payload = dict(existing["data"])
        payload.setdefault("_cache", {})
        payload["_cache"].update(_cache_meta(existing, "fresh-cache"))
        return payload

    try:
        built = builder()
        entry = _cache_set(name, built, ttl_sec=ttl_sec, stale_sec=stale_sec)
        payload = dict(built)
        payload.setdefault("_cache", {})
        payload["_cache"].update(_cache_meta(entry, "live"))
        return payload
    except Exception as exc:
        if _cache_stale_ok(existing):
            payload = dict(existing["data"])
            payload.setdefault("_cache", {})
            payload["_cache"].update(_cache_meta(existing, "stale-cache", warning=str(exc)))
            payload.setdefault("ok", True)
            _log_api(f"serving stale API cache for {name}: {exc}")
            return payload
        _log_api(f"cache build failed for {name} without stale fallback: {exc}")
        raise


# -------------------------------------------------------------------
# Builders
# -------------------------------------------------------------------

def _build_status():
    state = get_cached_portfolio_state()
    return {
        "ok": True,
        "service": "tradingbot",
        "status": "online",
        "time": int(_now()),
        "portfolio_cache": state,
        "freshness": _freshness_from_payload(state),
    }


def _build_portfolio():
    snapshot = get_portfolio_snapshot()
    persist_current_portfolio_snapshot(snapshot)
    summary = portfolio_summary(snapshot)
    return {
        "ok": True,
        "snapshot": snapshot,
        "summary": summary,
        "freshness": _freshness_from_payload(snapshot),
    }


def _build_rebalance_preview():
    snapshot = get_portfolio_snapshot()
    summary = portfolio_summary(snapshot)
    plan = get_rebalance_plan(snapshot=snapshot, summary=summary)
    harvest = get_profit_harvest_plan(snapshot=snapshot)
    return {
        "ok": True,
        "snapshot": snapshot,
        "summary": summary,
        "plan": plan,
        "harvest": harvest,
        "freshness": _freshness_from_payload(snapshot),
    }


def _build_heatmap():
    snapshot = get_portfolio_snapshot()
    summary = portfolio_summary(snapshot)

    assets = []
    for product_id, row in summary.get("assets", {}).items():
        assets.append({
            "product_id": product_id,
            "class": row.get("class"),
            "weight_total": row.get("weight_total", 0.0),
            "value_total_usd": row.get("value_total_usd", 0.0),
            "price_usd": row.get("price_usd", 0.0),
            "base_qty_total": row.get("base_qty_total", 0.0),
            "unrealized_pnl_pct": row.get("unrealized_pnl_pct", row.get("unrealized_gain_pct", 0.0)),
        })

    assets.sort(key=lambda x: float(x.get("value_total_usd", 0.0)), reverse=True)

    return {
        "ok": True,
        "heatmap": {
            "total_value_usd": snapshot.get("total_value_usd", 0.0),
            "market_regime": summary.get("market_regime"),
            "assets": assets,
        },
        "freshness": _freshness_from_payload(snapshot),
    }


def _build_config():
    cfg = load_asset_config() or {}
    drawdown = cfg.get("drawdown_controls") or {}
    sniper = cfg.get("sniper_mode") or {}
    harvest = cfg.get("profit_harvest") or {}
    rotation = cfg.get("meme_rotation") or {}
    cfg = dict(cfg)
    cfg["drawdown_warn_level"] = drawdown.get("warn_level")
    cfg["drawdown_reduce_level"] = drawdown.get("reduce_level")
    cfg["drawdown_freeze_level"] = drawdown.get("freeze_level")
    cfg["sniper_enabled"] = bool(sniper.get("enabled", True))
    cfg["sniper_buy_scale"] = sniper.get("buy_scale")
    cfg["sniper_min_score"] = sniper.get("min_score")
    cfg["sniper_block_pump_protected"] = bool(sniper.get("block_pump_protected", True))
    cfg["sniper_require_sniper_eligible"] = bool(sniper.get("require_sniper_eligible", True))
    cfg["sniper_relax_require_sniper_eligible"] = bool(sniper.get("relax_require_sniper_eligible", False))
    cfg["sniper_allowed_regimes"] = list(sniper.get("allow_in_regimes", ["bull", "neutral"]) or ["bull", "neutral"])
    cfg["min_harvest_usd"] = harvest.get("min_harvest_usd")
    cfg["max_active_satellites"] = rotation.get("max_active", cfg.get("max_active_satellites"))
    cfg["min_meme_score"] = rotation.get("min_score", cfg.get("min_meme_score"))
    return {
        "ok": True,
        "config": cfg,
        "runtime_settings": {
            "duplicate_window_sec": _safe_int(os.getenv("DUPLICATE_WINDOW_SEC", 60), 60),
            "max_alert_age_sec": _safe_int(os.getenv("MAX_ALERT_AGE_SEC", 120), 120),
            "profit_harvest_cooldown_hours": _safe_int((harvest or {}).get("cooldown_hours", 24), 24),
        },
        "meta": {
            "immediately_active_fields": [
                "max_quote_per_trade_usd",
                "trade_min_value_usd",
                "min_cash_reserve",
                "core_buy_fraction_of_shortfall",
                "satellite_total_max",
                "satellite_total_target",
                "drawdown_warn_level",
                "drawdown_reduce_level",
                "drawdown_freeze_level",
                "min_harvest_usd",
                "sniper_enabled",
                "sniper_buy_scale",
                "sniper_min_score",
                "sniper_block_pump_protected",
                "sniper_require_sniper_eligible",
                "sniper_relax_require_sniper_eligible",
                "sniper_allowed_regimes",
                "max_active_satellites",
                "min_meme_score",
                "max_new_satellites_per_cycle",
            ],
            "runtime_only_fields": [
                "duplicate_window_sec",
                "max_alert_age_sec",
                "profit_harvest_cooldown_hours",
            ],
        },
    }


def _build_meme_rotation():
    snapshot = get_portfolio_snapshot()
    summary = portfolio_summary(snapshot)
    cfg = load_asset_config() or {}
    rotation = load_meme_rotation() or {"candidates": []}

    held_assets = set((summary.get("assets") or {}).keys())
    allowed = set(cfg.get("satellite_allowed", []))
    blocked = set(cfg.get("satellite_blocked", []))
    core_assets = set((cfg.get("core_assets") or {}).keys())
    active_buy_universe = set(summary.get("active_satellite_buy_universe", []))

    asset_rows = summary.get("assets", {})
    candidates = []

    def _first_present(*values):
        for value in values:
            if value is not None:
                return value
        return None

    def _tag_score(value, mapping, default=50.0):
        normalized = str(value or "").strip().lower()
        return float(mapping.get(normalized, default))

    def _compute_shadow_scores(item, asset):
        score_breakdown = item.get("score_breakdown", {}) if isinstance(item.get("score_breakdown"), dict) else {}
        legacy_score = _safe_float(item.get("score"))
        trend_score_raw = item.get("trend_score")
        momentum_bonus = _safe_float(_first_present(item.get("momentum_bonus"), score_breakdown.get("momentum_bonus")))
        change_24h = _first_present(item.get("change_24h"), item.get("price_change_24h_pct"), item.get("price_change_24h"))
        change_24h = None if change_24h is None else _safe_float(change_24h)
        market_cap = _safe_float(item.get("market_cap"))
        total_volume = _safe_float(item.get("total_volume"))
        turnover_ratio = (total_volume / market_cap) if market_cap > 0 else 0.0
        momentum_tag_score = _tag_score(
            item.get("momentum_tag"),
            {
                "surging": 100,
                "strong": 85,
                "bullish": 75,
                "neutral": 50,
                "cooling": 35,
                "weak": 20,
                "crashing": 5,
            },
        )
        volume_tag_score = _tag_score(
            item.get("volume_tag"),
            {
                "explosive": 100,
                "active": 78,
                "normal": 58,
                "thin": 28,
                "illiquid": 10,
            },
        )
        legacy_score_norm = _normalize_score(legacy_score, 0.0, 30.0, default=40.0)
        momentum_bonus_norm = _normalize_score(momentum_bonus, 0.0, 12.0, default=40.0)
        price_momentum_norm = _normalize_score(change_24h, -15.0, 20.0, default=50.0)
        market_cap_norm = _log_normalize_score(market_cap, 1_000_000, 15_000_000_000, default=25.0)
        volume_norm = _log_normalize_score(total_volume, 100_000, 1_000_000_000, default=10.0)
        turnover_norm = _normalize_score(turnover_ratio, 0.01, 0.75, default=25.0)

        trend_quality = (
            _clamp_score(_safe_float(trend_score_raw))
            if trend_score_raw is not None
            else round((legacy_score_norm * 0.65) + (momentum_tag_score * 0.35), 2)
        )
        momentum_score = round((price_momentum_norm * 0.60) + (momentum_tag_score * 0.25) + (momentum_bonus_norm * 0.15), 2)
        relative_strength = round((price_momentum_norm * 0.70) + (volume_tag_score * 0.30), 2)
        liquidity_quality = round((volume_norm * 0.55) + (turnover_norm * 0.25) + (market_cap_norm * 0.20), 2)

        if summary.get("market_regime") == "bull":
            regime_fit_score = (
                88.0 if (change_24h is not None and change_24h >= 0 and not bool(item.get("pump_protected", False)))
                else 52.0 if change_24h is not None and change_24h >= 0
                else 26.0
            )
        elif summary.get("market_regime") == "neutral":
            regime_fit_score = 72.0 if change_24h is not None and change_24h >= -2.0 else 42.0
        else:
            regime_fit_score = (
                38.0 if bool(item.get("pump_protected", False)) or (change_24h is not None and abs(change_24h) <= 5.0)
                else 18.0
            )

        abs_move_24h = abs(change_24h) if change_24h is not None else 0.0
        volatility_bucket = _bucket_from_score(
            abs_move_24h,
            [
                (35.0, "extreme"),
                (18.0, "elevated"),
                (8.0, "active"),
                (0.0, "stable"),
            ],
        )
        liquidity_bucket = _bucket_from_score(
            liquidity_quality,
            [
                (75.0, "high"),
                (45.0, "medium"),
                (0.0, "low"),
            ],
        )

        volatility_penalty = round(
            {
                "stable": 4.0,
                "active": 10.0,
                "elevated": 18.0,
                "extreme": 30.0,
            }[volatility_bucket],
            2,
        )
        overextension_penalty = round(
            max(
                0.0,
                (
                    18.0 if bool(item.get("pump_protected", False)) else 0.0
                ) + (
                    22.0 if change_24h is not None and change_24h >= 45.0 else
                    14.0 if change_24h is not None and change_24h >= 25.0 else
                    7.0 if change_24h is not None and change_24h >= 15.0 else
                    0.0
                ),
            ),
            2,
        )
        churn_penalty = round(
            0.0
            if (
                asset.get("value_total_usd", 0.0) > 0
                or item.get("_allowed")
                or item.get("_core")
            )
            else 6.0,
            2,
        )

        component_scores = {
            "momentum_score": round(momentum_score, 2),
            "trend_quality": round(trend_quality, 2),
            "relative_strength": round(relative_strength, 2),
            "liquidity_quality": round(liquidity_quality, 2),
            "regime_fit_score": round(regime_fit_score, 2),
        }
        gross_raw = sum(component_scores.values())
        gross_score = round(gross_raw / len(component_scores), 2)
        total_penalty = volatility_penalty + overextension_penalty + churn_penalty
        net_score = round(_clamp_score((gross_raw - total_penalty) / len(component_scores)), 2)
        confidence_band = (
            "high" if net_score >= 85.0 else
            "medium" if net_score >= 70.0 else
            "low" if net_score >= 55.0 else
            "watch_only"
        )

        return {
            **component_scores,
            "gross_score": gross_score,
            "net_score": net_score,
            "confidence_band": confidence_band,
            "volatility_bucket": volatility_bucket,
            "liquidity_bucket": liquidity_bucket,
            "regime_fit_score": round(regime_fit_score, 2),
            "overextension_penalty": overextension_penalty,
            "volatility_penalty": volatility_penalty,
            "churn_penalty": churn_penalty,
        }

    def _append_shadow_rotation_log(log_payload):
        global _LAST_SHADOW_ROTATION_LOG_KEY
        log_key = (
            log_payload.get("rotation_generated_at"),
            log_payload.get("rotation_updated_at"),
            log_payload.get("snapshot_timestamp"),
            log_payload.get("candidate_count"),
        )
        with _SHADOW_ROTATION_LOG_LOCK:
            if log_key == _LAST_SHADOW_ROTATION_LOG_KEY:
                return
            with open(SATELLITE_ROTATION_SHADOW_LOG_PATH, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(log_payload, ensure_ascii=False) + "\n")
            _LAST_SHADOW_ROTATION_LOG_KEY = log_key

    def _display_score(row):
        for key in ("net_score", "gross_score", "score"):
            value = row.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except Exception:
                continue
        return 0.0

    def _display_status(row):
        if row.get("blocked") or row.get("enabled") is False:
            return "Paused"
        if row.get("core"):
            return "Core (Portfolio)"
        if row.get("held"):
            return "Live"
        if row.get("allowed"):
            return "Allowed"
        if row.get("active_buy_universe"):
            return "Ready"
        return "Watching"

    def _display_group(row):
        if row.get("blocked") or row.get("enabled") is False:
            return "paused"
        if row.get("held") or row.get("allowed") or row.get("active_buy_universe") or row.get("core"):
            return "active"
        return "watching"

    for item in rotation.get("candidates", []):
        product_id = _normalize_product_id(item.get("product_id"))
        if not product_id:
            continue

        asset = asset_rows.get(product_id, {})
        allowed_flag = product_id in allowed
        blocked_flag = product_id in blocked
        core_flag = product_id in core_assets
        active_buy_universe_flag = product_id in active_buy_universe
        shadow_scores = _compute_shadow_scores(
            {
                **item,
                "_allowed": allowed_flag,
                "_blocked": blocked_flag,
                "_core": core_flag,
                "_active_buy_universe": active_buy_universe_flag,
            },
            asset,
        )

        candidates.append({
            "product_id": product_id,
            "score": float(item.get("score", 0.0) or 0.0),
            "enabled": bool(item.get("enabled", True)),
            "source": item.get("source", "manual"),
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "trend_score": item.get("trend_score"),
            "momentum_bonus": item.get("momentum_bonus"),
            "change_1h": _first_present(item.get("change_1h"), item.get("price_change_1h_pct"), item.get("price_change_1h")),
            "change_24h": _first_present(item.get("change_24h"), item.get("price_change_24h_pct"), item.get("price_change_24h")),
            "change_7d": _first_present(item.get("change_7d"), item.get("price_change_7d_pct"), item.get("price_change_7d")),
            "price_change_24h": _first_present(item.get("price_change_24h"), item.get("price_change_24h_pct"), item.get("change_24h")),
            "held_value_usd": float(asset.get("value_total_usd", 0.0) or 0.0),
            "portfolio_weight": float(asset.get("weight_total", 0.0) or 0.0),
            "class": asset.get("class"),
            "held": product_id in held_assets,
            "allowed": allowed_flag,
            "blocked": blocked_flag,
            "core": core_flag,
            "active_buy_universe": active_buy_universe_flag,
            "unrealized_pnl_pct": (
                asset.get("unrealized_pnl_pct")
                if asset.get("unrealized_pnl_pct") is not None
                else asset.get("unrealized_gain_pct", 0.0)
            ),
            "updated_at": item.get("updated_at"),
            "status": (
                "Blocked" if product_id in blocked else
                "Core" if product_id in core_assets else
                "Enabled" if product_id in allowed else
                "Held" if product_id in held_assets else
                "Watching"
            ),
            **shadow_scores,
        })

    candidates.sort(
        key=lambda x: (
            float(x.get("score", 0.0) or 0.0),
            float(x.get("held_value_usd", 0.0) or 0.0),
        ),
        reverse=True,
    )

    candidate_count = len(candidates)
    current_system_selections = [item["product_id"] for item in get_rotation_products(snapshot)]
    shadow_ranked = sorted(
        candidates,
        key=lambda x: (
            float(x.get("net_score", 0.0) or 0.0),
            float(x.get("gross_score", 0.0) or 0.0),
            float(x.get("score", 0.0) or 0.0),
        ),
        reverse=True,
    )
    shadow_analysis_top_count = min(8, len(shadow_ranked))
    shadow_top_candidates = shadow_ranked[:shadow_analysis_top_count]
    shadow_target_count = len(current_system_selections) or int((cfg.get("meme_rotation") or {}).get("max_active", 8) or 8)
    shadow_top_selection_ids = [row["product_id"] for row in shadow_ranked[:shadow_target_count]]
    shadow_top_allowed_count = sum(1 for row in shadow_top_candidates if bool(row.get("allowed")))
    shadow_top_blocked_count = sum(1 for row in shadow_top_candidates if not bool(row.get("allowed")))
    shadow_top_held_count = sum(1 for row in shadow_top_candidates if bool(row.get("held")))
    shadow_top_new_candidates_count = sum(1 for row in shadow_top_candidates if not bool(row.get("held")))

    blocked_high_score_candidates = []
    for row in shadow_top_candidates:
        if row.get("product_id") in current_system_selections:
            continue

        if bool(row.get("blocked")):
            reason = "blocked"
        elif row.get("enabled") is False:
            reason = "disabled"
        elif bool(row.get("core")):
            reason = "core_asset"
        elif not bool(row.get("active_buy_universe")) and not bool(row.get("held")):
            reason = "not_in_active_universe"
        elif not bool(row.get("allowed")) and not bool(row.get("held")):
            reason = "not_allowed"
        else:
            reason = "below_current_selection_cutoff"

        blocked_high_score_candidates.append({
            "product_id": row.get("product_id"),
            "net_score": row.get("net_score"),
            "reason": reason,
        })

    satellite_decision_summary = {}
    try:
        configured_max_active = _safe_int(
            cfg.get("max_active_satellites")
            or ((cfg.get("meme_rotation") or {}).get("max_active"))
            or shadow_target_count,
            0,
        ) or None
        decision_bundle = build_satellite_decisions(
            shadow_ranked,
            cycles=None,
            current_system_selections=current_system_selections,
            configured_max_active=configured_max_active,
            recent_proposals=list_recent_config_proposals(limit=10, proposal_type=None),
        )
        decision_items = decision_bundle.get("items") if isinstance(decision_bundle, dict) else []
        decision_map = {
            str(row.get("product_id") or "").strip(): row
            for row in (decision_items if isinstance(decision_items, list) else [])
            if str(row.get("product_id") or "").strip()
        }
        satellite_decision_summary = (
            decision_bundle.get("summary")
            if isinstance(decision_bundle, dict) and isinstance(decision_bundle.get("summary"), dict)
            else {}
        )

        for candidate in candidates:
            decision_row = decision_map.get(str(candidate.get("product_id") or "").strip())
            if not decision_row:
                continue
            candidate.update({
                "decision": decision_row.get("decision"),
                "decision_reason": decision_row.get("decision_reason"),
                "decision_blockers": decision_row.get("decision_blockers") if isinstance(decision_row.get("decision_blockers"), list) else [],
                "decision_confidence": decision_row.get("decision_confidence"),
                "replacement_target": decision_row.get("replacement_target"),
                "replacement_score_delta": decision_row.get("replacement_score_delta"),
                "stability_hits": decision_row.get("stability_hits"),
                "stability_window_cycles": decision_row.get("stability_window_cycles"),
                "active_satellite_count": decision_row.get("active_satellite_count"),
                "configured_max_active": decision_row.get("configured_max_active"),
                "slots_remaining": decision_row.get("slots_remaining"),
                "held_context": decision_row.get("held_context"),
                "slot_pressure": decision_row.get("slot_pressure"),
                "portfolio_pressure": decision_row.get("portfolio_pressure"),
                "portfolio_context_note": decision_row.get("portfolio_context_note"),
            })
    except Exception as exc:
        _log_api(f"satellite decision engine enrichment skipped: {exc}")

    canonical_ranked = sorted(
        candidates,
        key=lambda x: (
            _display_score(x),
            float(x.get("gross_score", 0.0) or 0.0),
            float(x.get("score", 0.0) or 0.0),
        ),
        reverse=True,
    )
    rank_lookup = {
        str(row.get("product_id") or "").strip(): index + 1
        for index, row in enumerate(canonical_ranked)
        if str(row.get("product_id") or "").strip()
    }
    for candidate in candidates:
        candidate["display_score"] = round(_display_score(candidate), 2)
        candidate["display_status"] = _display_status(candidate)
        candidate["display_group"] = _display_group(candidate)
        candidate["display_rank"] = rank_lookup.get(str(candidate.get("product_id") or "").strip())
    candidates = canonical_ranked

    _append_shadow_rotation_log({
        "logged_at": int(_now()),
        "rotation_generated_at": _safe_int(rotation.get("generated_at")),
        "rotation_updated_at": _safe_int(rotation.get("updated_at")),
        "snapshot_timestamp": _safe_int(snapshot.get("timestamp")),
        "market_regime": summary.get("market_regime"),
        "candidate_count": candidate_count,
        "current_system_selections": current_system_selections,
        "shadow_top_selection_ids": shadow_top_selection_ids,
        "shadow_top_allowed_count": shadow_top_allowed_count,
        "shadow_top_blocked_count": shadow_top_blocked_count,
        "shadow_top_held_count": shadow_top_held_count,
        "shadow_top_new_candidates_count": shadow_top_new_candidates_count,
        "blocked_high_score_candidates": blocked_high_score_candidates,
        "ranking_by_net_score": [
            {
                "product_id": row.get("product_id"),
                "legacy_score": row.get("score"),
                "gross_score": row.get("gross_score"),
                "net_score": row.get("net_score"),
                "confidence_band": row.get("confidence_band"),
                "volatility_bucket": row.get("volatility_bucket"),
                "liquidity_bucket": row.get("liquidity_bucket"),
                "regime_fit_score": row.get("regime_fit_score"),
                "momentum_score": row.get("momentum_score"),
                "trend_quality": row.get("trend_quality"),
                "relative_strength": row.get("relative_strength"),
                "liquidity_quality": row.get("liquidity_quality"),
                "volatility_penalty": row.get("volatility_penalty"),
                "overextension_penalty": row.get("overextension_penalty"),
                "churn_penalty": row.get("churn_penalty"),
                "status": row.get("status"),
                "held": bool(row.get("held")),
                "allowed": bool(row.get("allowed")),
                "blocked": bool(row.get("blocked")),
                "active_buy_universe": bool(row.get("active_buy_universe")),
            }
            for row in shadow_ranked
        ],
    })

    return {
        "ok": True,
        "meme_rotation": rotation,
        "count": candidate_count,
        "candidate_count": candidate_count,
        "market_regime": summary.get("market_regime"),
        "active_satellite_buy_universe": sorted(active_buy_universe),
        "current_system_selections": current_system_selections,
        "satellite_decision_summary": satellite_decision_summary,
        "candidates": candidates,
        "last_updated_ts": _safe_int(rotation.get("updated_at")) or _safe_int(rotation.get("generated_at")) or _safe_int(snapshot.get("timestamp")),
        "ranking_source": "meme_rotation_shadow_v1",
        "freshness": _freshness_from_payload(snapshot),
    }


def _build_meme_heatmap():
    payload = _build_meme_rotation()
    return {
        "ok": True,
        "meme_heatmap": {
            "count": payload.get("count", 0),
            "market_regime": payload.get("market_regime"),
            "active_satellite_buy_universe": payload.get("active_satellite_buy_universe", []),
            "tiles": payload.get("candidates", []),
        },
    }


def _build_portfolio_summary_v2():
    realized_pnl_total = 0.0
    realized_pnl_points = 0
    trade_count = 0

    try:
        conn = _trading_db_conn()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) AS c FROM orders")
        trade_count = _safe_int(cur.fetchone()["c"])

        cur.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('realized_pnl', 'pnl_history')"
        )
        available_tables = {str(row["name"]) for row in cur.fetchall() if row and row["name"]}

        if "realized_pnl" in available_tables:
            cur.execute("SELECT COUNT(*) AS c, COALESCE(SUM(pnl_usd), 0) AS total FROM realized_pnl")
            row = cur.fetchone()
            realized_pnl_points = _safe_int(row["c"])
            realized_pnl_total = _safe_float(row["total"])
        elif "pnl_history" in available_tables:
            cur.execute("SELECT COUNT(*) AS c, COALESCE(SUM(realized_pnl), 0) AS total FROM pnl_history")
            row = cur.fetchone()
            realized_pnl_points = _safe_int(row["c"])
            realized_pnl_total = _safe_float(row["total"])

        conn.close()
    except Exception as exc:
        _log_api(f"portfolio summary DB stats degraded: {exc}")

    try:
        snapshot = get_portfolio_snapshot()
        persist_current_portfolio_snapshot(snapshot)
        summary = portfolio_summary(snapshot)
        freshness = _freshness_from_payload(snapshot)

        assets = list((summary.get("assets") or {}).values())
        assets.sort(key=lambda x: float(x.get("value_total_usd", 0.0) or 0.0), reverse=True)
        top_asset = assets[0]["product_id"] if assets else None

        return {
            "ok": True,
            "summary": {
                "timestamp": summary.get("timestamp"),
                "total_value_usd": _safe_float(summary.get("total_value_usd")),
                "usd_cash": _safe_float(summary.get("usd_cash")),
                "invested_usd": max(
                    0.0,
                    _safe_float(summary.get("total_value_usd")) - _safe_float(summary.get("usd_cash"))
                ),
                "cash_weight": _safe_float(summary.get("cash_weight")),
                "core_value_usd": _safe_float(summary.get("core_value_usd")),
                "core_weight": _safe_float(summary.get("core_weight")),
                "satellite_value_usd": _safe_float(summary.get("satellite_value_usd")),
                "satellite_weight": _safe_float(summary.get("satellite_weight")),
                "blocked_value_usd": _safe_float(summary.get("blocked_value_usd")),
                "blocked_weight": _safe_float(summary.get("blocked_weight")),
                "asset_count": len(assets),
                "market_regime": summary.get("market_regime"),
                "realized_pnl_total": realized_pnl_total,
                "realized_pnl_points": realized_pnl_points,
                "trade_count": trade_count,
                "top_asset": top_asset,
                "data_mode": "live",
                "freshness": freshness,
            },
        }

    except Exception as exc:
        return {
            "ok": True,
            "summary": {
                "timestamp": int(time.time()),
                "total_value_usd": 0.0,
                "usd_cash": 0.0,
                "invested_usd": 0.0,
                "cash_weight": 0.0,
                "core_value_usd": 0.0,
                "core_weight": 0.0,
                "satellite_value_usd": 0.0,
                "satellite_weight": 0.0,
                "blocked_value_usd": 0.0,
                "blocked_weight": 0.0,
                "asset_count": 0,
                "market_regime": "unknown",
                "realized_pnl_total": realized_pnl_total,
                "realized_pnl_points": realized_pnl_points,
                "trade_count": trade_count,
                "top_asset": None,
                "data_mode": "degraded",
                "warning": str(exc),
                "freshness": {},
            },
        }


def _build_portfolio_allocations():
    try:
        snapshot = get_portfolio_snapshot()
        summary = portfolio_summary(snapshot)

        rows = []
        for product_id, row in (summary.get("assets") or {}).items():
            rows.append({
                "product_id": product_id,
                "class": row.get("class"),
                "value_total_usd": _safe_float(row.get("value_total_usd")),
                "weight_total": _safe_float(row.get("weight_total")),
                "base_qty_total": _safe_float(row.get("base_qty_total")),
                "price_usd": _safe_float(row.get("price_usd")),
            })

        rows.sort(key=lambda x: x["value_total_usd"], reverse=True)

        return {
            "ok": True,
            "allocations": rows,
            "data_mode": "live",
            "freshness": _freshness_from_payload(snapshot),
        }

    except Exception as exc:
        return {
            "ok": True,
            "allocations": [],
            "data_mode": "degraded",
            "warning": str(exc),
            "freshness": {},
        }


def _build_portfolio_history():
    range_name = (request.args.get("range") or "30d").lower().strip()
    start_ts = _range_to_start_ts(range_name)

    points = []
    series_type = "empty"
    analytics = build_portfolio_history_analytics([], source="empty")
    risk_score = normalize_risk_score_payload()
    adaptive_suggestions = normalize_adaptive_suggestions_payload()
    auto_adaptive = normalize_auto_adaptive_payload()

    def _advisory_payload(history_analytics):
        try:
            snapshot = get_portfolio_snapshot()
            summary = portfolio_summary(snapshot)
            snapshot_total = float(snapshot.get("total_value_usd", 0.0) or 0.0)
            summary_total = float(summary.get("total_value_usd", 0.0) or 0.0)
            position_count = len(snapshot.get("positions", {}) or {})
            _log_api(
                "advisory live snapshot "
                f"total={snapshot_total:.2f} "
                f"summary_total={summary_total:.2f} "
                f"positions={position_count}"
            )
            live_risk_score = build_portfolio_risk_score(
                snapshot=snapshot,
                summary=summary,
                history_analytics=history_analytics,
            )
            advisory = {
                "risk_score": live_risk_score,
                "adaptive_suggestions": build_adaptive_suggestions(
                    snapshot=snapshot,
                    summary=summary,
                    history_analytics=history_analytics,
                    risk_score=live_risk_score,
                ),
                "auto_adaptive": build_auto_adaptive_recommendation(
                    snapshot=snapshot,
                    summary=summary,
                    history_analytics=history_analytics,
                    risk_score=live_risk_score,
                ),
            }
            return {
                "risk_score": normalize_risk_score_payload(advisory.get("risk_score")),
                "adaptive_suggestions": normalize_adaptive_suggestions_payload(advisory.get("adaptive_suggestions")),
                "auto_adaptive": normalize_auto_adaptive_payload(advisory.get("auto_adaptive")),
            }
        except Exception as exc:
            _log_api(f"advisory payload degraded during history build: {exc}")
            return {
                "risk_score": normalize_risk_score_payload(risk_score, fallback_note=str(exc)),
                "adaptive_suggestions": normalize_adaptive_suggestions_payload(
                    adaptive_suggestions,
                    fallback_note=str(exc),
                ),
                "auto_adaptive": normalize_auto_adaptive_payload(auto_adaptive, fallback_reason=str(exc)),
            }

    def _is_isolated_history_glitch(prev_row, row, next_row):
        prev_total = _safe_float(prev_row.get("total_value_usd"))
        row_total = _safe_float(row.get("total_value_usd"))
        next_total = _safe_float(next_row.get("total_value_usd"))

        if prev_total <= 0 or row_total <= 0 or next_total <= 0:
            return False

        anchor_high = max(prev_total, next_total)
        anchor_low = min(prev_total, next_total)
        if anchor_high <= 0:
            return False

        anchors_close = abs(prev_total - next_total) / anchor_high <= 0.20
        sharp_plunge = row_total < anchor_low * 0.50
        sharp_spike = row_total > anchor_high * 2.0
        return anchors_close and (sharp_plunge or sharp_spike)

    def _filter_portfolio_history_rows(rows):
        filtered = []
        dropped = 0

        for idx, row in enumerate(rows):
            total_value = _safe_float(row.get("total_value_usd"))
            cash_value = _safe_float(row.get("cash_value_usd"))

            if total_value <= 0 or cash_value < 0 or cash_value > total_value:
                dropped += 1
                continue

            if 0 < idx < len(rows) - 1 and _is_isolated_history_glitch(rows[idx - 1], row, rows[idx + 1]):
                dropped += 1
                continue

            filtered.append(row)

        if dropped:
            _log_api(f"filtered {dropped} invalid portfolio_history row(s) from API response")

        return filtered

    def _is_isolated_history_point_glitch(prev_point, point, next_point):
        prev_total = _safe_float(prev_point.get("equity_usd"))
        row_total = _safe_float(point.get("equity_usd"))
        next_total = _safe_float(next_point.get("equity_usd"))

        if prev_total <= 0 or row_total <= 0 or next_total <= 0:
            return False

        anchor_high = max(prev_total, next_total)
        anchor_low = min(prev_total, next_total)
        if anchor_high <= 0:
            return False

        anchors_close = abs(prev_total - next_total) / anchor_high <= 0.20
        sharp_plunge = row_total < anchor_low * 0.95
        sharp_spike = row_total > anchor_high * 1.05
        return anchors_close and (sharp_plunge or sharp_spike)

    def _filter_portfolio_value_points(points):
        filtered = []
        dropped = 0

        for idx, point in enumerate(points):
            equity_value = _safe_float(point.get("equity_usd"))
            if equity_value <= 0:
                dropped += 1
                continue

            if 0 < idx < len(points) - 1 and _is_isolated_history_point_glitch(points[idx - 1], point, points[idx + 1]):
                dropped += 1
                continue

            filtered.append(point)

        if dropped:
            _log_api(f"filtered {dropped} invalid portfolio_value point(s) from history response")

        return filtered

    try:
        history_rows = get_portfolio_history_since(start_ts=start_ts)
        history_rows = _filter_portfolio_history_rows(history_rows)
        analytics = build_portfolio_history_analytics(history_rows, source="portfolio_history")
        advisory_payload = _advisory_payload(analytics)
        risk_score = advisory_payload["risk_score"]
        adaptive_suggestions = advisory_payload["adaptive_suggestions"]
        auto_adaptive = advisory_payload["auto_adaptive"]
        points = [
            {
                "ts": _safe_int(row.get("ts")),
                "equity_usd": _safe_float(row.get("total_value_usd")),
            }
            for row in history_rows
        ]
        points = _filter_portfolio_value_points(points)
        if points:
            return {
                "ok": True,
                "range": range_name,
                "series_type": "portfolio_value",
                "points": points,
                "analytics": analytics,
                "risk_score": risk_score,
                "adaptive_suggestions": adaptive_suggestions,
                "auto_adaptive": auto_adaptive,
            }
    except Exception as exc:
        _log_api(f"portfolio history snapshot source unavailable: {exc}")
        points = []

    def _table_exists(conn, table_name):
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        return bool(row)

    def _table_columns(conn, table_name):
        try:
            rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        except Exception:
            return set()
        return {str(r["name"]).strip().lower() for r in rows}

    def _timestamp_expr(columns):
        if "ts" in columns:
            return "CAST(ts AS INTEGER)"
        if "timestamp" in columns:
            return "CAST(timestamp AS INTEGER)"
        if "created_at" in columns:
            return "CAST(strftime('%s', created_at) AS INTEGER)"
        return None

    conn = None
    try:
        conn = _trading_db_conn()
        cur = conn.cursor()

        if _table_exists(conn, "pnl_history"):
            columns = _table_columns(conn, "pnl_history")
            ts_expr = _timestamp_expr(columns)
            value_column = None

            for candidate in ("equity_usd", "portfolio_value_usd", "total_value_usd"):
                if candidate in columns:
                    value_column = candidate
                    break

            if value_column and ts_expr:
                if start_ts is None:
                    cur.execute(
                        f"""
                        SELECT {ts_expr} AS ts, {value_column} AS equity_usd
                        FROM pnl_history
                        ORDER BY ts ASC
                        """
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT {ts_expr} AS ts, {value_column} AS equity_usd
                        FROM pnl_history
                        WHERE {ts_expr} >= ?
                        ORDER BY ts ASC
                        """,
                        (start_ts,),
                    )

                rows = cur.fetchall()
                points = [
                    {
                        "ts": _safe_int(r["ts"]),
                        "equity_usd": _safe_float(r["equity_usd"]),
                    }
                    for r in rows
                ]
                points = _filter_portfolio_value_points(points)
                if points:
                    series_type = "portfolio_value"
            elif "realized_pnl" in columns and ts_expr:
                if start_ts is None:
                    cur.execute(
                        f"""
                        SELECT {ts_expr} AS ts, realized_pnl
                        FROM pnl_history
                        ORDER BY ts ASC
                        """
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT {ts_expr} AS ts, realized_pnl
                        FROM pnl_history
                        WHERE {ts_expr} >= ?
                        ORDER BY ts ASC
                        """,
                        (start_ts,),
                    )

                points = [
                    {
                        "ts": _safe_int(r["ts"]),
                        "realized_pnl": _safe_float(r["realized_pnl"]),
                    }
                    for r in cur.fetchall()
                ]
                if points:
                    series_type = "realized_pnl"

        if not points and _table_exists(conn, "realized_pnl"):
            columns = _table_columns(conn, "realized_pnl")
            ts_expr = _timestamp_expr(columns)
            if ts_expr and "pnl_usd" in columns:
                if start_ts is None:
                    cur.execute(
                        f"""
                        SELECT {ts_expr} AS ts, pnl_usd
                        FROM realized_pnl
                        ORDER BY ts ASC, id ASC
                        """
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT {ts_expr} AS ts, pnl_usd
                        FROM realized_pnl
                        WHERE {ts_expr} >= ?
                        ORDER BY ts ASC, id ASC
                        """,
                        (start_ts,),
                    )

                running_pnl = 0.0
                for row in cur.fetchall():
                    running_pnl += _safe_float(row["pnl_usd"])
                    points.append(
                        {
                            "ts": _safe_int(row["ts"]),
                            "realized_pnl": running_pnl,
                        }
                    )

                if points:
                    series_type = "realized_pnl"

    except Exception as exc:
        _log_api(f"portfolio history fallback degraded: {exc}")
        points = []
        series_type = "empty"
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    if points:
        if series_type != "portfolio_value":
            analytics = build_portfolio_history_analytics(
                [],
                source=series_type,
                note="Persisted portfolio history is still building, so analytics are limited while the chart falls back to realized PnL history.",
            )
        advisory_payload = _advisory_payload(analytics)
        risk_score = advisory_payload["risk_score"]
        adaptive_suggestions = advisory_payload["adaptive_suggestions"]
        auto_adaptive = advisory_payload["auto_adaptive"]
        return {
            "ok": True,
            "range": range_name,
            "series_type": series_type,
            "points": points,
            "analytics": analytics,
            "risk_score": risk_score,
            "adaptive_suggestions": adaptive_suggestions,
            "auto_adaptive": auto_adaptive,
        }

    advisory_payload = _advisory_payload(analytics)
    risk_score = advisory_payload["risk_score"]
    adaptive_suggestions = advisory_payload["adaptive_suggestions"]
    auto_adaptive = advisory_payload["auto_adaptive"]
    return {
        "ok": True,
        "range": range_name,
        "series_type": "empty",
        "points": [],
        "analytics": analytics,
        "risk_score": risk_score,
        "adaptive_suggestions": adaptive_suggestions,
        "auto_adaptive": auto_adaptive,
    }


def _build_trade_history():
    product_id = (request.args.get("product_id") or "").strip().upper()
    side = (request.args.get("side") or "").strip().upper()
    status = (request.args.get("status") or "").strip().upper()
    page = max(1, _safe_int(request.args.get("page"), 1))
    page_size = min(250, max(10, _safe_int(request.args.get("page_size"), 50)))
    offset = (page - 1) * page_size

    where = []
    params = []

    if product_id:
        where.append("product_id = ?")
        params.append(product_id)

    if side:
        where.append("side = ?")
        params.append(side)

    if status:
        where.append("status = ?")
        params.append(status)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    conn = _trading_db_conn()
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) AS c FROM orders {where_sql}", params)
    total = _safe_int(cur.fetchone()["c"])

    cur.execute(
        f"""
        SELECT order_id, product_id, side, base_size, price, status, created_at
        FROM orders
        {where_sql}
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
        """,
        params + [page_size, offset],
    )
    rows = cur.fetchall()
    conn.close()

    trades = [_normalize_trade_row(r) for r in rows]

    return {
        "ok": True,
        "page": page,
        "page_size": page_size,
        "total": total,
        "trades": trades,
    }


def _build_trade_stats():
    product_id = (request.args.get("product_id") or "").strip().upper()
    side = (request.args.get("side") or "").strip().upper()
    status = (request.args.get("status") or "").strip().upper()

    where = []
    params = []

    if product_id:
        where.append("product_id = ?")
        params.append(product_id)

    if side:
        where.append("side = ?")
        params.append(side)

    if status:
        where.append("status = ?")
        params.append(status)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    conn = _trading_db_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT
            COUNT(*) AS trade_count,
            COALESCE(SUM(CASE WHEN side = 'BUY' THEN base_size * price ELSE 0 END), 0) AS buy_notional_usd,
            COALESCE(SUM(CASE WHEN side = 'SELL' THEN base_size * price ELSE 0 END), 0) AS sell_notional_usd,
            COALESCE(SUM(base_size * price), 0) AS gross_notional_usd,
            MAX(created_at) AS last_trade_ts
        FROM orders
        {where_sql}
        """,
        params,
    )
    row = cur.fetchone()
    conn.close()

    return {
        "ok": True,
        "stats": {
            "trade_count": _safe_int(row["trade_count"]),
            "buy_notional_usd": _safe_float(row["buy_notional_usd"]),
            "sell_notional_usd": _safe_float(row["sell_notional_usd"]),
            "gross_notional_usd": _safe_float(row["gross_notional_usd"]),
            "last_trade_ts": _safe_int(row["last_trade_ts"]),
        },
    }


def _build_system_snapshot():
    portfolio_payload = _with_cache("portfolio", _build_portfolio, ttl_sec=30, stale_sec=300)
    status_payload = _with_cache("status", _build_status, ttl_sec=10, stale_sec=120)
    config_payload = _with_cache("config", _build_config, ttl_sec=10, stale_sec=120)
    meme_rotation_payload = _with_cache("meme_rotation", _build_meme_rotation, ttl_sec=10, stale_sec=120)
    portfolio_summary_payload = _with_cache(
        "portfolio_summary_v2",
        _build_portfolio_summary_v2,
        ttl_sec=20,
        stale_sec=120,
    )
    trade_stats_payload = _build_trade_stats()

    try:
        recent_count = min(100, max(1, _safe_int(request.args.get("recent_count"), 25)))
    except Exception:
        recent_count = 25

    recent_trades = []
    try:
        conn = _trading_db_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT order_id, product_id, side, base_size, price, status, created_at
            FROM orders
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (recent_count,),
        )
        rows = cur.fetchall()
        conn.close()
        recent_trades = [_normalize_trade_row(r) for r in rows]
    except Exception as exc:
        recent_trades = [{
            "warning": str(exc),
        }]

    try:
        valid_product_ids = get_valid_product_ids(quote_currency="USD", tradable_only=True)
    except Exception:
        valid_product_ids = []

    try:
        admin_state = get_admin_state()
    except Exception as exc:
        admin_state = {
            "warning": str(exc),
        }

    snapshot = portfolio_payload.get("snapshot", {}) if isinstance(portfolio_payload, dict) else {}
    summary = portfolio_payload.get("summary", {}) if isinstance(portfolio_payload, dict) else {}

    return {
        "ok": True,
        "timestamp": int(_now()),
        "status": {
            "service": status_payload.get("service"),
            "status": status_payload.get("status"),
            "time": status_payload.get("time"),
            "portfolio_cache": status_payload.get("portfolio_cache"),
            "_cache": status_payload.get("_cache", {}),
        },
        "portfolio": snapshot,
        "portfolio_summary": portfolio_summary_payload.get("summary", summary),
        "config": config_payload.get("config", {}),
        "admin_state": admin_state,
        "meme_rotation": meme_rotation_payload.get("rotation", meme_rotation_payload),
        "valid_product_ids": valid_product_ids,
        "trade_stats": trade_stats_payload.get("stats", {}),
        "recent_trades": recent_trades,
        "meta": {
            "recent_trade_count": len(recent_trades),
            "quote_currency": "USD",
            "source": "system_snapshot_v1",
        },
    }


def _build_shadow_rotation_report():
    return build_shadow_rotation_report(window_hours=24)


def _build_tradable_universe():
    snapshot = get_portfolio_snapshot()
    if not isinstance(snapshot, dict):
        raise RuntimeError("portfolio snapshot unavailable")

    config = load_asset_config() or {}
    summary = portfolio_summary(snapshot)

    core_assets = sorted(
        str(product_id).upper().strip()
        for product_id in (config.get("core_assets") or {}).keys()
        if str(product_id or "").strip()
    )
    satellite_allowed = sorted(
        str(product_id).upper().strip()
        for product_id in (config.get("satellite_allowed") or [])
        if str(product_id or "").strip()
    )
    satellite_blocked = sorted(
        str(product_id).upper().strip()
        for product_id in (config.get("satellite_blocked") or [])
        if str(product_id or "").strip()
    )

    active_satellite_buy_universe = sorted(
        set(
            str(product_id).upper().strip()
            for product_id in (
                summary.get("active_satellite_buy_universe")
                or snapshot.get("active_satellite_buy_universe")
                or []
            )
            if str(product_id or "").strip()
        )
    )

    current_system_selections = sorted(
        set(
            str((row or {}).get("product_id") or "").upper().strip()
            for row in (get_rotation_products(snapshot) or [])
            if str((row or {}).get("product_id") or "").strip()
        )
    )

    return {
        "ok": True,
        "generated_at": int(_now()),
        "snapshot_timestamp": _safe_int(snapshot.get("timestamp")),
        "satellite_mode": str(config.get("satellite_mode", "rotation") or "rotation"),
        "core_assets": core_assets,
        "satellite_allowed": satellite_allowed,
        "satellite_blocked": satellite_blocked,
        "active_satellite_buy_universe": active_satellite_buy_universe,
        "current_system_selections": current_system_selections,
        "summary": {
            "core_asset_count": len(core_assets),
            "satellite_allowed_count": len(satellite_allowed),
            "satellite_blocked_count": len(satellite_blocked),
            "active_satellite_buy_universe_count": len(active_satellite_buy_universe),
            "current_system_selection_count": len(current_system_selections),
        },
        "meta": {
            "source": "tradable_universe_v1",
            "active_buy_universe_is_live_eligible_set": True,
            "current_system_selections_are_rotation_selection_set": True,
        },
    }


def _asset_mode_from_config(product_id, config):
    product_id = _normalize_product_id(product_id)
    config = config if isinstance(config, dict) else {}
    core_assets = config.get("core_assets") or {}
    satellite_allowed = set(_normalize_product_id(x) for x in (config.get("satellite_allowed") or []))
    satellite_blocked = set(_normalize_product_id(x) for x in (config.get("satellite_blocked") or []))

    if product_id in core_assets:
        return "core"
    if product_id in satellite_blocked:
        return "disable"
    if product_id in satellite_allowed:
        return "enable"
    return "auto"


def _build_asset_config_rows():
    snapshot = get_portfolio_snapshot()
    if not isinstance(snapshot, dict):
        raise RuntimeError("portfolio snapshot unavailable")

    config = load_asset_config() or {}
    universe = _build_tradable_universe()
    summary = portfolio_summary(snapshot)
    valid_product_ids = {
        _normalize_product_id(product_id)
        for product_id in (get_valid_product_ids(quote_currency="USD", tradable_only=True) or [])
        if _normalize_product_id(product_id)
    }
    configured_product_ids = {
        _normalize_product_id(product_id)
        for product_id in list((config.get("core_assets") or {}).keys())
        + list(config.get("satellite_allowed") or [])
        + list(config.get("satellite_blocked") or [])
        if _normalize_product_id(product_id)
    }
    position_product_ids = {
        _normalize_product_id(product_id)
        for product_id in (snapshot.get("positions") or {}).keys()
        if _normalize_product_id(product_id).endswith("-USD")
    }
    active_buy_universe = {
        _normalize_product_id(product_id)
        for product_id in (universe.get("active_satellite_buy_universe") or [])
        if _normalize_product_id(product_id)
    }
    system_selected = {
        _normalize_product_id(product_id)
        for product_id in (universe.get("current_system_selections") or [])
        if _normalize_product_id(product_id)
    }

    product_ids = sorted(
        valid_product_ids
        | configured_product_ids
        | position_product_ids
        | active_buy_universe
        | system_selected
    )
    positions = snapshot.get("positions") or {}
    items = []

    for product_id in product_ids:
        position = positions.get(product_id) or {}
        held_value_usd = float(
            position.get("value_total_usd")
            or position.get("value_usd")
            or position.get("usd_value")
            or 0.0
        )
        mode = _asset_mode_from_config(product_id, config)
        is_valid_product = product_id in valid_product_ids
        invalid_reason = "" if is_valid_product else "not_valid_tradable_usd_product"
        can_assign_core = bool(is_valid_product)
        can_enable = bool(is_valid_product)
        items.append(
            {
                "product_id": product_id,
                "quote_currency_id": "USD",
                "state": mode,
                "manual_state": mode,
                "effective_state": mode,
                "is_core": product_id in (config.get("core_assets") or {}),
                "is_enabled": mode == "enable",
                "is_disabled": mode == "disable",
                "is_auto": mode == "auto",
                "is_valid_product": is_valid_product,
                "editable": bool(is_valid_product or mode in {"auto", "disable"}),
                "can_assign_core": can_assign_core,
                "can_enable": can_enable,
                "invalid_reason": invalid_reason,
                "held_value_usd": held_value_usd,
                "active_buy_universe": product_id in active_buy_universe,
                "system_selected": product_id in system_selected,
                "target_weight": float(((config.get("core_assets") or {}).get(product_id) or {}).get("target_weight", 0.0) or 0.0),
                "rebalance_band": float(((config.get("core_assets") or {}).get(product_id) or {}).get("rebalance_band", 0.0) or 0.0),
            }
        )

    return {
        "ok": True,
        "generated_at": int(_now()),
        "snapshot_timestamp": _safe_int(snapshot.get("timestamp")),
        "items": items,
        "summary": {
            "total_count": len(items),
            "core_count": len([item for item in items if item.get("state") == "core"]),
            "enabled_count": len([item for item in items if item.get("state") == "enable"]),
            "auto_count": len([item for item in items if item.get("state") == "auto"]),
            "disabled_count": len([item for item in items if item.get("state") == "disable"]),
            "active_buy_universe_count": len(active_buy_universe),
            "system_selected_count": len(system_selected),
        },
        "meta": {
            "source": "asset_config_rows_v1",
            "satellite_mode": str(config.get("satellite_mode", "rotation") or "rotation"),
            "market_regime": str(summary.get("market_regime", "") or ""),
        },
    }


def _build_tradingview_manifest():
    universe = _build_tradable_universe()

    core_assets = sorted(
        set(str(product_id).upper().strip() for product_id in (universe.get("core_assets") or []) if str(product_id or "").strip())
    )
    satellite_allowed = sorted(
        set(str(product_id).upper().strip() for product_id in (universe.get("satellite_allowed") or []) if str(product_id or "").strip())
    )
    satellite_blocked = sorted(
        set(str(product_id).upper().strip() for product_id in (universe.get("satellite_blocked") or []) if str(product_id or "").strip())
    )
    active_satellite_buy_universe = sorted(
        set(
            str(product_id).upper().strip()
            for product_id in (universe.get("active_satellite_buy_universe") or [])
            if str(product_id or "").strip()
        )
    )
    current_system_selections = sorted(
        set(
            str(product_id).upper().strip()
            for product_id in (universe.get("current_system_selections") or [])
            if str(product_id or "").strip()
        )
    )

    satellite_exit = sorted(
        set(satellite_allowed)
        | set(active_satellite_buy_universe)
        | set(current_system_selections)
    )

    manifest = {
        "ok": True,
        "generated_at": int(_now()),
        "snapshot_timestamp": universe.get("snapshot_timestamp"),
        "source": "tradingview_manifest_v1",
        "version": 1,
        "satellite_mode": universe.get("satellite_mode"),
        "core_assets": core_assets,
        "satellite_allowed": satellite_allowed,
        "satellite_blocked": satellite_blocked,
        "active_satellite_buy_universe": active_satellite_buy_universe,
        "current_system_selections": current_system_selections,
        "strategy_groups": {
            "core_buy": core_assets,
            "core_exit": core_assets,
            "satellite_buy": active_satellite_buy_universe,
            "satellite_exit": satellite_exit,
            "sniper_buy": active_satellite_buy_universe,
        },
        "summary": {
            "core_asset_count": len(core_assets),
            "satellite_allowed_count": len(satellite_allowed),
            "satellite_blocked_count": len(satellite_blocked),
            "active_satellite_buy_universe_count": len(active_satellite_buy_universe),
            "current_system_selection_count": len(current_system_selections),
            "core_buy_count": len(core_assets),
            "core_exit_count": len(core_assets),
            "satellite_buy_count": len(active_satellite_buy_universe),
            "satellite_exit_count": len(satellite_exit),
            "sniper_buy_count": len(active_satellite_buy_universe),
        },
        "notes": {
            "purpose": "Server-authoritative symbol sets for downstream TradingView maintenance and diagnostics tooling.",
            "satellite_buy_definition": "Current live-eligible satellite buy universe.",
            "satellite_exit_definition": "Best-effort managed satellite set inferred from allowed symbols, active buy universe, and current system selections.",
            "sniper_buy_definition": "Best-effort mirror of the live active satellite buy universe until a narrower dedicated sniper manifest is exported.",
        },
        "meta": {
            "built_from": "tradable_universe_v1",
            "server_authoritative": True,
            "read_only": True,
        },
    }

    return manifest


def _proposal_actor():
    return (
        str(session.get("username") or "").strip()
        or str(session.get("email") or "").strip()
        or str(session.get("user_id") or "").strip()
        or None
    )


# -------------------------------------------------------------------
# Session/bootstrap helpers
# -------------------------------------------------------------------

@api_bp.route("/api/auth/bootstrap_status", methods=["GET"])
def api_auth_bootstrap_status():
    try:
        conn = _trading_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM users")
        row = cur.fetchone()
        conn.close()
        user_count = _safe_int(row["c"])
    except Exception:
        user_count = 0

    return jsonify({
        "ok": True,
        "session_authenticated": _has_session_user(),
        "session_is_admin": _has_session_admin(),
        "api_secret_enabled": bool(get_api_secrets()),
        "user_count": user_count,
        "needs_bootstrap": user_count == 0,
    })


# -------------------------------------------------------------------
# Read endpoints
# -------------------------------------------------------------------

@api_bp.route("/api/status", methods=["GET"])
@require_secret
def api_status():
    try:
        return jsonify(_with_cache("status", _build_status, ttl_sec=10, stale_sec=120))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/system_status", methods=["GET"])
@require_secret
def api_system_status_alias():
    try:
        return jsonify(_with_cache("status", _build_status, ttl_sec=10, stale_sec=120))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/system-status", methods=["GET"])
@require_secret
def api_system_status_alias_dash():
    try:
        return jsonify(_with_cache("status", _build_status, ttl_sec=10, stale_sec=120))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/system_snapshot", methods=["GET"])
@require_secret
def api_system_snapshot():
    try:
        return jsonify(_with_cache("system_snapshot", _build_system_snapshot, ttl_sec=15, stale_sec=120))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/system/tradable_universe", methods=["GET"])
@require_secret
def api_system_tradable_universe():
    try:
        return jsonify(_with_cache("tradable_universe", _build_tradable_universe, ttl_sec=15, stale_sec=120))
    except Exception as exc:
        return jsonify({
            "ok": False,
            "error": str(exc),
            "source": "tradable_universe_v1",
        }), 500


@api_bp.route("/api/system/tradingview_manifest", methods=["GET"])
@require_secret
def api_system_tradingview_manifest():
    try:
        return jsonify(_with_cache("tradingview_manifest", _build_tradingview_manifest, ttl_sec=15, stale_sec=120))
    except Exception as exc:
        return jsonify({
            "ok": False,
            "error": str(exc),
            "source": "tradingview_manifest_v1",
        }), 500


@api_bp.route("/api/portfolio", methods=["GET"])
@require_secret
def api_portfolio():
    try:
        return jsonify(_with_cache("portfolio", _build_portfolio, ttl_sec=30, stale_sec=300))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/rebalance/preview", methods=["GET"])
@require_secret
def api_rebalance_preview():
    try:
        return jsonify(_with_cache("rebalance_preview", _build_rebalance_preview, ttl_sec=30, stale_sec=300))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/heatmap", methods=["GET"])
@require_secret
def api_heatmap():
    try:
        return jsonify(_with_cache("heatmap", _build_heatmap, ttl_sec=30, stale_sec=300))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/heatmap/meme", methods=["GET"])
@require_secret
def api_meme_heatmap():
    try:
        return jsonify(_with_cache("meme_heatmap", _build_meme_heatmap, ttl_sec=30, stale_sec=300))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/config", methods=["GET"])
@require_secret
def api_config():
    try:
        return jsonify(_with_cache("config", _build_config, ttl_sec=10, stale_sec=120))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/assets/config", methods=["GET"])
@require_secret
def api_assets_config():
    try:
        return jsonify(_with_cache("assets_config", _build_asset_config_rows, ttl_sec=10, stale_sec=120))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "items": []}), 500


@api_bp.route("/api/decision_trace", methods=["GET"])
@require_secret
def api_decision_trace():
    try:
        limit = max(1, min(100, _safe_int(request.args.get("limit"), 20)))
        product_id = str(request.args.get("product_id") or "").strip().upper()
        result_category = str(request.args.get("result_category") or "").strip().lower()
        items = list_decision_traces(limit=limit, product_id=product_id, result_category=result_category)
        return jsonify({
            "ok": True,
            "items": items,
            "count": len(items),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "items": []}), 500


@api_bp.route("/api/meme_rotation", methods=["GET"])
@require_secret
def api_meme_rotation():
    try:
        return jsonify(_with_cache("meme_rotation", _build_meme_rotation, ttl_sec=10, stale_sec=120))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/shadow_rotation_report", methods=["GET"])
@require_secret
def api_shadow_rotation_report():
    try:
        return jsonify(_with_cache("shadow_rotation_report", _build_shadow_rotation_report, ttl_sec=60, stale_sec=300))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/valid_products", methods=["GET"])
@require_secret
def api_valid_products():
    try:
        quote = (request.args.get("quote") or "USD").upper().strip()
        tradable_only = (request.args.get("tradable_only") or "true").lower() != "false"
        force_refresh = (request.args.get("refresh") or "false").lower() == "true"

        if force_refresh:
            clear_product_cache()

        products = get_valid_products(quote_currency=quote, tradable_only=tradable_only)
        return jsonify({
            "ok": True,
            "quote_currency": quote,
            "tradable_only": tradable_only,
            "count": len(products),
            "products": products,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "products": []}), 500


@api_bp.route("/api/valid_product_ids", methods=["GET"])
@require_secret
def api_valid_product_ids():
    try:
        quote = (request.args.get("quote") or "USD").upper().strip()
        tradable_only = (request.args.get("tradable_only") or "true").lower() != "false"
        products = get_valid_product_ids(quote_currency=quote, tradable_only=tradable_only)
        return jsonify({
            "ok": True,
            "quote_currency": quote,
            "tradable_only": tradable_only,
            "count": len(products),
            "products": products,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "products": []}), 500


@api_bp.route("/api/admin/state", methods=["GET"])
@require_secret
def api_admin_state():
    try:
        return jsonify({
            "ok": True,
            "admin": get_admin_state(),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/portfolio/summary", methods=["GET"])
@require_secret
def api_portfolio_summary_v2():
    try:
        return jsonify(_with_cache("portfolio_summary_v2", _build_portfolio_summary_v2, ttl_sec=20, stale_sec=120))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/portfolio/allocations", methods=["GET"])
@require_secret
def api_portfolio_allocations():
    try:
        return jsonify(_with_cache("portfolio_allocations", _build_portfolio_allocations, ttl_sec=20, stale_sec=120))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/portfolio/history", methods=["GET"])
@require_secret
def api_portfolio_history():
    try:
        return jsonify(_build_portfolio_history())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/config_proposals/latest", methods=["GET"])
@require_secret
def api_config_proposals_latest():
    try:
        return jsonify({
            "ok": True,
            "proposal": get_latest_config_proposal_any_status(proposal_type=None) or None,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "proposal": None}), 500


@api_bp.route("/api/config_proposals/recent", methods=["GET"])
@require_secret
def api_config_proposals_recent():
    try:
        limit = max(1, min(10, _safe_int(request.args.get("limit"), 5)))
        return jsonify({
            "ok": True,
            "items": list_recent_config_proposals(limit=limit, proposal_type=None),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "items": []}), 500


@api_bp.route("/api/config_proposals/generate", methods=["POST"])
@require_admin_auth
def api_config_proposals_generate():
    try:
        result = generate_review_proposals()
        return jsonify({
            "ok": bool(result.get("ok", False)),
            "status": result.get("status"),
            "proposal_id": result.get("proposal_id"),
            "expired_count": result.get("expired_count", 0),
            "superseded_count": result.get("superseded_count", 0),
            "notification_sent": bool(result.get("notification_sent", False)),
            "created_count": result.get("created_count", 0),
            "deduped_count": result.get("deduped_count", 0),
            "noop_count": result.get("noop_count", 0),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/config_proposals/auto_draft", methods=["POST"])
@require_admin_auth
def api_config_proposals_auto_draft():
    try:
        result = evaluate_auto_draft_review_proposals()
        return jsonify({
            "ok": bool(result.get("ok", False)),
            "status": result.get("status"),
            "proposal_id": result.get("proposal_id"),
            "expired_count": result.get("expired_count", 0),
            "superseded_count": result.get("superseded_count", 0),
            "notification_sent": bool(result.get("notification_sent", False)),
            "created_count": result.get("created_count", 0),
            "deduped_count": result.get("deduped_count", 0),
            "noop_count": result.get("noop_count", 0),
            "generation_mode": result.get("generation_mode"),
            "apply_mode": result.get("apply_mode"),
            "min_confidence": result.get("min_confidence"),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/config_proposals/<proposal_id>/approve", methods=["POST"])
@require_admin_auth
def api_config_proposals_approve(proposal_id):
    try:
        result = approve_config_proposal(proposal_id, actor=_proposal_actor())
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "proposal_id": str(proposal_id or "").strip()}), 500


@api_bp.route("/api/config_proposals/<proposal_id>/apply", methods=["POST"])
@require_admin_auth
def api_config_proposals_apply(proposal_id):
    try:
        result = apply_config_proposal(proposal_id, applied_by=_proposal_actor())
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "proposal_id": str(proposal_id or "").strip()}), 500


@api_bp.route("/api/config_proposals/<proposal_id>/reject", methods=["POST"])
@require_admin_auth
def api_config_proposals_reject(proposal_id):
    try:
        result = reject_config_proposal(proposal_id, actor=_proposal_actor())
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "proposal_id": str(proposal_id or "").strip()}), 500


@api_bp.route("/api/trades", methods=["GET"])
@require_secret
def api_trades():
    try:
        return jsonify(_build_trade_history())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/trades/stats", methods=["GET"])
@require_secret
def api_trades_stats():
    try:
        return jsonify(_build_trade_stats())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/performance", methods=["GET"])
@require_api_auth
def api_performance():
    try:
        days = request.args.get("days")
        days = int(days) if days else None

        summary = get_performance_summary(days=days)
        equity = get_equity_analytics(days=days or 30)
        daily = get_daily_pnl(days=days or 30)
        products = get_product_breakdown()
        recent = get_round_trips(limit=20)

        return jsonify({
            "ok": True,
            "summary": summary,
            "equity": equity,
            "daily_pnl": daily,
            "product_breakdown": products,
            "recent_round_trips": recent,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/backtest", methods=["POST"])
@require_admin_auth
def api_backtest():
    try:
        data = request.get_json(silent=True) or {}
        product_id = str(data.get("product_id") or "BTC-USD").upper().strip()
        timeframe = str(data.get("timeframe") or "4h").lower().strip()
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        params = data.get("params")

        bt = Backtester(
            product_id=product_id,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
        )
        candles = bt.load_candles()
        candles = bt.compute_indicators(candles)
        result = bt.run_strategy(candles, params=params)
        summary = bt.get_summary(result)

        return jsonify({
            "ok": True,
            "product_id": product_id,
            "timeframe": timeframe,
            "candle_count": len(candles),
            "trades": result.get("trades", []),
            "equity_curve": result.get("equity_curve", []),
            "summary": summary,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/backtest/quick", methods=["GET"])
@require_api_auth
def api_backtest_quick():
    try:
        product_id = (request.args.get("product_id") or "BTC-USD").upper().strip()
        timeframe = (request.args.get("timeframe") or "4h").lower().strip()
        days = int(request.args.get("days") or 90)
        params = request.args.get("params")
        if params:
            try:
                params = json.loads(params)
            except Exception:
                params = None

        end = datetime.utcnow()
        start = end - timedelta(days=days)

        bt = Backtester(
            product_id=product_id,
            timeframe=timeframe,
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )
        candles = bt.load_candles()
        candles = bt.compute_indicators(candles)
        result = bt.run_strategy(candles, params=params)
        summary = bt.get_summary(result)

        return jsonify({
            "ok": True,
            "product_id": product_id,
            "timeframe": timeframe,
            "days": days,
            "candle_count": len(candles),
            "summary": summary,
            "trade_count": len(result.get("trades", [])),
            "trades": result.get("trades", []),
            "equity_curve": result.get("equity_curve", []),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/backtest/portfolio", methods=["POST"])
@require_api_auth
def api_backtest_portfolio():
    try:
        data = request.get_json(silent=True) or {}
        bt = PortfolioBacktester(config=data)
        result = bt.run()

        full_equity_curve = list(result.get("equity_curve") or [])
        sampled_equity_curve = list(full_equity_curve)
        if len(sampled_equity_curve) > 500:
            step = max(1, len(sampled_equity_curve) // 500)
            sampled_equity_curve = sampled_equity_curve[::step]
            if sampled_equity_curve and full_equity_curve and sampled_equity_curve[-1] != full_equity_curve[-1]:
                sampled_equity_curve.append(full_equity_curve[-1])
            sampled_equity_curve = sampled_equity_curve[:500]

        return jsonify({
            "ok": True,
            "summary": result.get("summary", {}),
            "equity_curve": sampled_equity_curve,
            "trade_log": result.get("trade_log", []),
            "rebalance_log": result.get("rebalance_log", []),
            "trade_count": len(result.get("trade_log", [])),
            "rebalance_count": len(result.get("rebalance_log", [])),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/backtest/portfolio/quick", methods=["GET"])
@require_api_auth
def api_backtest_portfolio_quick():
    try:
        days = int(request.args.get("days") or 180)
        capital = float(request.args.get("capital") or 1000)

        end = datetime.utcnow()
        start = end - timedelta(days=days)

        bt = PortfolioBacktester(config={
            "starting_capital": capital,
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
        })
        result = bt.run()

        return jsonify({
            "ok": True,
            "summary": result.get("summary", {}),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/candles", methods=["GET"])
@require_api_auth
def api_candles():
    try:
        product_id = (request.args.get("product_id") or "BTC-USD").upper().strip()
        timeframe = (request.args.get("timeframe") or "1h").lower().strip()
        limit = int(request.args.get("limit") or 250)

        scanner = SignalScanner(timeframe=timeframe)
        candles = scanner.fetch_candles(product_id, limit=limit)
        candles = scanner.compute_indicators(candles)

        return jsonify({
            "ok": True,
            "product_id": product_id,
            "timeframe": timeframe,
            "candles": candles,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/signals/log", methods=["GET"])
@require_api_auth
def api_signals_log():
    try:
        limit = int(request.args.get("limit") or 100)
        return jsonify({
            "ok": True,
            "signals": get_signal_log(limit=limit),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/signals/state", methods=["GET"])
@require_api_auth
def api_signals_state():
    try:
        return jsonify({
            "ok": True,
            "states": get_scanner_state(),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/scanner/params", methods=["GET"])
@require_api_auth
def api_scanner_params_get():
    try:
        return jsonify({
            "ok": True,
            "params": get_scanner_params(),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/scanner/params", methods=["POST"])
@require_api_auth
def api_scanner_params_post():
    try:
        data = request.get_json(silent=True) or {}
        preset = str(data.get("preset") or "").strip()
        params = data.get("params") or {}
        updated = update_scanner_params(preset, params)
        return jsonify({
            "ok": True,
            "updated": updated,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/scanner/run", methods=["POST"])
@require_api_auth
def api_scanner_run():
    try:
        return jsonify(run_scanner_sweep())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/signals/chart", methods=["GET"])
@require_api_auth
def api_signals_chart():
    try:
        product_id = (request.args.get("product_id") or "BTC-USD").upper().strip()
        timeframe = (request.args.get("timeframe") or "1h").lower().strip()
        limit = int(request.args.get("limit") or 250)

        scanner = SignalScanner(timeframe=timeframe)
        candles = scanner.fetch_candles(product_id, limit=limit)
        candles = scanner.compute_indicators(candles)
        signals = [signal for signal in get_signal_log(limit=500) if str(signal.get("product_id") or "").upper().strip() == product_id]

        return jsonify({
            "ok": True,
            "candles": candles,
            "signals": signals,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/chain", methods=["GET"])
@require_api_auth
def api_options_chain():
    try:
        symbol = (request.args.get("symbol") or "").upper().strip()
        expiration = (request.args.get("expiration") or "").strip() or None
        if not symbol:
            return jsonify({"ok": False, "error": "missing_symbol"}), 400
        fetcher = OptionChainFetcher(broker="webull")
        result = fetcher.get_chain(symbol, expiration=expiration)
        return jsonify(result), (200 if result.get("ok") else 500)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/expirations", methods=["GET"])
@require_api_auth
def api_options_expirations():
    try:
        symbol = (request.args.get("symbol") or "").upper().strip()
        if not symbol:
            return jsonify({"ok": False, "error": "missing_symbol"}), 400
        fetcher = OptionChainFetcher(broker="webull")
        return jsonify({
            "ok": True,
            "symbol": symbol,
            "expirations": fetcher.get_expirations(symbol),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/iv-rank", methods=["GET"])
@require_api_auth
def api_options_iv_rank():
    try:
        symbol = (request.args.get("symbol") or "").upper().strip()
        if not symbol:
            return jsonify({"ok": False, "error": "missing_symbol"}), 400
        fetcher = OptionChainFetcher(broker="webull")
        chain = fetcher.get_chain(symbol)
        current_iv_values = []
        if chain.get("ok"):
            for expiry_data in (chain.get("chains") or {}).values():
                for side in ("calls", "puts"):
                    for row in (expiry_data.get(side) or []):
                        iv = _safe_float(row.get("iv"), 0.0)
                        if iv > 0:
                            current_iv_values.append(iv)
        current_iv = (sum(current_iv_values) / len(current_iv_values)) if current_iv_values else 0.0
        return jsonify({
            "ok": True,
            "symbol": symbol,
            "iv_rank": fetcher.get_iv_rank(symbol),
            "current_iv": round(current_iv, 4),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/earnings", methods=["GET"])
@require_api_auth
def api_options_earnings():
    try:
        days_ahead = max(1, int(request.args.get("days_ahead") or 14))
        calendar = EarningsCalendar()
        return jsonify(
            {
                "ok": True,
                "days_ahead": days_ahead,
                "earnings": calendar.get_upcoming_earnings(days_ahead=days_ahead),
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/earnings/history", methods=["GET"])
@require_api_auth
def api_options_earnings_history():
    try:
        symbol = (request.args.get("symbol") or "").upper().strip()
        if not symbol:
            return jsonify({"ok": False, "error": "missing_symbol"}), 400
        lookback_quarters = max(1, int(request.args.get("lookback_quarters") or 8))
        calendar = EarningsCalendar()
        history = calendar.get_earnings_dates(symbol, lookback_quarters=lookback_quarters)
        return jsonify(
            {
                "ok": True,
                "symbol": symbol,
                "lookback_quarters": lookback_quarters,
                "historical_move_avg": round(calendar.get_historical_earnings_move(symbol), 4),
                "earnings_history": history,
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/scan", methods=["POST"])
@require_api_auth
def api_options_scan():
    try:
        data = request.get_json(silent=True) or {}
        symbols = data.get("symbols")
        strategies = data.get("strategies")
        min_score = _safe_float(data.get("min_score"), 0.0)
        max_results = max(1, int(data.get("max_results") or 20))
        screener = OptionsScreener(watchlist=symbols, broker="webull")
        result = screener.scan_universe(strategies=strategies)
        opportunities = [
            opp for opp in (result.get("opportunities") or [])
            if _safe_float(opp.get("score"), 0.0) >= min_score
        ][:max_results]
        result["opportunities"] = opportunities
        result["summary"] = {
            **(result.get("summary") or {}),
            "opportunities_found": len(opportunities),
            "best_opportunity": opportunities[0] if opportunities else None,
        }
        return jsonify({"ok": True, **result})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/scan/quick", methods=["GET"])
@require_api_auth
def api_options_scan_quick():
    try:
        screener = OptionsScreener(watchlist=OptionsScreener().watchlist[:10], broker="webull")
        result = screener.scan_universe()
        opportunities = sorted(result.get("opportunities") or [], key=lambda row: _safe_float(row.get("score"), 0.0), reverse=True)[:10]
        return jsonify({
            "ok": True,
            "scan_time": result.get("scan_time"),
            "opportunities": opportunities,
            "summary": {
                **(result.get("summary") or {}),
                "opportunities_found": len(opportunities),
                "best_opportunity": opportunities[0] if opportunities else None,
            },
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/recommend", methods=["POST"])
@require_api_auth
def api_options_recommend():
    try:
        data = request.get_json(silent=True) or {}
        capital = _safe_float(data.get("capital"), 0.0)
        risk_tolerance = str(data.get("risk_tolerance") or "moderate").strip().lower()
        screener = OptionsScreener(broker="webull")
        recommendations = screener.get_recommendation(capital_available=capital, risk_tolerance=risk_tolerance)
        return jsonify({
            "ok": True,
            "capital": capital,
            "risk_tolerance": risk_tolerance,
            "recommendations": recommendations,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/positions", methods=["GET"])
@require_api_auth
def api_options_positions():
    try:
        adapter = WebullAdapter()
        positions = [
            row for row in (adapter.get_positions() or [])
            if str(row.get("asset_type") or "").lower() == "option"
        ]
        return jsonify({
            "ok": True,
            "positions": positions,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/trade", methods=["POST"])
@require_api_auth
def api_options_trade():
    try:
        data = request.get_json(silent=True) or {}
        strategy = str(data.get("strategy") or "").strip().lower()
        adapter = WebullAdapter()

        single_leg_map = {
            "covered_call": {"option_type": "call", "side": "sell"},
            "cash_secured_put": {"option_type": "put", "side": "sell"},
            "long_call": {"option_type": "call", "side": "buy"},
            "long_put": {"option_type": "put", "side": "buy"},
        }

        if strategy in single_leg_map:
            mapped = single_leg_map[strategy]
            order = {
                "underlying": str(data.get("symbol") or "").upper().strip(),
                "expiration": str(data.get("expiration") or "").strip(),
                "strike": _safe_float(data.get("strike"), 0.0),
                "option_type": mapped["option_type"],
                "side": mapped["side"],
                "qty": int(data.get("qty") or 1),
                "order_type": str(data.get("order_type") or "MKT").upper().strip(),
                "limit_price": data.get("limit_price"),
            }
            result = adapter.place_options_order(order)
            return jsonify(result), (200 if result.get("ok") else 500)

        explicit_order = {
            "underlying": str(data.get("symbol") or data.get("underlying") or "").upper().strip(),
            "expiration": str(data.get("expiration") or "").strip(),
            "strike": _safe_float(data.get("strike"), 0.0),
            "option_type": str(data.get("option_type") or "").strip().lower(),
            "side": str(data.get("side") or "").strip().lower(),
            "qty": int(data.get("qty") or 1),
            "order_type": str(data.get("order_type") or "MKT").upper().strip(),
            "limit_price": data.get("limit_price"),
        }
        if not explicit_order["underlying"] or explicit_order["option_type"] not in {"call", "put"} or explicit_order["side"] not in {"buy", "sell"}:
            return jsonify({"ok": False, "error": "unsupported_or_invalid_strategy"}), 400

        result = adapter.place_options_order(explicit_order)
        return jsonify(result), (200 if result.get("ok") else 500)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/bars", methods=["GET"])
@require_api_auth
def api_options_bars():
    try:
        symbol = str(request.args.get("symbol") or "AAPL").upper().strip()
        timeframe = str(request.args.get("timeframe") or "D").upper().strip()
        days = max(30, int(request.args.get("days") or 365))

        if timeframe != "D":
            return jsonify({"ok": False, "error": "unsupported_timeframe"}), 400

        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=days)
        fetcher = OptionsBacktester(
            config={
                "symbol": symbol,
                "start_date": start_dt.strftime("%Y-%m-%d"),
                "end_date": end_dt.strftime("%Y-%m-%d"),
            }
        )
        bars = fetcher.fetch_underlying_bars(
            symbol,
            start_dt.strftime("%Y-%m-%d"),
            end_dt.strftime("%Y-%m-%d"),
        )

        enriched_bars = []
        for row in bars:
            date_value = str(row.get("date") or "")
            try:
                ts = int(datetime.fromisoformat(date_value).replace(tzinfo=timezone.utc).timestamp())
            except Exception:
                ts = 0
            enriched_bars.append(
                {
                    "ts": ts,
                    "date": date_value,
                    "open": _safe_float(row.get("open"), 0.0),
                    "high": _safe_float(row.get("high"), 0.0),
                    "low": _safe_float(row.get("low"), 0.0),
                    "close": _safe_float(row.get("close"), 0.0),
                    "volume": int(_safe_float(row.get("volume"), 0.0)),
                }
            )

        closes = [_safe_float(row.get("close"), 0.0) for row in enriched_bars]
        ema200 = bt_ema(closes, 200)
        bb_upper, bb_middle, bb_lower = bollinger_bands(closes, 20, 2.0)
        rsi14 = bt_rsi(closes, 14)

        iv_estimate = [None] * len(enriched_bars)
        iv_rank = [None] * len(enriched_bars)
        for idx in range(len(enriched_bars)):
            if idx < 30:
                continue
            sample = closes[max(0, idx - 29): idx + 1]
            returns = []
            for j in range(1, len(sample)):
                prev = _safe_float(sample[j - 1], 0.0)
                curr = _safe_float(sample[j], 0.0)
                if prev > 0 and curr > 0:
                    returns.append(math.log(curr / prev))
            if len(returns) < 2:
                continue
            mean_ret = sum(returns) / len(returns)
            variance = sum((value - mean_ret) ** 2 for value in returns) / max(1, len(returns) - 1)
            realized_vol = math.sqrt(variance) * math.sqrt(252.0)
            iv_now = max(0.01, realized_vol * 1.2)
            iv_estimate[idx] = iv_now

            lookback = [value for value in iv_estimate[max(0, idx - 251): idx + 1] if value is not None]
            if len(lookback) >= 2:
                iv_low = min(lookback)
                iv_high = max(lookback)
                if iv_high > iv_low:
                    iv_rank[idx] = ((iv_now - iv_low) / (iv_high - iv_low)) * 100.0
                else:
                    iv_rank[idx] = 50.0

        return jsonify(
            {
                "ok": True,
                "symbol": symbol,
                "bars": enriched_bars,
                "indicators": {
                    "ema200": ema200,
                    "bb_upper": bb_upper,
                    "bb_lower": bb_lower,
                    "bb_middle": bb_middle,
                    "rsi": rsi14,
                    "iv_estimate": iv_estimate,
                    "iv_rank": iv_rank,
                },
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/backtest", methods=["POST"])
@require_api_auth
def api_options_backtest():
    try:
        data = request.get_json(silent=True) or {}
        symbol = str(data.get("symbol") or "AAPL").upper().strip()
        strategy = str(data.get("strategy") or "wheel").strip().lower()
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        starting_capital = _safe_float(data.get("starting_capital"), 5000.0)
        params = data.get("params") or {}

        bt = OptionsBacktester(
            config={
                "symbol": symbol,
                "strategy": strategy,
                "start_date": start_date,
                "end_date": end_date,
                "starting_capital": starting_capital,
                "params": params,
            }
        )
        result = bt.run()

        return jsonify(
            {
                "ok": True,
                "symbol": symbol,
                "strategy": strategy,
                "summary": result.get("summary", {}),
                "equity_curve": result.get("equity_curve", []),
                "trade_log": result.get("trade_log", []),
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/options/backtest/quick", methods=["GET"])
@require_api_auth
def api_options_backtest_quick():
    try:
        symbol = str(request.args.get("symbol") or "AAPL").upper().strip()
        strategy = str(request.args.get("strategy") or "wheel").strip().lower()
        days = int(request.args.get("days") or 365)

        end = datetime.utcnow()
        start = end - timedelta(days=days)

        bt = OptionsBacktester(
            config={
                "symbol": symbol,
                "strategy": strategy,
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": end.strftime("%Y-%m-%d"),
            }
        )
        result = bt.run()

        return jsonify(
            {
                "ok": True,
                "symbol": symbol,
                "strategy": strategy,
                "days": days,
                "summary": result.get("summary", {}),
                "trade_count": len(result.get("trade_log", [])),
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


# -------------------------------------------------------------------
# Existing admin endpoints
# -------------------------------------------------------------------

@api_bp.route("/api/admin/cache/refresh", methods=["POST"])
@require_admin_auth
def api_admin_cache_refresh():
    try:
        clear_product_cache()
        snapshot = force_refresh_portfolio_snapshot()
        with _API_CACHE_LOCK:
            _API_CACHE.clear()
        return jsonify({
            "ok": True,
            "message": "Caches refreshed",
            "snapshot_timestamp": snapshot.get("timestamp"),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/admin/meme_rotation", methods=["POST"])
@require_admin_auth
def api_admin_meme_rotation_toggle():
    try:
        body = request.get_json(silent=True) or {}
        enabled = body.get("enabled")
        if enabled is None:
            enabled = str(request.args.get("enabled", "")).lower() in {"1", "true", "yes", "on"}
        state = set_meme_rotation_enabled(bool(enabled))
        with _API_CACHE_LOCK:
            _API_CACHE.pop("config", None)
        return jsonify({
            "ok": True,
            "meme_rotation_enabled": state,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/admin/satellite/allow", methods=["POST"])
@require_admin_auth
def api_admin_satellite_allow():
    try:
        body = request.get_json(silent=True) or {}
        product_id = _normalize_product_id(body.get("product_id") or request.args.get("product_id"))
        action = str(body.get("action") or request.args.get("action") or "add").lower().strip()

        if not product_id:
            return jsonify({"ok": False, "error": "missing product_id"}), 400

        if action == "remove":
            values = remove_allowed_satellite_candidate(product_id)
        else:
            values = allow_satellite_candidate(product_id)

        with _API_CACHE_LOCK:
            _API_CACHE.pop("config", None)
            _API_CACHE.pop("meme_heatmap", None)
            _API_CACHE.pop("meme_rotation", None)

        return jsonify({
            "ok": True,
            "allowed_candidates": values,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/admin/satellite/block", methods=["POST"])
@require_admin_auth
def api_admin_satellite_block():
    try:
        body = request.get_json(silent=True) or {}
        product_id = _normalize_product_id(body.get("product_id") or request.args.get("product_id"))
        action = str(body.get("action") or request.args.get("action") or "add").lower().strip()

        if not product_id:
            return jsonify({"ok": False, "error": "missing product_id"}), 400

        if action == "remove":
            values = remove_blocked_satellite_candidate(product_id)
        else:
            values = block_satellite_candidate(product_id)

        with _API_CACHE_LOCK:
            _API_CACHE.pop("config", None)
            _API_CACHE.pop("meme_heatmap", None)
            _API_CACHE.pop("meme_rotation", None)

        return jsonify({
            "ok": True,
            "blocked_candidates": values,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


# -------------------------------------------------------------------
# Richer admin endpoint
# -------------------------------------------------------------------

@api_bp.route("/api/admin/asset", methods=["POST"])
@require_admin_auth
def api_admin_asset():
    try:
        payload = request.get_json(silent=True) or {}
        if not payload:
            payload = request.form.to_dict()

        action = str(payload.get("action", "")).strip().lower()
        product_id = _normalize_product_id(payload.get("product_id"))
        target_weight = float(payload.get("target_weight", 0.0) or 0.0)
        rebalance_band = float(
            payload.get("rebalance_band", payload.get("band", 0.0)) or 0.0
        )

        config = load_asset_config() or {}
        changed = False

        if action == "add_core" and product_id:
            config.setdefault("core_assets", {})[product_id] = {
                "target_weight": target_weight or 0.05,
                "rebalance_band": rebalance_band or 0.02,
            }
            if product_id in config.get("satellite_allowed", []):
                config["satellite_allowed"].remove(product_id)
            if product_id in config.get("satellite_blocked", []):
                config["satellite_blocked"].remove(product_id)
            changed = True

        elif action == "update_core" and product_id in config.get("core_assets", {}):
            if target_weight > 0:
                config["core_assets"][product_id]["target_weight"] = target_weight
            if rebalance_band > 0:
                config["core_assets"][product_id]["rebalance_band"] = rebalance_band
            changed = True

        elif action == "remove_core" and product_id in config.get("core_assets", {}):
            del config["core_assets"][product_id]
            changed = True

        elif action in {"add_satellite", "enable_satellite"} and product_id:
            arr = config.setdefault("satellite_allowed", [])
            if product_id not in arr:
                arr.append(product_id)
            if product_id in config.get("satellite_blocked", []):
                config["satellite_blocked"].remove(product_id)
            changed = True

        elif action in {"remove_satellite", "disable_satellite"} and product_id:
            arr = config.setdefault("satellite_allowed", [])
            if product_id in arr:
                arr.remove(product_id)
            changed = True

        elif action == "block" and product_id:
            arr = config.setdefault("satellite_blocked", [])
            if product_id not in arr:
                arr.append(product_id)
            if product_id in config.get("satellite_allowed", []):
                config["satellite_allowed"].remove(product_id)
            if product_id in config.get("core_assets", {}):
                del config["core_assets"][product_id]
            changed = True

        elif action == "unblock" and product_id:
            arr = config.setdefault("satellite_blocked", [])
            if product_id in arr:
                arr.remove(product_id)
            changed = True

        elif action == "set_mode":
            mode = str(payload.get("satellite_mode", "rotation")).lower().strip()
            config["satellite_mode"] = mode
            changed = True

        elif action == "set_risk":
            errors = []
            allowed_sniper_regimes = {"bull", "neutral", "risk_off"}

            def _optional_float(name, low=None, high=None):
                raw = payload.get(name)
                if raw in (None, ""):
                    return None
                try:
                    value = float(raw)
                except Exception:
                    errors.append(f"{name} must be numeric")
                    return None
                if low is not None and value < low:
                    errors.append(f"{name} must be >= {low}")
                if high is not None and value > high:
                    errors.append(f"{name} must be <= {high}")
                return value

            def _optional_int(name, low=None, high=None):
                raw = payload.get(name)
                if raw in (None, ""):
                    return None
                try:
                    value = int(float(raw))
                except Exception:
                    errors.append(f"{name} must be an integer")
                    return None
                if low is not None and value < low:
                    errors.append(f"{name} must be >= {low}")
                if high is not None and value > high:
                    errors.append(f"{name} must be <= {high}")
                return value

            def _optional_bool(name):
                raw = payload.get(name)
                if raw in (None, ""):
                    return None
                if isinstance(raw, bool):
                    return raw
                normalized = str(raw).strip().lower()
                if normalized in {"1", "true", "yes", "on"}:
                    return True
                if normalized in {"0", "false", "no", "off"}:
                    return False
                errors.append(f"{name} must be true/false")
                return None

            def _optional_regime_list(name):
                raw = payload.get(name)
                if raw in (None, ""):
                    return None
                if isinstance(raw, list):
                    values = raw
                else:
                    values = str(raw).split(",")
                normalized = []
                for value in values:
                    item = str(value or "").strip().lower()
                    if not item:
                        continue
                    if item not in allowed_sniper_regimes:
                        errors.append(f"{name} contains invalid value: {item}")
                        continue
                    if item not in normalized:
                        normalized.append(item)
                if not normalized and raw not in (None, ""):
                    errors.append(f"{name} must include at least one valid regime")
                return normalized

            satellite_total_max = _optional_float("satellite_total_max", 0.0, 1.0)
            satellite_total_target = _optional_float("satellite_total_target", 0.0, 1.0)
            min_cash_reserve = _optional_float("min_cash_reserve", 0.0, 1.0)
            max_quote_per_trade_usd = _optional_float("max_quote_per_trade_usd", 0.0, 1000000.0)
            trade_min_value_usd = _optional_float("trade_min_value_usd", 0.0, 1000000.0)
            core_buy_fraction_of_shortfall = _optional_float("core_buy_fraction_of_shortfall", 0.0, 1.0)
            max_active_satellites = _optional_int("max_active_satellites", 0, 100)
            max_new_satellites_per_cycle = _optional_int("max_new_satellites_per_cycle", 0, 100)
            rotation_cooldown_minutes = _optional_int("rotation_cooldown_minutes", 0, 10080)
            min_meme_score = _optional_float("min_meme_score", 0.0, 100.0)
            drawdown_warn_level = _optional_float("drawdown_warn_level", 0.0, 1.0)
            drawdown_reduce_level = _optional_float("drawdown_reduce_level", 0.0, 1.0)
            drawdown_freeze_level = _optional_float("drawdown_freeze_level", 0.0, 1.0)
            min_harvest_usd = _optional_float("min_harvest_usd", 0.0, 1000000.0)
            sniper_enabled = _optional_bool("sniper_enabled")
            sniper_buy_scale = _optional_float("sniper_buy_scale", 0.0, 1.0)
            sniper_min_score = _optional_float("sniper_min_score", 0.0, 100.0)
            sniper_block_pump_protected = _optional_bool("sniper_block_pump_protected")
            sniper_require_sniper_eligible = _optional_bool("sniper_require_sniper_eligible")
            sniper_relax_require_sniper_eligible = _optional_bool("sniper_relax_require_sniper_eligible")
            sniper_allowed_regimes = _optional_regime_list("sniper_allowed_regimes")

            next_drawdown = dict(config.get("drawdown_controls") or {})
            if drawdown_warn_level is not None:
                next_drawdown["warn_level"] = drawdown_warn_level
            if drawdown_reduce_level is not None:
                next_drawdown["reduce_level"] = drawdown_reduce_level
            if drawdown_freeze_level is not None:
                next_drawdown["freeze_level"] = drawdown_freeze_level
            warn_level = _safe_float(next_drawdown.get("warn_level"), 0.10)
            reduce_level = _safe_float(next_drawdown.get("reduce_level"), 0.15)
            freeze_level = _safe_float(next_drawdown.get("freeze_level"), 0.20)
            if not (warn_level < reduce_level < freeze_level):
                errors.append("drawdown levels must satisfy warn < reduce < freeze")
            effective_satellite_total_max = satellite_total_max if satellite_total_max is not None else _safe_float(config.get("satellite_total_max"), 0.50)
            effective_satellite_total_target = satellite_total_target if satellite_total_target is not None else _safe_float(config.get("satellite_total_target"), 0.50)
            if effective_satellite_total_target > effective_satellite_total_max:
                errors.append("satellite_total_target must be <= satellite_total_max")

            if errors:
                return jsonify({"ok": False, "error": "validation_failed", "errors": errors}), 400

            if payload.get("satellite_total_max") not in (None, ""):
                config["satellite_total_max"] = satellite_total_max

            if payload.get("satellite_total_target") not in (None, ""):
                config["satellite_total_target"] = satellite_total_target

            if payload.get("min_cash_reserve") not in (None, ""):
                config["min_cash_reserve"] = min_cash_reserve

            if payload.get("trade_min_value_usd") not in (None, ""):
                config["trade_min_value_usd"] = trade_min_value_usd

            if payload.get("max_quote_per_trade_usd") not in (None, ""):
                config["max_quote_per_trade_usd"] = max_quote_per_trade_usd

            if payload.get("core_buy_fraction_of_shortfall") not in (None, ""):
                config["core_buy_fraction_of_shortfall"] = core_buy_fraction_of_shortfall

            if payload.get("max_active_satellites") not in (None, ""):
                config["max_active_satellites"] = max_active_satellites
                config.setdefault("meme_rotation", {})
                config["meme_rotation"]["max_active"] = max_active_satellites

            if payload.get("max_new_satellites_per_cycle") not in (None, ""):
                config["max_new_satellites_per_cycle"] = max_new_satellites_per_cycle

            if payload.get("rotation_cooldown_minutes") not in (None, ""):
                config["rotation_cooldown_minutes"] = rotation_cooldown_minutes

            if payload.get("min_meme_score") not in (None, ""):
                config["min_meme_score"] = min_meme_score
                config.setdefault("meme_rotation", {})
                config["meme_rotation"]["min_score"] = min_meme_score

            if any(value is not None for value in [drawdown_warn_level, drawdown_reduce_level, drawdown_freeze_level]):
                config.setdefault("drawdown_controls", {})
                if drawdown_warn_level is not None:
                    config["drawdown_controls"]["warn_level"] = drawdown_warn_level
                if drawdown_reduce_level is not None:
                    config["drawdown_controls"]["reduce_level"] = drawdown_reduce_level
                if drawdown_freeze_level is not None:
                    config["drawdown_controls"]["freeze_level"] = drawdown_freeze_level

            if payload.get("min_harvest_usd") not in (None, ""):
                config.setdefault("profit_harvest", {})
                config["profit_harvest"]["min_harvest_usd"] = min_harvest_usd

            if payload.get("sniper_buy_scale") not in (None, ""):
                config.setdefault("sniper_mode", {})
                config["sniper_mode"]["buy_scale"] = sniper_buy_scale

            if payload.get("sniper_min_score") not in (None, ""):
                config.setdefault("sniper_mode", {})
                config["sniper_mode"]["min_score"] = sniper_min_score

            if payload.get("sniper_enabled") not in (None, ""):
                config.setdefault("sniper_mode", {})
                config["sniper_mode"]["enabled"] = bool(sniper_enabled)

            if payload.get("sniper_block_pump_protected") not in (None, ""):
                config.setdefault("sniper_mode", {})
                config["sniper_mode"]["block_pump_protected"] = bool(sniper_block_pump_protected)

            if payload.get("sniper_require_sniper_eligible") not in (None, ""):
                config.setdefault("sniper_mode", {})
                config["sniper_mode"]["require_sniper_eligible"] = bool(sniper_require_sniper_eligible)

            if payload.get("sniper_relax_require_sniper_eligible") not in (None, ""):
                config.setdefault("sniper_mode", {})
                config["sniper_mode"]["relax_require_sniper_eligible"] = bool(sniper_relax_require_sniper_eligible)

            if payload.get("sniper_allowed_regimes") not in (None, ""):
                config.setdefault("sniper_mode", {})
                config["sniper_mode"]["allow_in_regimes"] = list(sniper_allowed_regimes or [])

            generation_mode = _normalized_choice(
                payload.get("config_proposal_generation_mode"),
                {"manual", "auto"},
            )
            if generation_mode:
                config["config_proposal_generation_mode"] = generation_mode

            apply_mode = _normalized_choice(
                payload.get("config_proposal_apply_mode"),
                {"manual", "after_approval"},
            )
            if apply_mode:
                config["config_proposal_apply_mode"] = apply_mode

            min_confidence = _normalized_choice(
                payload.get("config_proposal_min_confidence"),
                {"medium", "high"},
            )
            if min_confidence:
                config["config_proposal_min_confidence"] = min_confidence

            changed = True

        if not changed:
            return jsonify({"ok": False, "error": "no valid change requested"}), 400

        save_asset_config(config)

        with _API_CACHE_LOCK:
            _API_CACHE.pop("config", None)
            _API_CACHE.pop("portfolio", None)
            _API_CACHE.pop("rebalance_preview", None)
            _API_CACHE.pop("heatmap", None)
            _API_CACHE.pop("meme_heatmap", None)
            _API_CACHE.pop("meme_rotation", None)
            _API_CACHE.pop("portfolio_summary_v2", None)
            _API_CACHE.pop("portfolio_allocations", None)

        return jsonify({
            "ok": True,
            "config": config,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/assets/config/update", methods=["POST"])
@require_admin_auth
def api_assets_config_update():
    try:
        payload = request.get_json(silent=True) or {}
        if not payload:
            payload = request.form.to_dict()

        product_id = _normalize_product_id(payload.get("product_id"))
        state = str(payload.get("state") or "").strip().lower()
        target_weight_raw = payload.get("target_weight")
        rebalance_band_raw = payload.get("rebalance_band")
        if not product_id:
            return jsonify({"ok": False, "error": "missing_product_id"}), 400
        if state not in {"core", "enable", "auto", "disable"}:
            return jsonify({"ok": False, "error": "invalid_state"}), 400

        config = load_asset_config() or {}
        valid_products = {
            _normalize_product_id(x)
            for x in (get_valid_product_ids(quote_currency="USD", tradable_only=True) or [])
            if _normalize_product_id(x)
        }
        known_products = {
            _normalize_product_id(x)
            for x in list((config.get("core_assets") or {}).keys())
            + list(config.get("satellite_allowed") or [])
            + list(config.get("satellite_blocked") or [])
            + list(valid_products)
            if _normalize_product_id(x)
        }
        if product_id not in known_products:
            return jsonify({"ok": False, "error": "invalid_product_id"}), 400
        if state in {"core", "enable"} and product_id not in valid_products:
            return jsonify({"ok": False, "error": "invalid_nontradable_product_for_state"}), 400

        target_weight = None
        rebalance_band = None
        if target_weight_raw not in (None, ""):
            try:
                target_weight = float(target_weight_raw)
            except Exception:
                return jsonify({"ok": False, "error": "invalid_target_weight"}), 400
            if target_weight <= 0 or target_weight > 1:
                return jsonify({"ok": False, "error": "target_weight_out_of_range"}), 400
        if rebalance_band_raw not in (None, ""):
            try:
                rebalance_band = float(rebalance_band_raw)
            except Exception:
                return jsonify({"ok": False, "error": "invalid_rebalance_band"}), 400
            if rebalance_band <= 0 or rebalance_band > 1:
                return jsonify({"ok": False, "error": "rebalance_band_out_of_range"}), 400

        old_state = _asset_mode_from_config(product_id, config)
        allowed = config.setdefault("satellite_allowed", [])
        blocked = config.setdefault("satellite_blocked", [])
        core_assets = config.setdefault("core_assets", {})

        if state == "core":
            if product_id in allowed:
                allowed.remove(product_id)
            if product_id in blocked:
                blocked.remove(product_id)
            current_core = core_assets.get(product_id) or {}
            core_assets[product_id] = {
                "target_weight": target_weight if target_weight is not None else float(current_core.get("target_weight", 0.05) or 0.05),
                "rebalance_band": rebalance_band if rebalance_band is not None else float(current_core.get("rebalance_band", 0.02) or 0.02),
            }
        else:
            if product_id in core_assets:
                del core_assets[product_id]

        if state == "enable":
            if product_id not in allowed:
                allowed.append(product_id)
            if product_id in blocked:
                blocked.remove(product_id)
        elif state == "disable":
            if product_id not in blocked:
                blocked.append(product_id)
            if product_id in allowed:
                allowed.remove(product_id)
        else:
            if product_id in allowed:
                allowed.remove(product_id)
            if product_id in blocked:
                blocked.remove(product_id)

        save_asset_config(config)
        _API_CACHE.clear()
        print(json.dumps({
            "component": "api",
            "event": "asset_state_updated",
            "payload": {
                "product_id": product_id,
                "old_state": old_state,
                "new_state": state,
                "target_weight": target_weight,
                "rebalance_band": rebalance_band,
                "storage": "asset_config.json",
            },
        }, sort_keys=True, ensure_ascii=False))

        rows = _build_asset_config_rows()
        item = next((row for row in (rows.get("items") or []) if row.get("product_id") == product_id), None)
        return jsonify({
            "ok": True,
            "product_id": product_id,
            "old_state": old_state,
            "state": state,
            "item": item,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_bp.route("/api/admin/meme_rotation/candidate", methods=["POST"])
@require_admin_auth
def api_admin_meme_rotation_candidate():
    try:
        payload = request.get_json(silent=True) or {}
        if not payload:
            payload = request.form.to_dict()

        product_id = _normalize_product_id(payload.get("product_id"))
        if not product_id:
            return jsonify({"ok": False, "error": "missing product_id"}), 400

        remove_flag = str(payload.get("remove", "false")).lower() == "true"
        enabled = str(payload.get("enabled", "true")).lower() == "true"
        score = float(payload.get("score", 0.0) or 0.0)
        source = str(payload.get("source", "manual")).strip() or "manual"

        rotation = load_meme_rotation() or {"candidates": []}
        candidates = rotation.setdefault("candidates", [])
        candidates = [
            c for c in candidates
            if _normalize_product_id(c.get("product_id")) != product_id
        ]

        if not remove_flag:
            candidates.append({
                "product_id": product_id,
                "score": score,
                "enabled": enabled,
                "source": source,
                "updated_at": int(_now()),
            })

        rotation["candidates"] = sorted(
            candidates,
            key=lambda x: float(x.get("score", 0.0) or 0.0),
            reverse=True,
        )
        rotation["updated_at"] = int(_now())
        save_meme_rotation(rotation)

        with _API_CACHE_LOCK:
            _API_CACHE.pop("meme_rotation", None)
            _API_CACHE.pop("meme_heatmap", None)

        return jsonify({
            "ok": True,
            "meme_rotation": rotation,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
