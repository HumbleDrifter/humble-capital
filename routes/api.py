import json
import os
import sqlite3
import threading
import time
from functools import wraps

from flask import Blueprint, jsonify, request, session

from env_runtime import load_runtime_env, preferred_env_path

load_runtime_env(override=True)

from execution import (
    clear_product_cache,
    get_valid_product_ids,
    get_valid_products,
)
from portfolio import (
    build_adaptive_suggestions,
    build_auto_adaptive_recommendation,
    build_portfolio_history_analytics,
    build_portfolio_risk_score,
    get_portfolio_snapshot,
    normalize_adaptive_suggestions_payload,
    normalize_auto_adaptive_payload,
    normalize_risk_score_payload,
    persist_current_portfolio_snapshot,
    portfolio_summary,
)
from rebalancer import get_profit_harvest_plan, get_rebalance_plan
from storage import get_portfolio_history_since

api_bp = Blueprint("api", __name__)

BASE_DIR = str(preferred_env_path().parent.resolve())
ASSET_CONFIG_PATH = os.path.join(BASE_DIR, "asset_config.json")
MEME_ROTATION_PATH = os.path.join(BASE_DIR, "meme_rotation.json")
TRADING_DB_PATH = os.getenv("TRADINGBOT_DB_PATH", os.path.join(BASE_DIR, "trading.db"))

_API_CACHE_LOCK = threading.Lock()
_API_CACHE = {}


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
    if state.get("ok"):
        persist_current_portfolio_snapshot(
            {
                "timestamp": state.get("timestamp") or int(_now()),
                "total_value_usd": state.get("total_value_usd", 0.0),
                "usd_cash": state.get("usd_cash", 0.0),
            }
        )
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
    return {
        "ok": True,
        "config": cfg,
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

    for item in rotation.get("candidates", []):
        product_id = _normalize_product_id(item.get("product_id"))
        if not product_id:
            continue

        asset = asset_rows.get(product_id, {})

        candidates.append({
            "product_id": product_id,
            "score": float(item.get("score", 0.0) or 0.0),
            "enabled": bool(item.get("enabled", True)),
            "source": item.get("source", "manual"),
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "trend_score": item.get("trend_score"),
            "momentum_bonus": item.get("momentum_bonus"),
            "change_1h": item.get("price_change_1h_pct", item.get("change_1h")),
            "change_24h": item.get("price_change_24h_pct", item.get("change_24h")),
            "change_7d": item.get("price_change_7d_pct", item.get("change_7d")),
            "held_value_usd": float(asset.get("value_total_usd", 0.0) or 0.0),
            "portfolio_weight": float(asset.get("weight_total", 0.0) or 0.0),
            "class": asset.get("class"),
            "held": product_id in held_assets,
            "allowed": product_id in allowed,
            "blocked": product_id in blocked,
            "core": product_id in core_assets,
            "active_buy_universe": product_id in active_buy_universe,
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
        })

    candidates.sort(
        key=lambda x: (
            float(x.get("score", 0.0) or 0.0),
            float(x.get("held_value_usd", 0.0) or 0.0),
        ),
        reverse=True,
    )

    return {
        "ok": True,
        "meme_rotation": rotation,
        "count": len(candidates),
        "market_regime": summary.get("market_regime"),
        "active_satellite_buy_universe": sorted(active_buy_universe),
        "candidates": candidates,
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

    try:
        history_rows = get_portfolio_history_since(start_ts=start_ts)
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


@api_bp.route("/api/meme_rotation", methods=["GET"])
@require_secret
def api_meme_rotation():
    try:
        return jsonify(_with_cache("meme_rotation", _build_meme_rotation, ttl_sec=10, stale_sec=120))
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
            if payload.get("satellite_total_max") not in (None, ""):
                config["satellite_total_max"] = float(payload.get("satellite_total_max"))

            if payload.get("satellite_total_target") not in (None, ""):
                config["satellite_total_target"] = float(payload.get("satellite_total_target"))

            if payload.get("min_cash_reserve") not in (None, ""):
                config["min_cash_reserve"] = float(payload.get("min_cash_reserve"))

            if payload.get("trade_min_value_usd") not in (None, ""):
                config["trade_min_value_usd"] = float(payload.get("trade_min_value_usd"))

            if payload.get("max_quote_per_trade_usd") not in (None, ""):
                config["max_quote_per_trade_usd"] = float(payload.get("max_quote_per_trade_usd"))

            if payload.get("max_active_satellites") not in (None, ""):
                config["max_active_satellites"] = int(float(payload.get("max_active_satellites")))

            if payload.get("rotation_cooldown_minutes") not in (None, ""):
                config["rotation_cooldown_minutes"] = int(float(payload.get("rotation_cooldown_minutes")))

            if payload.get("min_meme_score") not in (None, ""):
                config["min_meme_score"] = float(payload.get("min_meme_score"))

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
