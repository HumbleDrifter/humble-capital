import threading
import time
from collections import deque
from datetime import datetime, timezone


_TRACE_LOCK = threading.Lock()
_TRACE_BUFFER = deque(maxlen=200)


def _utcnow_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def _normalize_reason_code(value):
    text = str(value or "").strip()
    if not text:
        return ""
    if "=" in text and text.split("=", 1)[0].strip():
        text = text.split("=", 1)[0].strip()
    normalized = text.lower().replace(" ", "_")
    aliases = {
        "blocked_by_current_state": "manual_off",
        "candidate_not_flagged_sniper_eligible": "not_sniper_eligible",
        "score_below_min": "score_below_threshold",
        "tradability_unavailable": "snapshot_failed",
    }
    return aliases.get(normalized, normalized)


def _humanize_reason(reason_code):
    labels = {
        "filled": "Buy filled",
        "unsupported_signal_type": "Unsupported signal type",
        "invalid_symbol": "Symbol not tradable on venue",
        "not_valid_product": "Symbol not tradable on venue",
        "not_managed_asset": "Symbol not currently managed",
        "manual_off": "Asset manually turned off",
        "blocked_by_current_state": "Asset manually turned off",
        "not_sniper_eligible": "Not sniper eligible",
        "candidate_not_flagged_sniper_eligible": "Not sniper eligible",
        "score_below_min": "Score below threshold",
        "score_below_threshold": "Score below threshold",
        "pump_protected": "Pump protection blocked the buy",
        "satellite_budget_exhausted": "Satellite budget exhausted",
        "no_free_cash_after_reserve": "No free cash after reserve",
        "buy_not_allowed": "Buy not allowed under current portfolio rules",
        "trade_below_minimum": "Trade fell below minimum size",
        "drawdown_freeze": "Drawdown freeze is active",
        "execution_failed": "Execution failed",
        "duplicate_order_id": "Duplicate order id ignored",
        "duplicate_alert_window": "Duplicate alert ignored",
        "snapshot_failed": "Portfolio snapshot unavailable",
        "trading_disabled": "Trading is disabled",
    }
    normalized = _normalize_reason_code(reason_code)
    if normalized in labels:
        return labels[normalized]
    if not normalized:
        return "Decision recorded"
    return normalized.replace("_", " ").strip().capitalize()


def _build_summary(entry):
    product_id = str(entry.get("product_id") or "Unknown").strip()
    category = str(entry.get("result_category") or "").strip().lower()
    category_label = {
        "bought": "Bought",
        "blocked": "Blocked",
        "ignored": "Ignored",
        "invalid": "Invalid",
        "execution_failed": "Execution Failed",
    }.get(category, "Decision")
    reason_label = _humanize_reason(entry.get("reason_code"))
    return f"{product_id} — {category_label} — {reason_label}"


def record_decision_trace(entry):
    payload = dict(entry or {})
    payload["ts"] = int(time.time())
    payload["timestamp"] = _utcnow_iso()
    payload["product_id"] = str(payload.get("product_id") or "").strip().upper()
    payload["action"] = str(payload.get("action") or "").strip().upper()
    payload["signal_type"] = str(payload.get("signal_type") or "").strip().upper()
    payload["normalized_signal_type"] = str(payload.get("normalized_signal_type") or "").strip().upper()
    payload["strategy"] = str(payload.get("strategy") or "").strip()
    payload["timeframe"] = str(payload.get("timeframe") or "").strip()
    payload["asset_state"] = str(payload.get("asset_state") or "").strip().lower()
    payload["result_category"] = str(payload.get("result_category") or "").strip().lower()
    payload["reason_code"] = _normalize_reason_code(payload.get("reason_code"))
    payload["summary"] = str(payload.get("summary") or "").strip() or _build_summary(payload)

    for key in [
        "score",
        "threshold",
        "allowed_buy_usd",
        "requested_buy_usd",
        "max_quote_per_trade_usd",
        "free_cash_after_reserve_usd",
    ]:
        numeric = _safe_float(payload.get(key))
        if numeric is not None:
            payload[key] = numeric
        elif key in payload:
            payload.pop(key, None)

    with _TRACE_LOCK:
        _TRACE_BUFFER.appendleft(payload)
    return payload


def list_decision_traces(limit=25, product_id=None, result_category=None):
    product_filter = str(product_id or "").strip().upper()
    category_filter = str(result_category or "").strip().lower()
    with _TRACE_LOCK:
        items = list(_TRACE_BUFFER)
    if product_filter:
        items = [item for item in items if str(item.get("product_id") or "").strip().upper() == product_filter]
    if category_filter:
        items = [item for item in items if str(item.get("result_category") or "").strip().lower() == category_filter]
    return items[:max(1, int(limit or 25))]


def infer_asset_state(snapshot, product_id):
    snapshot = snapshot if isinstance(snapshot, dict) else {}
    config = snapshot.get("config") or {}
    product_id = str(product_id or "").strip().upper()
    core_assets = config.get("core_assets") or {}
    allowed_assets = set(str(item or "").strip().upper() for item in (config.get("satellite_allowed") or []))
    blocked_assets = set(str(item or "").strip().upper() for item in (config.get("satellite_blocked") or []))
    if product_id in core_assets:
        return "core"
    if product_id in blocked_assets:
        return "disable"
    if product_id in allowed_assets:
        return "enable"
    return "auto"
