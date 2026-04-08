import json
import os
import time
import threading
from flask import Blueprint, request, jsonify
from dotenv import load_dotenv

from execution import product_id_exists
from portfolio import get_active_satellite_buy_universe, get_portfolio_snapshot, is_satellite_buy_eligible, is_sniper_buy_eligible
from rebalancer import dispatch_signal_action
from notify import notify_execution_result

load_dotenv("/root/tradingbot/.env", override=True)

webhook_bp = Blueprint("webhook", __name__)

_RECENT_ALERTS = {}
_ALERTS_LOCK = threading.Lock()

_PROCESSED_ORDER_IDS = {}
_ORDER_LOCK = threading.Lock()
ORDER_CACHE_SECONDS = 600


def _norm(x):
    return str(x or "").strip()


def _log_webhook_event(event, payload=None):
    envelope = {
        "ts": int(time.time()),
        "component": "webhook",
        "event": str(event or "").strip() or "unknown",
        "payload": payload if isinstance(payload, dict) else {},
    }
    print(json.dumps(envelope, sort_keys=True, ensure_ascii=False))


def _normalize_webhook_product_id(*values):
    for value in values:
        raw = _norm(value).upper()
        if not raw:
            continue

        candidate = raw.split(":")[-1].strip()
        candidate = candidate.replace("/", "-").replace("_", "-")

        if product_id_exists(candidate):
            return candidate

        if candidate.endswith("USD") and "-" not in candidate and len(candidate) > 3:
            dashed = f"{candidate[:-3]}-USD"
            if product_id_exists(dashed):
                return dashed
            candidate = dashed

        if "-" not in candidate:
            usd_candidate = f"{candidate}-USD"
            if product_id_exists(usd_candidate):
                return usd_candidate

        return candidate

    return ""


def _get_env_int(name, default):
    try:
        return int(str(os.getenv(name, str(default)) or str(default)).strip())
    except Exception:
        return int(default)


def get_webhook_secrets():
    load_dotenv("/root/tradingbot/.env", override=True)

    raw_values = []

    primary = os.getenv("WEBHOOK_SHARED_SECRET", "")
    extra = os.getenv("WEBHOOK_SHARED_SECRETS", "")

    if primary:
        raw_values.extend(str(primary).split(","))

    if extra:
        raw_values.extend(str(extra).split(","))

    cleaned = []
    seen = set()

    for s in raw_values:
        s = str(s or "").strip().strip('"').strip("'")
        if s and s not in seen:
            cleaned.append(s)
            seen.add(s)

    return cleaned


def get_alert_dedupe_seconds():
    load_dotenv("/root/tradingbot/.env", override=True)
    return _get_env_int("DUPLICATE_WINDOW_SEC", 60)


def get_alert_max_age_sec():
    load_dotenv("/root/tradingbot/.env", override=True)
    return _get_env_int("MAX_ALERT_AGE_SEC", 120)


def _alert_key(product_id, action, signal_type, timeframe):
    return f"{product_id}|{action}|{signal_type}|{timeframe}"


def _is_duplicate(product_id, action, signal_type, timeframe):
    now = time.time()
    dedupe_seconds = get_alert_dedupe_seconds()
    key = _alert_key(product_id, action, signal_type, timeframe)

    with _ALERTS_LOCK:
        expired = [
            k for k, ts in _RECENT_ALERTS.items()
            if (now - ts) > dedupe_seconds
        ]

        for k in expired:
            _RECENT_ALERTS.pop(k, None)

        last_ts = _RECENT_ALERTS.get(key)

        if last_ts and (now - last_ts) <= dedupe_seconds:
            return True

        _RECENT_ALERTS[key] = now
        return False


def _order_already_processed(order_id):
    if not order_id:
        return False

    now = time.time()

    with _ORDER_LOCK:
        expired = [
            k for k, ts in _PROCESSED_ORDER_IDS.items()
            if (now - ts) > ORDER_CACHE_SECONDS
        ]

        for k in expired:
            _PROCESSED_ORDER_IDS.pop(k, None)

        if order_id in _PROCESSED_ORDER_IDS:
            return True

        _PROCESSED_ORDER_IDS[order_id] = now
        return False


def _validate_timestamp(timestamp_value):
    timestamp = _norm(timestamp_value)
    max_age_sec = get_alert_max_age_sec()

    if not timestamp:
        return True, None, None

    try:
        ts = int(timestamp) / 1000.0
    except Exception:
        return False, "invalid_timestamp", None

    age_sec = time.time() - ts

    if age_sec > max_age_sec:
        return False, "alert_expired", age_sec

    if age_sec < -30:
        return False, "timestamp_in_future", age_sec

    return True, None, age_sec


def _attempt_has_fill(attempt: dict) -> bool:
    if not isinstance(attempt, dict):
        return False

    try:
        filled_base = float(attempt.get("filled_base", 0) or 0)
    except Exception:
        filled_base = 0.0

    try:
        avg_fill_price = float(attempt.get("avg_fill_price", 0) or 0)
    except Exception:
        avg_fill_price = 0.0

    if filled_base > 0:
        return True

    if avg_fill_price > 0:
        return True

    raw = attempt.get("raw") or {}
    status = str(raw.get("status") or attempt.get("status") or "").upper()
    return status == "FILLED"


def _result_has_successful_fill(result_wrapper) -> bool:
    if not isinstance(result_wrapper, dict):
        return False

    attempts = result_wrapper.get("attempts") or []
    for attempt in attempts:
        if _attempt_has_fill(attempt):
            return True

    try:
        filled_base = float(result_wrapper.get("filled_base", 0) or 0)
    except Exception:
        filled_base = 0.0

    if filled_base > 0:
        return True

    status = str(result_wrapper.get("status", "") or "").upper()
    return status == "FILLED"


def _log_webhook_ignored(product_id, action, signal_type, reason, detail=""):
    message = (
        f"[webhook] ignored_signal product_id={product_id} "
        f"action={action} signal_type={signal_type} reason={reason}"
    )
    if detail:
        message += f" detail={detail}"
    print(message)


def _safe_snapshot():
    try:
        snapshot = get_portfolio_snapshot()
    except Exception as exc:
        return None, f"snapshot_unavailable:{exc}"

    if not isinstance(snapshot, dict):
        return None, "snapshot_invalid"

    config = snapshot.get("config")
    positions = snapshot.get("positions")
    if not isinstance(config, dict) or not isinstance(positions, dict):
        return None, "snapshot_incomplete"

    return snapshot, None


def _webhook_symbol_tradability(product_id, action, signal_type):
    if not product_id:
        return False, "missing_product_id", "No symbol was provided."

    if not product_id_exists(product_id):
        return False, "invalid_symbol", "Symbol is not recognized by the execution venue."

    snapshot, snapshot_error = _safe_snapshot()
    if snapshot is None:
        return False, "tradability_unavailable", snapshot_error or "Current tradability state could not be loaded."

    config = snapshot.get("config") or {}
    positions = snapshot.get("positions") or {}
    core_assets = set((config.get("core_assets") or {}).keys())
    blocked_assets = set(config.get("satellite_blocked") or [])
    allowed_assets = set(config.get("satellite_allowed") or [])
    active_buy_universe = set(get_active_satellite_buy_universe(snapshot) or [])
    has_position = product_id in positions

    if action == "BUY":
        if product_id in blocked_assets:
            return False, "blocked_by_current_state", "Symbol is currently blocked in satellite controls."

        if signal_type == "CORE_BUY_WINDOW":
            if product_id not in core_assets:
                return False, "not_core_asset", "Core buy signal received for a symbol that is not configured as core."
            return True, "core_buy_eligible", "Configured core asset is eligible for a core buy path."

        if signal_type == "SNIPER_BUY":
            if not is_sniper_buy_eligible(product_id, snapshot):
                return False, "not_sniper_eligible", "Symbol is not currently eligible for sniper buys."
            return True, "sniper_buy_eligible", "Symbol is currently eligible for sniper buys."

        if not is_satellite_buy_eligible(product_id, snapshot):
            if product_id in core_assets:
                return False, "not_satellite_eligible", "Symbol is configured as a core asset rather than a satellite buy candidate."
            if product_id in allowed_assets and product_id not in active_buy_universe:
                return False, "not_live_in_active_universe", "Symbol is allowed but not currently live in the active satellite buy universe."
            return False, "not_currently_tradable", "Symbol is not currently eligible in the active satellite buy universe."

        return True, "satellite_buy_eligible", "Symbol is currently eligible in the active satellite buy universe."

    if action in {"TRIM", "EXIT"}:
        if has_position:
            return True, "position_managed", "Symbol has an existing position and may be reduced or exited."
        if product_id in core_assets:
            return True, "core_managed", "Configured core asset may be reduced or exited."
        if product_id in active_buy_universe or product_id in allowed_assets:
            return True, "managed_asset", "Managed symbol may be reduced or exited."
        return False, "not_managed_asset", "Symbol is not currently managed in portfolio state."

    return False, "unsupported_action", f"Webhook action {action} is not tradability-gated."


@webhook_bp.route("/webhook", methods=["POST"])
def webhook():
    load_dotenv("/root/tradingbot/.env", override=True)

    data = request.get_json(silent=True) or {}
    _log_webhook_event(
        "received",
        {
            "remote_addr": request.headers.get("CF-Connecting-IP") or request.remote_addr,
            "product_id_raw": _norm(data.get("product_id") or data.get("symbol") or data.get("ticker")),
            "action_raw": _norm(data.get("action") or data.get("side")),
            "signal_type_raw": _norm(data.get("signal_type") or data.get("signal")),
        },
    )

    secret = _norm(data.get("secret")).strip('"').strip("'")
    webhook_secrets = get_webhook_secrets()

    if not webhook_secrets or secret not in webhook_secrets:
        return jsonify({"ok": False, "reason": "invalid_secret"}), 401

    timestamp = _norm(data.get("timestamp"))
    ts_ok, ts_reason, age_sec = _validate_timestamp(timestamp)

    if not ts_ok:
        payload = {"ok": False, "reason": ts_reason}
        if age_sec is not None:
            payload["age_sec"] = age_sec
        return jsonify(payload), 400
    _log_webhook_event("authenticated", {"age_sec": age_sec})

    product_id = _normalize_webhook_product_id(
        data.get("product_id"),
        data.get("symbol"),
        data.get("ticker"),
    )
    action = _norm(data.get("action") or data.get("side")).upper()
    signal_type = _norm(data.get("signal_type") or data.get("signal")).upper()
    timeframe = _norm(data.get("timeframe"))
    strategy = _norm(data.get("strategy"))
    price = data.get("price")
    order_id = _norm(data.get("order_id"))

    try:
        trim_pct = float(data.get("trim_pct", 0.50) or 0.50)
    except Exception:
        trim_pct = 0.50

    _log_webhook_event(
        "parsed",
        {
            "product_id": product_id,
            "action": action,
            "signal_type": signal_type,
            "timeframe": timeframe,
            "strategy": strategy,
            "order_id": order_id,
        },
    )

    if not product_id:
        return jsonify({"ok": False, "reason": "missing_product_id"}), 400

    if action not in {"BUY", "TRIM", "EXIT"}:
        return jsonify({"ok": False, "reason": f"unsupported_action={action}"}), 400

    if action == "BUY" and signal_type not in {
        "CORE_BUY_WINDOW",
        "SATELLITE_BUY_EARLY",
        "SATELLITE_BUY",
        "SATELLITE_BUY_HEAVY",
        "SNIPER_BUY",
	"APPROVED_REBALANCE_BUY",
    }:
        return jsonify({"ok": False, "reason": f"unsupported_signal_type={signal_type}"}), 400

    if action == "TRIM" and not signal_type:
        signal_type = "SNIPER_EXIT"

    if action == "EXIT" and not signal_type:
        signal_type = "EXIT"

    if _order_already_processed(order_id):
        return jsonify({"ok": False, "reason": "duplicate_order_id"}), 200

    exists = product_id_exists(product_id)
    _log_webhook_event(
        "product_validation",
        {
            "product_id": product_id,
            "exists": bool(exists),
        },
    )

    if _is_duplicate(product_id, action, signal_type, timeframe):
        return jsonify({"ok": False, "reason": "duplicate_alert_window"}), 200

    tradable_ok, tradability_reason, tradability_detail = _webhook_symbol_tradability(
        product_id=product_id,
        action=action,
        signal_type=signal_type,
    )
    if not tradable_ok:
        _log_webhook_ignored(
            product_id=product_id,
            action=action,
            signal_type=signal_type,
            reason=tradability_reason,
            detail=tradability_detail,
        )
        _log_webhook_event(
            "routing_blocked",
            {
                "product_id": product_id,
                "action": action,
                "signal_type": signal_type,
                "tradability_reason": tradability_reason,
                "detail": tradability_detail,
            },
        )
        return jsonify(
            {
                "ok": False,
                "ignored": True,
                "reason": "symbol_not_tradable",
                "tradability_reason": tradability_reason,
                "detail": tradability_detail,
                "product_id": product_id,
                "action": action,
                "signal_type": signal_type,
            }
        ), 200

    _log_webhook_event(
        "routing_allowed",
        {
            "product_id": product_id,
            "action": action,
            "signal_type": signal_type,
            "tradability_reason": tradability_reason,
        },
    )

    try:
        _log_webhook_event(
            "dispatch_start",
            {
                "product_id": product_id,
                "action": action,
                "signal_type": signal_type,
                "timeframe": timeframe,
            },
        )
        result = dispatch_signal_action(
            product_id=product_id,
            action=action,
            signal_type=signal_type,
            timeframe=timeframe,
            strategy=strategy,
            price=price,
            order_id=order_id,
            trim_pct=trim_pct,
	    quote_size=data.get("quote_size"),
        )

        result_wrapper = result.get("result") if isinstance(result, dict) else None
        _log_webhook_event(
            "dispatch_result",
            {
                "product_id": product_id,
                "action": action,
                "signal_type": signal_type,
                "ok": bool((result or {}).get("ok")),
                "reason": str((result or {}).get("reason") or "").strip(),
                "requested_buy_usd": (result or {}).get("requested_buy_usd"),
                "filled": bool((result_wrapper or {}).get("filled")),
                "wrapper_ok": (result_wrapper or {}).get("ok") if isinstance(result_wrapper, dict) else None,
            },
        )

        if _result_has_successful_fill(result_wrapper):
            if action == "BUY":
                notify_execution_result(
                    product_id=product_id,
                    side="BUY",
                    signal_type=signal_type,
                    requested_usd=result.get("requested_buy_usd"),
                    result_wrapper=result_wrapper,
                )
            else:
                notify_execution_result(
                    product_id=product_id,
                    side="SELL",
                    signal_type=signal_type,
                    result_wrapper=result_wrapper,
                )
        else:
            print(
                f"[webhook] no successful fill for {product_id} "
                f"action={action} signal_type={signal_type}"
            )

        return jsonify(result), 200

    except Exception as exc:
        print(
            f"[webhook] dispatch_exception product_id={product_id} "
            f"action={action} signal_type={signal_type} error={exc}"
        )
        return jsonify(
            {
                "ok": False,
                "reason": "dispatch_exception",
                "error": str(exc),
            }
        ), 500
