import json
import time

from brokers.ibkr_adapter import IBKRAdapter, get_ibkr_runtime_config
from options.validator import validate_options_order


def _safe_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _log_options_event(event_type, payload):
    envelope = {
        "ts": int(time.time()),
        "component": "options_executor",
        "event": str(event_type or "").strip() or "unknown",
        "payload": payload if isinstance(payload, dict) else {"value": payload},
    }
    print(json.dumps(envelope, sort_keys=True, ensure_ascii=False))


def execute_options_order(order_payload):
    _log_options_event(
        "attempt",
        {
            "proposal_id": (order_payload or {}).get("proposal_id"),
            "broker": (order_payload or {}).get("broker"),
            "underlying": (order_payload or {}).get("underlying"),
            "strategy": (order_payload or {}).get("strategy"),
            "source": (order_payload or {}).get("source"),
        },
    )

    validation = validate_options_order(
        order_payload,
        enforce_approval=True,
        approval_verified=_safe_bool((order_payload or {}).get("approval_verified")),
    )
    if not validation.get("ok"):
        result = {
            "ok": False,
            "reason": "options_validation_failed",
            "errors": validation.get("errors", []),
            "warnings": validation.get("warnings", []),
        }
        _log_options_event("validation_failed", result)
        return result

    order = validation.get("order") or {}
    broker = str(order.get("broker", "")).strip().lower()
    if broker != "ibkr":
        result = {"ok": False, "reason": "unsupported_options_broker", "broker": broker}
        _log_options_event("routing_failed", result)
        return result

    adapter = IBKRAdapter(get_ibkr_runtime_config())
    result = adapter.place_options_order(order)
    _log_options_event("result", result)
    return result
