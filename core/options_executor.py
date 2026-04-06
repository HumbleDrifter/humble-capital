import json
import time

from brokers.ibkr_adapter import IBKRAdapter, get_ibkr_runtime_config
from options.validator import validate_options_order
from storage import save_options_execution, save_options_order_record


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


def _build_execution_record(order_payload, order, result):
    payload = order_payload if isinstance(order_payload, dict) else {}
    normalized = order if isinstance(order, dict) else {}
    outcome = result if isinstance(result, dict) else {}
    return {
        "ts": int(time.time()),
        "proposal_id": str(payload.get("proposal_id") or normalized.get("proposal_id") or "").strip(),
        "underlying": str(normalized.get("underlying") or payload.get("underlying") or "").strip().upper(),
        "strategy": str(normalized.get("strategy") or payload.get("strategy") or "").strip().lower(),
        "broker": str(normalized.get("broker") or payload.get("broker") or "").strip().lower(),
        "order_type": str(normalized.get("order_type") or payload.get("order_type") or "").strip().upper(),
        "limit_price": float(normalized.get("limit_price") or payload.get("limit_price") or 0.0),
        "status": str(outcome.get("status") or "").strip(),
        "ok": bool(outcome.get("ok")),
        "reason": str(outcome.get("reason") or "").strip(),
        "order_id": str(outcome.get("order_id") or "").strip(),
        "reconnect_attempted": bool(outcome.get("reconnect_attempted")),
        "connection_reused": bool(outcome.get("connection_reused")),
        "error": str(outcome.get("error") or "").strip(),
        "source": str(normalized.get("source") or payload.get("source") or "").strip(),
    }


def _build_order_record(order, result):
    normalized = order if isinstance(order, dict) else {}
    outcome = result if isinstance(result, dict) else {}
    return {
        "proposal_id": str(normalized.get("proposal_id") or "").strip(),
        "broker_order_id": str(outcome.get("order_id") or "").strip(),
        "underlying": str(normalized.get("underlying") or "").strip().upper(),
        "strategy": str(normalized.get("strategy") or "").strip().lower(),
        "broker": str(normalized.get("broker") or "").strip().lower(),
        "asset_class": "option",
        "order_type": str(normalized.get("order_type") or "").strip().upper(),
        "limit_price": float(normalized.get("limit_price") or 0.0),
        "tif": str(normalized.get("tif") or "").strip().upper(),
        "status": str(outcome.get("status") or "").strip() or "Submitted",
        "source": str(normalized.get("source") or "").strip(),
        "contract_summary": outcome.get("contract_summary") if isinstance(outcome.get("contract_summary"), dict) else {},
        "legs": outcome.get("legs_summary") if isinstance(outcome.get("legs_summary"), list) else list(normalized.get("legs") or []),
        "created_at": outcome.get("created_at") or normalized.get("created_at"),
        "updated_at": outcome.get("updated_at") or None,
    }


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
        save_options_execution(_build_execution_record(order_payload, {}, result))
        _log_options_event("validation_failed", result)
        return result

    order = validation.get("order") or {}
    broker = str(order.get("broker", "")).strip().lower()
    if broker != "ibkr":
        result = {"ok": False, "reason": "unsupported_options_broker", "broker": broker}
        save_options_execution(_build_execution_record(order_payload, order, result))
        _log_options_event("routing_failed", result)
        return result

    adapter = IBKRAdapter(get_ibkr_runtime_config())
    result = adapter.place_options_order(order)
    save_options_execution(_build_execution_record(order_payload, order, result))
    if result.get("ok"):
        save_options_order_record(_build_order_record(order, result))
    _log_options_event("result", result)
    return result
