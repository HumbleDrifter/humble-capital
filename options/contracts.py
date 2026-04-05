from datetime import datetime


def _safe_dict(value):
    return value if isinstance(value, dict) else {}


def _safe_list(value):
    return value if isinstance(value, list) else []


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _safe_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_expiry(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y%m%d")
        except Exception:
            continue
    return raw.replace("-", "").replace("/", "")


def _normalize_right(value):
    raw = str(value or "").strip().upper()
    if raw in {"C", "CALL"}:
        return "CALL", "C"
    if raw in {"P", "PUT"}:
        return "PUT", "P"
    return raw, raw[:1]


def _normalize_order_type(value):
    raw = str(value or "").strip().upper()
    if raw in {"", "LIMIT", "LMT"}:
        return "LIMIT"
    return raw


def normalize_option_leg(payload):
    raw = _safe_dict(payload)
    right, right_code = _normalize_right(raw.get("right"))
    return {
        "side": str(raw.get("side", "BUY") or "BUY").strip().upper(),
        "right": right,
        "right_code": right_code,
        "strike": _safe_float(raw.get("strike")),
        "expiry": _normalize_expiry(raw.get("expiry")),
        "quantity": max(1, _safe_int(raw.get("quantity", raw.get("contracts", 1)), 1)),
        "exchange": str(raw.get("exchange", "SMART") or "SMART").strip() or "SMART",
        "currency": str(raw.get("currency", "USD") or "USD").strip() or "USD",
    }


def normalize_options_payload(payload):
    raw = _safe_dict(payload)
    legs = [normalize_option_leg(item) for item in _safe_list(raw.get("legs")) if _safe_dict(item)]
    return {
        "asset_class": str(raw.get("asset_class", "option") or "option").strip().lower(),
        "broker": str(raw.get("broker", "ibkr") or "ibkr").strip().lower(),
        "action": str(raw.get("action", "BUY") or "BUY").strip().upper(),
        "underlying": str(raw.get("underlying", "") or "").strip().upper(),
        "strategy": str(raw.get("strategy", "") or "").strip().lower(),
        "legs": legs,
        "order_type": _normalize_order_type(raw.get("order_type", "LIMIT")),
        "limit_price": _safe_float(raw.get("limit_price")),
        "tif": str(raw.get("tif", "DAY") or "DAY").strip().upper(),
        "source": str(raw.get("source", "manual") or "manual").strip() or "manual",
        "proposal_id": str(raw.get("proposal_id", "") or "").strip(),
        "approval_verified": _safe_bool(raw.get("approval_verified")),
        "broker_mode": str(raw.get("broker_mode", "") or "").strip().lower(),
    }


def describe_options_order(order):
    normalized = normalize_options_payload(order)
    legs = normalized.get("legs", [])
    leg_parts = []
    for leg in legs:
        leg_parts.append(
            f"{leg.get('side')} {leg.get('quantity')} {leg.get('underlying', normalized.get('underlying', '')) or normalized.get('underlying', '')} "
            f"{leg.get('expiry')} {leg.get('strike')} {leg.get('right_code')}"
        )
    if not leg_parts:
        leg_parts.append("no legs")
    return (
        f"{normalized.get('broker', 'ibkr').upper()} "
        f"{normalized.get('strategy', 'option')} "
        f"{normalized.get('underlying', '')} "
        f"@ {normalized.get('limit_price', 0.0):.2f} "
        f"({'; '.join(leg_parts)})"
    ).strip()
