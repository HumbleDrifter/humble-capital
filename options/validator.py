import os
import math
from datetime import datetime, timezone

from env_runtime import load_runtime_env

from options.contracts import normalize_options_payload
from options.strategies import has_naked_short_exposure, validate_strategy_structure

load_runtime_env(override=True)


def _env_bool(name, default=False):
    value = str(os.getenv(name, str(default)) or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _env_int(name, default=0):
    try:
        return int(float(os.getenv(name, default) or default))
    except Exception:
        return int(default)


def _env_float(name, default=0.0):
    try:
        return float(os.getenv(name, default) or default)
    except Exception:
        return float(default)


def _env_list(name):
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return []
    return sorted({part.strip().upper() for part in raw.split(",") if part.strip()})


def _parse_expiry(expiry):
    text = str(expiry or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def get_options_risk_config():
    load_runtime_env(override=True)
    return {
        "options_enabled": _env_bool("OPTIONS_ENABLED", False),
        "ibkr_enabled": _env_bool("IBKR_ENABLED", False),
        "require_approval": _env_bool("OPTIONS_REQUIRE_APPROVAL", True),
        "allowed_underlyings": _env_list("OPTIONS_ALLOWED_UNDERLYINGS"),
        "min_dte": _env_int("OPTIONS_MIN_DTE", 1),
        "max_dte": _env_int("OPTIONS_MAX_DTE", 45),
        "allow_0dte": _env_bool("OPTIONS_ALLOW_0DTE", False),
        "max_contracts": _env_int("OPTIONS_MAX_CONTRACTS", 5),
        "max_premium_usd": _env_float("OPTIONS_MAX_PREMIUM_USD", 2500.0),
        "paper_only": _env_bool("OPTIONS_PAPER_ONLY", True),
    }


def validate_options_order(payload, enforce_approval=False, approval_verified=False):
    order = normalize_options_payload(payload)
    risk = get_options_risk_config()
    errors = []
    warnings = []

    if not risk.get("options_enabled"):
        errors.append("options trading is disabled")

    if not risk.get("ibkr_enabled"):
        errors.append("IBKR options routing is disabled")

    if order.get("asset_class") not in {"option", "options"}:
        errors.append("asset_class must be option/options")

    if order.get("broker") != "ibkr":
        errors.append("broker must be ibkr")

    if order.get("action") != "BUY":
        errors.append("options v1 only supports BUY/opening actions")

    if order.get("order_type") not in {"LIMIT", "LMT"}:
        errors.append("options orders must use limit order_type")
    else:
        order["order_type"] = "LIMIT"

    limit_price = float(order.get("limit_price", 0.0) or 0.0)
    if not math.isfinite(limit_price) or limit_price <= 0:
        errors.append("limit_price must be greater than zero")

    underlying = str(order.get("underlying", "") or "").strip().upper()
    if not underlying:
        errors.append("underlying is required")
    elif risk.get("allowed_underlyings") and underlying not in set(risk.get("allowed_underlyings") or []):
        errors.append(f"underlying {underlying} is not allowed")

    legs = list(order.get("legs") or [])
    if not legs:
        errors.append("at least one options leg is required")

    strategy_check = validate_strategy_structure(order)
    if not strategy_check.get("ok"):
        errors.extend(strategy_check.get("errors", []))

    if has_naked_short_exposure(order):
        errors.append("naked short options are not allowed")

    expiries = {str((leg or {}).get("expiry", "") or "").strip() for leg in legs if str((leg or {}).get("expiry", "") or "").strip()}
    if len(expiries) > 1:
        errors.append("all legs must share the same expiry")

    quantities = {int((leg or {}).get("quantity", 0) or 0) for leg in legs if int((leg or {}).get("quantity", 0) or 0) > 0}
    if len(quantities) > 1:
        errors.append("all legs must share the same quantity")

    order_quantity = max(quantities or {0})
    if order_quantity <= 0:
        errors.append("leg quantity must be greater than zero")
    elif order_quantity > int(risk.get("max_contracts", 0) or 0):
        errors.append(f"contract quantity exceeds max_contracts={risk.get('max_contracts')}")

    first_expiry = _parse_expiry(next(iter(expiries), ""))
    if first_expiry is None:
        errors.append("valid expiry is required in YYYYMMDD format")
        dte = None
    else:
        today = datetime.now(timezone.utc).date()
        dte = (first_expiry.date() - today).days
        if dte == 0 and not risk.get("allow_0dte"):
            errors.append("0DTE options are disabled")
        if dte is not None and dte < int(risk.get("min_dte", 0) or 0) and not (dte == 0 and risk.get("allow_0dte")):
            errors.append(f"DTE {dte} is below min_dte={risk.get('min_dte')}")
        if dte is not None and dte > int(risk.get("max_dte", 0) or 0):
            errors.append(f"DTE {dte} exceeds max_dte={risk.get('max_dte')}")

    estimated_premium_usd = abs(float(order.get("limit_price", 0.0) or 0.0)) * 100.0 * max(order_quantity, 0)
    if estimated_premium_usd > float(risk.get("max_premium_usd", 0.0) or 0.0):
        errors.append(f"estimated premium exceeds max_premium_usd={risk.get('max_premium_usd')}")

    if len(legs) > 1 and (not math.isfinite(limit_price) or limit_price <= 0):
        errors.append("spread/combo orders require a positive net limit_price")

    if risk.get("paper_only") and str(order.get("broker_mode", "") or "").strip().lower() == "live":
        errors.append("live options mode is disabled")

    if enforce_approval and risk.get("require_approval") and not approval_verified:
        errors.append("approved proposal is required before options execution")

    return {
        "ok": not errors,
        "errors": errors,
        "warnings": warnings,
        "order": {
            **order,
            "dte": dte,
            "estimated_premium_usd": round(estimated_premium_usd, 2),
            "strategy_label": strategy_check.get("strategy_label"),
            "ibkr_action": "BUY",
        },
        "risk": risk,
    }
