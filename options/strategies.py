SUPPORTED_STRATEGIES = {
    "long_call": "Single long call",
    "long_put": "Single long put",
    "bull_call_spread": "Debit call spread",
    "bear_put_spread": "Debit put spread",
}


def _safe_list(value):
    return value if isinstance(value, list) else []


def _same_expiry_and_quantity(legs):
    expiries = {str((leg or {}).get("expiry", "") or "").strip() for leg in legs}
    quantities = {int((leg or {}).get("quantity", 0) or 0) for leg in legs}
    return len(expiries) == 1 and len(quantities) == 1


def validate_strategy_structure(order):
    order = order if isinstance(order, dict) else {}
    strategy = str(order.get("strategy", "") or "").strip().lower()
    legs = _safe_list(order.get("legs"))
    errors = []

    if strategy not in SUPPORTED_STRATEGIES:
        return {
            "ok": False,
            "errors": [f"unsupported strategy={strategy or 'unknown'}"],
            "supports_naked_short": False,
        }

    if strategy == "long_call":
        if len(legs) != 1:
            errors.append("long_call requires exactly one leg")
        elif str(legs[0].get("side", "")).upper() != "BUY" or str(legs[0].get("right", "")).upper() != "CALL":
            errors.append("long_call must be a single BUY CALL leg")

    elif strategy == "long_put":
        if len(legs) != 1:
            errors.append("long_put requires exactly one leg")
        elif str(legs[0].get("side", "")).upper() != "BUY" or str(legs[0].get("right", "")).upper() != "PUT":
            errors.append("long_put must be a single BUY PUT leg")

    elif strategy == "bull_call_spread":
        if len(legs) != 2:
            errors.append("bull_call_spread requires exactly two legs")
        elif not _same_expiry_and_quantity(legs):
            errors.append("bull_call_spread legs must share the same expiry and quantity")
        else:
            buy_legs = [leg for leg in legs if str(leg.get("side", "")).upper() == "BUY"]
            sell_legs = [leg for leg in legs if str(leg.get("side", "")).upper() == "SELL"]
            if len(buy_legs) != 1 or len(sell_legs) != 1:
                errors.append("bull_call_spread requires one BUY leg and one SELL leg")
            elif any(str(leg.get("right", "")).upper() != "CALL" for leg in legs):
                errors.append("bull_call_spread must use CALL legs only")
            elif float(buy_legs[0].get("strike", 0.0) or 0.0) >= float(sell_legs[0].get("strike", 0.0) or 0.0):
                errors.append("bull_call_spread buy strike must be lower than sell strike")

    elif strategy == "bear_put_spread":
        if len(legs) != 2:
            errors.append("bear_put_spread requires exactly two legs")
        elif not _same_expiry_and_quantity(legs):
            errors.append("bear_put_spread legs must share the same expiry and quantity")
        else:
            buy_legs = [leg for leg in legs if str(leg.get("side", "")).upper() == "BUY"]
            sell_legs = [leg for leg in legs if str(leg.get("side", "")).upper() == "SELL"]
            if len(buy_legs) != 1 or len(sell_legs) != 1:
                errors.append("bear_put_spread requires one BUY leg and one SELL leg")
            elif any(str(leg.get("right", "")).upper() != "PUT" for leg in legs):
                errors.append("bear_put_spread must use PUT legs only")
            elif float(buy_legs[0].get("strike", 0.0) or 0.0) <= float(sell_legs[0].get("strike", 0.0) or 0.0):
                errors.append("bear_put_spread buy strike must be higher than sell strike")

    return {
        "ok": not errors,
        "errors": errors,
        "supports_naked_short": False,
        "strategy_label": SUPPORTED_STRATEGIES.get(strategy, strategy or "unknown"),
    }


def has_naked_short_exposure(order):
    order = order if isinstance(order, dict) else {}
    strategy = str(order.get("strategy", "") or "").strip().lower()
    legs = _safe_list(order.get("legs"))

    if strategy in {"long_call", "long_put"}:
        return any(str(leg.get("side", "")).upper() == "SELL" for leg in legs)

    if strategy in {"bull_call_spread", "bear_put_spread"}:
        return False

    return any(str(leg.get("side", "")).upper() == "SELL" for leg in legs)
