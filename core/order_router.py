from core.options_executor import execute_options_order


def route_order(order):
    asset_class = str((order or {}).get("asset_class", "") or "").strip().lower()
    broker = str((order or {}).get("broker", "") or "").strip().lower()

    if asset_class in {"option", "options"} and broker == "ibkr":
        return execute_options_order(order)

    return {
        "ok": False,
        "reason": "unsupported_order_route",
        "asset_class": asset_class,
        "broker": broker,
    }
