import os
import time
from dotenv import load_dotenv

from execution import place_limit_quote_with_retries, place_limit_base_with_retries
from positions import compute_sell_base, compute_full_liquid_base
from portfolio import (
    get_portfolio_snapshot,
    portfolio_summary,
    classify_asset,
    allowed_core_buy_usd,
    allowed_satellite_buy_usd,
    required_trim_usd,
    get_core_target_weight,
    get_satellite_max_weight,
    get_profit_harvest_candidates,
)
from storage import record_buy_fill, record_sell_fill, mark_harvest

load_dotenv("/root/tradingbot/.env")

DEFAULT_OFFSET_BPS = int(os.getenv("DEFAULT_OFFSET_BPS", "8"))
DEFAULT_TIMEOUT_SEC = int(os.getenv("DEFAULT_TIMEOUT_SEC", "25"))
DEFAULT_POST_ONLY = os.getenv("DEFAULT_POST_ONLY", "true").lower() == "true"
MAX_QUOTE_SIZE = float(os.getenv("MAX_QUOTE_PER_TRADE_USD", "25"))


def get_rebalance_plan():
    snapshot = get_portfolio_snapshot()
    summary = portfolio_summary(snapshot)
    plan = {
        "ok": True,
        "total_value_usd": snapshot["total_value_usd"],
        "usd_cash": snapshot["usd_cash"],
        "core_weight": snapshot["core_weight"],
        "satellite_weight": snapshot["satellite_weight"],
        "market_regime": summary.get("market_regime"),
        "buys": [],
        "trims": [],
        "harvests": get_profit_harvest_plan(snapshot).get("harvests", []),
        "notes": [],
    }

    for product_id in snapshot["config"]["core_assets"].keys():
        current_weight = summary["assets"].get(product_id, {}).get("weight_total", 0.0)
        target_weight = get_core_target_weight(product_id, snapshot)
        amount_usd = allowed_core_buy_usd(product_id, snapshot)
        if amount_usd > 0:
            plan["buys"].append({
                "product_id": product_id,
                "signal_type": "CORE_BUY_WINDOW",
                "amount_usd": amount_usd,
                "current_weight": current_weight,
                "target_weight": target_weight,
                "tier": "core",
            })

    for product_id, asset in summary["assets"].items():
        asset_class = asset.get("class", "unknown")
        current_weight = float(asset.get("weight_total", 0.0) or 0.0)
        amount_usd = required_trim_usd(product_id, snapshot)
        if amount_usd <= 0:
            continue
        entry = {
            "product_id": product_id,
            "amount_usd": amount_usd,
            "current_weight": current_weight,
            "tier": asset_class,
        }
        if asset_class == "core":
            entry["target_weight"] = get_core_target_weight(product_id, snapshot)
        elif asset_class == "satellite_active":
            entry["max_weight"] = get_satellite_max_weight(product_id, snapshot)
        plan["trims"].append(entry)

    plan["trims"].sort(key=lambda x: x["amount_usd"], reverse=True)
    plan["buys"].sort(key=lambda x: x["amount_usd"], reverse=True)
    if not plan["buys"] and not plan["trims"] and not plan["harvests"]:
        plan["notes"].append("No rebalance actions needed.")
    return plan


def _build_attempts():
    return [
        {"offset_bps": max(DEFAULT_OFFSET_BPS, 4), "timeout_sec": DEFAULT_TIMEOUT_SEC, "post_only": DEFAULT_POST_ONLY},
        {"offset_bps": 2, "timeout_sec": 20, "post_only": False},
        {"offset_bps": 0, "timeout_sec": 15, "post_only": False},
    ]


def _scale_satellite_buy(signal_type, base_buy_usd):
    if signal_type == "SATELLITE_BUY_EARLY":
        return base_buy_usd * 0.40
    if signal_type == "SATELLITE_BUY_HEAVY":
        return base_buy_usd * 1.50
    return base_buy_usd


def _extract_fill(result_wrapper):
    final_result = (result_wrapper or {}).get("final_result", {}) if isinstance(result_wrapper, dict) else {}
    filled_base = float(final_result.get("filled_base", 0.0) or 0.0)
    avg_fill_price = float(final_result.get("avg_fill_price", 0.0) or 0.0)
    return filled_base, avg_fill_price


def execute_trim(product_id, trim_usd, snapshot):
    current_value = float(snapshot["positions"].get(product_id, {}).get("value_total_usd", 0.0) or 0.0)
    if current_value <= 0:
        return {"ok": False, "product_id": product_id, "reason": "no_position_value"}

    requested_sell_pct = min(1.0, trim_usd / current_value)
    base_size = compute_full_liquid_base(product_id) if requested_sell_pct >= 1.0 else compute_sell_base(product_id, requested_sell_pct)
    if base_size <= 0:
        return {"ok": False, "product_id": product_id, "reason": "no_liquid_balance"}

    result = place_limit_base_with_retries(product_id=product_id, side="SELL", base_size=base_size, attempts=_build_attempts())
    filled_base, _ = _extract_fill(result)
    if filled_base > 0:
        record_sell_fill(product_id, filled_base)

    return {
        "ok": True,
        "product_id": product_id,
        "side": "SELL",
        "requested_trim_usd": trim_usd,
        "requested_sell_pct": requested_sell_pct,
        "result": result,
    }


def execute_buy(product_id, buy_usd, signal_type="SATELLITE_BUY"):
    buy_usd = min(float(buy_usd), MAX_QUOTE_SIZE)
    if buy_usd <= 0:
        return {"ok": False, "product_id": product_id, "reason": "buy_usd_zero"}

    result = place_limit_quote_with_retries(product_id=product_id, side="BUY", quote_size_usd=buy_usd, attempts=_build_attempts())
    filled_base, avg_fill_price = _extract_fill(result)
    if filled_base > 0 and avg_fill_price > 0:
        record_buy_fill(product_id, filled_base, avg_fill_price)

    return {
        "ok": True,
        "product_id": product_id,
        "side": "BUY",
        "signal_type": signal_type,
        "requested_buy_usd": buy_usd,
        "result": result,
    }


def get_profit_harvest_plan(snapshot=None):
    snapshot = snapshot or get_portfolio_snapshot()
    cfg = snapshot["config"].get("profit_harvest", {})
    routes = cfg.get("routes", {"BTC-USD": 0.40, "ETH-USD": 0.40, "CASH": 0.20})
    harvests = []
    for item in get_profit_harvest_candidates(snapshot):
        route_preview = []
        for route_product, pct in routes.items():
            route_preview.append({
                "product_id": route_product,
                "pct": float(pct or 0.0),
                "amount_usd": float(item["amount_usd"]) * float(pct or 0.0),
            })
        harvests.append({**item, "routes": route_preview})
    return {"ok": True, "harvests": harvests}


def route_harvest_proceeds(amount_usd, snapshot):
    routes = snapshot["config"].get("profit_harvest", {}).get("routes", {})
    out = []
    for route_product, pct in routes.items():
        pct = float(pct or 0.0)
        if pct <= 0:
            continue
        route_usd = amount_usd * pct
        if route_product == "CASH":
            out.append({"ok": True, "product_id": "CASH", "requested_buy_usd": route_usd, "reason": "left_as_cash"})
            continue
        out.append(execute_buy(route_product, route_usd, signal_type="HARVEST_ROUTE"))
    return out


def execute_profit_harvest_plan():
    snapshot = get_portfolio_snapshot()
    plan = get_profit_harvest_plan(snapshot)
    report = {"ok": True, "harvest_results": [], "plan": plan}
    for item in plan.get("harvests", []):
        trim_res = execute_trim(item["product_id"], item["amount_usd"], snapshot)
        if trim_res.get("ok"):
            mark_harvest(item["product_id"], int(time.time()))
        refreshed = get_portfolio_snapshot()
        route_res = route_harvest_proceeds(item["amount_usd"], refreshed)
        report["harvest_results"].append({
            "product_id": item["product_id"],
            "gain_pct": item["gain_pct"],
            "trim_pct": item["trim_pct"],
            "amount_usd": item["amount_usd"],
            "trim": trim_res,
            "routes": route_res,
        })
    return report


def execute_rebalance_plan():
    snapshot = get_portfolio_snapshot()
    plan = get_rebalance_plan()
    execution_report = {"ok": True, "plan": plan, "harvest_results": [], "trim_results": [], "buy_results": []}

    harvest_report = execute_profit_harvest_plan()
    execution_report["harvest_results"] = harvest_report.get("harvest_results", [])

    snapshot = get_portfolio_snapshot()
    for entry in plan["trims"]:
        res = execute_trim(entry["product_id"], float(entry["amount_usd"]), snapshot)
        execution_report["trim_results"].append(res)

    snapshot = get_portfolio_snapshot()
    for entry in plan["buys"]:
        product_id = entry["product_id"]
        signal_type = entry.get("signal_type", "CORE_BUY_WINDOW")
        buy_usd = allowed_core_buy_usd(product_id, snapshot) if signal_type == "CORE_BUY_WINDOW" else entry["amount_usd"]
        if buy_usd <= 0:
            execution_report["buy_results"].append({"ok": False, "product_id": product_id, "signal_type": signal_type, "reason": "buy_amount_zero_after_refresh"})
            continue
        execution_report["buy_results"].append(execute_buy(product_id, buy_usd, signal_type=signal_type))
    return execution_report


def execute_satellite_signal(product_id, signal_type="SATELLITE_BUY"):
    snapshot = get_portfolio_snapshot()
    asset_class = classify_asset(product_id, snapshot)
    if asset_class != "satellite_active":
        return {"ok": False, "product_id": product_id, "signal_type": signal_type, "reason": f"asset_class={asset_class}"}

    base_buy_usd = allowed_satellite_buy_usd(product_id, snapshot)
    if base_buy_usd <= 0:
        return {"ok": False, "product_id": product_id, "signal_type": signal_type, "reason": "no_allowed_satellite_buy_usd"}

    buy_usd = min(_scale_satellite_buy(signal_type, base_buy_usd), MAX_QUOTE_SIZE)
    return execute_buy(product_id, buy_usd, signal_type=signal_type)


def print_rebalance_plan(plan):
    print("📊 Rebalance plan\n")
    if plan["harvests"]:
        print("Harvest candidates:")
        for entry in plan["harvests"]:
            print(f"{entry['product_id']}: harvest ${entry['amount_usd']:.2f} at gain {entry['gain_pct']:.2%}")
    else:
        print("Harvest candidates:\nNone")
    print("")
    if plan["buys"]:
        print("Buy candidates:")
        for entry in plan["buys"]:
            print(f"{entry['product_id']}: buy ${entry['amount_usd']:.2f} (weight {entry['current_weight']:.3f} -> target {entry['target_weight']:.3f})")
    else:
        print("Buy candidates:\nNone")
    print("")
    if plan["trims"]:
        print("Trim candidates:")
        for entry in plan["trims"]:
            if "max_weight" in entry:
                print(f"{entry['product_id']}: trim ${entry['amount_usd']:.2f} (weight {entry['current_weight']:.3f} -> max {entry['max_weight']:.3f})")
            elif "target_weight" in entry:
                print(f"{entry['product_id']}: trim ${entry['amount_usd']:.2f} (weight {entry['current_weight']:.3f} -> target {entry['target_weight']:.3f})")
            else:
                print(f"{entry['product_id']}: trim ${entry['amount_usd']:.2f} (weight {entry['current_weight']:.3f})")
    else:
        print("Trim candidates:\nNone")
    print("")
    if plan["notes"]:
        for note in plan["notes"]:
            print(note)


if __name__ == "__main__":
    plan = get_rebalance_plan()
    print_rebalance_plan(plan)
