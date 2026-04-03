import os
from dotenv import load_dotenv

from execution import (
    place_limit_quote_with_retries,
    place_limit_base_with_retries,
)

from positions import (
    compute_sell_base,
    compute_full_liquid_base,
)

from portfolio import (
    get_portfolio_snapshot,
    portfolio_summary,
    classify_asset,
    allowed_core_buy_usd,
    allowed_satellite_buy_usd,
    required_trim_usd,
    get_core_target_weight,
    get_satellite_max_weight,
)

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

    if not plan["buys"] and not plan["trims"]:
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
    elif signal_type == "SATELLITE_BUY_HEAVY":
        return base_buy_usd * 1.50
    return base_buy_usd


def execute_trim(product_id, trim_usd, snapshot):
    current_value = float(snapshot["positions"].get(product_id, {}).get("value_total_usd", 0.0) or 0.0)
    if current_value <= 0:
        return {
            "ok": False,
            "product_id": product_id,
            "reason": "no_position_value",
        }

    requested_sell_pct = min(1.0, trim_usd / current_value)

    if requested_sell_pct >= 1.0:
        base_size = compute_full_liquid_base(product_id)
    else:
        base_size = compute_sell_base(product_id, requested_sell_pct)

    if base_size <= 0:
        return {
            "ok": False,
            "product_id": product_id,
            "reason": "no_liquid_balance",
        }

    result = place_limit_base_with_retries(
        product_id=product_id,
        side="SELL",
        base_size=base_size,
        attempts=_build_attempts(),
    )

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
        return {
            "ok": False,
            "product_id": product_id,
            "reason": "buy_usd_zero",
        }

    result = place_limit_quote_with_retries(
        product_id=product_id,
        side="BUY",
        quote_size_usd=buy_usd,
        attempts=_build_attempts(),
    )

    return {
        "ok": True,
        "product_id": product_id,
        "side": "BUY",
        "signal_type": signal_type,
        "requested_buy_usd": buy_usd,
        "result": result,
    }


def execute_rebalance_plan():
    snapshot = get_portfolio_snapshot()
    plan = get_rebalance_plan()

    execution_report = {
        "ok": True,
        "plan": plan,
        "trim_results": [],
        "buy_results": [],
    }

    for entry in plan["trims"]:
        product_id = entry["product_id"]
        trim_usd = float(entry["amount_usd"])
        res = execute_trim(product_id, trim_usd, snapshot)
        execution_report["trim_results"].append(res)

    snapshot = get_portfolio_snapshot()

    for entry in plan["buys"]:
        product_id = entry["product_id"]
        signal_type = entry.get("signal_type", "CORE_BUY_WINDOW")

        if signal_type == "CORE_BUY_WINDOW":
            buy_usd = allowed_core_buy_usd(product_id, snapshot)
        else:
            buy_usd = entry["amount_usd"]

        if buy_usd <= 0:
            execution_report["buy_results"].append({
                "ok": False,
                "product_id": product_id,
                "signal_type": signal_type,
                "reason": "buy_amount_zero_after_refresh",
            })
            continue

        res = execute_buy(product_id, buy_usd, signal_type=signal_type)
        execution_report["buy_results"].append(res)

    return execution_report


def execute_satellite_signal(product_id, signal_type="SATELLITE_BUY"):
    snapshot = get_portfolio_snapshot()
    asset_class = classify_asset(product_id, snapshot)

    if asset_class != "satellite_active":
        return {
            "ok": False,
            "product_id": product_id,
            "signal_type": signal_type,
            "reason": f"asset_class={asset_class}",
        }

    base_buy_usd = allowed_satellite_buy_usd(product_id, snapshot)
    if base_buy_usd <= 0:
        return {
            "ok": False,
            "product_id": product_id,
            "signal_type": signal_type,
            "reason": "no_allowed_satellite_buy_usd",
        }

    buy_usd = _scale_satellite_buy(signal_type, base_buy_usd)
    buy_usd = min(buy_usd, MAX_QUOTE_SIZE)

    return execute_buy(product_id, buy_usd, signal_type=signal_type)


def print_rebalance_plan(plan):
    print("📊 Rebalance plan\n")

    if plan["buys"]:
        print("Buy candidates:")
        for entry in plan["buys"]:
            print(
                f"{entry['product_id']}: buy ${entry['amount_usd']:.2f} "
                f"(weight {entry['current_weight']:.3f} -> target {entry['target_weight']:.3f})"
            )
    else:
        print("Buy candidates:\nNone")

    print("")

    if plan["trims"]:
        print("Trim candidates:")
        for entry in plan["trims"]:
            if "max_weight" in entry:
                print(
                    f"{entry['product_id']}: trim ${entry['amount_usd']:.2f} "
                    f"(weight {entry['current_weight']:.3f} -> max {entry['max_weight']:.3f})"
                )
            elif "target_weight" in entry:
                print(
                    f"{entry['product_id']}: trim ${entry['amount_usd']:.2f} "
                    f"(weight {entry['current_weight']:.3f} -> target {entry['target_weight']:.3f})"
                )
            else:
                print(
                    f"{entry['product_id']}: trim ${entry['amount_usd']:.2f} "
                    f"(weight {entry['current_weight']:.3f})"
                )
    else:
        print("Trim candidates:\nNone")

    print("")
    if plan["notes"]:
        for note in plan["notes"]:
            print(note)


if __name__ == "__main__":
    plan = get_rebalance_plan()
    print_rebalance_plan(plan)
