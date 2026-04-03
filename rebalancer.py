import os
import time
from dotenv import load_dotenv

from execution import (
    place_limit_quote_with_retries,
    place_limit_base_with_retries,
    get_quote_attempts,
    get_base_attempts,
)

from positions import compute_sell_base, compute_full_liquid_base

from portfolio import (
    get_portfolio_snapshot,
    portfolio_summary,
    allowed_satellite_buy_usd,
    allowed_core_buy_usd,
    required_trim_usd,
    get_profit_harvest_candidates,
)

from storage import record_buy_fill, record_sell_fill, mark_harvest

load_dotenv("/root/tradingbot/.env", override=True)

MAX_QUOTE_SIZE = float(os.getenv("MAX_QUOTE_PER_TRADE_USD", "25"))


def get_max_quote_size(snapshot=None):
    snapshot = snapshot or get_portfolio_snapshot()
    cfg = snapshot.get("config", {}) or {}
    return float(cfg.get("max_quote_per_trade_usd", MAX_QUOTE_SIZE) or MAX_QUOTE_SIZE)


def _extract_final_result(result_wrapper):
    if isinstance(result_wrapper, dict):
        return (result_wrapper or {}).get("final_result", {}) or {}
    return {}


def _extract_fill(result_wrapper):
    final_result = _extract_final_result(result_wrapper)
    filled_base = float(final_result.get("filled_base", 0.0) or 0.0)
    avg_fill_price = float(final_result.get("avg_fill_price", 0.0) or 0.0)
    return filled_base, avg_fill_price


def execute_satellite_signal(product_id, signal_type="SATELLITE_BUY"):
    signal_type = str(signal_type or "SATELLITE_BUY").upper().strip()
    snapshot = get_portfolio_snapshot()

    if signal_type == "CORE_BUY_WINDOW":
        buy_usd = min(
            allowed_core_buy_usd(product_id, snapshot),
            get_max_quote_size(snapshot),
        )
    else:
        buy_usd = min(
            allowed_satellite_buy_usd(product_id, snapshot),
            get_max_quote_size(snapshot),
        )

    if buy_usd <= 0:
        return {
            "ok": False,
            "reason": "buy_not_allowed",
            "product_id": product_id,
            "signal_type": signal_type,
        }

    return execute_buy(product_id, buy_usd, signal_type=signal_type)


def execute_buy(product_id, buy_usd, signal_type="SATELLITE_BUY", external_order_id=None):
    snapshot = get_portfolio_snapshot()

    buy_usd = min(float(buy_usd), get_max_quote_size(snapshot))

    if buy_usd <= 0:
        return {
            "ok": False,
            "product_id": product_id,
            "reason": "buy_usd_zero",
        }

    asset = snapshot.get("positions", {}).get(product_id, {})
    volatility_bucket = str(asset.get("volatility_bucket", "") or "")

    result = place_limit_quote_with_retries(
        product_id=product_id,
        side="BUY",
        quote_size_usd=buy_usd,
        attempts=get_quote_attempts(
            signal_type=signal_type,
            volatility_bucket=volatility_bucket,
        ),
        signal_type=signal_type,
        volatility_bucket=volatility_bucket,
    )

    final_result = _extract_final_result(result)
    filled_base, avg_fill_price = _extract_fill(result)

    if filled_base > 0 and avg_fill_price > 0:
        record_buy_fill(
            product_id=product_id,
            filled_base=filled_base,
            avg_fill_price=avg_fill_price,
            order_id=final_result.get("coinbase_order_id") or external_order_id,
            status=final_result.get("status", "FILLED"),
            created_at=int(time.time()),
        )

    return {
        "ok": True,
        "product_id": product_id,
        "side": "BUY",
        "signal_type": signal_type,
        "requested_buy_usd": buy_usd,
        "result": result,
    }


def execute_trim(product_id, trim_usd, snapshot, signal_type="SNIPER_EXIT", external_order_id=None):
    current_value = float(
        snapshot.get("positions", {}).get(product_id, {}).get("value_total_usd", 0.0) or 0.0
    )

    if current_value <= 0:
        return {
            "ok": False,
            "product_id": product_id,
            "reason": "no_position_value",
        }

    requested_sell_pct = min(1.0, trim_usd / current_value)

    base_size = (
        compute_full_liquid_base(product_id)
        if requested_sell_pct >= 1.0
        else compute_sell_base(product_id, requested_sell_pct)
    )

    if base_size <= 0:
        return {
            "ok": False,
            "product_id": product_id,
            "reason": "no_liquid_balance",
        }

    asset = snapshot.get("positions", {}).get(product_id, {})
    volatility_bucket = str(asset.get("volatility_bucket", "") or "")

    result = place_limit_base_with_retries(
        product_id=product_id,
        side="SELL",
        base_size=base_size,
        attempts=get_base_attempts(
            signal_type=signal_type,
            volatility_bucket=volatility_bucket,
        ),
        signal_type=signal_type,
        volatility_bucket=volatility_bucket,
    )

    final_result = _extract_final_result(result)
    filled_base, avg_fill_price = _extract_fill(result)

    if filled_base > 0:
        record_sell_fill(
            product_id=product_id,
            filled_base=filled_base,
            avg_fill_price=avg_fill_price,
            order_id=final_result.get("coinbase_order_id") or external_order_id,
            status=final_result.get("status", "FILLED"),
            created_at=int(time.time()),
        )

    return {
        "ok": True,
        "product_id": product_id,
        "side": "SELL",
        "requested_trim_usd": trim_usd,
        "result": result,
    }


def get_profit_harvest_plan(snapshot=None):
    snapshot = snapshot or get_portfolio_snapshot()

    cfg = snapshot.get("config", {}).get("profit_harvest", {})
    routes = cfg.get("routes", {"BTC-USD": 0.40, "ETH-USD": 0.40, "CASH": 0.20})

    harvests = []

    for item in get_profit_harvest_candidates(snapshot):
        route_preview = []

        for route_product, pct in routes.items():
            pct = float(pct or 0.0)

            route_preview.append({
                "product_id": route_product,
                "pct": pct,
                "amount_usd": float(item.get("amount_usd", 0.0) or 0.0) * pct,
            })

        harvests.append({
            **item,
            "routes": route_preview,
        })

    return {
        "ok": True,
        "harvests": harvests,
    }


def route_harvest_proceeds(amount_usd, snapshot):
    routes = snapshot.get("config", {}).get("profit_harvest", {}).get("routes", {})
    out = []

    for route_product, pct in routes.items():
        pct = float(pct or 0.0)

        if pct <= 0:
            continue

        route_usd = amount_usd * pct

        if route_product == "CASH":
            out.append({
                "ok": True,
                "product_id": "CASH",
                "requested_buy_usd": route_usd,
                "reason": "left_as_cash",
            })
            continue

        out.append(execute_buy(route_product, route_usd, signal_type="HARVEST_ROUTE"))

    return out


def execute_profit_harvest_plan():
    snapshot = get_portfolio_snapshot()
    plan = get_profit_harvest_plan(snapshot)

    report = {
        "ok": True,
        "harvest_results": [],
        "plan": plan,
    }

    for item in plan.get("harvests", []):
        trim_res = execute_trim(item["product_id"], item["amount_usd"], snapshot)

        if trim_res.get("ok"):
            mark_harvest(item["product_id"], int(time.time()))

        refreshed = get_portfolio_snapshot()
        route_res = route_harvest_proceeds(item["amount_usd"], refreshed)

        report["harvest_results"].append({
            "product_id": item["product_id"],
            "gain_pct": item.get("gain_pct"),
            "trim_pct": item.get("trim_pct"),
            "amount_usd": item["amount_usd"],
            "trim": trim_res,
            "routes": route_res,
        })

    return report


def get_rebalance_plan(snapshot=None, summary=None):
    snapshot = snapshot or get_portfolio_snapshot()
    summary = summary or portfolio_summary(snapshot)

    buys = []
    trims = []

    core_assets = ((snapshot.get("config") or {}).get("core_assets") or {}).keys()
    for product_id in core_assets:
        amount_usd = float(allowed_core_buy_usd(product_id, snapshot) or 0.0)
        if amount_usd > 0:
            buys.append({
                "product_id": product_id,
                "amount_usd": amount_usd,
                "signal_type": "CORE_BUY_WINDOW",
                "tier": "core",
            })

    for product_id, row in (summary.get("assets") or {}).items():
        trim_usd = float(required_trim_usd(product_id, snapshot) or 0.0)
        if trim_usd > 0:
            trims.append({
                "product_id": product_id,
                "amount_usd": trim_usd,
            })

    return {
        "ok": True,
        "buys": buys,
        "trims": trims,
        "harvests": [],
        "notes": [],
        "market_regime": summary.get("market_regime"),
        "total_value_usd": summary.get("total_value_usd"),
        "usd_cash": summary.get("usd_cash"),
        "core_weight": summary.get("core_weight"),
        "satellite_weight": summary.get("satellite_weight"),
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

    for entry in plan.get("trims", []):
        product_id = entry["product_id"]
        trim_usd = float(entry["amount_usd"])
        res = execute_trim(product_id, trim_usd, snapshot)
        execution_report["trim_results"].append(res)

    snapshot = get_portfolio_snapshot()

    for entry in plan.get("buys", []):
        product_id = entry["product_id"]
        signal_type = entry.get("signal_type", "CORE_BUY_WINDOW")

        if signal_type == "CORE_BUY_WINDOW":
            buy_usd = allowed_core_buy_usd(product_id, snapshot)
        else:
            buy_usd = float(entry["amount_usd"])

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

def dispatch_signal_action(
    product_id,
    action,
    signal_type,
    timeframe,
    strategy=None,
    price=None,
    order_id=None,
    trim_pct=None,
    quote_size=None,
):
    try:
        snapshot = get_portfolio_snapshot()
    except Exception as e:
        return {
            "ok": False,
            "reason": "snapshot_failed",
            "error": str(e),
            "product_id": product_id,
            "action": action,
            "signal_type": signal_type,
            "timeframe": timeframe,
        }

    trading_enabled = os.getenv("TRADING_ENABLED", "false").lower() == "true"

    if not trading_enabled:
        return {
            "ok": False,
            "reason": "trading_disabled",
            "product_id": product_id,
            "action": action,
            "signal_type": signal_type,
            "timeframe": timeframe,
        }

    if action not in {"BUY", "TRIM", "EXIT"}:
        return {
            "ok": False,
            "reason": "unsupported_action",
            "product_id": product_id,
            "action": action,
            "signal_type": signal_type,
        }

    if action == "BUY":
        core_buy_signals = {"CORE_BUY_WINDOW"}
        satellite_buy_signals = {
            "SATELLITE_BUY_EARLY",
            "SATELLITE_BUY",
            "SATELLITE_BUY_HEAVY",
            "SNIPER_BUY",
	    "APPROVED_REBALANCE_BUY",
        }

        if signal_type in core_buy_signals:
            buy_usd = min(
                allowed_core_buy_usd(product_id, snapshot),
                get_max_quote_size(snapshot),
            )
            asset_class = "core"

        elif signal_type in satellite_buy_signals:
            if str(signal_type).upper() == "APPROVED_REBALANCE_BUY":
                try:
                    requested_quote_usd = float(quote_size or 0.0)
                except Exception:
                    requested_quote_usd = 0.0

                buy_usd = min(
                    requested_quote_usd,
                    get_max_quote_size(snapshot),
                )
            else:
                buy_usd = min(
                    allowed_satellite_buy_usd(product_id, snapshot),
                    get_max_quote_size(snapshot),
                )

            asset_class = "satellite"

        else:
            return {
                "ok": False,
                "reason": "unsupported_signal_type",
                "product_id": product_id,
                "action": action,
                "signal_type": signal_type,
            }

        if buy_usd <= 0:
            return {
                "ok": False,
                "reason": "buy_not_allowed",
                "product_id": product_id,
                "action": action,
                "signal_type": signal_type,
                "asset_class": asset_class,
            }

        return execute_buy(
            product_id=product_id,
            buy_usd=buy_usd,
            signal_type=signal_type,
            external_order_id=order_id,
        )

    if action in {"TRIM", "EXIT"}:
        position_value = float(
            snapshot.get("positions", {}).get(product_id, {}).get("value_total_usd", 0.0) or 0.0
        )

        if position_value <= 0:
            return {
                "ok": False,
                "reason": "no_position_value",
                "product_id": product_id,
                "action": action,
                "signal_type": signal_type,
            }

        if action == "EXIT":
            trim_usd = position_value
        else:
            trim_pct = float(trim_pct if trim_pct is not None else 0.50)
            trim_pct = max(0.0, min(1.0, trim_pct))
            trim_usd = position_value * trim_pct

        if trim_usd <= 0:
            return {
                "ok": False,
                "reason": "trim_not_allowed",
                "product_id": product_id,
                "action": action,
                "signal_type": signal_type,
            }

        sell_signal_type = signal_type or ("EXIT" if action == "EXIT" else "SNIPER_EXIT")

        return execute_trim(
            product_id=product_id,
            trim_usd=trim_usd,
            snapshot=snapshot,
            signal_type=sell_signal_type,
            external_order_id=order_id,
        )

    return {
        "ok": False,
        "reason": "unsupported_action",
        "product_id": product_id,
        "action": action,
        "signal_type": signal_type,
    }
