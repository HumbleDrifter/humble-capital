import json
import os
import threading
import time
from dotenv import load_dotenv

from execution import (
    place_limit_quote_with_retries,
    place_limit_base_with_retries,
    get_quote_attempts,
    get_base_attempts,
)
from decision_trace import infer_asset_state, record_decision_trace
from position_sizing import compute_risk_adjusted_size
from trailing_exit import evaluate_all_exits, clear_tracking

from positions import compute_sell_base, compute_full_liquid_base

from portfolio import (
    get_portfolio_snapshot,
    portfolio_summary,
    allowed_satellite_buy_usd,
    allowed_core_buy_usd,
    required_trim_usd,
    get_profit_harvest_candidates,
    get_min_cash_reserve_usd,
    get_free_cash_after_reserve,
    get_deployable_cash_buckets,
    get_asset_value,
    get_core_shortfall_usd,
    core_is_underweight,
    get_satellite_allocation_budget_usd,
    get_satellite_max_weight,
    get_satellite_volatility_info,
    get_sniper_buy_eligibility_detail,
    is_satellite_buy_eligible,
)

from storage import record_buy_fill, record_sell_fill, mark_harvest

load_dotenv("/root/tradingbot/.env", override=True)

MAX_QUOTE_SIZE = float(os.getenv("MAX_QUOTE_PER_TRADE_USD", "25"))
_SELL_LOCKS = {}
_SELL_LOCKS_LOCK = threading.Lock()


def _log_rebalancer_event(event, payload=None):
    envelope = {
        "ts": int(time.time()),
        "component": "rebalancer",
        "event": str(event or "").strip() or "unknown",
        "payload": payload if isinstance(payload, dict) else {},
    }
    print(json.dumps(envelope, sort_keys=True, ensure_ascii=False))


def _get_sell_lock(product_id):
    with _SELL_LOCKS_LOCK:
        if product_id not in _SELL_LOCKS:
            _SELL_LOCKS[product_id] = threading.Lock()
        return _SELL_LOCKS[product_id]


def _record_buy_trace(snapshot, result_category, reason_code, summary, **payload):
    record_decision_trace(
        {
            "product_id": payload.get("product_id"),
            "action": "BUY",
            "signal_type": payload.get("signal_type"),
            "strategy": payload.get("strategy"),
            "timeframe": payload.get("timeframe"),
            "asset_state": infer_asset_state(snapshot, payload.get("product_id")),
            "is_valid_product": True,
            "result_category": result_category,
            "reason_code": reason_code,
            "summary": summary,
            **payload,
        }
    )


def _derive_buy_block_reason(decision_context, signal_type):
    sniper_reason = str(decision_context.get("sniper_eligibility_reason") or "").strip()
    if str(signal_type or "").upper().strip() == "SNIPER_BUY" and sniper_reason and sniper_reason != "sniper_eligible":
        return sniper_reason
    if float(decision_context.get("free_cash_after_reserve_usd", 0.0) or 0.0) <= 0:
        return "no_free_cash_after_reserve"
    if float(decision_context.get("satellite_budget_usd", 0.0) or 0.0) <= 0 and str(decision_context.get("asset_class") or "") == "satellite":
        return "satellite_budget_exhausted"
    if float(decision_context.get("requested_buy_usd", 0.0) or 0.0) > 0 and float(decision_context.get("requested_buy_usd", 0.0) or 0.0) < float(decision_context.get("trade_min_value_usd", 0.0) or 0.0):
        return "trade_below_minimum"
    if float(decision_context.get("drawdown", 0.0) or 0.0) >= float(decision_context.get("freeze_level", 1.0) or 1.0):
        return "drawdown_freeze"
    return "buy_not_allowed"


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


def _common_buy_context(product_id, signal_type, snapshot):
    budgets = get_deployable_cash_buckets(snapshot)
    return {
        "product_id": product_id,
        "signal_type": signal_type,
        "total_value_usd": float(snapshot.get("total_value_usd", 0.0) or 0.0),
        "usd_cash": float(snapshot.get("usd_cash", 0.0) or 0.0),
        "min_cash_reserve_usd": float(get_min_cash_reserve_usd(snapshot) or 0.0),
        "free_cash_after_reserve_usd": float(get_free_cash_after_reserve(snapshot) or 0.0),
        "core_budget_usd": float(budgets.get("core_budget_usd", 0.0) or 0.0),
        "satellite_budget_usd": float(budgets.get("satellite_budget_usd", 0.0) or 0.0),
        "regime": str(budgets.get("regime", "") or ""),
        "max_quote_per_trade_usd": float(get_max_quote_size(snapshot) or 0.0),
        "trade_min_value_usd": float(snapshot.get("config", {}).get("trade_min_value_usd", 10.0) or 10.0),
    }


def _core_buy_context(product_id, signal_type, snapshot):
    payload = _common_buy_context(product_id, signal_type, snapshot)
    payload.update(
        {
            "asset_class": "core",
            "current_value_usd": float(get_asset_value(snapshot, product_id) or 0.0),
            "shortfall_usd": float(get_core_shortfall_usd(product_id, snapshot) or 0.0),
            "underweight": bool(core_is_underweight(product_id, snapshot)),
            "allowed_buy_usd": float(allowed_core_buy_usd(product_id, snapshot) or 0.0),
        }
    )
    return payload


def _satellite_buy_context(product_id, signal_type, snapshot):
    payload = _common_buy_context(product_id, signal_type, snapshot)
    summary = portfolio_summary(snapshot)
    regime = str(summary.get("market_regime", payload.get("regime", "neutral")) or "neutral").lower()
    total_value = float(snapshot.get("total_value_usd", 0.0) or 0.0)
    current_satellite_value = float(snapshot.get("satellite_value_usd", 0.0) or 0.0)
    current_asset_value = float(get_asset_value(snapshot, product_id) or 0.0)
    regime_caps = snapshot.get("config", {}).get("regime_satellite_caps", {}) or {}
    satellite_total_max = float(regime_caps.get(regime, snapshot.get("config", {}).get("satellite_total_max", 0.50)) or 0.50)
    satellite_budget_usd = float(get_satellite_allocation_budget_usd(snapshot) or 0.0)
    allowed_total_satellite_value = total_value * satellite_total_max
    satellite_headroom_usd = max(0.0, allowed_total_satellite_value - current_satellite_value)
    per_asset_max_value = total_value * float(get_satellite_max_weight(product_id, snapshot) or 0.0)
    asset_headroom_usd = max(0.0, per_asset_max_value - current_asset_value)
    raw_allowed_usd = max(0.0, min(satellite_budget_usd, satellite_headroom_usd, asset_headroom_usd))
    vol_info = get_satellite_volatility_info(product_id, snapshot) or {}
    adjusted_allowed_usd = max(0.0, raw_allowed_usd * float(vol_info.get("volatility_multiplier", 1.0) or 1.0))
    sniper_cfg = snapshot.get("config", {}).get("sniper_mode", {}) or {}
    sniper_detail = None
    if str(signal_type or "").upper().strip() == "SNIPER_BUY":
        sniper_detail = get_sniper_buy_eligibility_detail(product_id, snapshot)
    if str(signal_type or "").upper().strip() == "SNIPER_BUY" and bool(sniper_detail and sniper_detail.get("ok")):
        adjusted_allowed_usd *= float(sniper_cfg.get("buy_scale", 0.35) or 0.35)

    payload.update(
        {
            "asset_class": "satellite",
            "eligible": bool(is_satellite_buy_eligible(product_id, snapshot)),
            "market_regime": regime,
            "drawdown": float(snapshot.get("portfolio_drawdown", 0.0) or 0.0),
            "freeze_level": float(snapshot.get("config", {}).get("drawdown_controls", {}).get("freeze_level", 1.0) or 1.0),
            "current_satellite_value_usd": current_satellite_value,
            "current_asset_value_usd": current_asset_value,
            "satellite_total_max": satellite_total_max,
            "allowed_total_satellite_value_usd": allowed_total_satellite_value,
            "satellite_headroom_usd": satellite_headroom_usd,
            "per_asset_max_value_usd": per_asset_max_value,
            "asset_headroom_usd": asset_headroom_usd,
            "volatility_bucket": str(vol_info.get("bucket", "") or ""),
            "volatility_multiplier": float(vol_info.get("volatility_multiplier", 1.0) or 1.0),
            "raw_allowed_usd": raw_allowed_usd,
            "adjusted_allowed_usd": adjusted_allowed_usd,
            "allowed_buy_usd": float(allowed_satellite_buy_usd(product_id, snapshot, signal_type=signal_type) or 0.0),
            "sniper_eligibility_reason": str((sniper_detail or {}).get("reason", "") or ""),
            "sniper_relaxed": bool((sniper_detail or {}).get("relax_require_sniper_eligible")),
            "score": (sniper_detail or {}).get("score"),
            "threshold": (sniper_detail or {}).get("min_score"),
            "pump_protected": (sniper_detail or {}).get("pump_protected"),
            "sniper_eligible": bool((sniper_detail or {}).get("ok")) if isinstance(sniper_detail, dict) else None,
        }
    )
    return payload


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
        _record_buy_trace(
            snapshot,
            "blocked",
            "buy_usd_zero",
            f"{product_id} — Blocked — Quote size resolved to zero",
            product_id=product_id,
            signal_type=signal_type,
            requested_buy_usd=buy_usd,
            max_quote_per_trade_usd=get_max_quote_size(snapshot),
            free_cash_after_reserve_usd=float(get_free_cash_after_reserve(snapshot) or 0.0),
        )
        return {
            "ok": False,
            "product_id": product_id,
            "reason": "buy_usd_zero",
        }

    asset = snapshot.get("positions", {}).get(product_id, {})
    volatility_bucket = str(asset.get("volatility_bucket", "") or "")
    _log_rebalancer_event(
        "execution_attempt_started",
        {
            "product_id": product_id,
            "side": "BUY",
            "signal_type": signal_type,
            "requested_buy_usd": buy_usd,
            "volatility_bucket": volatility_bucket,
        },
    )

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
    _log_rebalancer_event(
        "execution_result_returned",
        {
            "product_id": product_id,
            "side": "BUY",
            "signal_type": signal_type,
            "requested_buy_usd": buy_usd,
            "wrapper_ok": bool((result or {}).get("ok")),
            "filled": bool((result or {}).get("filled")),
            "filled_base": filled_base,
            "avg_fill_price": avg_fill_price,
            "status": str(final_result.get("status", "") or ""),
            "error": str(final_result.get("error", "") or ""),
        },
    )
    if bool((result or {}).get("filled")) and filled_base > 0 and avg_fill_price > 0:
        _record_buy_trace(
            snapshot,
            "bought",
            "filled",
            f"{product_id} — Bought — Core buy filled" if str(signal_type or "").upper().strip() == "CORE_BUY_WINDOW" else f"{product_id} — Bought — Buy filled",
            product_id=product_id,
            signal_type=signal_type,
            requested_buy_usd=buy_usd,
            allowed_buy_usd=buy_usd,
            max_quote_per_trade_usd=get_max_quote_size(snapshot),
            free_cash_after_reserve_usd=float(get_free_cash_after_reserve(snapshot) or 0.0),
        )
    else:
        failure_reason = str(final_result.get("error") or final_result.get("status") or "execution_failed").strip() or "execution_failed"
        _record_buy_trace(
            snapshot,
            "execution_failed",
            failure_reason,
            f"{product_id} — Execution Failed — {failure_reason}",
            product_id=product_id,
            signal_type=signal_type,
            requested_buy_usd=buy_usd,
            allowed_buy_usd=buy_usd,
            max_quote_per_trade_usd=get_max_quote_size(snapshot),
            free_cash_after_reserve_usd=float(get_free_cash_after_reserve(snapshot) or 0.0),
        )

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
        "ok": bool((result or {}).get("ok", False)),
        "product_id": product_id,
        "side": "BUY",
        "signal_type": signal_type,
        "requested_buy_usd": buy_usd,
        "filled": bool((result or {}).get("filled")),
        "status": str(final_result.get("status", "") or ""),
        "error": str(final_result.get("error", "") or ""),
        "result": result,
    }


def execute_trim(product_id, trim_usd, snapshot, signal_type="SNIPER_EXIT", external_order_id=None):
    sell_lock = _get_sell_lock(product_id)
    if not sell_lock.acquire(blocking=False):
        _log_rebalancer_event(
            "sell_conflict_skipped",
            {
                "product_id": product_id,
                "signal_type": signal_type,
                "reason": "sell_in_progress",
                "conflicting_lock_holder": "existing_sell_lock",
            },
        )
        return {
            "ok": False,
            "product_id": product_id,
            "reason": "sell_in_progress",
        }

    try:
        current_snapshot = get_portfolio_snapshot()
        current_value = float(
            current_snapshot.get("positions", {}).get(product_id, {}).get("value_total_usd", 0.0) or 0.0
        )

        if current_value <= 0:
            _log_rebalancer_event(
                "sell_conflict_skipped",
                {
                    "product_id": product_id,
                    "signal_type": signal_type,
                    "reason": "position_already_closed",
                    "conflicting_lock_holder": "",
                },
            )
            return {
                "ok": False,
                "product_id": product_id,
                "reason": "position_already_closed",
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

        asset = current_snapshot.get("positions", {}).get(product_id, {})
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
    finally:
        sell_lock.release()


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
    conviction_score=None,
):
    try:
        snapshot = get_portfolio_snapshot()
    except Exception as e:
        if str(action or "").upper().strip() == "BUY":
            _record_buy_trace(
                {},
                "execution_failed",
                "snapshot_failed",
                f"{product_id} — Execution Failed — Portfolio snapshot unavailable",
                product_id=product_id,
                signal_type=signal_type,
                strategy=strategy,
                timeframe=timeframe,
            )
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
        if action == "BUY":
            _record_buy_trace(
                snapshot,
                "blocked",
                "trading_disabled",
                f"{product_id} — Blocked — Trading is disabled",
                product_id=product_id,
                signal_type=signal_type,
                strategy=strategy,
                timeframe=timeframe,
            )
        return {
            "ok": False,
            "reason": "trading_disabled",
            "product_id": product_id,
            "action": action,
            "signal_type": signal_type,
            "timeframe": timeframe,
        }

    if action not in {"BUY", "TRIM", "EXIT"}:
        if str(action or "").upper().strip() == "BUY":
            _record_buy_trace(
                snapshot,
                "invalid",
                "unsupported_action",
                f"{product_id} — Invalid — Unsupported action",
                product_id=product_id,
                signal_type=signal_type,
                strategy=strategy,
                timeframe=timeframe,
            )
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
            decision_context = _core_buy_context(product_id, signal_type, snapshot)
            buy_usd = min(
                float(decision_context.get("allowed_buy_usd", 0.0) or 0.0),
                get_max_quote_size(snapshot),
            )
            asset_class = "core"

        elif signal_type in satellite_buy_signals:
            if str(signal_type).upper() == "APPROVED_REBALANCE_BUY":
                try:
                    requested_quote_usd = float(quote_size or 0.0)
                except Exception:
                    requested_quote_usd = 0.0

                decision_context = _common_buy_context(product_id, signal_type, snapshot)
                decision_context.update(
                    {
                        "asset_class": "satellite",
                        "requested_quote_usd": requested_quote_usd,
                        "allowed_buy_usd": requested_quote_usd,
                    }
                )
                buy_usd = min(
                    requested_quote_usd,
                    get_max_quote_size(snapshot),
                )
            else:
                decision_context = _satellite_buy_context(product_id, signal_type, snapshot)
                buy_usd = min(
                    float(decision_context.get("allowed_buy_usd", 0.0) or 0.0),
                    get_max_quote_size(snapshot),
                )

            asset_class = "satellite"

        else:
            _record_buy_trace(
                snapshot,
                "invalid",
                "unsupported_signal_type",
                f"{product_id} — Invalid — Unsupported buy signal type",
                product_id=product_id,
                signal_type=signal_type,
                strategy=strategy,
                timeframe=timeframe,
            )
            return {
                "ok": False,
                "reason": "unsupported_signal_type",
                "product_id": product_id,
                "action": action,
                "signal_type": signal_type,
            }

        decision_context["requested_buy_usd"] = buy_usd
        _log_rebalancer_event("buy_decision", decision_context)

        try:
            regime = str(
                decision_context.get("market_regime")
                or decision_context.get("regime")
                or "neutral"
            )
            sizing_result = compute_risk_adjusted_size(
                product_id=product_id,
                base_size_usd=buy_usd,
                signal_type=signal_type,
                regime=regime,
                conviction_score=1.0 if conviction_score is None else conviction_score,
            )
            buy_usd = float(sizing_result.get("adjusted_size_usd", buy_usd) or buy_usd)
            decision_context["risk_adjusted_size"] = sizing_result
            decision_context["requested_buy_usd"] = buy_usd
        except Exception as exc:
            _log_rebalancer_event(
                "risk_adjusted_sizing_failed",
                {
                    "product_id": product_id,
                    "signal_type": signal_type,
                    "error": str(exc),
                    "base_size_usd": buy_usd,
                },
            )

        if str(signal_type or "").upper().strip() == "SNIPER_BUY" and str(decision_context.get("sniper_eligibility_reason") or "").strip() not in {"", "sniper_eligible"}:
            _log_rebalancer_event(
                "sniper_eligibility_blocked",
                {
                    "product_id": product_id,
                    "signal_type": signal_type,
                    "reason": str(decision_context.get("sniper_eligibility_reason") or "").strip() or "unknown",
                    "sniper_relaxed": bool(decision_context.get("sniper_relaxed")),
                },
            )

        if buy_usd <= 0:
            block_reason = _derive_buy_block_reason(decision_context, signal_type)
            _log_rebalancer_event(
                "buy_blocked",
                {
                    **decision_context,
                    "reason": block_reason,
                },
            )
            _record_buy_trace(
                snapshot,
                "blocked",
                block_reason,
                "",
                product_id=product_id,
                signal_type=signal_type,
                strategy=strategy,
                timeframe=timeframe,
                allowed_buy_usd=decision_context.get("allowed_buy_usd"),
                requested_buy_usd=decision_context.get("requested_buy_usd"),
                max_quote_per_trade_usd=decision_context.get("max_quote_per_trade_usd"),
                free_cash_after_reserve_usd=decision_context.get("free_cash_after_reserve_usd"),
                tradability_reason=decision_context.get("sniper_eligibility_reason"),
                sniper_eligible=decision_context.get("sniper_eligible"),
                score=decision_context.get("score"),
                threshold=decision_context.get("threshold"),
                pump_protected=decision_context.get("pump_protected"),
                market_regime=decision_context.get("market_regime") or decision_context.get("regime"),
            )
            return {
                "ok": False,
                "reason": "buy_not_allowed",
                "product_id": product_id,
                "action": action,
                "signal_type": signal_type,
                "asset_class": asset_class,
            }

        _log_rebalancer_event(
            "buy_allowed",
            {
                **decision_context,
                "asset_class": asset_class,
            },
        )
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


def run_trailing_exit_sweep():
    """Called periodically to check for trailing stop and stale position exits."""
    try:
        snapshot = get_portfolio_snapshot()
        exits = evaluate_all_exits(snapshot)

        results = []
        for exit_signal in exits:
            product_id = exit_signal["product_id"]
            reason = exit_signal["reason"]

            _log_rebalancer_event("trailing_exit_triggered", {
                "product_id": product_id,
                "reason": reason,
                "details": exit_signal.get("details", {}),
            })

            position_value = float(
                snapshot.get("positions", {}).get(product_id, {}).get("value_total_usd", 0.0) or 0.0
            )

            if position_value <= 0:
                continue

            result = execute_trim(
                product_id=product_id,
                trim_usd=position_value,
                snapshot=snapshot,
                signal_type=f"AUTO_{reason.upper()}",
                external_order_id=None,
            )

            if result.get("ok"):
                clear_tracking(product_id)

            results.append({
                "product_id": product_id,
                "reason": reason,
                "result": result,
            })

        return {"ok": True, "exits": results}
    except Exception as exc:
        _log_rebalancer_event("trailing_exit_sweep_failed", {"error": str(exc)})
        return {"ok": False, "error": str(exc), "exits": []}
