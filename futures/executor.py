"""
futures/executor.py — Automated futures execution engine.
"""
from __future__ import annotations

import time
import threading
from typing import Any

from futures.futures_client import FuturesClient
from futures.scanner import FuturesScanner
from portfolio import get_portfolio_snapshot

_EXECUTOR_LOCK = threading.Lock()
_POSITION_COOLDOWNS: dict[str, float] = {}
_PENDING_CLOSES: set[str] = set()
_EXECUTOR_LOG: list[dict[str, Any]] = []

MAX_OPEN_POSITIONS = 3
MAX_BUYING_POWER_PCT = 0.25
MIN_SCORE = 70.0
MIN_BUYING_POWER = 20.0
COOLDOWN_SECONDS = 14400
STOP_LOSS_BUFFER = 1.002
TAKE_PROFIT_BUFFER = 0.998
LONG_ALLOWED_REGIMES = {"bull", "neutral", "caution"}
SHORT_ALLOWED_REGIMES = {"neutral", "caution", "risk_off"}


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[futures_executor] {msg}", flush=True)
    with _EXECUTOR_LOCK:
        _EXECUTOR_LOG.append({"ts": ts, "msg": msg})
        if len(_EXECUTOR_LOG) > 500:
            _EXECUTOR_LOG.pop(0)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _get_regime() -> str:
    try:
        snapshot = get_portfolio_snapshot()
        return str(
            snapshot.get("market_regime")
            or snapshot.get("config", {}).get("market_regime")
            or "neutral"
        ).lower().strip()
    except Exception:
        return "neutral"


def _in_cooldown(product_id: str) -> bool:
    last = _POSITION_COOLDOWNS.get(product_id, 0.0)
    return (time.time() - last) < COOLDOWN_SECONDS


def _set_cooldown(product_id: str) -> None:
    with _EXECUTOR_LOCK:
        _POSITION_COOLDOWNS[product_id] = time.time()


def _size_order(buying_power: float, conviction: float, leverage: int) -> float:
    alloc = buying_power * MAX_BUYING_POWER_PCT * min(conviction, 1.0)
    return max(1.0, round(alloc / max(leverage, 1)))


def run_futures_scan_and_execute() -> dict[str, Any]:
    client = FuturesClient()
    scanner = FuturesScanner()

    balance = client.get_balance_summary()
    buying_power = _safe_float(balance.get("buying_power"))
    if buying_power < MIN_BUYING_POWER:
        _log(f"scan skipped: buying_power=${buying_power:.2f} below minimum ${MIN_BUYING_POWER}")
        return {"ok": True, "skipped": True, "reason": "insufficient_buying_power", "buying_power": buying_power}

    open_positions = client.get_positions()
    open_count = len(open_positions)
    open_products = {str(p.get("product_id", "")).upper() for p in open_positions}

    if open_count >= MAX_OPEN_POSITIONS:
        _log(f"scan skipped: {open_count} open positions >= max {MAX_OPEN_POSITIONS}")
        return {"ok": True, "skipped": True, "reason": "max_positions_reached", "open_count": open_count}

    regime = _get_regime()
    _log(f"scan starting regime={regime} open={open_count}/{MAX_OPEN_POSITIONS} buying_power=${buying_power:.2f}")

    try:
        opportunities = scanner.scan_perps(regime=regime)
    except Exception as exc:
        _log(f"scanner failed: {exc}")
        return {"ok": False, "error": str(exc)}

    executed = []
    slots_remaining = MAX_OPEN_POSITIONS - open_count

    for opp in opportunities:
        if slots_remaining <= 0:
            break

        product_id = str(opp.get("product_id") or "").upper()
        direction = str(opp.get("direction") or "long").lower()
        score = _safe_float(opp.get("score"))
        conviction = _safe_float(opp.get("conviction"), 0.5)
        leverage = max(1, int(opp.get("leverage_suggested") or 1))

        if score < MIN_SCORE:
            continue
        if product_id in open_products:
            _log(f"skip {product_id}: already have open position")
            continue
        if _in_cooldown(product_id):
            _log(f"skip {product_id}: in cooldown")
            continue
        if direction == "long" and regime not in LONG_ALLOWED_REGIMES:
            _log(f"skip {product_id} long: regime={regime} blocks longs")
            continue
        if direction == "short" and regime not in SHORT_ALLOWED_REGIMES:
            _log(f"skip {product_id} short: regime={regime} blocks shorts")
            continue

        side = "BUY" if direction == "long" else "SELL"
        size = _size_order(buying_power, conviction, leverage)

        _log(
            f"executing {direction} {product_id} score={score:.1f} "
            f"conviction={conviction:.2f} leverage={leverage}x size={size} "
            f"entry={opp.get('entry_price')} stop={opp.get('stop_loss')} "
            f"target={opp.get('take_profit')} rr={opp.get('risk_reward')}"
        )

        result = client.place_order(
            product_id=product_id,
            side=side,
            size=size,
            order_type="market",
            leverage=leverage,
        )

        if result.get("ok"):
            _set_cooldown(product_id)
            open_products.add(product_id)
            slots_remaining -= 1
            executed.append({
                "product_id": product_id,
                "direction": direction,
                "score": score,
                "size": size,
                "leverage": leverage,
                "order_id": result.get("order_id"),
                "entry_price": opp.get("entry_price"),
                "stop_loss": opp.get("stop_loss"),
                "take_profit": opp.get("take_profit"),
                "risk_reward": opp.get("risk_reward"),
            })
            _log(f"order placed {product_id} order_id={result.get('order_id')}")
        else:
            _log(f"order failed {product_id}: {result.get('error')}")

    _log(f"scan complete opportunities={len(opportunities)} executed={len(executed)}")
    return {
        "ok": True,
        "regime": regime,
        "buying_power": buying_power,
        "opportunities_scanned": len(opportunities),
        "executed": executed,
        "open_positions_before": open_count,
    }


def run_futures_position_monitor() -> dict[str, Any]:
    client = FuturesClient()
    positions = client.get_positions()

    if not positions:
        return {"ok": True, "actions": []}

    products = {p["product_id"]: p for p in client.get_all_products()}
    actions = []

    for pos in positions:
        product_id = str(pos.get("product_id") or "").upper()
        side = str(pos.get("side") or "long").lower()
        entry_price = _safe_float(pos.get("entry_price"))
        mark_price = _safe_float(
            pos.get("mark_price")
            or (products.get(product_id) or {}).get("price")
        )
        unrealized_pnl = _safe_float(pos.get("unrealized_pnl"))
        size = _safe_float(pos.get("size"))

        if mark_price <= 0 or entry_price <= 0 or size <= 0:
            continue

        # Use scanner-suggested levels if available, else fall back to defaults
        scanner_stop = _safe_float(pos.get("stop_loss"))
        scanner_target = _safe_float(pos.get("take_profit"))

        if side == "long":
            stop_price = scanner_stop if scanner_stop > 0 else entry_price * 0.97
            target_price = scanner_target if scanner_target > 0 else entry_price * 1.05
            hit_stop = mark_price <= stop_price * STOP_LOSS_BUFFER
            hit_target = mark_price >= target_price * TAKE_PROFIT_BUFFER
        else:
            stop_price = scanner_stop if scanner_stop > 0 else entry_price * 1.03
            target_price = scanner_target if scanner_target > 0 else entry_price * 0.95
            hit_stop = mark_price >= stop_price * (2 - STOP_LOSS_BUFFER)
            hit_target = mark_price <= target_price * (2 - TAKE_PROFIT_BUFFER)

        reason = None
        if hit_stop:
            reason = "stop_loss"
        elif hit_target:
            reason = "take_profit"

        if reason:
            with _EXECUTOR_LOCK:
                if product_id in _PENDING_CLOSES:
                    _log(f"skip {product_id}: close already pending")
                    continue
                _PENDING_CLOSES.add(product_id)
            try:
                _log(
                    f"closing {side} {product_id} reason={reason} "
                    f"entry={entry_price:.4f} mark={mark_price:.4f} pnl={unrealized_pnl:.2f}"
                )
                result = client.close_position(product_id)
                if result.get("ok"):
                    _set_cooldown(product_id)
                    actions.append({
                        "product_id": product_id,
                        "side": side,
                        "reason": reason,
                        "entry_price": entry_price,
                        "exit_price": mark_price,
                        "pnl": unrealized_pnl,
                        "order_id": result.get("order_id"),
                    })
                    _log(f"closed {product_id} order_id={result.get('order_id')}")
                else:
                    _log(f"close failed {product_id}: {result.get('error')}")
            finally:
                with _EXECUTOR_LOCK:
                    _PENDING_CLOSES.discard(product_id)

    if actions:
        _log(f"monitor closed {len(actions)} position(s)")

    return {"ok": True, "actions": actions, "positions_checked": len(positions)}


def get_executor_log(limit: int = 50) -> list[dict[str, Any]]:
    with _EXECUTOR_LOCK:
        return list(reversed(_EXECUTOR_LOG[-int(limit):]))
