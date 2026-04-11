"""
options/executor.py

Automated options execution engine.
- Hourly scan at :17 (offset: crypto :02, futures :32, options :17)
- Regular plays: Wheel / CSP / Covered Call based on VIX + regime
- Meme squeeze plays: uncapped long calls when squeeze conditions confirmed
- Executes via WebullAdapter
- Position monitor called every 5 min by _trailing_exit_loop
"""
from __future__ import annotations

import os
import time
import threading
from typing import Any

from brokers.webull_adapter import WebullAdapter
from options.screener import (
    OptionsScreener,
    allocate_options_capital,
    allocate_meme_squeeze_capital,
    is_meme_squeeze,
    MEME_STOCKS,
)
from options.sentiment import SocialSentimentScanner
from portfolio import get_portfolio_snapshot

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_EXECUTOR_LOCK = threading.Lock()
_SYMBOL_COOLDOWNS: dict[str, float] = {}    # symbol -> last_entry_ts
_EXECUTOR_LOG: list[dict[str, Any]] = []

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MIN_SCORE = 72.0                # minimum screener score to execute
MIN_SQUEEZE_SCORE = 65.0        # lower bar for confirmed squeeze plays
MIN_CASH = 200.0                # minimum Webull cash to place any trade
COOLDOWN_SECONDS = 14400        # 4 hours between re-entries on same symbol
MAX_REGULAR_POSITIONS = 5       # max concurrent regular options positions
MAX_SQUEEZE_POSITIONS = 2       # max concurrent squeeze/meme positions

# Regimes that allow premium-selling (wheel, CSP, CC)
INCOME_ALLOWED_REGIMES = {"bull", "neutral", "caution"}
# Regimes that allow squeeze calls
SQUEEZE_ALLOWED_REGIMES = {"bull", "neutral"}
# Regimes that block all new options entries
BLOCKED_REGIMES = {"risk_off"}

VIX_DEFAULT = 20.0              # fallback VIX if unavailable


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[options_executor] {msg}", flush=True)
    with _EXECUTOR_LOCK:
        _EXECUTOR_LOG.append({"ts": ts, "msg": msg})
        if len(_EXECUTOR_LOG) > 500:
            _EXECUTOR_LOG.pop(0)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return int(default)


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


def _in_cooldown(symbol: str) -> bool:
    last = _SYMBOL_COOLDOWNS.get(symbol.upper(), 0.0)
    return (time.time() - last) < COOLDOWN_SECONDS


def _set_cooldown(symbol: str) -> None:
    with _EXECUTOR_LOCK:
        _SYMBOL_COOLDOWNS[symbol.upper()] = time.time()


def _get_webull_cash() -> float:
    try:
        adapter = WebullAdapter()
        balance = adapter.get_balance() or {}
        return _safe_float(
            balance.get("cash")
            or balance.get("available_cash")
            or balance.get("buying_power")
            or balance.get("cash_balance"),
            0.0,
        )
    except Exception:
        return 0.0


def _get_open_options_positions() -> list[dict[str, Any]]:
    try:
        adapter = WebullAdapter()
        positions = adapter.get_positions() or []
        return [p for p in positions if str(p.get("asset_type") or "").lower() == "option"]
    except Exception:
        return []


def _count_squeeze_positions(positions: list[dict[str, Any]]) -> int:
    return sum(
        1 for p in positions
        if str(p.get("symbol") or "").upper() in MEME_STOCKS
    )


def _place_options_order(
    adapter: WebullAdapter,
    opp: dict[str, Any],
    qty: int,
    order_type: str = "MKT",
) -> dict[str, Any]:
    """
    Place an options order via WebullAdapter.
    Maps screener opportunity fields to order payload.
    """
    strategy = str(opp.get("strategy") or "").lower()
    symbol = str(opp.get("symbol") or "").upper()
    strike = _safe_float(opp.get("strike"))
    expiration = str(opp.get("expiration") or "")
    mid = _safe_float(opp.get("mid") or opp.get("ask") or opp.get("bid"))

    # Map strategy to option type and side
    strategy_map = {
        "covered_call":      {"option_type": "call", "side": "sell"},
        "cash_secured_put":  {"option_type": "put",  "side": "sell"},
        "bull_put":          {"option_type": "put",  "side": "sell"},
        "bear_call":         {"option_type": "call", "side": "sell"},
        "wheel":             {"option_type": "put",  "side": "sell"},
        "meme_call":         {"option_type": "call", "side": "buy"},
        "long_call":         {"option_type": "call", "side": "buy"},
        "earnings_strangle": {"option_type": "call", "side": "buy"},
    }
    mapped = strategy_map.get(strategy, {"option_type": "call", "side": "buy"})

    order = {
        "underlying": symbol,
        "expiration": expiration,
        "strike": strike,
        "option_type": mapped["option_type"],
        "side": mapped["side"],
        "qty": max(1, int(qty)),
        "order_type": order_type,
        "limit_price": round(mid, 2) if order_type == "LMT" and mid > 0 else None,
    }

    try:
        result = adapter.place_options_order(order)
        return result or {"ok": False, "error": "no_response"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Core execution
# ---------------------------------------------------------------------------

def run_options_scan_and_execute() -> dict[str, Any]:
    """
    Called hourly at :17. Scans options universe and executes
    high-conviction income plays and meme squeeze calls.
    """
    cash = _get_webull_cash()
    if cash < MIN_CASH:
        _log(f"scan skipped: cash=${cash:.2f} below minimum ${MIN_CASH}")
        return {"ok": True, "skipped": True, "reason": "insufficient_cash", "cash": cash}

    regime = _get_regime()
    if regime in BLOCKED_REGIMES:
        _log(f"scan skipped: regime={regime} blocks all new options entries")
        return {"ok": True, "skipped": True, "reason": f"regime_blocked_{regime}"}

    open_positions = _get_open_options_positions()
    open_symbols = {str(p.get("symbol") or "").upper() for p in open_positions}
    regular_count = len(open_positions) - _count_squeeze_positions(open_positions)
    squeeze_count = _count_squeeze_positions(open_positions)

    _log(
        f"scan starting regime={regime} cash=${cash:.2f} "
        f"open_regular={regular_count}/{MAX_REGULAR_POSITIONS} "
        f"open_squeeze={squeeze_count}/{MAX_SQUEEZE_POSITIONS}"
    )

    # Get VIX approximation from snapshot
    try:
        snapshot = get_portfolio_snapshot()
        vix = _safe_float(snapshot.get("vix") or snapshot.get("config", {}).get("vix"), VIX_DEFAULT)
    except Exception:
        vix = VIX_DEFAULT

    capital_alloc = allocate_options_capital(vix)
    adapter = WebullAdapter()
    executed_regular = []
    executed_squeeze = []

    # --- REGULAR INCOME PLAYS ---
    if regime in INCOME_ALLOWED_REGIMES and regular_count < MAX_REGULAR_POSITIONS:
        try:
            screener = OptionsScreener()
            scan_result = screener.scan_universe(
                strategies=["covered_calls", "cash_secured_puts", "wheel", "earnings_strangles"]
            )
            opportunities = scan_result.get("opportunities", [])
            _log(f"regular scan found {len(opportunities)} opportunities")

            for opp in opportunities:
                if regular_count >= MAX_REGULAR_POSITIONS:
                    break

                symbol = str(opp.get("symbol") or "").upper()
                score = _safe_float(opp.get("score"))
                strategy = str(opp.get("strategy") or "").lower()

                if score < MIN_SCORE:
                    continue
                if symbol in open_symbols:
                    _log(f"skip {symbol}: already have open position")
                    continue
                if _in_cooldown(symbol):
                    _log(f"skip {symbol}: in cooldown")
                    continue

                # Size the trade
                strategy_key = strategy.replace("_opportunities", "").replace("cash_secured_", "cash_secured_put")
                alloc_pct = _safe_float(capital_alloc.get(strategy_key) or capital_alloc.get("wheel"), 0.20)
                capital_for_trade = cash * alloc_pct
                cost_per_contract = _safe_float(opp.get("secured_capital") or opp.get("cost_per_contract"), 0.0)
                if cost_per_contract <= 0:
                    cost_per_contract = _safe_float(opp.get("strike"), 0.0) * 100.0
                qty = max(1, int(capital_for_trade / max(cost_per_contract, 1.0)))

                _log(
                    f"executing {strategy} {symbol} score={score:.1f} "
                    f"strike={opp.get('strike')} exp={opp.get('expiration')} "
                    f"qty={qty} capital=${capital_for_trade:.2f}"
                )

                result = _place_options_order(adapter, opp, qty, order_type="LMT")

                if result.get("ok"):
                    _set_cooldown(symbol)
                    open_symbols.add(symbol)
                    regular_count += 1
                    executed_regular.append({
                        "symbol": symbol,
                        "strategy": strategy,
                        "score": score,
                        "strike": opp.get("strike"),
                        "expiration": opp.get("expiration"),
                        "qty": qty,
                        "order_id": result.get("order_id"),
                        "capital_used": round(capital_for_trade, 2),
                    })
                    _log(f"order placed {symbol} {strategy} order_id={result.get('order_id')}")
                else:
                    _log(f"order failed {symbol}: {result.get('error')}")

        except Exception as exc:
            _log(f"regular scan failed: {exc}")

    # --- MEME SQUEEZE CALLS ---
    if regime in SQUEEZE_ALLOWED_REGIMES and squeeze_count < MAX_SQUEEZE_POSITIONS:
        try:
            sentiment_scanner = SocialSentimentScanner()
            screener = OptionsScreener(watchlist=list(MEME_STOCKS))
            scan_result = screener.scan_universe(strategies=["meme_calls"])
            squeeze_opps = [
                opp for opp in scan_result.get("opportunities", [])
                if opp.get("is_meme_squeeze") is True
            ]
            _log(f"squeeze scan found {len(squeeze_opps)} confirmed squeeze opportunities")

            for opp in squeeze_opps:
                if squeeze_count >= MAX_SQUEEZE_POSITIONS:
                    break

                symbol = str(opp.get("symbol") or "").upper()
                score = _safe_float(opp.get("score"))
                squeeze_conviction = _safe_float(opp.get("squeeze_conviction"), 0.5)
                squeeze_capital_pct = _safe_float(opp.get("squeeze_capital_pct"), 0.4)

                if score < MIN_SQUEEZE_SCORE:
                    continue
                if symbol in open_symbols:
                    _log(f"skip squeeze {symbol}: already open")
                    continue
                if _in_cooldown(symbol):
                    _log(f"skip squeeze {symbol}: in cooldown")
                    continue

                capital_for_squeeze = cash * squeeze_capital_pct
                cost_per_contract = _safe_float(opp.get("cost_per_contract"), 0.0)
                if cost_per_contract <= 0:
                    cost_per_contract = _safe_float(opp.get("mid"), 0.0) * 100.0
                qty = max(1, int(capital_for_squeeze / max(cost_per_contract, 1.0)))

                _log(
                    f"executing SQUEEZE CALL {symbol} score={score:.1f} "
                    f"conviction={squeeze_conviction:.2f} capital_pct={squeeze_capital_pct:.0%} "
                    f"strike={opp.get('strike')} exp={opp.get('expiration')} "
                    f"qty={qty} capital=${capital_for_squeeze:.2f}"
                )

                result = _place_options_order(adapter, opp, qty, order_type="LMT")

                if result.get("ok"):
                    _set_cooldown(symbol)
                    open_symbols.add(symbol)
                    squeeze_count += 1
                    executed_squeeze.append({
                        "symbol": symbol,
                        "strategy": "meme_call",
                        "score": score,
                        "squeeze_conviction": squeeze_conviction,
                        "capital_pct": squeeze_capital_pct,
                        "strike": opp.get("strike"),
                        "expiration": opp.get("expiration"),
                        "qty": qty,
                        "order_id": result.get("order_id"),
                        "capital_used": round(capital_for_squeeze, 2),
                    })
                    _log(f"squeeze order placed {symbol} order_id={result.get('order_id')}")
                else:
                    _log(f"squeeze order failed {symbol}: {result.get('error')}")

        except Exception as exc:
            _log(f"squeeze scan failed: {exc}")

    total_executed = len(executed_regular) + len(executed_squeeze)
    _log(f"scan complete regular={len(executed_regular)} squeeze={len(executed_squeeze)}")
    return {
        "ok": True,
        "regime": regime,
        "cash": cash,
        "vix": vix,
        "executed_regular": executed_regular,
        "executed_squeeze": executed_squeeze,
        "total_executed": total_executed,
    }


def run_options_position_monitor() -> dict[str, Any]:
    """
    Called every 5 minutes. Checks open options positions for
    profit targets and stop losses. Closes via WebullAdapter.

    Exit rules:
    - Income plays (CSP/CC/Wheel): close at 50% profit (buy back at half premium)
    - Squeeze calls: close at 100% gain or -50% loss
    """
    try:
        positions = _get_open_options_positions()
        if not positions:
            return {"ok": True, "actions": []}

        adapter = WebullAdapter()
        actions = []

        for pos in positions:
            symbol = str(pos.get("symbol") or "").upper()
            asset_type = str(pos.get("asset_type") or "").lower()
            if asset_type != "option":
                continue

            cost_basis = _safe_float(pos.get("cost_basis") or pos.get("avg_cost"))
            market_value = _safe_float(pos.get("market_value"))
            unrealized_pnl = _safe_float(pos.get("unrealized_pnl") or pos.get("unrealized_pl"))
            qty = _safe_int(pos.get("qty") or pos.get("quantity"), 1)
            is_short = str(pos.get("side") or pos.get("position_type") or "").lower() in {"short", "sell", "s"}
            is_meme = symbol in MEME_STOCKS

            if cost_basis <= 0 or market_value <= 0:
                continue

            pnl_pct = unrealized_pnl / max(abs(cost_basis), 0.01)
            reason = None

            if is_meme and not is_short:
                # Long squeeze call: take 100% gain or cut at -50%
                if pnl_pct >= 1.0:
                    reason = "take_profit_100pct"
                elif pnl_pct <= -0.5:
                    reason = "stop_loss_50pct"
            else:
                # Income play (short): close at 50% profit
                if pnl_pct >= 0.5:
                    reason = "take_profit_50pct"
                elif pnl_pct <= -1.0:
                    reason = "stop_loss_100pct"

            if reason:
                _log(
                    f"closing {symbol} reason={reason} "
                    f"pnl_pct={pnl_pct*100:.1f}% pnl=${unrealized_pnl:.2f}"
                )
                try:
                    close_order = {
                        "underlying": symbol,
                        "expiration": str(pos.get("expiration") or ""),
                        "strike": _safe_float(pos.get("strike")),
                        "option_type": str(pos.get("option_type") or "call").lower(),
                        "side": "buy" if is_short else "sell",
                        "qty": qty,
                        "order_type": "MKT",
                        "limit_price": None,
                    }
                    result = adapter.place_options_order(close_order)
                    if result and result.get("ok"):
                        _set_cooldown(symbol)
                        actions.append({
                            "symbol": symbol,
                            "reason": reason,
                            "pnl": unrealized_pnl,
                            "pnl_pct": round(pnl_pct * 100, 1),
                            "order_id": result.get("order_id"),
                        })
                        _log(f"closed {symbol} order_id={result.get('order_id')}")
                    else:
                        _log(f"close failed {symbol}: {(result or {}).get('error')}")
                except Exception as exc:
                    _log(f"close error {symbol}: {exc}")

        if actions:
            _log(f"monitor closed {len(actions)} option position(s)")

        return {"ok": True, "actions": actions, "positions_checked": len(positions)}

    except Exception as exc:
        _log(f"position monitor failed: {exc}")
        return {"ok": False, "error": str(exc), "actions": []}


def get_executor_log(limit: int = 50) -> list[dict[str, Any]]:
    with _EXECUTOR_LOCK:
        return list(reversed(_EXECUTOR_LOG[-int(limit):]))
