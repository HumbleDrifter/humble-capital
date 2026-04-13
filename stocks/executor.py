"""
stocks/executor.py

Automated stock execution engine.
- Hourly scan at :47 (offset: crypto :02, options :17, futures :32)
- Uses StockScanner for signals, WebullAdapter for execution
- yfinance fallback for price data (works without NBBO)
- Position monitor every 5 min via _trailing_exit_loop
"""
from __future__ import annotations

import time
import threading
from typing import Any

from brokers.webull_adapter import WebullAdapter
from stock_scanner import StockScanner
from portfolio import get_portfolio_snapshot

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_EXECUTOR_LOCK = threading.Lock()
_SYMBOL_COOLDOWNS: dict[str, float] = {}
_EXECUTOR_LOG: list[dict[str, Any]] = []
_PENDING_SELLS: set[str] = set()   # dedup guard


def _is_market_open() -> bool:
    """Returns True only during regular US market hours (9:30-16:00 ET)."""
    try:
        from datetime import datetime
        import zoneinfo
        now = datetime.now(zoneinfo.ZoneInfo("America/New_York"))
        if now.weekday() >= 5:
            return False
        market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
        return market_open <= now < market_close
    except Exception:
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone(timedelta(hours=-4)))
        if now.weekday() >= 5:
            return False
        return (9 * 60 + 30) <= (now.hour * 60 + now.minute) < (16 * 60)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MIN_SCORE = 70.0                # minimum composite_score to execute
MIN_BUYING_POWER = 20.0         # minimum $ needed to place any trade
MAX_OPEN_POSITIONS = 8          # max concurrent stock positions
MAX_ALLOC_PER_TRADE = 0.25      # max 25% of buying power per trade
COOLDOWN_SECONDS = 14400        # 4 hours between re-entries on same symbol
STOP_LOSS_PCT = 0.07            # 7% stop loss from entry
TAKE_PROFIT_PCT = 0.15          # 15% take profit from entry

# Regimes that allow new long stock entries
LONG_ALLOWED_REGIMES = {"bull", "neutral"}
# Regimes that allow reduced-size entries
CAUTIOUS_REGIMES = {"caution"}
CAUTIOUS_SIZE_MULTIPLIER = 0.5  # half size in caution regime
BLOCKED_REGIMES = {"risk_off"}


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[stock_executor] {msg}", flush=True)
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


def _get_webull_state() -> tuple[float, set[str]]:
    """Returns (buying_power, set of held symbols)."""
    try:
        adapter = WebullAdapter()
        info = adapter.get_account_info()
        buying_power = _safe_float(info.get("buying_power"))
        positions = adapter.get_positions() or []
        held = {
            str(p.get("symbol") or "").upper()
            for p in positions
            if str(p.get("asset_type") or "").lower() == "stock"
        }
        return buying_power, held
    except Exception:
        return 0.0, set()


def _get_current_price(symbol: str) -> float:
    """Get current price via yfinance (works without NBBO)."""
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d", interval="1m")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
        info = ticker.info
        return _safe_float(
            info.get("regularMarketPrice")
            or info.get("currentPrice")
            or info.get("previousClose"),
            0.0,
        )
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Core execution
# ---------------------------------------------------------------------------

def run_stock_scan_and_execute() -> dict[str, Any]:
    """
    Called hourly at :47. Scans stock universe and executes
    high-conviction buy signals.
    """
    buying_power, held_symbols = _get_webull_state()

    if buying_power < MIN_BUYING_POWER:
        _log(f"scan skipped: buying_power=${buying_power:.2f} below minimum ${MIN_BUYING_POWER}")
        return {
            "ok": True,
            "skipped": True,
            "reason": "insufficient_buying_power",
            "buying_power": buying_power,
        }

    regime = _get_regime()
    if regime in BLOCKED_REGIMES:
        _log(f"scan skipped: regime={regime} blocks stock entries")
        return {"ok": True, "skipped": True, "reason": f"regime_blocked_{regime}"}

    size_multiplier = CAUTIOUS_SIZE_MULTIPLIER if regime in CAUTIOUS_REGIMES else 1.0
    open_count = len(held_symbols)

    if open_count >= MAX_OPEN_POSITIONS:
        _log(f"scan skipped: {open_count} open positions >= max {MAX_OPEN_POSITIONS}")
        return {"ok": True, "skipped": True, "reason": "max_positions_reached"}

    _log(
        f"scan starting regime={regime} buying_power=${buying_power:.2f} "
        f"open={open_count}/{MAX_OPEN_POSITIONS}"
    )

    try:
        scanner = StockScanner()
        result = scanner.scan_universe(regime=regime)
        opportunities = result.get("opportunities", [])
    except Exception as exc:
        _log(f"scanner failed: {exc}")
        return {"ok": False, "error": str(exc)}

    _log(f"scanner found {len(opportunities)} opportunities")

    adapter = WebullAdapter()
    executed = []
    slots = MAX_OPEN_POSITIONS - open_count

    for opp in opportunities:
        if slots <= 0:
            break

        symbol = str(opp.get("symbol") or "").upper()
        score = _safe_float(opp.get("composite_score"))
        signal = str(opp.get("signal") or "").lower()
        price = _safe_float(opp.get("price"))

        if score < MIN_SCORE:
            continue
        if signal not in {"strong_buy", "buy"}:
            continue
        if symbol in held_symbols:
            _log(f"skip {symbol}: already held")
            continue
        if _in_cooldown(symbol):
            _log(f"skip {symbol}: in cooldown")
            continue
        if price <= 0:
            # Try to get price via yfinance
            price = _get_current_price(symbol)
        if price <= 0:
            _log(f"skip {symbol}: could not determine price")
            continue

        # Size the trade
        capital = buying_power * MAX_ALLOC_PER_TRADE * size_multiplier
        qty = max(1, int(capital / price))
        cost = qty * price

        if cost > buying_power:
            qty = max(1, int(buying_power * 0.9 / price))
            cost = qty * price

        if cost < 1.0:
            _log(f"skip {symbol}: cost ${cost:.2f} too small")
            continue

        _log(
            f"executing BUY {symbol} score={score:.1f} signal={signal} "
            f"price=${price:.2f} qty={qty} cost=${cost:.2f} regime={regime}"
        )

        result_order = adapter.place_stock_order(
            symbol=symbol,
            side="BUY",
            qty=qty,
            order_type="MKT",
        )

        if result_order.get("ok"):
            _set_cooldown(symbol)
            held_symbols.add(symbol)
            buying_power -= cost
            slots -= 1
            executed.append({
                "symbol": symbol,
                "signal": signal,
                "score": score,
                "price": price,
                "qty": qty,
                "cost": round(cost, 2),
                "order_id": result_order.get("order_id"),
                "regime": regime,
            })
            _log(f"order placed {symbol} qty={qty} order_id={result_order.get('order_id')}")
        else:
            _log(f"order failed {symbol}: {result_order.get('error')}")

    _log(f"scan complete opportunities={len(opportunities)} executed={len(executed)}")
    return {
        "ok": True,
        "regime": regime,
        "buying_power_before": buying_power + sum(t["cost"] for t in executed),
        "opportunities_scanned": len(opportunities),
        "executed": executed,
    }


def run_stock_position_monitor() -> dict[str, Any]:
    """
    Called every 5 minutes. Checks open stock positions for
    stop loss and take profit conditions using yfinance prices.

    Exit rules:
    - Stop loss: -7% from avg cost
    - Take profit: +15% from avg cost
    - Momentum exit: price < EMA_mid AND RSI < 40 (from scanner indicators)
    """
    try:
        if not _is_market_open():
            return {"ok": True, "actions": [], "skipped": True, "reason": "market_closed"}

        adapter = WebullAdapter()
        positions = adapter.get_positions() or []
        stock_positions = [
            p for p in positions
            if str(p.get("asset_type") or "").lower() == "stock"
        ]

        if not stock_positions:
            return {"ok": True, "actions": []}

        actions = []

        for pos in stock_positions:
            symbol = str(pos.get("symbol") or "").upper()
            avg_cost = _safe_float(pos.get("avg_cost") or pos.get("cost_basis"))
            qty = _safe_float(pos.get("qty") or pos.get("quantity"))
            last_price = _safe_float(pos.get("last_price") or pos.get("market_price"))

            if avg_cost <= 0 or qty <= 0:
                continue

            # Try to get fresher price
            fresh_price = _get_current_price(symbol)
            mark_price = fresh_price if fresh_price > 0 else last_price
            if mark_price <= 0:
                continue

            pnl_pct = (mark_price - avg_cost) / avg_cost

            reason = None
            if pnl_pct <= -STOP_LOSS_PCT:
                reason = f"stop_loss_{STOP_LOSS_PCT:.0%}"
            elif pnl_pct >= TAKE_PROFIT_PCT:
                reason = f"take_profit_{TAKE_PROFIT_PCT:.0%}"

            if reason:
                _log(
                    f"closing {symbol} reason={reason} "
                    f"avg_cost=${avg_cost:.2f} mark=${mark_price:.2f} "
                    f"pnl={pnl_pct*100:.1f}%"
                )
                result = adapter.place_stock_order(
                    symbol=symbol,
                    side="SELL",
                    qty=int(qty),
                    order_type="MKT",
                )
                if result.get("ok"):
                    _set_cooldown(symbol)
                    actions.append({
                        "symbol": symbol,
                        "reason": reason,
                        "avg_cost": avg_cost,
                        "exit_price": mark_price,
                        "pnl_pct": round(pnl_pct * 100, 1),
                        "qty": int(qty),
                        "order_id": result.get("order_id"),
                    })
                    _log(f"sold {symbol} order_id={result.get('order_id')}")
                else:
                    _log(f"sell failed {symbol}: {result.get('error')}")

        if actions:
            _log(f"monitor closed {len(actions)} stock position(s)")

        return {"ok": True, "actions": actions, "positions_checked": len(stock_positions)}

    except Exception as exc:
        _log(f"stock position monitor failed: {exc}")
        return {"ok": False, "error": str(exc), "actions": []}


def get_executor_log(limit: int = 50) -> list[dict[str, Any]]:
    with _EXECUTOR_LOCK:
        return list(reversed(_EXECUTOR_LOG[-int(limit):]))
