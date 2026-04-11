import math
from datetime import datetime, timedelta, timezone

from backtester import adx, atr, bollinger_bands, ema, macd, rsi


def _log(message):
    print(f"[stock_backtester] {message}")


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _normalize_symbol(symbol):
    return str(symbol or "").upper().strip()


def _max_drawdown(curve):
    peak = 0.0
    max_dd = 0.0
    for row in curve:
        equity = _safe_float(row.get("equity"), 0.0)
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak)
    return max_dd * 100.0


class StockBacktester:
    DEFAULTS = {
        "ema_momentum": {
            "ema_fast": 21,
            "ema_mid": 50,
            "ema_slow": 200,
            "rsi_buy": 40,
            "rsi_exit": 70,
            "adx_min": 20,
            "bb_period": 20,
            "bb_std": 2.0,
        },
        "bb_breakout": {
            "ema_fast": 21,
            "ema_mid": 50,
            "ema_slow": 200,
            "rsi_buy": 45,
            "rsi_exit": 70,
            "adx_min": 20,
            "bb_period": 20,
            "bb_std": 2.0,
        },
        "mean_reversion": {
            "ema_fast": 21,
            "ema_mid": 50,
            "ema_slow": 200,
            "rsi_buy": 30,
            "rsi_exit": 60,
            "adx_min": 15,
            "bb_period": 20,
            "bb_std": 2.0,
        },
        "trend_following": {
            "ema_fast": 21,
            "ema_mid": 50,
            "ema_slow": 200,
            "rsi_buy": 45,
            "rsi_exit": 40,
            "adx_min": 25,
            "bb_period": 20,
            "bb_std": 2.0,
        },
    }

    def __init__(self, config=None):
        config = dict(config or {})
        self.symbol = _normalize_symbol(config.get("symbol") or "AAPL")
        self.strategy = str(config.get("strategy") or "ema_momentum").strip().lower()
        self.start_date = config.get("start_date")
        self.end_date = config.get("end_date")
        self.starting_capital = _safe_float(config.get("starting_capital"), 10000.0)
        self.fee_pct = _safe_float(config.get("fee_pct"), 0.0)
        self.params = {
            **dict(self.DEFAULTS.get(self.strategy) or self.DEFAULTS["ema_momentum"]),
            **dict(config.get("params") or {}),
        }
        self.trade_log = []
        self.equity_curve = []

    def fetch_bars(self):
        try:
            import yfinance as yf

            end = datetime.fromisoformat(str(self.end_date)) if self.end_date else datetime.now(timezone.utc)
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)
            start = datetime.fromisoformat(str(self.start_date)) if self.start_date else end - timedelta(days=365)
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            hist = yf.Ticker(self.symbol).history(start=start.strftime("%Y-%m-%d"), end=(end + timedelta(days=1)).strftime("%Y-%m-%d"), interval="1d")
            bars = []
            for idx, row in hist.iterrows():
                ts = int(idx.timestamp()) if hasattr(idx, "timestamp") else 0
                bars.append({
                    "ts": ts,
                    "date": str(idx.date()),
                    "open": _safe_float(row.get("Open"), 0.0),
                    "high": _safe_float(row.get("High"), 0.0),
                    "low": _safe_float(row.get("Low"), 0.0),
                    "close": _safe_float(row.get("Close"), 0.0),
                    "volume": _safe_float(row.get("Volume"), 0.0),
                })
            return bars
        except Exception as exc:
            _log(f"fetch_bars failed symbol={self.symbol} error={exc}")
            return []

    def compute_indicators(self, bars):
        rows = [dict(row) for row in (bars or []) if isinstance(row, dict)]
        closes = [_safe_float(row.get("close"), 0.0) for row in rows]
        highs = [_safe_float(row.get("high"), 0.0) for row in rows]
        lows = [_safe_float(row.get("low"), 0.0) for row in rows]
        period = max(2, int(self.params.get("bb_period", 20)))
        std = _safe_float(self.params.get("bb_std"), 2.0)

        ema_fast = ema(closes, max(2, int(self.params.get("ema_fast", 21))))
        ema_mid = ema(closes, max(2, int(self.params.get("ema_mid", 50))))
        ema_slow = ema(closes, max(2, int(self.params.get("ema_slow", 200))))
        rsi_vals = rsi(closes, 14)
        _, _, macd_hist = macd(closes, 12, 26, 9)
        bb_upper, bb_middle, bb_lower = bollinger_bands(closes, period, std)
        adx_vals = adx(highs, lows, closes, 14)
        atr_vals = atr(highs, lows, closes, 14)

        for idx, row in enumerate(rows):
            row["ema_fast"] = ema_fast[idx]
            row["ema_mid"] = ema_mid[idx]
            row["ema_slow"] = ema_slow[idx]
            row["ema_trend"] = ema_slow[idx]
            row["rsi"] = rsi_vals[idx]
            row["macd_hist"] = macd_hist[idx]
            row["bb_upper"] = bb_upper[idx]
            row["bb_middle"] = bb_middle[idx]
            row["bb_lower"] = bb_lower[idx]
            row["adx"] = adx_vals[idx]
            row["atr"] = atr_vals[idx]
        return rows

    def run(self):
        bars = self.compute_indicators(self.fetch_bars())
        if len(bars) < 60:
            return {"equity_curve": [], "trade_log": [], "summary": self.get_summary({"equity_curve": [], "trade_log": []})}

        cash = self.starting_capital
        qty = 0.0
        entry_price = 0.0
        entry_index = None
        wins = []

        for idx, bar in enumerate(bars):
            close = _safe_float(bar.get("close"), 0.0)
            if close <= 0:
                continue

            position_value = qty * close
            self.equity_curve.append({
                "date": bar.get("date"),
                "ts": bar.get("ts"),
                "equity": round(cash + position_value, 2),
                "cash": round(cash, 2),
                "position_value": round(position_value, 2),
            })

            if idx < 50:
                continue

            prev = bars[idx - 1]
            ema_aligned = _safe_float(bar.get("ema_fast"), 0.0) > _safe_float(bar.get("ema_mid"), 0.0) > _safe_float(bar.get("ema_slow"), 0.0)
            above_trend = close > _safe_float(bar.get("ema_trend"), 0.0)
            rsi_val = _safe_float(bar.get("rsi"), 50.0)
            macd_rising = _safe_float(bar.get("macd_hist"), 0.0) > _safe_float(prev.get("macd_hist"), 0.0)
            adx_ok = _safe_float(bar.get("adx"), 0.0) >= _safe_float(self.params.get("adx_min"), 20.0)
            buy_signal = False
            exit_signal = False
            reason = ""

            if self.strategy == "ema_momentum":
                buy_signal = ema_aligned and above_trend and rsi_val >= _safe_float(self.params.get("rsi_buy"), 40.0) and macd_rising and adx_ok
                exit_signal = close < _safe_float(bar.get("ema_mid"), 0.0) or rsi_val >= _safe_float(self.params.get("rsi_exit"), 70.0) or _safe_float(bar.get("macd_hist"), 0.0) < 0
                reason = "ema_momentum"
            elif self.strategy == "bb_breakout":
                buy_signal = close > _safe_float(bar.get("bb_upper"), 0.0) and adx_ok and rsi_val >= 40 and rsi_val <= _safe_float(self.params.get("rsi_exit"), 70.0)
                exit_signal = close < _safe_float(bar.get("bb_middle"), 0.0) or close < _safe_float(bar.get("ema_mid"), 0.0)
                reason = "bb_breakout"
            elif self.strategy == "mean_reversion":
                buy_signal = rsi_val <= _safe_float(self.params.get("rsi_buy"), 30.0) and close <= _safe_float(bar.get("bb_lower"), 0.0)
                exit_signal = rsi_val >= _safe_float(self.params.get("rsi_exit"), 60.0) or close >= _safe_float(bar.get("bb_middle"), 0.0)
                reason = "mean_reversion"
            elif self.strategy == "trend_following":
                buy_signal = above_trend and ema_aligned and adx_ok and rsi_val >= _safe_float(self.params.get("rsi_buy"), 45.0)
                exit_signal = close < _safe_float(bar.get("ema_mid"), 0.0) or _safe_float(bar.get("adx"), 0.0) < max(10.0, _safe_float(self.params.get("adx_min"), 25.0) - 5.0)
                reason = "trend_following"

            if qty <= 0 and buy_signal:
                size_usd = cash * 0.95
                fee = size_usd * self.fee_pct
                qty = (size_usd - fee) / close if close > 0 else 0.0
                cash -= size_usd
                entry_price = close
                entry_index = idx
                self.trade_log.append({"date": bar.get("date"), "action": "BUY", "symbol": self.symbol, "price": round(close, 2), "qty": round(qty, 6), "pnl": 0.0, "reason": reason})
            elif qty > 0 and exit_signal:
                proceeds = qty * close
                fee = proceeds * self.fee_pct
                pnl = proceeds - fee - (qty * entry_price)
                cash += proceeds - fee
                hold_bars = idx - entry_index if entry_index is not None else 0
                self.trade_log.append({"date": bar.get("date"), "action": "EXIT", "symbol": self.symbol, "price": round(close, 2), "qty": round(qty, 6), "pnl": round(pnl, 2), "hold_bars": hold_bars, "reason": reason})
                wins.append(pnl)
                qty = 0.0
                entry_price = 0.0
                entry_index = None

        if qty > 0 and bars:
            last = bars[-1]
            close = _safe_float(last.get("close"), 0.0)
            proceeds = qty * close
            fee = proceeds * self.fee_pct
            pnl = proceeds - fee - (qty * entry_price)
            cash += proceeds - fee
            hold_bars = (len(bars) - 1) - entry_index if entry_index is not None else 0
            self.trade_log.append({"date": last.get("date"), "action": "EXIT", "symbol": self.symbol, "price": round(close, 2), "qty": round(qty, 6), "pnl": round(pnl, 2), "hold_bars": hold_bars, "reason": "end_of_test"})
            wins.append(pnl)

        result = {"equity_curve": self.equity_curve, "trade_log": self.trade_log}
        result["summary"] = self.get_summary(result)
        return result

    def get_summary(self, result):
        equity_curve = list((result or {}).get("equity_curve", []) or [])
        trade_log = list((result or {}).get("trade_log", []) or [])
        pnls = [_safe_float(row.get("pnl"), 0.0) for row in trade_log if str(row.get("action") or "").upper() == "EXIT"]
        wins = [p for p in pnls if p > 0]
        losses = [abs(p) for p in pnls if p < 0]
        hold_bars = [_safe_float(row.get("hold_bars"), 0.0) for row in trade_log if str(row.get("action") or "").upper() == "EXIT"]
        ending = _safe_float((equity_curve[-1] if equity_curve else {}).get("equity"), self.starting_capital)
        return {
            "starting_capital": round(self.starting_capital, 2),
            "ending_capital": round(ending, 2),
            "total_return_pct": round(((ending - self.starting_capital) / self.starting_capital * 100.0) if self.starting_capital > 0 else 0.0, 2),
            "win_rate": round((len(wins) / len(pnls) * 100.0) if pnls else 0.0, 2),
            "profit_factor": round((sum(wins) / sum(losses)) if losses else (sum(wins) if wins else 0.0), 2),
            "max_drawdown_pct": round(_max_drawdown(equity_curve), 2),
            "avg_hold_bars": round(sum(hold_bars) / len(hold_bars), 2) if hold_bars else 0.0,
            "total_trades": len(pnls),
        }
