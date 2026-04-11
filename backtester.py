import math
import time
from datetime import datetime, timedelta, timezone

from execution import get_client


PARAMS_4H_CORE = {
    "ema_fast": 21,
    "ema_mid": 50,
    "ema_slow": 200,
    "rsi_buy_min": 35,
    "rsi_buy_max": 70,
    "rsi_trim_thresh": 80,
    "rsi_exit_thresh": 40,
    "cooldown_bars": 4,
    "trim_cooldown": 8,
    "ext_pct": 8.0,
    "position_size_pct": 0.10,
}

PARAMS_1H_SATELLITE = {
    "ema_fast": 9,
    "ema_mid": 21,
    "ema_slow": 50,
    "rsi_buy_min": 45,
    "rsi_buy_max": 75,
    "rsi_trim_thresh": 82,
    "rsi_exit_thresh": 38,
    "cooldown_bars": 3,
    "trim_cooldown": 4,
    "ext_pct": 10.0,
    "position_size_pct": 0.10,
}

_CANDLE_CACHE = {}


def _log(message):
    print(f"[backtester] {message}")


def _to_dict(x):
    return x.to_dict() if hasattr(x, "to_dict") else x


def _safe_float(value, default=0.0):
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _safe_int(value, default=0):
    try:
        return int(value or 0)
    except Exception:
        return int(default)


def _utc_ts_from_date_string(value, end_of_day=False):
    if not value:
        return None
    dt = datetime.fromisoformat(str(value).strip())
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=0)
    else:
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(dt.timestamp())


def _granularity_for_timeframe(timeframe):
    tf = str(timeframe or "1h").lower().strip()
    if tf == "4h":
        return "FOUR_HOUR", 4 * 60 * 60
    return "ONE_HOUR", 60 * 60


def ema(values: list, period: int) -> list:
    values = [_safe_float(value, None) if value is not None else None for value in values]
    period = max(1, int(period or 1))
    out = [None] * len(values)
    if len(values) < period:
        return out

    seed = []
    for value in values:
        if value is None:
            seed = []
        else:
            seed.append(value)
        if len(seed) == period:
            break

    if len(seed) < period:
        return out

    multiplier = 2.0 / (period + 1.0)
    start_index = period - 1
    running = sum(seed) / period
    out[start_index] = running

    for index in range(start_index + 1, len(values)):
        value = values[index]
        if value is None:
            out[index] = None
            continue
        running = ((value - running) * multiplier) + running
        out[index] = running

    return out


def rsi(closes: list, period: int = 14) -> list:
    period = max(1, int(period or 14))
    closes = [_safe_float(value, None) if value is not None else None for value in closes]
    out = [None] * len(closes)
    if len(closes) <= period:
        return out

    gains = []
    losses = []
    for idx in range(1, period + 1):
        if closes[idx] is None or closes[idx - 1] is None:
            return out
        delta = closes[idx] - closes[idx - 1]
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    out[period] = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

    for idx in range(period + 1, len(closes)):
        if closes[idx] is None or closes[idx - 1] is None:
            continue
        delta = closes[idx] - closes[idx - 1]
        gain = max(delta, 0.0)
        loss = abs(min(delta, 0.0))
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        out[idx] = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

    return out


def macd(closes: list, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple:
    fast_ema = ema(closes, fast)
    slow_ema = ema(closes, slow)

    macd_line = []
    for fast_value, slow_value in zip(fast_ema, slow_ema):
        if fast_value is None or slow_value is None:
            macd_line.append(None)
        else:
            macd_line.append(fast_value - slow_value)

    signal_line = ema(macd_line, signal)
    histogram = []
    for macd_value, signal_value in zip(macd_line, signal_line):
        if macd_value is None or signal_value is None:
            histogram.append(None)
        else:
            histogram.append(macd_value - signal_value)

    return macd_line, signal_line, histogram


class Backtester:
    def __init__(self, product_id: str, timeframe: str = "1h", start_date: str = None, end_date: str = None):
        self.product_id = str(product_id or "").upper().strip()
        self.timeframe = str(timeframe or "1h").lower().strip()
        self.granularity, self.bar_seconds = _granularity_for_timeframe(self.timeframe)

        end_ts = _utc_ts_from_date_string(end_date, end_of_day=True)
        if end_ts is None:
            end_ts = int(time.time())
        start_ts = _utc_ts_from_date_string(start_date, end_of_day=False)
        if start_ts is None:
            start_ts = end_ts - (90 * 24 * 60 * 60)

        self.start_ts = int(start_ts)
        self.end_ts = int(end_ts)
        self.trade_log = []
        self.position_state = {"state": 0, "base_qty": 0.0, "avg_entry": 0.0, "entry_bar": None, "entry_ts": None}
        self.equity_curve = []

    def load_candles(self) -> list:
        cache_key = (self.product_id, self.timeframe, self.start_ts, self.end_ts)
        if cache_key in _CANDLE_CACHE:
            return [dict(row) for row in _CANDLE_CACHE[cache_key]]

        client = get_client()
        candles = []
        cursor_start = self.start_ts
        max_span = self.bar_seconds * 300

        try:
            while cursor_start < self.end_ts:
                cursor_end = min(self.end_ts, cursor_start + max_span)
                response = _to_dict(
                    client.get_candles(
                        product_id=self.product_id,
                        start=str(cursor_start),
                        end=str(cursor_end),
                        granularity=self.granularity,
                    )
                )
                chunk = (response or {}).get("candles") or []
                for candle in chunk:
                    try:
                        candles.append(
                            {
                                "ts": _safe_int(candle.get("start"), 0),
                                "open": _safe_float(candle.get("open"), 0.0),
                                "high": _safe_float(candle.get("high"), 0.0),
                                "low": _safe_float(candle.get("low"), 0.0),
                                "close": _safe_float(candle.get("close"), 0.0),
                                "volume": _safe_float(candle.get("volume"), 0.0),
                            }
                        )
                    except Exception:
                        continue
                cursor_start = cursor_end + self.bar_seconds

            deduped = {}
            for row in candles:
                if row["ts"] > 0:
                    deduped[row["ts"]] = row
            ordered = [deduped[key] for key in sorted(deduped.keys())]
            _CANDLE_CACHE[cache_key] = [dict(row) for row in ordered]
            _log(f"loaded candles product_id={self.product_id} timeframe={self.timeframe} count={len(ordered)}")
            return ordered
        except Exception as exc:
            _log(f"load_candles failed product_id={self.product_id} error={exc}")
            return []

    def compute_indicators(self, candles: list) -> list:
        try:
            params = PARAMS_4H_CORE if self.timeframe == "4h" else PARAMS_1H_SATELLITE
            closes = [_safe_float(candle.get("close"), 0.0) for candle in candles]
            ema_fast = ema(closes, params["ema_fast"])
            ema_mid = ema(closes, params["ema_mid"])
            ema_slow = ema(closes, params["ema_slow"])
            rsi_values = rsi(closes, 14)
            macd_line, signal_line, histogram = macd(closes, 12, 26, 9)

            enriched = []
            for idx, candle in enumerate(candles):
                row = dict(candle)
                row["ema_fast"] = ema_fast[idx]
                row["ema_mid"] = ema_mid[idx]
                row["ema_slow"] = ema_slow[idx]
                row["rsi"] = rsi_values[idx]
                row["macd_line"] = macd_line[idx]
                row["macd_signal"] = signal_line[idx]
                row["macd_hist"] = histogram[idx]
                enriched.append(row)
            return enriched
        except Exception as exc:
            _log(f"compute_indicators failed product_id={self.product_id} error={exc}")
            return candles

    def run_strategy(self, candles: list, params: dict = None) -> dict:
        params_base = dict(PARAMS_4H_CORE if self.timeframe == "4h" else PARAMS_1H_SATELLITE)
        params_base.update(params or {})
        trade_log = []
        equity_curve = []
        cash_usd = 1000.0
        base_qty = 0.0
        avg_entry = 0.0
        state = 0
        entry_bar = None
        entry_ts = None
        last_buy_bar = -10_000
        last_trim_bar = -10_000
        realized_pnl = 0.0

        def current_equity(last_price):
            return cash_usd + (base_qty * _safe_float(last_price, 0.0))

        def conviction_for_bar(bar, prev_bar):
            score = 0.0
            if bar.get("close", 0.0) > (bar.get("ema_fast") or 0.0):
                score += 0.3
            if (bar.get("ema_fast") or 0.0) > (bar.get("ema_mid") or 0.0):
                score += 0.25
            if (bar.get("ema_mid") or 0.0) > (bar.get("ema_slow") or 0.0):
                score += 0.2
            if (bar.get("macd_hist") or 0.0) > 0:
                score += 0.15
            if prev_bar and (bar.get("macd_hist") or 0.0) > (prev_bar.get("macd_hist") or 0.0):
                score += 0.1
            return round(min(1.0, score), 4)

        for idx, bar in enumerate(candles):
            close = _safe_float(bar.get("close"), 0.0)
            if close <= 0:
                continue

            prev_bar = candles[idx - 1] if idx > 0 else None
            ema_fast_value = bar.get("ema_fast")
            ema_mid_value = bar.get("ema_mid")
            ema_slow_value = bar.get("ema_slow")
            rsi_value = bar.get("rsi")
            hist_value = bar.get("macd_hist")
            prev_hist = prev_bar.get("macd_hist") if prev_bar else None

            equity_curve.append({"ts": bar["ts"], "equity": round(current_equity(close), 2), "close": close})

            if None in (ema_fast_value, ema_mid_value, ema_slow_value, rsi_value, hist_value):
                continue

            ema_aligned = close > ema_fast_value > ema_mid_value > ema_slow_value
            macd_rising = prev_hist is not None and hist_value > 0 and hist_value > prev_hist
            not_overextended = ((close - ema_fast_value) / ema_fast_value) * 100.0 <= _safe_float(params_base["ext_pct"], 10.0)
            cooldown_ok = (idx - last_buy_bar) >= int(params_base["cooldown_bars"])
            trim_cooldown_ok = (idx - last_trim_bar) >= int(params_base["trim_cooldown"])
            conviction_score = conviction_for_bar(bar, prev_bar)

            if state == 0 and cooldown_ok and ema_aligned and macd_rising and not_overextended:
                if _safe_float(params_base["rsi_buy_min"], 0.0) <= rsi_value <= _safe_float(params_base["rsi_buy_max"], 100.0):
                    size_usd = current_equity(close) * _safe_float(params_base["position_size_pct"], 0.10)
                    qty = size_usd / close if close > 0 else 0.0
                    if qty > 0 and cash_usd >= size_usd:
                        cash_usd -= size_usd
                        base_qty += qty
                        avg_entry = close if base_qty <= qty else (((base_qty - qty) * avg_entry) + (qty * close)) / base_qty
                        state = 1
                        entry_bar = idx
                        entry_ts = bar["ts"]
                        last_buy_bar = idx
                        trade_log.append(
                            {
                                "action": "BUY",
                                "price": round(close, 8),
                                "ts": bar["ts"],
                                "bar_index": idx,
                                "conviction_score": conviction_score,
                            }
                        )

            elif state == 1 and trim_cooldown_ok:
                trim_signal = False
                if rsi_value >= _safe_float(params_base["rsi_trim_thresh"], 82.0):
                    trim_signal = prev_bar is not None and rsi_value < _safe_float(prev_bar.get("rsi"), rsi_value)
                if prev_hist is not None and hist_value < prev_hist and hist_value > 0:
                    trim_signal = True

                if trim_signal and base_qty > 0:
                    sell_qty = base_qty * 0.5
                    proceeds = sell_qty * close
                    pnl_usd = (close - avg_entry) * sell_qty
                    realized_pnl += pnl_usd
                    cash_usd += proceeds
                    base_qty -= sell_qty
                    last_trim_bar = idx
                    state = 2 if base_qty > 0 else 0
                    trade_log.append(
                        {
                            "action": "TRIM",
                            "price": round(close, 8),
                            "ts": bar["ts"],
                            "bar_index": idx,
                            "conviction_score": conviction_score,
                            "pnl_usd": round(pnl_usd, 2),
                            "hold_bars": 0 if entry_bar is None else idx - entry_bar,
                        }
                    )

            if state in {1, 2} and base_qty > 0:
                exit_signal = close < ema_mid_value or hist_value < 0 or rsi_value < _safe_float(params_base["rsi_exit_thresh"], 38.0)
                if exit_signal:
                    sell_qty = base_qty
                    proceeds = sell_qty * close
                    pnl_usd = (close - avg_entry) * sell_qty
                    realized_pnl += pnl_usd
                    cash_usd += proceeds
                    base_qty = 0.0
                    state = 0
                    trade_log.append(
                        {
                            "action": "EXIT",
                            "price": round(close, 8),
                            "ts": bar["ts"],
                            "bar_index": idx,
                            "conviction_score": conviction_score,
                            "pnl_usd": round(pnl_usd, 2),
                            "hold_bars": 0 if entry_bar is None else idx - entry_bar,
                            "opened_at": entry_ts,
                            "closed_at": bar["ts"],
                        }
                    )
                    avg_entry = 0.0
                    entry_bar = None
                    entry_ts = None

        if candles:
            final_close = _safe_float(candles[-1].get("close"), 0.0)
            final_equity = current_equity(final_close)
            equity_curve.append({"ts": candles[-1]["ts"], "equity": round(final_equity, 2), "close": final_close})
        else:
            final_equity = 1000.0

        result = {
            "trades": trade_log,
            "equity_curve": equity_curve,
            "final_equity": round(final_equity, 2),
            "summary": self.get_summary(
                {
                    "trades": trade_log,
                    "equity_curve": equity_curve,
                    "final_equity": final_equity,
                    "realized_pnl": realized_pnl,
                }
            ),
        }
        self.trade_log = trade_log
        self.equity_curve = equity_curve
        self.position_state = {
            "state": state,
            "base_qty": base_qty,
            "avg_entry": avg_entry,
            "entry_bar": entry_bar,
            "entry_ts": entry_ts,
        }
        return result

    def get_summary(self, result: dict) -> dict:
        trades = list((result or {}).get("trades") or [])
        equity_curve = list((result or {}).get("equity_curve") or [])
        final_equity = _safe_float((result or {}).get("final_equity"), 1000.0)
        realized_segments = [trade for trade in trades if trade.get("action") in {"TRIM", "EXIT"}]
        pnl_values = [_safe_float(trade.get("pnl_usd"), 0.0) for trade in realized_segments]
        wins = [value for value in pnl_values if value > 0]
        losses = [value for value in pnl_values if value < 0]
        hold_bars = [_safe_int(trade.get("hold_bars"), 0) for trade in realized_segments if trade.get("hold_bars") is not None]

        peak = 0.0
        max_drawdown_pct = 0.0
        returns = []
        prev_equity = None
        for point in equity_curve:
            equity = _safe_float(point.get("equity"), 0.0)
            if equity > peak:
                peak = equity
            if peak > 0:
                drawdown_pct = ((peak - equity) / peak) * 100.0
                max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)
            if prev_equity and prev_equity > 0:
                returns.append((equity - prev_equity) / prev_equity)
            prev_equity = equity

        avg_return = sum(returns) / len(returns) if returns else 0.0
        variance = sum((value - avg_return) ** 2 for value in returns) / len(returns) if returns else 0.0
        std_dev = math.sqrt(variance) if variance > 0 else 0.0
        sharpe_estimate = (avg_return / std_dev) * math.sqrt(len(returns)) if std_dev > 0 and returns else 0.0

        gross_wins = sum(wins)
        gross_losses = sum(losses)

        return {
            "total_trades": len(realized_segments),
            "win_rate": round((len(wins) / len(realized_segments)) if realized_segments else 0.0, 4),
            "total_pnl_pct": round(((final_equity - 1000.0) / 1000.0) * 100.0, 4),
            "max_drawdown_pct": round(max_drawdown_pct, 4),
            "profit_factor": round(gross_wins / abs(gross_losses), 4) if gross_losses < 0 else 0.0,
            "avg_hold_bars": round(sum(hold_bars) / len(hold_bars), 2) if hold_bars else 0.0,
            "sharpe_estimate": round(sharpe_estimate, 4),
        }
