import math
import time
from datetime import datetime, timedelta, timezone

from execution import get_client


PARAMS_4H_CORE = {
    "ema_fast": 21,
    "ema_mid": 50,
    "ema_slow": 200,
    "ema_trend": 200,
    "rsi_buy_min": 35,
    "rsi_buy_max": 70,
    "rsi_trim_thresh": 80,
    "rsi_exit_thresh": 40,
    "cooldown_bars": 4,
    "trim_cooldown": 8,
    "ext_pct": 8.0,
    "position_size_pct": 0.10,
    "adx_min": 20,
    "atr_stop_mult": 1.5,
    "min_hold_bars": 8,
    "require_aligned_bars": 2,
    "use_mtf": False,
}

PARAMS_1H_SATELLITE = {
    "ema_fast": 9,
    "ema_mid": 21,
    "ema_slow": 50,
    "ema_trend": 200,
    "rsi_buy_min": 45,
    "rsi_buy_max": 75,
    "rsi_trim_thresh": 82,
    "rsi_exit_thresh": 38,
    "cooldown_bars": 3,
    "trim_cooldown": 4,
    "ext_pct": 10.0,
    "position_size_pct": 0.10,
    "adx_min": 20,
    "atr_stop_mult": 1.5,
    "min_hold_bars": 8,
    "require_aligned_bars": 2,
    "use_mtf": True,
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


def adx(highs, lows, closes, period=14):
    """
    Compute ADX (trend strength). Returns list same length as input, None for insufficient data.
    ADX > 25 = trending market, ADX < 20 = choppy market.
    """
    n = len(closes)
    result = [None] * n
    period = max(1, int(period or 14))
    if n < period * 2 + 1:
        return result

    highs = [_safe_float(value, 0.0) for value in highs]
    lows = [_safe_float(value, 0.0) for value in lows]
    closes = [_safe_float(value, 0.0) for value in closes]

    plus_dm = [0.0] * n
    minus_dm = [0.0] * n
    tr = [0.0] * n

    for i in range(1, n):
        high_diff = highs[i] - highs[i - 1]
        low_diff = lows[i - 1] - lows[i]
        plus_dm[i] = max(high_diff, 0.0) if high_diff > low_diff else 0.0
        minus_dm[i] = max(low_diff, 0.0) if low_diff > high_diff else 0.0
        tr[i] = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )

    smoothed_plus = sum(plus_dm[1 : period + 1])
    smoothed_minus = sum(minus_dm[1 : period + 1])
    smoothed_tr = sum(tr[1 : period + 1])
    dx_values = []

    for i in range(period, n):
        if i == period:
            smoothed_plus = sum(plus_dm[1 : period + 1])
            smoothed_minus = sum(minus_dm[1 : period + 1])
            smoothed_tr = sum(tr[1 : period + 1])
        else:
            smoothed_plus = smoothed_plus - (smoothed_plus / period) + plus_dm[i]
            smoothed_minus = smoothed_minus - (smoothed_minus / period) + minus_dm[i]
            smoothed_tr = smoothed_tr - (smoothed_tr / period) + tr[i]

        if smoothed_tr > 0:
            plus_di = 100.0 * smoothed_plus / smoothed_tr
            minus_di = 100.0 * smoothed_minus / smoothed_tr
        else:
            plus_di = 0.0
            minus_di = 0.0

        di_sum = plus_di + minus_di
        dx = 100.0 * abs(plus_di - minus_di) / di_sum if di_sum > 0 else 0.0
        dx_values.append(dx)

    if len(dx_values) >= period:
        adx_val = sum(dx_values[:period]) / period
        result[period * 2 - 1] = adx_val
        for j in range(period, len(dx_values)):
            adx_val = (adx_val * (period - 1) + dx_values[j]) / period
            idx = j + period
            if idx < n:
                result[idx] = adx_val

    return result


def atr(highs, lows, closes, period=14):
    """
    Compute ATR for volatility-based stops. Returns list same length as input.
    """
    n = len(closes)
    result = [None] * n
    period = max(1, int(period or 14))
    if n < period + 1:
        return result

    highs = [_safe_float(value, 0.0) for value in highs]
    lows = [_safe_float(value, 0.0) for value in lows]
    closes = [_safe_float(value, 0.0) for value in closes]
    tr_values = [0.0] * n

    for i in range(1, n):
        tr_values[i] = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )

    atr_val = sum(tr_values[1 : period + 1]) / period
    result[period] = atr_val

    for i in range(period + 1, n):
        atr_val = (atr_val * (period - 1) + tr_values[i]) / period
        result[i] = atr_val

    return result


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

    def _load_candles_for_timeframe(self, timeframe: str) -> list:
        original_timeframe = self.timeframe
        original_granularity = self.granularity
        original_bar_seconds = self.bar_seconds
        try:
            self.timeframe = str(timeframe or "1h").lower().strip()
            self.granularity, self.bar_seconds = _granularity_for_timeframe(self.timeframe)
            return self.load_candles()
        finally:
            self.timeframe = original_timeframe
            self.granularity = original_granularity
            self.bar_seconds = original_bar_seconds

    def load_mtf_candles(self):
        """Load both 1H and 4H candles for multi-timeframe analysis."""
        candles_1h = self._load_candles_for_timeframe("1h")
        candles_4h = self._load_candles_for_timeframe("4h")
        return candles_1h, candles_4h

    def map_htf_trend(self, candles_1h, candles_4h):
        """
        For each 1H candle, determine if the 4H chart is bullish.
        A 4H bar is bullish when:
          - 4H close > 4H EMA 50
          - 4H EMA 21 > 4H EMA 50
          - 4H RSI > 45

        Map each 1H bar to the most recent completed 4H bar.
        Add 'htf_bullish' boolean to each 1H candle.
        """
        if not candles_1h:
            return candles_1h

        closes_4h = [_safe_float(candle.get("close"), 0.0) for candle in candles_4h]
        ema21_4h = ema(closes_4h, 21)
        ema50_4h = ema(closes_4h, 50)
        rsi_4h = rsi(closes_4h, 14)

        htf_states = []
        for i, candle in enumerate(candles_4h):
            if ema21_4h[i] is None or ema50_4h[i] is None or rsi_4h[i] is None:
                htf_states.append({"ts": _safe_int(candle.get("ts"), 0), "bullish": False})
            else:
                close_4h = _safe_float(candle.get("close"), 0.0)
                bullish = close_4h > ema50_4h[i] and ema21_4h[i] > ema50_4h[i] and rsi_4h[i] > 45
                htf_states.append({"ts": _safe_int(candle.get("ts"), 0), "bullish": bullish})

        if not htf_states:
            for candle in candles_1h:
                candle["htf_bullish"] = False
            return candles_1h

        htf_idx = 0
        for candle in candles_1h:
            ts = _safe_int(candle.get("ts"), 0)
            while htf_idx < len(htf_states) - 1 and _safe_int(htf_states[htf_idx + 1]["ts"], 0) <= ts:
                htf_idx += 1
            candle["htf_bullish"] = bool(htf_states[htf_idx]["bullish"]) if htf_idx < len(htf_states) else False

        return candles_1h

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
            highs = [_safe_float(candle.get("high"), 0.0) for candle in candles]
            lows = [_safe_float(candle.get("low"), 0.0) for candle in candles]
            ema_fast = ema(closes, params["ema_fast"])
            ema_mid = ema(closes, params["ema_mid"])
            ema_slow = ema(closes, params["ema_slow"])
            ema_trend = ema(closes, params["ema_trend"])
            rsi_values = rsi(closes, 14)
            macd_line, signal_line, histogram = macd(closes, 12, 26, 9)
            adx_values = adx(highs, lows, closes, 14)
            atr_values = atr(highs, lows, closes, 14)

            enriched = []
            for idx, candle in enumerate(candles):
                row = dict(candle)
                row["ema_fast"] = ema_fast[idx]
                row["ema_mid"] = ema_mid[idx]
                row["ema_slow"] = ema_slow[idx]
                row["ema_trend"] = ema_trend[idx]
                row["rsi"] = rsi_values[idx]
                row["macd_line"] = macd_line[idx]
                row["macd_signal"] = signal_line[idx]
                row["macd_hist"] = histogram[idx]
                row["adx"] = adx_values[idx]
                row["atr"] = atr_values[idx]
                enriched.append(row)
            return enriched
        except Exception as exc:
            _log(f"compute_indicators failed product_id={self.product_id} error={exc}")
            return candles

    def run_strategy(self, candles: list, params: dict = None) -> dict:
        params_base = dict(PARAMS_4H_CORE if self.timeframe == "4h" else PARAMS_1H_SATELLITE)
        params_base.update(params or {})
        if self.timeframe == "1h" and params_base.get("use_mtf", True) and candles and "htf_bullish" not in candles[0]:
            try:
                mtf_1h, mtf_4h = self.load_mtf_candles()
                if mtf_1h and mtf_4h:
                    self.map_htf_trend(mtf_1h, mtf_4h)
                    htf_lookup = {int(row.get("ts", 0)): bool(row.get("htf_bullish", False)) for row in mtf_1h}
                    for candle in candles:
                        candle["htf_bullish"] = htf_lookup.get(_safe_int(candle.get("ts"), 0), True)
            except Exception as exc:
                _log(f"mtf mapping failed product_id={self.product_id} error={exc}")

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
            ema_trend_value = bar.get("ema_trend")
            rsi_value = bar.get("rsi")
            hist_value = bar.get("macd_hist")
            adx_value = bar.get("adx")
            atr_value = bar.get("atr")
            prev_hist = prev_bar.get("macd_hist") if prev_bar else None

            equity_curve.append({"ts": bar["ts"], "equity": round(current_equity(close), 2), "close": close})

            if None in (ema_fast_value, ema_mid_value, ema_slow_value, ema_trend_value, rsi_value, hist_value):
                continue

            ema_aligned = ema_fast_value > ema_mid_value > ema_slow_value
            price_above_fast = close > ema_fast_value
            above_trend = close > ema_trend_value
            macd_rising = prev_hist is not None and hist_value > 0 and hist_value > prev_hist
            not_overextended = ((close - ema_fast_value) / ema_fast_value) * 100.0 <= _safe_float(params_base["ext_pct"], 10.0)
            cooldown_ok = (idx - last_buy_bar) >= int(params_base["cooldown_bars"])
            trim_cooldown_ok = (idx - last_trim_bar) >= int(params_base["trim_cooldown"])
            conviction_score = conviction_for_bar(bar, prev_bar)
            adx_ok = adx_value is not None and _safe_float(adx_value, 0.0) >= _safe_float(params_base.get("adx_min", 20), 20.0)
            htf_ok = bool(bar.get("htf_bullish", True))

            required_aligned = max(1, int(params_base.get("require_aligned_bars", 2)))
            aligned_count = 0
            for lookback in range(required_aligned):
                lb_idx = idx - lookback
                if lb_idx < 0:
                    break
                lb = candles[lb_idx]
                lb_ef = lb.get("ema_fast")
                lb_em = lb.get("ema_mid")
                lb_es = lb.get("ema_slow")
                lb_close = _safe_float(lb.get("close"), 0.0)
                if lb_ef is not None and lb_em is not None and lb_es is not None and lb_ef > lb_em > lb_es and lb_close > lb_ef:
                    aligned_count += 1
            consecutive_aligned = aligned_count >= required_aligned

            if state == 0 and cooldown_ok and ema_aligned and price_above_fast and above_trend and macd_rising and not_overextended and adx_ok and consecutive_aligned and htf_ok:
                if _safe_float(params_base["rsi_buy_min"], 0.0) <= rsi_value <= _safe_float(params_base["rsi_buy_max"], 100.0):
                    base_pct = _safe_float(params_base.get("position_size_pct"), 0.10)
                    if conviction_score > 0.8 and _safe_float(bar.get("adx"), 0.0) > 30 and bool(bar.get("htf_bullish", False)):
                        position_pct = base_pct * 1.5
                    else:
                        position_pct = base_pct
                    size_usd = current_equity(close) * position_pct
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
                                "position_pct": round(position_pct, 4),
                                "htf_bullish": bool(bar.get("htf_bullish", False)),
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
                min_hold_bars = max(0, int(params_base.get("min_hold_bars", 8)))
                hold_bars = 0 if entry_bar is None else idx - entry_bar
                can_exit_by_time = hold_bars >= min_hold_bars
                atr_stop_mult = _safe_float(params_base.get("atr_stop_mult", 1.5), 1.5)
                atr_stop_hit = False
                if avg_entry > 0 and _safe_float(atr_value, 0.0) > 0:
                    stop_price = avg_entry - (_safe_float(atr_value, 0.0) * atr_stop_mult)
                    atr_stop_hit = close < stop_price

                momentum_broken = close < ema_mid_value and hist_value < 0 and rsi_value < _safe_float(params_base["rsi_exit_thresh"], 38.0)
                trend_lost = close < ema_trend_value and close < ema_slow_value and rsi_value < 45
                htf_bearish_exit = state in (1, 2) and (not bool(bar.get("htf_bullish", True))) and close < _safe_float(bar.get("ema_mid"), 0.0)
                sig_exit = can_exit_by_time and (momentum_broken or trend_lost or atr_stop_hit or htf_bearish_exit)
                if sig_exit:
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
                            "htf_bullish": bool(bar.get("htf_bullish", False)),
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
