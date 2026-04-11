import math
import time
from datetime import datetime, timezone

from execution import get_client


PARAMS_4H_SATELLITE = {
    "bb_period": 20,
    "bb_std": 2.0,
    "rsi_len": 14,
    "rsi_oversold": 30,
    "rsi_overbought": 75,
    "atr_period": 14,
    "atr_stop_mult": 2.0,
    "atr_target_mult": 3.0,
    "volume_breakout_mult": 1.5,
    "min_squeeze_bars": 8,
    "cooldown_bars": 6,
    "position_size_pct": 0.15,
    "fee_pct": 0.006,
    "adx_min": 20,
}

PARAMS_4H_CORE = {
    "bb_period": 20,
    "bb_std": 2.0,
    "rsi_len": 14,
    "rsi_oversold": 35,
    "rsi_overbought": 70,
    "atr_period": 14,
    "atr_stop_mult": 2.5,
    "atr_target_mult": 3.5,
    "volume_breakout_mult": 1.3,
    "min_squeeze_bars": 10,
    "cooldown_bars": 8,
    "position_size_pct": 0.15,
    "fee_pct": 0.006,
    "adx_min": 18,
}

_CORE_ASSETS = {"BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"}
_CANDLE_CACHE = {}


def _log(message):
    print(f"[backtester] {message}")


def _to_dict(x):
    return x.to_dict() if hasattr(x, "to_dict") else x


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return int(default)


def _normalize_product_id(product_id):
    return str(product_id or "").upper().strip()


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
    tf = str(timeframe or "4h").lower().strip()
    if tf == "1h":
        return "ONE_HOUR", 60 * 60
    return "FOUR_HOUR", 4 * 60 * 60


def _default_params_for_product(product_id):
    return dict(PARAMS_4H_CORE if _normalize_product_id(product_id) in _CORE_ASSETS else PARAMS_4H_SATELLITE)


def sma(values, period):
    values = [_safe_float(value, None) if value is not None else None for value in values]
    period = max(1, int(period or 1))
    out = [None] * len(values)
    if len(values) < period:
        return out

    for idx in range(period - 1, len(values)):
        window = values[idx - period + 1: idx + 1]
        if any(value is None for value in window):
            continue
        out[idx] = sum(window) / period
    return out


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

    smoothed_plus = sum(plus_dm[1: period + 1])
    smoothed_minus = sum(minus_dm[1: period + 1])
    smoothed_tr = sum(tr[1: period + 1])
    dx_values = []

    for i in range(period, n):
        if i == period:
            smoothed_plus = sum(plus_dm[1: period + 1])
            smoothed_minus = sum(minus_dm[1: period + 1])
            smoothed_tr = sum(tr[1: period + 1])
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

    atr_val = sum(tr_values[1: period + 1]) / period
    result[period] = atr_val

    for i in range(period + 1, n):
        atr_val = (atr_val * (period - 1) + tr_values[i]) / period
        result[i] = atr_val

    return result


def bollinger_bands(closes, period=20, std_dev=2.0):
    closes = [_safe_float(value, None) if value is not None else None for value in closes]
    period = max(1, int(period or 20))
    std_dev = _safe_float(std_dev, 2.0)
    upper = [None] * len(closes)
    middle = sma(closes, period)
    lower = [None] * len(closes)

    for idx in range(period - 1, len(closes)):
        window = closes[idx - period + 1: idx + 1]
        if any(value is None for value in window) or middle[idx] is None:
            continue
        mean = middle[idx]
        variance = sum((value - mean) ** 2 for value in window) / period
        sigma = math.sqrt(max(variance, 0.0))
        upper[idx] = mean + (sigma * std_dev)
        lower[idx] = mean - (sigma * std_dev)

    return upper, middle, lower


def bb_width(upper, lower, middle):
    out = [None] * len(middle)
    for idx, (up, low, mid) in enumerate(zip(upper, lower, middle)):
        if up is None or low is None or mid in (None, 0):
            continue
        out[idx] = (up - low) / mid
    return out


def bb_percent_b(closes, upper, lower):
    closes = [_safe_float(value, None) if value is not None else None for value in closes]
    out = [None] * len(closes)
    for idx, (close, up, low) in enumerate(zip(closes, upper, lower)):
        if close is None or up is None or low is None:
            continue
        band_range = up - low
        if band_range <= 0:
            out[idx] = 0.5
        else:
            out[idx] = (close - low) / band_range
    return out


class Backtester:
    def __init__(self, product_id: str, timeframe: str = "4h", start_date: str = None, end_date: str = None):
        self.product_id = _normalize_product_id(product_id)
        self.timeframe = str(timeframe or "4h").lower().strip()
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
        self.position_state = {
            "state": 0,
            "base_qty": 0.0,
            "entry_price": 0.0,
            "entry_bar": None,
            "entry_ts": None,
            "max_price_since_entry": 0.0,
            "atr_at_entry": 0.0,
        }
        self.equity_curve = []

    def load_mtf_candles(self):
        return self.load_candles(), []

    def map_htf_trend(self, candles_1h, candles_4h):
        for candle in candles_1h:
            candle["htf_bullish"] = True
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
            params = _default_params_for_product(self.product_id)
            closes = [_safe_float(candle.get("close"), 0.0) for candle in candles]
            highs = [_safe_float(candle.get("high"), 0.0) for candle in candles]
            lows = [_safe_float(candle.get("low"), 0.0) for candle in candles]
            volumes = [_safe_float(candle.get("volume"), 0.0) for candle in candles]

            upper, middle, lower = bollinger_bands(closes, params["bb_period"], params["bb_std"])
            widths = bb_width(upper, lower, middle)
            width_avg_50 = sma(widths, 50)
            pct_b = bb_percent_b(closes, upper, lower)
            atr_values = atr(highs, lows, closes, params["atr_period"])
            adx_values = adx(highs, lows, closes, 14)
            rsi_values = rsi(closes, params["rsi_len"])
            vol_sma = sma(volumes, 20)

            ema_fast = ema(closes, 9)
            ema_mid = ema(closes, 21)
            ema_slow = ema(closes, 50)
            ema_trend = ema(closes, 200)
            macd_line, macd_signal, macd_hist = macd(closes, 12, 26, 9)

            enriched = []
            for idx, candle in enumerate(candles):
                row = dict(candle)
                row["bb_upper"] = upper[idx]
                row["bb_middle"] = middle[idx]
                row["bb_lower"] = lower[idx]
                row["bb_width"] = widths[idx]
                row["bb_width_avg50"] = width_avg_50[idx]
                row["bb_pctb"] = pct_b[idx]
                row["atr"] = atr_values[idx]
                row["adx"] = adx_values[idx]
                row["rsi"] = rsi_values[idx]
                row["vol_sma"] = vol_sma[idx]
                row["ema_fast"] = ema_fast[idx]
                row["ema_mid"] = ema_mid[idx]
                row["ema_slow"] = ema_slow[idx]
                row["ema_trend"] = ema_trend[idx]
                row["macd_line"] = macd_line[idx]
                row["macd_signal"] = macd_signal[idx]
                row["macd_hist"] = macd_hist[idx]
                enriched.append(row)
            return enriched
        except Exception as exc:
            _log(f"compute_indicators failed product_id={self.product_id} error={exc}")
            return candles

    def run_strategy(self, candles: list, params: dict = None) -> dict:
        params_base = _default_params_for_product(self.product_id)
        params_base.update(params or {})

        trade_log = []
        equity_curve = []
        starting_equity = 1000.0
        cash_usd = starting_equity
        base_qty = 0.0
        state = 0
        entry_price = 0.0
        entry_bar = None
        entry_ts = None
        atr_at_entry = 0.0
        max_price_since_entry = 0.0
        last_exit_bar = -10_000
        realized_pnl = 0.0
        total_fees_paid = 0.0

        def current_equity(last_price):
            return cash_usd + (base_qty * _safe_float(last_price, 0.0))

        def squeeze_detected(idx):
            min_bars = max(1, int(params_base.get("min_squeeze_bars", 8)))
            for offset in range(min_bars):
                j = idx - offset
                if j < 0:
                    return False
                width_val = candles[j].get("bb_width")
                width_avg = candles[j].get("bb_width_avg50")
                if width_val is None or width_avg is None or width_val >= width_avg:
                    return False
            return True

        def conviction_for_bar(bar):
            width_val = _safe_float(bar.get("bb_width"), 0.0)
            width_avg = _safe_float(bar.get("bb_width_avg50"), width_val or 1.0)
            squeeze_sub = max(0.0, min(1.0, (width_avg - width_val) / width_avg)) if width_avg > 0 else 0.0

            vol_sma_val = _safe_float(bar.get("vol_sma"), 0.0)
            volume_ratio = (_safe_float(bar.get("volume"), 0.0) / vol_sma_val) if vol_sma_val > 0 else 1.0
            vol_needed = max(1.0, _safe_float(params_base.get("volume_breakout_mult", 1.5), 1.5))
            volume_sub = max(0.0, min(1.0, (volume_ratio - 1.0) / vol_needed))

            adx_val = _safe_float(bar.get("adx"), 0.0)
            adx_min = _safe_float(params_base.get("adx_min", 20), 20.0)
            adx_sub = max(0.0, min(1.0, (adx_val - adx_min) / max(1.0, 40.0 - adx_min)))

            rsi_val = _safe_float(bar.get("rsi"), 50.0)
            rsi_low = 40.0
            rsi_high = _safe_float(params_base.get("rsi_overbought", 75), 75.0)
            rsi_mid = (rsi_low + rsi_high) / 2.0
            rsi_range = max(1.0, (rsi_high - rsi_low) / 2.0)
            rsi_sub = max(0.0, min(1.0, 1.0 - abs(rsi_val - rsi_mid) / rsi_range))

            conviction = 0.5 + (
                squeeze_sub * 0.25
                + volume_sub * 0.25
                + adx_sub * 0.25
                + rsi_sub * 0.25
            )
            return round(max(0.5, min(1.5, conviction)), 4)

        for idx, bar in enumerate(candles):
            close = _safe_float(bar.get("close"), 0.0)
            if close <= 0:
                continue

            equity_curve.append({"ts": _safe_int(bar.get("ts"), 0), "equity": round(current_equity(close), 2), "close": close})

            required = ["bb_upper", "bb_middle", "bb_lower", "bb_width", "bb_width_avg50", "bb_pctb", "atr", "adx", "rsi", "vol_sma"]
            if any(bar.get(key) is None for key in required):
                continue

            bb_upper = _safe_float(bar.get("bb_upper"), 0.0)
            bb_middle = _safe_float(bar.get("bb_middle"), 0.0)
            bb_pctb = _safe_float(bar.get("bb_pctb"), 0.0)
            rsi_val = _safe_float(bar.get("rsi"), 50.0)
            adx_val = _safe_float(bar.get("adx"), 0.0)
            atr_val = _safe_float(bar.get("atr"), 0.0)
            volume = _safe_float(bar.get("volume"), 0.0)
            vol_sma_val = _safe_float(bar.get("vol_sma"), 0.0)
            cooldown_ok = (idx - last_exit_bar) >= max(1, int(params_base.get("cooldown_bars", 6)))

            squeeze_ok = squeeze_detected(idx - 1)
            breakout = close > bb_upper and bb_pctb > 1.0
            volume_ok = volume > (_safe_float(params_base.get("volume_breakout_mult", 1.5), 1.5) * vol_sma_val if vol_sma_val > 0 else 0.0)
            rsi_ok = 40.0 <= rsi_val <= _safe_float(params_base.get("rsi_overbought", 75), 75.0)
            adx_ok = adx_val >= _safe_float(params_base.get("adx_min", 20), 20.0)
            conviction_score = conviction_for_bar(bar)

            if state == 0 and cooldown_ok and squeeze_ok and breakout and volume_ok and rsi_ok and adx_ok:
                base_pct = _safe_float(params_base.get("position_size_pct", 0.15), 0.15)
                position_pct = base_pct
                size_usd = current_equity(close) * position_pct
                buy_fee = size_usd * _safe_float(params_base.get("fee_pct", 0.006), 0.006) * 0.5
                actual_buy = size_usd - buy_fee
                qty = actual_buy / close if close > 0 else 0.0
                if qty > 0 and cash_usd >= size_usd:
                    cash_usd -= size_usd
                    total_fees_paid += buy_fee
                    base_qty = qty
                    entry_price = close
                    entry_bar = idx
                    entry_ts = _safe_int(bar.get("ts"), 0)
                    atr_at_entry = atr_val
                    max_price_since_entry = close
                    state = 1
                    trade_log.append(
                        {
                            "action": "BUY",
                            "price": round(close, 8),
                            "ts": entry_ts,
                            "bar_index": idx,
                            "conviction_score": conviction_score,
                            "position_pct": round(position_pct, 4),
                            "fee_usd": round(buy_fee, 2),
                        }
                    )
                continue

            if state == 1 and base_qty > 0:
                max_price_since_entry = max(max_price_since_entry, close)
                atr_for_exit = atr_val if atr_val > 0 else atr_at_entry
                stop_price = entry_price - (_safe_float(params_base.get("atr_stop_mult", 2.0), 2.0) * atr_for_exit)
                target_price = entry_price + (_safe_float(params_base.get("atr_target_mult", 3.0), 3.0) * atr_for_exit)
                trailing_stop = max_price_since_entry - (_safe_float(params_base.get("atr_target_mult", 3.0), 3.0) * atr_for_exit)

                stop_loss_hit = atr_for_exit > 0 and close < stop_price
                target_hit = atr_for_exit > 0 and close > target_price
                trailing_stop_hit = atr_for_exit > 0 and max_price_since_entry > entry_price and close < trailing_stop
                rsi_exhaustion = rsi_val > _safe_float(params_base.get("rsi_overbought", 75), 75.0) and close < bb_upper

                exit_reason = None
                if stop_loss_hit:
                    exit_reason = "stop_loss"
                elif target_hit:
                    exit_reason = "target_hit"
                elif trailing_stop_hit:
                    exit_reason = "trailing_stop"
                elif rsi_exhaustion:
                    exit_reason = "rsi_exhaustion"

                if exit_reason:
                    sell_qty = base_qty
                    position_value = sell_qty * entry_price
                    proceeds = sell_qty * close
                    gross_pnl = ((close - entry_price) / entry_price) * position_value if entry_price > 0 else 0.0
                    fee_cost = position_value * _safe_float(params_base.get("fee_pct", 0.006), 0.006)
                    net_pnl = gross_pnl - fee_cost
                    realized_pnl += net_pnl
                    total_fees_paid += fee_cost * 0.5
                    cash_usd += (proceeds - (fee_cost * 0.5))
                    base_qty = 0.0
                    state = 0
                    last_exit_bar = idx
                    trade_log.append(
                        {
                            "action": "EXIT",
                            "price": round(close, 8),
                            "ts": _safe_int(bar.get("ts"), 0),
                            "bar_index": idx,
                            "conviction_score": conviction_score,
                            "pnl_usd": round(net_pnl, 2),
                            "gross_pnl_usd": round(gross_pnl, 2),
                            "fee_usd": round(fee_cost * 0.5, 2),
                            "hold_bars": 0 if entry_bar is None else idx - entry_bar,
                            "opened_at": entry_ts,
                            "closed_at": _safe_int(bar.get("ts"), 0),
                            "exit_reason": exit_reason,
                        }
                    )
                    entry_price = 0.0
                    entry_bar = None
                    entry_ts = None
                    atr_at_entry = 0.0
                    max_price_since_entry = 0.0

        if candles:
            final_close = _safe_float(candles[-1].get("close"), 0.0)
            final_equity = current_equity(final_close)
            equity_curve.append({"ts": _safe_int(candles[-1].get("ts"), 0), "equity": round(final_equity, 2), "close": final_close})
        else:
            final_equity = starting_equity

        result = {
            "trades": trade_log,
            "equity_curve": equity_curve,
            "final_equity": round(final_equity, 2),
            "total_fees_paid": round(total_fees_paid, 2),
            "summary": self.get_summary(
                {
                    "trades": trade_log,
                    "equity_curve": equity_curve,
                    "final_equity": final_equity,
                    "total_fees_paid": total_fees_paid,
                }
            ),
        }
        self.trade_log = trade_log
        self.equity_curve = equity_curve
        self.position_state = {
            "state": state,
            "base_qty": base_qty,
            "entry_price": entry_price,
            "entry_bar": entry_bar,
            "entry_ts": entry_ts,
            "max_price_since_entry": max_price_since_entry,
            "atr_at_entry": atr_at_entry,
        }
        return result

    def get_summary(self, result: dict) -> dict:
        trades = list((result or {}).get("trades") or [])
        equity_curve = list((result or {}).get("equity_curve") or [])
        final_equity = _safe_float((result or {}).get("final_equity"), 1000.0)
        total_fees_paid = _safe_float((result or {}).get("total_fees_paid"), 0.0)
        gross_final_equity = final_equity + total_fees_paid

        completed_trades = [trade for trade in trades if trade.get("action") == "EXIT"]
        pnl_values = [_safe_float(trade.get("pnl_usd"), 0.0) for trade in completed_trades]
        wins = [value for value in pnl_values if value > 0]
        losses = [value for value in pnl_values if value < 0]
        hold_bars = [_safe_int(trade.get("hold_bars"), 0) for trade in completed_trades if trade.get("hold_bars") is not None]

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
            "total_trades": len(completed_trades),
            "win_rate": round((len(wins) / len(completed_trades)) if completed_trades else 0.0, 4),
            "total_pnl_pct": round(((final_equity - 1000.0) / 1000.0) * 100.0, 4),
            "gross_return_pct": round(((gross_final_equity - 1000.0) / 1000.0) * 100.0, 4),
            "net_return_pct": round(((final_equity - 1000.0) / 1000.0) * 100.0, 4),
            "total_fees": round(total_fees_paid, 2),
            "max_drawdown_pct": round(max_drawdown_pct, 4),
            "profit_factor": round(gross_wins / abs(gross_losses), 4) if gross_losses < 0 else 0.0,
            "avg_hold_bars": round(sum(hold_bars) / len(hold_bars), 2) if hold_bars else 0.0,
            "sharpe_estimate": round(sharpe_estimate, 4),
        }
