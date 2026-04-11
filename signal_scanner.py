import math
import time

from scoring_engine import ScoringEngine
from execution import get_client
from coinbase_universe import get_all_usd_products
from rebalancer import dispatch_signal_action
from portfolio import get_manual_holds, get_portfolio_snapshot


_CORE_ASSETS = {"BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"}
_SCANNER_STATE = {}
_SIGNAL_LOG = []
_DIP_COOLDOWNS = {}
_SCORING_ENGINE = None

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


def _log(message):
    print(f"[signal_scanner] {message}")


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


def _default_params_for_product(product_id):
    return dict(PARAMS_4H_CORE if _normalize_product_id(product_id) in _CORE_ASSETS else PARAMS_4H_SATELLITE)


def _get_scoring_engine():
    global _SCORING_ENGINE
    if _SCORING_ENGINE is None:
        _SCORING_ENGINE = ScoringEngine()
    return _SCORING_ENGINE


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


def ema(values, period):
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


def rsi(closes, period=14):
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


def macd_histogram(closes, fast=12, slow=26, signal=9):
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
    return histogram


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


class SignalScanner:
    def __init__(self, timeframe="4h", params=None):
        self.timeframe = str(timeframe or "4h").lower().strip()
        self.granularity = "ONE_HOUR" if self.timeframe == "1h" else "FOUR_HOUR"
        self.params = {**PARAMS_4H_SATELLITE, **(params or {})}

    def _to_dict(self, x):
        return x.to_dict() if hasattr(x, "to_dict") else x

    def fetch_candles(self, product_id, limit=250):
        client = get_client()
        end_ts = int(time.time())
        seconds_per_bar = 3600 if self.timeframe == "1h" else 14400
        start_ts = end_ts - (int(limit) * seconds_per_bar)
        product_id = _normalize_product_id(product_id)
        try:
            response = self._to_dict(
                client.get_candles(
                    product_id=product_id,
                    start=str(start_ts),
                    end=str(end_ts),
                    granularity=self.granularity,
                )
            )
            candles = response.get("candles", []) if isinstance(response, dict) else []
            candles = sorted(candles, key=lambda c: int(c.get("start", 0)))
            return [
                {
                    "ts": _safe_int(c.get("start"), 0),
                    "open": _safe_float(c.get("open"), 0.0),
                    "high": _safe_float(c.get("high"), 0.0),
                    "low": _safe_float(c.get("low"), 0.0),
                    "close": _safe_float(c.get("close"), 0.0),
                    "volume": _safe_float(c.get("volume"), 0.0),
                }
                for c in candles
            ]
        except Exception as exc:
            _log(f"fetch_candles failed product_id={product_id} error={exc}")
            return []

    def fetch_candles_4h(self, product_id, limit=250):
        return self.fetch_candles(product_id, limit=limit)

    def _compute_htf_trend(self, candles_4h):
        return True

    def compute_indicators(self, candles):
        params = dict(self.params)
        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        volumes = [c["volume"] for c in candles]

        upper, middle, lower = bollinger_bands(closes, params["bb_period"], params["bb_std"])
        widths = bb_width(upper, lower, middle)
        width_avg_50 = sma(widths, 50)
        pct_b = bb_percent_b(closes, upper, lower)
        atr_vals = atr(highs, lows, closes, params["atr_period"])
        adx_vals = adx(highs, lows, closes, 14)
        rsi_vals = rsi(closes, params["rsi_len"])
        vol_sma_vals = sma(volumes, 20)
        macd_hist_vals = macd_histogram(closes, 12, 26, 9)

        ema_fast_vals = ema(closes, 9)
        ema_mid_vals = ema(closes, 21)
        ema_slow_vals = ema(closes, 50)
        ema_trend_vals = ema(closes, 200)

        for i, candle in enumerate(candles):
            candle["bb_upper"] = upper[i]
            candle["bb_middle"] = middle[i]
            candle["bb_lower"] = lower[i]
            candle["bb_width"] = widths[i]
            candle["bb_width_avg50"] = width_avg_50[i]
            candle["bb_pctb"] = pct_b[i]
            candle["atr"] = atr_vals[i]
            candle["adx"] = adx_vals[i]
            candle["rsi"] = rsi_vals[i]
            candle["vol_sma"] = vol_sma_vals[i]
            candle["macd_hist"] = macd_hist_vals[i]
            candle["ema_fast"] = ema_fast_vals[i]
            candle["ema_mid"] = ema_mid_vals[i]
            candle["ema_slow"] = ema_slow_vals[i]
            candle["ema_trend"] = ema_trend_vals[i]
        return candles

    def evaluate_product(self, product_id, candles, **kwargs):
        product_id = _normalize_product_id(product_id)
        if len(candles) < 3:
            return {"signal": None, "product_id": product_id}

        params = _default_params_for_product(product_id)
        params.update(self.params or {})
        bar = candles[-2]

        required = ["bb_upper", "bb_middle", "bb_lower", "bb_width", "bb_width_avg50", "bb_pctb", "atr", "adx", "rsi", "vol_sma"]
        if any(bar.get(key) is None for key in required):
            return {"signal": None, "product_id": product_id}

        state = _SCANNER_STATE.setdefault(
            product_id,
            {
                "state": 0,
                "bars_since_exit": 999,
                "entry_price": 0.0,
                "entry_bar": None,
                "entry_ts": None,
                "max_price_since_entry": 0.0,
                "atr_at_entry": 0.0,
            },
        )

        close = _safe_float(bar.get("close"), 0.0)
        bb_upper = _safe_float(bar.get("bb_upper"), 0.0)
        bb_middle = _safe_float(bar.get("bb_middle"), 0.0)
        bb_pctb = _safe_float(bar.get("bb_pctb"), 0.0)
        bb_width_val = _safe_float(bar.get("bb_width"), 0.0)
        bb_width_avg = _safe_float(bar.get("bb_width_avg50"), 0.0)
        atr_val = _safe_float(bar.get("atr"), 0.0)
        adx_val = _safe_float(bar.get("adx"), 0.0)
        rsi_val = _safe_float(bar.get("rsi"), 50.0)
        volume = _safe_float(bar.get("volume"), 0.0)
        vol_sma_val = _safe_float(bar.get("vol_sma"), 0.0)

        def squeeze_detected():
            min_bars = max(1, int(params.get("min_squeeze_bars", 8)))
            bar_index = len(candles) - 2
            for offset in range(min_bars):
                idx = bar_index - offset
                if idx < 0:
                    return False
                width = candles[idx].get("bb_width")
                width_avg50 = candles[idx].get("bb_width_avg50")
                if width is None or width_avg50 is None or _safe_float(width, 0.0) >= _safe_float(width_avg50, 0.0):
                    return False
            return True

        squeeze_ok = squeeze_detected()
        breakout = close > bb_upper and bb_pctb > 1.0
        volume_ok = volume > (_safe_float(params.get("volume_breakout_mult", 1.5), 1.5) * vol_sma_val if vol_sma_val > 0 else 0.0)
        rsi_ok = 40.0 <= rsi_val <= _safe_float(params.get("rsi_overbought", 75), 75.0)
        adx_ok = adx_val >= _safe_float(params.get("adx_min", 20), 20.0)

        squeeze_sub = max(0.0, min(1.0, (bb_width_avg - bb_width_val) / bb_width_avg)) if bb_width_avg > 0 else 0.0
        volume_ratio = (volume / vol_sma_val) if vol_sma_val > 0 else 1.0
        volume_sub = max(0.0, min(1.0, (volume_ratio - 1.0) / max(1.0, _safe_float(params.get("volume_breakout_mult", 1.5), 1.5))))
        adx_sub = max(0.0, min(1.0, (adx_val - _safe_float(params.get("adx_min", 20), 20.0)) / max(1.0, 40.0 - _safe_float(params.get("adx_min", 20), 20.0))))
        rsi_mid = (40.0 + _safe_float(params.get("rsi_overbought", 75), 75.0)) / 2.0
        rsi_range = max(1.0, (_safe_float(params.get("rsi_overbought", 75), 75.0) - 40.0) / 2.0)
        rsi_sub = max(0.0, min(1.0, 1.0 - abs(rsi_val - rsi_mid) / rsi_range))
        conviction = max(0.5, min(1.5, 0.5 + (squeeze_sub * 0.25 + volume_sub * 0.25 + adx_sub * 0.25 + rsi_sub * 0.25)))

        signal = None
        state["bars_since_exit"] = min(999, int(state.get("bars_since_exit", 999)) + 1)
        current_state = int(state.get("state", 0))

        if current_state == 0:
            if (
                int(state.get("bars_since_exit", 999)) >= int(params.get("cooldown_bars", 6))
                and squeeze_ok
                and breakout
                and volume_ok
                and rsi_ok
                and adx_ok
            ):
                signal = "buy"
                state["state"] = 1
                state["entry_price"] = close
                state["entry_bar"] = len(candles) - 2
                state["entry_ts"] = _safe_int(bar.get("ts"), 0)
                state["max_price_since_entry"] = close
                state["atr_at_entry"] = atr_val
        elif current_state == 1:
            state["max_price_since_entry"] = max(_safe_float(state.get("max_price_since_entry"), close), close)
            entry_price = _safe_float(state.get("entry_price"), 0.0)
            atr_for_exit = atr_val if atr_val > 0 else _safe_float(state.get("atr_at_entry"), 0.0)
            stop_price = entry_price - (_safe_float(params.get("atr_stop_mult", 2.0), 2.0) * atr_for_exit)
            target_price = entry_price + (_safe_float(params.get("atr_target_mult", 3.0), 3.0) * atr_for_exit)
            trailing_stop = _safe_float(state.get("max_price_since_entry"), close) - (_safe_float(params.get("atr_target_mult", 3.0), 3.0) * atr_for_exit)

            stop_loss_hit = atr_for_exit > 0 and close < stop_price
            target_hit = atr_for_exit > 0 and close > target_price
            trailing_stop_hit = atr_for_exit > 0 and _safe_float(state.get("max_price_since_entry"), close) > entry_price and close < trailing_stop
            rsi_exhaustion = rsi_val > _safe_float(params.get("rsi_overbought", 75), 75.0) and close < bb_upper

            if stop_loss_hit or target_hit or trailing_stop_hit or rsi_exhaustion:
                signal = "exit"
                state["state"] = 0
                state["bars_since_exit"] = 0
                state["entry_price"] = 0.0
                state["entry_bar"] = None
                state["entry_ts"] = None
                state["max_price_since_entry"] = 0.0
                state["atr_at_entry"] = 0.0

        result = {
            "signal": signal,
            "product_id": product_id,
            "price": close,
            "conviction": round(conviction, 2),
            "indicators": {
                "rsi": round(rsi_val, 2),
                "macd_hist": round(_safe_float(bar.get("macd_hist"), 0.0), 6) if bar.get("macd_hist") is not None else None,
                "ema_fast": round(_safe_float(bar.get("ema_fast"), 0.0), 4) if bar.get("ema_fast") is not None else None,
                "ema_mid": round(_safe_float(bar.get("ema_mid"), 0.0), 4) if bar.get("ema_mid") is not None else None,
                "ema_slow": round(_safe_float(bar.get("ema_slow"), 0.0), 4) if bar.get("ema_slow") is not None else None,
                "ema_trend": round(_safe_float(bar.get("ema_trend"), 0.0), 4) if bar.get("ema_trend") is not None else None,
                "bb_upper": round(bb_upper, 4),
                "bb_middle": round(bb_middle, 4),
                "bb_lower": round(_safe_float(bar.get("bb_lower"), 0.0), 4),
                "bb_width": round(bb_width_val, 6),
                "bb_pctb": round(bb_pctb, 4),
                "volume_ratio": round(volume_ratio, 2),
                "adx": round(adx_val, 4) if adx_val > 0 else None,
                "atr": round(atr_val, 6) if atr_val > 0 else None,
            },
            "state": state["state"],
        }

        if signal:
            _log(f"{signal.upper()} {product_id} price={close:.4f} conviction={conviction:.2f} rsi={rsi_val:.1f}")
            _SIGNAL_LOG.append(
                {
                    "ts": int(time.time()),
                    "product_id": product_id,
                    "signal": signal,
                    "price": close,
                    "conviction": conviction,
                    "indicators": result["indicators"],
                }
            )
            if len(_SIGNAL_LOG) > 500:
                _SIGNAL_LOG[:] = _SIGNAL_LOG[-500:]

        return result

    def scan_universe(self):
        snapshot = get_portfolio_snapshot()
        config = snapshot.get("config", {})
        regime = str(
            snapshot.get("market_regime")
            or snapshot.get("config", {}).get("market_regime")
            or "neutral"
        ).lower()
        allowed_regimes = ["bull"]
        if regime not in allowed_regimes:
            _log(f"satellite scan skipped — regime={regime} not in {allowed_regimes}")
            return []
        core_assets = set((config.get("core_assets") or {}).keys())
        blocked = set(config.get("satellite_blocked") or [])
        allowed_list = {
            _normalize_product_id(product_id)
            for product_id in (config.get("satellite_allowed") or [])
            if _normalize_product_id(product_id)
        }
        manual_holds = set(get_manual_holds(snapshot))
        held_positions = {
            _normalize_product_id(product_id): (position or {})
            for product_id, position in (snapshot.get("positions") or {}).items()
            if _normalize_product_id(product_id) not in core_assets
            and _safe_float((position or {}).get("value_total_usd"), 0.0) > 1.0
        }
        held_satellites = sorted(held_positions.keys())
        universe = set(get_all_usd_products()) | allowed_list | set(held_satellites)
        targets = sorted(universe - core_assets - blocked)
        engine = _get_scoring_engine()
        max_satellites = int(config.get("max_active_satellites", 4) or 4)
        all_candles = {}
        ranked = []
        ranked_by_id = {}

        dispatched = []
        for product_id in targets:
            try:
                candles = self.fetch_candles(product_id)
                if len(candles) < 60:
                    continue
                all_candles[product_id] = candles
            except Exception as exc:
                _log(f"scan preload failed for {product_id}: {exc}")
            time.sleep(0.15)

        try:
            ranked = engine.rank_universe(all_candles, regime, snapshot)
            ranked_by_id = {row.get("product_id"): row for row in ranked if row.get("product_id")}
        except Exception as exc:
            _log(f"universe ranking failed: {exc}")
            ranked = []
            ranked_by_id = {}

        def _append_signal_log(product_id, signal, conviction, score_payload, reason):
            candle_rows = all_candles.get(product_id) or []
            last_price = _safe_float((candle_rows[-1] if candle_rows else {}).get("close"), 0.0)
            _SIGNAL_LOG.append(
                {
                    "ts": int(time.time()),
                    "product_id": product_id,
                    "signal": signal,
                    "price": last_price,
                    "conviction": conviction,
                    "indicators": {
                        "technical_score": _safe_float((score_payload or {}).get("technical_score"), 0.0),
                        "sentiment_score": _safe_float((score_payload or {}).get("sentiment_score"), 0.0),
                        "momentum_score": _safe_float((score_payload or {}).get("momentum_score"), 0.0),
                        "regime_score": _safe_float((score_payload or {}).get("regime_score"), 0.0),
                        "composite_score": _safe_float((score_payload or {}).get("composite_score"), 0.0),
                        "reason": reason,
                    },
                }
            )
            if len(_SIGNAL_LOG) > 500:
                _SIGNAL_LOG[:] = _SIGNAL_LOG[-500:]

        def fire_exit_signal(product_id, reason, score_payload=None):
            dispatch_result = dispatch_signal_action(
                product_id=product_id,
                action="EXIT",
                signal_type="SATELLITE_EXIT",
                timeframe="4h",
                strategy="server_scanner_scoring_engine",
                conviction_score=0.5,
            )
            result = {
                "product_id": product_id,
                "signal": "exit",
                "conviction": 0.5,
                "reason": reason,
                "score": score_payload or {},
                "dispatch": dispatch_result,
            }
            _append_signal_log(product_id, "exit", 0.5, score_payload, reason)
            dispatched.append(result)
            return result

        def fire_buy_signal(product_id, entry_payload, score_payload):
            dispatch_result = dispatch_signal_action(
                product_id=product_id,
                action="BUY",
                signal_type="SATELLITE_BUY",
                timeframe="4h",
                strategy="server_scanner_scoring_engine",
                conviction_score=_safe_float(entry_payload.get("conviction"), 0.5),
            )
            result = {
                "product_id": product_id,
                "signal": "buy",
                "conviction": _safe_float(entry_payload.get("conviction"), 0.5),
                "reason": entry_payload.get("reason"),
                "score": score_payload or {},
                "dispatch": dispatch_result,
            }
            _append_signal_log(product_id, "buy", _safe_float(entry_payload.get("conviction"), 0.5), score_payload, entry_payload.get("reason"))
            dispatched.append(result)
            return result

        active_satellites = {pid for pid in held_satellites if pid not in manual_holds}
        for held_pid in held_satellites:
            if held_pid in manual_holds:
                _log(f"held satellite {held_pid} remains operator-controlled")
                continue
            score = ranked_by_id.get(held_pid)
            candles = all_candles.get(held_pid)
            if not candles:
                continue
            if not score or _safe_float(score.get("composite_score"), 0.0) < 35.0:
                pos = held_positions.get(held_pid, {})
                exit_check = engine.score_for_exit(
                    held_pid,
                    candles,
                    regime,
                    entry_price=(pos.get("avg_entry") or pos.get("avg_entry_price") or pos.get("entry_price") or 0),
                    bars_held=72,
                )
                if exit_check.get("exit") or not score:
                    fire_exit_signal(held_pid, exit_check.get("reason") or "score_degraded", score)
                    active_satellites.discard(held_pid)

        active_count = len(active_satellites)
        for candidate in ranked:
            product_id = _normalize_product_id(candidate.get("product_id"))
            if active_count >= max_satellites:
                break
            if not product_id or product_id in held_satellites or product_id in core_assets or product_id in manual_holds:
                continue

            candles = all_candles.get(product_id)
            if not candles:
                continue

            if product_id not in allowed_list and _safe_float(candidate.get("composite_score"), 0.0) < 65.0:
                continue

            entry = engine.score_for_entry(product_id, candles, regime)
            if entry.get("enter"):
                fire_buy_signal(product_id, entry, candidate)
                active_count += 1

        _log(f"satellite sweep done scanned={len(targets)} signals={len(dispatched)}")
        return dispatched

    def scan_core(self):
        snapshot = get_portfolio_snapshot()
        config = snapshot.get("config", {})
        core_assets = list((config.get("core_assets") or {}).keys())

        dispatched = []
        for product_id in core_assets:
            try:
                core_scanner = SignalScanner(timeframe="4h", params=PARAMS_4H_CORE)
                candles = core_scanner.fetch_candles(product_id)
                if len(candles) < 60:
                    continue
                candles = core_scanner.compute_indicators(candles)
                result = core_scanner.evaluate_product(product_id, candles)

                if result["signal"]:
                    action_map = {"buy": "BUY", "exit": "EXIT"}
                    signal_map = {"buy": "CORE_BUY_WINDOW", "exit": "CORE_EXIT"}
                    dispatch_result = dispatch_signal_action(
                        product_id=product_id,
                        action=action_map[result["signal"]],
                        signal_type=signal_map[result["signal"]],
                        timeframe="4h",
                        strategy="server_scanner_bb_breakout_core",
                        conviction_score=result["conviction"],
                    )
                    dispatched.append({**result, "dispatch": dispatch_result})
            except Exception as exc:
                _log(f"core scan failed for {product_id}: {exc}")
            time.sleep(0.15)

        _log(f"core sweep done scanned={len(core_assets)} signals={len(dispatched)}")
        return dispatched


def run_scanner_sweep():
    try:
        snapshot = get_portfolio_snapshot()
        regime = str(
            snapshot.get("market_regime")
            or snapshot.get("config", {}).get("market_regime")
            or "neutral"
        ).lower()
        _log(f"sweep starting regime={regime}")
        scanner = SignalScanner(timeframe="4h")
        core_signals = scanner.scan_core()
        satellite_signals = scanner.scan_universe()
        return {
            "ok": True,
            "core_signals": core_signals,
            "satellite_signals": satellite_signals,
            "products_scanned": len(get_all_usd_products()),
        }
    except Exception as exc:
        _log(f"scanner sweep failed: {exc}")
        return {"ok": False, "error": str(exc), "core_signals": [], "satellite_signals": []}


def run_dip_detector():
    """
    Detect sharp drops in core assets and trigger aggressive buys.
    Called every 5 minutes by the scheduler.

    Logic:
    - For each core asset, fetch the last 6 candles (4H each = 24h)
    - If the asset dropped > 5% in the last 24h AND RSI < 35, it's a dip buy opportunity
    - Buy 3x the normal DCA amount (aggressive accumulation at depressed prices)
    - Only trigger in neutral or caution regime (not risk_off — that's capitulation)
    - Cooldown: only one dip buy per asset per 24 hours
    """
    try:
        from portfolio import get_portfolio_snapshot, load_asset_config
        from rebalancer import dispatch_signal_action
        import time

        snapshot = get_portfolio_snapshot()
        if not isinstance(snapshot, dict):
            return {"ok": False, "error": "snapshot_unavailable"}

        config = snapshot.get("config", {})
        regime = str(snapshot.get("market_regime", "neutral")).lower()

        if regime not in ["neutral", "caution", "bull"]:
            return {"ok": True, "actions": [], "regime": regime, "reason": "regime_blocked"}

        core_assets = config.get("core_assets", {})
        total_value = float(snapshot.get("total_value_usd", 0) or 0)
        cash = float(snapshot.get("usd_cash", 0) or 0)
        reserve = total_value * float(config.get("min_cash_reserve", 0.08) or 0.08)
        available = max(0, cash - reserve)

        if available < 10:
            return {"ok": True, "actions": [], "reason": "insufficient_cash"}

        global _DIP_COOLDOWNS
        now = int(time.time())
        actions = []
        client = get_client()

        for product_id in core_assets:
            last_dip = _DIP_COOLDOWNS.get(product_id, 0)
            if (now - last_dip) < 86400:
                continue

            try:
                end_ts = now
                start_ts = end_ts - (6 * 14400)
                response = client.get_candles(
                    product_id=product_id,
                    start=str(start_ts),
                    end=str(end_ts),
                    granularity="FOUR_HOUR",
                )
                data = response.to_dict() if hasattr(response, 'to_dict') else response
                candles = sorted(data.get("candles", []), key=lambda c: int(c.get("start", 0)))

                if len(candles) < 2:
                    continue

                first_close = float(candles[0]["close"])
                last_close = float(candles[-1]["close"])
                change_24h = ((last_close - first_close) / first_close) if first_close > 0 else 0

                closes = [float(c["close"]) for c in candles]
                gains = []
                losses = []
                for i in range(1, len(closes)):
                    diff = closes[i] - closes[i - 1]
                    if diff > 0:
                        gains.append(diff)
                        losses.append(0)
                    else:
                        gains.append(0)
                        losses.append(abs(diff))

                avg_gain = sum(gains) / len(gains) if gains else 0
                avg_loss = sum(losses) / len(losses) if losses else 0.001
                rs = avg_gain / avg_loss if avg_loss > 0 else 100
                rsi_approx = 100 - (100 / (1 + rs))

                _log(f"dip check {product_id}: 24h_change={change_24h*100:.1f}% rsi={rsi_approx:.0f}")

                if change_24h < -0.05 and rsi_approx < 35:
                    normal_dca = total_value * float(config.get("dca_amount_pct", 0.02) or 0.02)
                    dip_buy_amount = min(normal_dca * 3, available * 0.25)

                    if dip_buy_amount < 5:
                        continue

                    _log(
                        f"DIP BUY triggered {product_id} drop={change_24h*100:.1f}% "
                        f"rsi={rsi_approx:.0f} amount=${dip_buy_amount:.2f}"
                    )

                    result = dispatch_signal_action(
                        product_id=product_id,
                        action="BUY",
                        signal_type="DIP_BUY",
                        timeframe="4h",
                        strategy="dip_detector",
                        quote_size=dip_buy_amount,
                        conviction_score=1.3,
                    )

                    if result.get("ok"):
                        _DIP_COOLDOWNS[product_id] = now
                        actions.append({
                            "product_id": product_id,
                            "amount": dip_buy_amount,
                            "change_24h": round(change_24h * 100, 2),
                            "rsi": round(rsi_approx, 1),
                        })

            except Exception as exc:
                _log(f"dip check failed {product_id}: {exc}")
                continue

        return {"ok": True, "actions": actions, "regime": regime}

    except Exception as exc:
        _log(f"dip detector failed: {exc}")
        return {"ok": False, "error": str(exc)}


def get_signal_log(limit=100):
    return list(reversed(_SIGNAL_LOG[-int(limit):]))


def get_scanner_state():
    return dict(_SCANNER_STATE)


def get_scanner_params():
    return {
        "satellite_1h": dict(PARAMS_4H_SATELLITE),
        "satellite_4h": dict(PARAMS_4H_SATELLITE),
        "core_4h": dict(PARAMS_4H_CORE),
    }


def update_scanner_params(preset, params):
    global PARAMS_4H_SATELLITE, PARAMS_4H_CORE
    params = params if isinstance(params, dict) else {}
    if preset in {"satellite_1h", "satellite_4h"}:
        PARAMS_4H_SATELLITE.update(params)
        return dict(PARAMS_4H_SATELLITE)
    if preset == "core_4h":
        PARAMS_4H_CORE.update(params)
        return dict(PARAMS_4H_CORE)
    return {}
