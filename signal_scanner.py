import time

from execution import get_client
from coinbase_universe import get_all_usd_products
from rebalancer import dispatch_signal_action
from portfolio import get_portfolio_snapshot


_SCANNER_STATE = {}
_SIGNAL_LOG = []

PARAMS_1H_SATELLITE = {
    "ema_fast": 9,
    "ema_mid": 21,
    "ema_slow": 50,
    "ema_trend": 200,
    "rsi_len": 14,
    "rsi_buy_min": 45,
    "rsi_buy_max": 75,
    "rsi_trim_thresh": 82,
    "rsi_exit_thresh": 38,
    "ext_pct": 10.0,
    "cooldown_bars": 3,
    "trim_cooldown": 4,
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_sig": 9,
    "vol_sma_len": 20,
    "vol_mult": 1.0,
}

PARAMS_4H_CORE = {
    "ema_fast": 21,
    "ema_mid": 50,
    "ema_slow": 200,
    "ema_trend": 200,
    "rsi_len": 14,
    "rsi_buy_min": 35,
    "rsi_buy_max": 70,
    "rsi_trim_thresh": 80,
    "rsi_exit_thresh": 40,
    "ext_pct": 8.0,
    "cooldown_bars": 4,
    "trim_cooldown": 8,
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_sig": 9,
    "vol_sma_len": 20,
    "vol_mult": 1.0,
}


def _log(message):
    print(f"[signal_scanner] {message}")


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


def _normalize_product_id(product_id):
    return str(product_id or "").upper().strip()


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
            out[index] = None
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


def sma(values, period):
    values = [_safe_float(value, None) if value is not None else None for value in values]
    period = max(1, int(period or 1))
    out = [None] * len(values)
    if len(values) < period:
        return out

    for idx in range(period - 1, len(values)):
        window = values[idx - period + 1:idx + 1]
        if any(value is None for value in window):
            out[idx] = None
            continue
        out[idx] = sum(window) / period
    return out


class SignalScanner:
    def __init__(self, timeframe="1h", params=None):
        self.timeframe = str(timeframe or "1h").lower().strip()
        self.granularity = "ONE_HOUR" if self.timeframe == "1h" else "FOUR_HOUR"
        base_params = PARAMS_1H_SATELLITE if self.timeframe == "1h" else PARAMS_4H_CORE
        self.params = {**base_params, **(params or {})}

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
                    "ts": int(c["start"]),
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                    "volume": float(c.get("volume", 0)),
                }
                for c in candles
            ]
        except Exception as exc:
            _log(f"fetch_candles failed product_id={product_id} error={exc}")
            return []

    def compute_indicators(self, candles):
        p = self.params
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        ema_fast_vals = ema(closes, p["ema_fast"])
        ema_mid_vals = ema(closes, p["ema_mid"])
        ema_slow_vals = ema(closes, p["ema_slow"])
        ema_trend_vals = ema(closes, p["ema_trend"])
        rsi_vals = rsi(closes, p["rsi_len"])
        hist_vals = macd_histogram(closes, p["macd_fast"], p["macd_slow"], p["macd_sig"])
        vol_sma_vals = sma(volumes, p["vol_sma_len"])

        for i, candle in enumerate(candles):
            candle["ema_fast"] = ema_fast_vals[i]
            candle["ema_mid"] = ema_mid_vals[i]
            candle["ema_slow"] = ema_slow_vals[i]
            candle["ema_trend"] = ema_trend_vals[i]
            candle["rsi"] = rsi_vals[i]
            candle["macd_hist"] = hist_vals[i]
            candle["vol_sma"] = vol_sma_vals[i]
        return candles

    def evaluate_product(self, product_id, candles):
        product_id = _normalize_product_id(product_id)
        if len(candles) < 3:
            return {"signal": None, "product_id": product_id}

        p = self.params
        bar = candles[-2]
        prev = candles[-3]

        required = ["ema_fast", "ema_mid", "ema_slow", "ema_trend", "rsi", "macd_hist", "vol_sma"]
        if any(bar.get(key) is None for key in required):
            return {"signal": None, "product_id": product_id}

        state = _SCANNER_STATE.setdefault(product_id, {"state": 0, "bars_since_exit": 999, "bars_since_trim": 999})

        close = _safe_float(bar["close"], 0.0)
        ef = _safe_float(bar["ema_fast"], 0.0)
        em = _safe_float(bar["ema_mid"], 0.0)
        es = _safe_float(bar["ema_slow"], 0.0)
        et = _safe_float(bar["ema_trend"], 0.0)
        r = _safe_float(bar["rsi"], 50.0)
        hist = _safe_float(bar["macd_hist"], 0.0)
        prev_hist = _safe_float(prev.get("macd_hist"), 0.0)
        prev_rsi = _safe_float(prev.get("rsi"), 50.0)
        vol = _safe_float(bar.get("volume"), 0.0)
        vs = _safe_float(bar.get("vol_sma"), 1.0) or 1.0

        above_trend = close > et
        ema_aligned = ef > em and em > es
        price_above = close > ef
        macd_positive = hist > 0
        macd_rising = hist > prev_hist
        ext = ((close - em) / em) * 100.0 if em > 0 else 0.0
        not_overextended = 0 <= ext < _safe_float(p["ext_pct"], 10.0)
        volume_ok = vol >= (vs * _safe_float(p["vol_mult"], 1.0))
        rsi_ok = _safe_float(p["rsi_buy_min"], 0.0) <= r <= _safe_float(p["rsi_buy_max"], 100.0)
        momentum_broken = close < em and hist < 0 and r < _safe_float(p["rsi_exit_thresh"], 38.0)
        trend_lost = close < et and close < es and r < 45

        rsi_was_hot = prev_rsi > _safe_float(p["rsi_trim_thresh"], 82.0)
        rsi_cooling = r < prev_rsi
        macd_fading = hist < prev_hist
        trim_condition = price_above and rsi_was_hot and (rsi_cooling or macd_fading)

        rsi_mid = (_safe_float(p["rsi_buy_min"], 45.0) + _safe_float(p["rsi_buy_max"], 75.0)) / 2.0
        rsi_range = (_safe_float(p["rsi_buy_max"], 75.0) - _safe_float(p["rsi_buy_min"], 45.0)) / 2.0
        rsi_sub = max(0.0, 1.0 - abs(r - rsi_mid) / rsi_range) if rsi_range > 0 else 0.5

        hist_values = [abs(_safe_float(candle.get("macd_hist"), 0.0)) for candle in candles[-50:] if candle.get("macd_hist") is not None]
        hist_max = max(hist_values) if hist_values else max(abs(hist), 0.001)
        macd_sub = max(0.0, min(1.0, hist / hist_max)) if hist_max > 0 else 0.5

        ema_stack = (
            (0.2 if ef > em else 0.0)
            + (0.2 if em > es else 0.0)
            + (0.2 if close > ef else 0.0)
            + (0.2 if macd_rising else 0.0)
            + (0.2 if above_trend else 0.0)
        )
        ext_pct = _safe_float(p["ext_pct"], 10.0)
        ext_sub = max(0.0, min(1.0, 1.0 - (ext / ext_pct))) if ext_pct > 0 else 0.5
        vol_ratio = vol / vs if vs > 0 else 1.0
        vol_sub = max(0.0, min(1.0, vol_ratio / 2.0))
        conviction = 0.5 + (rsi_sub * 0.25 + macd_sub * 0.20 + ema_stack * 0.25 + ext_sub * 0.15 + vol_sub * 0.15)

        signal = None
        state["bars_since_exit"] = min(999, int(state.get("bars_since_exit", 999)) + 1)
        state["bars_since_trim"] = min(999, int(state.get("bars_since_trim", 999)) + 1)
        current_state = int(state.get("state", 0))

        if current_state in (1, 2) and (momentum_broken or trend_lost):
            signal = "exit"
            state["state"] = 0
            state["bars_since_exit"] = 0
        elif current_state == 1 and int(state.get("bars_since_trim", 999)) >= int(p["trim_cooldown"]) and trim_condition:
            signal = "trim"
            state["state"] = 2
            state["bars_since_trim"] = 0
        elif (
            current_state == 0
            and int(state.get("bars_since_exit", 999)) >= int(p["cooldown_bars"])
            and ema_aligned
            and price_above
            and macd_positive
            and macd_rising
            and rsi_ok
            and not_overextended
            and above_trend
            and volume_ok
        ):
            signal = "buy"
            state["state"] = 1

        result = {
            "signal": signal,
            "product_id": product_id,
            "price": close,
            "conviction": round(conviction, 2),
            "indicators": {
                "rsi": round(r, 2),
                "macd_hist": round(hist, 6),
                "ema_fast": round(ef, 4),
                "ema_mid": round(em, 4),
                "ema_slow": round(es, 4),
                "ema_trend": round(et, 4),
                "extension_pct": round(ext, 2),
                "volume_ratio": round(vol_ratio, 2),
            },
            "state": state["state"],
        }

        if signal:
            _log(f"{signal.upper()} {product_id} price={close:.4f} conviction={conviction:.2f} rsi={r:.1f}")
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
        core_assets = set((config.get("core_assets") or {}).keys())
        blocked = set(config.get("satellite_blocked") or [])
        universe = get_all_usd_products()
        targets = sorted(universe - core_assets - blocked)

        dispatched = []
        for product_id in targets:
            try:
                candles = self.fetch_candles(product_id)
                if len(candles) < 50:
                    continue
                candles = self.compute_indicators(candles)
                result = self.evaluate_product(product_id, candles)

                if result["signal"]:
                    action_map = {"buy": "BUY", "trim": "TRIM", "exit": "EXIT"}
                    signal_map = {"buy": "SATELLITE_BUY", "trim": "SATELLITE_TRIM", "exit": "SATELLITE_EXIT"}
                    dispatch_result = dispatch_signal_action(
                        product_id=product_id,
                        action=action_map[result["signal"]],
                        signal_type=signal_map[result["signal"]],
                        timeframe=self.timeframe,
                        strategy="server_scanner_v6",
                        conviction_score=result["conviction"],
                    )
                    dispatched.append({**result, "dispatch": dispatch_result})
            except Exception as exc:
                _log(f"scan failed for {product_id}: {exc}")
            time.sleep(0.15)

        _log(f"satellite sweep done scanned={len(targets)} signals={len(dispatched)}")
        return dispatched

    def scan_core(self):
        snapshot = get_portfolio_snapshot()
        config = snapshot.get("config", {})
        core_assets = list((config.get("core_assets") or {}).keys())

        core_scanner = SignalScanner(timeframe="4h")
        dispatched = []
        for product_id in core_assets:
            try:
                candles = core_scanner.fetch_candles(product_id)
                if len(candles) < 50:
                    continue
                candles = core_scanner.compute_indicators(candles)
                result = core_scanner.evaluate_product(product_id, candles)

                if result["signal"]:
                    action_map = {"buy": "BUY", "trim": "TRIM", "exit": "EXIT"}
                    signal_map = {"buy": "CORE_BUY_WINDOW", "trim": "CORE_TRIM", "exit": "CORE_EXIT"}
                    dispatch_result = dispatch_signal_action(
                        product_id=product_id,
                        action=action_map[result["signal"]],
                        signal_type=signal_map[result["signal"]],
                        timeframe="4h",
                        strategy="server_scanner_v6_core",
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
        scanner = SignalScanner(timeframe="1h")
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


def get_signal_log(limit=100):
    return list(reversed(_SIGNAL_LOG[-int(limit):]))


def get_scanner_state():
    return dict(_SCANNER_STATE)


def get_scanner_params():
    return {"satellite_1h": dict(PARAMS_1H_SATELLITE), "core_4h": dict(PARAMS_4H_CORE)}


def update_scanner_params(preset, params):
    global PARAMS_1H_SATELLITE, PARAMS_4H_CORE
    params = params if isinstance(params, dict) else {}
    if preset == "satellite_1h":
        PARAMS_1H_SATELLITE.update(params)
        return dict(PARAMS_1H_SATELLITE)
    if preset == "core_4h":
        PARAMS_4H_CORE.update(params)
        return dict(PARAMS_4H_CORE)
    return {}
