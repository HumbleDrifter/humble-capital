import math
import time
from datetime import datetime, timezone

from execution import get_client
from backtester import (
    ema,
    rsi,
    macd,
    bollinger_bands,
    bb_width,
    bb_percent_b,
    adx,
    atr,
    sma,
)


_CANDLE_CACHE = {}

DEFAULT_CORE_ASSETS = {
    "BTC-USD": {"target_weight": 0.30},
    "ETH-USD": {"target_weight": 0.15},
    "SOL-USD": {"target_weight": 0.13},
    "XRP-USD": {"target_weight": 0.05},
}

DEFAULT_SATELLITES = [
    "DOGE-USD",
    "SHIB-USD",
    "PEPE-USD",
    "BONK-USD",
    "SUI-USD",
    "RENDER-USD",
    "HYPE-USD",
    "TAO-USD",
    "WIF-USD",
    "PENGU-USD",
    "JASMY-USD",
    "ICP-USD",
    "LINK-USD",
    "UNI-USD",
    "AAVE-USD",
    "AVAX-USD",
    "DOT-USD",
    "ADA-USD",
    "ATOM-USD",
]

SATELLITE_PARAMS = {
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
    "adx_min": 20,
}


def _log(message):
    print(f"[portfolio_backtester] {message}")


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


class PortfolioBacktester:
    def __init__(self, config=None):
        config = config if isinstance(config, dict) else {}

        end_ts = _utc_ts_from_date_string(config.get("end_date"), end_of_day=True)
        if end_ts is None:
            end_ts = int(time.time())
        start_ts = _utc_ts_from_date_string(config.get("start_date"), end_of_day=False)
        if start_ts is None:
            start_ts = end_ts - (180 * 24 * 60 * 60)

        self.config = {
            "starting_capital": _safe_float(config.get("starting_capital", 1000.0), 1000.0),
            "core_assets": config.get("core_assets") or dict(DEFAULT_CORE_ASSETS),
            "satellite_max_weight": _safe_float(config.get("satellite_max_weight", 0.20), 0.20),
            "satellite_per_asset_max": _safe_float(config.get("satellite_per_asset_max", 0.05), 0.05),
            "max_active_satellites": _safe_int(config.get("max_active_satellites", 4), 4),
            "cash_reserve": _safe_float(config.get("cash_reserve", 0.08), 0.08),
            "rebalance_band": _safe_float(config.get("rebalance_band", 0.08), 0.08),
            "rebalance_frequency_bars": _safe_int(config.get("rebalance_frequency_bars", 42), 42),
            "dca_frequency_bars": _safe_int(config.get("dca_frequency_bars", 42), 42),
            "dca_amount_pct": _safe_float(config.get("dca_amount_pct", 0.02), 0.02),
            "fee_pct": _safe_float(config.get("fee_pct", 0.006), 0.006),
            "satellite_strategy": str(config.get("satellite_strategy", "bb_breakout") or "bb_breakout"),
            "satellite_regime_filter": list(config.get("satellite_regime_filter") or ["bull"]),
            "satellite_params": {**SATELLITE_PARAMS, **(config.get("satellite_params") or {})},
            "start_date": config.get("start_date"),
            "end_date": config.get("end_date"),
            "timeframe": str(config.get("timeframe", "4h") or "4h").lower().strip(),
        }
        self.start_ts = int(start_ts)
        self.end_ts = int(end_ts)
        self.bar_seconds = 4 * 60 * 60 if self.config["timeframe"] == "4h" else 60 * 60
        self.cash = self.config["starting_capital"]
        self.positions = {}
        self.equity_curve = []
        self.trade_log = []
        self.rebalance_log = []
        self.total_fees = 0.0
        self.dca_total_invested = 0.0
        self._all_candles = {}
        self._current_bar = 0
        self._current_ts = 0
        self._candle_by_ts = {}
        self._index_by_ts = {}
        self._timeline = []

    def _to_dict(self, x):
        return x.to_dict() if hasattr(x, "to_dict") else x

    def fetch_candles(self, product_id, start_ts, end_ts):
        """Fetch ALL candles between start_ts and end_ts by paginating in chunks."""
        client = get_client()
        granularity = self.config.get("timeframe_granularity", "FOUR_HOUR" if self.config["timeframe"] == "4h" else "ONE_HOUR")
        seconds_per_bar = 14400 if "FOUR" in str(granularity).upper() else 3600
        max_candles_per_request = 300
        chunk_seconds = max_candles_per_request * seconds_per_bar

        all_candles = []
        current_start = int(start_ts)
        end_ts = int(end_ts)
        product_id = _normalize_product_id(product_id)

        while current_start < end_ts:
            current_end = min(current_start + chunk_seconds, end_ts)
            try:
                response = self._to_dict(
                    client.get_candles(
                        product_id=product_id,
                        start=str(current_start),
                        end=str(current_end),
                        granularity=granularity,
                    )
                )
                candles = response.get("candles", []) if isinstance(response, dict) else []
                for candle in candles:
                    try:
                        all_candles.append(
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
            except Exception as exc:
                _log(f"fetch chunk failed product_id={product_id} start={current_start} error={exc}")

            current_start = current_end
            time.sleep(0.1)

        seen = set()
        unique = []
        for candle in all_candles:
            ts = _safe_int(candle.get("ts"), 0)
            if ts > 0 and ts not in seen:
                seen.add(ts)
                unique.append(candle)
        unique.sort(key=lambda candle: candle["ts"])

        _log(f"fetched {len(unique)} candles for {product_id} from {start_ts} to {end_ts}")
        return unique

    def fetch_all_candles(self) -> dict:
        all_assets = set(_normalize_product_id(pid) for pid in (self.config["core_assets"] or {}).keys())
        all_assets.update(DEFAULT_SATELLITES)
        out = {}

        for product_id in sorted(all_assets):
            cache_key = (product_id, self.config["timeframe"], self.start_ts, self.end_ts)
            if cache_key in _CANDLE_CACHE:
                out[product_id] = [dict(row) for row in _CANDLE_CACHE[cache_key]]
                continue

            try:
                candles = self.fetch_candles(product_id, self.start_ts, self.end_ts)
                _CANDLE_CACHE[cache_key] = [dict(row) for row in candles]
                if candles:
                    out[product_id] = candles
            except Exception as exc:
                _log(f"fetch failed product_id={product_id} error={exc}")
                continue

        return out

    def compute_all_indicators(self, all_candles: dict) -> dict:
        enriched = {}
        sat_params = self.config["satellite_params"]

        for product_id, candles in (all_candles or {}).items():
            try:
                closes = [_safe_float(candle.get("close"), 0.0) for candle in candles]
                highs = [_safe_float(candle.get("high"), 0.0) for candle in candles]
                lows = [_safe_float(candle.get("low"), 0.0) for candle in candles]
                volumes = [_safe_float(candle.get("volume"), 0.0) for candle in candles]

                upper, middle, lower = bollinger_bands(closes, sat_params["bb_period"], sat_params["bb_std"])
                widths = bb_width(upper, lower, middle)
                widths_avg50 = sma(widths, 50)
                pctb = bb_percent_b(closes, upper, lower)
                rsi_vals = rsi(closes, sat_params["rsi_len"])
                atr_vals = atr(highs, lows, closes, sat_params["atr_period"])
                adx_vals = adx(highs, lows, closes, 14)
                vol_sma_vals = sma(volumes, 20)
                ema50 = ema(closes, 50)
                ema200 = ema(closes, 200)
                _, _, macd_hist = macd(closes, 12, 26, 9)

                rows = []
                for idx, candle in enumerate(candles):
                    row = dict(candle)
                    row["bb_upper"] = upper[idx]
                    row["bb_middle"] = middle[idx]
                    row["bb_lower"] = lower[idx]
                    row["bb_width"] = widths[idx]
                    row["bb_width_avg50"] = widths_avg50[idx]
                    row["bb_pctb"] = pctb[idx]
                    row["rsi"] = rsi_vals[idx]
                    row["atr"] = atr_vals[idx]
                    row["adx"] = adx_vals[idx]
                    row["vol_sma"] = vol_sma_vals[idx]
                    row["ema50"] = ema50[idx]
                    row["ema200"] = ema200[idx]
                    row["macd_hist"] = macd_hist[idx]
                    rows.append(row)
                enriched[product_id] = rows
            except Exception as exc:
                _log(f"indicator build failed product_id={product_id} error={exc}")
        return enriched

    def estimate_regime(self, btc_candles, bar_ref) -> str:
        if not btc_candles:
            return "neutral"
        if isinstance(bar_ref, (int, float)):
            ts_lookup = _safe_int(bar_ref, 0)
            btc_index = (self._index_by_ts.get("BTC-USD") or {}).get(ts_lookup)
            if btc_index is None:
                btc_index = _safe_int(bar_ref, 0)
        else:
            btc_index = 0
        if btc_index < 0 or btc_index >= len(btc_candles):
            return "neutral"
        bar = btc_candles[btc_index]
        close = _safe_float(bar.get("close"), 0.0)
        ema50 = bar.get("ema50")
        ema200 = bar.get("ema200")
        rsi_val = _safe_float(bar.get("rsi"), 50.0)
        if ema50 is None or ema200 is None:
            return "neutral"
        if close < ema200 and rsi_val < 35:
            return "risk_off"
        if close < ema200 and rsi_val < 45:
            return "caution"
        if close > ema50 and ema50 > ema200 and rsi_val > 50:
            return "bull"
        if close > ema200 and rsi_val > 40:
            return "neutral"
        return "caution"

    def get_portfolio_value(self, all_candles, bar_ref) -> dict:
        core_assets = set(_normalize_product_id(pid) for pid in (self.config["core_assets"] or {}).keys())
        total_value = self.cash
        core_value = 0.0
        satellite_value = 0.0
        weights = {}
        ts_lookup = _safe_int(bar_ref, self._current_ts)

        for product_id, position in list(self.positions.items()):
            candle = (self._candle_by_ts.get(product_id) or {}).get(ts_lookup)
            if not isinstance(candle, dict):
                continue
            price = _safe_float(candle.get("close"), 0.0)
            value = _safe_float(position.get("qty"), 0.0) * price
            position["value"] = value
            total_value += value
            if product_id in core_assets:
                core_value += value
            else:
                satellite_value += value

        if total_value > 0:
            for product_id, position in self.positions.items():
                weights[product_id] = _safe_float(position.get("value"), 0.0) / total_value

        return {
            "total_value": total_value,
            "cash": self.cash,
            "core_value": core_value,
            "satellite_value": satellite_value,
            "weights": weights,
        }

    def _get_core_return(self, product_id, ts, lookback_bars=180):
        product_id = _normalize_product_id(product_id)
        current_ts = _safe_int(ts, self._current_ts)
        current_bar = (self._candle_by_ts.get(product_id) or {}).get(current_ts)
        if not isinstance(current_bar, dict):
            return 0.0

        target_ts = current_ts - (max(1, _safe_int(lookback_bars, 180)) * self.bar_seconds)
        candle_map = self._candle_by_ts.get(product_id) or {}
        past_bar = None
        for check_ts in range(target_ts - self.bar_seconds, target_ts + self.bar_seconds + 1, self.bar_seconds):
            past_bar = candle_map.get(check_ts)
            if isinstance(past_bar, dict):
                break

        if not isinstance(past_bar, dict):
            return 0.0

        current_close = _safe_float(current_bar.get("close"), 0.0)
        past_close = _safe_float(past_bar.get("close"), 0.0)
        if current_close <= 0 or past_close <= 0:
            return 0.0

        return (current_close - past_close) / past_close

    def check_rebalance(self, weights, bar_ref) -> list:
        actions = []
        snapshot = self.get_portfolio_value(self._all_candles, bar_ref)
        total_value = _safe_float(snapshot.get("total_value"), 0.0)
        if total_value <= 0:
            return actions

        available_cash = max(0.0, self.cash - (total_value * _safe_float(self.config.get("cash_reserve", 0.08), 0.08)))
        if available_cash <= 5.0:
            return actions

        rebalance_band = _safe_float(self.config.get("rebalance_band", 0.08), 0.08)
        for product_id, cfg in (self.config["core_assets"] or {}).items():
            product_id = _normalize_product_id(product_id)
            target_weight = _safe_float((cfg or {}).get("target_weight"), 0.0)
            current_weight = _safe_float((weights or {}).get(product_id), 0.0)
            if abs(current_weight - target_weight) < rebalance_band:
                continue

            if current_weight >= target_weight - rebalance_band:
                continue

            target_value = total_value * target_weight
            current_value = _safe_float((self.positions.get(product_id) or {}).get("value"), 0.0)
            shortfall_value = max(0.0, target_value - current_value)
            buy_amount = min(shortfall_value * 0.5, available_cash)
            if buy_amount <= 5.0:
                continue

            actions.append(
                {
                    "product_id": product_id,
                    "action": "buy",
                    "target_value": target_value,
                    "current_value": current_value,
                    "adjustment": buy_amount,
                }
            )
            available_cash = max(0.0, available_cash - buy_amount)
            if available_cash <= 5.0:
                break

        for product_id, cfg in (self.config["core_assets"] or {}).items():
            product_id = _normalize_product_id(product_id)
            target_weight = _safe_float((cfg or {}).get("target_weight"), 0.0)
            current_weight = _safe_float((weights or {}).get(product_id), 0.0)

            if current_weight > target_weight + rebalance_band:
                current_value = _safe_float((self.positions.get(product_id) or {}).get("value"), 0.0)
                target_value = total_value * target_weight
                excess = current_value - target_value
                sell_amount = excess * 0.5
                if sell_amount > 5.0:
                    actions.append(
                        {
                            "product_id": product_id,
                            "action": "sell",
                            "target_value": target_value,
                            "current_value": current_value,
                            "adjustment": sell_amount,
                        }
                    )
        return actions

    def execute_dca(self, all_candles, bar_ref, regime) -> list:
        allowed_dca_regimes = ["bull", "neutral"]
        if regime not in allowed_dca_regimes:
            return []

        snapshot = self.get_portfolio_value(all_candles, bar_ref)
        total_value = _safe_float(snapshot.get("total_value"), 0.0)
        dca_budget = total_value * _safe_float(self.config["dca_amount_pct"], 0.02)
        reserve_floor = total_value * _safe_float(self.config["cash_reserve"], 0.08)
        available_cash = max(0.0, self.cash - reserve_floor)
        dca_budget = min(dca_budget, available_cash)
        if dca_budget <= 1.0:
            return []

        underweights = []
        for product_id, cfg in (self.config["core_assets"] or {}).items():
            product_id = _normalize_product_id(product_id)
            target_weight = _safe_float((cfg or {}).get("target_weight"), 0.0)
            current_weight = _safe_float((snapshot.get("weights") or {}).get(product_id), 0.0)
            if current_weight < target_weight:
                underweights.append(product_id)

        actions = []
        if not underweights:
            return actions

        per_asset = dca_budget / len(underweights)
        for product_id in underweights:
            candle = (self._candle_by_ts.get(product_id) or {}).get(_safe_int(bar_ref, self._current_ts))
            if not isinstance(candle, dict):
                continue
            price = _safe_float(candle.get("close"), 0.0)
            if price <= 0 or per_asset <= 1.0:
                continue
            res = self.execute_trade(product_id, "buy", per_asset, price, "dca", _safe_int(candle.get("ts"), 0))
            if res:
                self.dca_total_invested += per_asset
                actions.append(res)
        return actions

    def evaluate_satellite_entry(self, product_id, candles, bar_ref, regime) -> dict:
        if regime not in set(self.config["satellite_regime_filter"] or ["bull"]):
            return {"signal": None}
        ts_lookup = _safe_int(bar_ref, self._current_ts)
        bar_index = (self._index_by_ts.get(product_id) or {}).get(ts_lookup)
        if bar_index is None or bar_index < 50 or bar_index >= len(candles):
            return {"signal": None}

        params = self.config["satellite_params"]
        bar = candles[bar_index]
        required = ["bb_upper", "bb_width", "bb_width_avg50", "bb_pctb", "rsi", "atr", "adx", "vol_sma"]
        if any(bar.get(key) is None for key in required):
            return {"signal": None}

        min_squeeze_bars = max(1, _safe_int(params.get("min_squeeze_bars", 8), 8))
        squeeze = True
        for offset in range(1, min_squeeze_bars + 1):
            idx = bar_index - offset
            if idx < 0:
                squeeze = False
                break
            row = candles[idx]
            width = _safe_float(row.get("bb_width"), 0.0)
            width_avg = _safe_float(row.get("bb_width_avg50"), 0.0)
            if width <= 0 or width_avg <= 0 or width >= width_avg:
                squeeze = False
                break

        close = _safe_float(bar.get("close"), 0.0)
        upper = _safe_float(bar.get("bb_upper"), 0.0)
        pctb = _safe_float(bar.get("bb_pctb"), 0.0)
        rsi_val = _safe_float(bar.get("rsi"), 50.0)
        adx_val = _safe_float(bar.get("adx"), 0.0)
        volume = _safe_float(bar.get("volume"), 0.0)
        vol_sma_val = _safe_float(bar.get("vol_sma"), 0.0)
        breakout = close > upper and pctb > 1.0
        volume_ok = volume > (_safe_float(params.get("volume_breakout_mult", 1.5), 1.5) * vol_sma_val if vol_sma_val > 0 else 0.0)
        rsi_ok = 40.0 <= rsi_val <= _safe_float(params.get("rsi_overbought", 75), 75.0)
        adx_ok = adx_val >= _safe_float(params.get("adx_min", 20), 20.0)

        if not (squeeze and breakout and volume_ok and rsi_ok and adx_ok):
            return {"signal": None}

        width = _safe_float(bar.get("bb_width"), 0.0)
        width_avg = _safe_float(bar.get("bb_width_avg50"), width or 1.0)
        squeeze_sub = max(0.0, min(1.0, (width_avg - width) / width_avg)) if width_avg > 0 else 0.0
        volume_ratio = (volume / vol_sma_val) if vol_sma_val > 0 else 1.0
        volume_sub = max(0.0, min(1.0, (volume_ratio - 1.0) / max(1.0, _safe_float(params.get("volume_breakout_mult", 1.5), 1.5))))
        adx_sub = max(0.0, min(1.0, (adx_val - _safe_float(params.get("adx_min", 20), 20.0)) / max(1.0, 40.0 - _safe_float(params.get("adx_min", 20), 20.0))))
        rsi_mid = (40.0 + _safe_float(params.get("rsi_overbought", 75), 75.0)) / 2.0
        rsi_range = max(1.0, (_safe_float(params.get("rsi_overbought", 75), 75.0) - 40.0) / 2.0)
        rsi_sub = max(0.0, min(1.0, 1.0 - abs(rsi_val - rsi_mid) / rsi_range))
        conviction = 0.5 + (squeeze_sub * 0.25 + volume_sub * 0.25 + adx_sub * 0.25 + rsi_sub * 0.25)

        return {"signal": "buy", "conviction": round(max(0.5, min(1.5, conviction)), 4), "price": close}

    def evaluate_satellite_exit(self, product_id, candles, bar_ref) -> dict:
        ts_lookup = _safe_int(bar_ref, self._current_ts)
        bar_index = (self._index_by_ts.get(product_id) or {}).get(ts_lookup)
        if product_id not in self.positions or bar_index is None or bar_index >= len(candles):
            return {"signal": None}

        position = self.positions.get(product_id) or {}
        bar = candles[bar_index]
        close = _safe_float(bar.get("close"), 0.0)
        upper = _safe_float(bar.get("bb_upper"), 0.0)
        atr_val = _safe_float(bar.get("atr"), 0.0)
        rsi_val = _safe_float(bar.get("rsi"), 50.0)
        entry_price = _safe_float(position.get("avg_entry"), 0.0)
        max_price = max(_safe_float(position.get("max_price_since_entry"), close), close)
        position["max_price_since_entry"] = max_price
        atr_at_entry = _safe_float(position.get("atr_at_entry"), atr_val)
        atr_for_exit = atr_val if atr_val > 0 else atr_at_entry

        stop_price = entry_price - (_safe_float(self.config["satellite_params"].get("atr_stop_mult", 2.0), 2.0) * atr_for_exit)
        target_price = entry_price + (_safe_float(self.config["satellite_params"].get("atr_target_mult", 3.0), 3.0) * atr_for_exit)
        trailing_stop = max_price - (_safe_float(self.config["satellite_params"].get("atr_target_mult", 3.0), 3.0) * atr_for_exit)

        if atr_for_exit > 0 and close < stop_price:
            return {"signal": "exit", "reason": "stop_loss", "price": close}
        if atr_for_exit > 0 and close > target_price:
            return {"signal": "exit", "reason": "target_hit", "price": close}
        if atr_for_exit > 0 and max_price > entry_price and close < trailing_stop:
            return {"signal": "exit", "reason": "trailing_stop", "price": close}
        if rsi_val > _safe_float(self.config["satellite_params"].get("rsi_overbought", 75), 75.0) and close < upper:
            return {"signal": "exit", "reason": "rsi_exhaustion", "price": close}
        return {"signal": None}

    def execute_trade(self, product_id, action, value_usd, price, reason, ts):
        product_id = _normalize_product_id(product_id)
        value_usd = _safe_float(value_usd, 0.0)
        price = _safe_float(price, 0.0)
        fee_pct = _safe_float(self.config["fee_pct"], 0.006)
        if not product_id or value_usd <= 0 or price <= 0:
            return None

        fee = value_usd * fee_pct * 0.5
        if action == "buy":
            total_cash_needed = value_usd
            if self.cash < total_cash_needed:
                return None
            qty = max(0.0, (value_usd - fee) / price)
            if qty <= 0:
                return None
            self.cash -= total_cash_needed
            self.total_fees += fee
            pos = self.positions.setdefault(product_id, {"qty": 0.0, "avg_entry": 0.0, "value": 0.0})
            current_qty = _safe_float(pos.get("qty"), 0.0)
            new_qty = current_qty + qty
            pos["avg_entry"] = price if current_qty <= 0 else (((current_qty * _safe_float(pos.get("avg_entry"), 0.0)) + (qty * price)) / new_qty)
            pos["qty"] = new_qty
            pos["value"] = new_qty * price
            if product_id not in self.config["core_assets"]:
                entry_candle = (self._candle_by_ts.get(product_id) or {}).get(self._current_ts, {})
                pos["atr_at_entry"] = _safe_float((entry_candle or {}).get("atr"), 0.0)
                pos["max_price_since_entry"] = price
            record = {"ts": ts, "product_id": product_id, "action": "BUY", "qty": qty, "price": price, "value": value_usd, "fee": fee, "reason": reason}
        else:
            pos = self.positions.get(product_id) or {}
            qty = min(_safe_float(pos.get("qty"), 0.0), value_usd / price if price > 0 else 0.0)
            if qty <= 0:
                return None
            gross_value = qty * price
            fee = gross_value * fee_pct * 0.5
            self.cash += (gross_value - fee)
            self.total_fees += fee
            remaining_qty = max(0.0, _safe_float(pos.get("qty"), 0.0) - qty)
            if remaining_qty <= 1e-12:
                self.positions.pop(product_id, None)
            else:
                pos["qty"] = remaining_qty
                pos["value"] = remaining_qty * price
            record = {"ts": ts, "product_id": product_id, "action": action.upper(), "qty": qty, "price": price, "value": gross_value, "fee": fee, "reason": reason}

        self.trade_log.append(record)
        return record

    def run(self) -> dict:
        all_candles = self.fetch_all_candles()
        self._all_candles = self.compute_all_indicators(all_candles)
        self._candle_by_ts = {}
        self._index_by_ts = {}
        for product_id, candles in self._all_candles.items():
            self._candle_by_ts[product_id] = {row["ts"]: row for row in candles if _safe_int(row.get("ts"), 0) > 0}
            self._index_by_ts[product_id] = {
                _safe_int(row.get("ts"), 0): idx
                for idx, row in enumerate(candles)
                if _safe_int(row.get("ts"), 0) > 0
            }

        btc_candles = self._all_candles.get("BTC-USD") or []
        if not btc_candles:
            return {
                "equity_curve": [],
                "trade_log": [],
                "rebalance_log": [],
                "summary": self.get_summary({}),
                "final_positions": {},
            }

        self._timeline = [_safe_int(row.get("ts"), 0) for row in btc_candles if _safe_int(row.get("ts"), 0) > 0]
        regime_counts = {"bull": 0, "neutral": 0, "caution": 0, "risk_off": 0}

        first_ts = self._timeline[0] if self._timeline else 0
        for product_id, cfg in self.config["core_assets"].items():
            product_id_norm = _normalize_product_id(product_id)
            target_weight = _safe_float(cfg.get("target_weight"), 0.0)
            buy_value = self.cash * target_weight / max(0.01, 1.0 - _safe_float(self.config.get("cash_reserve"), 0.08))
            buy_value = min(
                buy_value,
                self.cash - (
                    _safe_float(self.config.get("starting_capital", 1000), 1000)
                    * _safe_float(self.config.get("cash_reserve"), 0.08)
                ),
            )
            if buy_value > 1.0:
                candle = (self._candle_by_ts.get(product_id_norm) or {}).get(first_ts)
                if isinstance(candle, dict):
                    price = _safe_float(candle.get("close"), 0.0)
                    if price > 0:
                        self.execute_trade(product_id_norm, "buy", buy_value, price, "initial_allocation", first_ts)

        for bar_index, ts in enumerate(self._timeline):
            self._current_bar = bar_index
            self._current_ts = ts
            regime = self.estimate_regime(btc_candles, ts)
            regime_counts[regime] = regime_counts.get(regime, 0) + 1

            snapshot = self.get_portfolio_value(self._all_candles, ts)
            self.equity_curve.append(
                {
                    "ts": ts,
                    "total_value": round(_safe_float(snapshot.get("total_value"), 0.0), 2),
                    "cash": round(self.cash, 2),
                    "core_value": round(_safe_float(snapshot.get("core_value"), 0.0), 2),
                    "satellite_value": round(_safe_float(snapshot.get("satellite_value"), 0.0), 2),
                    "positions": {pid: round(_safe_float(pos.get("value"), 0.0), 2) for pid, pos in self.positions.items()},
                    "regime": regime,
                }
            )

            if bar_index > 0 and bar_index % max(1, _safe_int(self.config["rebalance_frequency_bars"], 42)) == 0:
                rebalance_actions = self.check_rebalance(snapshot.get("weights") or {}, ts)
                executed = []
                for action in rebalance_actions:
                    product_id = action["product_id"]
                    candle = (self._candle_by_ts.get(product_id) or {}).get(ts)
                    if not isinstance(candle, dict):
                        continue
                    price = _safe_float(candle.get("close"), 0.0)
                    if price <= 0:
                        continue
                    amount = abs(_safe_float(action.get("adjustment"), 0.0))
                    if action["action"] == "buy":
                        reserve_floor = _safe_float(snapshot.get("total_value"), 0.0) * _safe_float(self.config["cash_reserve"], 0.08)
                        amount = min(amount, max(0.0, self.cash - reserve_floor))
                        res = self.execute_trade(product_id, "buy", amount, price, "rebalance_buy", ts) if amount > 1.0 else None
                    else:
                        current_value = _safe_float((self.positions.get(product_id) or {}).get("value"), 0.0)
                        amount = min(amount, current_value)
                        res = self.execute_trade(product_id, "sell", amount, price, "rebalance_sell", ts) if amount > 1.0 else None
                    if res:
                        executed.append({"product_id": product_id, "action": action["action"], "amount": amount})
                if executed:
                    self.rebalance_log.append({"ts": ts, "actions": executed})

            if bar_index > 0 and bar_index % max(1, _safe_int(self.config["dca_frequency_bars"], 42)) == 0:
                self.execute_dca(self._all_candles, ts, regime)

            held_satellites = [pid for pid in self.positions if pid not in self.config["core_assets"]]
            for product_id in list(held_satellites):
                candles = self._all_candles.get(product_id) or []
                exit_sig = self.evaluate_satellite_exit(product_id, candles, ts)
                if exit_sig.get("signal") == "exit":
                    current_value = _safe_float((self.positions.get(product_id) or {}).get("value"), 0.0)
                    if current_value > 1.0:
                        self.execute_trade(product_id, "sell", current_value, _safe_float(exit_sig.get("price"), 0.0), exit_sig.get("reason"), ts)

            if len([pid for pid in self.positions if pid not in self.config["core_assets"]]) < _safe_int(self.config["max_active_satellites"], 4):
                candidates = []
                for product_id in DEFAULT_SATELLITES:
                    if product_id in self.positions:
                        continue
                    candles = self._all_candles.get(product_id) or []
                    entry_sig = self.evaluate_satellite_entry(product_id, candles, ts, regime)
                    if entry_sig.get("signal") == "buy":
                        candidates.append((product_id, entry_sig))

                candidates.sort(key=lambda item: _safe_float(item[1].get("conviction"), 0.0), reverse=True)
                for product_id, entry_sig in candidates:
                    if len([pid for pid in self.positions if pid not in self.config["core_assets"]]) >= _safe_int(self.config["max_active_satellites"], 4):
                        break
                    snapshot = self.get_portfolio_value(self._all_candles, ts)
                    total_value = _safe_float(snapshot.get("total_value"), 0.0)
                    reserve_floor = total_value * _safe_float(self.config["cash_reserve"], 0.08)
                    available_cash = max(0.0, self.cash - reserve_floor)
                    if available_cash <= 1.0:
                        break
                    alloc = min(total_value * _safe_float(self.config["satellite_per_asset_max"], 0.05), available_cash)
                    sat_cap = total_value * _safe_float(self.config["satellite_max_weight"], 0.20)
                    current_sat_value = _safe_float(snapshot.get("satellite_value"), 0.0)
                    alloc = min(alloc, max(0.0, sat_cap - current_sat_value))
                    if alloc <= 1.0:
                        continue
                    self.execute_trade(product_id, "buy", alloc, _safe_float(entry_sig.get("price"), 0.0), "satellite_entry", ts)

        final_snapshot = self.get_portfolio_value(self._all_candles, self._timeline[-1] if self._timeline else 0)
        summary = self.get_summary(
            {
                "equity_curve": self.equity_curve,
                "trade_log": self.trade_log,
                "rebalance_log": self.rebalance_log,
                "final_snapshot": final_snapshot,
                "regime_counts": regime_counts,
            }
        )

        return {
            "equity_curve": self.equity_curve,
            "trade_log": self.trade_log,
            "rebalance_log": self.rebalance_log,
            "summary": summary,
            "final_positions": {pid: dict(pos) for pid, pos in self.positions.items()},
        }

    def get_summary(self, result) -> dict:
        result = result if isinstance(result, dict) else {}
        equity_curve = list(result.get("equity_curve") or [])
        trade_log = list(result.get("trade_log") or [])
        rebalance_log = list(result.get("rebalance_log") or [])
        final_snapshot = result.get("final_snapshot") or {}
        regime_counts = result.get("regime_counts") or {}

        starting_capital = _safe_float(self.config["starting_capital"], 1000.0)
        ending_capital = _safe_float(final_snapshot.get("total_value"), starting_capital)
        total_return_pct = ((ending_capital - starting_capital) / starting_capital) * 100.0 if starting_capital > 0 else 0.0

        duration_days = max(1.0, (self.end_ts - self.start_ts) / 86400.0)
        annual_factor = 365.0 / duration_days
        annualized = ((ending_capital / starting_capital) ** annual_factor - 1.0) * 100.0 if starting_capital > 0 and ending_capital > 0 else 0.0

        peak = 0.0
        max_drawdown_pct = 0.0
        returns = []
        prev_value = None
        for point in equity_curve:
            value = _safe_float(point.get("total_value"), 0.0)
            if value > peak:
                peak = value
            if peak > 0:
                max_drawdown_pct = max(max_drawdown_pct, ((peak - value) / peak) * 100.0)
            if prev_value and prev_value > 0:
                returns.append((value - prev_value) / prev_value)
            prev_value = value

        avg_return = sum(returns) / len(returns) if returns else 0.0
        variance = sum((value - avg_return) ** 2 for value in returns) / len(returns) if returns else 0.0
        std_dev = math.sqrt(variance) if variance > 0 else 0.0
        sharpe = (avg_return / std_dev) * math.sqrt(len(returns)) if std_dev > 0 and returns else 0.0

        satellite_exits = [row for row in trade_log if row.get("action") == "SELL" and str(row.get("reason") or "").startswith("satellite")]
        satellite_pnls = []
        by_product = {}
        for row in trade_log:
            product_id = _normalize_product_id(row.get("product_id"))
            by_product.setdefault(product_id, 0.0)
            if row.get("action") == "SELL":
                by_product[product_id] += _safe_float(row.get("value"), 0.0)
            elif row.get("action") == "BUY":
                by_product[product_id] -= _safe_float(row.get("value"), 0.0)
        for row in satellite_exits:
            product_id = _normalize_product_id(row.get("product_id"))
            satellite_pnls.append(by_product.get(product_id, 0.0))

        wins = [pnl for pnl in satellite_pnls if pnl > 0]
        losses = [pnl for pnl in satellite_pnls if pnl < 0]
        gross_wins = sum(wins)
        gross_losses = sum(losses)

        regime_total = sum(regime_counts.values()) or 1
        regime_distribution = {k: round((_safe_float(v) / regime_total), 4) for k, v in regime_counts.items()}

        final_allocation = {}
        total_value = _safe_float(final_snapshot.get("total_value"), 0.0)
        if total_value > 0:
            final_allocation["CASH"] = round(self.cash / total_value, 4)
            for product_id, pos in self.positions.items():
                final_allocation[product_id] = round(_safe_float(pos.get("value"), 0.0) / total_value, 4)

        core_assets = set(_normalize_product_id(pid) for pid in (self.config["core_assets"] or {}).keys())
        core_profit = sum(value for product_id, value in by_product.items() if product_id in core_assets)
        satellite_profit = sum(value for product_id, value in by_product.items() if product_id not in core_assets)
        total_profit = core_profit + satellite_profit

        satellite_only = {product_id: value for product_id, value in by_product.items() if product_id not in core_assets}
        best_satellite = max(satellite_only.items(), key=lambda item: item[1])[0] if satellite_only else ""
        worst_satellite = min(satellite_only.items(), key=lambda item: item[1])[0] if satellite_only else ""

        return {
            "starting_capital": round(starting_capital, 2),
            "ending_capital": round(ending_capital, 2),
            "total_return_pct": round(total_return_pct, 4),
            "total_return_pct_annualized": round(annualized, 4),
            "max_drawdown_pct": round(max_drawdown_pct, 4),
            "total_trades": len(trade_log),
            "total_rebalances": len(rebalance_log),
            "total_fees": round(self.total_fees, 2),
            "core_contribution_pct": round((core_profit / total_profit), 4) if total_profit != 0 else 0.0,
            "satellite_contribution_pct": round((satellite_profit / total_profit), 4) if total_profit != 0 else 0.0,
            "dca_total_invested": round(self.dca_total_invested, 2),
            "sharpe_estimate": round(sharpe, 4),
            "win_rate": round((len(wins) / len(satellite_pnls)) if satellite_pnls else 0.0, 4),
            "profit_factor": round(gross_wins / abs(gross_losses), 4) if gross_losses < 0 else 0.0,
            "regime_distribution": regime_distribution,
            "final_allocation": final_allocation,
            "best_satellite": best_satellite,
            "worst_satellite": worst_satellite,
        }
