from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Any

from backtester import adx, atr, bollinger_bands, ema, macd, rsi, sma
from futures.futures_client import FuturesClient
from scoring_engine import ScoringEngine


def _log(message: str) -> None:
    print(f"[futures_scanner] {message}")


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


def _normalize_product_id(product_id: Any) -> str:
    return str(product_id or "").upper().strip()


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, _safe_float(value)))


def _annualize_funding(rate: float, periods_per_day: int = 3) -> float:
    return _safe_float(rate) * periods_per_day * 365.0 * 100.0


def _days_to_expiry(expiry: Any) -> int | None:
    text = str(expiry or "").strip()
    if not text:
        return None
    now = datetime.now(timezone.utc)
    formats = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%d%b%y")
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0, (dt - now).days)
        except Exception:
            continue
    return None


class FuturesScanner:
    def __init__(self):
        self.client = FuturesClient()
        self.scoring_engine = ScoringEngine()

    def _compute_indicators(self, candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = [dict(row) for row in (candles or []) if isinstance(row, dict)]
        if not rows:
            return []

        closes = [_safe_float(row.get("close"), 0.0) for row in rows]
        highs = [_safe_float(row.get("high"), 0.0) for row in rows]
        lows = [_safe_float(row.get("low"), 0.0) for row in rows]
        volumes = [_safe_float(row.get("volume"), 0.0) for row in rows]

        ema_fast = ema(closes, 9)
        ema_mid = ema(closes, 21)
        ema_slow = ema(closes, 50)
        ema_trend = ema(closes, 200)
        rsi_vals = rsi(closes, 14)
        macd_line, macd_signal, macd_hist = macd(closes, 12, 26, 9)
        adx_vals = adx(highs, lows, closes, 14)
        atr_vals = atr(highs, lows, closes, 14)
        bb_upper, bb_middle, bb_lower = bollinger_bands(closes, 20, 2.0)
        vol_sma = sma(volumes, 20)

        for idx, row in enumerate(rows):
            row["ema_fast"] = ema_fast[idx]
            row["ema_mid"] = ema_mid[idx]
            row["ema_slow"] = ema_slow[idx]
            row["ema_trend"] = ema_trend[idx]
            row["rsi"] = rsi_vals[idx]
            row["macd"] = macd_line[idx]
            row["macd_signal"] = macd_signal[idx]
            row["macd_hist"] = macd_hist[idx]
            row["adx"] = adx_vals[idx]
            row["atr"] = atr_vals[idx]
            row["bb_upper"] = bb_upper[idx]
            row["bb_middle"] = bb_middle[idx]
            row["bb_lower"] = bb_lower[idx]
            row["vol_sma"] = vol_sma[idx]
        return rows

    def _score_short_setup(self, product_id: str, candles: list[dict[str, Any]], regime: str, funding_rate: float) -> dict[str, Any]:
        rows = self._compute_indicators(candles)
        if len(rows) < 3:
            return {
                "product_id": product_id,
                "direction": "short",
                "score": 0.0,
                "signal": "neutral",
                "conviction": 0.0,
                "reasoning": "Not enough data for short scoring.",
            }

        bar = rows[-1]
        prev = rows[-2]
        close = _safe_float(bar.get("close"), 0.0)
        ema_fast = _safe_float(bar.get("ema_fast"), 0.0)
        ema_mid = _safe_float(bar.get("ema_mid"), 0.0)
        ema_slow = _safe_float(bar.get("ema_slow"), 0.0)
        ema_trend = _safe_float(bar.get("ema_trend"), 0.0)
        rsi_val = _safe_float(bar.get("rsi"), 50.0)
        macd_hist = _safe_float(bar.get("macd_hist"), 0.0)
        prev_macd = _safe_float(prev.get("macd_hist"), 0.0)
        adx_val = _safe_float(bar.get("adx"), 0.0)
        bb_upper = _safe_float(bar.get("bb_upper"), 0.0)
        atr_val = _safe_float(bar.get("atr"), 0.0)
        volume = _safe_float(bar.get("volume"), 0.0)
        vol_sma_val = _safe_float(bar.get("vol_sma"), 0.0)

        inverse_alignment = 100.0 if (ema_fast < ema_mid < ema_slow) else 45.0 if (ema_fast < ema_mid) else 10.0
        overbought_score = _clamp((rsi_val - 55.0) / 25.0 * 100.0)
        macd_score = 100.0 if macd_hist < 0 and macd_hist <= prev_macd else 50.0 if macd_hist < 0 else 10.0
        trend_score = 100.0 if close < ema_trend and ema_trend > 0 else 20.0
        adx_score = _clamp((adx_val - 15.0) / 20.0 * 100.0)
        extension_score = 100.0 if bb_upper > 0 and close >= bb_upper * 0.985 else 40.0 if rsi_val >= 65.0 else 10.0
        funding_bonus = _clamp(funding_rate * 500000.0, 0.0, 100.0) if funding_rate > 0 else 0.0
        volume_score = _clamp((volume / vol_sma_val) * 50.0, 0.0, 100.0) if vol_sma_val > 0 else 50.0

        regime_name = str(regime or "neutral").lower().strip()
        regime_multiplier = {
            "bull": 0.60,
            "neutral": 0.90,
            "caution": 1.00,
            "risk_off": 1.10,
        }.get(regime_name, 0.85)

        raw_score = (
            inverse_alignment * 0.20
            + overbought_score * 0.18
            + macd_score * 0.16
            + trend_score * 0.14
            + adx_score * 0.12
            + extension_score * 0.10
            + volume_score * 0.05
            + funding_bonus * 0.05
        )
        score = round(_clamp(raw_score * regime_multiplier), 2)

        if score >= 80:
            signal = "strong_sell"
        elif score >= 65:
            signal = "sell"
        elif score <= 25:
            signal = "strong_buy"
        elif score <= 40:
            signal = "buy"
        else:
            signal = "neutral"

        leverage = 1
        if score >= 85:
            leverage = 5
        elif score >= 75:
            leverage = 4
        elif score >= 65:
            leverage = 3
        elif score >= 55:
            leverage = 2

        stop_loss = close + (atr_val * 2.0 if atr_val > 0 else close * 0.03)
        take_profit = close - (atr_val * 3.0 if atr_val > 0 else close * 0.05)
        risk = max(0.0000001, stop_loss - close)
        reward = max(0.0, close - take_profit)
        risk_reward = round(reward / risk, 2) if risk > 0 else 0.0

        reasoning = (
            f"{product_id} short setup scores {score:.1f}/100. "
            f"RSI is {rsi_val:.1f}, ADX is {adx_val:.1f}, MACD histogram is {macd_hist:.4f}, "
            f"and funding is {funding_rate * 100:.4f}% per period."
        )
        return {
            "product_id": product_id,
            "direction": "short",
            "score": score,
            "signal": signal,
            "conviction": round(score / 100.0, 3),
            "entry_price": close,
            "stop_loss": round(stop_loss, 6),
            "take_profit": round(take_profit, 6),
            "risk_reward": risk_reward,
            "leverage_suggested": leverage,
            "reasoning": reasoning,
        }

    def scan_perps(self, regime: str = "neutral") -> list[dict[str, Any]]:
        opportunities: list[dict[str, Any]] = []
        perps = list(self.client.get_perps() or [])

        for product in perps:
            product_id = _normalize_product_id(product.get("product_id"))
            if not product_id:
                continue
            try:
                candles = self.client.get_candles(product_id, granularity="FOUR_HOUR", limit=250)
                if len(candles) < 60:
                    continue
                rows = self._compute_indicators(candles)
                funding = self.client.get_funding_rate(product_id)
                funding_rate = _safe_float(funding.get("funding_rate"), 0.0)
                funding_annualized = _annualize_funding(funding_rate)

                long_score = self.scoring_engine.score_crypto_asset(product_id, rows, regime)
                latest = rows[-1]
                atr_val = _safe_float(latest.get("atr"), 0.0)
                close = _safe_float(latest.get("close"), 0.0)
                long_stop = close - (atr_val * 2.0 if atr_val > 0 else close * 0.03)
                long_target = close + (atr_val * 3.0 if atr_val > 0 else close * 0.05)
                long_rr = round((long_target - close) / max(0.0000001, close - long_stop), 2) if close > long_stop else 0.0
                long_leverage = 1
                if long_score["conviction"] >= 0.85:
                    long_leverage = 5
                elif long_score["conviction"] >= 0.75:
                    long_leverage = 4
                elif long_score["conviction"] >= 0.65:
                    long_leverage = 3
                elif long_score["conviction"] >= 0.55:
                    long_leverage = 2

                short_score = self._score_short_setup(product_id, rows, regime, funding_rate)

                long_opportunity = {
                    "product_id": product_id,
                    "display_name": str(product.get("display_name") or product_id),
                    "direction": "long",
                    "score": round(_safe_float(long_score.get("composite_score"), 0.0), 2),
                    "signal": str(long_score.get("signal") or "neutral"),
                    "funding_rate": funding_rate,
                    "funding_annualized": round(funding_annualized, 4),
                    "leverage_suggested": long_leverage,
                    "entry_price": round(close, 6),
                    "stop_loss": round(long_stop, 6),
                    "take_profit": round(long_target, 6),
                    "risk_reward": long_rr,
                    "reasoning": str(long_score.get("reasoning") or ""),
                }
                short_opportunity = {
                    "product_id": product_id,
                    "display_name": str(product.get("display_name") or product_id),
                    "direction": "short",
                    "score": round(_safe_float(short_score.get("score"), 0.0), 2),
                    "signal": str(short_score.get("signal") or "neutral"),
                    "funding_rate": funding_rate,
                    "funding_annualized": round(funding_annualized, 4),
                    "leverage_suggested": _safe_int(short_score.get("leverage_suggested"), 1),
                    "entry_price": round(_safe_float(short_score.get("entry_price"), close), 6),
                    "stop_loss": round(_safe_float(short_score.get("stop_loss"), close), 6),
                    "take_profit": round(_safe_float(short_score.get("take_profit"), close), 6),
                    "risk_reward": round(_safe_float(short_score.get("risk_reward"), 0.0), 2),
                    "reasoning": str(short_score.get("reasoning") or ""),
                }

                if long_opportunity["signal"] in {"strong_buy", "buy"} or long_opportunity["score"] >= 60:
                    opportunities.append(long_opportunity)
                if short_opportunity["signal"] in {"strong_sell", "sell"} or short_opportunity["score"] >= 60:
                    opportunities.append(short_opportunity)
            except Exception as exc:
                _log(f"scan_perps failed product_id={product_id} error={exc}")

        opportunities.sort(key=lambda row: row.get("score", 0.0), reverse=True)
        return opportunities

    def scan_funding_arbitrage(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for product in self.client.get_perps():
            product_id = _normalize_product_id(product.get("product_id"))
            if not product_id:
                continue
            try:
                funding = self.client.get_funding_rate(product_id)
                rate = _safe_float(funding.get("funding_rate"), 0.0)
                annualized = _annualize_funding(rate)
                if rate > 0.0003:
                    price = _safe_float(product.get("price"), 0.0)
                    estimated_daily_income = price * rate * 3.0
                    results.append(
                        {
                            "product_id": product_id,
                            "funding_rate_8h": round(rate * 100.0, 5),
                            "funding_annualized_pct": round(annualized, 2),
                            "direction": "short_perp_long_spot",
                            "estimated_daily_income": round(estimated_daily_income, 4),
                        }
                    )
            except Exception as exc:
                _log(f"scan_funding_arbitrage failed product_id={product_id} error={exc}")
        results.sort(key=lambda row: row.get("funding_annualized_pct", 0.0), reverse=True)
        return results

    def scan_basis_trade(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        products = list(self.client.get_all_products() or [])
        spot_map: dict[str, str] = {}
        for product in products:
            if product.get("category") == "crypto_perp":
                underlying = str(product.get("underlying") or "").upper().strip()
                if underlying:
                    spot_map.setdefault(underlying, f"{underlying}-USD")

        for product in products:
            if product.get("category") != "crypto_dated":
                continue

            underlying = str(product.get("underlying") or "").upper().strip()
            spot_product = spot_map.get(underlying) or (f"{underlying}-USD" if underlying else "")
            futures_product = _normalize_product_id(product.get("product_id"))
            futures_price = _safe_float(product.get("price"), 0.0)
            if not spot_product or not futures_product or futures_price <= 0:
                continue

            try:
                spot_quote = self.client.client.get_product(spot_product)
                spot_payload = spot_quote.to_dict() if hasattr(spot_quote, "to_dict") else dict(spot_quote or {})
                spot_price = _safe_float(spot_payload.get("price"), 0.0)
                if spot_price <= 0:
                    continue

                premium_pct = ((futures_price - spot_price) / spot_price) * 100.0
                days = _days_to_expiry(product.get("expiry"))
                if not days or days <= 0:
                    continue
                premium_annualized = premium_pct * (365.0 / max(days, 1))
                if premium_annualized <= 5.0:
                    continue

                results.append(
                    {
                        "spot_product": spot_product,
                        "futures_product": futures_product,
                        "spot_price": round(spot_price, 6),
                        "futures_price": round(futures_price, 6),
                        "premium_pct": round(premium_pct, 4),
                        "premium_annualized_pct": round(premium_annualized, 2),
                        "days_to_expiry": days,
                    }
                )
            except Exception as exc:
                _log(f"scan_basis_trade failed product_id={futures_product} error={exc}")

        results.sort(key=lambda row: row.get("premium_annualized_pct", 0.0), reverse=True)
        return results
