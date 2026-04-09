import time
from execution import get_client


_REGIME_CACHE = {"ts": 0, "value": None}
_REGIME_CACHE_TTL_SEC = 300
_ALT_BREADTH_SYMBOLS = ("ETH-USD", "SOL-USD", "XRP-USD")


def _to_dict(x):
    return x.to_dict() if hasattr(x, "to_dict") else x


def get_daily_closes(product_id="BTC-USD", days=250):
    client = get_client()

    end_ts = int(time.time())
    start_ts = end_ts - (days * 24 * 60 * 60)

    resp = _to_dict(
        client.get_candles(
            product_id=product_id,
            start=str(start_ts),
            end=str(end_ts),
            granularity="ONE_DAY",
        )
    )

    candles = resp.get("candles", [])
    if not candles:
        return []

    candles = sorted(candles, key=lambda x: int(x["start"]))
    closes = [float(c["close"]) for c in candles]

    return closes


def ema(values, length):
    if not values or len(values) < length:
        return None

    k = 2 / (length + 1)
    ema_val = values[0]

    for v in values[1:]:
        ema_val = (v * k) + (ema_val * (1 - k))

    return ema_val


def _safe_float(value, default=None):
    try:
        return float(value)
    except Exception:
        return default


def _get_daily_candles(product_id="BTC-USD", days=250):
    client = get_client()

    end_ts = int(time.time())
    start_ts = end_ts - (days * 24 * 60 * 60)

    resp = _to_dict(
        client.get_candles(
            product_id=product_id,
            start=str(start_ts),
            end=str(end_ts),
            granularity="ONE_DAY",
        )
    )

    candles = resp.get("candles", [])
    if not candles:
        return []

    candles = sorted(candles, key=lambda x: int(x["start"]))
    normalized = []

    for candle in candles:
        normalized.append(
            {
                "start": int(candle["start"]),
                "low": float(candle["low"]),
                "high": float(candle["high"]),
                "open": float(candle["open"]),
                "close": float(candle["close"]),
                "volume": float(candle.get("volume", 0.0) or 0.0),
            }
        )

    return normalized


def get_atr_pct(product_id, period=14):
    candles = _get_daily_candles(product_id, max(period + 2, 40))
    if len(candles) < period + 1:
        return None

    true_ranges = []
    prev_close = candles[0]["close"]

    for candle in candles[1:]:
        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle["close"])
        true_range = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        true_ranges.append(true_range)
        prev_close = close

    if len(true_ranges) < period:
        return None

    atr = sum(true_ranges[-period:]) / period
    last_close = float(candles[-1]["close"] or 0.0)
    if last_close <= 0:
        return None

    return (atr / last_close) * 100.0


def _score_btc_trend(closes):
    last_close = closes[-1] if closes else None
    ema50 = ema(closes[-50:], 50) if len(closes) >= 50 else None
    ema200 = ema(closes[-200:], 200) if len(closes) >= 200 else None

    if last_close is None or ema50 is None or ema200 is None:
        return {
            "score": 50.0,
            "state": "insufficient_data",
            "reason": "insufficient_data",
            "last_close": last_close,
            "ema50": ema50,
            "ema200": ema200,
        }

    above_50 = last_close >= ema50
    above_200 = last_close >= ema200

    if above_50 and above_200:
        score = 100.0
        state = "bullish"
        reason = "above_ema50_and_ema200"
    elif (last_close >= min(ema50, ema200)) and (last_close <= max(ema50, ema200)):
        score = 50.0
        state = "caution"
        reason = "between_ema50_and_ema200"
    else:
        score = 0.0
        state = "bearish"
        reason = "below_ema50_and_ema200"

    return {
        "score": score,
        "state": state,
        "reason": reason,
        "last_close": last_close,
        "ema50": ema50,
        "ema200": ema200,
    }


def _score_btc_volatility(atr_pct):
    if atr_pct is None:
        return {
            "score": 50.0,
            "state": "unknown",
            "reason": "atr_unavailable",
            "atr_pct": None,
        }

    if atr_pct < 2.0:
        score = 100.0
        state = "low_volatility"
        reason = "atr_below_2pct"
    elif atr_pct > 4.0:
        score = 0.0
        state = "high_volatility"
        reason = "atr_above_4pct"
    else:
        score = round(((4.0 - atr_pct) / 2.0) * 100.0, 2)
        state = "moderate_volatility"
        reason = "atr_between_2pct_and_4pct"

    return {
        "score": score,
        "state": state,
        "reason": reason,
        "atr_pct": atr_pct,
    }


def _score_alt_breadth():
    try:
        details = []
        above_count = 0

        for product_id in _ALT_BREADTH_SYMBOLS:
            closes = get_daily_closes(product_id, 40)
            if len(closes) < 21:
                raise ValueError(f"not enough data for {product_id}")

            last_close = closes[-1]
            ema21 = ema(closes[-21:], 21)
            if ema21 is None:
                raise ValueError(f"ema failed for {product_id}")

            above_ema21 = last_close >= ema21
            if above_ema21:
                above_count += 1

            details.append(
                {
                    "product_id": product_id,
                    "last_close": last_close,
                    "ema21": ema21,
                    "above_ema21": above_ema21,
                }
            )

        breadth_pct = (above_count / len(_ALT_BREADTH_SYMBOLS)) * 100.0

        if breadth_pct > 66.0:
            score = 100.0
            state = "bullish_confirmation"
            reason = "breadth_above_66pct"
        elif breadth_pct < 33.0:
            score = 0.0
            state = "bearish_confirmation"
            reason = "breadth_below_33pct"
        else:
            score = round(((breadth_pct - 33.0) / 33.0) * 100.0, 2)
            state = "mixed_confirmation"
            reason = "breadth_between_33pct_and_66pct"

        return {
            "score": score,
            "state": state,
            "reason": reason,
            "breadth_pct": round(breadth_pct, 2),
            "details": details,
            "fallback_used": False,
        }
    except Exception:
        return {
            "score": 50.0,
            "state": "neutral_fallback",
            "reason": "breadth_fallback_neutral",
            "breadth_pct": 50.0,
            "details": [],
            "fallback_used": True,
        }


def _classify_regime(score):
    if score >= 70.0:
        return "bull"
    if score >= 45.0:
        return "neutral"
    if score >= 25.0:
        return "caution"
    return "risk_off"


def _compute_regime_detail():
    closes = get_daily_closes("BTC-USD", 250)
    last_close = closes[-1] if closes else None
    ema200 = ema(closes[-200:], 200) if len(closes) >= 200 else None
    ema50 = ema(closes[-50:], 50) if len(closes) >= 50 else None

    if len(closes) < 200:
        return {
            "regime": "neutral",
            "reason": "not_enough_data",
            "last_close": last_close,
            "ema200": ema200,
            "ema50": ema50,
            "diff_pct": None,
            "atr_pct": None,
            "composite_score": 50.0,
            "trend_score": 50.0,
            "volatility_score": 50.0,
            "breadth_score": 50.0,
            "trend_state": "insufficient_data",
            "volatility_state": "unknown",
            "breadth_state": "neutral_fallback",
            "breadth_pct": 50.0,
            "breadth_details": [],
            "updated_at": int(time.time()),
        }

    if ema200 is None:
        return {
            "regime": "neutral",
            "reason": "ema_failed",
            "last_close": last_close,
            "ema200": None,
            "ema50": ema50,
            "diff_pct": None,
            "atr_pct": None,
            "composite_score": 50.0,
            "trend_score": 50.0,
            "volatility_score": 50.0,
            "breadth_score": 50.0,
            "trend_state": "insufficient_data",
            "volatility_state": "unknown",
            "breadth_state": "neutral_fallback",
            "breadth_pct": 50.0,
            "breadth_details": [],
            "updated_at": int(time.time()),
        }

    trend_detail = _score_btc_trend(closes)
    atr_pct = get_atr_pct("BTC-USD", 14)
    volatility_detail = _score_btc_volatility(atr_pct)
    breadth_detail = _score_alt_breadth()

    composite_score = round(
        (trend_detail["score"] * 0.50)
        + (volatility_detail["score"] * 0.20)
        + (breadth_detail["score"] * 0.30),
        2,
    )
    regime = _classify_regime(composite_score)
    diff_pct = ((last_close - ema200) / ema200) * 100 if ema200 else None

    return {
        "regime": regime,
        "reason": "btc_trend_volatility_breadth",
        "last_close": last_close,
        "ema200": ema200,
        "ema50": trend_detail.get("ema50"),
        "diff_pct": diff_pct,
        "atr_pct": volatility_detail.get("atr_pct"),
        "breadth_pct": breadth_detail.get("breadth_pct"),
        "composite_score": composite_score,
        "trend_score": trend_detail.get("score"),
        "volatility_score": volatility_detail.get("score"),
        "breadth_score": breadth_detail.get("score"),
        "trend_state": trend_detail.get("state"),
        "volatility_state": volatility_detail.get("state"),
        "breadth_state": breadth_detail.get("state"),
        "trend_reason": trend_detail.get("reason"),
        "volatility_reason": volatility_detail.get("reason"),
        "breadth_reason": breadth_detail.get("reason"),
        "breadth_details": breadth_detail.get("details", []),
        "breadth_fallback_used": breadth_detail.get("fallback_used", False),
        "weights": {
            "btc_trend": 0.50,
            "btc_volatility": 0.20,
            "alt_breadth": 0.30,
        },
        "updated_at": int(time.time()),
    }


def get_regime_detail():
    now = int(time.time())
    cached = _REGIME_CACHE.get("value")
    cached_ts = int(_REGIME_CACHE.get("ts") or 0)

    if cached and (now - cached_ts) < _REGIME_CACHE_TTL_SEC:
        return dict(cached)

    detail = _compute_regime_detail()
    _REGIME_CACHE["ts"] = now
    _REGIME_CACHE["value"] = dict(detail)
    return dict(detail)


def get_market_regime():
    return get_regime_detail()


if __name__ == "__main__":
    print(get_market_regime())
