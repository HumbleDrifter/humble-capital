import time
from execution import get_client


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


def get_market_regime():
    closes = get_daily_closes("BTC-USD", 250)

    if len(closes) < 200:
        return {
            "regime": "neutral",
            "reason": "not_enough_data",
            "last_close": closes[-1] if closes else None,
            "ema200": None,
        }

    last_close = closes[-1]
    ema200 = ema(closes[-200:], 200)

    if ema200 is None:
        return {
            "regime": "neutral",
            "reason": "ema_failed",
            "last_close": last_close,
            "ema200": None,
        }

    diff_pct = ((last_close - ema200) / ema200) * 100

    if diff_pct >= 2:
        regime = "bull"
    elif diff_pct <= -2:
        regime = "risk_off"
    else:
        regime = "neutral"

    return {
        "regime": regime,
        "reason": "btc_vs_ema200",
        "last_close": last_close,
        "ema200": ema200,
        "diff_pct": diff_pct,
    }


if __name__ == "__main__":
    print(get_market_regime())
