import math
import time

from regime import get_daily_closes


_CORRELATION_CACHE_TTL_SEC = 1800
_CORRELATION_CACHE = {}
_CORE_SKIP = {"BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"}


def _log(message):
    print(f"[correlation] {message}")


def _normalize_product_id(product_id):
    return str(product_id or "").upper().strip()


def _cache_key(product_a, product_b, window_days):
    left = _normalize_product_id(product_a)
    right = _normalize_product_id(product_b)
    ordered = tuple(sorted((left, right)))
    return f"{ordered[0]}|{ordered[1]}|{int(window_days)}"


def _cache_get(product_a, product_b, window_days):
    key = _cache_key(product_a, product_b, window_days)
    entry = _CORRELATION_CACHE.get(key)
    if not isinstance(entry, dict):
        return None
    ts = float(entry.get("ts", 0.0) or 0.0)
    if (time.time() - ts) >= _CORRELATION_CACHE_TTL_SEC:
        return None
    try:
        return float(entry.get("value", 0.0) or 0.0)
    except Exception:
        return None


def _cache_set(product_a, product_b, window_days, value):
    key = _cache_key(product_a, product_b, window_days)
    _CORRELATION_CACHE[key] = {
        "ts": time.time(),
        "value": float(value or 0.0),
    }


def _pearson_correlation(xs, ys):
    n = min(len(xs), len(ys))
    if n < 2:
        return 0.0

    x_vals = [float(v) for v in xs[-n:]]
    y_vals = [float(v) for v in ys[-n:]]

    sum_x = sum(x_vals)
    sum_y = sum(y_vals)
    sum_xy = sum(x * y for x, y in zip(x_vals, y_vals))
    sum_x2 = sum(x * x for x in x_vals)
    sum_y2 = sum(y * y for y in y_vals)

    numerator = (n * sum_xy) - (sum_x * sum_y)
    denom_left = (n * sum_x2) - (sum_x * sum_x)
    denom_right = (n * sum_y2) - (sum_y * sum_y)
    denominator = math.sqrt(max(0.0, denom_left) * max(0.0, denom_right))

    if denominator <= 0:
        return 0.0

    corr = numerator / denominator
    return max(-1.0, min(1.0, float(corr)))


def get_rolling_correlation(product_a: str, product_b: str, window_days: int = 7) -> float:
    try:
        product_a = _normalize_product_id(product_a)
        product_b = _normalize_product_id(product_b)
        window_days = max(2, int(window_days or 7))

        if not product_a or not product_b:
            return 0.0
        if product_a == product_b:
            return 1.0

        cached = _cache_get(product_a, product_b, window_days)
        if cached is not None:
            return cached

        lookback_days = max(window_days + 10, 21)
        closes_a = get_daily_closes(product_a, lookback_days)
        closes_b = get_daily_closes(product_b, lookback_days)

        if not closes_a or not closes_b:
            _log(f"insufficient data product_a={product_a} product_b={product_b}")
            _cache_set(product_a, product_b, window_days, 0.0)
            return 0.0

        n = min(len(closes_a), len(closes_b), window_days)
        if n < 2:
            _cache_set(product_a, product_b, window_days, 0.0)
            return 0.0

        corr = _pearson_correlation(closes_a[-n:], closes_b[-n:])
        _cache_set(product_a, product_b, window_days, corr)
        _log(
            f"computed correlation product_a={product_a} product_b={product_b} "
            f"window_days={window_days} corr={corr:.4f}"
        )
        return corr
    except Exception as exc:
        _log(
            f"correlation failed product_a={product_a} product_b={product_b} "
            f"window_days={window_days} error={exc}"
        )
        return 0.0


def get_correlated_positions(product_id: str, snapshot: dict, threshold: float = 0.70) -> list:
    try:
        product_id = _normalize_product_id(product_id)
        snapshot = snapshot if isinstance(snapshot, dict) else {}
        positions = snapshot.get("positions") or {}
        config = snapshot.get("config") or {}
        core_assets = set(str(x or "").upper().strip() for x in (config.get("core_assets") or {}).keys())
        threshold = float(threshold or 0.70)

        correlated = []
        for existing_product_id, position in positions.items():
            existing_product_id = _normalize_product_id(existing_product_id)
            if not existing_product_id or existing_product_id == product_id:
                continue
            if existing_product_id in _CORE_SKIP:
                continue
            if existing_product_id in core_assets:
                continue

            value_usd = float((position or {}).get("value_total_usd", 0.0) or 0.0)
            if value_usd <= 0:
                continue

            corr = get_rolling_correlation(product_id, existing_product_id, window_days=7)
            if corr >= threshold:
                correlated.append(
                    {
                        "product_id": existing_product_id,
                        "correlation": round(float(corr), 4),
                        "value_usd": round(value_usd, 2),
                    }
                )

        correlated.sort(
            key=lambda row: (
                float(row.get("correlation", 0.0)),
                float(row.get("value_usd", 0.0)),
            ),
            reverse=True,
        )
        return correlated
    except Exception as exc:
        _log(f"get_correlated_positions failed product_id={product_id} error={exc}")
        return []


def should_block_correlated_buy(
    product_id: str,
    snapshot: dict,
    threshold: float = 0.70,
    max_correlated_usd: float = 100.0,
) -> dict:
    try:
        product_id = _normalize_product_id(product_id)
        snapshot = snapshot if isinstance(snapshot, dict) else {}

        if product_id in _CORE_SKIP:
            return {
                "blocked": False,
                "correlated_positions": [],
                "total_correlated_usd": 0.0,
            }

        correlated_positions = get_correlated_positions(product_id, snapshot, threshold=threshold)
        total_correlated_usd = round(
            sum(float(row.get("value_usd", 0.0) or 0.0) for row in correlated_positions),
            2,
        )

        blocked = total_correlated_usd > float(max_correlated_usd or 100.0)
        result = {
            "blocked": blocked,
            "correlated_positions": correlated_positions,
            "total_correlated_usd": total_correlated_usd,
        }
        if blocked:
            result["reason"] = "correlated_exposure"
            _log(
                f"blocked correlated buy product_id={product_id} "
                f"total_correlated_usd={total_correlated_usd:.2f}"
            )
        return result
    except Exception as exc:
        _log(f"should_block_correlated_buy failed product_id={product_id} error={exc}")
        return {
            "blocked": False,
            "correlated_positions": [],
            "total_correlated_usd": 0.0,
        }
