import time

from execution import get_client
from portfolio import load_asset_config
from regime import get_daily_closes


_VOLATILITY_CACHE_TTL_SEC = 600
_VOLATILITY_CACHE = {}


def _to_dict(value):
    return value.to_dict() if hasattr(value, "to_dict") else value


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _log(message):
    print(f"[position_sizing] {message}")


def _normalize_product_id(product_id):
    return str(product_id or "").upper().strip()


def _get_runtime_config():
    try:
        config = load_asset_config() or {}
        if isinstance(config, dict):
            return config
    except Exception as exc:
        _log(f"load_asset_config failed; using defaults error={exc}")
    return {}


def _get_config_thresholds():
    config = _get_runtime_config()
    return {
        "trade_min_value_usd": _safe_float(config.get("trade_min_value_usd", 5.0), 5.0),
        "max_quote_per_trade_usd": _safe_float(config.get("max_quote_per_trade_usd", 50.0), 50.0),
    }


def _cache_get(product_id):
    entry = _VOLATILITY_CACHE.get(product_id)
    if not isinstance(entry, dict):
        return None
    ts = float(entry.get("ts", 0.0) or 0.0)
    if (time.time() - ts) >= _VOLATILITY_CACHE_TTL_SEC:
        return None
    value = entry.get("value")
    return dict(value) if isinstance(value, dict) else None


def _cache_set(product_id, value):
    _VOLATILITY_CACHE[product_id] = {
        "ts": time.time(),
        "value": dict(value),
    }


def get_daily_hlc(product_id, days):
    product_id = _normalize_product_id(product_id)
    if not product_id:
        return []

    client = get_client()
    end_ts = int(time.time())
    start_ts = end_ts - (int(days) * 24 * 60 * 60)

    try:
        response = _to_dict(
            client.get_candles(
                product_id=product_id,
                start=str(start_ts),
                end=str(end_ts),
                granularity="ONE_DAY",
            )
        )
    except Exception as exc:
        _log(f"get_daily_hlc failed product_id={product_id} error={exc}")
        return []

    candles = response.get("candles", []) if isinstance(response, dict) else []
    if not candles:
        return []

    candles = sorted(candles, key=lambda row: int(row["start"]))
    out = []
    for candle in candles:
        try:
            out.append(
                {
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": float(candle["close"]),
                }
            )
        except Exception:
            continue
    return out


def _compute_atr(hlc_rows, period=14):
    if len(hlc_rows) < period + 1:
        return None, None

    true_ranges = []
    prev_close = float(hlc_rows[0]["close"])

    for row in hlc_rows[1:]:
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        true_range = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        true_ranges.append(true_range)
        prev_close = close

    if len(true_ranges) < period:
        return None, None

    atr = sum(true_ranges[-period:]) / float(period)
    price = float(hlc_rows[-1]["close"])
    return atr, price


def _classify_bucket(atr_pct):
    if atr_pct > 6.0:
        return "very_high"
    if atr_pct >= 3.0:
        return "high"
    if atr_pct >= 1.5:
        return "medium"
    return "low"


def get_asset_volatility(product_id: str) -> dict:
    product_id = _normalize_product_id(product_id)
    if not product_id:
        return {
            "product_id": "",
            "atr_pct": 0.0,
            "bucket": "medium",
            "raw_atr": 0.0,
            "price": 0.0,
        }

    cached = _cache_get(product_id)
    if cached:
        return cached

    hlc_rows = get_daily_hlc(product_id, 30)
    atr, price = _compute_atr(hlc_rows, period=14)

    if atr is None or price is None or price <= 0:
        closes = get_daily_closes(product_id, 30)
        price = _safe_float(closes[-1], 0.0) if closes else 0.0
        atr = 0.0
        atr_pct = 0.0
        bucket = "medium"
        _log(f"using fallback volatility values product_id={product_id}")
    else:
        atr_pct = (atr / price) * 100.0
        bucket = _classify_bucket(atr_pct)

    result = {
        "product_id": product_id,
        "atr_pct": round(float(atr_pct), 4),
        "bucket": bucket,
        "raw_atr": round(float(atr or 0.0), 8),
        "price": round(float(price or 0.0), 8),
    }
    _cache_set(product_id, result)
    _log(
        "computed volatility "
        f"product_id={product_id} bucket={result['bucket']} atr_pct={result['atr_pct']}"
    )
    return result


def compute_risk_adjusted_size(
    product_id: str,
    base_size_usd: float,
    signal_type: str = "",
    regime: str = "neutral",
    conviction_score: float = 1.0,
) -> dict:
    product_id = _normalize_product_id(product_id)
    base_size_usd = max(0.0, _safe_float(base_size_usd, 0.0))
    signal_type = str(signal_type or "").upper().strip()
    regime = str(regime or "neutral").lower().strip()

    volatility = get_asset_volatility(product_id)
    bucket = str(volatility.get("bucket") or "medium").lower().strip()
    atr_pct = _safe_float(volatility.get("atr_pct"), 0.0)

    vol_scalars = {
        "very_high": 0.4,
        "high": 0.6,
        "medium": 0.85,
        "low": 1.0,
    }
    regime_scalars = {
        "bull": 1.2,
        "neutral": 1.0,
        "caution": 0.7,
        "risk_off": 0.4,
    }
    signal_scalars = {
        "CORE_BUY_WINDOW": 1.0,
        "SATELLITE_BUY": 0.8,
        "SNIPER_BUY": 0.5,
        "SATELLITE_BUY_HEAVY": 1.0,
    }

    vol_scalar = vol_scalars.get(bucket, 0.85)
    regime_scalar = regime_scalars.get(regime, 1.0)
    conviction_scalar = min(1.5, max(0.5, _safe_float(conviction_score, 1.0)))
    signal_scalar = signal_scalars.get(signal_type, 1.0)

    raw_size = base_size_usd * vol_scalar * regime_scalar * conviction_scalar * signal_scalar
    thresholds = _get_config_thresholds()
    trade_min_value_usd = max(0.0, _safe_float(thresholds["trade_min_value_usd"], 5.0))
    max_quote_per_trade_usd = max(0.0, _safe_float(thresholds["max_quote_per_trade_usd"], 50.0))

    adjusted_size = raw_size
    if adjusted_size > 0 and adjusted_size < trade_min_value_usd:
        adjusted_size = trade_min_value_usd
    if max_quote_per_trade_usd > 0:
        adjusted_size = min(adjusted_size, max_quote_per_trade_usd)

    result = {
        "adjusted_size_usd": round(max(0.0, adjusted_size), 2),
        "base_size_usd": round(base_size_usd, 2),
        "volatility_bucket": bucket,
        "atr_pct": round(atr_pct, 4),
        "scalars": {
            "volatility": vol_scalar,
            "regime": regime_scalar,
            "conviction": conviction_scalar,
            "signal_type": signal_scalar,
            "trade_min_value_usd": trade_min_value_usd,
            "max_quote_per_trade_usd": max_quote_per_trade_usd,
        },
    }
    _log(
        "computed risk adjusted size "
        f"product_id={product_id} base={base_size_usd:.2f} adjusted={result['adjusted_size_usd']:.2f} "
        f"bucket={bucket} regime={regime} signal_type={signal_type or 'DEFAULT'}"
    )
    return result


def get_portfolio_risk_summary(snapshot: dict) -> dict:
    snapshot = snapshot if isinstance(snapshot, dict) else {}
    positions = snapshot.get("positions") or {}
    total_value_usd = _safe_float(snapshot.get("total_value_usd"), 0.0)

    concentration_by_bucket = {}
    total_atr_weighted_exposure = 0.0
    highest_risk_position = None
    processed_positions = 0

    for product_id, position in positions.items():
        product_id = _normalize_product_id(product_id)
        if not product_id:
            continue

        value_total_usd = _safe_float(position.get("value_total_usd"), 0.0)
        if value_total_usd <= 0:
            continue

        vol = get_asset_volatility(product_id)
        bucket = str(vol.get("bucket") or "medium").lower().strip()
        atr_pct = _safe_float(vol.get("atr_pct"), 0.0)
        risk_exposure_usd = value_total_usd * (atr_pct / 100.0)

        bucket_row = concentration_by_bucket.setdefault(
            bucket,
            {
                "bucket": bucket,
                "positions": 0,
                "value_usd": 0.0,
                "portfolio_weight": 0.0,
                "atr_weighted_exposure_usd": 0.0,
            },
        )
        bucket_row["positions"] += 1
        bucket_row["value_usd"] += value_total_usd
        bucket_row["atr_weighted_exposure_usd"] += risk_exposure_usd

        total_atr_weighted_exposure += risk_exposure_usd
        processed_positions += 1

        candidate = {
            "product_id": product_id,
            "value_total_usd": round(value_total_usd, 2),
            "weight_total": round(_safe_float(position.get("weight_total"), 0.0), 6),
            "volatility_bucket": bucket,
            "atr_pct": round(atr_pct, 4),
            "atr_weighted_exposure_usd": round(risk_exposure_usd, 2),
        }
        if (
            highest_risk_position is None
            or candidate["atr_weighted_exposure_usd"] > highest_risk_position["atr_weighted_exposure_usd"]
        ):
            highest_risk_position = candidate

    for row in concentration_by_bucket.values():
        if total_value_usd > 0:
            row["portfolio_weight"] = round(row["value_usd"] / total_value_usd, 6)
        row["value_usd"] = round(row["value_usd"], 2)
        row["atr_weighted_exposure_usd"] = round(row["atr_weighted_exposure_usd"], 2)

    result = {
        "total_positions": processed_positions,
        "total_value_usd": round(total_value_usd, 2),
        "total_atr_weighted_exposure": round(total_atr_weighted_exposure, 2),
        "concentration_by_bucket": dict(sorted(concentration_by_bucket.items())),
        "highest_risk_position": highest_risk_position,
        "generated_at": int(time.time()),
    }
    _log(
        "built portfolio risk summary "
        f"positions={processed_positions} total_atr_weighted_exposure={result['total_atr_weighted_exposure']:.2f}"
    )
    return result
