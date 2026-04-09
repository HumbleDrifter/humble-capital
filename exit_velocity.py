import time

_PRICE_TICKS = {}
_CORE_SKIP = {"BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"}
_MAX_TICKS_PER_PRODUCT = 500
_MAX_AGE_SEC = 2 * 60 * 60


def _log(message):
    print(f"[exit_velocity] {message}")


def _now():
    return int(time.time())


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


def _prune_ticks(product_id, now_ts=None):
    product_id = _normalize_product_id(product_id)
    if not product_id:
        return []

    now_ts = _safe_int(now_ts, _now())
    ticks = _PRICE_TICKS.get(product_id) or []
    cutoff = now_ts - _MAX_AGE_SEC
    ticks = [
        {
            "price": _safe_float(tick.get("price"), 0.0),
            "ts": _safe_int(tick.get("ts"), 0),
        }
        for tick in ticks
        if _safe_int(tick.get("ts"), 0) >= cutoff
    ]

    if len(ticks) > _MAX_TICKS_PER_PRODUCT:
        ticks = ticks[-_MAX_TICKS_PER_PRODUCT:]

    _PRICE_TICKS[product_id] = ticks
    return ticks


def _latest_tick_before_or_at(ticks, ts_cutoff):
    eligible = [tick for tick in ticks if _safe_int(tick.get("ts"), 0) <= ts_cutoff]
    if not eligible:
        return None
    return eligible[-1]


def record_price_tick(product_id: str, price: float, timestamp: int = None) -> None:
    try:
        product_id = _normalize_product_id(product_id)
        price = _safe_float(price, 0.0)
        ts = _safe_int(timestamp, _now())

        if not product_id or price <= 0:
            return

        ticks = _prune_ticks(product_id, ts)
        ticks.append({"price": price, "ts": ts})

        if len(ticks) > _MAX_TICKS_PER_PRODUCT:
            ticks = ticks[-_MAX_TICKS_PER_PRODUCT:]

        _PRICE_TICKS[product_id] = ticks
    except Exception as exc:
        _log(f"record tick failed product_id={product_id} error={exc}")


def check_velocity_exit(product_id: str, current_price: float, entry_price: float) -> dict:
    try:
        product_id = _normalize_product_id(product_id)
        current_price = _safe_float(current_price, 0.0)
        entry_price = _safe_float(entry_price, 0.0)

        result = {"should_exit": False}

        if not product_id or product_id in _CORE_SKIP or current_price <= 0:
            return result

        record_price_tick(product_id, current_price)
        now_ts = _now()
        ticks = _prune_ticks(product_id, now_ts)
        if not ticks:
            return result

        crash_threshold_pct = 8.0
        accelerating_loss_pct = 5.0

        peak_tick = max(ticks, key=lambda tick: _safe_float(tick.get("price"), 0.0))
        peak_2h = _safe_float(peak_tick.get("price"), 0.0)
        peak_ts = _safe_int(peak_tick.get("ts"), now_ts)

        if peak_2h > 0:
            drop_pct = ((peak_2h - current_price) / peak_2h) * 100.0
            if drop_pct > crash_threshold_pct:
                return {
                    "should_exit": True,
                    "reason": "velocity_crash",
                    "drop_pct": round(drop_pct, 4),
                    "peak_2h": round(peak_2h, 8),
                    "minutes_since_peak": round(max(0, now_ts - peak_ts) / 60.0, 2),
                }

        if entry_price > 0 and current_price < entry_price:
            tick_30m = _latest_tick_before_or_at(ticks, now_ts - (30 * 60))
            if tick_30m:
                price_30m = _safe_float(tick_30m.get("price"), 0.0)
                if price_30m > 0:
                    drop_30m_pct = ((price_30m - current_price) / price_30m) * 100.0
                    if drop_30m_pct > accelerating_loss_pct:
                        return {
                            "should_exit": True,
                            "reason": "accelerating_loss",
                            "drop_pct": round(drop_30m_pct, 4),
                            "peak_2h": round(peak_2h, 8),
                            "minutes_since_peak": round(max(0, now_ts - peak_ts) / 60.0, 2),
                        }

        return result
    except Exception as exc:
        _log(f"velocity exit check failed product_id={product_id} error={exc}")
        return {"should_exit": False}


def get_velocity_status(product_id: str) -> dict:
    try:
        product_id = _normalize_product_id(product_id)
        if not product_id:
            return {
                "product_id": "",
                "change_30m_pct": 0.0,
                "change_1h_pct": 0.0,
                "high_2h": 0.0,
                "tick_count": 0,
            }

        now_ts = _now()
        ticks = _prune_ticks(product_id, now_ts)
        if not ticks:
            return {
                "product_id": product_id,
                "change_30m_pct": 0.0,
                "change_1h_pct": 0.0,
                "high_2h": 0.0,
                "tick_count": 0,
            }

        current_price = _safe_float(ticks[-1].get("price"), 0.0)
        tick_30m = _latest_tick_before_or_at(ticks, now_ts - (30 * 60))
        tick_1h = _latest_tick_before_or_at(ticks, now_ts - (60 * 60))
        high_2h = max((_safe_float(tick.get("price"), 0.0) for tick in ticks), default=0.0)

        change_30m_pct = 0.0
        if tick_30m and _safe_float(tick_30m.get("price"), 0.0) > 0:
            base_30m = _safe_float(tick_30m.get("price"), 0.0)
            change_30m_pct = ((current_price - base_30m) / base_30m) * 100.0

        change_1h_pct = 0.0
        if tick_1h and _safe_float(tick_1h.get("price"), 0.0) > 0:
            base_1h = _safe_float(tick_1h.get("price"), 0.0)
            change_1h_pct = ((current_price - base_1h) / base_1h) * 100.0

        return {
            "product_id": product_id,
            "change_30m_pct": round(change_30m_pct, 4),
            "change_1h_pct": round(change_1h_pct, 4),
            "high_2h": round(high_2h, 8),
            "tick_count": len(ticks),
        }
    except Exception as exc:
        _log(f"velocity status failed product_id={product_id} error={exc}")
        return {
            "product_id": _normalize_product_id(product_id),
            "change_30m_pct": 0.0,
            "change_1h_pct": 0.0,
            "high_2h": 0.0,
            "tick_count": 0,
        }
