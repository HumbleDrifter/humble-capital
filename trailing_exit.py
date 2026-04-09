import time

_POSITION_PEAKS = {}
_CORE_SKIP = {"BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"}


def _log(message):
    print(f"[trailing_exit] {message}")


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


def update_position_tracking(product_id: str, current_price: float, entry_price: float, position_age_sec: int) -> None:
    try:
        product_id = _normalize_product_id(product_id)
        current_price = _safe_float(current_price, 0.0)
        entry_price = _safe_float(entry_price, 0.0)
        position_age_sec = max(0, _safe_int(position_age_sec, 0))

        if not product_id or current_price <= 0:
            return

        now = _now()
        first_seen_at = now - position_age_sec if position_age_sec > 0 else now
        existing = _POSITION_PEAKS.get(product_id) or {}

        peak_price = max(
            current_price,
            _safe_float(existing.get("peak_price"), 0.0),
            entry_price,
        )

        _POSITION_PEAKS[product_id] = {
            "peak_price": peak_price,
            "first_seen_at": min(
                _safe_int(existing.get("first_seen_at"), first_seen_at) or first_seen_at,
                first_seen_at,
            ),
        }
    except Exception as exc:
        _log(f"update tracking failed product_id={product_id} error={exc}")


def check_trailing_stop(product_id: str, current_price: float, entry_price: float) -> dict:
    try:
        product_id = _normalize_product_id(product_id)
        current_price = _safe_float(current_price, 0.0)
        entry_price = _safe_float(entry_price, 0.0)

        result = {
            "should_exit": False,
            "reason": "trailing_stop",
            "gain_from_entry_pct": 0.0,
            "pullback_from_peak_pct": 0.0,
            "peak_price": 0.0,
        }

        if not product_id or current_price <= 0 or entry_price <= 0:
            return result

        tracking = _POSITION_PEAKS.get(product_id) or {}
        peak_price = max(
            current_price,
            _safe_float(tracking.get("peak_price"), 0.0),
            entry_price,
        )
        _POSITION_PEAKS[product_id] = {
            "peak_price": peak_price,
            "first_seen_at": _safe_int(tracking.get("first_seen_at"), _now()),
        }

        activation_gain_pct = 20.0
        trailing_stop_pct = 10.0
        gain_from_entry_pct = ((current_price - entry_price) / entry_price) * 100.0
        pullback_from_peak_pct = ((peak_price - current_price) / peak_price) * 100.0 if peak_price > 0 else 0.0

        result.update(
            {
                "gain_from_entry_pct": round(gain_from_entry_pct, 4),
                "pullback_from_peak_pct": round(pullback_from_peak_pct, 4),
                "peak_price": round(peak_price, 8),
            }
        )

        if (((peak_price - entry_price) / entry_price) * 100.0) >= activation_gain_pct and pullback_from_peak_pct >= trailing_stop_pct:
            result["should_exit"] = True

        return result
    except Exception as exc:
        _log(f"check trailing stop failed product_id={product_id} error={exc}")
        return {"should_exit": False}


def check_stale_position(product_id: str, current_price: float, entry_price: float, position_opened_at: int) -> dict:
    try:
        product_id = _normalize_product_id(product_id)
        current_price = _safe_float(current_price, 0.0)
        entry_price = _safe_float(entry_price, 0.0)
        position_opened_at = _safe_int(position_opened_at, 0)

        result = {
            "should_exit": False,
            "reason": "stale_position",
            "age_hours": 0.0,
            "move_pct": 0.0,
        }

        if not product_id or current_price <= 0 or entry_price <= 0 or position_opened_at <= 0:
            return result

        stale_hours = 72.0
        stale_move_pct = 5.0
        age_sec = max(0, _now() - position_opened_at)
        age_hours = age_sec / 3600.0
        move_pct = abs(((current_price - entry_price) / entry_price) * 100.0)

        result.update(
            {
                "age_hours": round(age_hours, 4),
                "move_pct": round(move_pct, 4),
            }
        )

        if age_hours > stale_hours and move_pct < stale_move_pct:
            result["should_exit"] = True

        return result
    except Exception as exc:
        _log(f"check stale position failed product_id={product_id} error={exc}")
        return {"should_exit": False}


def evaluate_all_exits(snapshot: dict) -> list:
    try:
        snapshot = snapshot if isinstance(snapshot, dict) else {}
        positions = snapshot.get("positions") or {}
        config = snapshot.get("config") or {}
        core_assets = set(str(k or "").upper().strip() for k in (config.get("core_assets") or {}).keys())

        results = []

        for product_id, position in positions.items():
            product_id = _normalize_product_id(product_id)
            position = position if isinstance(position, dict) else {}

            if not product_id or product_id in _CORE_SKIP or product_id in core_assets:
                continue

            value_total_usd = _safe_float(position.get("value_total_usd"), 0.0)
            if value_total_usd <= 0:
                continue

            current_price = _safe_float(
                position.get("price_usd", position.get("current_price", position.get("last_price", 0.0))),
                0.0,
            )
            entry_price = _safe_float(
                position.get("avg_entry_price", position.get("entry_price", 0.0)),
                0.0,
            )
            opened_at = _safe_int(
                position.get("position_opened_at", position.get("first_buy_ts", position.get("opened_at", 0))),
                0,
            )

            if current_price <= 0 or entry_price <= 0:
                continue

            age_sec = max(0, _now() - opened_at) if opened_at > 0 else 0
            update_position_tracking(product_id, current_price, entry_price, age_sec)

            trailing = check_trailing_stop(product_id, current_price, entry_price)
            if trailing.get("should_exit"):
                results.append(
                    {
                        "product_id": product_id,
                        "reason": "trailing_stop",
                        "details": trailing,
                    }
                )
                continue

            stale = check_stale_position(product_id, current_price, entry_price, opened_at)
            if stale.get("should_exit"):
                results.append(
                    {
                        "product_id": product_id,
                        "reason": "stale_position",
                        "details": stale,
                    }
                )

        return results
    except Exception as exc:
        _log(f"evaluate all exits failed error={exc}")
        return []


def clear_tracking(product_id: str) -> None:
    try:
        product_id = _normalize_product_id(product_id)
        if not product_id:
            return
        _POSITION_PEAKS.pop(product_id, None)
    except Exception as exc:
        _log(f"clear tracking failed product_id={product_id} error={exc}")
