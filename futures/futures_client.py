from __future__ import annotations

import re
import threading
import time
import uuid
from typing import Any

from execution import get_client

_FUTURES_CACHE: dict[str, dict[str, Any]] = {}
_CACHE_LOCK = threading.RLock()
_CACHE_TTL = 60  # 1 minute


def _to_dict(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        try:
            return value.to_dict()
        except Exception:
            pass
    if isinstance(value, dict):
        return value
    if isinstance(value, (list, tuple)):
        return [_to_dict(item) for item in value]
    if hasattr(value, "__dict__"):
        try:
            return {
                key: _to_dict(val)
                for key, val in vars(value).items()
                if not key.startswith("_")
            }
        except Exception:
            pass
    return value


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, dict):
            value = value.get("value", value.get("amount", default))
        return float(value or 0.0)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return int(default)


def _normalize_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"BUY", "BID", "LONG"}:
        return "BUY"
    if side in {"SELL", "ASK", "SHORT"}:
        return "SELL"
    return side or "BUY"


def _money_value(value: Any) -> float:
    if isinstance(value, dict):
        return _safe_float(value.get("value", value.get("amount", 0.0)))
    return _safe_float(value, 0.0)


def _cache_get(key: str) -> Any:
    with _CACHE_LOCK:
        row = _FUTURES_CACHE.get(key)
        if not row:
            return None
        if time.time() - _safe_float(row.get("ts"), 0.0) > _CACHE_TTL:
            _FUTURES_CACHE.pop(key, None)
            return None
        return row.get("data")


def _cache_set(key: str, data: Any) -> Any:
    with _CACHE_LOCK:
        _FUTURES_CACHE[key] = {"ts": time.time(), "data": data}
    return data


def _extract_products(data: Any) -> list[dict[str, Any]]:
    payload = _to_dict(data)
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("products", "data", "product_details"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _extract_positions(data: Any) -> list[dict[str, Any]]:
    payload = _to_dict(data)
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("positions", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        position = payload.get("position")
        if isinstance(position, dict):
            return [position]
    return []


def _parse_expiry(product: dict[str, Any]) -> str | None:
    details = product.get("future_product_details") or {}
    if isinstance(details, dict):
        for key in (
            "contract_expiry",
            "contract_expiry_type",
            "expiry",
            "expiry_time",
            "expiration_time",
            "contract_root_unit",
        ):
            raw = details.get(key)
            if isinstance(raw, dict):
                raw = raw.get("value") or raw.get("time")
            if raw:
                return str(raw)

    product_id = str(product.get("product_id") or "")
    match = re.search(r"(\d{2}[A-Z]{3}\d{2})", product_id)
    if match:
        return match.group(1)
    return None


def _classify_product(product: dict[str, Any]) -> tuple[str, bool]:
    product_id = str(product.get("product_id") or "").upper()
    display_name = str(product.get("display_name") or "").upper()
    details = product.get("future_product_details") or {}
    details = details if isinstance(details, dict) else {}

    is_perp = (
        "PERP" in product_id
        or "PERP" in display_name
        or str(details.get("contract_expiry_type", "")).upper() == "PERPETUAL"
    )
    if any(token in display_name for token in ("GLD", "GOLD", "OIL", "WTI", "SLVR", "SILVER")):
        return "commodity", is_perp
    if any(token in display_name for token in ("MAG7", "NASDAQ", "S&P", "INDEX")):
        return "index", is_perp
    if is_perp:
        return "crypto_perp", True
    return "crypto_dated", False


class FuturesClient:
    def __init__(self):
        self.client = get_client()

    def get_balance_summary(self) -> dict[str, float]:
        cache_key = "balance_summary"
        cached = _cache_get(cache_key)
        if cached is not None:
            return dict(cached)

        try:
            response = self.client.get_futures_balance_summary()
            payload = _to_dict(response) or {}
            summary = payload.get("balance_summary") or payload
            summary = summary if isinstance(summary, dict) else {}

            parsed = {
                "buying_power": _money_value(summary.get("futures_buying_power")),
                "total_balance": _money_value(summary.get("total_usd_balance")),
                "futures_balance": _money_value(summary.get("cfm_usd_balance")),
                "spot_balance": _money_value(summary.get("cbi_usd_balance")),
                "unrealized_pnl": _money_value(summary.get("unrealized_pnl")),
                "daily_realized_pnl": _money_value(summary.get("daily_realized_pnl")),
                "initial_margin": _money_value(summary.get("initial_margin")),
                "available_margin": _money_value(summary.get("available_margin")),
                "liquidation_threshold": _money_value(summary.get("liquidation_threshold")),
            }
            return _cache_set(cache_key, parsed)
        except Exception:
            return {
                "buying_power": 0.0,
                "total_balance": 0.0,
                "futures_balance": 0.0,
                "spot_balance": 0.0,
                "unrealized_pnl": 0.0,
                "daily_realized_pnl": 0.0,
                "initial_margin": 0.0,
                "available_margin": 0.0,
                "liquidation_threshold": 0.0,
            }

    def get_all_products(self) -> list[dict[str, Any]]:
        cache_key = "all_products"
        cached = _cache_get(cache_key)
        if cached is not None:
            return list(cached)

        products: list[dict[str, Any]] = []
        cursor = None
        try:
            while True:
                kwargs = {"product_type": "FUTURE", "limit": 250}
                if cursor:
                    kwargs["cursor"] = cursor
                response = self.client.get_products(**kwargs)
                payload = _to_dict(response) or {}
                chunk = _extract_products(payload)
                if not chunk:
                    break
                products.extend(chunk)
                cursor = payload.get("cursor")
                if not payload.get("has_next") or not cursor:
                    break
        except Exception:
            try:
                payload = _to_dict(self.client.get_products(product_type="FUTURE")) or {}
                products = _extract_products(payload)
            except Exception:
                products = []

        parsed: list[dict[str, Any]] = []
        for raw in products:
            category, is_perp = _classify_product(raw)
            product_id = str(raw.get("product_id") or raw.get("id") or "").strip()
            display_name = str(raw.get("display_name") or product_id).strip()
            underlying = (
                str(raw.get("base_display_symbol") or raw.get("base_currency_id") or "").strip()
                or display_name.split(" ")[0].strip()
            )
            parsed.append(
                {
                    "product_id": product_id,
                    "display_name": display_name,
                    "underlying": underlying,
                    "category": category,
                    "price": _safe_float(raw.get("price") or raw.get("mid_market_price")),
                    "change_24h": _safe_float(raw.get("price_percentage_change_24h")),
                    "volume_24h": _safe_float(raw.get("volume_24h") or raw.get("approximate_quote_24h_volume")),
                    "is_perp": bool(is_perp),
                    "expiry": _parse_expiry(raw),
                }
            )
        parsed.sort(key=lambda item: item.get("display_name") or item.get("product_id") or "")
        return _cache_set(cache_key, parsed)

    def get_positions(self) -> list[dict[str, Any]]:
        cache_key = "positions"
        cached = _cache_get(cache_key)
        if cached is not None:
            return list(cached)

        raw_positions: list[dict[str, Any]] = []
        try:
            if hasattr(self.client, "list_futures_positions"):
                raw_positions = _extract_positions(self.client.list_futures_positions())
        except Exception:
            raw_positions = []

        if not raw_positions:
            for product in self.get_all_products():
                product_id = str(product.get("product_id") or "")
                if not product_id:
                    continue
                try:
                    raw_positions.extend(_extract_positions(self.client.get_futures_position(product_id)))
                except Exception:
                    continue

        product_map = {item["product_id"]: item for item in self.get_all_products()}
        positions: list[dict[str, Any]] = []
        for raw in raw_positions:
            product_id = str(raw.get("product_id") or "").strip()
            size = _safe_float(raw.get("number_of_contracts") or raw.get("size"))
            if size == 0:
                continue
            product = product_map.get(product_id, {})
            side_raw = str(raw.get("side") or "").lower()
            side = "short" if side_raw.startswith("s") else "long"
            entry_price = _safe_float(raw.get("avg_entry_price"))
            mark_price = _safe_float(raw.get("current_price") or product.get("price"))
            unrealized = _safe_float(raw.get("unrealized_pnl"))
            margin_used = abs(size * entry_price) / max(_safe_float(raw.get("leverage"), 1.0), 1.0)
            leverage = _safe_float(raw.get("leverage"), 1.0)
            liquidation_price = _safe_float(raw.get("liquidation_price"))

            positions.append(
                {
                    "product_id": product_id,
                    "display_name": str(product.get("display_name") or product_id),
                    "side": side,
                    "size": size,
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "unrealized_pnl": unrealized,
                    "margin_used": margin_used,
                    "leverage": leverage,
                    "liquidation_price": liquidation_price,
                }
            )
        return _cache_set(cache_key, positions)

    def place_order(
        self,
        product_id: str,
        side: str,
        size: float,
        order_type: str = "market",
        limit_price: float | None = None,
        leverage: int | float = 1,
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        product_id = str(product_id or "").strip().upper()
        order_side = _normalize_side(side)
        order_type = str(order_type or "market").strip().lower()
        base_size = str(size)
        client_order_id = uuid.uuid4().hex

        try:
            if order_type == "limit":
                if limit_price is None or _safe_float(limit_price) <= 0:
                    return {"ok": False, "order_id": "", "error": "limit_price_required"}
                order_configuration = {
                    "limit_limit_gtc": {
                        "base_size": base_size,
                        "limit_price": str(limit_price),
                        "post_only": False,
                    }
                }
            else:
                order_configuration = {"market_market_ioc": {"base_size": base_size}}

            if reduce_only:
                for config in order_configuration.values():
                    if isinstance(config, dict):
                        config["reduce_only"] = True

            response = self.client.create_order(
                client_order_id=client_order_id,
                product_id=product_id,
                side=order_side,
                order_configuration=order_configuration,
                leverage=str(leverage),
            )
            payload = _to_dict(response) or {}
            order_id = (
                ((payload.get("success_response") or {}) if isinstance(payload.get("success_response"), dict) else {}).get("order_id")
                or payload.get("order_id")
                or payload.get("id")
                or ""
            )
            success = bool(order_id or payload.get("success") or payload.get("success_response"))
            error = ""
            if not success:
                error = str(payload.get("error_response") or payload.get("message") or "order_failed")
            with _CACHE_LOCK:
                _FUTURES_CACHE.pop("positions", None)
            return {"ok": success, "order_id": str(order_id), "error": error}
        except Exception as exc:
            return {"ok": False, "order_id": "", "error": str(exc)}

    def close_position(self, product_id: str) -> dict[str, Any]:
        product_id = str(product_id or "").strip().upper()
        position = next((row for row in self.get_positions() if row.get("product_id") == product_id), None)
        if not position:
            return {"ok": False, "order_id": "", "error": "position_not_found"}

        side = "SELL" if str(position.get("side")).lower() == "long" else "BUY"
        size = _safe_float(position.get("size"))
        if size <= 0:
            return {"ok": False, "order_id": "", "error": "invalid_position_size"}
        return self.place_order(
            product_id=product_id,
            side=side,
            size=size,
            order_type="market",
            reduce_only=True,
        )

    def get_candles(self, product_id: str, granularity: str = "FOUR_HOUR", limit: int = 250) -> list[dict[str, Any]]:
        cache_key = f"candles:{product_id}:{granularity}:{limit}"
        cached = _cache_get(cache_key)
        if cached is not None:
            return list(cached)

        try:
            response = self.client.get_candles(product_id=product_id, granularity=granularity)
            payload = _to_dict(response) or {}
            candles = payload.get("candles") if isinstance(payload, dict) else payload
            if not isinstance(candles, list):
                candles = []
            rows = []
            for candle in candles:
                row = _to_dict(candle) or {}
                if not isinstance(row, dict):
                    continue
                rows.append(
                    {
                        "ts": _safe_int(row.get("start")),
                        "open": _safe_float(row.get("open")),
                        "high": _safe_float(row.get("high")),
                        "low": _safe_float(row.get("low")),
                        "close": _safe_float(row.get("close")),
                        "volume": _safe_float(row.get("volume")),
                    }
                )
            rows.sort(key=lambda item: item["ts"])
            if limit and len(rows) > limit:
                rows = rows[-int(limit):]
            return _cache_set(cache_key, rows)
        except Exception:
            return []

    def get_funding_rate(self, product_id: str) -> dict[str, Any]:
        try:
            product = self.client.get_product(product_id)
            payload = _to_dict(product) or {}
            details = payload.get("future_product_details") or {}
            details = details if isinstance(details, dict) else {}
            rate = (
                details.get("funding_rate")
                or details.get("hourly_funding_rate")
                or details.get("next_funding_rate")
                or payload.get("funding_rate")
            )
            next_time = details.get("next_funding_time") or details.get("funding_time")
            return {
                "product_id": str(product_id or "").upper(),
                "funding_rate": _safe_float(rate),
                "next_funding_time": str(next_time or ""),
                "ok": rate is not None,
            }
        except Exception as exc:
            return {
                "product_id": str(product_id or "").upper(),
                "funding_rate": 0.0,
                "next_funding_time": "",
                "ok": False,
                "error": str(exc),
            }

    def get_perps(self) -> list[dict[str, Any]]:
        return [p for p in self.get_all_products() if p["is_perp"]]

    def get_commodities(self) -> list[dict[str, Any]]:
        return [p for p in self.get_all_products() if p["category"] == "commodity"]
