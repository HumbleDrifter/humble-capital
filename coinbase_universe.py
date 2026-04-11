import json
import os
import time
from pathlib import Path

from execution import get_client


_UNIVERSE_CACHE_TTL_SEC = 6 * 60 * 60
_UNIVERSE_CACHE = {"ts": 0.0, "value": set()}
_STABLECOIN_EXCLUDES = {
    "USDT-USD",
    "USDC-USD",
    "DAI-USD",
    "PYUSD-USD",
    "GUSD-USD",
    "PAX-USD",
    "BUSD-USD",
    "USDP-USD",
    "PAXG-USD",
}
_BASE_DIR = Path(os.getenv("BASE_PATH", Path(__file__).resolve().parent)).resolve()
_CONFIG_PATH = _BASE_DIR / "asset_config.json"


def _log(message):
    print(f"[coinbase_universe] {message}")


def _to_dict(x):
    return x.to_dict() if hasattr(x, "to_dict") else x


def _normalize_product_id(product_id):
    return str(product_id or "").upper().strip()


def _safe_float(value, default=0.0):
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _load_static_satellite_allowed():
    try:
        with open(_CONFIG_PATH, "r", encoding="utf-8") as handle:
            config = json.load(handle) or {}
        return {
            _normalize_product_id(product_id)
            for product_id in (config.get("satellite_allowed") or [])
            if _normalize_product_id(product_id)
        }
    except Exception as exc:
        _log(f"failed to load static satellite fallback error={exc}")
        return set()


def _extract_products(data):
    if data is None:
        return []
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ("products", "product_details", "data"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _is_tradeable_product(product):
    status = str(product.get("status") or product.get("trading_status") or "").upper().strip()
    if product.get("trading_disabled") is True:
        return False
    if product.get("is_disabled") is True:
        return False
    if product.get("cancel_only") is True:
        return False
    if product.get("auction_mode") is True:
        return False
    if product.get("view_only") is True:
        return False
    if status in {"OFFLINE", "DISABLED", "DELISTED"}:
        return False
    return True


def _fetch_all_products():
    client = get_client()
    if not hasattr(client, "get_products") and not hasattr(client, "list_products"):
        raise RuntimeError("Coinbase client does not expose product listing methods")

    cursor = None
    products = []

    while True:
        if hasattr(client, "get_products"):
            response = client.get_products(limit=250, cursor=cursor) if cursor else client.get_products(limit=250)
        else:
            response = client.list_products(limit=250, cursor=cursor) if cursor else client.list_products(limit=250)

        data = _to_dict(response)
        chunk = _extract_products(data)
        products.extend(chunk)

        has_next = bool((data or {}).get("has_next"))
        cursor = (data or {}).get("cursor")
        if not has_next or not cursor:
            break

    return products


def get_all_usd_products() -> set:
    cache_age = time.time() - _safe_float(_UNIVERSE_CACHE.get("ts"), 0.0)
    cached_value = _UNIVERSE_CACHE.get("value")
    if cache_age < _UNIVERSE_CACHE_TTL_SEC and isinstance(cached_value, set) and cached_value:
        return set(cached_value)

    try:
        products = _fetch_all_products()
        usd_products = set()

        for product in products:
            product_id = _normalize_product_id(product.get("product_id") or product.get("id"))
            if not product_id.endswith("-USD"):
                continue
            if product_id in _STABLECOIN_EXCLUDES:
                continue
            if not _is_tradeable_product(product):
                continue
            usd_products.add(product_id)

        if usd_products:
            _UNIVERSE_CACHE["ts"] = time.time()
            _UNIVERSE_CACHE["value"] = set(usd_products)
            _log(f"loaded dynamic Coinbase USD universe count={len(usd_products)}")
            return usd_products

        raise RuntimeError("Coinbase product list returned no tradeable USD products")
    except Exception as exc:
        fallback = _load_static_satellite_allowed()
        _UNIVERSE_CACHE["ts"] = time.time()
        _UNIVERSE_CACHE["value"] = set(fallback)
        _log(f"failed to refresh Coinbase universe, using static fallback count={len(fallback)} error={exc}")
        return fallback


def is_in_coinbase_universe(product_id: str) -> bool:
    try:
        product_id = _normalize_product_id(product_id)
        if not product_id:
            return False
        return product_id in get_all_usd_products()
    except Exception as exc:
        _log(f"is_in_coinbase_universe failed product_id={product_id} error={exc}")
        return False


def get_satellite_universe(snapshot: dict) -> set:
    try:
        snapshot = snapshot if isinstance(snapshot, dict) else {}
        config = snapshot.get("config") or {}
        core_assets = {
            _normalize_product_id(product_id)
            for product_id in (config.get("core_assets") or {}).keys()
            if _normalize_product_id(product_id)
        }
        blocked_assets = {
            _normalize_product_id(product_id)
            for product_id in (config.get("satellite_blocked") or [])
            if _normalize_product_id(product_id)
        }
        universe = get_all_usd_products()
        return {product_id for product_id in universe if product_id not in core_assets and product_id not in blocked_assets}
    except Exception as exc:
        _log(f"get_satellite_universe failed error={exc}")
        fallback = _load_static_satellite_allowed()
        snapshot = snapshot if isinstance(snapshot, dict) else {}
        config = snapshot.get("config") or {}
        blocked_assets = {
            _normalize_product_id(product_id)
            for product_id in (config.get("satellite_blocked") or [])
            if _normalize_product_id(product_id)
        }
        core_assets = {
            _normalize_product_id(product_id)
            for product_id in (config.get("core_assets") or {}).keys()
            if _normalize_product_id(product_id)
        }
        return {product_id for product_id in fallback if product_id not in blocked_assets and product_id not in core_assets}
