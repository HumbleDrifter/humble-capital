"""
Unusual Whales API client for options flow, dark pool, and market intelligence.
"""
import os
import time
import requests
from typing import Optional

_BASE = "https://api.unusualwhales.com"
_CACHE: dict = {}
_CACHE_TTL = 60  # 1 minute default cache


def _key():
    from env_runtime import load_runtime_env
    load_runtime_env(override=True)
    return str(os.getenv("UNUSUAL_WHALES_API_KEY", "") or "").strip()


def _get(path: str, params: dict = None, ttl: int = _CACHE_TTL) -> dict:
    """Make a cached GET request to UW API."""
    cache_key = f"{path}:{params}"
    if cache_key in _CACHE:
        cached, ts = _CACHE[cache_key]
        if time.time() - ts < ttl:
            return cached
    api_key = _key()
    if not api_key:
        return {"error": "UNUSUAL_WHALES_API_KEY not set", "data": []}
    try:
        resp = requests.get(
            f"{_BASE}{path}",
            headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
            params=params or {},
            timeout=10,
        )
        data = resp.json() if resp.ok else {"error": resp.text, "data": []}
        _CACHE[cache_key] = (data, time.time())
        return data
    except Exception as e:
        return {"error": str(e), "data": []}


# ── Core flow endpoints ────────────────────────────────────────────────────────

def get_flow_alerts(limit: int = 50) -> list:
    """Get recent unusual options flow alerts — sweeps, large prints, whales."""
    data = _get("/api/option-trades/flow-alerts", {"limit": limit}, ttl=30)
    return data.get("data", []) if isinstance(data, dict) else []


def get_stock_flow(ticker: str, limit: int = 20) -> list:
    """Get recent options flow for a specific ticker."""
    data = _get(f"/api/option-trades/flow-alerts", {"ticker": ticker.upper(), "limit": limit}, ttl=30)
    return data.get("data", []) if isinstance(data, dict) else []


def get_darkpool_recent(limit: int = 20) -> list:
    """Get recent dark pool block trades."""
    data = _get("/api/darkpool/recent", {"limit": limit}, ttl=60)
    return data.get("data", []) if isinstance(data, dict) else []


def get_darkpool_ticker(ticker: str) -> list:
    """Get dark pool trades for a specific ticker."""
    data = _get(f"/api/darkpool/{ticker.upper()}", ttl=60)
    return data.get("data", []) if isinstance(data, dict) else []


def get_market_tide() -> dict:
    """Get overall market call/put flow direction."""
    data = _get("/api/market/market-tide", ttl=120)
    return data.get("data", {}) if isinstance(data, dict) else {}


def get_oi_change(limit: int = 20) -> list:
    """Get tickers with largest open interest changes."""
    data = _get("/api/market/oi-change", {"limit": limit}, ttl=300)
    return data.get("data", []) if isinstance(data, dict) else []


def get_expiry_breakdown(ticker: str) -> list:
    """Get options positioning by expiry for a ticker."""
    data = _get(f"/api/stock/{ticker.upper()}/expiry-breakdown", ttl=120)
    return data.get("data", []) if isinstance(data, dict) else []


def get_crypto_whales(limit: int = 20) -> list:
    """Get recent large crypto transactions."""
    data = _get("/api/crypto/whales/recent", {"limit": limit}, ttl=60)
    return data.get("data", []) if isinstance(data, dict) else []


def get_congress_trades(limit: int = 10) -> list:
    """Get recent congressional trades."""
    data = _get("/api/congress/recent-trades", {"limit": limit}, ttl=3600)
    return data.get("data", []) if isinstance(data, dict) else []


def get_earnings_today() -> dict:
    """Get today's earnings releases."""
    pre = _get("/api/earnings/premarket", ttl=3600).get("data", [])
    after = _get("/api/earnings/afterhours", ttl=3600).get("data", [])
    return {"premarket": pre, "afterhours": after}


def get_news_headlines(limit: int = 10) -> list:
    """Get latest market news headlines."""
    data = _get("/api/news/headlines", {"limit": limit}, ttl=300)
    return data.get("data", []) if isinstance(data, dict) else []


def is_configured() -> bool:
    """Check if UW API key is set."""
    return bool(_key())


def get_meme_flow(symbols: list = None) -> list:
    """Get flow alerts filtered to meme/momentum stocks."""
    if symbols is None:
        symbols = ["GME", "AMC", "MARA", "RIOT", "NIO", "BBAI", "SOFI",
                   "PLTR", "HOOD", "COIN", "TSLA", "NVDA", "AMD"]
    all_flow = get_flow_alerts(limit=200)
    return [f for f in all_flow
            if str(f.get("ticker") or f.get("symbol") or "").upper() in symbols]


