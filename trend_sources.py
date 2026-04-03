import os
import requests
from typing import Dict, List, Any

COINGECKO_BASE = "https://api.coingecko.com/api/v3"


def safe_get_json(url: str, params: dict | None = None, timeout: int = 20) -> Any:
    r = requests.get(
        url,
        params=params,
        timeout=timeout,
        headers={
            "Accept": "application/json",
            "User-Agent": "tradingbot-meme-scanner/2.0"
        }
    )
    r.raise_for_status()
    return r.json()


def fetch_coingecko_trending() -> List[Dict[str, Any]]:
    try:
        data = safe_get_json(f"{COINGECKO_BASE}/search/trending")
        coins = data.get("coins", [])
        out = []
        for idx, item in enumerate(coins):
            coin = item.get("item", {})
            symbol = (coin.get("symbol") or "").upper()
            name = coin.get("name") or symbol
            base_score = max(0, 100 - idx * 8)
            if symbol:
                out.append({
                    "symbol": symbol,
                    "name": name,
                    "source": "coingecko",
                    "score": float(base_score),
                })
        return out
    except Exception:
        return []


def fetch_coingecko_markets() -> Dict[str, Dict[str, Any]]:
    """
    Returns market data indexed by symbol.
    This is coarse because symbols can collide, but works well enough for meme v1.
    """
    try:
        data = safe_get_json(
            f"{COINGECKO_BASE}/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "volume_desc",
                "per_page": 250,
                "page": 1,
                "sparkline": "false",
                "price_change_percentage": "1h,24h,7d"
            }
        )
        out: Dict[str, Dict[str, Any]] = {}
        for row in data:
            symbol = (row.get("symbol") or "").upper()
            if not symbol:
                continue
            out[symbol] = {
                "name": row.get("name") or symbol,
                "current_price": row.get("current_price", 0.0),
                "market_cap": row.get("market_cap", 0.0),
                "market_cap_rank": row.get("market_cap_rank"),
                "total_volume": row.get("total_volume", 0.0),
                "price_change_percentage_1h_in_currency": row.get("price_change_percentage_1h_in_currency", 0.0),
                "price_change_percentage_24h_in_currency": row.get("price_change_percentage_24h_in_currency", 0.0),
                "price_change_percentage_7d_in_currency": row.get("price_change_percentage_7d_in_currency", 0.0),
            }
        return out
    except Exception:
        return {}


def fetch_reddit_signal() -> List[Dict[str, Any]]:
    """
    Still lightweight, but now customizable.
    """
    watchlist = os.getenv(
        "MEME_SYMBOL_WATCHLIST",
        "DOGE,SHIB,PEPE,WIF,BONK,FLOKI,BRETT,MOG,POPCAT,NEIRO,PENGU"
    )
    symbols = [x.strip().upper() for x in watchlist.split(",") if x.strip()]
    out = []
    for i, sym in enumerate(symbols):
        out.append({
            "symbol": sym,
            "name": sym,
            "source": "reddit_stub",
            "score": float(max(5, 30 - i)),
        })
    return out


def fetch_google_trends_signal() -> List[Dict[str, Any]]:
    watchlist = os.getenv(
        "MEME_SYMBOL_WATCHLIST",
        "DOGE,SHIB,PEPE,WIF,BONK,FLOKI,BRETT,MOG,POPCAT,NEIRO,PENGU"
    )
    symbols = [x.strip().upper() for x in watchlist.split(",") if x.strip()]
    out = []
    for i, sym in enumerate(symbols):
        out.append({
            "symbol": sym,
            "name": sym,
            "source": "google_trends_stub",
            "score": float(max(3, 20 - i)),
        })
    return out


def fetch_all_trend_inputs() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    rows.extend(fetch_coingecko_trending())
    rows.extend(fetch_reddit_signal())
    rows.extend(fetch_google_trends_signal())
    return rows
