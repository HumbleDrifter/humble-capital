import math
import os
import time
from threading import RLock


def _load_universe_thresholds():
    try:
        import json
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "asset_config.json")
        with open(config_path) as f:
            cfg = json.load(f).get("stock_universe", {})
        return (
            float(cfg.get("min_market_cap", os.getenv("STOCK_MIN_MARKET_CAP", "500000000"))),
            int(float(cfg.get("min_avg_volume", os.getenv("STOCK_MIN_AVG_VOLUME", "1000000")))),
            float(cfg.get("min_price", os.getenv("STOCK_MIN_PRICE", "5.0"))),
            float(cfg.get("max_price", os.getenv("STOCK_MAX_PRICE", "500.0"))),
        )
    except Exception:
        return (500_000_000.0, 1_000_000, 5.0, 500.0)


MIN_MARKET_CAP_USD, MIN_AVG_VOLUME, MIN_PRICE, MAX_PRICE = _load_universe_thresholds()


_DEFAULT_SEED_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD", "SPY", "QQQ",
    "IWM", "NFLX", "DIS", "BA", "JPM", "GS", "BAC", "XOM", "CVX", "PFE",
    "JNJ", "V", "MA", "COST", "WMT", "HD", "LOW", "CRM", "ORCL", "ADBE",
    "INTC", "MU", "QCOM", "COIN", "MARA", "RIOT", "SQ", "PYPL", "UBER", "ABNB",
    "PLTR", "SOFI", "NIO", "RIVN", "F", "GM", "T", "VZ", "KO", "PEP",
]


class StockUniverse:
    """
    Dynamically discovers and maintains the tradeable stock universe.
    """

    CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conf", "stock_universe_cache.json")

    def __init__(self):
        self.cache = {"ts": 0, "tiers": {}}
        self.cache_ttl = 86400
        self._lock = RLock()
        self._load_disk_cache()

    def _load_disk_cache(self):
        try:
            import json
            if os.path.exists(self.CACHE_FILE):
                with open(self.CACHE_FILE) as f:
                    data = json.load(f)
                if data.get("tiers") and (time.time() - float(data.get("ts", 0))) < self.cache_ttl:
                    self.cache = data
                    self._log(f"loaded universe from disk tier1={len(data['tiers'].get('tier1', []))}")
        except Exception as exc:
            self._log(f"disk cache load failed: {exc}")

    def _save_disk_cache(self):
        try:
            import json
            os.makedirs(os.path.dirname(self.CACHE_FILE), exist_ok=True)
            with open(self.CACHE_FILE, "w") as f:
                json.dump(self.cache, f)
        except Exception as exc:
            self._log(f"disk cache save failed: {exc}")

    def _log(self, message):
        print(f"[stock_universe] {message}")

    def _normalize_symbol(self, symbol):
        return str(symbol or "").strip().upper().replace(".", "-")

    def _safe_float(self, value, default=0.0):
        try:
            return float(value)
        except Exception:
            return float(default)

    def _fetch_sp500(self):
        import pandas as pd

        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url, storage_options={"User-Agent": "Mozilla/5.0"})
        return [self._normalize_symbol(v) for v in tables[0]["Symbol"].tolist()]

    def _fetch_nasdaq100(self):
        import pandas as pd

        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        tables = pd.read_html(url, storage_options={"User-Agent": "Mozilla/5.0"})
        for table in tables:
            for col in ["Ticker", "Symbol"]:
                if col in table.columns:
                    return [self._normalize_symbol(v) for v in table[col].tolist()]
        return []

    def _fetch_russell2000(self):
        import pandas as pd

        urls = [
            "https://en.wikipedia.org/wiki/Russell_2000_Index",
            "https://en.wikipedia.org/wiki/List_of_Russell_2000_companies",
        ]
        for url in urls:
            try:
                tables = pd.read_html(url, storage_options={"User-Agent": "Mozilla/5.0"})
                for table in tables:
                    for col in ["Ticker", "Symbol"]:
                        if col in table.columns:
                            rows = [self._normalize_symbol(v) for v in table[col].tolist()]
                            if rows:
                                return rows
            except Exception:
                continue
        return []

    def _check_liquidity(self, symbol):
        import yfinance as yf

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info or {}
            avg_vol = int(info.get("averageVolume", 0) or 0)
            market_cap = float(info.get("marketCap", 0) or 0)
            price = float(info.get("currentPrice", 0) or info.get("regularMarketPrice", 0) or 0)
            try:
                has_options = len(ticker.options or []) > 0
            except Exception:
                has_options = False
            tradeable = avg_vol >= MIN_AVG_VOLUME and market_cap >= MIN_MARKET_CAP_USD and MIN_PRICE <= price <= MAX_PRICE and has_options
            return {
                "tradeable": tradeable,
                "avg_volume": avg_vol,
                "market_cap": market_cap,
                "price": price,
                "has_options": has_options,
            }
        except Exception:
            return {
                "tradeable": False,
                "avg_volume": 0,
                "market_cap": 0,
                "price": 0,
                "has_options": False,
            }

    def _liquidity_score(self, row):
        avg_volume = max(1.0, self._safe_float((row or {}).get("avg_volume"), 0.0))
        market_cap = max(1.0, self._safe_float((row or {}).get("market_cap"), 0.0))
        return (math.log10(avg_volume) * 10.0) + (math.log10(market_cap) * 5.0)

    def discover_universe(self):
        with self._lock:
            if self.cache.get("tiers") and (time.time() - float(self.cache.get("ts", 0) or 0)) < self.cache_ttl:
                return self.cache["tiers"]

            try:
                tickers = set(_DEFAULT_SEED_UNIVERSE)
                for loader in (self._fetch_sp500, self._fetch_nasdaq100, self._fetch_russell2000):
                    try:
                        tickers.update([s for s in loader() if s])
                    except Exception as exc:
                        self._log(f"source fetch failed loader={loader.__name__} error={exc}")

                ranked = []
                for symbol in sorted(tickers):
                    liq = self._check_liquidity(symbol)
                    if not liq.get("tradeable"):
                        continue
                    ranked.append({"symbol": symbol, **liq, "liquidity_score": self._liquidity_score(liq)})
                    time.sleep(0.05)

                ranked.sort(key=lambda row: row.get("liquidity_score", 0.0), reverse=True)
                tier1 = [row["symbol"] for row in ranked[:100]]
                tier2 = [row["symbol"] for row in ranked[100:300]]
                tier3 = [row["symbol"] for row in ranked[300:800]]

                tiers = {
                    "tier1": tier1,
                    "tier2": tier2,
                    "tier3": tier3,
                    "total": len(ranked),
                    "updated": int(time.time()),
                }
                self.cache = {"ts": time.time(), "tiers": tiers}
                self._save_disk_cache()
                self._log(f"discovered universe total={len(ranked)} tier1={len(tier1)} tier2={len(tier2)} tier3={len(tier3)}")
                return tiers
            except Exception as exc:
                self._log(f"discover_universe failed error={exc}")
                fallback = {
                    "tier1": _DEFAULT_SEED_UNIVERSE[:25],
                    "tier2": _DEFAULT_SEED_UNIVERSE[25:],
                    "tier3": [],
                    "total": len(_DEFAULT_SEED_UNIVERSE),
                    "updated": int(time.time()),
                }
                self.cache = {"ts": time.time(), "tiers": fallback}
                return fallback

    def promote_ticker(self, symbol, from_tier, to_tier, reason):
        with self._lock:
            tiers = self.discover_universe()
            symbol = self._normalize_symbol(symbol)
            from_key = str(from_tier or "").lower().strip()
            to_key = str(to_tier or "").lower().strip()
            if from_key in tiers and symbol in tiers[from_key]:
                tiers[from_key] = [s for s in tiers[from_key] if s != symbol]
            if to_key in tiers and symbol not in tiers[to_key]:
                tiers[to_key].insert(0, symbol)
            self.cache = {"ts": time.time(), "tiers": tiers}
            self._log(f"promoted symbol={symbol} from={from_tier} to={to_tier} reason={reason}")

    def get_tier1(self):
        return list((self.discover_universe() or {}).get("tier1", []))

    def get_tier2(self):
        return list((self.discover_universe() or {}).get("tier2", []))

    def get_all_tradeable(self):
        tiers = self.discover_universe() or {}
        return list(dict.fromkeys((tiers.get("tier1", []) or []) + (tiers.get("tier2", []) or []) + (tiers.get("tier3", []) or [])))

    def get_universe_stats(self):
        tiers = self.discover_universe() or {}
        return {
            "tier1_count": len(tiers.get("tier1", []) or []),
            "tier2_count": len(tiers.get("tier2", []) or []),
            "tier3_count": len(tiers.get("tier3", []) or []),
            "total": int(tiers.get("total", 0) or 0),
            "updated": int(tiers.get("updated", 0) or 0),
            "tier1": list(tiers.get("tier1", []) or []),
            "tier2": list(tiers.get("tier2", []) or []),
            "tier3": list(tiers.get("tier3", []) or []),
        }
