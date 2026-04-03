import json
import os
import time

import requests
from dotenv import load_dotenv
from trend_sources import fetch_coingecko_markets

load_dotenv("/root/tradingbot/.env", override=True)

OUTPUT_FILE = "/root/tradingbot/meme_rotation.json"
LOG_FILE = "/root/tradingbot/tradingbot.log"

MAX_CANDIDATES = int(os.getenv("MAX_MEME_CANDIDATES", "20"))
MIN_MARKET_CAP = float(os.getenv("MEME_MIN_MARKET_CAP_USD", "250000"))
MIN_VOLUME = float(os.getenv("MEME_MIN_VOLUME_24H_USD", "50000"))

INTERNAL_API_BASE = os.getenv("INTERNAL_API_BASE", "http://127.0.0.1:8000").rstrip("/")


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [meme_scanner] {msg}\n"
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    print(line, end="")


def get_api_secret():
    load_dotenv("/root/tradingbot/.env", override=True)
    return (
        os.getenv("INTERNAL_API_SECRET")
        or os.getenv("STATUS_SECRET")
        or os.getenv("WEBHOOK_SHARED_SECRET")
        or ""
    )

def _watchlist():
    raw = str(os.getenv("MEME_SYMBOL_WATCHLIST", "") or "").strip()

    if not raw:
        return set()

    normalized = raw.upper().strip()

    # Treat these as full-universe mode
    if normalized in {"NONE", "*", "ALL"}:
        return set()

    return {x.strip().upper() for x in raw.split(",") if x.strip()}

def _load_core_assets():
    path = "/root/tradingbot/asset_config.json"

    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return set()

    if not isinstance(cfg, dict):
        return set()

    core_assets = cfg.get("core_assets", []) or []

    return {
        str(x).upper().strip()
        for x in core_assets
        if str(x or "").strip()
    }

def _is_excluded_candidate(pid, core_assets):
    pid = str(pid or "").upper().strip()

    if not pid.endswith("-USD"):
        return True

    if pid in core_assets:
        return True

    if pid in _manual_excluded_products():
        return True

    return False

def compute_meme_boost(pid):
    pid = str(pid or "").upper().strip()

    if pid in _meme_boost_products():
        return 6.0

    return 0.0

def _manual_excluded_products():
    # Hard exclusions for assets you do not want in meme/satellite scanning
    return {
        "USDC-USD",
        "USDT-USD",
        "DAI-USD",
        "PYUSD-USD",
        "EURC-USD",
        "PAXG-USD",
        "XAUT-USD",
        "WBTC-USD",
        "BNB-USD",
        "ADA-USD",
        "LINK-USD",
        "LTC-USD",
        "UNI-USD",
    }

def _meme_boost_products():
    return {
        "DOGE-USD",
        "SHIB-USD",
        "PEPE-USD",
        "BONK-USD",
        "FLOKI-USD",
        "WIF-USD",
        "BRETT-USD",
        "POPCAT-USD",
        "MOG-USD",
        "PENGU-USD",
        "PNUT-USD",
        "TURBO-USD",
        "MYRO-USD",
        "NEIRO-USD",
        "ACT-USD",
    }

def get_valid_products():
    try:
        params = {
            "quote": "USD",
            "tradable_only": "true",
        }

        secret = get_api_secret()
        if secret:
            params["secret"] = secret

        r = requests.get(
            f"{INTERNAL_API_BASE}/api/valid_products",
            params=params,
            timeout=10,
        )
        if r.status_code != 200:
            log(f"valid product lookup http {r.status_code}")
            return set()

        data = r.json()

        out = set()
        if isinstance(data, list):
            items = data
        else:
            items = data.get("products", [])

        for x in items:
            if isinstance(x, str):
                out.add(str(x).strip().upper())
            elif isinstance(x, dict):
                pid = str(x.get("product_id", "")).strip().upper()
                if pid:
                    out.add(pid)

        return out
    except Exception as e:
        log(f"valid product lookup failed: {e}")
        return set()

def compute_momentum_bonus(m):
    try:
        p1h = float(m.get("price_change_percentage_1h_in_currency") or 0)
        p24h = float(m.get("price_change_percentage_24h_in_currency") or 0)
    except Exception:
        return 0.0, "neutral"

    bonus = 0.0
    tag = "neutral"

    if p1h > 8:
        bonus += 12
        tag = "surging"
    elif p1h > 4:
        bonus += 6
        tag = "strong"

    if p24h > 25:
        bonus += 8

    return bonus, tag


def compute_liquidity_bonus(m):
    try:
        mc = float(m.get("market_cap") or 0)
        vol = float(m.get("total_volume") or 0)
    except Exception:
        return 0.0

    bonus = 0.0

    if mc > 100_000_000:
        bonus += 4
    elif mc > 25_000_000:
        bonus += 2

    if vol > 25_000_000:
        bonus += 4
    elif vol > 5_000_000:
        bonus += 2

    return bonus


def compute_volume_surge_bonus(m):
    try:
        mc = float(m.get("market_cap") or 0)
        vol = float(m.get("total_volume") or 0)
    except Exception:
        return 0.0, "normal"

    if mc <= 0 or vol <= 0:
        return 0.0, "normal"

    turnover = vol / mc
    bonus = 0.0
    tag = "normal"

    if turnover > 1:
        bonus += 10
        tag = "explosive"
    elif turnover > 0.5:
        bonus += 6
        tag = "hot"
    elif turnover > 0.2:
        bonus += 3
        tag = "active"

    return bonus, tag


def compute_score(m):
    try:
        mc = float(m.get("market_cap") or 0)
        vol = float(m.get("total_volume") or 0)
    except Exception:
        return {
            "total": 0.0,
            "base_bonus": 0.0,
            "momentum_bonus": 0.0,
            "liquidity_bonus": 0.0,
            "volume_surge_bonus": 0.0,
            "momentum_tag": "neutral",
            "volume_tag": "normal",
        }

    if mc < MIN_MARKET_CAP or vol < MIN_VOLUME:
        return {
            "total": 0.0,
            "base_bonus": 0.0,
            "momentum_bonus": 0.0,
            "liquidity_bonus": 0.0,
            "volume_surge_bonus": 0.0,
            "momentum_tag": "neutral",
            "volume_tag": "normal",
        }

    momentum_bonus, momentum_tag = compute_momentum_bonus(m)
    liquidity_bonus = compute_liquidity_bonus(m)
    volume_surge_bonus, volume_tag = compute_volume_surge_bonus(m)

    # Small baseline so assets that pass hard filters do not collapse to zero
    base_bonus = 1.0

    total = min(100, base_bonus + momentum_bonus + liquidity_bonus + volume_surge_bonus)

    return {
        "total": total,
        "base_bonus": base_bonus,
        "momentum_bonus": momentum_bonus,
        "liquidity_bonus": liquidity_bonus,
        "volume_surge_bonus": volume_surge_bonus,
        "momentum_tag": momentum_tag,
        "volume_tag": volume_tag,
    }

def run():
    log("starting meme scan")

    markets = fetch_coingecko_markets()
    valid = get_valid_products()
    watchlist = _watchlist()
    core_assets = _load_core_assets()

    log(f"valid tradable USD products: {len(valid)}")
    log(f"configured core assets excluded: {len(core_assets)}")

    candidates = []

    for symbol, m in markets.items():
        pid = f"{str(symbol).upper()}-USD"

        if valid and pid not in valid:
            continue

        if _is_excluded_candidate(pid, core_assets):
            continue

        if watchlist and pid not in watchlist:
            continue

        score_info = compute_score(m)
        meme_boost = compute_meme_boost(pid)
        score = float(score_info.get("total", 0) or 0) + meme_boost

        if score <= 0:
            score = 1.0 + meme_boost
            score_info = {
                "base_bonus": 1.0,
                "momentum_bonus": 0.0,
                "liquidity_bonus": 0.0,
                "volume_surge_bonus": 0.0,
                "meme_boost_bonus": meme_boost,
                "momentum_tag": "neutral",
                "volume_tag": "normal",
            }
        else:
            score_info["meme_boost_bonus"] = meme_boost

        try:
            p24h = float(m.get("price_change_percentage_24h_in_currency") or 0)
        except Exception:
            p24h = 0.0

        candidates.append({
            "product_id": pid,
            "symbol": str(symbol).upper(),
            "score": score,
            "momentum_tag": score_info.get("momentum_tag"),
            "volume_tag": score_info.get("volume_tag"),
            "score_breakdown": {
                "base_bonus": score_info.get("base_bonus", 0),
                "momentum_bonus": score_info.get("momentum_bonus", 0),
                "liquidity_bonus": score_info.get("liquidity_bonus", 0),
                "volume_surge_bonus": score_info.get("volume_surge_bonus", 0),
		"meme_boost_bonus": score_info.get("meme_boost_bonus", 0),
            },
            "price_change_24h": p24h,
            "market_cap": m.get("market_cap"),
            "total_volume": m.get("total_volume"),
            "pump_protected": p24h > float(os.getenv("PUMP_PROTECTION_24H", "70")),
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    candidates = candidates[:MAX_CANDIDATES]
    if not candidates:
        log("scanner produced 0 candidates; writing empty shortlist fallback")
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "generated_at": int(time.time()),
                "candidate_count": 0,
                "watchlist_mode": bool(watchlist),
                "excluded_core_assets": sorted(core_assets),
		"manual_excluded_products": sorted(_manual_excluded_products()),
		"meme_bias_mode": "boosted",
                "candidates": []
            }, f, indent=2)
        return

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": int(time.time()),
            "candidate_count": len(candidates),
            "watchlist_mode": bool(watchlist),
            "excluded_core_assets": sorted(core_assets),
	    "manual_excluded_products": sorted(_manual_excluded_products()),
	    "meme_bias_mode": "boosted",
            "candidates": candidates
        }, f, indent=2)

    log(f"scanner finished — {len(candidates)} candidates")

if __name__ == "__main__":
    while True:
        try:
            run()
        except Exception as e:
            import traceback
            log(f"error: {e}\n{traceback.format_exc()}")
        time.sleep(300)
