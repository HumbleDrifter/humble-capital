import json
import os
import time
import uuid
import threading
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, Tuple

import requests
from dotenv import load_dotenv
from coinbase.rest import RESTClient

from notify import notify_execution_result, notify_execution_error

load_dotenv('/root/tradingbot/.env', override=True)

TERMINAL_STATUSES = {'FILLED', 'CANCELED', 'CANCELLED', 'EXPIRED', 'FAILED', 'REJECTED'}
PRODUCT_CACHE_TTL_SEC = int(os.getenv('PRODUCT_CACHE_TTL_SEC', '300'))
PRODUCT_CACHE_STALE_SEC = int(os.getenv('PRODUCT_CACHE_STALE_SEC', '3600'))

COINBASE_MIN_REQUEST_INTERVAL_SEC = float(os.getenv('COINBASE_MIN_REQUEST_INTERVAL_SEC', '0.20'))
COINBASE_429_MAX_RETRIES = int(os.getenv('COINBASE_429_MAX_RETRIES', '4'))
COINBASE_429_BACKOFF_BASE_SEC = float(os.getenv('COINBASE_429_BACKOFF_BASE_SEC', '1.0'))

_client = None
_client_fingerprint = None
_CLIENT_LOCK = threading.Lock()

_COINBASE_RATE_LOCK = threading.Lock()
_LAST_COINBASE_REQUEST_TS = 0.0

_PRODUCT_CACHE_LOCK = threading.Lock()
_PRODUCT_CACHE = {
    'ts': 0.0,
    'products': [],
    'error': None,
}
_EXECUTION_CONFIG_CACHE = {
    'ts': 0.0,
    'value': {},
}
_EXECUTION_CONFIG_TTL_SEC = 30.0
_ASSET_CONFIG_PATH = Path(__file__).resolve().parent / 'asset_config.json'


def _current_coinbase_creds():
    load_dotenv('/root/tradingbot/.env', override=True)

    api_key = os.getenv('COINBASE_API_KEY')
    api_secret = os.getenv('COINBASE_API_SECRET')

    if api_secret:
        api_secret = api_secret.replace('\\n', '\n')

    return api_key, api_secret


def _current_client_fingerprint():
    api_key, api_secret = _current_coinbase_creds()
    return (api_key or '', api_secret or '')


def reset_client():
    global _client, _client_fingerprint
    with _CLIENT_LOCK:
        _client = None
        _client_fingerprint = None


def get_client() -> RESTClient:
    global _client, _client_fingerprint

    fingerprint = _current_client_fingerprint()

    with _CLIENT_LOCK:
        if _client is not None and _client_fingerprint == fingerprint:
            return _client

        api_key, api_secret = _current_coinbase_creds()

        if not api_key or not api_secret:
            raise RuntimeError(
                'Missing COINBASE_API_KEY or COINBASE_API_SECRET. '
                'Check /root/tradingbot/.env formatting.'
            )

        _client = RESTClient(api_key=api_key, api_secret=api_secret)
        _client_fingerprint = fingerprint
        return _client

def _coinbase_call(fn, *args, **kwargs):
    global _LAST_COINBASE_REQUEST_TS

    attempt = 0
    last_exc = None

    while attempt <= COINBASE_429_MAX_RETRIES:
        attempt += 1

        with _COINBASE_RATE_LOCK:
            now = time.time()
            wait_for = COINBASE_MIN_REQUEST_INTERVAL_SEC - (now - _LAST_COINBASE_REQUEST_TS)
            if wait_for > 0:
                time.sleep(wait_for)
            _LAST_COINBASE_REQUEST_TS = time.time()

        try:
            return fn(*args, **kwargs)

        except requests.HTTPError as exc:
            last_exc = exc
            status_code = getattr(exc.response, "status_code", None)

            if status_code == 429 and attempt <= COINBASE_429_MAX_RETRIES:
                backoff = COINBASE_429_BACKOFF_BASE_SEC * (2 ** (attempt - 1))
                print(f"[coinbase] 429 rate limit on attempt {attempt}; sleeping {backoff:.2f}s")
                time.sleep(backoff)
                continue

            raise

        except Exception as exc:
            last_exc = exc
            message = str(exc)

            if "429" in message and attempt <= COINBASE_429_MAX_RETRIES:
                backoff = COINBASE_429_BACKOFF_BASE_SEC * (2 ** (attempt - 1))
                print(f"[coinbase] inferred 429 on attempt {attempt}; sleeping {backoff:.2f}s")
                time.sleep(backoff)
                continue

            raise

    if last_exc:
        raise last_exc

    raise RuntimeError("_coinbase_call failed without returning or raising an underlying exception")

def _to_dict(x: Any) -> Dict[str, Any]:
    return x.to_dict() if hasattr(x, 'to_dict') else x


def _load_execution_config() -> Dict[str, Any]:
    now = time.time()
    if (now - float(_EXECUTION_CONFIG_CACHE.get('ts', 0.0) or 0.0)) < _EXECUTION_CONFIG_TTL_SEC:
        return dict(_EXECUTION_CONFIG_CACHE.get('value') or {})

    try:
        with _ASSET_CONFIG_PATH.open('r', encoding='utf-8') as handle:
            data = json.load(handle) or {}
            if not isinstance(data, dict):
                data = {}
    except Exception as exc:
        print(f"[execution] failed to load asset_config.json: {exc}")
        data = {}

    _EXECUTION_CONFIG_CACHE['ts'] = now
    _EXECUTION_CONFIG_CACHE['value'] = dict(data)
    return dict(data)


def _use_limit_orders() -> bool:
    cfg = _load_execution_config()
    value = cfg.get('use_limit_orders', True)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {'0', 'false', 'no', 'off'}


def _extract_order_id(resp_dict: Dict[str, Any]) -> str | None:
    return (
        resp_dict.get('success_response', {}).get('order_id')
        or resp_dict.get('order_id')
        or resp_dict.get('order', {}).get('order_id')
    )


def _safe_notify_execution_result(
    product_id: str,
    side: str,
    signal_type: str,
    requested_usd: float | None,
    requested_base: float | None,
    result_wrapper: Dict[str, Any],
) -> None:
    try:
        notify_execution_result(
            product_id=product_id,
            side=side,
            signal_type=signal_type,
            requested_usd=requested_usd,
            requested_base=requested_base,
            result_wrapper=result_wrapper,
        )
    except Exception as exc:
        print(f"[notify] execution result notification failed: {exc}")


def _safe_notify_execution_error(
    product_id: str,
    side: str,
    signal_type: str,
    reason: str,
) -> None:
    try:
        notify_execution_error(
            product_id=product_id,
            action=side,
            signal_type=signal_type,
            reason=reason,
        )
    except Exception as exc:
        print(f"[notify] execution error notification failed: {exc}")


def get_best_bid_ask(product_id: str) -> Tuple[Decimal, Decimal]:
    resp = _coinbase_call(get_client().get_best_bid_ask, product_ids=[product_id])
    d = _to_dict(resp)
    pricebooks = d.get('pricebooks') or []
    if not pricebooks:
        raise RuntimeError(f'No pricebook returned for {product_id}')
    pb = pricebooks[0]
    bid = Decimal(str(pb['bids'][0]['price']))
    ask = Decimal(str(pb['asks'][0]['price']))
    return bid, ask


def get_quote_increment(product_id: str) -> Decimal:
    prod = _to_dict(_coinbase_call(get_client().get_product, product_id=product_id))
    inc = prod.get('quote_increment') or prod.get('quote_increment_value') or '0.00000001'
    return Decimal(str(inc))


def get_base_increment(product_id: str) -> Decimal:
    prod = _to_dict(_coinbase_call(get_client().get_product, product_id=product_id))
    inc = prod.get('base_increment') or prod.get('base_increment_value') or '0.00000001'
    return Decimal(str(inc))


def round_to_increment(value: Decimal, inc: Decimal) -> Decimal:
    return (value / inc).to_integral_value(rounding=ROUND_DOWN) * inc


def _calc_limit_price(product_id: str, side: str, offset_bps: int) -> Tuple[Decimal, Decimal, Decimal]:
    side = side.upper()
    bid, ask = get_best_bid_ask(product_id)
    inc = get_quote_increment(product_id)

    offset = Decimal(offset_bps) / Decimal(10_000)

    if side == 'BUY':
        raw_price = ask * (Decimal('1') - offset)
    else:
        raw_price = bid * (Decimal('1') + offset)

    limit_price = round_to_increment(raw_price, inc)
    return bid, ask, limit_price


def _calc_aggressive_limit_price(product_id: str, side: str) -> Tuple[Decimal, Decimal, Decimal]:
    side = side.upper()
    bid, ask = get_best_bid_ask(product_id)
    inc = get_quote_increment(product_id)
    product = _to_dict(_coinbase_call(get_client().get_product, product_id=product_id))
    current_price = Decimal(str(product.get('price') or ask if side == 'BUY' else bid))

    if side == 'BUY':
        raw_price = current_price * Decimal('1.001')
    else:
        raw_price = current_price * Decimal('0.999')

    limit_price = round_to_increment(raw_price, inc)
    if limit_price <= 0:
        limit_price = ask if side == 'BUY' else bid
    return bid, ask, limit_price


def get_quote_attempts(signal_type: str = "", volatility_bucket: str = ""):
    signal_type = str(signal_type or "").upper().strip()
    volatility_bucket = str(volatility_bucket or "").lower().strip()

    if signal_type == "CORE_BUY_WINDOW":
        return [
            {"offset_bps": 4, "timeout_sec": 12, "post_only": False},
            {"offset_bps": 1, "timeout_sec": 8, "post_only": False},
            {"offset_bps": 0, "timeout_sec": 5, "post_only": False},
        ]

    if signal_type == "SNIPER_BUY":
        return [
            {"offset_bps": 1, "timeout_sec": 4, "post_only": False},
            {"offset_bps": 0, "timeout_sec": 3, "post_only": False},
        ]

    attempts = [
        {"offset_bps": 2, "timeout_sec": 8, "post_only": False},
        {"offset_bps": 1, "timeout_sec": 5, "post_only": False},
        {"offset_bps": 0, "timeout_sec": 3, "post_only": False},
    ]

    if volatility_bucket in {"high", "very_high"}:
        attempts = [
            {"offset_bps": 1, "timeout_sec": 5, "post_only": False},
            {"offset_bps": 0, "timeout_sec": 3, "post_only": False},
        ]

    return attempts


def get_base_attempts(signal_type: str = "", volatility_bucket: str = ""):
    signal_type = str(signal_type or "").upper().strip()
    volatility_bucket = str(volatility_bucket or "").lower().strip()

    attempts = [
        {"offset_bps": 2, "timeout_sec": 8, "post_only": False},
        {"offset_bps": 1, "timeout_sec": 5, "post_only": False},
        {"offset_bps": 0, "timeout_sec": 3, "post_only": False},
    ]

    if signal_type in {"EXIT", "SNIPER_EXIT"} or volatility_bucket in {"high", "very_high"}:
        attempts = [
            {"offset_bps": 1, "timeout_sec": 4, "post_only": False},
            {"offset_bps": 0, "timeout_sec": 2, "post_only": False},
        ]

    return attempts


def _get_order_status(order_id: str) -> Tuple[str, float, float, Dict[str, Any]]:
    gd = _to_dict(_coinbase_call(get_client().get_order, order_id=order_id))
    order = gd.get('order', gd)

    status = str(order.get('status', '')).upper()

    filled_base = float(order.get('filled_size') or 0.0)
    filled_value = float(order.get('filled_value') or 0.0)
    avg_price = float(order.get('average_filled_price') or 0.0)

    if avg_price == 0.0 and filled_base > 0.0 and filled_value > 0.0:
        avg_price = filled_value / filled_base

    return status, filled_base, avg_price, order


def _finalize_order_result(
    *,
    order_id: str,
    status: str,
    bid: Decimal | None,
    ask: Decimal | None,
    limit_price: Decimal | None,
    requested_quote_usd: float | None,
    requested_base_size: str | None,
    filled_base: float,
    avg_fill_price: float,
    raw_order: Dict[str, Any],
    execution_path: str,
    fill_liquidity: str,
    result: str | None = None,
    cancel: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload = {
        'ok': True,
        'coinbase_order_id': order_id,
        'status': status,
        'best_bid': str(bid) if bid is not None else None,
        'best_ask': str(ask) if ask is not None else None,
        'limit_price': str(limit_price) if limit_price is not None else None,
        'requested_quote_usd': requested_quote_usd,
        'requested_base_size': requested_base_size,
        'filled_base': filled_base,
        'avg_fill_price': avg_fill_price,
        'execution_path': execution_path,
        'fill_liquidity': fill_liquidity,
        'raw': raw_order,
    }
    if result is not None:
        payload['result'] = result
    if cancel is not None:
        payload['cancel'] = cancel
    print(
        f"[execution] {execution_path} status={status} liquidity={fill_liquidity} "
        f"order_id={order_id} filled_base={filled_base:.8f} avg_fill_price={avg_fill_price:.8f}"
    )
    return payload


def _place_market_order_with_timeout(
    *,
    product_id: str,
    side: str,
    quote_size_usd: float | None = None,
    base_size: float | Decimal | None = None,
    timeout_sec: int = 30,
) -> Dict[str, Any]:
    side = side.upper()
    client = get_client()
    method_candidates = []

    if side == 'BUY':
        if quote_size_usd is None or float(quote_size_usd or 0.0) <= 0:
            return {'ok': False, 'error': 'invalid_quote_size_for_market_buy'}
        method_candidates = [
            ('market_order_buy', {'product_id': product_id, 'quote_size': str(quote_size_usd)}),
            ('market_order_ioc_buy', {'product_id': product_id, 'quote_size': str(quote_size_usd)}),
        ]
    else:
        base_size_value = float(base_size or 0.0)
        if base_size_value <= 0:
            return {'ok': False, 'error': 'invalid_base_size_for_market_sell'}
        base_inc = get_base_increment(product_id)
        rounded_base = round_to_increment(Decimal(str(base_size_value)), base_inc)
        if rounded_base <= 0:
            return {'ok': False, 'error': 'base_size_rounded_to_zero'}
        method_candidates = [
            ('market_order_sell', {'product_id': product_id, 'base_size': str(rounded_base)}),
            ('market_order_ioc_sell', {'product_id': product_id, 'base_size': str(rounded_base)}),
        ]

    chosen = None
    for method_name, kwargs in method_candidates:
        if hasattr(client, method_name):
            chosen = (getattr(client, method_name), method_name, kwargs)
            break

    if chosen is None:
        return {'ok': False, 'error': f'no_market_order_method_available_for_{side.lower()}'}

    method, method_name, kwargs = chosen
    order_resp = _coinbase_call(method, **kwargs)
    od = _to_dict(order_resp)
    order_id = _extract_order_id(od)

    if not order_id:
        return {'ok': False, 'error': 'no_order_id_returned', 'raw': od}

    deadline = time.time() + float(timeout_sec)
    while time.time() < deadline:
        status, filled_base, avg_price, raw_order = _get_order_status(order_id)
        if status in TERMINAL_STATUSES:
            return _finalize_order_result(
                order_id=order_id,
                status=status,
                bid=None,
                ask=None,
                limit_price=None,
                requested_quote_usd=quote_size_usd,
                requested_base_size=str(base_size) if base_size is not None else None,
                filled_base=filled_base,
                avg_fill_price=avg_price,
                raw_order=raw_order,
                execution_path=method_name,
                fill_liquidity='taker',
            )
        time.sleep(1)

    return {'ok': False, 'error': 'market_order_not_terminal_before_timeout', 'coinbase_order_id': order_id, 'raw': od}


def place_limit_quote_with_timeout(
    product_id: str,
    side: str,
    quote_size_usd: float,
    offset_bps: int = 8,
    timeout_sec: int = 25,
    post_only: bool = True,
) -> Dict[str, Any]:
    side = side.upper()
    if _use_limit_orders():
        bid, ask, limit_price = _calc_aggressive_limit_price(product_id, side)
    else:
        return _place_market_order_with_timeout(
            product_id=product_id,
            side=side,
            quote_size_usd=quote_size_usd,
            timeout_sec=max(int(timeout_sec), 30),
        )

    base_inc = get_base_increment(product_id)
    raw_base_size = Decimal(str(quote_size_usd)) / limit_price
    base_size = round_to_increment(raw_base_size, base_inc)

    if base_size <= 0:
        return {
            'ok': False,
            'error': 'base_size_rounded_to_zero',
            'requested_quote_usd': quote_size_usd,
            'limit_price': str(limit_price),
        }

    client_order_id = str(uuid.uuid4())

    order_resp = _coinbase_call(
        get_client().limit_order_gtc,
        client_order_id=client_order_id,
        product_id=product_id,
        side=side,
        base_size=str(base_size),
        limit_price=str(limit_price),
        post_only=bool(post_only),
    )

    od = _to_dict(order_resp)
    order_id = _extract_order_id(od)

    if not order_id:
        return {'ok': False, 'error': 'no_order_id_returned', 'raw': od}

    deadline = time.time() + float(max(timeout_sec, 30))

    while time.time() < deadline:
        status, filled_base, avg_price, raw_order = _get_order_status(order_id)

        if status in TERMINAL_STATUSES:
            return _finalize_order_result(
                order_id=order_id,
                status=status,
                bid=bid,
                ask=ask,
                limit_price=limit_price,
                requested_quote_usd=quote_size_usd,
                requested_base_size=str(base_size),
                filled_base=filled_base,
                avg_fill_price=avg_price,
                raw_order=raw_order,
                execution_path='limit_order_gtc',
                fill_liquidity='taker_limit',
            )

        time.sleep(1)

    cancel_resp = _to_dict(_coinbase_call(get_client().cancel_orders, order_ids=[order_id]))
    status, filled_base, avg_price, raw_order = _get_order_status(order_id)

    if filled_base <= 0 and status not in {'FILLED'}:
        fallback = _place_market_order_with_timeout(
            product_id=product_id,
            side=side,
            quote_size_usd=quote_size_usd,
            timeout_sec=30,
        )
        if fallback.get('ok'):
            fallback['fallback_from'] = 'limit_order_gtc'
            fallback['limit_cancel'] = cancel_resp
            return fallback

    return _finalize_order_result(
        order_id=order_id,
        status=status,
        bid=bid,
        ask=ask,
        limit_price=limit_price,
        requested_quote_usd=quote_size_usd,
        requested_base_size=str(base_size),
        filled_base=filled_base,
        avg_fill_price=avg_price,
        raw_order=raw_order,
        execution_path='limit_order_gtc',
        fill_liquidity='taker_limit',
        result='timeout_cancel',
        cancel=cancel_resp,
    )


def place_limit_base_with_timeout(
    product_id: str,
    side: str,
    base_size: float,
    offset_bps: int = 8,
    timeout_sec: int = 25,
    post_only: bool = True,
) -> Dict[str, Any]:
    side = side.upper()
    if _use_limit_orders():
        bid, ask, limit_price = _calc_aggressive_limit_price(product_id, side)
    else:
        return _place_market_order_with_timeout(
            product_id=product_id,
            side=side,
            base_size=base_size,
            timeout_sec=max(int(timeout_sec), 30),
        )

    base_inc = get_base_increment(product_id)
    base_size_dec = round_to_increment(Decimal(str(base_size)), base_inc)

    if base_size_dec <= 0:
        return {
            'ok': False,
            'error': 'base_size_rounded_to_zero',
            'requested_base_size': str(base_size),
            'limit_price': str(limit_price),
        }

    client_order_id = str(uuid.uuid4())

    order_resp = _coinbase_call(
        get_client().limit_order_gtc,
        client_order_id=client_order_id,
        product_id=product_id,
        side=side,
        base_size=str(base_size_dec),
        limit_price=str(limit_price),
        post_only=bool(post_only),
    )

    od = _to_dict(order_resp)
    order_id = _extract_order_id(od)

    if not order_id:
        return {'ok': False, 'error': 'no_order_id_returned', 'raw': od}

    deadline = time.time() + float(max(timeout_sec, 30))

    while time.time() < deadline:
        status, filled_base, avg_price, raw_order = _get_order_status(order_id)

        if status in TERMINAL_STATUSES:
            return _finalize_order_result(
                order_id=order_id,
                status=status,
                bid=bid,
                ask=ask,
                limit_price=limit_price,
                requested_quote_usd=None,
                requested_base_size=str(base_size_dec),
                filled_base=filled_base,
                avg_fill_price=avg_price,
                raw_order=raw_order,
                execution_path='limit_order_gtc',
                fill_liquidity='taker_limit',
            )

        time.sleep(2)

    cancel_resp = _to_dict(_coinbase_call(get_client().cancel_orders, order_ids=[order_id]))
    status, filled_base, avg_price, raw_order = _get_order_status(order_id)

    if filled_base <= 0 and status not in {'FILLED'}:
        fallback = _place_market_order_with_timeout(
            product_id=product_id,
            side=side,
            base_size=float(base_size_dec),
            timeout_sec=30,
        )
        if fallback.get('ok'):
            fallback['fallback_from'] = 'limit_order_gtc'
            fallback['limit_cancel'] = cancel_resp
            return fallback

    return _finalize_order_result(
        order_id=order_id,
        status=status,
        bid=bid,
        ask=ask,
        limit_price=limit_price,
        requested_quote_usd=None,
        requested_base_size=str(base_size_dec),
        filled_base=filled_base,
        avg_fill_price=avg_price,
        raw_order=raw_order,
        execution_path='limit_order_gtc',
        fill_liquidity='taker_limit',
        result='timeout_cancel',
        cancel=cancel_resp,
    )


def place_limit_quote_with_retries(
    product_id: str,
    side: str,
    quote_size_usd: float,
    attempts=None,
    signal_type: str = "",
    volatility_bucket: str = "",
) -> Dict[str, Any]:
    if attempts is None:
        attempts = get_quote_attempts(signal_type=signal_type, volatility_bucket=volatility_bucket)

    results = []

    for idx, cfg in enumerate(attempts, start=1):
        result = place_limit_quote_with_timeout(
            product_id=product_id,
            side=side,
            quote_size_usd=quote_size_usd,
            offset_bps=int(cfg["offset_bps"]),
            timeout_sec=int(cfg["timeout_sec"]),
            post_only=bool(cfg["post_only"]),
        )

        result["attempt_number"] = idx
        result["attempt_config"] = cfg
        results.append(result)

        filled_base = float(result.get("filled_base", 0.0) or 0.0)
        status = str(result.get("status", "")).upper()

        if result.get("ok") is False:
            wrapper = {
                "ok": False,
                "mode": "retry_execution",
                "filled": False,
                "final_result": result,
                "attempts": results,
            }
            _safe_notify_execution_error(
                product_id=product_id,
                side=side,
                signal_type=signal_type,
                reason=str(result.get("error", "unknown_execution_error")),
            )
            return wrapper

        if filled_base > 0 or status == "FILLED":
            wrapper = {
                "ok": True,
                "mode": "retry_execution",
                "filled": True,
                "final_result": result,
                "attempts": results,
            }
            _safe_notify_execution_result(
                product_id=product_id,
                side=side,
                signal_type=signal_type,
                requested_usd=quote_size_usd,
                requested_base=None,
                result_wrapper=wrapper,
            )
            return wrapper

    wrapper = {
        "ok": True,
        "mode": "retry_execution",
        "filled": False,
        "final_result": results[-1] if results else {},
        "attempts": results,
    }
    return wrapper


def place_limit_base_with_retries(
    product_id: str,
    side: str,
    base_size: float,
    attempts=None,
    signal_type: str = "",
    volatility_bucket: str = "",
) -> Dict[str, Any]:
    if attempts is None:
        attempts = get_base_attempts(signal_type=signal_type, volatility_bucket=volatility_bucket)

    results = []

    for idx, cfg in enumerate(attempts, start=1):
        result = place_limit_base_with_timeout(
            product_id=product_id,
            side=side,
            base_size=base_size,
            offset_bps=int(cfg["offset_bps"]),
            timeout_sec=int(cfg["timeout_sec"]),
            post_only=bool(cfg["post_only"]),
        )

        result["attempt_number"] = idx
        result["attempt_config"] = cfg
        results.append(result)

        filled_base = float(result.get("filled_base", 0.0) or 0.0)
        status = str(result.get("status", "")).upper()

        if result.get("ok") is False:
            wrapper = {
                "ok": False,
                "mode": "retry_execution",
                "filled": False,
                "final_result": result,
                "attempts": results,
            }
            _safe_notify_execution_error(
                product_id=product_id,
                side=side,
                signal_type=signal_type,
                reason=str(result.get("error", "unknown_execution_error")),
            )
            return wrapper

        if filled_base > 0 or status == "FILLED":
            wrapper = {
                "ok": True,
                "mode": "retry_execution",
                "filled": True,
                "final_result": result,
                "attempts": results,
            }
            _safe_notify_execution_result(
                product_id=product_id,
                side=side,
                signal_type=signal_type,
                requested_usd=None,
                requested_base=base_size,
                result_wrapper=wrapper,
            )
            return wrapper

    wrapper = {
        "ok": True,
        "mode": "retry_execution",
        "filled": False,
        "final_result": results[-1] if results else {},
        "attempts": results,
    }
    return wrapper


def _as_list_products_payload(data: Any) -> list[dict]:
    if data is None:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        if isinstance(data.get('products'), list):
            return [x for x in data['products'] if isinstance(x, dict)]
        if isinstance(data.get('product_details'), list):
            return [x for x in data['product_details'] if isinstance(x, dict)]
        if isinstance(data.get('data'), list):
            return [x for x in data['data'] if isinstance(x, dict)]
    return []


def _product_id(prod: dict) -> str:
    return str(prod.get('product_id') or prod.get('id') or '').upper().strip()


def _quote_currency(prod: dict) -> str:
    return str(
        prod.get('quote_currency_id')
        or prod.get('quote_currency')
        or prod.get('quote_currency_symbol')
        or ''
    ).upper().strip()


def _base_currency(prod: dict) -> str:
    return str(
        prod.get('base_currency_id')
        or prod.get('base_currency')
        or prod.get('base_currency_symbol')
        or ''
    ).upper().strip()


def _is_product_tradable(prod: dict) -> bool:
    status = str(prod.get('status') or prod.get('trading_status') or '').upper().strip()

    if prod.get('trading_disabled') is True:
        return False
    if prod.get('is_disabled') is True:
        return False
    if prod.get('cancel_only') is True:
        return False
    if prod.get('auction_mode') is True:
        return False
    if prod.get('view_only') is True:
        return False
    if status in {'OFFLINE', 'DISABLED', 'DELISTED'}:
        return False
    return True


def _normalize_product(prod: dict) -> dict | None:
    pid = _product_id(prod)
    if not pid:
        return None
    return {
        'product_id': pid,
        'base_currency': _base_currency(prod),
        'quote_currency': _quote_currency(prod),
        'status': str(prod.get('status') or prod.get('trading_status') or '').upper().strip(),
        'trading_disabled': bool(prod.get('trading_disabled', False)),
        'is_disabled': bool(prod.get('is_disabled', False)),
        'cancel_only': bool(prod.get('cancel_only', False)),
        'limit_only': bool(prod.get('limit_only', False)),
        'post_only': bool(prod.get('post_only', False)),
        'base_min_size': prod.get('base_min_size') or prod.get('min_market_funds') or prod.get('base_min_size_value'),
        'base_increment': prod.get('base_increment') or prod.get('base_increment_value'),
        'quote_increment': prod.get('quote_increment') or prod.get('quote_increment_value'),
        'price': prod.get('price'),
        'alias_to': prod.get('alias_to'),
        'raw': prod,
    }


def _fetch_products_via_sdk() -> list[dict]:
    try:
        client = get_client()
        if not hasattr(client, 'get_products') and not hasattr(client, 'list_products'):
            return []

        cursor = None
        out = []
        while True:
            if hasattr(client, 'get_products'):
                if cursor:
                    resp = _coinbase_call(client.get_products, limit=250, cursor=cursor)
                else:
                    resp = _coinbase_call(client.get_products, limit=250)
            else:
                if cursor:
                    resp = _coinbase_call(client.list_products, limit=250, cursor=cursor)
                else:
                    resp = _coinbase_call(client.list_products, limit=250)

            data = _to_dict(resp)
            chunk = _as_list_products_payload(data)
            out.extend(chunk)
            has_next = bool(data.get('has_next'))
            cursor = data.get('cursor')
            if not has_next or not cursor:
                break
        return out
    except Exception:
        return []


def _fetch_products_public() -> list[dict]:
    out: list[dict] = []
    cursor = None

    try:
        while True:
            params = {'limit': 500}
            if cursor:
                params['cursor'] = cursor

            r = requests.get(
                'https://api.coinbase.com/api/v3/brokerage/products',
                params=params,
                timeout=20,
                headers={
                    'Accept': 'application/json',
                    'User-Agent': 'tradingbot/valid-products',
                },
            )

            if not r.ok:
                return []

            data = r.json()
            chunk = _as_list_products_payload(data)
            out.extend(chunk)

            has_next = bool(data.get('has_next'))
            cursor = data.get('cursor')

            if not has_next or not cursor:
                break

        return out

    except Exception:
        return []


def clear_product_cache() -> None:
    global _PRODUCT_CACHE
    with _PRODUCT_CACHE_LOCK:
        _PRODUCT_CACHE = {'ts': 0.0, 'products': [], 'error': None}


def get_all_products(force_refresh: bool = False) -> list[dict]:
    global _PRODUCT_CACHE

    now = time.time()

    with _PRODUCT_CACHE_LOCK:
        cached_products = list(_PRODUCT_CACHE.get('products') or [])
        cached_ts = float(_PRODUCT_CACHE.get('ts') or 0.0)

        if not force_refresh and cached_products and (now - cached_ts) < PRODUCT_CACHE_TTL_SEC:
            return cached_products

    products = _fetch_products_via_sdk()

    if not products:
        products = _fetch_products_public()

    cleaned = []
    seen = set()

    for prod in products:
        item = _normalize_product(_to_dict(prod))
        if not item:
            continue

        pid = item['product_id']

        if pid in seen:
            continue

        seen.add(pid)
        cleaned.append(item)

    cleaned.sort(key=lambda x: x['product_id'])

    with _PRODUCT_CACHE_LOCK:
        _PRODUCT_CACHE = {
            'ts': now,
            'products': cleaned,
            'error': None if cleaned else 'no_products_available',
        }

    return list(cleaned)


def get_products_catalog(force_refresh: bool = False):
    try:
        return get_all_products(force_refresh=force_refresh)
    except Exception as exc:
        now = time.time()
        with _PRODUCT_CACHE_LOCK:
            cached_products = list(_PRODUCT_CACHE.get('products') or [])
            cached_ts = float(_PRODUCT_CACHE.get('ts') or 0.0)
            _PRODUCT_CACHE['error'] = str(exc)
        if cached_products and (now - cached_ts) < PRODUCT_CACHE_STALE_SEC:
            return cached_products
        raise


def get_valid_products(quote_currency: str = 'USD', tradable_only: bool = True):
    quote_currency = str(quote_currency or 'USD').upper().strip()
    out = []
    for item in get_products_catalog():
        quote = str(item.get('quote_currency') or '').upper()
        if quote_currency and quote != quote_currency:
            continue
        if tradable_only and not _is_product_tradable(item.get('raw') or item):
            continue
        out.append({k: v for k, v in item.items() if k != 'raw'})
    return out


def get_valid_product_ids(quote_currency: str = 'USD', tradable_only: bool = True) -> list[str]:
    return [p['product_id'] for p in get_valid_products(quote_currency=quote_currency, tradable_only=tradable_only)]


def product_exists(product_id: str) -> bool:
    product_id = str(product_id or "").upper().strip()

    if not product_id:
        return False

    try:
        for item in get_products_catalog():
            if str(item.get("product_id") or "").upper().strip() == product_id:
                return True
    except Exception:
        pass

    try:
        for item in get_products_catalog(force_refresh=True):
            if str(item.get("product_id") or "").upper().strip() == product_id:
                return True
    except Exception:
        pass

    return False


def product_id_exists(product_id: str) -> bool:
    return product_exists(product_id)
