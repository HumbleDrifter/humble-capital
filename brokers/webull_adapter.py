import base64
import hashlib
import hmac
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

import requests

from env_runtime import load_runtime_env

from brokers.base import BrokerAdapter, broker_result

load_runtime_env(override=True)

_REQUEST_TIMEOUT_SEC = 10
_SESSION_TTL_SEC = 24 * 60 * 60
_SIGNATURE_VERSION = "1.0"
_SIGNATURE_ALGORITHM = "HMAC-SHA1"
_USER_AGENT = "HumbleCapital-WebullOpenAPI/1.0"

_LIVE_BASE_URL = "https://api.webull.com"
_PAPER_BASE_URL = "https://paper-api.webull.com"

_DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": _USER_AGENT,
}

_SHARED_SESSION = None
_SHARED_SESSION_TS = 0.0
_SHARED_BASE_URL = None
_SHARED_LOCK = threading.RLock()


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _env_bool(name, default=False):
    raw = str(os.getenv(name, str(default)) or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _utcnow_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_symbol(symbol):
    return str(symbol or "").upper().strip()


def _normalize_side(side):
    raw = str(side or "").strip().lower()
    if raw in {"buy", "b"}:
        return "BUY"
    if raw in {"sell", "s"}:
        return "SELL"
    return raw.upper()


def _normalize_asset_type(value):
    raw = str(value or "").strip().lower()
    if "option" in raw or raw in {"opt"}:
        return "option"
    return "stock"


def _json_dumps(value):
    if value is None:
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _flatten_candidates(value):
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in ("data", "items", "rows", "list", "results", "records"):
            row = value.get(key)
            if isinstance(row, list):
                return row
    return []


def _first_dict(value):
    if isinstance(value, dict):
        for key in ("data", "item", "result"):
            row = value.get(key)
            if isinstance(row, dict):
                return row
        return value
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    return {}


def _parse_money_fields(row, names):
    for name in names:
        value = _safe_float((row or {}).get(name), None)
        if value is not None:
            return value
    return 0.0


def _log_webull_event(event, payload=None):
    envelope = {
        "ts": int(time.time()),
        "pid": os.getpid(),
        "thread": threading.current_thread().name,
        "component": "webull_adapter",
        "event": str(event or "").strip() or "unknown",
        "payload": payload if isinstance(payload, dict) else {},
    }
    print(json.dumps(envelope, sort_keys=True, ensure_ascii=False))


class WebullAdapter(BrokerAdapter):
    name = "webull"

    def __init__(self):
        load_runtime_env(override=True)
        self.enabled = _env_bool("WEBULL_ENABLED", False)
        self.app_key = str(os.getenv("WEBULL_APP_KEY", "") or "").strip()
        self.app_secret = str(os.getenv("WEBULL_APP_SECRET", "") or "").strip()
        self.paper_trading = _env_bool("WEBULL_PAPER_TRADING", True)
        self.base_url = _PAPER_BASE_URL if self.paper_trading else _LIVE_BASE_URL
        self._lock = threading.RLock()
        self._account_id = None

    def _mode(self):
        return "paper" if self.paper_trading else "live"

    def _ensure_ready(self):
        if not self.enabled:
            raise RuntimeError("webull_disabled")
        if not self.app_key or not self.app_secret:
            raise RuntimeError("missing_webull_openapi_credentials")

    def _reset_session(self):
        global _SHARED_SESSION, _SHARED_SESSION_TS, _SHARED_BASE_URL
        with _SHARED_LOCK:
            _SHARED_SESSION = None
            _SHARED_SESSION_TS = 0.0
            _SHARED_BASE_URL = None

    def _get_session(self, force_reset=False):
        global _SHARED_SESSION, _SHARED_SESSION_TS, _SHARED_BASE_URL
        self._ensure_ready()

        with _SHARED_LOCK:
            is_fresh = (
                _SHARED_SESSION is not None
                and not force_reset
                and _SHARED_BASE_URL == self.base_url
                and (time.time() - _SHARED_SESSION_TS) < _SESSION_TTL_SEC
            )
            if is_fresh:
                return _SHARED_SESSION

            session = requests.Session()
            session.headers.update(_DEFAULT_HEADERS)
            _SHARED_SESSION = session
            _SHARED_SESSION_TS = time.time()
            _SHARED_BASE_URL = self.base_url
            _log_webull_event("session_ready", {"mode": self._mode(), "base_url": self.base_url})
            return session

    def _signature_headers(self, method: str, path: str, query: Optional[Dict[str, Any]], body: Optional[Dict[str, Any]]):
        nonce = uuid.uuid4().hex
        timestamp = _utcnow_iso()
        parsed_host = self.base_url.split("://", 1)[-1].strip().rstrip("/")
        body_json = _json_dumps(body)

        signed_headers = {
            "host": parsed_host,
            "x-app-key": self.app_key,
            "x-signature-algorithm": _SIGNATURE_ALGORITHM,
            "x-signature-version": _SIGNATURE_VERSION,
            "x-signature-nonce": nonce,
            "x-timestamp": timestamp,
        }

        kv_pairs = []
        for key, value in (query or {}).items():
            if value is None:
                continue
            kv_pairs.append((str(key), str(value)))
        for key, value in signed_headers.items():
            kv_pairs.append((str(key), str(value)))
        kv_pairs.sort(key=lambda row: row[0])

        canonical = "&".join(f"{key}={value}" for key, value in kv_pairs)
        parts = [path, canonical]
        if body_json:
            parts.append(hashlib.md5(body_json.encode("utf-8")).hexdigest().upper())
        sign_source = "&".join(parts)
        encoded_source = quote(sign_source, safe="")

        digest = hmac.new(
            f"{self.app_secret}&".encode("utf-8"),
            encoded_source.encode("utf-8"),
            hashlib.sha1,
        ).digest()
        signature = base64.b64encode(digest).decode("utf-8")

        return {
            "x-app-key": self.app_key,
            "x-signature": signature,
            "x-signature-algorithm": _SIGNATURE_ALGORITHM,
            "x-signature-version": _SIGNATURE_VERSION,
            "x-signature-nonce": nonce,
            "x-timestamp": timestamp,
            "Host": parsed_host,
        }

    def _request_once(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        allow_404: bool = True,
    ) -> Dict[str, Any]:
        session = self._get_session(force_reset=False)
        headers = self._signature_headers(method, path, query, body)
        response = session.request(
            method=str(method).upper().strip(),
            url=f"{self.base_url.rstrip('/')}{path}",
            params={k: v for k, v in (query or {}).items() if v is not None},
            json=body if body is not None else None,
            headers=headers,
            timeout=_REQUEST_TIMEOUT_SEC,
        )

        try:
            payload = response.json()
        except Exception:
            payload = {"raw_text": response.text}

        _log_webull_event(
            "http_response",
            {
                "method": str(method).upper().strip(),
                "path": path,
                "status_code": response.status_code,
                "mode": self._mode(),
            },
        )

        if response.status_code == 404 and allow_404:
            raise FileNotFoundError(path)
        if response.status_code >= 400:
            raise RuntimeError(f"http_{response.status_code}:{payload}")
        return payload if isinstance(payload, dict) else {"data": payload}

    def _request(
        self,
        method: str,
        paths: Iterable[str],
        *,
        query: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        last_error = None
        path_list = list(paths)
        for index, path in enumerate(path_list):
            allow_404 = index < len(path_list) - 1
            try:
                return self._request_once(method, path, query=query, body=body, allow_404=allow_404)
            except FileNotFoundError as exc:
                last_error = exc
                continue
            except requests.RequestException as exc:
                last_error = exc
                self._get_session(force_reset=True)
                continue
            except Exception as exc:
                last_error = exc
                continue
        if last_error is not None:
            raise last_error
        raise RuntimeError("no_request_paths_provided")

    def _extract_accounts(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates = _flatten_candidates(payload)
        accounts = []
        for row in candidates:
            if not isinstance(row, dict):
                continue
            account_id = str(row.get("account_id") or row.get("accountId") or row.get("id") or "").strip()
            if account_id:
                accounts.append(row)
        if not accounts:
            first = _first_dict(payload)
            account_id = str(first.get("account_id") or first.get("accountId") or first.get("id") or "").strip()
            if account_id:
                accounts.append(first)
        return accounts

    def _get_account_id(self) -> str:
        if self._account_id:
            return self._account_id

        payload = self._request(
            "GET",
            [
                "/openapi/trade/account/list",
                "/openapi/account/list",
                "/trade/account/list",
                "/account/list",
            ],
        )
        accounts = self._extract_accounts(payload)
        if not accounts:
            raise RuntimeError("webull_account_list_empty")
        account = accounts[0]
        self._account_id = str(account.get("account_id") or account.get("accountId") or account.get("id") or "").strip()
        return self._account_id

    def _get_balance_payload(self) -> Dict[str, Any]:
        return self._request(
            "GET",
            [
                "/openapi/trade/account/balance",
                "/openapi/assets/balance",
                "/trade/account/balance",
                "/assets/balance",
            ],
            query={"account_id": self._get_account_id()},
        )

    def _get_positions_payload(self) -> Dict[str, Any]:
        return self._request(
            "GET",
            [
                "/openapi/trade/account/positions",
                "/openapi/assets/positions",
                "/trade/account/positions",
                "/assets/positions",
            ],
            query={"account_id": self._get_account_id()},
        )

    def _extract_position_rows(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates = _flatten_candidates(payload)
        if candidates:
            return [row for row in candidates if isinstance(row, dict)]
        first = _first_dict(payload)
        rows = first.get("positions") if isinstance(first, dict) else None
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        return []

    def _resolve_symbol_quote(self, symbol: str) -> Dict[str, Any]:
        payload = self._request(
            "GET",
            [
                "/openapi/market-data/stock/snapshot",
                "/openapi/market-data/snapshot",
                "/market-data/stock/snapshot",
            ],
            query={"symbols": _normalize_symbol(symbol)},
        )
        rows = _flatten_candidates(payload)
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_symbol = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("displaySymbol"))
            if not row_symbol or row_symbol == _normalize_symbol(symbol):
                return row
        return _first_dict(payload)

    def _resolve_instrument(self, symbol: str, asset_type: str = "stock") -> Dict[str, Any]:
        query = {"symbols": _normalize_symbol(symbol)}
        paths = [
            "/openapi/trade/instrument",
            "/openapi/market-data/instrument",
            "/openapi/instrument",
        ]
        if asset_type == "option":
            paths = [
                "/openapi/trade/option/instrument",
                "/openapi/trade/instrument",
                "/openapi/instrument",
            ]
        payload = self._request("GET", paths, query=query)
        rows = _flatten_candidates(payload)
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_symbol = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("displaySymbol"))
            if row_symbol == _normalize_symbol(symbol):
                return row
        return _first_dict(payload)

    def _quote_price_from_row(self, row: Dict[str, Any]) -> Tuple[float, float, float, int]:
        bid = _safe_float(row.get("bid") or row.get("bid_price") or row.get("bestBid"), 0.0)
        ask = _safe_float(row.get("ask") or row.get("ask_price") or row.get("bestAsk"), 0.0)
        price = _safe_float(
            row.get("close") or row.get("last") or row.get("last_price") or row.get("price") or row.get("latestPrice"),
            0.0,
        )
        volume = _safe_int(row.get("volume") or row.get("vol"), 0)
        return price, bid, ask, volume

    def connect(self):
        try:
            self._ensure_ready()
            account_id = self._get_account_id()
            balance_payload = self._get_balance_payload()
            _log_webull_event("connect_ok", {"mode": self._mode(), "account_id": account_id})
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                connected=True,
                account_id=account_id,
                account_probe=balance_payload,
                last_ready_at=_utcnow_iso(),
            )
        except Exception as exc:
            _log_webull_event("connect_failed", {"mode": self._mode(), "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), connected=False, error=str(exc))

    def get_account_info(self) -> dict:
        try:
            balance_payload = self._get_balance_payload()
            positions = self.get_positions()
            balance_row = _first_dict(balance_payload)
            balance = _parse_money_fields(
                balance_row,
                ["net_liquidation", "netLiquidation", "netAssetValue", "balance", "totalAsset", "total_value"],
            )
            buying_power = _parse_money_fields(
                balance_row,
                ["buying_power", "buyingPower", "cash_balance", "cashBalance", "available_funds"],
            )
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                balance=balance,
                buying_power=buying_power,
                positions=positions,
            )
        except Exception as exc:
            _log_webull_event("get_account_info_failed", {"error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), balance=0.0, buying_power=0.0, positions=[])

    def get_positions(self) -> list:
        try:
            payload = self._get_positions_payload()
            rows = self._extract_position_rows(payload)
            positions = []
            for row in rows:
                symbol = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("underlying"))
                qty = _safe_float(row.get("qty") or row.get("quantity") or row.get("position"), 0.0)
                avg_cost = _safe_float(row.get("avg_cost") or row.get("avgCost") or row.get("costPrice"), 0.0)
                market_value = _safe_float(row.get("market_value") or row.get("marketValue") or row.get("value"), 0.0)
                unrealized_pnl = _safe_float(
                    row.get("unrealized_pnl") or row.get("unrealizedProfitLoss") or row.get("unrealizedProfit"),
                    0.0,
                )
                if not symbol:
                    continue
                positions.append(
                    {
                        "symbol": symbol,
                        "qty": qty,
                        "avg_cost": avg_cost,
                        "market_value": market_value,
                        "unrealized_pnl": unrealized_pnl,
                        "asset_type": _normalize_asset_type(row.get("asset_type") or row.get("category") or row.get("secType")),
                    }
                )
            return positions
        except Exception as exc:
            _log_webull_event("get_positions_failed", {"error": str(exc)})
            return []

    def get_stock_quote(self, symbol) -> dict:
        symbol = _normalize_symbol(symbol)
        if not symbol:
            return broker_result(False, broker=self.name, mode=self._mode(), error="missing_symbol", symbol=symbol)
        try:
            row = self._resolve_symbol_quote(symbol)
            price, bid, ask, volume = self._quote_price_from_row(row)
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                symbol=symbol,
                price=price,
                bid=bid,
                ask=ask,
                volume=volume,
                raw=row,
            )
        except Exception as exc:
            _log_webull_event("get_stock_quote_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), symbol=symbol)

    def get_option_chain(self, symbol, expiration=None) -> dict:
        symbol = _normalize_symbol(symbol)
        if not symbol:
            return broker_result(False, broker=self.name, mode=self._mode(), error="missing_symbol", symbol=symbol, expirations=[], chains={})
        try:
            quote = self.get_stock_quote(symbol)
            underlying_price = _safe_float(quote.get("price"), 0.0)
            payload = self._request(
                "GET",
                [
                    "/openapi/market-data/option/chain",
                    "/openapi/market-data/options/chain",
                    "/openapi/trade/option/chain",
                    "/openapi/option/chain",
                ],
                query={"symbol": symbol, "expiration": expiration},
            )

            rows = _flatten_candidates(payload)
            chains = {}
            expirations = set()
            for row in rows:
                if not isinstance(row, dict):
                    continue
                expiry = str(row.get("expiration") or row.get("expire_date") or row.get("expirationDate") or "").strip()
                if expiration and expiry and expiry != str(expiration):
                    continue
                if not expiry:
                    continue
                expirations.add(expiry)
                bucket = "calls" if str(row.get("option_type") or row.get("optionType") or row.get("direction") or "").lower().startswith("c") else "puts"
                chain = chains.setdefault(expiry, {"calls": [], "puts": []})
                chain[bucket].append(
                    {
                        "symbol": symbol,
                        "expiration": expiry,
                        "contract_id": str(row.get("instrument_id") or row.get("option_id") or row.get("contract_id") or "").strip(),
                        "strike": _safe_float(row.get("strike") or row.get("strike_price") or row.get("strikePrice"), 0.0),
                        "bid": _safe_float(row.get("bid") or row.get("bid_price"), 0.0),
                        "ask": _safe_float(row.get("ask") or row.get("ask_price"), 0.0),
                        "last": _safe_float(row.get("last") or row.get("last_price") or row.get("close"), 0.0),
                        "volume": _safe_int(row.get("volume"), 0),
                        "open_interest": _safe_int(row.get("open_interest") or row.get("openInterest"), 0),
                        "delta": _safe_float(row.get("delta"), 0.0),
                        "gamma": _safe_float(row.get("gamma"), 0.0),
                        "theta": _safe_float(row.get("theta"), 0.0),
                        "vega": _safe_float(row.get("vega"), 0.0),
                        "iv": _safe_float(row.get("iv") or row.get("implied_volatility") or row.get("impliedVolatility"), 0.0),
                    }
                )

            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                symbol=symbol,
                underlying_price=underlying_price,
                expirations=sorted(expirations),
                chains=chains,
            )
        except Exception as exc:
            _log_webull_event("get_option_chain_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), symbol=symbol, expirations=[], chains={})

    def _build_stock_order_body(self, symbol, side, qty, order_type, limit_price):
        instrument = self._resolve_instrument(symbol, asset_type="stock")
        instrument_id = str(instrument.get("instrument_id") or instrument.get("instrumentId") or instrument.get("id") or "").strip()
        return {
            "account_id": self._get_account_id(),
            "client_order_id": uuid.uuid4().hex,
            "instrument_id": instrument_id,
            "symbol": symbol,
            "side": side,
            "order_type": str(order_type or "MKT").upper().strip(),
            "qty": str(qty),
            "limit_price": str(limit_price) if limit_price is not None else None,
            "time_in_force": "DAY",
        }

    def place_stock_order(self, symbol, side, qty, order_type="MKT", limit_price=None) -> dict:
        symbol = _normalize_symbol(symbol)
        side = _normalize_side(side)
        qty = _safe_float(qty, 0.0)
        order_type = str(order_type or "MKT").upper().strip()
        if not symbol or side not in {"BUY", "SELL"} or qty <= 0:
            return broker_result(False, broker=self.name, mode=self._mode(), error="invalid_stock_order")
        try:
            body = self._build_stock_order_body(symbol, side, qty, order_type, limit_price)
            payload = self._request(
                "POST",
                [
                    "/openapi/trade/stock/order/place",
                    "/openapi/trade/order/place",
                    "/trade/stock/order/place",
                ],
                body={k: v for k, v in body.items() if v not in {None, ""}},
            )
            data = _first_dict(payload)
            order_id = str(data.get("order_id") or data.get("orderId") or body["client_order_id"]).strip()
            filled_qty = _safe_float(data.get("filled_qty") or data.get("filledQuantity"), 0.0)
            fill_price = _safe_float(data.get("filled_price") or data.get("filledPrice") or data.get("avgFilledPrice"), 0.0)
            status = str(data.get("status") or data.get("order_status") or "submitted").strip().lower()
            _log_webull_event("stock_order_submitted", {"symbol": symbol, "side": side, "qty": qty, "mode": self._mode()})
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                order_id=order_id,
                filled_qty=filled_qty,
                fill_price=fill_price,
                status=status,
                raw=data,
            )
        except Exception as exc:
            _log_webull_event("place_stock_order_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc))

    def _find_option_contract(self, order: Dict[str, Any]) -> Dict[str, Any]:
        symbol = _normalize_symbol(order.get("underlying"))
        expiration = str(order.get("expiration") or "").strip()
        strike = _safe_float(order.get("strike"), 0.0)
        option_type = str(order.get("option_type") or "").lower().strip()
        chain = self.get_option_chain(symbol, expiration=expiration)
        if not chain.get("ok"):
            return {}
        bucket = "calls" if option_type == "call" else "puts"
        rows = ((chain.get("chains") or {}).get(expiration) or {}).get(bucket) or []
        for row in rows:
            if abs(_safe_float(row.get("strike"), 0.0) - strike) < 1e-8:
                return row
        return {}

    def place_options_order(self, order) -> dict:
        order = order if isinstance(order, dict) else {}
        side = _normalize_side(order.get("side"))
        qty = _safe_int(order.get("qty"), 0)
        order_type = str(order.get("order_type") or "MKT").upper().strip()
        option_type = str(order.get("option_type") or "").lower().strip()
        symbol = _normalize_symbol(order.get("underlying"))
        expiration = str(order.get("expiration") or "").strip()
        if not symbol or option_type not in {"call", "put"} or side not in {"BUY", "SELL"} or qty <= 0:
            return broker_result(False, broker=self.name, mode=self._mode(), error="invalid_options_order")
        try:
            contract = self._find_option_contract(order)
            account_id = self._get_account_id()
            client_order_id = uuid.uuid4().hex
            body = {
                "account_id": account_id,
                "client_order_id": client_order_id,
                "symbol": symbol,
                "expiration": expiration,
                "strike": _safe_float(order.get("strike"), 0.0),
                "option_type": option_type.upper(),
                "contract_id": str(contract.get("contract_id") or "").strip(),
                "side": side,
                "qty": str(qty),
                "order_type": order_type,
                "limit_price": str(order.get("limit_price")) if order.get("limit_price") is not None else None,
                "time_in_force": "DAY",
            }
            payload = self._request(
                "POST",
                [
                    "/openapi/trade/option/order/place",
                    "/openapi/trade/options/order/place",
                    "/openapi/trade/order/place",
                ],
                body={k: v for k, v in body.items() if v not in {None, ""}},
            )
            data = _first_dict(payload)
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                order_id=str(data.get("order_id") or data.get("orderId") or client_order_id).strip(),
                filled_qty=_safe_float(data.get("filled_qty") or data.get("filledQuantity"), 0.0),
                fill_price=_safe_float(data.get("filled_price") or data.get("filledPrice") or data.get("avgFilledPrice"), 0.0),
                status=str(data.get("status") or data.get("order_status") or "submitted").strip().lower(),
                raw=data,
            )
        except Exception as exc:
            _log_webull_event("place_options_order_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc))

    def cancel_order(self, order_id) -> dict:
        order_id = str(order_id or "").strip()
        if not order_id:
            return broker_result(False, broker=self.name, mode=self._mode(), error="missing_order_id", order_id=order_id)
        try:
            payload = self._request(
                "POST",
                [
                    "/openapi/trade/stock/order/cancel",
                    "/openapi/trade/option/order/cancel",
                    "/openapi/trade/order/cancel",
                ],
                body={"account_id": self._get_account_id(), "client_order_id": order_id, "order_id": order_id},
            )
            data = _first_dict(payload)
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                order_id=order_id,
                status=str(data.get("status") or data.get("order_status") or "cancel_requested").strip().lower(),
                raw=data,
            )
        except Exception as exc:
            _log_webull_event("cancel_order_failed", {"order_id": order_id, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), order_id=order_id)

    def get_order_status(self, order_id) -> dict:
        order_id = str(order_id or "").strip()
        if not order_id:
            return broker_result(False, broker=self.name, mode=self._mode(), error="missing_order_id", order_id=order_id)
        try:
            payload = self._request(
                "GET",
                [
                    "/openapi/trade/order/detail",
                    "/trade/order/detail",
                    "/openapi/trade/order/history",
                ],
                query={"account_id": self._get_account_id(), "client_order_id": order_id, "order_id": order_id},
            )
            data = _first_dict(payload)
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                order_id=order_id,
                status=str(data.get("order_status") or data.get("status") or "unknown").strip().lower(),
                filled_qty=_safe_float(data.get("filled_qty") or data.get("filledQuantity"), 0.0),
                fill_price=_safe_float(data.get("filled_price") or data.get("filledPrice") or data.get("avgFilledPrice"), 0.0),
                raw=data,
            )
        except Exception as exc:
            _log_webull_event("get_order_status_failed", {"order_id": order_id, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), order_id=order_id)

    def get_watchlist(self) -> list:
        try:
            payload = self._request(
                "GET",
                [
                    "/openapi/market-data/watchlist",
                    "/openapi/watchlist",
                ],
            )
            rows = _flatten_candidates(payload)
            symbols = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                symbol = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("displaySymbol"))
                if symbol:
                    symbols.append(symbol)
            return sorted(set(symbols))
        except Exception as exc:
            _log_webull_event("get_watchlist_failed", {"error": str(exc)})
            return []
