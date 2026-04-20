import json
import os
import threading
import time
from datetime import datetime, timezone

from env_runtime import load_runtime_env

from brokers.base import BrokerAdapter, broker_result

load_runtime_env(override=True)

try:
    from webull.core.client import ApiClient
    from webull.trade.trade_client import TradeClient
    from webull.data.data_client import DataClient
    _WEBULL_SDK_ERROR = None
except Exception as exc:
    ApiClient = None
    TradeClient = None
    DataClient = None
    _WEBULL_SDK_ERROR = exc

_REQUEST_TIMEOUT_SEC = 10
_SESSION_TTL_SEC = 24 * 60 * 60
_SHARED_API_CLIENT = None
_SHARED_TRADE_CLIENT = None
_SHARED_DATA_CLIENT = None
_SHARED_SESSION_TS = 0.0
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


def _to_dict(value):
    return value.to_dict() if hasattr(value, "to_dict") else value


def _response_json(value):
    if hasattr(value, "json") and callable(getattr(value, "json")):
        try:
            return value.json()
        except Exception:
            pass
    return _to_dict(value)


def _response_status_code(value, default=200):
    try:
        code = getattr(value, "status_code", default)
        return int(code)
    except Exception:
        return int(default)


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


def _resolve_path(obj, dotted_path):
    current = obj
    for part in str(dotted_path or "").split("."):
        current = getattr(current, part, None)
        if current is None:
            return None
    return current


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
        self.region = "us"
        self.endpoint = "api.webull.com"
        self._lock = threading.RLock()
        self.account_id = os.getenv("WEBULL_ACCOUNT_ID", "").strip() or None

    def _mode(self):
        return "paper" if self.paper_trading else "live"

    def _ensure_ready(self):
        if not self.enabled:
            raise RuntimeError("webull_disabled")
        if _WEBULL_SDK_ERROR is not None or ApiClient is None or TradeClient is None or DataClient is None:
            raise RuntimeError(f"webull_openapi_sdk_unavailable:{_WEBULL_SDK_ERROR}")
        if not self.app_key or not self.app_secret:
            raise RuntimeError("missing_webull_openapi_credentials")

    def _reset_clients(self):
        global _SHARED_API_CLIENT, _SHARED_TRADE_CLIENT, _SHARED_DATA_CLIENT, _SHARED_SESSION_TS
        with _SHARED_LOCK:
            _SHARED_API_CLIENT = None
            _SHARED_TRADE_CLIENT = None
            _SHARED_DATA_CLIENT = None
            _SHARED_SESSION_TS = 0.0

    def _build_clients(self):
        api_client = ApiClient(self.app_key, self.app_secret, self.region)
        try:
            api_client.add_endpoint(self.region, self.endpoint)
        except Exception:
            pass
        try:
            setattr(api_client, "timeout", _REQUEST_TIMEOUT_SEC)
        except Exception:
            pass
        trade_client = TradeClient(api_client)
        data_client = DataClient(api_client)
        return api_client, trade_client, data_client

    def _get_clients(self, force_reset=False):
        global _SHARED_API_CLIENT, _SHARED_TRADE_CLIENT, _SHARED_DATA_CLIENT, _SHARED_SESSION_TS
        self._ensure_ready()

        with _SHARED_LOCK:
            is_fresh = (
                _SHARED_API_CLIENT is not None
                and _SHARED_TRADE_CLIENT is not None
                and _SHARED_DATA_CLIENT is not None
                and not force_reset
                and (time.time() - _SHARED_SESSION_TS) < _SESSION_TTL_SEC
            )
            if is_fresh:
                return _SHARED_API_CLIENT, _SHARED_TRADE_CLIENT, _SHARED_DATA_CLIENT

            api_client, trade_client, data_client = self._build_clients()
            _SHARED_API_CLIENT = api_client
            _SHARED_TRADE_CLIENT = trade_client
            _SHARED_DATA_CLIENT = data_client
            _SHARED_SESSION_TS = time.time()
            _log_webull_event(
                "session_ready",
                {
                    "mode": self._mode(),
                    "endpoint": self.endpoint,
                    "token_path": "conf/token.txt",
                },
            )
            return _SHARED_API_CLIENT, _SHARED_TRADE_CLIENT, _SHARED_DATA_CLIENT

    def _call_first(self, client, dotted_paths, call_variants=None):
        call_variants = call_variants or [({}, ())]
        last_error = None
        for dotted_path in dotted_paths:
            method = _resolve_path(client, dotted_path)
            if not callable(method):
                continue
            for kwargs, args in call_variants:
                try:
                    response = method(*args, **kwargs)
                    return _response_json(response)
                except Exception as exc:
                    last_error = exc
                    continue
        if last_error is not None:
            raise last_error
        raise AttributeError(f"no_callable_sdk_method:{dotted_paths}")

    def _extract_accounts(self, payload):
        rows = _flatten_candidates(payload)
        accounts = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            account_id = str(row.get("accountId") or row.get("account_id") or row.get("id") or "").strip()
            if account_id:
                accounts.append(row)
        if not accounts:
            row = _first_dict(payload)
            account_id = str(row.get("accountId") or row.get("account_id") or row.get("id") or "").strip()
            if account_id:
                accounts.append(row)
        return accounts

    def _get_account_id(self):
        if self.account_id:
            return self.account_id

        _api_client, trade_client, _data_client = self._get_clients(force_reset=False)
        payload = self._call_first(
            trade_client,
            [
                "account_v2.get_account_list",
                "account.get_account_list",
                "account_v2.list_accounts",
            ],
            call_variants=[({}, ())],
        )
        accounts = self._extract_accounts(payload)
        if not accounts:
            raise RuntimeError("webull_account_list_empty_or_unapproved")

        preferred = None
        for row in accounts:
            account_class = str(row.get("account_class") or row.get("accountClass") or "").strip().upper()
            if account_class == "INDIVIDUAL_CASH":
                preferred = row
                break
        if preferred is None:
            for row in accounts:
                account_type = str(row.get("account_type") or row.get("accountType") or "").strip().upper()
                if account_type == "CASH":
                    preferred = row
                    break
        if preferred is None:
            preferred = accounts[0]

        self.account_id = str(
            preferred.get("accountId") or preferred.get("account_id") or preferred.get("id") or ""
        ).strip()
        if not self.account_id:
            raise RuntimeError("webull_account_id_missing")
        return self.account_id

    def _extract_positions(self, payload):
        rows = _flatten_candidates(payload)
        if rows:
            return [row for row in rows if isinstance(row, dict)]
        row = _first_dict(payload)
        nested = row.get("positions") if isinstance(row, dict) else None
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
        return []

    def connect(self):
        try:
            self._ensure_ready()
            account_id = self._get_account_id()
            status = "verified"
            _log_webull_event("connect_ok", {"mode": self._mode(), "account_id": account_id})
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                connected=True,
                account_id=account_id,
                approval_status=status,
                endpoint=self.endpoint,
                token_path="conf/token.txt",
                last_ready_at=_utcnow_iso(),
            )
        except Exception as exc:
            _log_webull_event("connect_failed", {"mode": self._mode(), "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), connected=False, error=str(exc))

    def get_account_info(self) -> dict:
        try:
            _api_client, trade_client, _data_client = self._get_clients(force_reset=False)
            account_id = self._get_account_id()
            response = trade_client.account_v2.get_account_balance(account_id)
            payload = _response_json(response)
            status_code = _response_status_code(response)
            if status_code >= 400:
                raise RuntimeError(f"webull_http_{status_code}:{payload}")
            row = _first_dict(payload)
            balance = _safe_float(
                row.get("total_net_liquidation_value")
                or row.get("netLiquidation")
                or row.get("netAssetValue")
                or row.get("balance")
                or row.get("totalAsset")
                or 0.0
            )
            currency_assets = row.get("account_currency_assets") or []
            currency_row = currency_assets[0] if isinstance(currency_assets, list) and currency_assets else {}
            buying_power = _safe_float(
                currency_row.get("buying_power")
                or row.get("total_cash_balance")
                or row.get("buyingPower")
                or row.get("buying_power")
                or row.get("cashBalance")
                or row.get("cash")
                or 0.0
            )
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                balance=balance,
                buying_power=buying_power,
                positions=self.get_positions(),
                raw=row,
            )
        except Exception as exc:
            _log_webull_event("get_account_info_failed", {"error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), balance=0.0, buying_power=0.0, positions=[])

    def get_positions(self) -> list:
        import time as _time
        # Return cached positions on 429 to avoid hammering Webull rate limits
        _now = _time.time()
        cached = getattr(self, "_positions_cache", None)
        cache_ts = getattr(self, "_positions_cache_ts", 0)
        if cached is not None and (_now - cache_ts) < 90:
            return cached
        try:
            _api_client, trade_client, _data_client = self._get_clients(force_reset=False)
            account_id = self._get_account_id()
            response = trade_client.account_v2.get_account_position(account_id)
            status_code = getattr(response, "status_code", None) or _response_status_code(response)
            if status_code == 429:
                _log_webull_event("get_positions_rate_limited", {"backoff_sec": 90})
                return cached if cached is not None else []
            if status_code != 200:
                return cached if cached is not None else []
            raw_positions = response.json() if hasattr(response, "json") else _response_json(response)
            if not isinstance(raw_positions, list):
                return []
            positions = []
            for row in raw_positions:
                if not isinstance(row, dict):
                    continue
                instrument_type = str(row.get("instrument_type", "")).upper()
                legs = row.get("legs", [])
                leg = legs[0] if isinstance(legs, list) and legs else {}
                symbol = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("underlying"))
                if not symbol:
                    continue
                position = {
                    "symbol": symbol,
                    "qty": _safe_float(row.get("quantity") or row.get("qty") or row.get("position"), 0.0),
                    "avg_cost": _safe_float(row.get("cost_price") or row.get("costPrice") or row.get("avg_cost") or row.get("avgCost"), 0.0),
                    "market_value": _safe_float(row.get("market_value") or row.get("marketValue") or row.get("value"), 0.0),
                    "unrealized_pnl": _safe_float(
                        row.get("unrealized_profit_loss") or row.get("unrealizedProfitLoss") or row.get("unrealized_pnl"),
                        0.0,
                    ),
                    "unrealized_pnl_pct": _safe_float(
                        row.get("unrealized_profit_loss_rate") or row.get("unrealizedProfitLossRate") or row.get("unrealized_pnl_pct"),
                        0.0,
                    ) * 100.0,
                    "day_pnl": _safe_float(row.get("day_profit_loss") or row.get("dayProfitLoss"), 0.0),
                    "last_price": _safe_float(row.get("last_price") or row.get("lastPrice"), 0.0),
                    "cost_basis": _safe_float(row.get("cost") or row.get("cost_basis"), 0.0),
                    "asset_type": "option" if instrument_type == "OPTION" else "stock",
                    "instrument_type": instrument_type,
                }

                if instrument_type == "OPTION" and isinstance(leg, dict) and leg:
                    strike = _safe_float(leg.get("option_exercise_price") or leg.get("strike_price"), 0.0)
                    option_type = str(leg.get("option_type", "")).upper()
                    expiration = str(leg.get("option_expire_date") or leg.get("expiration_date") or "").strip()
                    multiplier = int(_safe_float(leg.get("option_contract_multiplier"), 100) or 100)
                    position.update(
                        {
                            "option_type": option_type,
                            "strike": strike,
                            "expiration": expiration,
                            "multiplier": multiplier,
                            "display_name": f"{symbol} ${strike:.2f} {option_type} {expiration}".strip(),
                        }
                    )

                positions.append(position)
            self._positions_cache = positions
            self._positions_cache_ts = _time.time()
            return positions
        except Exception as exc:
            _log_webull_event("get_positions_failed", {"error": str(exc)})
            return cached if cached is not None else []

    def get_stock_quote(self, symbol) -> dict:
        symbol = _normalize_symbol(symbol)
        if not symbol:
            return broker_result(False, broker=self.name, mode=self._mode(), error="missing_symbol", symbol=symbol)
        try:
            _api_client, _trade_client, data_client = self._get_clients(force_reset=False)
            payload = self._call_first(
                data_client,
                [
                    "stock.get_quote",
                    "stock.get_snapshot",
                    "quote.get_quote",
                    "get_stock_quote",
                ],
                call_variants=[
                    ({"symbol": symbol}, ()),
                    ({"symbols": [symbol]}, ()),
                    ({}, (symbol,)),
                ],
            )
            row = _first_dict(payload)
            bid = _safe_float(row.get("bid") or row.get("bidPrice") or row.get("bestBid"), 0.0)
            ask = _safe_float(row.get("ask") or row.get("askPrice") or row.get("bestAsk"), 0.0)
            price = _safe_float(row.get("close") or row.get("last") or row.get("lastPrice") or row.get("price"), 0.0)
            volume = _safe_int(row.get("volume"), 0)
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
            _api_client, _trade_client, data_client = self._get_clients(force_reset=False)
            payload = self._call_first(
                data_client,
                [
                    "option.get_option_chain",
                    "option.get_chain",
                    "options.get_option_chain",
                    "get_option_chain",
                ],
                call_variants=[
                    ({"symbol": symbol, "expiration": expiration}, ()),
                    ({"symbol": symbol}, ()),
                    ({}, (symbol, expiration) if expiration else (symbol,)),
                ],
            )
            rows = _flatten_candidates(payload)
            chains = {}
            expirations = set()
            for row in rows:
                if not isinstance(row, dict):
                    continue
                exp = str(row.get("expiration") or row.get("expireDate") or row.get("expirationDate") or "").strip()
                if expiration and exp and exp != str(expiration):
                    continue
                if not exp:
                    continue
                expirations.add(exp)
                bucket = "calls" if str(row.get("optionType") or row.get("direction") or "").lower().startswith("c") else "puts"
                chain = chains.setdefault(exp, {"calls": [], "puts": []})
                chain[bucket].append(
                    {
                        "strike": _safe_float(row.get("strike") or row.get("strikePrice"), 0.0),
                        "bid": _safe_float(row.get("bid") or row.get("bidPrice"), 0.0),
                        "ask": _safe_float(row.get("ask") or row.get("askPrice"), 0.0),
                        "last": _safe_float(row.get("last") or row.get("lastPrice") or row.get("close"), 0.0),
                        "volume": _safe_int(row.get("volume"), 0),
                        "open_interest": _safe_int(row.get("openInterest") or row.get("open_interest"), 0),
                        "delta": _safe_float(row.get("delta"), 0.0),
                        "gamma": _safe_float(row.get("gamma"), 0.0),
                        "theta": _safe_float(row.get("theta"), 0.0),
                        "vega": _safe_float(row.get("vega"), 0.0),
                        "iv": _safe_float(row.get("iv") or row.get("impliedVolatility"), 0.0),
                        "contract_id": str(row.get("contractId") or row.get("instrumentId") or row.get("id") or "").strip(),
                    }
                )
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                symbol=symbol,
                expirations=sorted(expirations),
                chains=chains,
            )
        except Exception as exc:
            # Fallback to yfinance for options chain data
            try:
                import yfinance as yf
                ticker = yf.Ticker(symbol)
                expirations_list = ticker.options or []
                if not expirations_list:
                    _log_webull_event("get_option_chain_failed", {"symbol": symbol, "error": str(exc)})
                    return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), symbol=symbol, expirations=[], chains={})
                chains = {}
                target_exps = [expiration] if expiration and expiration in expirations_list else expirations_list[:6]
                for exp in target_exps:
                    try:
                        chain = ticker.option_chain(exp)
                        def _safe_num(val, default=0.0):
                            import math
                            try:
                                v = float(val)
                                return default if (math.isnan(v) or math.isinf(v)) else v
                            except Exception:
                                return default
                        def _row_to_contract(row):
                            return {
                                "strike": _safe_num(row["strike"] if "strike" in row.index else 0),
                                "bid": _safe_num(row["bid"] if "bid" in row.index else 0),
                                "ask": _safe_num(row["ask"] if "ask" in row.index else 0),
                                "last": _safe_num(row["lastPrice"] if "lastPrice" in row.index else 0),
                                "volume": int(row["volume"] if "volume" in row.index and row["volume"] == row["volume"] else 0),
                                "open_interest": int(row["openInterest"] if "openInterest" in row.index and row["openInterest"] == row["openInterest"] else 0),
                                "iv": _safe_num(row["impliedVolatility"] if "impliedVolatility" in row.index else 0),
                                "delta": None, "gamma": None, "theta": None, "vega": None,
                            }
                        calls = [_row_to_contract(row) for _, row in chain.calls.iterrows()]
                        puts = [_row_to_contract(row) for _, row in chain.puts.iterrows()]
                        chains[exp] = {"calls": calls, "puts": puts}
                    except Exception:
                        continue
                if chains:
                    _log_webull_event("get_option_chain_yfinance", {"symbol": symbol, "expirations": len(chains)})
                    return broker_result(True, broker=self.name, mode=self._mode(), symbol=symbol,
                                        expirations=list(chains.keys()), chains=chains)
            except Exception as yf_exc:
                _log_webull_event("get_option_chain_yfinance_failed", {"symbol": symbol, "error": str(yf_exc)})
            _log_webull_event("get_option_chain_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), symbol=symbol, expirations=[], chains={})

    def _get_instrument_id(self, symbol, data_client) -> str:
        """Resolve Webull instrument_id from ticker symbol."""
        symbol = _normalize_symbol(symbol)
        try:
            result = data_client.instrument.get_instrument(symbol, "US_STOCK")
            rows = result.json() if hasattr(result, "json") else []
            if isinstance(rows, list) and rows:
                return str(rows[0].get("instrument_id", "") or "").strip()
            if isinstance(rows, dict):
                return str(rows.get("instrument_id", "") or "").strip()
        except Exception as exc:
            _log_webull_event("get_instrument_id_failed", {"symbol": symbol, "error": str(exc)})
        return ""

    def place_stock_order(self, symbol, side, qty, order_type="MKT", limit_price=None) -> dict:
        symbol = _normalize_symbol(symbol)
        side = _normalize_side(side)
        qty = _safe_float(qty, 0.0)
        order_type = str(order_type or "MKT").upper().strip()
        if not symbol or side not in {"BUY", "SELL"} or qty <= 0:
            return broker_result(False, broker=self.name, mode=self._mode(), error="invalid_stock_order")
        try:
            _api_client, trade_client, data_client = self._get_clients(force_reset=False)
            account_id = self._get_account_id()
            instrument_id = self._get_instrument_id(symbol, data_client)
            if not instrument_id:
                return broker_result(False, broker=self.name, mode=self._mode(),
                                     error=f"instrument_id_not_found:{symbol}", symbol=symbol)
            import uuid
            client_order_id = str(uuid.uuid4()).replace("-", "")[:32]
            # Map order type to Webull SDK v3 US format (MARKET/LIMIT, not MKT/LMT)
            wb_order_type = "MARKET" if order_type in ("MKT", "MARKET") else "LIMIT"
            wb_side = "BUY" if side == "BUY" else "SELL"
            # Use order_v3 — the US-specific endpoint (/openapi/trade/order/place)
            # trade_client.order routes to the HK endpoint and rejects MARKET orders
            new_order = {
                "market": "US",
                "combo_type": "NORMAL",
                "symbol": symbol,
                "client_order_id": client_order_id,
                "instrument_id": instrument_id,
                "quantity": int(qty),
                "side": wb_side,
                "time_in_force": "DAY",
                "order_type": wb_order_type,
                "entrust_type": "QTY",
                "support_trading_session": "CORE",
            }
            if wb_order_type == "LIMIT" and limit_price is not None:
                new_order["limit_price"] = str(limit_price)
            # order_v2 = /openapi/trade/stock/order/place (US stocks)
            response = trade_client.order_v2.place_order(
                account_id,
                [new_order],
            )
            payload = _response_json(response)
            status_code = _response_status_code(response)
            if status_code >= 400:
                raise RuntimeError(f"webull_http_{status_code}:{payload}")
            row = _first_dict(payload)
            order_id = str(row.get("orderId") or row.get("order_id") or row.get("id") or "").strip()
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                order_id=order_id,
                filled_qty=_safe_float(row.get("filledQty") or row.get("filled_qty"), 0.0),
                fill_price=_safe_float(row.get("fillPrice") or row.get("avgFilledPrice"), 0.0),
                status=str(row.get("status") or row.get("orderStatus") or "submitted").strip().lower(),
                raw=row,
            )
        except Exception as exc:
            _log_webull_event("place_stock_order_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc))

    def place_options_order(self, order) -> dict:
        order = order if isinstance(order, dict) else {}
        symbol = _normalize_symbol(order.get("underlying") or order.get("symbol"))
        side = _normalize_side(order.get("side"))
        qty = _safe_int(order.get("qty"), 0)
        order_type = str(order.get("order_type") or "MARKET").upper().strip()
        if order_type in {"MKT", "MARKET"}:
            order_type = "MARKET"
        elif order_type in {"LMT", "LIMIT"}:
            order_type = "LIMIT"
        if not symbol or side not in {"BUY", "SELL"} or qty <= 0:
            return broker_result(False, broker=self.name, mode=self._mode(), error="invalid_options_order")
        # Reject expired contracts
        try:
            from datetime import date as _date
            _exp = str(order.get('expiration') or '')
            if _exp and _date.fromisoformat(_exp) < _date.today():
                return broker_result(False, broker=self.name, mode=self._mode(), error=f'expired_contract:{_exp}')
        except Exception:
            pass
        try:
            _api_client, trade_client, _data_client = self._get_clients(force_reset=False)
            account_id = self._get_account_id()
            import uuid as _uuid
            import uuid as _uuid
            # Correct Webull US options format per SDK docs
            option_side = "SELL" if side == "SELL" else "BUY"
            new_order = {
                "client_order_id": str(_uuid.uuid4()).replace("-", "")[:32],
                "combo_type": "NORMAL",
                "order_type": order_type,
                "quantity": str(int(qty)),
                "option_strategy": "SINGLE",
                "side": option_side,
                "time_in_force": "GTC",
                "entrust_type": "QTY",
                "orders": [
                    {
                        "side": option_side,
                        "quantity": str(int(qty)),
                        "symbol": symbol,
                        "strike_price": str(order.get("strike") or ""),
                        "init_exp_date": str(order.get("expiration") or ""),
                        "instrument_type": "OPTION",
                        "option_type": str(order.get("option_type") or "call").upper(),
                        "market": "US",
                    }
                ],
            }
            if order_type == "LIMIT" and order.get("limit_price"):
                new_order["limit_price"] = str(round(_safe_float(order.get("limit_price"), 0.0), 2))
            response = trade_client.order_v2.place_option(account_id, [new_order])
            payload = _response_json(response)
            status_code = _response_status_code(response)
            if status_code >= 400:
                raise RuntimeError(f"webull_http_{status_code}:{payload}")
            row = _first_dict(payload)
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                order_id=str(row.get("orderId") or row.get("order_id") or row.get("id") or "").strip(),
                filled_qty=_safe_float(row.get("filledQty") or row.get("filled_qty"), 0.0),
                fill_price=_safe_float(row.get("fillPrice") or row.get("avgFilledPrice"), 0.0),
                status=str(row.get("status") or row.get("orderStatus") or "submitted").strip().lower(),
                raw=row,
            )
        except Exception as exc:
            _log_webull_event("place_options_order_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc))

    def get_open_orders(self) -> list:
        """Get all open/pending orders from Webull."""
        try:
            _api_client, trade_client, _data_client = self._get_clients(force_reset=False)
            account_id = self._get_account_id()
            response = trade_client.order_v2.get_order_open(account_id, page_size=50)
            payload = _response_json(response)
            orders = payload.get("items") or payload.get("orders") or []
            if isinstance(payload, list):
                orders = payload
            return [
                {
                    "order_id": str(o.get("client_order_id") or o.get("orderId") or ""),
                    "symbol": str(o.get("symbol") or o.get("underlying") or ""),
                    "status": str(o.get("status") or o.get("orderStatus") or ""),
                    "order_type": str(o.get("order_type") or o.get("orderType") or ""),
                    "side": str(o.get("side") or ""),
                    "qty": _safe_float(o.get("qty") or o.get("quantity")),
                    "filled_qty": _safe_float(o.get("filled_qty") or o.get("filledQty")),
                    "limit_price": _safe_float(o.get("limit_price") or o.get("limitPrice")),
                    "create_time": str(o.get("create_time") or o.get("createTime") or ""),
                    "raw": o,
                }
                for o in orders
            ]
        except Exception as exc:
            _log_webull_event("get_open_orders_failed", {"error": str(exc)})
            return []

    def cancel_stale_orders(self, max_age_minutes: int = 5) -> list:
        """Cancel any open orders older than max_age_minutes. Returns list of cancelled order_ids."""
        from datetime import datetime, timezone, timedelta
        cancelled = []
        try:
            open_orders = self.get_open_orders()
            now = datetime.now(timezone.utc)
            for order in open_orders:
                order_id = order.get("order_id")
                if not order_id:
                    continue
                # Try to parse create time
                create_str = order.get("create_time", "")
                try:
                    if create_str:
                        create_time = datetime.fromisoformat(create_str.replace("Z", "+00:00"))
                        age_minutes = (now - create_time).total_seconds() / 60
                    else:
                        age_minutes = max_age_minutes + 1  # assume stale if no time
                except Exception:
                    age_minutes = max_age_minutes + 1

                if age_minutes >= max_age_minutes:
                    result = self.cancel_order(order_id)
                    status = "cancelled" if result.get("ok") else f"cancel_failed:{result.get('error')}"
                    _log_webull_event("stale_order_cancelled", {
                        "order_id": order_id,
                        "symbol": order.get("symbol"),
                        "age_minutes": round(age_minutes, 1),
                        "status": status,
                    })
                    cancelled.append({"order_id": order_id, "symbol": order.get("symbol"), "status": status})
        except Exception as exc:
            _log_webull_event("cancel_stale_orders_failed", {"error": str(exc)})
        return cancelled

    def cancel_order(self, order_id) -> dict:
        order_id = str(order_id or "").strip()
        if not order_id:
            return broker_result(False, broker=self.name, mode=self._mode(), error="missing_order_id", order_id=order_id)
        try:
            _api_client, trade_client, _data_client = self._get_clients(force_reset=False)
            response = trade_client.order.cancel_order(order_id)
            payload = _response_json(response)
            status_code = _response_status_code(response)
            if status_code >= 400:
                raise RuntimeError(f"webull_http_{status_code}:{payload}")
            row = _first_dict(payload)
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                order_id=order_id,
                status=str(row.get("status") or row.get("orderStatus") or "cancel_requested").strip().lower(),
                raw=row,
            )
        except Exception as exc:
            _log_webull_event("cancel_order_failed", {"order_id": order_id, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), order_id=order_id)

    def get_order_status(self, order_id) -> dict:
        order_id = str(order_id or "").strip()
        if not order_id:
            return broker_result(False, broker=self.name, mode=self._mode(), error="missing_order_id", order_id=order_id)
        try:
            _api_client, trade_client, _data_client = self._get_clients(force_reset=False)
            response = trade_client.order.query_order_detail(order_id)
            payload = _response_json(response)
            status_code = _response_status_code(response)
            if status_code >= 400:
                raise RuntimeError(f"webull_http_{status_code}:{payload}")
            row = _first_dict(payload)
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                order_id=order_id,
                status=str(row.get("status") or row.get("orderStatus") or "unknown").strip().lower(),
                filled_qty=_safe_float(row.get("filledQty") or row.get("filled_qty"), 0.0),
                fill_price=_safe_float(row.get("fillPrice") or row.get("avgFilledPrice"), 0.0),
                raw=row,
            )
        except Exception as exc:
            _log_webull_event("get_order_status_failed", {"order_id": order_id, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), order_id=order_id)

    def get_watchlist(self) -> list:
        try:
            _api_client, _trade_client, data_client = self._get_clients(force_reset=False)
            payload = self._call_first(
                data_client,
                [
                    "watchlist.get_watchlist",
                    "watchlist.list_watchlists",
                    "get_watchlist",
                ],
                call_variants=[({}, ())],
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
