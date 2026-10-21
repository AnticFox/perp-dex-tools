"""
Ondo Perps exchange client.

REST and WebSocket docs: https://docs.ondoperps.xyz/api-reference/api_key_authentication
"""

import asyncio
import hmac
import hashlib
import json
import os
import ssl
import time
import traceback
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import aiohttp
import certifi
import websockets
from websockets.exceptions import ConnectionClosed

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger


DEFAULT_BASE_URL = "https://api.ondoperps.xyz"
DEFAULT_WS_URL = "wss://api.ondoperps.xyz/ws"
WS_LOGIN_PAYLOAD = "ondo_perps_ws_login"


class OndoApiError(Exception):
    """Ondo REST error that includes the API error code."""

    def __init__(self, message: str, error_code: Optional[str] = None, status: Optional[int] = None):
        super().__init__(message)
        self.error_code = error_code
        self.status = status


def sign_ondo_request(secret: str, timestamp: str, method: str, path: str, body: str) -> str:
    """HMAC-SHA256 of timestamp + METHOD + path + body, hex encoded."""
    message = f"{timestamp}{method.upper()}{path}{body}"
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def sign_ondo_ws_login(secret: str, timestamp: str) -> str:
    """HMAC-SHA256 of timestamp + 'ondo_perps_ws_login', hex encoded."""
    message = f"{timestamp}{WS_LOGIN_PAYLOAD}"
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def market_id_for_ticker(ticker: str) -> str:
    """Map a user ticker to an Ondo perpetual market id."""
    symbol = (ticker or "").strip().upper()
    if not symbol:
        raise ValueError("Ticker is empty")
    if symbol.endswith(".P"):
        return symbol
    if symbol.endswith("-USD"):
        return f"{symbol}.P"
    return f"{symbol}-USD.P"


def align_to_step(value: Decimal, step: Decimal, rounding=ROUND_HALF_UP) -> Decimal:
    """Round value to a multiple of step."""
    value = Decimal(value)
    step = Decimal(step)
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=rounding)
    return units * step


def format_decimal(value: Decimal) -> str:
    return format(Decimal(value), "f")


def map_order_status(status: str, size: Decimal, filled: Decimal) -> str:
    """Map Ondo order status onto the statuses the grid bot expects."""
    normalized = (status or "").lower()
    if normalized == "fullyfilled":
        return "FILLED"
    if normalized == "canceled":
        return "CANCELED"
    if normalized in {"open", "pending", "untriggered"}:
        if filled > 0 and (size <= 0 or filled < size):
            return "PARTIALLY_FILLED"
        return "OPEN"
    return normalized.upper()


def first_level_price(levels: Any) -> Decimal:
    """Read the best price from an Ondo book side."""
    if not levels:
        return Decimal(0)
    first = levels[0]
    if isinstance(first, (list, tuple)):
        return Decimal(str(first[0])) if first else Decimal(0)
    if isinstance(first, dict):
        for key in ("price", "p"):
            if key in first:
                return Decimal(str(first[key]))
        return Decimal(0)
    if isinstance(first, str):
        return Decimal(first)
    return Decimal(0)


def build_request_path(path: str, params: Optional[Dict[str, Any]] = None) -> str:
    if not params:
        return path
    query = urlencode([(key, value) for key, value in params.items() if value is not None])
    if not query:
        return path
    return f"{path}?{query}"


class OndoClient(BaseExchangeClient):
    """Ondo Perps client used by the grid trading bot."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.key_id = os.getenv("ONDO_KEY_ID", "")
        self.api_secret = os.getenv("ONDO_API_SECRET", "")
        self.base_url = os.getenv("ONDO_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
        self.ws_url = os.getenv("ONDO_WS_URL", DEFAULT_WS_URL)
        self.logger = TradingLogger(exchange="ondo", ticker=self.config.ticker, log_to_console=False)

        self._base_increment = Decimal("0.01")
        self._session: Optional[aiohttp.ClientSession] = None
        self._order_update_handler = None
        self._best_bid = Decimal(0)
        self._best_ask = Decimal(0)
        self._bbo_ts = 0.0

        self._ws_task: Optional[asyncio.Task] = None
        self._ws_stop = asyncio.Event()
        self._ws_ready = asyncio.Event()
        self._ws = None

    def _validate_config(self) -> None:
        missing = [name for name in ("ONDO_KEY_ID", "ONDO_API_SECRET") if not os.getenv(name)]
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")

    def get_exchange_name(self) -> str:
        return "ondo"

    def round_to_tick(self, price) -> Decimal:
        return align_to_step(Decimal(price), self.config.tick_size, rounding=ROUND_HALF_UP)

    def setup_order_update_handler(self, handler) -> None:
        self._order_update_handler = handler

    async def connect(self) -> None:
        await self._ensure_session()
        self._ws_stop.clear()
        if self._ws_task is None or self._ws_task.done():
            self._ws_ready.clear()
            self._ws_task = asyncio.create_task(self._run_ws())
        try:
            await asyncio.wait_for(self._ws_ready.wait(), timeout=15)
            self.logger.log("[WS] private order stream ready", "INFO")
        except asyncio.TimeoutError:
            self.logger.log("[WS] not ready within 15s; order updates will fall back to REST", "WARNING")

    async def disconnect(self) -> None:
        self._ws_stop.set()
        ws = self._ws
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
        if self._ws_task is not None:
            try:
                await asyncio.wait_for(self._ws_task, timeout=5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._ws_task.cancel()
            self._ws_task = None
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        self.logger.log("Ondo Perps disconnected", "INFO")

    def _ssl_context(self) -> ssl.SSLContext:
        return ssl.create_default_context(cafile=certifi.where())

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(ssl=self._ssl_context())
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self._session

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        auth: bool = True,
    ) -> Dict[str, Any]:
        session = await self._ensure_session()
        request_path = build_request_path(path, params)
        body_str = "" if body is None else json.dumps(body, separators=(",", ":"))
        headers = {"Accept": "application/json"}
        data = None
        if body is not None:
            data = body_str.encode()
            headers["Content-Type"] = "application/json"
        if auth:
            timestamp = str(int(time.time() * 1000))
            headers["ONDO-KEY-ID"] = self.key_id
            headers["ONDO-TIMESTAMP"] = timestamp
            headers["ONDO-SIGN"] = sign_ondo_request(
                self.api_secret, timestamp, method, request_path, body_str
            )

        url = f"{self.base_url}{request_path}"
        try:
            async with session.request(method.upper(), url, data=data, headers=headers) as response:
                text = await response.text()
                status = response.status
        except aiohttp.ClientError as exc:
            raise OndoApiError(f"Ondo request failed: {exc}") from exc

        try:
            payload = json.loads(text) if text else {}
        except json.JSONDecodeError as exc:
            raise OndoApiError(f"Ondo returned non-JSON ({status}): {text[:300]}", status=status) from exc

        if status >= 400 or payload.get("success") is False:
            error_code = payload.get("error_code")
            message = payload.get("error") or text[:300] or f"HTTP {status}"
            raise OndoApiError(message, error_code=error_code, status=status)
        return payload

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        ticker = self.config.ticker
        market_id = market_id_for_ticker(ticker)
        payload = await self._request("GET", "/v1/markets", auth=False)
        pairs = ((payload.get("result") or {}).get("perps") or {}).get("tradingPairs") or []

        market = None
        for pair in pairs:
            if pair.get("market") == market_id or str(pair.get("pair", {}).get("base", "")).upper() == ticker.upper():
                if pair.get("market") == market_id or market is None:
                    market = pair
                if pair.get("market") == market_id:
                    break

        if market is None:
            raise ValueError(
                f"Ondo perpetual market not found for ticker {ticker}. "
                f"Expected a market like {market_id}."
            )

        self.config.contract_id = market["market"]
        quote_increment = Decimal(str(market["quoteIncrement"]))
        base_increment = Decimal(str(market["baseIncrement"]))
        if quote_increment <= 0 or base_increment <= 0:
            raise ValueError(f"Invalid market increments for {self.config.contract_id}")

        self.config.tick_size = quote_increment
        self._base_increment = base_increment

        if self.config.quantity < base_increment:
            raise ValueError(
                f"Order quantity is smaller than the market size step: "
                f"{self.config.quantity} < {base_increment} ({self.config.contract_id})"
            )
        return self.config.contract_id, self.config.tick_size

    @query_retry(default_return=(Decimal(0), Decimal(0)))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        if self._bbo_ts and time.monotonic() - self._bbo_ts < 2 and self._best_bid > 0 and self._best_ask > 0:
            return self._best_bid, self._best_ask

        try:
            payload = await self._request(
                "GET",
                "/v1/perps/depth",
                params={"market": contract_id, "depth": 1},
                auth=False,
            )
        except OndoApiError:
            if self._best_bid > 0 and self._best_ask > 0:
                return self._best_bid, self._best_ask
            raise

        book = payload.get("result") or {}
        best_bid = first_level_price(book.get("bids"))
        best_ask = first_level_price(book.get("asks"))
        if best_bid > 0 and best_ask > 0:
            self._best_bid = best_bid
            self._best_ask = best_ask
            self._bbo_ts = time.monotonic()
        return best_bid, best_ask

    async def get_order_price(self, direction: str) -> Decimal:
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0:
            raise ValueError("Invalid bid/ask prices")
        if direction == "buy":
            order_price = best_ask - self.config.tick_size
        else:
            order_price = best_bid + self.config.tick_size
        return self.round_to_tick(order_price)

    def _align_size(self, quantity: Decimal) -> Decimal:
        return align_to_step(Decimal(quantity), self._base_increment, rounding=ROUND_DOWN)

    async def _place_post_only(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        size = self._align_size(quantity)
        if size <= 0:
            return OrderResult(success=False, error_message=f"Order size rounds to zero at step {self._base_increment}")

        rounded_price = self.round_to_tick(price)
        if rounded_price <= 0:
            return OrderResult(success=False, error_message="Order price rounds to zero")

        max_retries = 15
        retryable = {"post_only_has_match"}
        for attempt in range(max_retries):
            try:
                payload = await self._request(
                    "POST",
                    "/v1/perps/orders",
                    body={
                        "market": contract_id,
                        "side": side.lower(),
                        "size": format_decimal(size),
                        "price": format_decimal(rounded_price),
                        "type": "limit",
                        "timeInForce": "GTC",
                        "postOnly": True,
                    },
                )
                order = payload.get("result") or {}
                order_id = order.get("orderId")
                if not order_id:
                    return OrderResult(success=False, error_message="No order ID in response")

                await asyncio.sleep(0.05)
                order_info = await self.get_order_info(order_id)
                status = order_info.status if order_info else map_order_status(
                    order.get("status", "open"),
                    Decimal(str(order.get("size") or size)),
                    Decimal(str(order.get("filledSize") or 0)),
                )
                if status == "CANCELED" and attempt < max_retries - 1:
                    self.logger.log("Post-only order was canceled, retrying", "INFO")
                    best_bid, best_ask = await self.fetch_bbo_prices(contract_id)
                    rounded_price = self._maker_price(side, best_bid, best_ask, rounded_price)
                    continue
                if status not in {"OPEN", "PARTIALLY_FILLED", "FILLED"}:
                    return OrderResult(success=False, error_message=f"Unexpected order status: {status}")
                return OrderResult(
                    success=True,
                    order_id=str(order_id),
                    side=side.lower(),
                    size=size,
                    price=rounded_price,
                    status=status,
                    filled_size=order_info.filled_size if order_info else Decimal(0),
                )
            except OndoApiError as exc:
                if exc.error_code in retryable and attempt < max_retries - 1:
                    self.logger.log(f"Post-only order would cross ({exc.error_code}), retrying", "INFO")
                    best_bid, best_ask = await self.fetch_bbo_prices(contract_id)
                    rounded_price = self._maker_price(side, best_bid, best_ask, rounded_price)
                    await asyncio.sleep(0.1)
                    continue
                return OrderResult(success=False, error_message=f"{exc.error_code or 'error'}: {exc}")
            except Exception as exc:
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.1)
                    continue
                return OrderResult(success=False, error_message=str(exc))

        return OrderResult(success=False, error_message="Max retries exceeded")

    def _maker_price(self, side: str, best_bid: Decimal, best_ask: Decimal, fallback: Decimal) -> Decimal:
        if best_bid <= 0 or best_ask <= 0:
            return self.round_to_tick(fallback)
        if side.lower() == "buy":
            return self.round_to_tick(best_ask - self.config.tick_size)
        return self.round_to_tick(best_bid + self.config.tick_size)

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        best_bid, best_ask = await self.fetch_bbo_prices(contract_id)
        if best_bid <= 0 or best_ask <= 0:
            return OrderResult(success=False, error_message="Invalid bid/ask prices")
        if direction == "buy":
            price = best_ask - self.config.tick_size
        else:
            price = best_bid + self.config.tick_size
        return await self._place_post_only(contract_id, quantity, price, direction)

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        best_bid, best_ask = await self.fetch_bbo_prices(contract_id)
        if best_bid <= 0 or best_ask <= 0:
            return OrderResult(success=False, error_message="Invalid bid/ask prices")

        adjusted = Decimal(price)
        if side.lower() == "sell" and adjusted <= best_bid:
            adjusted = best_bid + self.config.tick_size
        elif side.lower() == "buy" and adjusted >= best_ask:
            adjusted = best_ask - self.config.tick_size
        return await self._place_post_only(contract_id, quantity, adjusted, side)

    async def cancel_order(self, order_id: str) -> OrderResult:
        try:
            payload = await self._request("DELETE", f"/v1/perps/orders/{order_id}")
            order = payload.get("result") or {}
            return OrderResult(
                success=True,
                order_id=str(order.get("orderId") or order_id),
                filled_size=Decimal(str(order.get("filledSize") or 0)),
                status=map_order_status(
                    order.get("status", "canceled"),
                    Decimal(str(order.get("size") or 0)),
                    Decimal(str(order.get("filledSize") or 0)),
                ),
            )
        except OndoApiError as exc:
            if exc.error_code == "order_already_canceled":
                return OrderResult(success=True, order_id=str(order_id), status="CANCELED")
            return OrderResult(success=False, error_message=f"{exc.error_code or 'error'}: {exc}")
        except Exception as exc:
            return OrderResult(success=False, error_message=str(exc))

    def _to_order_info(self, order: Dict[str, Any]) -> OrderInfo:
        size = Decimal(str(order.get("size") or 0))
        filled = Decimal(str(order.get("filledSize") or 0))
        return OrderInfo(
            order_id=str(order.get("orderId") or ""),
            side=str(order.get("side") or "").lower(),
            size=size,
            price=Decimal(str(order.get("price") or 0)),
            status=map_order_status(str(order.get("status") or ""), size, filled),
            filled_size=filled,
            remaining_size=size - filled,
            cancel_reason=str(order.get("cancelReason") or ""),
        )

    @query_retry()
    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        try:
            payload = await self._request("GET", f"/v1/perps/orders/{order_id}")
        except OndoApiError as exc:
            if exc.error_code == "order_not_found":
                return None
            raise
        order = payload.get("result") or {}
        if not order:
            return None
        return self._to_order_info(order)

    @query_retry(default_return=[])
    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        orders: List[OrderInfo] = []
        cursor = None
        for _ in range(10):
            params: Dict[str, Any] = {"market": contract_id, "status": "open", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            payload = await self._request("GET", "/v1/perps/orders", params=params)
            for order in payload.get("result") or []:
                if order.get("market") != contract_id:
                    continue
                if order.get("type") not in (None, "limit"):
                    continue
                orders.append(self._to_order_info(order))
            cursor = (payload.get("pageInfo") or {}).get("nextCursor")
            if not cursor:
                break
        return orders

    @query_retry(default_return=Decimal(0))
    async def get_account_positions(self) -> Decimal:
        payload = await self._request("GET", "/v1/perps/positions")
        for position in payload.get("result") or []:
            if position.get("market") != self.config.contract_id:
                continue
            if str(position.get("direction") or "").lower() == "neutral":
                return Decimal(0)
            return abs(Decimal(str(position.get("netQuantity") or 0)))
        return Decimal(0)

    async def _run_ws(self) -> None:
        backoff = 1.0
        while not self._ws_stop.is_set():
            try:
                async with websockets.connect(
                    self.ws_url,
                    ssl=self._ssl_context(),
                    # Ondo keeps the socket alive with {"op":"ping"}, not protocol pings.
                    ping_interval=None,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    await self._login_and_subscribe(ws)
                    self._ws_ready.set()
                    backoff = 1.0
                    self.logger.log("[WS] connected", "INFO")
                    await self._consume_ws(ws)
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, OndoApiError, ConnectionError, OSError) as exc:
                self._ws_ready.clear()
                if not self._ws_stop.is_set():
                    self.logger.log(f"[WS] disconnected ({exc}); reconnecting in {backoff:.0f}s", "WARNING")
            except Exception as exc:
                self._ws_ready.clear()
                if not self._ws_stop.is_set():
                    self.logger.log(f"[WS] connection error: {exc}; reconnecting in {backoff:.0f}s", "ERROR")
                    self.logger.log(traceback.format_exc(), "DEBUG")
            finally:
                self._ws = None

            if self._ws_stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _login_and_subscribe(self, ws) -> None:
        timestamp = str(int(time.time() * 1000))
        await ws.send(json.dumps({
            "op": "login",
            "args": {
                "key": self.key_id,
                "time": timestamp,
                "sign": sign_ondo_ws_login(self.api_secret, timestamp),
            },
        }))
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            message = json.loads(raw)
            msg_type = message.get("type")
            if msg_type == "loggedIn":
                break
            if msg_type == "error":
                raise OndoApiError(message.get("msg") or "WebSocket login failed")
        else:
            raise OndoApiError("WebSocket login timed out")

        market = self.config.contract_id
        markets = [market] if market else None
        subscribe = {"op": "subscribe", "channel": "ordersPerps"}
        book = {"op": "subscribe", "channel": "topOfBooksPerps"}
        if markets:
            subscribe["markets"] = markets
            book["markets"] = markets
        await ws.send(json.dumps(subscribe))
        await ws.send(json.dumps(book))

    async def _consume_ws(self, ws) -> None:
        last_msg = time.monotonic()
        while not self._ws_stop.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=20)
            except asyncio.TimeoutError:
                if time.monotonic() - last_msg > 90:
                    raise ConnectionError("no WebSocket message for 90s")
                await ws.send(json.dumps({"op": "ping"}))
                continue
            last_msg = time.monotonic()
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            msg_type = message.get("type")
            if msg_type in {"pong", "subscribed", "loggedIn"}:
                continue
            if msg_type == "error":
                self.logger.log(f"[WS] {message.get('msg') or message}", "ERROR")
                continue
            if msg_type != "update":
                continue
            channel = message.get("channel")
            data = message.get("data")
            if channel == "ordersPerps":
                self._handle_order_update(data)
            elif channel == "topOfBooksPerps":
                self._handle_book_update(data)

    def _handle_book_update(self, data: Any) -> None:
        snapshots = data if isinstance(data, list) else [data]
        market = self.config.contract_id
        for snapshot in snapshots:
            if not isinstance(snapshot, dict):
                continue
            if market and snapshot.get("market") != market:
                continue
            bid = first_level_price(snapshot.get("bids"))
            ask = first_level_price(snapshot.get("asks"))
            if bid > 0 and ask > 0:
                self._best_bid = bid
                self._best_ask = ask
                self._bbo_ts = time.monotonic()

    def _handle_order_update(self, data: Any) -> None:
        orders = data if isinstance(data, list) else [data]
        for order in orders:
            if not isinstance(order, dict):
                continue
            if order.get("market") != self.config.contract_id:
                continue
            if order.get("type") not in (None, "limit"):
                continue
            info = self._to_order_info(order)
            if info.status not in {"OPEN", "PARTIALLY_FILLED", "FILLED", "CANCELED"}:
                continue
            side = info.side
            order_type = "CLOSE" if side == self.config.close_order_side else "OPEN"
            if info.status == "CANCELED" and order_type == "CLOSE":
                continue
            if self._order_update_handler is None:
                continue
            self._order_update_handler({
                "order_id": info.order_id,
                "side": side,
                "order_type": order_type,
                "status": info.status,
                "size": format_decimal(info.size),
                "price": format_decimal(info.price),
                "contract_id": order.get("market"),
                "filled_size": format_decimal(info.filled_size),
            })
