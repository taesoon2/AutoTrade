from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import socket
import ssl
import struct
import threading
import time
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from decimal import InvalidOperation
from json import JSONDecodeError
from pathlib import Path
from typing import Final
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.request import Request
from urllib.request import urlopen
from zoneinfo import ZoneInfo

from autotrade.broker.exceptions import BrokerNormalizationError
from autotrade.broker.normalization import normalize_holding
from autotrade.broker.normalization import normalize_order_capacity
from autotrade.broker.normalization import normalize_quote
from autotrade.broker.readers import BrokerReader
from autotrade.broker.trading import BrokerTrader
from autotrade.common import AccountPerformance
from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import Holding
from autotrade.common import HoldingPerformance
from autotrade.common import OrderAmendRequest
from autotrade.common import OrderCapacity
from autotrade.common import OrderCancelRequest
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.common import OrderType
from autotrade.common import Quote
from autotrade.common.price_ticks import is_valid_krx_symbol_order_price
from autotrade.common.price_ticks import normalize_krx_symbol_order_price
from autotrade.config.models import BrokerSettings
from autotrade.data import Bar
from autotrade.data import KRX_SESSION_CLOSE
from autotrade.data import KRX_SESSION_OPEN
from autotrade.data import Timeframe

PAPER_BASE_URL: Final[str] = "https://openapivts.koreainvestment.com:29443"
LIVE_BASE_URL: Final[str] = "https://openapi.koreainvestment.com:9443"
KST: Final[ZoneInfo] = ZoneInfo("Asia/Seoul")
LIVE_ACCOUNT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^\d{8}$|^\d{8}-\d{2}$|^\d{10}$"
)
PAPER_ACCOUNT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^\d{8}$|^\d{8}-\d{2}$|^\d{10}$"
)
PAPER_PRODUCT_CODE: Final[str] = "01"
TOKEN_CACHE_REFRESH_BUFFER: Final[timedelta] = timedelta(minutes=1)
KIS_TOKEN_PATH: Final[str] = "/oauth2/tokenP"
KIS_APPROVAL_PATH: Final[str] = "/oauth2/Approval"
KIS_HASHKEY_PATH: Final[str] = "/uapi/hashkey"
KIS_QUOTE_PATH: Final[str] = "/uapi/domestic-stock/v1/quotations/inquire-price"
KIS_DAILY_CHART_PATH: Final[str] = (
    "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
)
KIS_INTRADAY_CHART_PATH: Final[str] = (
    "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice"
)
KIS_BALANCE_PATH: Final[str] = "/uapi/domestic-stock/v1/trading/inquire-balance"
KIS_ORDER_CAPACITY_PATH: Final[str] = (
    "/uapi/domestic-stock/v1/trading/inquire-psbl-order"
)
KIS_ORDER_PATH: Final[str] = "/uapi/domestic-stock/v1/trading/order-cash"
KIS_ORDER_AMEND_CANCEL_PATH: Final[str] = (
    "/uapi/domestic-stock/v1/trading/order-rvsecncl"
)
KIS_AMENDABLE_ORDER_PATH: Final[str] = (
    "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"
)
KIS_ORDER_HISTORY_PATH: Final[str] = (
    "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
)
KIS_LIMIT_ORDER_DIVISION: Final[str] = "00"
KIS_PAPER_MIN_REQUEST_INTERVAL_SECONDS: Final[float] = 1.1
KIS_DEFAULT_MIN_REQUEST_INTERVAL_SECONDS: Final[float] = (
    KIS_PAPER_MIN_REQUEST_INTERVAL_SECONDS
)
KIS_LIVE_MAX_REQUESTS_PER_SECOND: Final[float] = 15.0
KIS_LIVE_MIN_REQUEST_INTERVAL_SECONDS: Final[float] = (
    1.0 / KIS_LIVE_MAX_REQUESTS_PER_SECOND
)
KIS_RATE_LIMIT_MSG_CODE: Final[str] = "EGW00201"
KIS_RATE_LIMIT_RETRY_DELAYS_SECONDS: Final[tuple[float, ...]] = (1.5, 3.0, 5.0)
KIS_TRANSPORT_RETRY_DELAYS_SECONDS: Final[tuple[float, ...]] = (1.0, 2.0, 4.0)
KIS_ORDER_HISTORY_LOOKUP_DELAY_MULTIPLIERS: Final[tuple[int, ...]] = (1, 2, 3)
KIS_DAILY_CHART_PAGE_SIZE: Final[int] = 100
KIS_INTRADAY_CHART_PAGE_SIZE: Final[int] = 120
KIS_INTRADAY_MAX_PAGE_COUNT: Final[int] = 64
KIS_CUMULATIVE_FILL_ID_SUFFIX: Final[str] = ":cumulative"
KIS_TOKEN_EXPIRED_MSG_CODE: Final[str] = "EGW00123"
KIS_WS_LIVE_URL: Final[str] = "ws://ops.koreainvestment.com:21000/tryitout"
KIS_WS_PAPER_URL: Final[str] = "ws://ops.koreainvestment.com:31000/tryitout"
KIS_LIVE_CCNL_NOTICE_TR_ID: Final[str] = "H0STCNI0"
KIS_PAPER_CCNL_NOTICE_TR_ID: Final[str] = "H0STCNI9"
KIS_WS_SUBSCRIBE_TR_TYPE: Final[str] = "1"
KIS_WS_CUSTOMER_TYPE: Final[str] = "P"
KIS_WS_TIMEOUT_SECONDS: Final[float] = 5.0
KIS_WS_RECONNECT_DELAY_SECONDS: Final[tuple[float, ...]] = (1.0, 2.0, 5.0)
KIS_CCNL_NOTICE_COLUMNS: Final[tuple[str, ...]] = (
    "CUST_ID",
    "ACNT_NO",
    "ODER_NO",
    "OODER_NO",
    "SELN_BYOV_CLS",
    "RCTF_CLS",
    "ODER_KIND",
    "ODER_COND",
    "STCK_SHRN_ISCD",
    "CNTG_QTY",
    "CNTG_UNPR",
    "STCK_CNTG_HOUR",
    "RFUS_YN",
    "CNTG_YN",
    "ACPT_YN",
    "BRNC_NO",
    "ODER_QTY",
    "ACNT_NAME",
    "ORD_COND_PRC",
    "ORD_EXG_GB",
    "POPUP_YN",
    "FILLER",
    "CRDT_CLS",
    "CRDT_LOAN_DATE",
    "CNTG_ISNM40",
    "ODER_PRC",
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _OrderHistoryLookup:
    records: tuple[Mapping[str, object], ...]
    summary: Mapping[str, object] | None


@dataclass(frozen=True, slots=True)
class HttpRequest:
    method: str
    url: str
    headers: Mapping[str, str]
    body: bytes | None = None
    timeout: float = 30.0


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


class KoreaInvestmentBrokerError(RuntimeError):
    """Raised when the Korea Investment HTTP API cannot be used safely."""


HttpTransport = Callable[[HttpRequest], HttpResponse]
WebSocketConnector = Callable[[str], "_WebSocketConnection"]


@dataclass(frozen=True, slots=True)
class CachedAccessToken:
    access_token: str
    expires_at: datetime | None


class _WebSocketConnection:
    _GUID: Final[str] = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def __init__(
        self,
        url: str,
        *,
        timeout_seconds: float = KIS_WS_TIMEOUT_SECONDS,
    ) -> None:
        parsed = urlsplit(url)
        if parsed.scheme not in {"ws", "wss"}:
            raise KoreaInvestmentBrokerError(
                f"unsupported websocket scheme: {parsed.scheme}"
            )
        if parsed.hostname is None:
            raise KoreaInvestmentBrokerError("websocket url missing hostname")

        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        raw_socket = socket.create_connection(
            (parsed.hostname, port),
            timeout=timeout_seconds,
        )
        connected_socket: socket.socket
        if parsed.scheme == "wss":
            context = ssl.create_default_context()
            connected_socket = context.wrap_socket(
                raw_socket,
                server_hostname=parsed.hostname,
            )
        else:
            connected_socket = raw_socket
        self._socket = connected_socket
        self._socket.settimeout(timeout_seconds)
        self._buffer = bytearray()
        self._closed = False
        self._handshake(parsed)

    def send_text(self, payload: str) -> None:
        self._send_frame(0x1, payload.encode("utf-8"))

    def send_pong(self, payload: bytes = b"") -> None:
        self._send_frame(0xA, payload)

    def receive_text(self, *, timeout_seconds: float | None = None) -> str:
        if timeout_seconds is not None:
            self._socket.settimeout(timeout_seconds)

        fragments: list[bytes] = []
        expecting_continuation = False
        while True:
            fin, opcode, payload = self._read_frame()
            if opcode == 0x0:
                if not expecting_continuation:
                    raise KoreaInvestmentBrokerError(
                        "unexpected websocket continuation frame"
                    )
                fragments.append(payload)
                if fin:
                    return b"".join(fragments).decode("utf-8")
                continue
            if opcode == 0x1:
                if fin:
                    return payload.decode("utf-8")
                fragments = [payload]
                expecting_continuation = True
                continue
            if opcode == 0x8:
                raise ConnectionError("websocket connection closed by server")
            if opcode == 0x9:
                self.send_pong(payload)
                continue
            if opcode == 0xA:
                continue
            raise KoreaInvestmentBrokerError(f"unsupported websocket opcode: {opcode}")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._send_frame(0x8, b"")
        except OSError:
            pass
        finally:
            self._socket.close()

    def _handshake(self, parsed_url) -> None:
        path = parsed_url.path or "/"
        if parsed_url.query:
            path = f"{path}?{parsed_url.query}"
        host = parsed_url.netloc
        websocket_key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = "\r\n".join(
            (
                f"GET {path} HTTP/1.1",
                f"Host: {host}",
                "Upgrade: websocket",
                "Connection: Upgrade",
                f"Sec-WebSocket-Key: {websocket_key}",
                "Sec-WebSocket-Version: 13",
                "",
                "",
            )
        )
        self._socket.sendall(request.encode("ascii"))

        response = self._read_http_response()
        if not response.startswith("HTTP/1.1 101 "):
            raise KoreaInvestmentBrokerError(
                f"websocket handshake failed: {response.splitlines()[0]}"
            )

        header_lines = response.split("\r\n")[1:]
        headers: dict[str, str] = {}
        for line in header_lines:
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            headers[key.strip().lower()] = value.strip()

        accept_key = headers.get("sec-websocket-accept")
        expected_accept = base64.b64encode(
            hashlib.sha1((websocket_key + self._GUID).encode("ascii")).digest()
        ).decode("ascii")
        if accept_key != expected_accept:
            raise KoreaInvestmentBrokerError(
                "websocket handshake returned invalid Sec-WebSocket-Accept"
            )

    def _read_http_response(self) -> str:
        while b"\r\n\r\n" not in self._buffer:
            chunk = self._socket.recv(4096)
            if not chunk:
                raise ConnectionError("websocket closed during handshake")
            self._buffer.extend(chunk)
        header_bytes, _, remainder = self._buffer.partition(b"\r\n\r\n")
        self._buffer = bytearray(remainder)
        return header_bytes.decode("utf-8", errors="replace")

    def _read_frame(self) -> tuple[bool, int, bytes]:
        first_byte, second_byte = self._read_exact(2)
        fin = bool(first_byte & 0x80)
        opcode = first_byte & 0x0F
        masked = bool(second_byte & 0x80)
        payload_length = second_byte & 0x7F
        if payload_length == 126:
            payload_length = struct.unpack("!H", self._read_exact(2))[0]
        elif payload_length == 127:
            payload_length = struct.unpack("!Q", self._read_exact(8))[0]
        mask_key = self._read_exact(4) if masked else b""
        payload = self._read_exact(payload_length)
        if masked:
            payload = bytes(
                value ^ mask_key[index % 4] for index, value in enumerate(payload)
            )
        return fin, opcode, payload

    def _read_exact(self, size: int) -> bytes:
        while len(self._buffer) < size:
            chunk = self._socket.recv(4096)
            if not chunk:
                raise ConnectionError("websocket connection closed")
            self._buffer.extend(chunk)
        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self._closed and opcode != 0x8:
            raise ConnectionError("websocket connection is already closed")
        header = bytearray([0x80 | (opcode & 0x0F)])
        payload_length = len(payload)
        if payload_length < 126:
            header.append(0x80 | payload_length)
        elif payload_length < 65536:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", payload_length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", payload_length))

        mask_key = os.urandom(4)
        masked_payload = bytes(
            value ^ mask_key[index % 4] for index, value in enumerate(payload)
        )
        self._socket.sendall(bytes(header) + mask_key + masked_payload)


@dataclass(slots=True)
class _RealtimeNoticeCryptoState:
    encrypted: bool = False
    key: str | None = None
    iv: str | None = None


class _RealtimeFillCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._fills_by_order: dict[str, dict[str, ExecutionFill]] = {}

    def add(self, fill: ExecutionFill) -> None:
        order_id = _normalize_order_identifier(fill.order_id)
        with self._lock:
            fills = self._fills_by_order.setdefault(order_id, {})
            fills[fill.fill_id] = fill

    def get(self, order_id: str) -> tuple[ExecutionFill, ...]:
        normalized_order_id = _normalize_order_identifier(order_id)
        with self._lock:
            fills = tuple(self._fills_by_order.get(normalized_order_id, {}).values())
        return tuple(
            sorted(
                (replace(fill, order_id=order_id) for fill in fills),
                key=lambda fill: (fill.filled_at, fill.fill_id),
            )
        )


class _KoreaInvestmentRealtimeFillListener:
    def __init__(
        self,
        settings: BrokerSettings,
        *,
        request_approval_key: Callable[[], str],
        fill_cache: _RealtimeFillCache,
        websocket_connector: WebSocketConnector,
        clock: Callable[[], datetime],
        sleep: Callable[[float], None],
    ) -> None:
        if settings.hts_id is None:
            raise ValueError("hts_id is required for realtime fill listener")
        self._request_approval_key = request_approval_key
        self._fill_cache = fill_cache
        self._websocket_connector = websocket_connector
        self._clock = clock
        self._sleep = sleep
        self._subscription_key = settings.hts_id
        self._websocket_url = (
            KIS_WS_PAPER_URL if settings.environment == "paper" else KIS_WS_LIVE_URL
        )
        self._tr_id = (
            KIS_PAPER_CCNL_NOTICE_TR_ID
            if settings.environment == "paper"
            else KIS_LIVE_CCNL_NOTICE_TR_ID
        )
        self._thread_lock = threading.Lock()
        self._active_connection_lock = threading.Lock()
        self._active_connection: _WebSocketConnection | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def ensure_started(self) -> None:
        if self._stop_event.is_set():
            return
        with self._thread_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run,
                name="kis-realtime-fill-listener",
                daemon=True,
            )
            self._thread.start()

    def close(self) -> None:
        self._stop_event.set()
        with self._active_connection_lock:
            connection = self._active_connection
            self._active_connection = None
        if connection is not None:
            connection.close()
        if self._thread is not None:
            self._thread.join(timeout=1.0)

    def _run(self) -> None:
        delay_index = 0
        while not self._stop_event.is_set():
            connection: _WebSocketConnection | None = None
            try:
                approval_key = self._request_approval_key()
                connection = self._websocket_connector(self._websocket_url)
                with self._active_connection_lock:
                    self._active_connection = connection
                connection.send_text(
                    _build_fill_notice_subscription_message(
                        approval_key=approval_key,
                        tr_id=self._tr_id,
                        tr_key=self._subscription_key,
                    )
                )
                crypto_state = _RealtimeNoticeCryptoState()
                delay_index = 0
                while not self._stop_event.is_set():
                    try:
                        message = connection.receive_text(timeout_seconds=1.0)
                    except TimeoutError:
                        continue
                    except socket.timeout:
                        continue
                    self._handle_message(connection, message, crypto_state)
            except Exception as error:
                if self._stop_event.is_set():
                    break
                logger.warning(
                    "KIS realtime fill listener disconnected, falling back to REST polling. reason=%s",
                    error,
                )
                delay_seconds = KIS_WS_RECONNECT_DELAY_SECONDS[
                    min(delay_index, len(KIS_WS_RECONNECT_DELAY_SECONDS) - 1)
                ]
                delay_index += 1
                self._sleep(delay_seconds)
            finally:
                with self._active_connection_lock:
                    active_connection = self._active_connection
                    self._active_connection = None
                if active_connection is not None:
                    active_connection.close()
                elif connection is not None:
                    connection.close()

    def _handle_message(
        self,
        connection: _WebSocketConnection,
        message: str,
        crypto_state: _RealtimeNoticeCryptoState,
    ) -> None:
        if message[:1] in {"0", "1"} and "|" in message:
            self._handle_data_message(message, crypto_state)
            return

        try:
            payload = json.loads(message)
        except JSONDecodeError:
            logger.debug("Ignoring non-JSON websocket system message: %s", message)
            return
        if not isinstance(payload, Mapping):
            return

        header = payload.get("header")
        if not isinstance(header, Mapping):
            return
        tr_id = _optional_output_string(header.get("tr_id"))
        if tr_id == "PINGPONG":
            connection.send_pong(message.encode("utf-8"))
            return
        if tr_id != self._tr_id:
            return

        crypto_state.encrypted = _optional_output_string(header.get("encrypt")) == "Y"
        body = payload.get("body")
        if not isinstance(body, Mapping):
            return
        output = body.get("output")
        if isinstance(output, Mapping):
            key = _optional_output_string(output.get("key"))
            iv = _optional_output_string(output.get("iv"))
            if key is not None:
                crypto_state.key = key
            if iv is not None:
                crypto_state.iv = iv
        if body.get("rt_cd") not in {None, "0"}:
            logger.warning(
                "KIS realtime fill subscription was not acknowledged. rt_cd=%s msg=%s",
                body.get("rt_cd"),
                body.get("msg1"),
            )

    def _handle_data_message(
        self,
        message: str,
        crypto_state: _RealtimeNoticeCryptoState,
    ) -> None:
        prefix, tr_id, _, payload = message.split("|", 3)
        if prefix not in {"0", "1"} or tr_id != self._tr_id:
            return

        decoded_payload = payload
        if crypto_state.encrypted:
            if crypto_state.key is None or crypto_state.iv is None:
                logger.warning(
                    "Ignoring encrypted KIS fill notice before key exchange completes."
                )
                return
            decoded_payload = _decrypt_fill_notice_payload(
                key=crypto_state.key,
                iv=crypto_state.iv,
                cipher_text=payload,
            )
        for fill in _parse_realtime_fill_notices(
            decoded_payload,
            fallback=self._clock(),
        ):
            self._fill_cache.add(fill)


class _RequestThrottle:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_allowed_at = 0.0

    def wait(
        self,
        *,
        monotonic: Callable[[], float],
        sleep: Callable[[float], None],
        min_interval_seconds: float,
    ) -> None:
        if min_interval_seconds <= 0:
            return

        with self._lock:
            current = monotonic()
            scheduled_at = max(current, self._next_allowed_at)
            self._next_allowed_at = scheduled_at + min_interval_seconds

        delay_seconds = scheduled_at - current
        if delay_seconds > 0:
            sleep(delay_seconds)


_REQUEST_THROTTLES: dict[str, _RequestThrottle] = {}
_REQUEST_THROTTLES_LOCK = threading.Lock()
_RAW_LOG_LOCK = threading.Lock()


class _KoreaInvestmentApiClient:
    def __init__(
        self,
        settings: BrokerSettings,
        *,
        transport: HttpTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        token_cache_path: Path | None = None,
        sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] | None = None,
        min_request_interval_seconds: float | None = None,
    ) -> None:
        self._settings = settings
        self._transport = transport or _urllib_transport
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sleep = sleep or time.sleep
        self._monotonic = monotonic or time.monotonic
        self._access_token: str | None = None
        self._access_token_expires_at: datetime | None = None
        self._base_url = (
            PAPER_BASE_URL if settings.environment == "paper" else LIVE_BASE_URL
        )
        self._token_cache_path = token_cache_path or _default_token_cache_path(settings)
        self._raw_log_dir = _resolve_raw_log_directory(settings)
        self._min_request_interval_seconds = _resolve_min_request_interval_seconds(
            settings=settings,
            transport=transport,
            explicit_seconds=min_request_interval_seconds,
        )
        self._order_history_lookup_delays_seconds = (
            _resolve_order_history_lookup_delays_seconds(
                settings=settings,
                explicit_seconds=min_request_interval_seconds,
            )
        )
        self._request_throttle = (
            _request_throttle_for(settings)
            if self._min_request_interval_seconds > 0
            else None
        )
        self._cano, self._account_product_code = _split_account(
            settings.account,
            environment=settings.environment,
        )

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        body: Mapping[str, object] | None = None,
        tr_id: str | None = None,
        require_hashkey: bool = False,
    ) -> Mapping[str, object]:
        method_name = method.upper()
        allow_token_refresh_retry = path != KIS_TOKEN_PATH
        request_body = (
            json.dumps(body, separators=(",", ":")).encode("utf-8")
            if body is not None
            else None
        )

        token_refresh_attempted = False
        rate_limit_retry_index = 0
        transport_retry_index = 0
        while True:
            request = HttpRequest(
                method=method_name,
                url=_build_url(self._base_url, path, params),
                headers=self._build_headers(
                    method=method_name,
                    path=path,
                    tr_id=tr_id,
                    hashkey_body=body if require_hashkey else None,
                ),
                body=request_body,
            )
            self._wait_for_request_slot()
            try:
                response = self._transport(request)
            except TimeoutError as error:
                next_retry_index = self._maybe_retry_transport_failure(
                    method=method_name,
                    path=path,
                    url=request.url,
                    message="request timed out",
                    detail=error,
                    retry_index=transport_retry_index,
                )
                if next_retry_index is not None:
                    transport_retry_index = next_retry_index
                    continue
                raise KoreaInvestmentBrokerError(
                    _format_transport_failure("request timed out", error)
                ) from error
            except URLError as error:
                next_retry_index = self._maybe_retry_transport_failure(
                    method=method_name,
                    path=path,
                    url=request.url,
                    message="network request failed",
                    detail=error.reason,
                    retry_index=transport_retry_index,
                )
                if next_retry_index is not None:
                    transport_retry_index = next_retry_index
                    continue
                raise KoreaInvestmentBrokerError(
                    _format_transport_failure("network request failed", error.reason)
                ) from error
            except ConnectionError as error:
                next_retry_index = self._maybe_retry_transport_failure(
                    method=method_name,
                    path=path,
                    url=request.url,
                    message="network request failed",
                    detail=error,
                    retry_index=transport_retry_index,
                )
                if next_retry_index is not None:
                    transport_retry_index = next_retry_index
                    continue
                raise KoreaInvestmentBrokerError(
                    _format_transport_failure("network request failed", error)
                ) from error

            self._write_raw_http_log(request=request, response=response)
            payload = _try_decode_json(response.body)
            if (
                allow_token_refresh_retry
                and not token_refresh_attempted
                and _is_token_expired_payload(payload)
            ):
                logger.warning(
                    "KIS access token expired during request, refreshing and retrying. method=%s path=%s",
                    method_name,
                    path,
                )
                self._invalidate_access_token()
                token_refresh_attempted = True
                continue

            if _is_rate_limit_payload(payload) and rate_limit_retry_index < len(
                KIS_RATE_LIMIT_RETRY_DELAYS_SECONDS
            ):
                delay_seconds = KIS_RATE_LIMIT_RETRY_DELAYS_SECONDS[
                    rate_limit_retry_index
                ]
                rate_limit_retry_index += 1
                logger.warning(
                    "KIS request hit rate limit, retrying after delay. method=%s url=%s delay_seconds=%.1f",
                    method_name,
                    _build_url(self._base_url, path, params),
                    delay_seconds,
                )
                self._sleep(delay_seconds)
                continue

            if response.status >= 400:
                raise KoreaInvestmentBrokerError(
                    _format_http_error(
                        status=response.status,
                        method=request.method,
                        url=request.url,
                        body=response.body,
                    )
                )

            if payload is None:
                raise KoreaInvestmentBrokerError("response body is not valid JSON")
            _raise_for_api_error(payload)
            return payload

    def _maybe_retry_transport_failure(
        self,
        *,
        method: str,
        path: str,
        url: str,
        message: str,
        detail: object,
        retry_index: int,
    ) -> int | None:
        if not _is_retryable_transport_request(method, path):
            return None
        if retry_index >= len(KIS_TRANSPORT_RETRY_DELAYS_SECONDS):
            return None

        delay_seconds = KIS_TRANSPORT_RETRY_DELAYS_SECONDS[retry_index]
        logger.warning(
            "KIS request transport failure, retrying after delay. method=%s url=%s delay_seconds=%.1f error=%s",
            method,
            url,
            delay_seconds,
            _format_transport_failure(message, detail),
        )
        self._sleep(delay_seconds)
        return retry_index + 1

    def _write_raw_http_log(
        self,
        *,
        request: HttpRequest,
        response: HttpResponse,
    ) -> None:
        if self._raw_log_dir is None:
            return

        log_directory = self._raw_log_dir
        log_path = log_directory / f"kis_raw_{self._clock().astimezone(KST):%Y%m%d}.log"
        entry = {
            "timestamp": self._clock().astimezone(KST).isoformat(),
            "method": request.method,
            "url": _sanitize_url_for_log(request.url),
            "request_headers": _sanitize_headers_for_log(request.headers),
            "request_body": _decode_body_for_log(request.body),
            "response_status": response.status,
            "response_headers": _sanitize_headers_for_log(response.headers),
            "response_body": _decode_body_for_log(response.body),
        }
        try:
            log_directory.mkdir(parents=True, exist_ok=True)
            with _RAW_LOG_LOCK:
                with log_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(entry, ensure_ascii=False))
                    handle.write("\n")
        except OSError:
            return

    def _wait_for_request_slot(self) -> None:
        if self._request_throttle is None:
            return
        self._request_throttle.wait(
            monotonic=self._monotonic,
            sleep=self._sleep,
            min_interval_seconds=self._min_request_interval_seconds,
        )

    def _build_headers(
        self,
        *,
        method: str,
        path: str,
        tr_id: str | None,
        hashkey_body: Mapping[str, object] | None,
    ) -> dict[str, str]:
        headers: dict[str, str] = {
            "content-type": "application/json; charset=utf-8",
            "appkey": self._settings.api_key,
            "appsecret": self._settings.api_secret,
        }
        if tr_id is not None:
            headers["tr_id"] = tr_id
        if path not in {KIS_TOKEN_PATH, KIS_APPROVAL_PATH}:
            headers["authorization"] = f"Bearer {self._get_access_token()}"
            headers["custtype"] = "P"
        if hashkey_body is not None:
            headers["hashkey"] = self._request_hashkey(hashkey_body)
        if method != "POST":
            headers["custtype"] = "P"
        return headers

    def _request_hashkey(self, body: Mapping[str, object]) -> str:
        payload = self._request_json("POST", KIS_HASHKEY_PATH, body=body)
        hashkey = payload.get("HASH")
        if not isinstance(hashkey, str) or not hashkey.strip():
            raise KoreaInvestmentBrokerError("hashkey response missing HASH")
        return hashkey.strip()

    def _request_approval_key(self) -> str:
        payload = self._request_json(
            "POST",
            KIS_APPROVAL_PATH,
            body={
                "grant_type": "client_credentials",
                "appkey": self._settings.api_key,
                "secretkey": self._settings.api_secret,
            },
        )
        approval_key = payload.get("approval_key")
        if not isinstance(approval_key, str) or not approval_key.strip():
            raise KoreaInvestmentBrokerError("approval response missing approval_key")
        return approval_key.strip()

    def _get_access_token(self) -> str:
        if self._has_usable_access_token():
            assert self._access_token is not None
            return self._access_token

        cached_token = self._load_access_token_from_cache()
        if cached_token is not None:
            self._access_token = cached_token.access_token
            self._access_token_expires_at = cached_token.expires_at
            return cached_token.access_token

        payload = self._request_json(
            "POST",
            KIS_TOKEN_PATH,
            body={
                "grant_type": "client_credentials",
                "appkey": self._settings.api_key,
                "appsecret": self._settings.api_secret,
            },
        )
        access_token = payload.get("access_token")
        if not isinstance(access_token, str) or not access_token.strip():
            raise KoreaInvestmentBrokerError("token response missing access_token")

        self._access_token = access_token.strip()
        self._access_token_expires_at = _resolve_access_token_expiration(
            payload,
            now=self._clock(),
        )
        self._write_access_token_cache(
            CachedAccessToken(
                access_token=self._access_token,
                expires_at=self._access_token_expires_at,
            )
        )
        return self._access_token

    def _has_usable_access_token(self) -> bool:
        if self._access_token is None:
            return False
        if self._access_token_expires_at is None:
            return True
        return not _is_token_expired(
            self._access_token_expires_at,
            now=self._clock(),
        )

    def _invalidate_access_token(self) -> None:
        self._access_token = None
        self._access_token_expires_at = None
        try:
            self._token_cache_path.unlink()
        except FileNotFoundError:
            return
        except OSError:
            return

    def _load_access_token_from_cache(self) -> CachedAccessToken | None:
        try:
            raw_cache = self._token_cache_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError:
            return None

        try:
            decoded = json.loads(raw_cache)
        except JSONDecodeError:
            return None

        if not isinstance(decoded, Mapping):
            return None

        access_token = decoded.get("access_token")
        if not isinstance(access_token, str) or not access_token.strip():
            return None

        expires_at = _parse_cached_datetime(decoded.get("expires_at"))
        if expires_at is None or _is_token_expired(expires_at, now=self._clock()):
            return None

        return CachedAccessToken(
            access_token=access_token.strip(),
            expires_at=expires_at,
        )

    def _write_access_token_cache(self, cached_token: CachedAccessToken) -> None:
        if cached_token.expires_at is None:
            return

        payload = {
            "access_token": cached_token.access_token,
            "expires_at": cached_token.expires_at.isoformat(),
        }
        cache_directory = self._token_cache_path.parent
        temporary_path = self._token_cache_path.with_suffix(".tmp")

        try:
            cache_directory.mkdir(parents=True, exist_ok=True)
            temporary_path.write_text(
                json.dumps(payload, separators=(",", ":")),
                encoding="utf-8",
            )
            temporary_path.replace(self._token_cache_path)
        except OSError:
            return


class KoreaInvestmentBrokerReader(_KoreaInvestmentApiClient, BrokerReader):
    def __init__(
        self,
        settings: BrokerSettings,
        *,
        transport: HttpTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        token_cache_path: Path | None = None,
        sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] | None = None,
        min_request_interval_seconds: float | None = None,
    ) -> None:
        super().__init__(
            settings,
            transport=transport,
            clock=clock,
            token_cache_path=token_cache_path,
            sleep=sleep,
            monotonic=monotonic,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        self._quote_tr_id = "FHKST01010100"
        self._balance_tr_id = (
            "VTTC8434R" if settings.environment == "paper" else "TTTC8434R"
        )
        self._order_capacity_tr_id = (
            "VTTC8908R" if settings.environment == "paper" else "TTTC8908R"
        )

    def get_quote(self, symbol: str) -> Quote:
        try:
            payload = self._request_json(
                "GET",
                KIS_QUOTE_PATH,
                params={
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD": symbol,
                },
                tr_id=self._quote_tr_id,
            )
            output = _require_mapping(payload, "output")
            normalized_payload = {
                "symbol": symbol,
                "price": output.get("stck_prpr"),
                "as_of": _resolve_as_of(output, self._clock()),
                "currency": "KRW",
            }
            return normalize_quote(normalized_payload)
        except BrokerNormalizationError as error:
            raise KoreaInvestmentBrokerError(str(error)) from error

    def get_holdings(self) -> tuple[Holding, ...]:
        try:
            return _parse_balance_holdings(self._request_balance_payload())
        except BrokerNormalizationError as error:
            raise KoreaInvestmentBrokerError(str(error)) from error

    def get_account_performance(self) -> AccountPerformance:
        try:
            payload = self._request_balance_payload()
            rows = _require_sequence(payload, "output1")
            summary = _balance_summary_mapping(payload)
            holding_performance_list: list[HoldingPerformance] = []
            for row in rows:
                if not isinstance(row, Mapping):
                    raise KoreaInvestmentBrokerError("output1 entries must be mappings")
                performance = _parse_holding_performance(row)
                if performance is not None:
                    holding_performance_list.append(performance)
            holding_performances = tuple(
                sorted(
                    holding_performance_list,
                    key=lambda holding: holding.symbol,
                )
            )
            return AccountPerformance(
                total_purchase_amount=_summary_decimal(
                    summary,
                    "pchs_amt_smtl_amt",
                    fallback=sum(
                        (
                            holding.purchase_amount
                            for holding in holding_performances
                        ),
                        start=Decimal("0"),
                    ),
                ),
                total_evaluation_amount=_summary_decimal(
                    summary,
                    "evlu_amt_smtl_amt",
                    "scts_evlu_amt",
                    fallback=sum(
                        (
                            holding.evaluation_amount
                            for holding in holding_performances
                        ),
                        start=Decimal("0"),
                    ),
                ),
                total_profit_loss=_summary_decimal(
                    summary,
                    "evlu_pfls_smtl_amt",
                    fallback=sum(
                        (holding.profit_loss for holding in holding_performances),
                        start=Decimal("0"),
                    ),
                ),
                total_profit_loss_rate=_summary_decimal(
                    summary,
                    "evlu_pfls_rt",
                    fallback=_calculate_profit_loss_rate(
                        sum(
                            (
                                holding.profit_loss
                                for holding in holding_performances
                            ),
                            start=Decimal("0"),
                        ),
                        sum(
                            (
                                holding.purchase_amount
                                for holding in holding_performances
                            ),
                            start=Decimal("0"),
                        ),
                    ),
                ),
                cash_available=_summary_decimal(
                    summary,
                    "dnca_tot_amt",
                    "nxdy_excc_amt",
                    "prvs_rcdl_excc_amt",
                    fallback=Decimal("0"),
                ),
                holdings=holding_performances,
            )
        except BrokerNormalizationError as error:
            raise KoreaInvestmentBrokerError(str(error)) from error

    def get_order_capacity(
        self,
        symbol: str,
        order_price: Decimal,
    ) -> OrderCapacity:
        try:
            payload = self._request_json(
                "GET",
                KIS_ORDER_CAPACITY_PATH,
                params={
                    "CANO": self._cano,
                    "ACNT_PRDT_CD": self._account_product_code,
                    "PDNO": symbol,
                    "ORD_UNPR": str(order_price),
                    "ORD_DVSN": "01",
                    "CMA_EVLU_AMT_ICLD_YN": "N",
                    "OVRS_ICLD_YN": "N",
                },
                tr_id=self._order_capacity_tr_id,
            )
            output = _require_mapping(payload, "output")
            cash_available_raw = output.get("ord_psbl_cash")
            if cash_available_raw is None:
                cash_available_raw = output.get("nrcvb_buy_amt")
            if cash_available_raw is None:
                cash_available_raw = output.get("cash_available")
            cash_available = _coerce_decimal(
                cash_available_raw,
                field_name="ord_psbl_cash",
            )
            max_orderable_quantity_raw = output.get("nrcvb_buy_qty")
            if max_orderable_quantity_raw is None:
                max_orderable_quantity_raw = output.get("max_buy_qty")
            if max_orderable_quantity_raw is None:
                max_orderable_quantity_raw = output.get("max_orderable_quantity")
            if max_orderable_quantity_raw is None:
                max_orderable_quantity_raw = output.get("ord_psbl_qty")
            max_orderable_quantity = _coerce_optional_int(max_orderable_quantity_raw)
            if max_orderable_quantity is None:
                max_orderable_quantity = (
                    0 if order_price <= 0 else int(cash_available // order_price)
                )

            return normalize_order_capacity(
                {
                    "symbol": symbol,
                    "order_price": order_price,
                    "max_orderable_quantity": max_orderable_quantity,
                    "cash_available": cash_available,
                },
            )
        except BrokerNormalizationError as error:
            raise KoreaInvestmentBrokerError(str(error)) from error

    def _request_balance_payload(self) -> Mapping[str, object]:
        return self._request_json(
            "GET",
            KIS_BALANCE_PATH,
            params={
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._account_product_code,
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "02",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "00",
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            },
            tr_id=self._balance_tr_id,
        )


class KoreaInvestmentBarSource(_KoreaInvestmentApiClient):
    def __init__(
        self,
        settings: BrokerSettings,
        *,
        transport: HttpTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        token_cache_path: Path | None = None,
        sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] | None = None,
        min_request_interval_seconds: float | None = None,
    ) -> None:
        super().__init__(
            settings,
            transport=transport,
            clock=clock,
            token_cache_path=token_cache_path,
            sleep=sleep,
            monotonic=monotonic,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        self._daily_chart_tr_id = "FHKST03010100"
        self._intraday_chart_tr_id = "FHKST03010230"

    def load_bars(
        self,
        symbol: str,
        timeframe: Timeframe,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[Bar, ...]:
        resolved_end = _normalize_chart_datetime(
            end or self._clock(),
            field_name="end",
        )
        resolved_start = (
            None
            if start is None
            else _normalize_chart_datetime(start, field_name="start")
        )
        if resolved_start is not None and resolved_start > resolved_end:
            return ()

        if timeframe is Timeframe.DAY:
            bars = self._load_daily_bars(
                symbol,
                start=resolved_start,
                end=resolved_end,
            )
            return _filter_bars_for_window(
                bars,
                start=resolved_start,
                end=resolved_end,
            )

        minute_bars = self._load_minute_bars(
            symbol,
            start=resolved_start,
            end=resolved_end,
        )
        if timeframe is Timeframe.MINUTE_1:
            return _filter_bars_for_window(
                minute_bars,
                start=resolved_start,
                end=resolved_end,
            )

        if timeframe is Timeframe.DAY:
            raise KoreaInvestmentBrokerError("unreachable daily timeframe branch")

        aggregated = _aggregate_intraday_bars(
            minute_bars,
            timeframe=timeframe,
            end=resolved_end,
        )
        return _filter_bars_for_window(
            aggregated,
            start=resolved_start,
            end=resolved_end,
        )

    def _load_daily_bars(
        self,
        symbol: str,
        *,
        start: datetime | None,
        end: datetime,
    ) -> tuple[Bar, ...]:
        start_date = end.date() - timedelta(days=180) if start is None else start.date()
        current_end = end.date()
        seen_timestamps: set[datetime] = set()
        collected: list[Bar] = []

        while True:
            payload = self._request_json(
                "GET",
                KIS_DAILY_CHART_PATH,
                params={
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD": symbol,
                    "FID_INPUT_DATE_1": start_date.strftime("%Y%m%d"),
                    "FID_INPUT_DATE_2": current_end.strftime("%Y%m%d"),
                    "FID_PERIOD_DIV_CODE": "D",
                    "FID_ORG_ADJ_PRC": "0",
                },
                tr_id=self._daily_chart_tr_id,
            )
            rows = _require_sequence(payload, "output2")
            if not rows:
                break

            page_bars: list[Bar] = []
            for row in rows:
                if not isinstance(row, Mapping):
                    raise KoreaInvestmentBrokerError("output2 entries must be mappings")
                bar = _parse_daily_chart_bar(row, symbol=symbol)
                if bar.timestamp in seen_timestamps:
                    continue
                seen_timestamps.add(bar.timestamp)
                page_bars.append(bar)

            if page_bars:
                collected.extend(page_bars)

            oldest_date = _oldest_bar_date(page_bars)
            if oldest_date is None:
                break
            if oldest_date <= start_date or len(rows) < KIS_DAILY_CHART_PAGE_SIZE:
                break
            current_end = oldest_date - timedelta(days=1)

        collected.sort(key=lambda bar: bar.timestamp)
        return tuple(collected)

    def _load_minute_bars(
        self,
        symbol: str,
        *,
        start: datetime | None,
        end: datetime,
    ) -> tuple[Bar, ...]:
        cursor = _resolve_intraday_cursor(end)
        start_boundary = cursor - timedelta(days=21) if start is None else start
        seen_timestamps: set[datetime] = set()
        collected: list[Bar] = []

        for _ in range(KIS_INTRADAY_MAX_PAGE_COUNT):
            payload = self._request_json(
                "GET",
                KIS_INTRADAY_CHART_PATH,
                params={
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD": symbol,
                    "FID_INPUT_HOUR_1": cursor.strftime("%H%M%S"),
                    "FID_INPUT_DATE_1": cursor.strftime("%Y%m%d"),
                    "FID_PW_DATA_INCU_YN": "Y",
                    "FID_FAKE_TICK_INCU_YN": "",
                },
                tr_id=self._intraday_chart_tr_id,
            )
            rows = _require_sequence(payload, "output2")
            if not rows:
                break

            page_bars: list[Bar] = []
            for row in rows:
                if not isinstance(row, Mapping):
                    raise KoreaInvestmentBrokerError("output2 entries must be mappings")
                bar = _parse_intraday_chart_bar(row, symbol=symbol)
                if bar.timestamp > end or bar.timestamp in seen_timestamps:
                    continue
                seen_timestamps.add(bar.timestamp)
                page_bars.append(bar)

            if page_bars:
                collected.extend(page_bars)

            oldest_timestamp = _oldest_bar_timestamp(page_bars)
            if oldest_timestamp is None:
                break
            if (
                oldest_timestamp <= start_boundary
                or len(rows) < KIS_INTRADAY_CHART_PAGE_SIZE
            ):
                break
            if oldest_timestamp >= cursor:
                break
            cursor = oldest_timestamp

        collected.sort(key=lambda bar: bar.timestamp)
        return tuple(bar for bar in collected if bar.timestamp >= start_boundary)


class KoreaInvestmentBrokerTrader(_KoreaInvestmentApiClient, BrokerTrader):
    def __init__(
        self,
        settings: BrokerSettings,
        *,
        transport: HttpTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        token_cache_path: Path | None = None,
        sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] | None = None,
        min_request_interval_seconds: float | None = None,
        websocket_connector: WebSocketConnector | None = None,
    ) -> None:
        super().__init__(
            settings,
            transport=transport,
            clock=clock,
            token_cache_path=token_cache_path,
            sleep=sleep,
            monotonic=monotonic,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        self._buy_order_tr_id = (
            "VTTC0802U" if settings.environment == "paper" else "TTTC0802U"
        )
        self._sell_order_tr_id = (
            "VTTC0801U" if settings.environment == "paper" else "TTTC0801U"
        )
        self._amend_cancel_tr_id = (
            "VTTC0803U" if settings.environment == "paper" else "TTTC0803U"
        )
        self._amendable_order_tr_ids = (
            ("VTTC0084R", "VTTC8036R")
            if settings.environment == "paper"
            else ("TTTC0084R", "TTTC8036R")
        )
        self._amendable_order_lookup_supported: bool | None = None
        self._management_order_records: dict[str, Mapping[str, object]] = {}
        self._order_history_tr_id = (
            "VTTC8001R" if settings.environment == "paper" else "TTTC8001R"
        )
        self._realtime_fill_cache = _RealtimeFillCache()
        self._realtime_fill_listener = (
            _KoreaInvestmentRealtimeFillListener(
                settings,
                request_approval_key=self._request_approval_key,
                fill_cache=self._realtime_fill_cache,
                websocket_connector=websocket_connector or _open_websocket_connection,
                clock=self._clock,
                sleep=self._sleep,
            )
            if settings.hts_id is not None
            else None
        )

    def submit_order(self, request: OrderRequest) -> ExecutionOrder:
        self._ensure_realtime_fill_listener_started()
        if request.order_type is not OrderType.LIMIT:
            raise KoreaInvestmentBrokerError(
                f"unsupported order_type={request.order_type}"
            )
        _ensure_valid_krx_order_price(request)

        try:
            payload = self._request_json(
                "POST",
                KIS_ORDER_PATH,
                body={
                    "CANO": self._cano,
                    "ACNT_PRDT_CD": self._account_product_code,
                    "PDNO": request.symbol,
                    "ORD_DVSN": KIS_LIMIT_ORDER_DIVISION,
                    "ORD_QTY": str(request.quantity),
                    "ORD_UNPR": _format_order_price(request.limit_price),
                },
                tr_id=(
                    self._buy_order_tr_id
                    if request.side is OrderSide.BUY
                    else self._sell_order_tr_id
                ),
                require_hashkey=True,
            )
        except KoreaInvestmentBrokerError as error:
            raise KoreaInvestmentBrokerError(
                f"order submit failed {_format_order_request_context(request)}: {error}"
            ) from error
        output = _require_mapping(payload, "output")
        order_id = _coerce_string(output.get("ODNO"), field_name="ODNO")
        self._remember_management_order_record(
            _build_management_order_record_from_submission(
                request=request,
                output=output,
            )
        )
        return ExecutionOrder(
            order_id=order_id,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            limit_price=request.limit_price,
            status=OrderStatus.ACKNOWLEDGED,
            created_at=request.requested_at,
            updated_at=request.requested_at,
        )

    def amend_order(self, request: OrderAmendRequest) -> ExecutionOrder:
        self._ensure_realtime_fill_listener_started()
        current_record = self._require_order_record_for_management(
            request.order_id,
            reference_time=request.requested_at,
        )
        current_order = _record_to_execution_order(
            current_record,
            fallback_order_id=request.order_id,
            fallback_time=request.requested_at,
        )
        next_quantity = request.quantity or current_order.quantity
        next_limit_price = request.limit_price or current_order.limit_price
        _ensure_valid_krx_order_price_for_amend(
            request=request,
            current_order=current_order,
            next_limit_price=next_limit_price,
        )
        amend_all_remaining = request.quantity is None
        try:
            payload = self._request_json(
                "POST",
                KIS_ORDER_AMEND_CANCEL_PATH,
                body={
                    "CANO": self._cano,
                    "ACNT_PRDT_CD": self._account_product_code,
                    "KRX_FWDG_ORD_ORGNO": _resolve_order_branch(current_record),
                    "ORGN_ODNO": _normalize_order_identifier(request.order_id),
                    "ORD_DVSN": _resolve_order_division(current_record),
                    "RVSE_CNCL_DVSN_CD": "01",
                    "ORD_QTY": "0" if amend_all_remaining else str(next_quantity),
                    "ORD_UNPR": _format_order_price(next_limit_price),
                    "QTY_ALL_ORD_YN": "Y" if amend_all_remaining else "N",
                },
                tr_id=self._amend_cancel_tr_id,
                require_hashkey=True,
            )
        except KoreaInvestmentBrokerError as error:
            raise KoreaInvestmentBrokerError(
                "order amend failed "
                f"{_format_order_amend_context(request, current_order, next_limit_price)}: "
                f"{error}"
            ) from error
        output = _require_mapping(payload, "output")
        amended_order_id = (
            _optional_output_string(output.get("ODNO")) or request.order_id
        )
        self._remember_management_order_record(
            {
                "ord_dt": request.requested_at.astimezone(KST).strftime("%Y%m%d"),
                "ord_gno_brno": _resolve_order_branch(current_record),
                "ord_orgno": _resolve_order_branch(current_record),
                "odno": amended_order_id,
                "orgn_odno": _normalize_order_identifier(request.order_id),
                "sll_buy_dvsn_cd": "02"
                if current_order.side is OrderSide.BUY
                else "01",
                "sll_buy_dvsn_cd_name": (
                    "매수" if current_order.side is OrderSide.BUY else "매도"
                ),
                "pdno": current_order.symbol,
                "ord_qty": str(next_quantity),
                "ord_unpr": _format_order_price(next_limit_price),
                "ord_tmd": request.requested_at.astimezone(KST).strftime("%H%M%S"),
                "tot_ccld_qty": str(current_order.filled_quantity),
                "avg_prvs": "",
                "cncl_yn": "N",
                "ord_dvsn_cd": _resolve_order_division(current_record),
                "rjct_qty": "0",
            }
        )
        return ExecutionOrder(
            order_id=amended_order_id,
            symbol=current_order.symbol,
            side=current_order.side,
            quantity=next_quantity,
            limit_price=next_limit_price,
            status=OrderStatus.ACKNOWLEDGED,
            created_at=min(current_order.created_at, request.requested_at),
            updated_at=request.requested_at,
            filled_quantity=current_order.filled_quantity,
        )

    def cancel_order(self, request: OrderCancelRequest) -> ExecutionOrder:
        self._ensure_realtime_fill_listener_started()
        current_record = self._require_order_record_for_management(
            request.order_id,
            reference_time=request.requested_at,
        )
        current_order = _record_to_execution_order(
            current_record,
            fallback_order_id=request.order_id,
            fallback_time=request.requested_at,
        )
        try:
            payload = self._request_json(
                "POST",
                KIS_ORDER_AMEND_CANCEL_PATH,
                body={
                    "CANO": self._cano,
                    "ACNT_PRDT_CD": self._account_product_code,
                    "KRX_FWDG_ORD_ORGNO": _resolve_order_branch(current_record),
                    "ORGN_ODNO": _normalize_order_identifier(request.order_id),
                    "ORD_DVSN": _resolve_order_division(current_record),
                    "RVSE_CNCL_DVSN_CD": "02",
                    "ORD_QTY": "0",
                    "ORD_UNPR": "0",
                    "QTY_ALL_ORD_YN": "Y",
                },
                tr_id=self._amend_cancel_tr_id,
                require_hashkey=True,
            )
        except KoreaInvestmentBrokerError as error:
            if _is_no_cancelable_quantity_error(error):
                filled_order = ExecutionOrder(
                    order_id=request.order_id,
                    symbol=current_order.symbol,
                    side=current_order.side,
                    quantity=current_order.quantity,
                    limit_price=current_order.limit_price,
                    status=OrderStatus.FILLED,
                    created_at=min(current_order.created_at, request.requested_at),
                    updated_at=request.requested_at,
                    filled_quantity=current_order.quantity,
                )
                self._remember_management_order_record(
                    {
                        "ord_dt": request.requested_at.astimezone(KST).strftime(
                            "%Y%m%d"
                        ),
                        "ord_gno_brno": _resolve_order_branch(current_record),
                        "ord_orgno": _resolve_order_branch(current_record),
                        "odno": request.order_id,
                        "orgn_odno": _normalize_order_identifier(request.order_id),
                        "sll_buy_dvsn_cd": (
                            "02" if current_order.side is OrderSide.BUY else "01"
                        ),
                        "sll_buy_dvsn_cd_name": (
                            "매수" if current_order.side is OrderSide.BUY else "매도"
                        ),
                        "pdno": current_order.symbol,
                        "ord_qty": str(current_order.quantity),
                        "ord_unpr": _format_order_price(current_order.limit_price),
                        "ord_tmd": request.requested_at.astimezone(KST).strftime(
                            "%H%M%S"
                        ),
                        "tot_ccld_qty": str(current_order.quantity),
                        "avg_prvs": "",
                        "cncl_yn": "N",
                        "ord_dvsn_cd": _resolve_order_division(current_record),
                        "rjct_qty": "0",
                    }
                )
                return filled_order
            raise
        output = _require_mapping(payload, "output")
        canceled_order_id = (
            _optional_output_string(output.get("ODNO")) or request.order_id
        )
        self._remember_management_order_record(
            {
                "ord_dt": request.requested_at.astimezone(KST).strftime("%Y%m%d"),
                "ord_gno_brno": _resolve_order_branch(current_record),
                "ord_orgno": _resolve_order_branch(current_record),
                "odno": canceled_order_id,
                "orgn_odno": _normalize_order_identifier(request.order_id),
                "sll_buy_dvsn_cd": "02"
                if current_order.side is OrderSide.BUY
                else "01",
                "sll_buy_dvsn_cd_name": (
                    "매수" if current_order.side is OrderSide.BUY else "매도"
                ),
                "pdno": current_order.symbol,
                "ord_qty": str(current_order.quantity),
                "ord_unpr": _format_order_price(current_order.limit_price),
                "ord_tmd": request.requested_at.astimezone(KST).strftime("%H%M%S"),
                "tot_ccld_qty": str(current_order.filled_quantity),
                "avg_prvs": "",
                "cncl_yn": "Y",
                "ord_dvsn_cd": _resolve_order_division(current_record),
                "rjct_qty": "0",
            }
        )
        return ExecutionOrder(
            order_id=canceled_order_id,
            symbol=current_order.symbol,
            side=current_order.side,
            quantity=current_order.quantity,
            limit_price=current_order.limit_price,
            status=OrderStatus.CANCELED,
            created_at=min(current_order.created_at, request.requested_at),
            updated_at=request.requested_at,
            filled_quantity=current_order.filled_quantity,
        )

    def get_fills(self, order_id: str) -> tuple[ExecutionFill, ...]:
        return self._get_fills(order_id, current_order=None)

    def get_fills_for_order(self, order: ExecutionOrder) -> tuple[ExecutionFill, ...]:
        return self._get_fills(order.order_id, current_order=order)

    def _get_fills(
        self,
        order_id: str,
        *,
        current_order: ExecutionOrder | None,
    ) -> tuple[ExecutionFill, ...]:
        self._ensure_realtime_fill_listener_started()
        reference_time = self._clock()
        realtime_fills = self._realtime_fill_cache.get(order_id)
        lookup = self._load_order_history(
            reference_time,
            order_id=order_id,
        )
        record = self._find_order_record_for_fills(
            order_id,
            records=lookup.records,
        )
        if record is None and current_order is not None:
            inferred_fill = _build_fill_from_order_history_summary(
                current_order,
                lookup.summary,
                fallback=reference_time,
            )
            if inferred_fill is not None:
                return (*realtime_fills, inferred_fill)
        if record is None:
            if realtime_fills:
                return realtime_fills
            raise KoreaInvestmentBrokerError(
                f"order history missing order_id={order_id}"
            )
        filled_quantity = _coerce_optional_int(record.get("tot_ccld_qty")) or 0
        if filled_quantity <= 0:
            return realtime_fills

        average_price_raw = record.get("avg_prvs")
        if _is_blank_value(average_price_raw):
            average_price_raw = record.get("ord_unpr")
        return (
            *realtime_fills,
            ExecutionFill(
                fill_id=f"{order_id}{KIS_CUMULATIVE_FILL_ID_SUFFIX}",
                order_id=order_id,
                symbol=_coerce_string(record.get("pdno"), field_name="pdno"),
                quantity=filled_quantity,
                price=_coerce_decimal(average_price_raw, field_name="avg_prvs"),
                filled_at=_parse_fill_timestamp(record, fallback=reference_time),
            ),
        )

    def close(self) -> None:
        if self._realtime_fill_listener is not None:
            self._realtime_fill_listener.close()

    def _require_order_record(
        self,
        order_id: str,
        *,
        reference_time: datetime,
    ) -> Mapping[str, object]:
        normalized_order_id = order_id.strip()
        for delay_seconds in (*self._order_history_lookup_delays_seconds, None):
            records = self._load_order_records(reference_time)
            exact_match = _find_order_record_by_field(
                records,
                field_name="odno",
                value=normalized_order_id,
            )
            if exact_match is not None:
                return exact_match
            origin_match = _find_order_record_by_field(
                records,
                field_name="orgn_odno",
                value=normalized_order_id,
            )
            if origin_match is not None:
                return origin_match
            if delay_seconds is None:
                break
            self._sleep(delay_seconds)
        raise KoreaInvestmentBrokerError(f"order history missing order_id={order_id}")

    def _require_order_record_for_management(
        self,
        order_id: str,
        *,
        reference_time: datetime,
    ) -> Mapping[str, object]:
        cached_record = self._lookup_management_order_record(order_id)
        if cached_record is not None:
            return cached_record

        normalized_order_id = order_id.strip()
        for delay_seconds in (*self._order_history_lookup_delays_seconds, None):
            history_match = _find_matching_order_record(
                self._load_order_records(reference_time),
                normalized_order_id,
            )
            if history_match is not None:
                self._remember_management_order_record(history_match)
                return history_match

            amendable_match = _find_matching_order_record(
                self._load_amendable_order_records(),
                normalized_order_id,
            )
            if amendable_match is not None:
                self._remember_management_order_record(amendable_match)
                return amendable_match

            if delay_seconds is None:
                break
            self._sleep(delay_seconds)
        raise KoreaInvestmentBrokerError(f"order history missing order_id={order_id}")

    def _load_order_records(
        self,
        reference_time: datetime,
    ) -> tuple[Mapping[str, object], ...]:
        return self._load_order_history(reference_time).records

    def _load_order_history(
        self,
        reference_time: datetime,
        *,
        order_id: str | None = None,
    ) -> _OrderHistoryLookup:
        inquiry_date = reference_time.astimezone(KST).strftime("%Y%m%d")
        normalized_order_id = (
            _normalize_order_identifier(order_id)
            if order_id is not None and order_id.strip()
            else ""
        )
        payload = self._request_json(
            "GET",
            KIS_ORDER_HISTORY_PATH,
            params={
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._account_product_code,
                "INQR_STRT_DT": inquiry_date,
                "INQR_END_DT": inquiry_date,
                "SLL_BUY_DVSN_CD": "00",
                "INQR_DVSN": "00",
                "PDNO": "",
                "CCLD_DVSN": "00",
                "ORD_GNO_BRNO": "",
                "ODNO": normalized_order_id,
                "INQR_DVSN_3": "00",
                "INQR_DVSN_1": "",
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            },
            tr_id=self._order_history_tr_id,
        )
        rows = _require_sequence(payload, "output1")
        records = tuple(
            row
            for row in rows
            if isinstance(row, Mapping) and not _is_blank_value(row.get("odno"))
        )
        summary = payload.get("output2")
        return _OrderHistoryLookup(
            records=tuple(
                sorted(
                    records,
                    key=lambda record: _parse_order_timestamp(
                        record,
                        fallback=reference_time,
                    ),
                    reverse=True,
                )
            ),
            summary=summary if isinstance(summary, Mapping) else None,
        )

    def _load_amendable_order_records(self) -> tuple[Mapping[str, object], ...]:
        if self._amendable_order_lookup_supported is False:
            return ()

        last_error: KoreaInvestmentBrokerError | None = None
        for tr_id in self._amendable_order_tr_ids:
            try:
                payload = self._request_json(
                    "GET",
                    KIS_AMENDABLE_ORDER_PATH,
                    params={
                        "CANO": self._cano,
                        "ACNT_PRDT_CD": self._account_product_code,
                        "INQR_DVSN_1": "0",
                        "INQR_DVSN_2": "0",
                        "CTX_AREA_FK100": "",
                        "CTX_AREA_NK100": "",
                    },
                    tr_id=tr_id,
                )
            except KoreaInvestmentBrokerError as error:
                if _is_unsupported_amendable_order_lookup_error(error):
                    self._amendable_order_lookup_supported = False
                    return ()
                last_error = error
                continue

            self._amendable_order_lookup_supported = True
            rows = _require_sequence(payload, "output")
            return tuple(
                row
                for row in rows
                if isinstance(row, Mapping) and not _is_blank_value(row.get("odno"))
            )

        if last_error is not None:
            raise last_error
        return ()

    def _lookup_management_order_record(
        self,
        order_id: str,
        *,
        require_branch: bool = True,
    ) -> Mapping[str, object] | None:
        normalized_order_id = _normalize_order_identifier(order_id)
        record = self._management_order_records.get(normalized_order_id)
        if record is None:
            return None
        if require_branch and _is_blank_value(_resolve_order_branch(record)):
            return None
        return record

    def _remember_management_order_record(
        self,
        record: Mapping[str, object],
    ) -> None:
        for field_name in ("odno", "orgn_odno"):
            order_id = _optional_output_string(record.get(field_name))
            if order_id is None:
                continue
            self._management_order_records[_normalize_order_identifier(order_id)] = (
                record
            )

    def _find_order_record_for_fills(
        self,
        order_id: str,
        *,
        records: Sequence[Mapping[str, object]],
    ) -> Mapping[str, object] | None:
        history_match = _find_matching_order_record(
            records,
            order_id.strip(),
        )
        if history_match is not None:
            self._remember_management_order_record(history_match)
            return history_match
        return self._lookup_management_order_record(order_id, require_branch=False)

    def _ensure_realtime_fill_listener_started(self) -> None:
        if self._realtime_fill_listener is None:
            return
        self._realtime_fill_listener.ensure_started()


def _open_websocket_connection(url: str) -> _WebSocketConnection:
    return _WebSocketConnection(url, timeout_seconds=KIS_WS_TIMEOUT_SECONDS)


def _build_fill_notice_subscription_message(
    *,
    approval_key: str,
    tr_id: str,
    tr_key: str,
) -> str:
    return json.dumps(
        {
            "header": {
                "approval_key": approval_key,
                "content-type": "utf-8",
                "custtype": KIS_WS_CUSTOMER_TYPE,
                "tr_type": KIS_WS_SUBSCRIBE_TR_TYPE,
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": tr_key,
                }
            },
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _decrypt_fill_notice_payload(
    *,
    key: str,
    iv: str,
    cipher_text: str,
) -> str:
    try:
        from Crypto.Cipher import AES
        from Crypto.Util.Padding import unpad
    except ModuleNotFoundError as error:
        raise KoreaInvestmentBrokerError(
            "pycryptodome is required to decrypt KIS websocket fill notices"
        ) from error

    cipher = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
    decrypted = cipher.decrypt(base64.b64decode(cipher_text))
    return unpad(decrypted, AES.block_size).decode("utf-8")


def _parse_realtime_fill_notices(
    payload: str,
    *,
    fallback: datetime,
) -> tuple[ExecutionFill, ...]:
    values = payload.split("^")
    if not values or values == [""]:
        return ()
    if len(values) % len(KIS_CCNL_NOTICE_COLUMNS) != 0:
        raise KoreaInvestmentBrokerError("unexpected realtime fill payload shape")

    fills: list[ExecutionFill] = []
    column_count = len(KIS_CCNL_NOTICE_COLUMNS)
    for offset in range(0, len(values), column_count):
        row_values = values[offset : offset + column_count]
        row = dict(zip(KIS_CCNL_NOTICE_COLUMNS, row_values, strict=True))
        fill = _parse_realtime_fill_notice_row(
            row,
            raw_payload="^".join(row_values),
            fallback=fallback,
        )
        if fill is not None:
            fills.append(fill)
    return tuple(fills)


def _parse_realtime_fill_notice_row(
    row: Mapping[str, object],
    *,
    raw_payload: str,
    fallback: datetime,
) -> ExecutionFill | None:
    if _optional_output_string(row.get("CNTG_YN")) != "2":
        return None

    order_id = _optional_output_string(row.get("ODER_NO")) or _optional_output_string(
        row.get("OODER_NO")
    )
    if order_id is None:
        return None

    quantity = _coerce_optional_int(row.get("CNTG_QTY")) or 0
    if quantity <= 0:
        return None

    price_raw = row.get("CNTG_UNPR")
    if _is_blank_value(price_raw):
        price_raw = row.get("ODER_PRC")
    if _is_blank_value(price_raw):
        return None

    normalized_order_id = _normalize_order_identifier(order_id)
    fill_hash = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()[:16]
    return ExecutionFill(
        fill_id=f"{normalized_order_id}:ws:{fill_hash}",
        order_id=normalized_order_id,
        symbol=_coerce_string(row.get("STCK_SHRN_ISCD"), field_name="STCK_SHRN_ISCD"),
        quantity=quantity,
        price=_coerce_decimal(price_raw, field_name="CNTG_UNPR"),
        filled_at=_parse_realtime_fill_notice_timestamp(
            _optional_output_string(row.get("STCK_CNTG_HOUR")),
            fallback=fallback,
        ),
    )


def _parse_realtime_fill_notice_timestamp(
    raw_time: str | None,
    *,
    fallback: datetime,
) -> datetime:
    if raw_time is None:
        return fallback
    normalized_time = raw_time.strip().zfill(6)
    if len(normalized_time) != 6:
        return fallback

    base_date = fallback.astimezone(KST).date()
    try:
        parsed = datetime.strptime(
            f"{base_date:%Y%m%d}{normalized_time}",
            "%Y%m%d%H%M%S",
        )
    except ValueError:
        return fallback
    return parsed.replace(tzinfo=KST)


def _is_blank_value(value: object) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _optional_output_string(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _find_order_record_by_field(
    records: Sequence[Mapping[str, object]],
    *,
    field_name: str,
    value: str,
) -> Mapping[str, object] | None:
    for record in records:
        if _order_identifiers_match(
            _optional_output_string(record.get(field_name)),
            value,
        ):
            return record
    return None


def _find_matching_order_record(
    records: Sequence[Mapping[str, object]],
    order_id: str,
) -> Mapping[str, object] | None:
    exact_match = _find_order_record_by_field(
        records,
        field_name="odno",
        value=order_id,
    )
    if exact_match is not None:
        return exact_match
    return _find_order_record_by_field(
        records,
        field_name="orgn_odno",
        value=order_id,
    )


def _build_management_order_record_from_submission(
    *,
    request: OrderRequest,
    output: Mapping[str, object],
) -> Mapping[str, object]:
    order_id = _coerce_string(output.get("ODNO"), field_name="ODNO")
    submitted_at = request.requested_at.astimezone(KST)
    order_branch = (
        _optional_output_string(output.get("KRX_FWDG_ORD_ORGNO"))
        or _optional_output_string(output.get("ORD_GNO_BRNO"))
        or _optional_output_string(output.get("ord_gno_brno"))
        or ""
    )
    order_time = (
        _optional_output_string(output.get("ORD_TMD"))
        or _optional_output_string(output.get("ord_tmd"))
        or submitted_at.strftime("%H%M%S")
    )
    return {
        "ord_dt": submitted_at.strftime("%Y%m%d"),
        "ord_gno_brno": order_branch,
        "ord_orgno": order_branch,
        "odno": order_id,
        "orgn_odno": "",
        "sll_buy_dvsn_cd": "02" if request.side is OrderSide.BUY else "01",
        "sll_buy_dvsn_cd_name": "매수" if request.side is OrderSide.BUY else "매도",
        "pdno": request.symbol,
        "ord_qty": str(request.quantity),
        "ord_unpr": _format_order_price(request.limit_price),
        "ord_tmd": order_time,
        "tot_ccld_qty": "0",
        "avg_prvs": "",
        "cncl_yn": "N",
        "ord_dvsn_cd": KIS_LIMIT_ORDER_DIVISION,
        "rjct_qty": "0",
    }


def _resolve_raw_log_directory(settings: BrokerSettings) -> Path | None:
    if settings.environment != "paper":
        return None

    raw_value = os.getenv("AUTOTRADE_LOG_DIR")
    if raw_value is None or not raw_value.strip():
        return None

    log_dir = Path(raw_value).expanduser()
    if log_dir.exists() and not log_dir.is_dir():
        return None
    return log_dir


def _normalize_order_identifier(value: str) -> str:
    normalized = value.strip()
    if normalized.isdigit():
        return normalized.lstrip("0") or "0"
    return normalized


def _order_identifiers_match(left: str | None, right: str | None) -> bool:
    if left is None or right is None:
        return False
    if left == right:
        return True
    return _normalize_order_identifier(left) == _normalize_order_identifier(right)


def _is_unsupported_amendable_order_lookup_error(
    error: KoreaInvestmentBrokerError,
) -> bool:
    message = str(error)
    return message.startswith("90000000 - ")


def _is_no_cancelable_quantity_error(error: KoreaInvestmentBrokerError) -> bool:
    message = str(error)
    return message.startswith("40330000 - ")


def _sanitize_url_for_log(url: str) -> str:
    parsed = urlsplit(url)
    if not parsed.query:
        return url

    sanitized_query = urlencode(
        [
            (key, _redact_log_value(key, value))
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        ]
    )
    return parsed._replace(query=sanitized_query).geturl()


def _sanitize_headers_for_log(headers: Mapping[str, str]) -> dict[str, str]:
    return {key: _redact_log_value(key, value) for key, value in headers.items()}


def _decode_body_for_log(body: bytes | None) -> object | None:
    if body is None:
        return None

    decoded_text = body.decode("utf-8", errors="replace")
    parsed = _try_decode_json(body)
    if parsed is None:
        return decoded_text
    return _sanitize_json_value_for_log(parsed)


def _sanitize_json_value_for_log(value: object, key: str | None = None) -> object:
    if isinstance(value, Mapping):
        return {
            nested_key: _sanitize_json_value_for_log(nested_value, nested_key)
            for nested_key, nested_value in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_sanitize_json_value_for_log(item, key) for item in value]
    if isinstance(value, str) and key is not None:
        return _redact_log_value(key, value)
    return value


def _redact_log_value(key: str, value: str) -> str:
    normalized_key = key.strip().lower()
    if normalized_key in {
        "authorization",
        "appkey",
        "appsecret",
        "hashkey",
        "cano",
    }:
        return "[REDACTED]"
    return value


def _ensure_valid_krx_order_price(request: OrderRequest) -> None:
    if is_valid_krx_symbol_order_price(request.symbol, request.limit_price):
        return
    normalized_price = normalize_krx_symbol_order_price(
        request.symbol,
        request.limit_price,
        request.side,
    )
    raise KoreaInvestmentBrokerError(
        "invalid KRX order tick price "
        f"{_format_order_request_context(request)} "
        f"normalized_price={normalized_price}"
    )


def _ensure_valid_krx_order_price_for_amend(
    *,
    request: OrderAmendRequest,
    current_order: ExecutionOrder,
    next_limit_price: Decimal,
) -> None:
    if is_valid_krx_symbol_order_price(current_order.symbol, next_limit_price):
        return
    normalized_price = normalize_krx_symbol_order_price(
        current_order.symbol,
        next_limit_price,
        current_order.side,
    )
    raise KoreaInvestmentBrokerError(
        "invalid KRX order tick price "
        f"{_format_order_amend_context(request, current_order, next_limit_price)} "
        f"normalized_price={normalized_price}"
    )


def _format_order_request_context(request: OrderRequest) -> str:
    return (
        f"request_id={request.request_id} "
        f"symbol={request.symbol} "
        f"side={request.side.value} "
        f"quantity={request.quantity} "
        f"limit_price={request.limit_price}"
    )


def _format_order_amend_context(
    request: OrderAmendRequest,
    current_order: ExecutionOrder,
    next_limit_price: Decimal,
) -> str:
    return (
        f"request_id={request.request_id} "
        f"order_id={request.order_id} "
        f"symbol={current_order.symbol} "
        f"side={current_order.side.value} "
        f"quantity={request.quantity or current_order.quantity} "
        f"limit_price={next_limit_price}"
    )


def _format_order_price(value: Decimal) -> str:
    if value != value.to_integral_value():
        raise KoreaInvestmentBrokerError("order price must be an integer KRW amount")
    return str(int(value))


def _parse_order_timestamp(
    record: Mapping[str, object],
    *,
    fallback: datetime,
) -> datetime:
    order_date = _optional_output_string(record.get("ord_dt"))
    order_time = _optional_output_string(record.get("ord_tmd"))
    if order_date is None or order_time is None:
        return fallback

    normalized_time = order_time.zfill(6)
    if len(order_date) != 8 or len(normalized_time) != 6:
        return fallback

    try:
        parsed = datetime.strptime(order_date + normalized_time, "%Y%m%d%H%M%S")
    except ValueError:
        return fallback
    return parsed.replace(tzinfo=KST)


def _parse_fill_timestamp(
    record: Mapping[str, object],
    *,
    fallback: datetime,
) -> datetime:
    notification_time = _optional_output_string(record.get("infm_tmd"))
    if notification_time is not None:
        fill_record = dict(record)
        fill_record["ord_tmd"] = notification_time
        return _parse_order_timestamp(fill_record, fallback=fallback)
    return _parse_order_timestamp(record, fallback=fallback)


def _resolve_order_side(record: Mapping[str, object]) -> OrderSide:
    side_code = _optional_output_string(record.get("sll_buy_dvsn_cd"))
    if side_code == "02":
        return OrderSide.BUY
    if side_code == "01":
        return OrderSide.SELL

    side_name = _optional_output_string(record.get("sll_buy_dvsn_cd_name"))
    if side_name == "매수":
        return OrderSide.BUY
    if side_name == "매도":
        return OrderSide.SELL
    raise KoreaInvestmentBrokerError("order history missing side")


def _resolve_order_status(
    record: Mapping[str, object],
    *,
    quantity: int,
) -> OrderStatus:
    filled_quantity = _coerce_optional_int(record.get("tot_ccld_qty")) or 0
    rejected_quantity = _coerce_optional_int(record.get("rjct_qty")) or 0
    canceled = _optional_output_string(record.get("cncl_yn")) == "Y"

    if rejected_quantity >= quantity and filled_quantity == 0:
        return OrderStatus.REJECTED
    if canceled:
        return OrderStatus.CANCELED
    if filled_quantity >= quantity:
        return OrderStatus.FILLED
    if filled_quantity > 0:
        return OrderStatus.PARTIALLY_FILLED
    return OrderStatus.ACKNOWLEDGED


def _resolve_order_branch(record: Mapping[str, object]) -> str:
    return (
        _optional_output_string(record.get("ord_gno_brno"))
        or _optional_output_string(record.get("ord_orgno"))
        or ""
    )


def _resolve_order_division(record: Mapping[str, object]) -> str:
    return (
        _optional_output_string(record.get("ord_dvsn_cd")) or KIS_LIMIT_ORDER_DIVISION
    )


def _record_to_execution_order(
    record: Mapping[str, object],
    *,
    fallback_order_id: str,
    fallback_time: datetime,
) -> ExecutionOrder:
    quantity = _coerce_int(record.get("ord_qty"), field_name="ord_qty")
    created_at = _parse_order_timestamp(record, fallback=fallback_time)
    return ExecutionOrder(
        order_id=_optional_output_string(record.get("odno")) or fallback_order_id,
        symbol=_coerce_string(record.get("pdno"), field_name="pdno"),
        side=_resolve_order_side(record),
        quantity=quantity,
        limit_price=_coerce_decimal(record.get("ord_unpr"), field_name="ord_unpr"),
        status=_resolve_order_status(record, quantity=quantity),
        created_at=created_at,
        updated_at=created_at,
        filled_quantity=min(
            _coerce_optional_int(record.get("tot_ccld_qty")) or 0,
            quantity,
        ),
    )


def _build_fill_from_order_history_summary(
    order: ExecutionOrder,
    summary: Mapping[str, object] | None,
    *,
    fallback: datetime,
) -> ExecutionFill | None:
    if summary is None:
        return None

    filled_quantity = _coerce_optional_int(summary.get("tot_ccld_qty")) or 0
    if filled_quantity <= 0 or filled_quantity > order.quantity:
        return None

    total_amount_raw = summary.get("tot_ccld_amt")
    if _is_blank_value(total_amount_raw):
        return None
    total_amount = _coerce_decimal(total_amount_raw, field_name="tot_ccld_amt")
    if total_amount <= Decimal("0"):
        return None

    return ExecutionFill(
        fill_id=f"{order.order_id}{KIS_CUMULATIVE_FILL_ID_SUFFIX}",
        order_id=order.order_id,
        symbol=order.symbol,
        quantity=filled_quantity,
        price=total_amount / Decimal(filled_quantity),
        filled_at=fallback,
    )


def _normalize_chart_datetime(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise KoreaInvestmentBrokerError(f"{field_name} must be timezone-aware")
    return value.astimezone(KST).replace(microsecond=0)


def _filter_bars_for_window(
    bars: Sequence[Bar],
    *,
    start: datetime | None,
    end: datetime,
) -> tuple[Bar, ...]:
    return tuple(
        bar
        for bar in bars
        if (start is None or bar.timestamp >= start) and bar.timestamp <= end
    )


def _resolve_intraday_cursor(end: datetime) -> datetime:
    local_end = end.astimezone(KST).replace(second=0, microsecond=0)
    session_close = local_end.replace(
        hour=KRX_SESSION_CLOSE.hour,
        minute=KRX_SESSION_CLOSE.minute,
        second=0,
        microsecond=0,
    )
    if local_end > session_close:
        return session_close
    return local_end


def _oldest_bar_date(bars: Sequence[Bar]) -> date | None:
    if not bars:
        return None
    return min(bar.timestamp.date() for bar in bars)


def _oldest_bar_timestamp(bars: Sequence[Bar]) -> datetime | None:
    if not bars:
        return None
    return min(bar.timestamp for bar in bars)


def _parse_daily_chart_bar(
    row: Mapping[str, object],
    *,
    symbol: str,
) -> Bar:
    base_date = _coerce_string(row.get("stck_bsop_date"), field_name="stck_bsop_date")
    if len(base_date) != 8:
        raise KoreaInvestmentBrokerError("stck_bsop_date must use YYYYMMDD format")
    try:
        parsed_date = datetime.strptime(base_date, "%Y%m%d")
    except ValueError as error:
        raise KoreaInvestmentBrokerError(
            "stck_bsop_date must use YYYYMMDD format"
        ) from error
    open_price = _coerce_decimal(row.get("stck_oprc"), field_name="stck_oprc")
    high = _coerce_decimal(row.get("stck_hgpr"), field_name="stck_hgpr")
    low = _coerce_decimal(row.get("stck_lwpr"), field_name="stck_lwpr")
    close = _coerce_decimal(row.get("stck_clpr"), field_name="stck_clpr")
    volume = _coerce_optional_int(row.get("acml_vol")) or 0
    if volume == 0 and close > 0:
        open_price = close
        high = close
        low = close

    try:
        return Bar(
            symbol=symbol,
            timeframe=Timeframe.DAY,
            timestamp=parsed_date.replace(
                hour=KRX_SESSION_CLOSE.hour,
                minute=KRX_SESSION_CLOSE.minute,
                tzinfo=KST,
            ),
            open=open_price,
            high=high,
            low=low,
            close=close,
            volume=volume,
        )
    except ValueError as error:
        raise KoreaInvestmentBrokerError(
            "invalid KIS daily bar "
            f"symbol={symbol} date={base_date} "
            f"open={open_price} high={high} low={low} close={close} "
            f"volume={volume}: {error}"
        ) from error


def _parse_intraday_chart_bar(
    row: Mapping[str, object],
    *,
    symbol: str,
) -> Bar:
    timestamp = _parse_chart_timestamp(row)
    return Bar(
        symbol=symbol,
        timeframe=Timeframe.MINUTE_1,
        timestamp=timestamp,
        open=_coerce_decimal(row.get("stck_oprc"), field_name="stck_oprc"),
        high=_coerce_decimal(row.get("stck_hgpr"), field_name="stck_hgpr"),
        low=_coerce_decimal(row.get("stck_lwpr"), field_name="stck_lwpr"),
        close=_coerce_decimal(row.get("stck_prpr"), field_name="stck_prpr"),
        volume=_coerce_optional_int(row.get("cntg_vol")) or 0,
    )


def _parse_chart_timestamp(row: Mapping[str, object]) -> datetime:
    base_date = _coerce_string(row.get("stck_bsop_date"), field_name="stck_bsop_date")
    base_time = _coerce_string(row.get("stck_cntg_hour"), field_name="stck_cntg_hour")
    normalized_time = base_time.zfill(6)
    if len(base_date) != 8 or len(normalized_time) != 6:
        raise KoreaInvestmentBrokerError("chart timestamp must use YYYYMMDD/HHMMSS")
    try:
        parsed = datetime.strptime(base_date + normalized_time, "%Y%m%d%H%M%S")
    except ValueError as error:
        raise KoreaInvestmentBrokerError(
            "chart timestamp must use YYYYMMDD/HHMMSS"
        ) from error
    return parsed.replace(tzinfo=KST)


def _aggregate_intraday_bars(
    bars: Sequence[Bar],
    *,
    timeframe: Timeframe,
    end: datetime,
) -> tuple[Bar, ...]:
    if timeframe is Timeframe.DAY:
        raise KoreaInvestmentBrokerError("daily bars must not be aggregated intraday")
    if timeframe is Timeframe.MINUTE_1:
        return tuple(sorted(bars, key=lambda bar: bar.timestamp))

    interval_minutes = int(timeframe.interval.total_seconds() // 60)
    if interval_minutes <= 0:
        raise KoreaInvestmentBrokerError("timeframe interval must be positive")

    grouped: dict[datetime, list[Bar]] = {}
    for bar in bars:
        bucket_start = _intraday_bucket_start(bar.timestamp, interval_minutes)
        session_close = bucket_start.replace(
            hour=KRX_SESSION_CLOSE.hour,
            minute=KRX_SESSION_CLOSE.minute,
            second=0,
            microsecond=0,
        )
        if bucket_start > session_close:
            continue
        bucket_complete_at = min(bucket_start + timeframe.interval, session_close)
        if bucket_complete_at > end:
            continue
        grouped.setdefault(bucket_start, []).append(bar)

    aggregated: list[Bar] = []
    for bucket_start, series in sorted(grouped.items()):
        series.sort(key=lambda bar: bar.timestamp)
        aggregated.append(
            Bar(
                symbol=series[0].symbol,
                timeframe=timeframe,
                timestamp=bucket_start,
                open=series[0].open,
                high=max(bar.high for bar in series),
                low=min(bar.low for bar in series),
                close=series[-1].close,
                volume=sum(bar.volume for bar in series),
            )
        )
    return tuple(aggregated)


def _intraday_bucket_start(timestamp: datetime, interval_minutes: int) -> datetime:
    local_timestamp = timestamp.astimezone(KST)
    session_open = local_timestamp.replace(
        hour=KRX_SESSION_OPEN.hour,
        minute=KRX_SESSION_OPEN.minute,
        second=0,
        microsecond=0,
    )
    elapsed_minutes = int((local_timestamp - session_open).total_seconds() // 60)
    bucket_offset = (elapsed_minutes // interval_minutes) * interval_minutes
    return session_open + timedelta(minutes=bucket_offset)


def _urllib_transport(request: HttpRequest) -> HttpResponse:
    try:
        urllib_request = Request(
            request.url,
            data=request.body,
            headers=dict(request.headers),
            method=request.method,
        )
        with urlopen(urllib_request, timeout=request.timeout) as response:
            return HttpResponse(
                status=response.getcode(),
                headers=dict(response.headers.items()),
                body=response.read(),
            )
    except HTTPError as error:
        return HttpResponse(
            status=error.code,
            headers=dict(error.headers.items()) if error.headers is not None else {},
            body=error.read(),
        )


def _build_url(
    base_url: str,
    path: str,
    params: Mapping[str, str] | None,
) -> str:
    if not params:
        return f"{base_url}{path}"
    return f"{base_url}{path}?{urlencode(params)}"


def _decode_json(raw_body: bytes) -> Mapping[str, object]:
    try:
        decoded = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, JSONDecodeError) as error:
        raise KoreaInvestmentBrokerError("response body is not valid JSON") from error

    if not isinstance(decoded, Mapping):
        raise KoreaInvestmentBrokerError("response JSON must be an object")
    return decoded


def _raise_for_api_error(payload: Mapping[str, object]) -> None:
    rt_cd = payload.get("rt_cd")
    if isinstance(rt_cd, str) and rt_cd.strip() and rt_cd != "0":
        raise KoreaInvestmentBrokerError(_format_kis_payload_error(payload))


def _format_http_error(
    *,
    status: int,
    method: str,
    url: str,
    body: bytes,
) -> str:
    details = _format_http_error_body(body)
    if details is None:
        return f"HTTP {status} from {method} {url}"
    return f"HTTP {status} from {method} {url}: {details}"


def _format_transport_failure(message: str, detail: object) -> str:
    normalized_detail = str(detail).strip()
    if not normalized_detail:
        return message
    return f"{message}: {normalized_detail}"


def _format_http_error_body(body: bytes) -> str | None:
    payload = _try_decode_json(body)
    if payload is not None:
        return _format_kis_payload_error(payload)

    raw_body = body.decode("utf-8", errors="replace").strip()
    if not raw_body:
        return None
    return raw_body


def _try_decode_json(raw_body: bytes) -> Mapping[str, object] | None:
    try:
        decoded = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, JSONDecodeError):
        return None

    if not isinstance(decoded, Mapping):
        return None
    return decoded


def _is_token_expired_payload(payload: Mapping[str, object] | None) -> bool:
    if payload is None:
        return False
    msg_cd = payload.get("msg_cd")
    return isinstance(msg_cd, str) and msg_cd.strip() == KIS_TOKEN_EXPIRED_MSG_CODE


def _is_rate_limit_payload(payload: Mapping[str, object] | None) -> bool:
    if payload is None:
        return False
    msg_cd = payload.get("msg_cd")
    return isinstance(msg_cd, str) and msg_cd.strip() == KIS_RATE_LIMIT_MSG_CODE


def _is_retryable_transport_request(method: str, path: str) -> bool:
    if method == "GET":
        return True
    return method == "POST" and path in {
        KIS_TOKEN_PATH,
        KIS_APPROVAL_PATH,
        KIS_HASHKEY_PATH,
    }


def _format_kis_payload_error(payload: Mapping[str, object]) -> str:
    msg_cd = payload.get("msg_cd")
    msg1 = payload.get("msg1")
    error_description = payload.get("error_description")

    details: list[str] = []
    if isinstance(msg_cd, str) and msg_cd.strip():
        details.append(msg_cd.strip())
    if isinstance(msg1, str) and msg1.strip():
        details.append(msg1.strip())
    if isinstance(error_description, str) and error_description.strip():
        details.append(error_description.strip())

    if details:
        return " - ".join(details)
    return "KIS request failed"


def _resolve_access_token_expiration(
    payload: Mapping[str, object],
    *,
    now: datetime,
) -> datetime | None:
    expiration_value = payload.get("access_token_token_expired")
    if isinstance(expiration_value, str) and expiration_value.strip():
        try:
            return datetime.strptime(
                expiration_value.strip(),
                "%Y-%m-%d %H:%M:%S",
            ).replace(tzinfo=KST)
        except ValueError:
            return None

    expires_in_seconds = _coerce_optional_int(payload.get("expires_in"))
    if expires_in_seconds is None:
        return None
    return now + timedelta(seconds=expires_in_seconds)


def _parse_cached_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _is_token_expired(expires_at: datetime, *, now: datetime) -> bool:
    return expires_at <= now + TOKEN_CACHE_REFRESH_BUFFER


def _default_token_cache_path(settings: BrokerSettings) -> Path:
    cache_root = os.environ.get("XDG_CACHE_HOME")
    base_directory = (
        Path(cache_root).expanduser() if cache_root else Path.home() / ".cache"
    )
    cache_key = hashlib.sha256(
        f"{settings.environment}:{settings.api_key}:{settings.account}".encode("utf-8")
    ).hexdigest()
    return (
        base_directory
        / "autotrade"
        / "korea_investment"
        / f"access-token-{cache_key[:16]}.json"
    )


def _request_throttle_for(settings: BrokerSettings) -> _RequestThrottle:
    key = hashlib.sha256(
        f"{settings.environment}:{settings.api_key}:{settings.account}".encode("utf-8")
    ).hexdigest()
    with _REQUEST_THROTTLES_LOCK:
        throttle = _REQUEST_THROTTLES.get(key)
        if throttle is None:
            throttle = _RequestThrottle()
            _REQUEST_THROTTLES[key] = throttle
        return throttle


def _resolve_min_request_interval_seconds(
    *,
    settings: BrokerSettings,
    transport: HttpTransport | None,
    explicit_seconds: float | None,
) -> float:
    if explicit_seconds is not None:
        if explicit_seconds < 0:
            raise ValueError("min_request_interval_seconds must be non-negative")
        return explicit_seconds
    if transport is not None:
        return 0.0
    return _default_min_request_interval_seconds_for(settings.environment)


def _resolve_order_history_lookup_delays_seconds(
    *,
    settings: BrokerSettings,
    explicit_seconds: float | None,
) -> tuple[float, ...]:
    base_interval_seconds = (
        explicit_seconds
        if explicit_seconds is not None
        else _default_min_request_interval_seconds_for(settings.environment)
    )
    return tuple(
        base_interval_seconds * multiplier
        for multiplier in KIS_ORDER_HISTORY_LOOKUP_DELAY_MULTIPLIERS
    )


def _default_min_request_interval_seconds_for(environment: str) -> float:
    if environment == "live":
        return KIS_LIVE_MIN_REQUEST_INTERVAL_SECONDS
    return KIS_DEFAULT_MIN_REQUEST_INTERVAL_SECONDS


def _resolve_as_of(output: Mapping[str, object], fallback: datetime) -> datetime:
    base_date = output.get("stck_bsop_date")
    current_time = output.get("stck_cntg_hour")
    if isinstance(base_date, str) and isinstance(current_time, str):
        normalized_date = base_date.strip()
        normalized_time = current_time.strip()
        if len(normalized_date) == 8 and len(normalized_time) == 6:
            try:
                parsed = datetime.strptime(
                    normalized_date + normalized_time,
                    "%Y%m%d%H%M%S",
                )
            except ValueError:
                return fallback
            return parsed.replace(tzinfo=KST)
    return fallback


def _require_mapping(
    payload: Mapping[str, object],
    key: str,
) -> Mapping[str, object]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise KoreaInvestmentBrokerError(f"{key} must be an object")
    return value


def _require_sequence(
    payload: Mapping[str, object],
    key: str,
) -> Sequence[object]:
    value = payload.get(key)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise KoreaInvestmentBrokerError(f"{key} must be an array")
    return value


def _parse_balance_holdings(payload: Mapping[str, object]) -> tuple[Holding, ...]:
    rows = _require_sequence(payload, "output1")
    holdings: list[Holding] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise KoreaInvestmentBrokerError("output1 entries must be mappings")

        quantity = _coerce_int(row.get("hldg_qty"), field_name="hldg_qty")
        if quantity <= 0:
            continue

        holdings.append(
            normalize_holding(
                {
                    "symbol": _coerce_string(row.get("pdno"), field_name="pdno"),
                    "quantity": quantity,
                    "average_price": row.get("pchs_avg_pric"),
                    "current_price": row.get("prpr"),
                },
            ),
        )
    return tuple(sorted(holdings, key=lambda holding: holding.symbol))


def _parse_holding_performance(
    row: Mapping[str, object],
) -> HoldingPerformance | None:
    quantity = _coerce_int(row.get("hldg_qty"), field_name="hldg_qty")
    if quantity <= 0:
        return None
    symbol = _coerce_string(row.get("pdno"), field_name="pdno")
    average_price = _coerce_decimal(
        row.get("pchs_avg_pric"),
        field_name="pchs_avg_pric",
    )
    current_price = _coerce_decimal(row.get("prpr"), field_name="prpr")
    purchase_amount = _row_decimal(
        row,
        "pchs_amt",
        fallback=average_price * Decimal(quantity),
    )
    evaluation_amount = _row_decimal(
        row,
        "evlu_amt",
        fallback=current_price * Decimal(quantity),
    )
    profit_loss = _row_decimal(
        row,
        "evlu_pfls_amt",
        fallback=evaluation_amount - purchase_amount,
    )
    profit_loss_rate = _row_decimal(
        row,
        "evlu_pfls_rt",
        fallback=_calculate_profit_loss_rate(profit_loss, purchase_amount),
    )
    return HoldingPerformance(
        symbol=symbol,
        quantity=quantity,
        average_price=average_price,
        current_price=current_price,
        purchase_amount=purchase_amount,
        evaluation_amount=evaluation_amount,
        profit_loss=profit_loss,
        profit_loss_rate=profit_loss_rate,
    )


def _balance_summary_mapping(payload: Mapping[str, object]) -> Mapping[str, object]:
    summary = payload.get("output2")
    if isinstance(summary, Mapping):
        return summary
    if isinstance(summary, Sequence) and not isinstance(
        summary,
        (str, bytes, bytearray),
    ):
        if not summary:
            return {}
        first = summary[0]
        if isinstance(first, Mapping):
            return first
    raise KoreaInvestmentBrokerError("output2 must be an object or object array")


def _row_decimal(
    row: Mapping[str, object],
    key: str,
    *,
    fallback: Decimal,
) -> Decimal:
    value = row.get(key)
    if value is None:
        return fallback
    return _coerce_decimal(value, field_name=key)


def _summary_decimal(
    summary: Mapping[str, object],
    *keys: str,
    fallback: Decimal,
) -> Decimal:
    for key in keys:
        value = summary.get(key)
        if value is not None:
            return _coerce_decimal(value, field_name=key)
    return fallback


def _calculate_profit_loss_rate(
    profit_loss: Decimal,
    purchase_amount: Decimal,
) -> Decimal:
    if purchase_amount == Decimal("0"):
        return Decimal("0")
    return (profit_loss / purchase_amount) * Decimal("100")


def _coerce_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise KoreaInvestmentBrokerError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise KoreaInvestmentBrokerError(f"{field_name} must not be blank")
    return normalized


def _coerce_decimal(value: object, *, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return Decimal(value)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise KoreaInvestmentBrokerError(f"{field_name} must not be blank")
        try:
            return Decimal(normalized)
        except InvalidOperation as error:
            raise KoreaInvestmentBrokerError(
                f"{field_name} must be decimal-compatible"
            ) from error
    raise KoreaInvestmentBrokerError(f"{field_name} must be decimal-compatible")


def _coerce_int(value: object, *, field_name: str) -> int:
    normalized = _coerce_optional_int(value)
    if normalized is None:
        raise KoreaInvestmentBrokerError(f"{field_name} must be an integer")
    return normalized


def _coerce_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        if value != value.to_integral_value():
            return None
        return int(value)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        try:
            return int(normalized)
        except ValueError:
            return None
    return None


def _split_account(account: str, *, environment: str) -> tuple[str, str]:
    normalized = account.strip()
    if not normalized:
        raise KoreaInvestmentBrokerError("account must not be blank")

    account_pattern = (
        PAPER_ACCOUNT_PATTERN if environment == "paper" else LIVE_ACCOUNT_PATTERN
    )
    if account_pattern.fullmatch(normalized) is None:
        if environment == "paper":
            raise KoreaInvestmentBrokerError(
                "paper account must be 8 digits, or 8 digits + 2 digits with optional hyphen",
            )
        raise KoreaInvestmentBrokerError(
            "live account must be 8 digits, or 8 digits + 2 digits with optional hyphen",
        )

    if "-" in normalized:
        cano, acnt_prdt_cd = normalized.split("-", 1)
    elif len(normalized) == 8:
        cano, acnt_prdt_cd = normalized, PAPER_PRODUCT_CODE
    else:
        cano, acnt_prdt_cd = normalized[:8], normalized[8:]
    return cano, acnt_prdt_cd
