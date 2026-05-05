from __future__ import annotations

import base64
import hashlib
import json
import time
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from http.client import RemoteDisconnected
from pathlib import Path
from urllib.error import URLError
from urllib.parse import parse_qs
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import pytest

from autotrade.broker import BrokerReader
from autotrade.broker.korea_investment import HttpRequest
from autotrade.broker.korea_investment import HttpResponse
from autotrade.broker.korea_investment import KoreaInvestmentBarSource
from autotrade.broker.korea_investment import KoreaInvestmentBrokerError
from autotrade.broker.korea_investment import KoreaInvestmentBrokerReader
from autotrade.broker.korea_investment import KoreaInvestmentBrokerTrader
from autotrade.broker.korea_investment import KIS_DEFAULT_MIN_REQUEST_INTERVAL_SECONDS
from autotrade.broker.korea_investment import KIS_LIVE_MIN_REQUEST_INTERVAL_SECONDS
from autotrade.broker.korea_investment import KIS_TRANSPORT_RETRY_DELAYS_SECONDS
from autotrade.broker.korea_investment import _resolve_min_request_interval_seconds
from autotrade.broker.korea_investment import _split_account
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
from autotrade.common import Quote
from autotrade.config.models import BrokerEnvironment
from autotrade.config import BrokerSettings
from autotrade.data import Bar
from autotrade.data import Timeframe


@pytest.fixture(autouse=True)
def isolate_kis_token_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))


def test_korea_investment_broker_reader_returns_standard_models() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {"access_token": "token-123"},
            ),
            ("GET", "/uapi/domestic-stock/v1/quotations/inquire-price"): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "stck_bsop_date": "20260411",
                        "stck_cntg_hour": "090000",
                        "stck_prpr": "12345.67",
                    },
                },
            ),
            ("GET", "/uapi/domestic-stock/v1/trading/inquire-balance"): json_response(
                {
                    "rt_cd": "0",
                    "output1": [
                        {
                            "pdno": "357870",
                            "hldg_qty": "2",
                            "pchs_avg_pric": "10000",
                            "prpr": "10100",
                        },
                        {
                            "pdno": "069500",
                            "hldg_qty": "1",
                            "pchs_avg_pric": "9000",
                            "prpr": "9500",
                        },
                    ],
                },
            ),
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
            ): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "ord_psbl_cash": "133250",
                        "nrcvb_buy_qty": "13",
                    },
                },
            ),
        },
    )
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert isinstance(reader, BrokerReader)

    quote = reader.get_quote("069500")
    holdings = reader.get_holdings()
    capacity = reader.get_order_capacity("114800", Decimal("10250"))

    assert quote == Quote(
        symbol="069500",
        price=Decimal("12345.67"),
        as_of=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )
    assert holdings == (
        Holding(
            symbol="069500",
            quantity=1,
            average_price=Decimal("9000"),
            current_price=Decimal("9500"),
        ),
        Holding(
            symbol="357870",
            quantity=2,
            average_price=Decimal("10000"),
            current_price=Decimal("10100"),
        ),
    )
    assert capacity == OrderCapacity(
        symbol="114800",
        order_price=Decimal("10250"),
        max_orderable_quantity=13,
        cash_available=Decimal("133250"),
    )
    assert [request.method for request in transport.requests] == [
        "POST",
        "GET",
        "GET",
        "GET",
    ]
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "/uapi/domestic-stock/v1/trading/inquire-balance",
        "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
    ]
    assert json.loads(transport.requests[0].body.decode("utf-8")) == {
        "grant_type": "client_credentials",
        "appkey": "demo-key",
        "appsecret": "demo-secret",
    }
    assert transport.requests[1].headers["authorization"] == "Bearer token-123"
    assert parse_qs(urlsplit(transport.requests[1].url).query) == {
        "FID_COND_MRKT_DIV_CODE": ["J"],
        "FID_INPUT_ISCD": ["069500"],
    }
    assert parse_qs(urlsplit(transport.requests[2].url).query)["PRCS_DVSN"] == ["00"]
    assert parse_qs(urlsplit(transport.requests[3].url).query) == {
        "CANO": ["12345678"],
        "ACNT_PRDT_CD": ["01"],
        "PDNO": ["114800"],
        "ORD_UNPR": ["10250"],
        "ORD_DVSN": ["01"],
        "CMA_EVLU_AMT_ICLD_YN": ["N"],
        "OVRS_ICLD_YN": ["N"],
    }


def test_korea_investment_broker_reader_returns_account_performance() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            ("GET", "/uapi/domestic-stock/v1/trading/inquire-balance"): json_response(
                {
                    "rt_cd": "0",
                    "output1": [
                        {
                            "pdno": "357870",
                            "hldg_qty": "2",
                            "pchs_avg_pric": "10000",
                            "prpr": "10100",
                            "pchs_amt": "20000",
                            "evlu_amt": "20200",
                            "evlu_pfls_amt": "200",
                            "evlu_pfls_rt": "1.00",
                        },
                        {
                            "pdno": "069500",
                            "hldg_qty": "1",
                            "pchs_avg_pric": "9000",
                            "prpr": "9500",
                            "pchs_amt": "9000",
                            "evlu_amt": "9500",
                            "evlu_pfls_amt": "500",
                            "evlu_pfls_rt": "5.56",
                        },
                    ],
                    "output2": [
                        {
                            "pchs_amt_smtl_amt": "29000",
                            "evlu_amt_smtl_amt": "29700",
                            "evlu_pfls_smtl_amt": "700",
                            "evlu_pfls_rt": "2.41",
                            "dnca_tot_amt": "133250",
                        }
                    ],
                },
            ),
        },
    )
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert reader.get_account_performance() == AccountPerformance(
        total_purchase_amount=Decimal("29000"),
        total_evaluation_amount=Decimal("29700"),
        total_profit_loss=Decimal("700"),
        total_profit_loss_rate=Decimal("2.41"),
        cash_available=Decimal("133250"),
        holdings=(
            HoldingPerformance(
                symbol="069500",
                quantity=1,
                average_price=Decimal("9000"),
                current_price=Decimal("9500"),
                purchase_amount=Decimal("9000"),
                evaluation_amount=Decimal("9500"),
                profit_loss=Decimal("500"),
                profit_loss_rate=Decimal("5.56"),
            ),
            HoldingPerformance(
                symbol="357870",
                quantity=2,
                average_price=Decimal("10000"),
                current_price=Decimal("10100"),
                purchase_amount=Decimal("20000"),
                evaluation_amount=Decimal("20200"),
                profit_loss=Decimal("200"),
                profit_loss_rate=Decimal("1.00"),
            ),
        ),
    )


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (URLError("connection reset"), "network request failed"),
        (
            RemoteDisconnected("Remote end closed connection without response"),
            "network request failed",
        ),
        (TimeoutError("timed out"), "request timed out"),
    ],
)
def test_korea_investment_broker_reader_normalizes_transport_failures(
    error: Exception,
    message: str,
) -> None:
    def broken_transport(request: HttpRequest) -> HttpResponse:
        raise error

    sleeps: list[float] = []
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=broken_transport,
        sleep=sleeps.append,
    )

    with pytest.raises(KoreaInvestmentBrokerError, match=message):
        reader.get_quote("069500")
    assert sleeps == list(KIS_TRANSPORT_RETRY_DELAYS_SECONDS)


def test_korea_investment_broker_reader_retries_transient_network_failure() -> None:
    request_paths: list[str] = []
    quote_attempts = 0

    def flaky_transport(request: HttpRequest) -> HttpResponse:
        nonlocal quote_attempts
        path = urlsplit(request.url).path
        request_paths.append(path)
        if path == "/oauth2/tokenP":
            return json_response({"access_token": "token-123"})
        if path == "/uapi/domestic-stock/v1/quotations/inquire-price":
            quote_attempts += 1
            if quote_attempts == 1:
                raise URLError(OSError(-3, "Temporary failure in name resolution"))
            return json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "stck_bsop_date": "20260411",
                        "stck_cntg_hour": "090000",
                        "stck_prpr": "12345",
                    },
                },
            )
        raise AssertionError(f"unexpected request path: {path}")

    sleeps: list[float] = []
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=flaky_transport,
        sleep=sleeps.append,
    )

    quote = reader.get_quote("069500")

    assert quote.price == Decimal("12345")
    assert sleeps == [KIS_TRANSPORT_RETRY_DELAYS_SECONDS[0]]
    assert request_paths == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
    ]


def test_korea_investment_broker_reader_retries_remote_disconnected() -> None:
    request_paths: list[str] = []
    quote_attempts = 0

    def flaky_transport(request: HttpRequest) -> HttpResponse:
        nonlocal quote_attempts
        path = urlsplit(request.url).path
        request_paths.append(path)
        if path == "/oauth2/tokenP":
            return json_response({"access_token": "token-123"})
        if path == "/uapi/domestic-stock/v1/quotations/inquire-price":
            quote_attempts += 1
            if quote_attempts == 1:
                raise RemoteDisconnected(
                    "Remote end closed connection without response"
                )
            return json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "stck_bsop_date": "20260411",
                        "stck_cntg_hour": "090000",
                        "stck_prpr": "12345",
                    },
                },
            )
        raise AssertionError(f"unexpected request path: {path}")

    sleeps: list[float] = []
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=flaky_transport,
        sleep=sleeps.append,
    )

    quote = reader.get_quote("069500")

    assert quote.price == Decimal("12345")
    assert sleeps == [KIS_TRANSPORT_RETRY_DELAYS_SECONDS[0]]
    assert request_paths == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
    ]


def test_korea_investment_broker_reader_normalizes_invalid_json_response() -> None:
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=RecordingTransport(
            {
                ("POST", "/oauth2/tokenP"): json_response(
                    {"access_token": "token-123"}
                ),
                ("GET", "/uapi/domestic-stock/v1/quotations/inquire-price"): (
                    HttpResponse(
                        status=200,
                        headers={"content-type": "application/json"},
                        body=b"{invalid-json",
                    )
                ),
            }
        ),
    )

    with pytest.raises(
        KoreaInvestmentBrokerError,
        match="response body is not valid JSON",
    ):
        reader.get_quote("069500")


def test_korea_investment_logs_raw_http_exchange_for_paper_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log_dir = tmp_path / "logs"
    monkeypatch.setenv("AUTOTRADE_LOG_DIR", str(log_dir))
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {"access_token": "token-123"},
            ),
            ("GET", "/uapi/domestic-stock/v1/quotations/inquire-price"): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "stck_bsop_date": "20260411",
                        "stck_cntg_hour": "090000",
                        "stck_prpr": "12345",
                    },
                },
            ),
        },
    )
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    reader.get_quote("069500")

    log_path = log_dir / "kis_raw_20260411.log"
    assert log_path.exists()
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    token_entry = json.loads(lines[0])
    quote_entry = json.loads(lines[1])
    assert token_entry["request_headers"]["appkey"] == "[REDACTED]"
    assert token_entry["request_headers"]["appsecret"] == "[REDACTED]"
    assert quote_entry["request_headers"]["authorization"] == "[REDACTED]"
    assert quote_entry["response_body"]["output"]["stck_prpr"] == "12345"


def test_korea_investment_broker_reader_raises_when_token_is_missing() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {"rt_cd": "0"},
            ),
        },
    )
    reader = KoreaInvestmentBrokerReader(_make_settings(), transport=transport)

    with pytest.raises(KoreaInvestmentBrokerError, match="access_token"):
        reader.get_quote("069500")


def test_korea_investment_broker_reader_includes_http_error_body_details() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {
                    "msg_cd": "EGW00123",
                    "msg1": "token blocked",
                },
                status=403,
            ),
        },
    )
    reader = KoreaInvestmentBrokerReader(_make_settings(), transport=transport)

    with pytest.raises(
        KoreaInvestmentBrokerError,
        match=r"HTTP 403.*EGW00123 - token blocked",
    ):
        reader.get_quote("069500")


def test_korea_investment_broker_reader_reuses_cached_token_file(
    tmp_path: Path,
) -> None:
    cache_path = tmp_path / "token-cache.json"
    cache_path.write_text(
        json.dumps(
            {
                "access_token": "cached-token",
                "expires_at": "2026-04-12T09:00:00+09:00",
            }
        ),
        encoding="utf-8",
    )
    transport = RecordingTransport(
        {
            ("GET", "/uapi/domestic-stock/v1/quotations/inquire-price"): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "stck_bsop_date": "20260411",
                        "stck_cntg_hour": "090000",
                        "stck_prpr": "12345.67",
                    },
                },
            ),
        },
    )
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=transport,
        token_cache_path=cache_path,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    quote = reader.get_quote("069500")

    assert quote.symbol == "069500"
    assert [request.method for request in transport.requests] == ["GET"]
    assert transport.requests[0].headers["authorization"] == "Bearer cached-token"


def test_korea_investment_broker_reader_writes_token_cache_after_fetch(
    tmp_path: Path,
) -> None:
    cache_path = tmp_path / "token-cache.json"
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=RecordingTransport(
            {
                ("POST", "/oauth2/tokenP"): json_response(
                    {
                        "access_token": "token-123",
                        "access_token_token_expired": "2026-04-12 09:00:00",
                    },
                ),
                (
                    "GET",
                    "/uapi/domestic-stock/v1/quotations/inquire-price",
                ): json_response(
                    {
                        "rt_cd": "0",
                        "output": {
                            "stck_bsop_date": "20260411",
                            "stck_cntg_hour": "090000",
                            "stck_prpr": "12345.67",
                        },
                    },
                ),
            },
        ),
        token_cache_path=cache_path,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    reader.get_quote("069500")

    cached_payload = json.loads(cache_path.read_text(encoding="utf-8"))
    assert cached_payload == {
        "access_token": "token-123",
        "expires_at": "2026-04-12T09:00:00+09:00",
    }


def test_korea_investment_broker_reader_refreshes_expired_cached_token(
    tmp_path: Path,
) -> None:
    cache_path = tmp_path / "token-cache.json"
    expired_at = datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    cache_path.write_text(
        json.dumps(
            {
                "access_token": "expired-token",
                "expires_at": expired_at.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {
                    "access_token": "fresh-token",
                    "expires_in": 7200,
                },
            ),
            ("GET", "/uapi/domestic-stock/v1/quotations/inquire-price"): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "stck_bsop_date": "20260411",
                        "stck_cntg_hour": "090000",
                        "stck_prpr": "12345.67",
                    },
                },
            ),
        },
    )
    now = expired_at + timedelta(minutes=2)
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=transport,
        token_cache_path=cache_path,
        clock=lambda: now,
    )

    reader.get_quote("069500")

    assert [request.method for request in transport.requests] == ["POST", "GET"]
    assert transport.requests[1].headers["authorization"] == "Bearer fresh-token"


def test_korea_investment_broker_reader_refreshes_expired_in_memory_token() -> None:
    current_time = datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): [
                json_response(
                    {
                        "access_token": "token-1",
                        "expires_in": 300,
                    },
                ),
                json_response(
                    {
                        "access_token": "token-2",
                        "expires_in": 300,
                    },
                ),
            ],
            ("GET", "/uapi/domestic-stock/v1/quotations/inquire-price"): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "stck_bsop_date": "20260411",
                        "stck_cntg_hour": "090000",
                        "stck_prpr": "12345.67",
                    },
                },
            ),
            ("GET", "/uapi/domestic-stock/v1/trading/inquire-balance"): json_response(
                {
                    "rt_cd": "0",
                    "output1": [
                        {
                            "pdno": "069500",
                            "hldg_qty": "1",
                            "pchs_avg_pric": "9000",
                            "prpr": "9500",
                        }
                    ],
                },
            ),
        },
    )
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=transport,
        clock=lambda: current_time,
    )

    reader.get_quote("069500")
    current_time += timedelta(minutes=6)
    holdings = reader.get_holdings()

    assert holdings == (
        Holding(
            symbol="069500",
            quantity=1,
            average_price=Decimal("9000"),
            current_price=Decimal("9500"),
        ),
    )
    assert [request.method for request in transport.requests] == [
        "POST",
        "GET",
        "POST",
        "GET",
    ]
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/trading/inquire-balance",
    ]
    assert transport.requests[1].headers["authorization"] == "Bearer token-1"
    assert transport.requests[3].headers["authorization"] == "Bearer token-2"


def test_korea_investment_broker_reader_retries_after_token_expiration_response() -> (
    None
):
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): [
                json_response({"access_token": "token-1"}),
                json_response({"access_token": "token-2"}),
            ],
            ("GET", "/uapi/domestic-stock/v1/quotations/inquire-price"): [
                json_response(
                    {
                        "msg_cd": "EGW00123",
                        "msg1": "expired token",
                    },
                    status=500,
                ),
                json_response(
                    {
                        "rt_cd": "0",
                        "output": {
                            "stck_bsop_date": "20260411",
                            "stck_cntg_hour": "090000",
                            "stck_prpr": "12345.67",
                        },
                    },
                ),
            ],
        },
    )
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    quote = reader.get_quote("069500")

    assert quote == Quote(
        symbol="069500",
        price=Decimal("12345.67"),
        as_of=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
    ]
    assert transport.requests[1].headers["authorization"] == "Bearer token-1"
    assert transport.requests[3].headers["authorization"] == "Bearer token-2"


def test_korea_investment_broker_trader_submits_limit_order_with_hashkey() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            ("POST", "/uapi/hashkey"): json_response({"HASH": "hash-123"}),
            ("POST", "/uapi/domestic-stock/v1/trading/order-cash"): json_response(
                {
                    "rt_cd": "0",
                    "output": {"ODNO": "order-1"},
                }
            ),
        }
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    order = trader.submit_order(
        OrderRequest(
            request_id="submit-1",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=3,
            limit_price=Decimal("10000"),
            requested_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        )
    )

    assert order == ExecutionOrder(
        order_id="order-1",
        symbol="069500",
        side=OrderSide.BUY,
        quantity=3,
        limit_price=Decimal("10000"),
        status=OrderStatus.ACKNOWLEDGED,
        created_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        updated_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/hashkey",
        "/uapi/domestic-stock/v1/trading/order-cash",
    ]
    assert transport.requests[2].headers["hashkey"] == "hash-123"
    assert transport.requests[2].headers["tr_id"] == "VTTC0802U"
    assert transport.requests[2].headers["authorization"] == "Bearer token-123"
    assert json.loads(transport.requests[2].body.decode("utf-8")) == {
        "CANO": "12345678",
        "ACNT_PRDT_CD": "01",
        "PDNO": "069500",
        "ORD_DVSN": "00",
        "ORD_QTY": "3",
        "ORD_UNPR": "10000",
    }


def test_korea_investment_broker_trader_rejects_invalid_tick_before_submit() -> None:
    transport = RecordingTransport({})
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    with pytest.raises(
        KoreaInvestmentBrokerError,
        match=(
            "invalid KRX order tick price .*symbol=005930 .*side=SELL "
            ".*limit_price=222750 .*normalized_price=222500"
        ),
    ):
        trader.submit_order(
            OrderRequest(
                request_id="submit-invalid-tick",
                symbol="005930",
                side=OrderSide.SELL,
                quantity=2,
                limit_price=Decimal("222750"),
                requested_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
            )
        )

    assert transport.requests == []


def test_korea_investment_broker_trader_does_not_retry_order_transport_failure() -> (
    None
):
    request_paths: list[str] = []

    def broken_order_transport(request: HttpRequest) -> HttpResponse:
        path = urlsplit(request.url).path
        request_paths.append(path)
        if path == "/oauth2/tokenP":
            return json_response({"access_token": "token-123"})
        if path == "/uapi/hashkey":
            return json_response({"HASH": "hash-123"})
        if path == "/uapi/domestic-stock/v1/trading/order-cash":
            raise URLError("connection reset")
        raise AssertionError(f"unexpected request path: {path}")

    sleeps: list[float] = []
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=broken_order_transport,
        sleep=sleeps.append,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    with pytest.raises(KoreaInvestmentBrokerError, match="network request failed"):
        trader.submit_order(
            OrderRequest(
                request_id="submit-1",
                symbol="069500",
                side=OrderSide.BUY,
                quantity=3,
                limit_price=Decimal("10000"),
                requested_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
            )
        )

    assert sleeps == []
    assert request_paths == [
        "/oauth2/tokenP",
        "/uapi/hashkey",
        "/uapi/domestic-stock/v1/trading/order-cash",
    ]


def test_korea_investment_broker_trader_amends_and_cancels_using_order_history() -> (
    None
):
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            ("GET", "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"): [
                json_response(
                    {
                        "rt_cd": "0",
                        "output1": [_order_history_row(order_id="order-1")],
                    }
                ),
                json_response(
                    {
                        "rt_cd": "0",
                        "output1": [
                            _order_history_row(
                                order_id="order-2",
                                origin_order_id="order-1",
                                limit_price="10100",
                            )
                        ],
                    }
                ),
            ],
            ("POST", "/uapi/hashkey"): [
                json_response({"HASH": "hash-amend"}),
                json_response({"HASH": "hash-cancel"}),
            ],
            ("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl"): [
                json_response(
                    {
                        "rt_cd": "0",
                        "output": {"ODNO": "order-2"},
                    }
                ),
                json_response(
                    {
                        "rt_cd": "0",
                        "output": {"ODNO": "order-2"},
                    }
                ),
            ],
        }
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 9, 2, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    amended = trader.amend_order(
        OrderAmendRequest(
            request_id="amend-1",
            order_id="order-1",
            limit_price=Decimal("10100"),
            requested_at=datetime(2026, 4, 11, 9, 1, tzinfo=ZoneInfo("Asia/Seoul")),
        )
    )
    canceled = trader.cancel_order(
        OrderCancelRequest(
            request_id="cancel-1",
            order_id="order-2",
            requested_at=datetime(2026, 4, 11, 9, 2, tzinfo=ZoneInfo("Asia/Seoul")),
        )
    )

    assert amended == ExecutionOrder(
        order_id="order-2",
        symbol="069500",
        side=OrderSide.BUY,
        quantity=3,
        limit_price=Decimal("10100"),
        status=OrderStatus.ACKNOWLEDGED,
        created_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        updated_at=datetime(2026, 4, 11, 9, 1, tzinfo=ZoneInfo("Asia/Seoul")),
    )
    assert canceled == ExecutionOrder(
        order_id="order-2",
        symbol="069500",
        side=OrderSide.BUY,
        quantity=3,
        limit_price=Decimal("10100"),
        status=OrderStatus.CANCELED,
        created_at=datetime(2026, 4, 11, 9, 1, tzinfo=ZoneInfo("Asia/Seoul")),
        updated_at=datetime(2026, 4, 11, 9, 2, tzinfo=ZoneInfo("Asia/Seoul")),
    )
    amend_request = transport.requests[3]
    cancel_request = transport.requests[5]
    assert amend_request.headers["hashkey"] == "hash-amend"
    assert cancel_request.headers["hashkey"] == "hash-cancel"
    assert json.loads(amend_request.body.decode("utf-8")) == {
        "CANO": "12345678",
        "ACNT_PRDT_CD": "01",
        "KRX_FWDG_ORD_ORGNO": "06010",
        "ORGN_ODNO": "order-1",
        "ORD_DVSN": "00",
        "RVSE_CNCL_DVSN_CD": "01",
        "ORD_QTY": "0",
        "ORD_UNPR": "10100",
        "QTY_ALL_ORD_YN": "Y",
    }
    assert json.loads(cancel_request.body.decode("utf-8")) == {
        "CANO": "12345678",
        "ACNT_PRDT_CD": "01",
        "KRX_FWDG_ORD_ORGNO": "06010",
        "ORGN_ODNO": "order-2",
        "ORD_DVSN": "00",
        "RVSE_CNCL_DVSN_CD": "02",
        "ORD_QTY": "0",
        "ORD_UNPR": "0",
        "QTY_ALL_ORD_YN": "Y",
    }


def test_korea_investment_broker_trader_returns_cumulative_fill_snapshot() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            ): json_response(
                {
                    "rt_cd": "0",
                    "output1": [
                        _order_history_row(
                            order_id="order-1",
                            filled_quantity="2",
                            average_price="10050",
                        )
                    ],
                }
            ),
        }
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 9, 3, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    fills = trader.get_fills("order-1")

    assert fills == (
        ExecutionFill(
            fill_id="order-1:cumulative",
            order_id="order-1",
            symbol="069500",
            quantity=2,
            price=Decimal("10050"),
            filled_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        ),
    )


def test_korea_investment_broker_trader_uses_filtered_summary_for_missing_fill_row() -> (
    None
):
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            ): json_response(
                {
                    "rt_cd": "0",
                    "output1": [],
                    "output2": {
                        "tot_ord_qty": "1",
                        "tot_ccld_qty": "1",
                        "tot_ccld_amt": "98100",
                    },
                }
            ),
        }
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 24, 10, 30, tzinfo=ZoneInfo("Asia/Seoul")),
    )
    order = ExecutionOrder(
        order_id="0000011960",
        symbol="069500",
        side=OrderSide.BUY,
        quantity=1,
        limit_price=Decimal("98100"),
        status=OrderStatus.ACKNOWLEDGED,
        created_at=datetime(2026, 4, 24, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        updated_at=datetime(2026, 4, 24, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        filled_quantity=0,
    )

    fills = trader.get_fills_for_order(order)

    assert fills == (
        ExecutionFill(
            fill_id="0000011960:cumulative",
            order_id="0000011960",
            symbol="069500",
            quantity=1,
            price=Decimal("98100"),
            filled_at=datetime(2026, 4, 24, 10, 30, tzinfo=ZoneInfo("Asia/Seoul")),
        ),
    )
    assert parse_qs(urlsplit(transport.requests[1].url).query)["ODNO"] == ["11960"]


def test_korea_investment_broker_trader_keeps_tracked_order_when_history_missing() -> (
    None
):
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            ): json_response({"rt_cd": "0", "output1": [], "output2": {}}),
        }
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 24, 10, 30, tzinfo=ZoneInfo("Asia/Seoul")),
    )
    order = ExecutionOrder(
        order_id="0000011960",
        symbol="069500",
        side=OrderSide.BUY,
        quantity=1,
        limit_price=Decimal("98100"),
        status=OrderStatus.ACKNOWLEDGED,
        created_at=datetime(2026, 4, 24, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        updated_at=datetime(2026, 4, 24, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        filled_quantity=0,
    )

    assert trader.get_fills_for_order(order) == ()


def test_korea_investment_broker_trader_returns_realtime_fill_notice_without_history() -> (
    None
):
    websocket = ScriptedWebSocketConnection(
        [
            _fill_notice_ack_message(tr_id="H0STCNI9"),
            _fill_notice_data_message(
                tr_id="H0STCNI9",
                payload=_fill_notice_payload(
                    order_id="order-1",
                    quantity="2",
                    price="10050",
                    filled_at="090001",
                ),
            ),
        ]
    )
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            ("POST", "/oauth2/Approval"): json_response(
                {"approval_key": "approval-123"}
            ),
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            ): json_response({"rt_cd": "0", "output1": []}),
        }
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(hts_id="my-hts-id"),
        transport=transport,
        websocket_connector=lambda url: websocket,
        clock=lambda: datetime(2026, 4, 11, 9, 3, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    try:
        fills = _wait_for_realtime_fills(trader, "order-1")
    finally:
        trader.close()

    assert fills == (
        ExecutionFill(
            fill_id=f"order-1:ws:{_fill_notice_hash('order-1', '2', '10050', '090001')}",
            order_id="order-1",
            symbol="069500",
            quantity=2,
            price=Decimal("10050"),
            filled_at=datetime(2026, 4, 11, 9, 0, 1, tzinfo=ZoneInfo("Asia/Seoul")),
        ),
    )
    assert json.loads(websocket.sent_texts[0]) == {
        "header": {
            "approval_key": "approval-123",
            "content-type": "utf-8",
            "custtype": "P",
            "tr_type": "1",
        },
        "body": {
            "input": {
                "tr_id": "H0STCNI9",
                "tr_key": "my-hts-id",
            }
        },
    }


def test_korea_investment_broker_trader_returns_realtime_fill_before_cumulative_fill() -> (
    None
):
    websocket = ScriptedWebSocketConnection(
        [
            _fill_notice_ack_message(tr_id="H0STCNI9"),
            _fill_notice_data_message(
                tr_id="H0STCNI9",
                payload=_fill_notice_payload(
                    order_id="order-1",
                    quantity="2",
                    price="10050",
                    filled_at="090001",
                ),
            ),
        ]
    )
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            ("POST", "/oauth2/Approval"): json_response(
                {"approval_key": "approval-123"}
            ),
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            ): json_response(
                {
                    "rt_cd": "0",
                    "output1": [
                        _order_history_row(
                            order_id="order-1",
                            filled_quantity="5",
                            average_price="10080",
                        )
                    ],
                }
            ),
        }
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(hts_id="my-hts-id"),
        transport=transport,
        websocket_connector=lambda url: websocket,
        clock=lambda: datetime(2026, 4, 11, 9, 3, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    try:
        fills = _wait_for_realtime_fills(trader, "order-1", expected_count=2)
    finally:
        trader.close()

    assert fills[0] == ExecutionFill(
        fill_id=f"order-1:ws:{_fill_notice_hash('order-1', '2', '10050', '090001')}",
        order_id="order-1",
        symbol="069500",
        quantity=2,
        price=Decimal("10050"),
        filled_at=datetime(2026, 4, 11, 9, 0, 1, tzinfo=ZoneInfo("Asia/Seoul")),
    )
    assert fills[1] == ExecutionFill(
        fill_id="order-1:cumulative",
        order_id="order-1",
        symbol="069500",
        quantity=5,
        price=Decimal("10080"),
        filled_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )


def test_korea_investment_broker_trader_retries_when_order_history_is_delayed() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            ("GET", "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"): [
                json_response({"rt_cd": "0", "output1": []}),
                json_response(
                    {
                        "rt_cd": "0",
                        "output1": [_order_history_row(order_id="order-1")],
                    }
                ),
            ],
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
            ): json_response(
                {
                    "rt_cd": "1",
                    "msg_cd": "90000000",
                    "msg1": "모의투자에서는 해당업무가 제공되지 않습니다.",
                }
            ),
            ("POST", "/uapi/hashkey"): json_response({"HASH": "hash-amend"}),
            ("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl"): json_response(
                {
                    "rt_cd": "0",
                    "output": {"ODNO": "order-2"},
                }
            ),
        }
    )
    sleeps: list[float] = []
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        sleep=sleeps.append,
        clock=lambda: datetime(2026, 4, 11, 9, 1, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    amended = trader.amend_order(
        OrderAmendRequest(
            request_id="amend-1",
            order_id="order-1",
            limit_price=Decimal("10100"),
            requested_at=datetime(2026, 4, 11, 9, 1, tzinfo=ZoneInfo("Asia/Seoul")),
        )
    )

    assert amended.order_id == "order-2"
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
        "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
        "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
        "/uapi/hashkey",
        "/uapi/domestic-stock/v1/trading/order-rvsecncl",
    ]
    assert sleeps == [KIS_DEFAULT_MIN_REQUEST_INTERVAL_SECONDS]


def test_korea_investment_broker_trader_matches_zero_padded_order_ids() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            ): json_response(
                {
                    "rt_cd": "0",
                    "output1": [_order_history_row(order_id="12093")],
                }
            ),
            ("POST", "/uapi/hashkey"): json_response({"HASH": "hash-amend"}),
            ("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl"): json_response(
                {
                    "rt_cd": "0",
                    "output": {"ODNO": "12094"},
                }
            ),
        }
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 9, 1, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    amended = trader.amend_order(
        OrderAmendRequest(
            request_id="amend-1",
            order_id="0000012093",
            limit_price=Decimal("10100"),
            requested_at=datetime(2026, 4, 11, 9, 1, tzinfo=ZoneInfo("Asia/Seoul")),
        )
    )

    assert amended.order_id == "12094"
    amend_request = transport.requests[-1]
    assert json.loads(amend_request.body.decode("utf-8"))["ORGN_ODNO"] == "12093"


def test_korea_investment_broker_trader_uses_submission_cache_for_immediate_amend() -> (
    None
):
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            ("POST", "/uapi/hashkey"): [
                json_response({"HASH": "hash-submit"}),
                json_response({"HASH": "hash-amend"}),
            ],
            ("POST", "/uapi/domestic-stock/v1/trading/order-cash"): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "ODNO": "0000013438",
                        "KRX_FWDG_ORD_ORGNO": "06010",
                        "ORD_TMD": "103021",
                    },
                }
            ),
            ("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl"): json_response(
                {
                    "rt_cd": "0",
                    "output": {"ODNO": "0000013439"},
                }
            ),
        }
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 10, 30, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    submitted = trader.submit_order(
        OrderRequest(
            request_id="submit-1",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=1,
            limit_price=Decimal("92900"),
            requested_at=datetime(
                2026, 4, 11, 10, 30, 21, tzinfo=ZoneInfo("Asia/Seoul")
            ),
        )
    )
    amended = trader.amend_order(
        OrderAmendRequest(
            request_id="amend-1",
            order_id=submitted.order_id,
            limit_price=Decimal("94600"),
            requested_at=datetime(
                2026, 4, 11, 10, 30, 24, tzinfo=ZoneInfo("Asia/Seoul")
            ),
        )
    )

    assert amended.order_id == "0000013439"
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/hashkey",
        "/uapi/domestic-stock/v1/trading/order-cash",
        "/uapi/hashkey",
        "/uapi/domestic-stock/v1/trading/order-rvsecncl",
    ]
    amend_request = transport.requests[-1]
    assert json.loads(amend_request.body.decode("utf-8")) == {
        "CANO": "12345678",
        "ACNT_PRDT_CD": "01",
        "KRX_FWDG_ORD_ORGNO": "06010",
        "ORGN_ODNO": "13438",
        "ORD_DVSN": "00",
        "RVSE_CNCL_DVSN_CD": "01",
        "ORD_QTY": "0",
        "ORD_UNPR": "94600",
        "QTY_ALL_ORD_YN": "Y",
    }


def test_korea_investment_broker_trader_returns_empty_fills_from_cached_order_when_history_is_empty() -> (
    None
):
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            ("POST", "/uapi/hashkey"): [
                json_response({"HASH": "hash-submit"}),
                json_response({"HASH": "hash-amend"}),
                json_response({"HASH": "hash-cancel"}),
            ],
            ("POST", "/uapi/domestic-stock/v1/trading/order-cash"): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "KRX_FWDG_ORD_ORGNO": "00950",
                        "ODNO": "0000013929",
                        "ORD_TMD": "104605",
                    },
                }
            ),
            ("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl"): [
                json_response(
                    {
                        "rt_cd": "0",
                        "output": {
                            "KRX_FWDG_ORD_ORGNO": "00950",
                            "ODNO": "0000013934",
                            "ORD_TMD": "104609",
                        },
                    }
                ),
                json_response(
                    {
                        "rt_cd": "0",
                        "output": {
                            "KRX_FWDG_ORD_ORGNO": "00950",
                            "ODNO": "0000013939",
                            "ORD_TMD": "104613",
                        },
                    }
                ),
            ],
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            ): json_response(
                {
                    "rt_cd": "0",
                    "output1": [],
                    "output2": {
                        "tot_ord_qty": "1",
                        "tot_ccld_qty": "1",
                        "tot_ccld_amt": "94350",
                    },
                }
            ),
        }
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 10, 46, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    submitted = trader.submit_order(
        OrderRequest(
            request_id="submit-1",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=1,
            limit_price=Decimal("90000"),
            requested_at=datetime(
                2026, 4, 11, 10, 45, 42, tzinfo=ZoneInfo("Asia/Seoul")
            ),
        )
    )
    amended = trader.amend_order(
        OrderAmendRequest(
            request_id="amend-1",
            order_id=submitted.order_id,
            limit_price=Decimal("93900"),
            requested_at=datetime(
                2026, 4, 11, 10, 45, 47, tzinfo=ZoneInfo("Asia/Seoul")
            ),
        )
    )
    canceled = trader.cancel_order(
        OrderCancelRequest(
            request_id="cancel-1",
            order_id=amended.order_id,
            requested_at=datetime(
                2026, 4, 11, 10, 45, 52, tzinfo=ZoneInfo("Asia/Seoul")
            ),
        )
    )

    fills = trader.get_fills(canceled.order_id)

    assert fills == ()
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/hashkey",
        "/uapi/domestic-stock/v1/trading/order-cash",
        "/uapi/hashkey",
        "/uapi/domestic-stock/v1/trading/order-rvsecncl",
        "/uapi/hashkey",
        "/uapi/domestic-stock/v1/trading/order-rvsecncl",
        "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
    ]


def test_korea_investment_broker_trader_treats_no_cancelable_quantity_as_filled() -> (
    None
):
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            ("POST", "/uapi/hashkey"): [
                json_response({"HASH": "hash-submit"}),
                json_response({"HASH": "hash-cancel"}),
            ],
            ("POST", "/uapi/domestic-stock/v1/trading/order-cash"): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "KRX_FWDG_ORD_ORGNO": "00950",
                        "ODNO": "0000014086",
                        "ORD_TMD": "104906",
                    },
                }
            ),
            ("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl"): json_response(
                {
                    "rt_cd": "1",
                    "msg_cd": "40330000",
                    "msg1": "모의투자 정정/취소할 수량이 없습니다.",
                }
            ),
        }
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 10, 49, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    submitted = trader.submit_order(
        OrderRequest(
            request_id="submit-1",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=1,
            limit_price=Decimal("95000"),
            requested_at=datetime(
                2026, 4, 11, 10, 49, 6, tzinfo=ZoneInfo("Asia/Seoul")
            ),
        )
    )
    canceled = trader.cancel_order(
        OrderCancelRequest(
            request_id="cancel-1",
            order_id=submitted.order_id,
            requested_at=datetime(
                2026, 4, 11, 10, 49, 39, tzinfo=ZoneInfo("Asia/Seoul")
            ),
        )
    )

    assert canceled == ExecutionOrder(
        order_id="0000014086",
        symbol="069500",
        side=OrderSide.BUY,
        quantity=1,
        limit_price=Decimal("95000"),
        status=OrderStatus.FILLED,
        created_at=datetime(2026, 4, 11, 10, 49, 6, tzinfo=ZoneInfo("Asia/Seoul")),
        updated_at=datetime(2026, 4, 11, 10, 49, 39, tzinfo=ZoneInfo("Asia/Seoul")),
        filled_quantity=1,
    )


def test_korea_investment_broker_trader_uses_amendable_lookup_when_history_missing() -> (
    None
):
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response({"access_token": "token-123"}),
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            ): json_response({"rt_cd": "0", "output1": []}),
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
            ): json_response(
                {
                    "rt_cd": "0",
                    "output": [_order_history_row(order_id="12799")],
                }
            ),
            ("POST", "/uapi/hashkey"): json_response({"HASH": "hash-amend"}),
            ("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl"): json_response(
                {
                    "rt_cd": "0",
                    "output": {"ODNO": "12800"},
                }
            ),
        }
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 9, 1, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    amended = trader.amend_order(
        OrderAmendRequest(
            request_id="amend-1",
            order_id="0000012799",
            limit_price=Decimal("10100"),
            requested_at=datetime(2026, 4, 11, 9, 1, tzinfo=ZoneInfo("Asia/Seoul")),
        )
    )

    assert amended.order_id == "12800"
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
        "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
        "/uapi/hashkey",
        "/uapi/domestic-stock/v1/trading/order-rvsecncl",
    ]
    amend_request = transport.requests[-1]
    assert json.loads(amend_request.body.decode("utf-8")) == {
        "CANO": "12345678",
        "ACNT_PRDT_CD": "01",
        "KRX_FWDG_ORD_ORGNO": "06010",
        "ORGN_ODNO": "12799",
        "ORD_DVSN": "00",
        "RVSE_CNCL_DVSN_CD": "01",
        "ORD_QTY": "0",
        "ORD_UNPR": "10100",
        "QTY_ALL_ORD_YN": "Y",
    }


def test_korea_investment_bar_source_loads_daily_bars() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {"access_token": "token-123"},
            ),
            (
                "GET",
                "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            ): json_response(
                {
                    "rt_cd": "0",
                    "output2": [
                        {
                            "stck_bsop_date": "20260411",
                            "stck_oprc": "102",
                            "stck_hgpr": "105",
                            "stck_lwpr": "101",
                            "stck_clpr": "104",
                            "acml_vol": "15",
                        },
                        {
                            "stck_bsop_date": "20260410",
                            "stck_oprc": "100",
                            "stck_hgpr": "103",
                            "stck_lwpr": "99",
                            "stck_clpr": "102",
                            "acml_vol": "10",
                        },
                    ],
                },
            ),
        },
    )
    source = KoreaInvestmentBarSource(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 21, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    bars = source.load_bars(
        "069500",
        Timeframe.DAY,
        start=datetime(2026, 4, 10, 0, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        end=datetime(2026, 4, 11, 21, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert bars == (
        Bar(
            symbol="069500",
            timeframe=Timeframe.DAY,
            timestamp=datetime(2026, 4, 10, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
            open=Decimal("100"),
            high=Decimal("103"),
            low=Decimal("99"),
            close=Decimal("102"),
            volume=10,
        ),
        Bar(
            symbol="069500",
            timeframe=Timeframe.DAY,
            timestamp=datetime(2026, 4, 11, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
            open=Decimal("102"),
            high=Decimal("105"),
            low=Decimal("101"),
            close=Decimal("104"),
            volume=15,
        ),
    )
    assert parse_qs(urlsplit(transport.requests[1].url).query) == {
        "FID_COND_MRKT_DIV_CODE": ["J"],
        "FID_INPUT_ISCD": ["069500"],
        "FID_INPUT_DATE_1": ["20260410"],
        "FID_INPUT_DATE_2": ["20260411"],
        "FID_PERIOD_DIV_CODE": ["D"],
        "FID_ORG_ADJ_PRC": ["0"],
    }


def test_korea_investment_bar_source_normalizes_zero_volume_daily_bar() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {"access_token": "token-123"},
            ),
            (
                "GET",
                "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            ): json_response(
                {
                    "rt_cd": "0",
                    "output2": [
                        {
                            "stck_bsop_date": "20260411",
                            "stck_oprc": "0",
                            "stck_hgpr": "0",
                            "stck_lwpr": "0",
                            "stck_clpr": "2360",
                            "acml_vol": "0",
                        },
                    ],
                },
            ),
        },
    )
    source = KoreaInvestmentBarSource(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 21, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    bars = source.load_bars(
        "199290",
        Timeframe.DAY,
        start=datetime(2026, 4, 11, 0, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        end=datetime(2026, 4, 11, 21, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert bars == (
        Bar(
            symbol="199290",
            timeframe=Timeframe.DAY,
            timestamp=datetime(2026, 4, 11, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
            open=Decimal("2360"),
            high=Decimal("2360"),
            low=Decimal("2360"),
            close=Decimal("2360"),
            volume=0,
        ),
    )


def test_korea_investment_bar_source_reports_invalid_daily_ohlc_context() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {"access_token": "token-123"},
            ),
            (
                "GET",
                "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            ): json_response(
                {
                    "rt_cd": "0",
                    "output2": [
                        {
                            "stck_bsop_date": "20260411",
                            "stck_oprc": "100",
                            "stck_hgpr": "105",
                            "stck_lwpr": "99",
                            "stck_clpr": "120",
                            "acml_vol": "10",
                        },
                    ],
                },
            ),
        },
    )
    source = KoreaInvestmentBarSource(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 21, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    with pytest.raises(
        KoreaInvestmentBrokerError,
        match=(
            "invalid KIS daily bar symbol=069500 date=20260411 "
            "open=100 high=105 low=99 close=120 volume=10"
        ),
    ):
        source.load_bars(
            "069500",
            Timeframe.DAY,
            start=datetime(2026, 4, 11, 0, 0, tzinfo=ZoneInfo("Asia/Seoul")),
            end=datetime(2026, 4, 11, 21, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        )


def test_korea_investment_bar_source_retries_rate_limit_http_error() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {"access_token": "token-123"},
            ),
            (
                "GET",
                "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            ): [
                json_response(
                    {
                        "rt_cd": "1",
                        "msg_cd": "EGW00201",
                        "msg1": "초당 거래건수를 초과하였습니다.",
                    },
                    status=500,
                ),
                json_response(
                    {
                        "rt_cd": "0",
                        "output2": [
                            {
                                "stck_bsop_date": "20260411",
                                "stck_oprc": "102",
                                "stck_hgpr": "105",
                                "stck_lwpr": "101",
                                "stck_clpr": "104",
                                "acml_vol": "15",
                            },
                        ],
                    },
                ),
            ],
        },
    )
    sleeps: list[float] = []
    source = KoreaInvestmentBarSource(
        _make_settings(),
        transport=transport,
        sleep=sleeps.append,
        clock=lambda: datetime(2026, 4, 11, 21, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    bars = source.load_bars(
        "069500",
        Timeframe.DAY,
        start=datetime(2026, 4, 11, 0, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        end=datetime(2026, 4, 11, 21, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert len(bars) == 1
    assert sleeps == [1.5]
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
    ]


def test_korea_investment_reader_retries_rate_limit_payload_error() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {"access_token": "token-123"},
            ),
            ("GET", "/uapi/domestic-stock/v1/quotations/inquire-price"): [
                json_response(
                    {
                        "rt_cd": "1",
                        "msg_cd": "EGW00201",
                        "msg1": "초당 거래건수를 초과하였습니다.",
                    },
                ),
                json_response(
                    {
                        "rt_cd": "0",
                        "output": {
                            "stck_bsop_date": "20260411",
                            "stck_cntg_hour": "090000",
                            "stck_prpr": "12345",
                        },
                    },
                ),
            ],
        },
    )
    sleeps: list[float] = []
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=transport,
        sleep=sleeps.append,
    )

    quote = reader.get_quote("069500")

    assert quote.price == Decimal("12345")
    assert sleeps == [1.5]
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
    ]


def test_korea_investment_bar_source_aggregates_intraday_bars_into_30m_bars() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {"access_token": "token-123"},
            ),
            (
                "GET",
                "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
            ): [
                json_response(
                    {
                        "rt_cd": "0",
                        "output2": _intraday_rows(
                            "20260411",
                            start_hour=10,
                            start_minute=0,
                            periods=120,
                        ),
                    },
                ),
                json_response(
                    {
                        "rt_cd": "0",
                        "output2": _intraday_rows(
                            "20260411",
                            start_hour=9,
                            start_minute=0,
                            periods=61,
                        ),
                    },
                ),
            ],
        },
    )
    source = KoreaInvestmentBarSource(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 12, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    bars = source.load_bars(
        "069500",
        Timeframe.MINUTE_30,
        start=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        end=datetime(2026, 4, 11, 12, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert [bar.timestamp for bar in bars] == [
        datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        datetime(2026, 4, 11, 9, 30, tzinfo=ZoneInfo("Asia/Seoul")),
        datetime(2026, 4, 11, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        datetime(2026, 4, 11, 10, 30, tzinfo=ZoneInfo("Asia/Seoul")),
        datetime(2026, 4, 11, 11, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        datetime(2026, 4, 11, 11, 30, tzinfo=ZoneInfo("Asia/Seoul")),
    ]
    assert bars[0] == Bar(
        symbol="069500",
        timeframe=Timeframe.MINUTE_30,
        timestamp=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        open=Decimal("99"),
        high=Decimal("130"),
        low=Decimal("98"),
        close=Decimal("129"),
        volume=30,
    )
    assert bars[-1] == Bar(
        symbol="069500",
        timeframe=Timeframe.MINUTE_30,
        timestamp=datetime(2026, 4, 11, 11, 30, tzinfo=ZoneInfo("Asia/Seoul")),
        open=Decimal("249"),
        high=Decimal("280"),
        low=Decimal("248"),
        close=Decimal("279"),
        volume=30,
    )
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
        "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
    ]
    assert parse_qs(urlsplit(transport.requests[1].url).query)["FID_INPUT_HOUR_1"] == [
        "120000"
    ]
    assert parse_qs(urlsplit(transport.requests[2].url).query)["FID_INPUT_HOUR_1"] == [
        "100000"
    ]


def test_korea_investment_bar_source_keeps_market_close_bar_for_30m() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {"access_token": "token-123"},
            ),
            (
                "GET",
                "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
            ): json_response(
                {
                    "rt_cd": "0",
                    "output2": _intraday_rows(
                        "20260410",
                        start_hour=15,
                        start_minute=0,
                        periods=31,
                    ),
                },
            ),
        },
    )
    source = KoreaInvestmentBarSource(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 10, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    bars = source.load_bars(
        "069500",
        Timeframe.MINUTE_30,
        start=datetime(2026, 4, 10, 15, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        end=datetime(2026, 4, 10, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert [bar.timestamp for bar in bars] == [
        datetime(2026, 4, 10, 15, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        datetime(2026, 4, 10, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
    ]
    assert bars[-1] == Bar(
        symbol="069500",
        timeframe=Timeframe.MINUTE_30,
        timestamp=datetime(2026, 4, 10, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
        open=Decimal("489"),
        high=Decimal("491"),
        low=Decimal("488"),
        close=Decimal("490"),
        volume=1,
    )


def test_korea_investment_clients_share_request_throttle_across_instances(
    tmp_path: Path,
) -> None:
    cache_path = tmp_path / "token-cache.json"
    cache_path.write_text(
        json.dumps(
            {
                "access_token": "cached-token",
                "expires_at": "2026-04-12T09:00:00+09:00",
            }
        ),
        encoding="utf-8",
    )
    transport = RecordingTransport(
        {
            ("GET", "/uapi/domestic-stock/v1/quotations/inquire-price"): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "stck_bsop_date": "20260411",
                        "stck_cntg_hour": "090000",
                        "stck_prpr": "12345",
                    },
                },
            ),
            ("POST", "/uapi/hashkey"): json_response({"HASH": "hash-123"}),
            ("POST", "/uapi/domestic-stock/v1/trading/order-cash"): json_response(
                {
                    "rt_cd": "0",
                    "output": {"ODNO": "order-1"},
                }
            ),
        }
    )
    current_time = 0.0
    sleeps: list[float] = []

    def monotonic() -> float:
        return current_time

    def sleep(seconds: float) -> None:
        nonlocal current_time
        sleeps.append(seconds)
        current_time += seconds

    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=transport,
        token_cache_path=cache_path,
        min_request_interval_seconds=1.1,
        monotonic=monotonic,
        sleep=sleep,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )
    trader = KoreaInvestmentBrokerTrader(
        _make_settings(),
        transport=transport,
        token_cache_path=cache_path,
        min_request_interval_seconds=1.1,
        monotonic=monotonic,
        sleep=sleep,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    reader.get_quote("069500")
    trader.submit_order(
        OrderRequest(
            request_id="submit-1",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=1,
            limit_price=Decimal("10000"),
            requested_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        )
    )

    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "/uapi/hashkey",
        "/uapi/domestic-stock/v1/trading/order-cash",
    ]
    assert sleeps == [1.1, 1.1]


@pytest.mark.parametrize(
    ("account", "environment", "expected"),
    [
        ("12345678", "paper", ("12345678", "01")),
        ("12345678-01", "paper", ("12345678", "01")),
        ("1234567801", "paper", ("12345678", "01")),
        ("12345678", "live", ("12345678", "01")),
        ("12345678-01", "live", ("12345678", "01")),
        ("1234567801", "live", ("12345678", "01")),
    ],
)
def test_split_account_accepts_expected_formats(
    account: str,
    environment: BrokerEnvironment,
    expected: tuple[str, str],
) -> None:
    assert _split_account(account, environment=environment) == expected


def test_split_account_rejects_invalid_format() -> None:
    with pytest.raises(KoreaInvestmentBrokerError):
        _split_account("12345", environment="paper")


@pytest.mark.parametrize(
    ("account", "environment", "message"),
    [
        ("1234567-01", "paper", "paper account"),
        ("12345678-1", "paper", "paper account"),
        ("12345678-AB", "paper", "paper account"),
        ("12345678901", "paper", "paper account"),
        ("12345678--01", "paper", "paper account"),
        ("1234567", "live", "live account"),
        ("12345678-1", "live", "live account"),
        ("12345678-AB", "live", "live account"),
        ("12345678901", "live", "live account"),
        ("12345678--01", "live", "live account"),
    ],
)
def test_split_account_rejects_invalid_digit_structure(
    account: str,
    environment: BrokerEnvironment,
    message: str,
) -> None:
    with pytest.raises(KoreaInvestmentBrokerError, match=message):
        _split_account(account, environment=environment)


@pytest.mark.parametrize(
    ("environment", "expected"),
    [
        ("paper", KIS_DEFAULT_MIN_REQUEST_INTERVAL_SECONDS),
        ("live", KIS_LIVE_MIN_REQUEST_INTERVAL_SECONDS),
    ],
)
def test_resolve_min_request_interval_seconds_uses_environment_default(
    environment: BrokerEnvironment,
    expected: float,
) -> None:
    assert (
        _resolve_min_request_interval_seconds(
            settings=_make_settings(environment),
            transport=None,
            explicit_seconds=None,
        )
        == expected
    )


def test_kis_request_interval_defaults_match_environment_policy() -> None:
    assert KIS_DEFAULT_MIN_REQUEST_INTERVAL_SECONDS == 1.1
    assert KIS_LIVE_MIN_REQUEST_INTERVAL_SECONDS == pytest.approx(1.0 / 15.0)


def _make_settings(
    environment: BrokerEnvironment = "paper",
    *,
    hts_id: str | None = None,
) -> BrokerSettings:
    return BrokerSettings(
        provider="koreainvestment",
        api_key="demo-key",
        api_secret="demo-secret",
        account="12345678-01",
        environment=environment,
        hts_id=hts_id,
    )


def json_response(payload: dict[str, object], status: int = 200) -> HttpResponse:
    return HttpResponse(
        status=status,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode("utf-8"),
    )


class RecordingTransport:
    def __init__(
        self,
        responses: dict[
            tuple[str, str],
            HttpResponse | list[HttpResponse],
        ],
    ) -> None:
        self._responses = {
            key: list(value) if isinstance(value, list) else value
            for key, value in responses.items()
        }
        self.requests: list[HttpRequest] = []

    def __call__(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        key = (request.method, urlsplit(request.url).path)
        if key not in self._responses:
            raise AssertionError(f"unexpected request: {key}")
        response = self._responses[key]
        if isinstance(response, list):
            if not response:
                raise AssertionError(f"missing scripted response for: {key}")
            return response.pop(0)
        return response


class ScriptedWebSocketConnection:
    def __init__(self, messages: list[str]) -> None:
        self._messages = list(messages)
        self.sent_texts: list[str] = []
        self.sent_pongs: list[bytes] = []
        self.closed = False

    def send_text(self, payload: str) -> None:
        self.sent_texts.append(payload)

    def send_pong(self, payload: bytes = b"") -> None:
        self.sent_pongs.append(payload)

    def receive_text(self, *, timeout_seconds: float | None = None) -> str:
        del timeout_seconds
        if self.closed:
            raise ConnectionError("websocket closed")
        if self._messages:
            return self._messages.pop(0)
        raise TimeoutError()

    def close(self) -> None:
        self.closed = True


def _wait_for_realtime_fills(
    trader: KoreaInvestmentBrokerTrader,
    order_id: str,
    *,
    expected_count: int = 1,
) -> tuple[ExecutionFill, ...]:
    deadline = time.monotonic() + 1.0
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            fills = trader.get_fills(order_id)
        except KoreaInvestmentBrokerError as error:
            last_error = error
            time.sleep(0.01)
            continue
        if len(fills) >= expected_count:
            return fills
        time.sleep(0.01)
    if last_error is not None:
        raise last_error
    raise AssertionError("realtime fills did not arrive before timeout")


def _fill_notice_ack_message(*, tr_id: str) -> str:
    return json.dumps(
        {
            "header": {
                "tr_id": tr_id,
                "tr_key": "my-hts-id",
                "encrypt": "Y",
            },
            "body": {
                "rt_cd": "0",
                "msg1": "OK",
                "output": {
                    "key": "0123456789abcdef0123456789abcdef",
                    "iv": "abcdef9876543210",
                },
            },
        }
    )


def _fill_notice_data_message(*, tr_id: str, payload: str) -> str:
    return f"0|{tr_id}|1|{_encrypt_fill_notice_payload(payload)}"


def _encrypt_fill_notice_payload(payload: str) -> str:
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad

    key = b"0123456789abcdef0123456789abcdef"
    iv = b"abcdef9876543210"
    cipher = AES.new(key, AES.MODE_CBC, iv)
    encrypted = cipher.encrypt(pad(payload.encode("utf-8"), AES.block_size))
    return base64.b64encode(encrypted).decode("ascii")


def _fill_notice_hash(
    order_id: str,
    quantity: str,
    price: str,
    filled_at: str,
) -> str:
    payload = _fill_notice_payload(
        order_id=order_id,
        quantity=quantity,
        price=price,
        filled_at=filled_at,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _fill_notice_payload(
    *,
    order_id: str,
    quantity: str,
    price: str,
    filled_at: str,
) -> str:
    values = [
        "",
        "12345678-01",
        order_id,
        "",
        "02",
        "",
        "",
        "",
        "069500",
        quantity,
        price,
        filled_at,
        "N",
        "2",
        "Y",
        "06010",
        quantity,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        price,
    ]
    return "^".join(values)


def _order_history_row(
    *,
    order_id: str,
    origin_order_id: str = "",
    limit_price: str = "10000",
    filled_quantity: str = "0",
    average_price: str = "0",
    canceled: str = "N",
    rejected_quantity: str = "0",
) -> dict[str, str]:
    return {
        "ord_dt": "20260411",
        "ord_gno_brno": "06010",
        "odno": order_id,
        "orgn_odno": origin_order_id,
        "sll_buy_dvsn_cd": "02",
        "sll_buy_dvsn_cd_name": "매수",
        "pdno": "069500",
        "ord_qty": "3",
        "ord_unpr": limit_price,
        "ord_tmd": "090000" if order_id == "order-1" else "090100",
        "tot_ccld_qty": filled_quantity,
        "avg_prvs": average_price,
        "cncl_yn": canceled,
        "ord_dvsn_cd": "00",
        "rjct_qty": rejected_quantity,
    }


def _intraday_rows(
    date_text: str,
    *,
    start_hour: int,
    start_minute: int,
    periods: int,
) -> list[dict[str, str]]:
    start = datetime(
        2026,
        4,
        11,
        start_hour,
        start_minute,
        tzinfo=ZoneInfo("Asia/Seoul"),
    )
    rows: list[dict[str, str]] = []
    for offset in reversed(range(periods)):
        current = start + timedelta(minutes=offset)
        close_value = Decimal(100 + (current.hour - 9) * 60 + current.minute)
        rows.append(
            {
                "stck_bsop_date": date_text,
                "stck_cntg_hour": current.strftime("%H%M%S"),
                "stck_oprc": str(close_value - Decimal("1")),
                "stck_hgpr": str(close_value + Decimal("1")),
                "stck_lwpr": str(close_value - Decimal("2")),
                "stck_prpr": str(close_value),
                "cntg_vol": "1",
            }
        )
    return rows
