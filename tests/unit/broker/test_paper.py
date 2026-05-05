from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from autotrade.broker import PaperBroker
from autotrade.common import OrderAmendRequest
from autotrade.common import OrderCancelRequest
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.data import KST
from autotrade.data import Bar
from autotrade.data import Timeframe


def test_paper_broker_supports_fill_amend_cancel_and_reject_scenarios() -> None:
    broker = PaperBroker(Decimal("100000"))
    broker.advance_bar(_bar("2026-04-13T09:00:00+09:00", close="100", high="100"))

    resting_buy = broker.submit_order(
        OrderRequest(
            request_id="buy-1",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=10,
            limit_price=Decimal("99"),
            requested_at=_dt("2026-04-13T09:00:00+09:00"),
        )
    )
    rejected_buy = broker.submit_order(
        OrderRequest(
            request_id="buy-2",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=2000,
            limit_price=Decimal("100"),
            requested_at=_dt("2026-04-13T09:00:01+09:00"),
        )
    )

    assert resting_buy.status is OrderStatus.ACKNOWLEDGED
    assert rejected_buy.status is OrderStatus.REJECTED

    broker.advance_bar(_bar("2026-04-13T09:30:00+09:00", close="98", low="97"))
    filled_buy = broker.get_fills(resting_buy.order_id)
    capacity = broker.get_order_capacity("069500", Decimal("100"))

    assert filled_buy[0].price == Decimal("98")
    assert broker.get_holdings()[0].quantity == 10
    assert broker.get_holdings()[0].average_price == Decimal("98")
    assert capacity.cash_available == Decimal("99020")
    assert capacity.max_orderable_quantity == 990

    resting_sell = broker.submit_order(
        OrderRequest(
            request_id="sell-1",
            symbol="069500",
            side=OrderSide.SELL,
            quantity=10,
            limit_price=Decimal("105"),
            requested_at=_dt("2026-04-13T09:31:00+09:00"),
        )
    )
    amended_sell = broker.amend_order(
        OrderAmendRequest(
            request_id="amend-1",
            order_id=resting_sell.order_id,
            limit_price=Decimal("101"),
            requested_at=_dt("2026-04-13T09:31:30+09:00"),
        )
    )
    resting_again = broker.submit_order(
        OrderRequest(
            request_id="buy-3",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=1,
            limit_price=Decimal("90"),
            requested_at=_dt("2026-04-13T09:32:00+09:00"),
        )
    )
    canceled = broker.cancel_order(
        OrderCancelRequest(
            request_id="cancel-1",
            order_id=resting_again.order_id,
            requested_at=_dt("2026-04-13T09:32:30+09:00"),
        )
    )

    assert amended_sell.status is OrderStatus.ACKNOWLEDGED
    assert canceled.status is OrderStatus.CANCELED

    broker.advance_bar(_bar("2026-04-13T10:00:00+09:00", close="101", high="101"))

    sell_fills = broker.get_fills(resting_sell.order_id)
    holdings = broker.get_holdings()

    assert sell_fills[0].price == Decimal("101")
    assert holdings == ()
    assert broker.get_quote("069500").price == Decimal("101")
    assert broker.snapshot().cash == Decimal("100030")
    assert broker.get_fills(resting_again.order_id) == ()


def test_paper_broker_snapshot_restores_open_orders_and_positions() -> None:
    broker = PaperBroker(Decimal("1000"))
    broker.advance_bar(_bar("2026-04-13T09:00:00+09:00", close="100"))
    buy = broker.submit_order(
        OrderRequest(
            request_id="buy-1",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=5,
            limit_price=Decimal("100"),
            requested_at=_dt("2026-04-13T09:00:00+09:00"),
        )
    )
    broker.advance_bar(_bar("2026-04-13T09:30:00+09:00", close="102", high="103"))
    sell = broker.submit_order(
        OrderRequest(
            request_id="sell-1",
            symbol="069500",
            side=OrderSide.SELL,
            quantity=5,
            limit_price=Decimal("110"),
            requested_at=_dt("2026-04-13T09:30:00+09:00"),
        )
    )

    restored = PaperBroker.from_snapshot(broker.snapshot())
    restored.advance_bar(_bar("2026-04-13T10:00:00+09:00", close="111", high="111"))

    assert buy.order_id == "paper-1"
    assert sell.order_id == "paper-2"
    assert restored.get_fills("paper-1")[0].price == Decimal("100")
    assert restored.get_fills("paper-2")[0].price == Decimal("111")
    assert restored.snapshot().cash == Decimal("1055")


def test_paper_broker_returns_account_performance() -> None:
    broker = PaperBroker(Decimal("1000"))
    broker.advance_bar(_bar("2026-04-13T09:00:00+09:00", close="100"))
    broker.submit_order(
        OrderRequest(
            request_id="buy-1",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=5,
            limit_price=Decimal("100"),
            requested_at=_dt("2026-04-13T09:00:00+09:00"),
        )
    )
    broker.advance_bar(_bar("2026-04-13T09:30:00+09:00", close="110"))

    performance = broker.get_account_performance()

    assert performance.total_purchase_amount == Decimal("500")
    assert performance.total_evaluation_amount == Decimal("550")
    assert performance.total_profit_loss == Decimal("50")
    assert performance.total_profit_loss_rate == Decimal("10.0")
    assert performance.cash_available == Decimal("500")
    assert performance.holdings[0].profit_loss_rate == Decimal("10.0")


def test_paper_broker_defers_fill_until_bar_at_or_after_order_time() -> None:
    broker = PaperBroker(Decimal("1000"))
    broker.advance_bar(_bar("2026-04-13T09:00:00+09:00", close="100", low="99"))

    resting_buy = broker.submit_order(
        OrderRequest(
            request_id="buy-late",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=5,
            limit_price=Decimal("100"),
            requested_at=_dt("2026-04-13T09:30:00+09:00"),
        )
    )

    broker.advance_bar(_bar("2026-04-13T09:00:00+09:00", close="99", low="99"))

    assert resting_buy.status is OrderStatus.ACKNOWLEDGED
    assert broker.get_fills(resting_buy.order_id) == ()

    broker.advance_bar(_bar("2026-04-13T10:00:00+09:00", close="99", low="99"))

    fills = broker.get_fills(resting_buy.order_id)
    stored_order = next(
        order
        for order in broker.snapshot().orders
        if order.order_id == resting_buy.order_id
    )

    assert fills[0].filled_at == _dt("2026-04-13T10:00:00+09:00")
    assert stored_order.updated_at >= stored_order.created_at


def _bar(
    timestamp: str,
    *,
    close: str,
    low: str | None = None,
    high: str | None = None,
) -> Bar:
    close_price = Decimal(close)
    return Bar(
        symbol="069500",
        timeframe=Timeframe.MINUTE_30,
        timestamp=_dt(timestamp),
        open=close_price,
        high=Decimal(high or close),
        low=Decimal(low or close),
        close=close_price,
        volume=1,
    )


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(KST)
