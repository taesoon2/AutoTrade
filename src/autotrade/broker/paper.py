from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from autotrade.broker.readers import BrokerReader
from autotrade.broker.trading import BrokerTrader
from autotrade.common import AccountPerformance
from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import Holding
from autotrade.common import HoldingPerformance
from autotrade.common import OrderAmendRequest
from autotrade.common import OrderCancelRequest
from autotrade.common import OrderCapacity
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.common import Quote
from autotrade.data import Bar

ZERO = Decimal("0")
_OPEN_ORDER_STATUSES = {
    OrderStatus.PENDING,
    OrderStatus.ACKNOWLEDGED,
    OrderStatus.PARTIALLY_FILLED,
    OrderStatus.CANCEL_PENDING,
}


def _require_non_negative_decimal(field_name: str, value: Decimal) -> None:
    if value < ZERO:
        raise ValueError(f"{field_name} must be non-negative")


def _require_positive_int(field_name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")


@dataclass(frozen=True, slots=True)
class PaperBrokerSnapshot:
    cash: Decimal
    holdings: tuple[Holding, ...]
    orders: tuple[ExecutionOrder, ...]
    fills: tuple[ExecutionFill, ...]
    market_bars: tuple[Bar, ...]
    next_order_sequence: int = 1

    def __post_init__(self) -> None:
        _require_non_negative_decimal("cash", self.cash)
        _require_positive_int("next_order_sequence", self.next_order_sequence)


@dataclass(slots=True)
class _PaperPosition:
    quantity: int
    average_price: Decimal


class PaperBroker(BrokerReader, BrokerTrader):
    def __init__(self, initial_cash: Decimal) -> None:
        _require_non_negative_decimal("initial_cash", initial_cash)
        self._cash = initial_cash
        self._positions: dict[str, _PaperPosition] = {}
        self._orders: dict[str, ExecutionOrder] = {}
        self._fills: dict[str, tuple[ExecutionFill, ...]] = {}
        self._market_bars: dict[str, Bar] = {}
        self._next_order_sequence = 1

    @classmethod
    def from_snapshot(cls, snapshot: PaperBrokerSnapshot) -> PaperBroker:
        broker = cls(snapshot.cash)
        broker._positions = {
            holding.symbol: _PaperPosition(
                quantity=holding.quantity,
                average_price=holding.average_price,
            )
            for holding in snapshot.holdings
            if holding.quantity > 0
        }
        broker._orders = {order.order_id: order for order in snapshot.orders}
        fills_by_order: dict[str, list[ExecutionFill]] = {}
        for fill in snapshot.fills:
            fills_by_order.setdefault(fill.order_id, []).append(fill)
        broker._fills = {
            order_id: tuple(
                sorted(
                    fills,
                    key=lambda item: (item.filled_at, item.fill_id),
                )
            )
            for order_id, fills in fills_by_order.items()
        }
        broker._market_bars = {bar.symbol: bar for bar in snapshot.market_bars}
        broker._next_order_sequence = snapshot.next_order_sequence
        return broker

    def snapshot(self) -> PaperBrokerSnapshot:
        return PaperBrokerSnapshot(
            cash=self._cash,
            holdings=self.get_holdings(),
            orders=tuple(
                sorted(
                    self._orders.values(),
                    key=lambda order: (order.created_at, order.order_id),
                )
            ),
            fills=tuple(
                sorted(
                    (fill for fills in self._fills.values() for fill in fills),
                    key=lambda fill: (fill.filled_at, fill.fill_id),
                )
            ),
            market_bars=tuple(
                sorted(
                    self._market_bars.values(),
                    key=lambda bar: (bar.timestamp, bar.symbol),
                )
            ),
            next_order_sequence=self._next_order_sequence,
        )

    def advance_bar(self, bar: Bar) -> None:
        self._market_bars[bar.symbol] = bar
        for order_id in tuple(self._orders):
            order = self._orders[order_id]
            if order.symbol != bar.symbol or order.status not in _OPEN_ORDER_STATUSES:
                continue
            self._maybe_fill_order(order_id, bar)

    def get_quote(self, symbol: str) -> Quote:
        bar = self._require_market_bar(symbol)
        return Quote(
            symbol=symbol,
            price=bar.close,
            as_of=bar.timestamp,
        )

    def get_holdings(self) -> tuple[Holding, ...]:
        holdings = []
        for symbol, position in sorted(self._positions.items()):
            if position.quantity <= 0:
                continue
            current_price = None
            bar = self._market_bars.get(symbol)
            if bar is not None:
                current_price = bar.close
            holdings.append(
                Holding(
                    symbol=symbol,
                    quantity=position.quantity,
                    average_price=position.average_price,
                    current_price=current_price,
                )
            )
        return tuple(holdings)

    def get_account_performance(self) -> AccountPerformance:
        holdings = []
        total_purchase_amount = ZERO
        total_evaluation_amount = ZERO
        total_profit_loss = ZERO
        for symbol, position in sorted(self._positions.items()):
            if position.quantity <= 0:
                continue
            bar = self._market_bars.get(symbol)
            current_price = (
                position.average_price if bar is None else bar.close
            )
            quantity = Decimal(position.quantity)
            purchase_amount = position.average_price * quantity
            evaluation_amount = current_price * quantity
            profit_loss = evaluation_amount - purchase_amount
            profit_loss_rate = _calculate_profit_loss_rate(
                profit_loss,
                purchase_amount,
            )
            holdings.append(
                HoldingPerformance(
                    symbol=symbol,
                    quantity=position.quantity,
                    average_price=position.average_price,
                    current_price=current_price,
                    purchase_amount=purchase_amount,
                    evaluation_amount=evaluation_amount,
                    profit_loss=profit_loss,
                    profit_loss_rate=profit_loss_rate,
                )
            )
            total_purchase_amount += purchase_amount
            total_evaluation_amount += evaluation_amount
            total_profit_loss += profit_loss

        return AccountPerformance(
            total_purchase_amount=total_purchase_amount,
            total_evaluation_amount=total_evaluation_amount,
            total_profit_loss=total_profit_loss,
            total_profit_loss_rate=_calculate_profit_loss_rate(
                total_profit_loss,
                total_purchase_amount,
            ),
            cash_available=self._cash,
            holdings=tuple(holdings),
        )

    def get_order_capacity(
        self,
        symbol: str,
        order_price: Decimal,
    ) -> OrderCapacity:
        _require_non_negative_decimal("order_price", order_price)
        reserved_cash = sum(
            (
                order.limit_price * Decimal(order.quantity)
                for order in self._orders.values()
                if order.side is OrderSide.BUY and order.status in _OPEN_ORDER_STATUSES
            ),
            start=ZERO,
        )
        available_cash = max(ZERO, self._cash - reserved_cash)
        max_orderable_quantity = (
            0 if order_price == ZERO else int(available_cash / order_price)
        )
        return OrderCapacity(
            symbol=symbol,
            order_price=order_price,
            max_orderable_quantity=max_orderable_quantity,
            cash_available=available_cash,
        )

    def submit_order(self, request: OrderRequest) -> ExecutionOrder:
        self._require_market_bar(request.symbol)
        order_id = self._next_order_id()
        status = self._resolve_submission_status(
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            limit_price=request.limit_price,
        )
        order = ExecutionOrder(
            order_id=order_id,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            limit_price=request.limit_price,
            status=status,
            created_at=request.requested_at,
            updated_at=request.requested_at,
        )
        self._orders[order_id] = order
        if status in _OPEN_ORDER_STATUSES:
            self._maybe_fill_order(order_id, self._market_bars[request.symbol])
        return self._orders[order_id]

    def amend_order(self, request: OrderAmendRequest) -> ExecutionOrder:
        current = self._require_order(request.order_id)
        if current.status not in _OPEN_ORDER_STATUSES:
            return current

        quantity = request.quantity or current.quantity
        limit_price = request.limit_price or current.limit_price
        status = self._resolve_submission_status(
            symbol=current.symbol,
            side=current.side,
            quantity=quantity,
            limit_price=limit_price,
            exclude_order_id=current.order_id,
        )
        amended = ExecutionOrder(
            order_id=current.order_id,
            symbol=current.symbol,
            side=current.side,
            quantity=quantity,
            limit_price=limit_price,
            status=status,
            created_at=current.created_at,
            updated_at=request.requested_at,
            filled_quantity=0,
        )
        self._orders[current.order_id] = amended
        if status in _OPEN_ORDER_STATUSES:
            self._maybe_fill_order(current.order_id, self._market_bars[current.symbol])
        return self._orders[current.order_id]

    def cancel_order(self, request: OrderCancelRequest) -> ExecutionOrder:
        current = self._require_order(request.order_id)
        if current.status not in _OPEN_ORDER_STATUSES:
            return current

        canceled = ExecutionOrder(
            order_id=current.order_id,
            symbol=current.symbol,
            side=current.side,
            quantity=current.quantity,
            limit_price=current.limit_price,
            status=OrderStatus.CANCELED,
            created_at=current.created_at,
            updated_at=request.requested_at,
            filled_quantity=current.filled_quantity,
        )
        self._orders[current.order_id] = canceled
        return canceled

    def get_fills(self, order_id: str) -> tuple[ExecutionFill, ...]:
        self._require_order(order_id)
        return self._fills.get(order_id, ())

    def _resolve_submission_status(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: int,
        limit_price: Decimal,
        exclude_order_id: str | None = None,
    ) -> OrderStatus:
        if side is OrderSide.BUY:
            reserved_cash = sum(
                (
                    order.limit_price * Decimal(order.quantity)
                    for order in self._orders.values()
                    if order.order_id != exclude_order_id
                    and order.side is OrderSide.BUY
                    and order.status in _OPEN_ORDER_STATUSES
                ),
                start=ZERO,
            )
            if (limit_price * Decimal(quantity)) > (self._cash - reserved_cash):
                return OrderStatus.REJECTED
            return OrderStatus.ACKNOWLEDGED

        reserved_quantity = sum(
            (
                order.quantity
                for order in self._orders.values()
                if order.order_id != exclude_order_id
                and order.symbol == symbol
                and order.side is OrderSide.SELL
                and order.status in _OPEN_ORDER_STATUSES
            ),
            start=0,
        )
        available_quantity = self._positions.get(
            symbol, _PaperPosition(0, ZERO)
        ).quantity
        if quantity > max(0, available_quantity - reserved_quantity):
            return OrderStatus.REJECTED
        return OrderStatus.ACKNOWLEDGED

    def _require_order(self, order_id: str) -> ExecutionOrder:
        order = self._orders.get(order_id)
        if order is None:
            raise ValueError(f"unknown paper order_id={order_id}")
        return order

    def _require_market_bar(self, symbol: str) -> Bar:
        bar = self._market_bars.get(symbol)
        if bar is None:
            raise ValueError(f"missing market bar for symbol={symbol}")
        return bar

    def _next_order_id(self) -> str:
        order_id = f"paper-{self._next_order_sequence}"
        self._next_order_sequence += 1
        return order_id

    def _maybe_fill_order(self, order_id: str, bar: Bar) -> None:
        order = self._orders[order_id]
        if order.status not in _OPEN_ORDER_STATUSES:
            return
        if bar.timestamp < order.updated_at:
            return
        if not self._is_fillable(order, bar):
            return

        execution_price = self._resolve_execution_price(order, bar)
        if order.side is OrderSide.BUY:
            total_cost = execution_price * Decimal(order.quantity)
            if total_cost > self._cash:
                self._orders[order_id] = ExecutionOrder(
                    order_id=order.order_id,
                    symbol=order.symbol,
                    side=order.side,
                    quantity=order.quantity,
                    limit_price=order.limit_price,
                    status=OrderStatus.REJECTED,
                    created_at=order.created_at,
                    updated_at=bar.timestamp,
                )
                return
            self._apply_buy_fill(order.symbol, order.quantity, execution_price)
        else:
            position = self._positions.get(order.symbol)
            if position is None or order.quantity > position.quantity:
                self._orders[order_id] = ExecutionOrder(
                    order_id=order.order_id,
                    symbol=order.symbol,
                    side=order.side,
                    quantity=order.quantity,
                    limit_price=order.limit_price,
                    status=OrderStatus.REJECTED,
                    created_at=order.created_at,
                    updated_at=bar.timestamp,
                )
                return
            self._apply_sell_fill(order.symbol, order.quantity, execution_price)

        fill = ExecutionFill(
            fill_id=f"{order.order_id}:fill-1",
            order_id=order.order_id,
            symbol=order.symbol,
            quantity=order.quantity,
            price=execution_price,
            filled_at=bar.timestamp,
        )
        self._fills[order.order_id] = (fill,)
        self._orders[order_id] = ExecutionOrder(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            limit_price=order.limit_price,
            status=OrderStatus.FILLED,
            created_at=order.created_at,
            updated_at=bar.timestamp,
            filled_quantity=order.quantity,
        )

    def _apply_buy_fill(
        self,
        symbol: str,
        quantity: int,
        execution_price: Decimal,
    ) -> None:
        total_cost = execution_price * Decimal(quantity)
        position = self._positions.get(symbol)
        if position is None:
            self._positions[symbol] = _PaperPosition(
                quantity=quantity,
                average_price=execution_price,
            )
        else:
            total_quantity = position.quantity + quantity
            updated_notional = (position.average_price * Decimal(position.quantity)) + (
                execution_price * Decimal(quantity)
            )
            position.quantity = total_quantity
            position.average_price = updated_notional / Decimal(total_quantity)
        self._cash -= total_cost

    def _apply_sell_fill(
        self,
        symbol: str,
        quantity: int,
        execution_price: Decimal,
    ) -> None:
        position = self._positions[symbol]
        position.quantity -= quantity
        if position.quantity == 0:
            del self._positions[symbol]
        self._cash += execution_price * Decimal(quantity)

    def _is_fillable(self, order: ExecutionOrder, bar: Bar) -> bool:
        if order.side is OrderSide.BUY:
            return order.limit_price >= bar.low
        return order.limit_price <= bar.high

    def _resolve_execution_price(self, order: ExecutionOrder, bar: Bar) -> Decimal:
        if order.side is OrderSide.BUY:
            return min(order.limit_price, bar.close)
        return max(order.limit_price, bar.close)


def _calculate_profit_loss_rate(
    profit_loss: Decimal,
    purchase_amount: Decimal,
) -> Decimal:
    if purchase_amount == ZERO:
        return ZERO
    return (profit_loss / purchase_amount) * Decimal("100")
