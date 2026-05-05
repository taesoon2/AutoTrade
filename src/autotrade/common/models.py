from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_non_blank(field_name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be blank")


def _require_non_empty_symbol(symbol: str) -> None:
    _require_non_blank("symbol", symbol)


def _require_non_negative_decimal(field_name: str, value: Decimal) -> None:
    if value < Decimal("0"):
        raise ValueError(f"{field_name} must be non-negative")


def _require_non_negative_int(field_name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


def _require_positive_decimal(field_name: str, value: Decimal) -> None:
    if value <= Decimal("0"):
        raise ValueError(f"{field_name} must be positive")


def _require_positive_int(field_name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")


class SignalAction(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    LIMIT = "LIMIT"


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


@dataclass(frozen=True, slots=True)
class Quote:
    symbol: str
    price: Decimal
    as_of: datetime
    currency: str = "KRW"

    def __post_init__(self) -> None:
        _require_non_empty_symbol(self.symbol)
        _require_non_negative_decimal("price", self.price)
        _require_aware_datetime("as_of", self.as_of)
        if not self.currency.strip():
            raise ValueError("currency must not be blank")


@dataclass(frozen=True, slots=True)
class Holding:
    symbol: str
    quantity: int
    average_price: Decimal
    current_price: Decimal | None = None

    def __post_init__(self) -> None:
        _require_non_empty_symbol(self.symbol)
        _require_non_negative_int("quantity", self.quantity)
        _require_non_negative_decimal("average_price", self.average_price)
        if self.current_price is not None:
            _require_non_negative_decimal("current_price", self.current_price)


@dataclass(frozen=True, slots=True)
class HoldingPerformance:
    symbol: str
    quantity: int
    average_price: Decimal
    current_price: Decimal
    purchase_amount: Decimal
    evaluation_amount: Decimal
    profit_loss: Decimal
    profit_loss_rate: Decimal

    def __post_init__(self) -> None:
        _require_non_empty_symbol(self.symbol)
        _require_non_negative_int("quantity", self.quantity)
        _require_non_negative_decimal("average_price", self.average_price)
        _require_non_negative_decimal("current_price", self.current_price)
        _require_non_negative_decimal("purchase_amount", self.purchase_amount)
        _require_non_negative_decimal("evaluation_amount", self.evaluation_amount)


@dataclass(frozen=True, slots=True)
class AccountPerformance:
    total_purchase_amount: Decimal
    total_evaluation_amount: Decimal
    total_profit_loss: Decimal
    total_profit_loss_rate: Decimal
    cash_available: Decimal
    holdings: tuple[HoldingPerformance, ...]

    def __post_init__(self) -> None:
        _require_non_negative_decimal(
            "total_purchase_amount",
            self.total_purchase_amount,
        )
        _require_non_negative_decimal(
            "total_evaluation_amount",
            self.total_evaluation_amount,
        )
        _require_non_negative_decimal("cash_available", self.cash_available)


@dataclass(frozen=True, slots=True)
class OrderCapacity:
    symbol: str
    order_price: Decimal
    max_orderable_quantity: int
    cash_available: Decimal

    def __post_init__(self) -> None:
        _require_non_empty_symbol(self.symbol)
        _require_non_negative_decimal("order_price", self.order_price)
        _require_non_negative_int(
            "max_orderable_quantity",
            self.max_orderable_quantity,
        )
        _require_non_negative_decimal("cash_available", self.cash_available)


@dataclass(frozen=True, slots=True)
class Signal:
    symbol: str
    action: SignalAction
    generated_at: datetime
    reason: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty_symbol(self.symbol)
        _require_aware_datetime("generated_at", self.generated_at)
        if self.reason is not None and not self.reason.strip():
            raise ValueError("reason must not be blank when provided")


@dataclass(frozen=True, slots=True)
class OrderRequest:
    request_id: str
    symbol: str
    side: OrderSide
    quantity: int
    limit_price: Decimal
    requested_at: datetime
    order_type: OrderType = OrderType.LIMIT

    def __post_init__(self) -> None:
        _require_non_blank("request_id", self.request_id)
        _require_non_empty_symbol(self.symbol)
        _require_positive_int("quantity", self.quantity)
        _require_positive_decimal("limit_price", self.limit_price)
        _require_aware_datetime("requested_at", self.requested_at)


@dataclass(frozen=True, slots=True)
class OrderAmendRequest:
    request_id: str
    order_id: str
    requested_at: datetime
    quantity: int | None = None
    limit_price: Decimal | None = None

    def __post_init__(self) -> None:
        _require_non_blank("request_id", self.request_id)
        _require_non_blank("order_id", self.order_id)
        _require_aware_datetime("requested_at", self.requested_at)
        if self.quantity is None and self.limit_price is None:
            raise ValueError("quantity or limit_price must be provided")
        if self.quantity is not None:
            _require_positive_int("quantity", self.quantity)
        if self.limit_price is not None:
            _require_positive_decimal("limit_price", self.limit_price)


@dataclass(frozen=True, slots=True)
class OrderCancelRequest:
    request_id: str
    order_id: str
    requested_at: datetime

    def __post_init__(self) -> None:
        _require_non_blank("request_id", self.request_id)
        _require_non_blank("order_id", self.order_id)
        _require_aware_datetime("requested_at", self.requested_at)


@dataclass(frozen=True, slots=True)
class ExecutionOrder:
    order_id: str
    symbol: str
    side: OrderSide
    quantity: int
    limit_price: Decimal
    status: OrderStatus
    created_at: datetime
    updated_at: datetime
    filled_quantity: int = 0

    def __post_init__(self) -> None:
        _require_non_blank("order_id", self.order_id)
        _require_non_empty_symbol(self.symbol)
        _require_positive_int("quantity", self.quantity)
        _require_positive_decimal("limit_price", self.limit_price)
        _require_aware_datetime("created_at", self.created_at)
        _require_aware_datetime("updated_at", self.updated_at)
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must not be earlier than created_at")
        _require_non_negative_int("filled_quantity", self.filled_quantity)
        if self.filled_quantity > self.quantity:
            raise ValueError("filled_quantity must not exceed quantity")


@dataclass(frozen=True, slots=True)
class ExecutionFill:
    fill_id: str
    order_id: str
    symbol: str
    quantity: int
    price: Decimal
    filled_at: datetime

    def __post_init__(self) -> None:
        _require_non_blank("fill_id", self.fill_id)
        _require_non_blank("order_id", self.order_id)
        _require_non_empty_symbol(self.symbol)
        _require_positive_int("quantity", self.quantity)
        _require_positive_decimal("price", self.price)
        _require_aware_datetime("filled_at", self.filled_at)
