from __future__ import annotations

from decimal import Decimal
from typing import Protocol
from typing import runtime_checkable

from autotrade.common import AccountPerformance
from autotrade.common import Holding
from autotrade.common import OrderCapacity
from autotrade.common import Quote


@runtime_checkable
class BrokerReader(Protocol):
    """Synchronous, read-only broker contract.

    Implementations should return holdings sorted by symbol so callers receive
    deterministic snapshots.
    """

    def get_quote(self, symbol: str) -> Quote: ...

    def get_holdings(self) -> tuple[Holding, ...]: ...

    def get_account_performance(self) -> AccountPerformance: ...

    def get_order_capacity(
        self,
        symbol: str,
        order_price: Decimal,
    ) -> OrderCapacity: ...
