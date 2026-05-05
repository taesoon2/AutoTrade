from __future__ import annotations

from decimal import Decimal

from autotrade.common import AccountPerformance
from autotrade.common import HoldingPerformance
from autotrade.runtime.operations import render_account_performance


def test_render_account_performance_prefers_holding_name_before_symbol() -> None:
    rendered = render_account_performance(
        AccountPerformance(
            total_purchase_amount=Decimal("9000"),
            total_evaluation_amount=Decimal("9500"),
            total_profit_loss=Decimal("500"),
            total_profit_loss_rate=Decimal("5.56"),
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
                    name="KODEX 200",
                ),
            ),
        )
    )

    assert "- KODEX 200(069500) quantity=1" in rendered


def test_render_account_performance_falls_back_to_symbol_without_name() -> None:
    rendered = render_account_performance(
        AccountPerformance(
            total_purchase_amount=Decimal("9000"),
            total_evaluation_amount=Decimal("9500"),
            total_profit_loss=Decimal("500"),
            total_profit_loss_rate=Decimal("5.56"),
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
            ),
        )
    )

    assert "- 069500 quantity=1" in rendered
