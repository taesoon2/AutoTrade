from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import autotrade.runtime.operations as operations
from autotrade.common import Signal
from autotrade.common import SignalAction
from autotrade.config import AppSettings
from autotrade.config import BrokerSettings
from autotrade.data import Bar
from autotrade.data import CsvBarStore
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import KST
from autotrade.data import Timeframe


class ScriptedStrategy:
    def __init__(self, actions: Sequence[SignalAction]) -> None:
        self._actions = tuple(actions)

    def generate_signal(self, bars: Sequence[Bar]) -> Signal:
        index = len(bars) - 1
        bar = bars[-1]
        return Signal(
            symbol=bar.symbol,
            action=self._actions[index],
            generated_at=bar.timestamp,
            reason=f"index={index}",
        )


def test_handle_backtest_writes_report_trades_and_equity(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    log_dir = tmp_path / "logs"
    bar_root = tmp_path / "bars"
    output_dir = tmp_path / "backtests"
    CsvBarStore(bar_root).store_bars(_make_daily_bars("069500", [100, 100, 110]))
    monkeypatch.setattr(
        operations,
        "_load_runtime_settings",
        lambda env_file: _settings(log_dir),
    )
    monkeypatch.setattr(
        operations,
        "create_strategy",
        lambda strategy_kind: ScriptedStrategy(
            [SignalAction.BUY, SignalAction.HOLD, SignalAction.SELL]
        ),
    )

    result = operations._handle_backtest(
        argparse.Namespace(
            env_file=tmp_path / ".env",
            symbol="069500",
            strategy=operations.StrategyKind.DAILY_TREND_FOLLOWING.value,
            timeframe=Timeframe.DAY.value,
            bar_root=bar_root,
            output_dir=output_dir,
            initial_cash=Decimal("1000"),
            commission_rate=Decimal("0"),
            tax_rate=Decimal("0"),
            slippage_rate=Decimal("0"),
            in_sample_ratio=Decimal("0"),
            start=None,
            end=None,
            close_open_position_on_finish=True,
        )
    )

    assert result == operations.EXIT_CODE_SUCCESS
    stdout = capsys.readouterr().out
    assert "symbol=069500" in stdout
    assert "timeframe=1d" in stdout
    assert "trade_count=1" in stdout
    artifacts = _parse_artifact_paths(stdout)
    assert set(artifacts) == {"report", "trades", "equity"}
    assert artifacts["report"].exists()
    assert artifacts["trades"].exists()
    assert artifacts["equity"].exists()
    assert "section=combined" in artifacts["report"].read_text(encoding="utf-8")
    assert "069500" in artifacts["trades"].read_text(encoding="utf-8")
    assert "total_equity" in artifacts["equity"].read_text(encoding="utf-8")


def test_handle_backtest_runs_without_broker_environment_when_paths_are_explicit(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    bar_root = tmp_path / "bars"
    output_dir = tmp_path / "backtests"
    CsvBarStore(bar_root).store_bars(_make_daily_bars("069500", [100, 100, 110]))
    monkeypatch.setattr(
        operations,
        "create_strategy",
        lambda strategy_kind: ScriptedStrategy(
            [SignalAction.BUY, SignalAction.HOLD, SignalAction.SELL]
        ),
    )

    result = operations._handle_backtest(
        argparse.Namespace(
            env_file=tmp_path / "missing.env",
            symbol="069500",
            strategy=operations.StrategyKind.DAILY_TREND_FOLLOWING.value,
            timeframe=Timeframe.DAY.value,
            bar_root=bar_root,
            output_dir=output_dir,
            initial_cash=Decimal("1000"),
            commission_rate=Decimal("0"),
            tax_rate=Decimal("0"),
            slippage_rate=Decimal("0"),
            in_sample_ratio=Decimal("0"),
            start=None,
            end=None,
            close_open_position_on_finish=True,
        )
    )

    assert result == operations.EXIT_CODE_SUCCESS
    stdout = capsys.readouterr().out
    assert "symbol=069500" in stdout
    artifacts = _parse_artifact_paths(stdout)
    assert artifacts["report"].is_relative_to(output_dir)


def test_handle_backtest_returns_operation_failure_when_bars_missing(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    monkeypatch.setattr(
        operations,
        "_load_runtime_settings",
        lambda env_file: _settings(tmp_path / "logs"),
    )
    caplog.set_level("ERROR")

    result = operations._handle_backtest(
        argparse.Namespace(
            env_file=tmp_path / ".env",
            symbol="069500",
            strategy=operations.StrategyKind.DAILY_TREND_FOLLOWING.value,
            timeframe=Timeframe.DAY.value,
            bar_root=tmp_path / "missing-bars",
            output_dir=tmp_path / "backtests",
            initial_cash=Decimal("1000"),
            commission_rate=Decimal("0"),
            tax_rate=Decimal("0"),
            slippage_rate=Decimal("0"),
            in_sample_ratio=Decimal("0"),
            start=None,
            end=None,
            close_open_position_on_finish=True,
        )
    )

    assert result == operations.EXIT_CODE_OPERATION_FAILED
    assert "backtest bars not found" in caplog.text


def _parse_artifact_paths(stdout: str) -> dict[str, Path]:
    artifact_lines = (
        line
        for line in stdout.splitlines()
        if line.startswith(("report=", "trades=", "equity="))
    )
    return {
        name: Path(value)
        for name, value in (line.split("=", maxsplit=1) for line in artifact_lines)
    }


def _make_daily_bars(symbol: str, closes: Sequence[int]) -> tuple[Bar, ...]:
    calendar = KrxRegularSessionCalendar()
    timestamp = datetime(2026, 4, 10, 15, 30, tzinfo=KST)
    bars: list[Bar] = []
    for close in closes:
        price = Decimal(str(close))
        bars.append(
            Bar(
                symbol=symbol,
                timeframe=Timeframe.DAY,
                timestamp=timestamp,
                open=price,
                high=price + Decimal("1"),
                low=price - Decimal("1"),
                close=price,
                volume=len(bars) + 1,
            )
        )
        timestamp = calendar.next_timestamp(timestamp, Timeframe.DAY)
    return tuple(bars)


def _settings(log_dir: Path) -> AppSettings:
    return AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
        ),
        target_symbols=("069500",),
        log_dir=log_dir,
    )
