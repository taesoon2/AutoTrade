from __future__ import annotations

import argparse
import json
from datetime import date
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import autotrade.runtime.operations as operations
import pytest
from autotrade.common import OrderCapacity
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderType
from autotrade.common import Quote
from autotrade.config import AppSettings
from autotrade.config import BrokerSettings
from autotrade.config import TelegramSettings
from autotrade.data import Bar
from autotrade.data import CsvBarStore
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import KST
from autotrade.data import Timeframe
from autotrade.recommendation import RecommendationPolicy
from autotrade.report import NotificationMessage
from autotrade.runtime.market_close import MarketCloseRuntime
from autotrade.scheduler import JobContext
from autotrade.scheduler import MarketSessionPhase
from autotrade.scheduler import ScheduledJob


def test_load_env_file_parses_simple_entries(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "AUTOTRADE_BROKER_ENV=paper",
                "export AUTOTRADE_LOG_DIR=./logs",
                'AUTOTRADE_TARGET_SYMBOLS="069500,357870"',
            ]
        ),
        encoding="utf-8",
    )

    parsed = operations._load_env_file(env_file)

    assert parsed == {
        "AUTOTRADE_BROKER_ENV": "paper",
        "AUTOTRADE_LOG_DIR": "./logs",
        "AUTOTRADE_TARGET_SYMBOLS": "069500,357870",
    }


def test_resolve_environment_prefers_shell_values_over_env_file(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "AUTOTRADE_BROKER_ENV=paper",
                "AUTOTRADE_LOG_DIR=./logs-from-file",
            ]
        ),
        encoding="utf-8",
    )

    resolved = operations._resolve_environment(
        {
            "AUTOTRADE_LOG_DIR": "./logs-from-shell",
            "EXTRA": "1",
        },
        env_file=env_file,
    )

    assert resolved["AUTOTRADE_BROKER_ENV"] == "paper"
    assert resolved["AUTOTRADE_LOG_DIR"] == "./logs-from-shell"
    assert resolved["EXTRA"] == "1"


def test_handle_run_once_returns_operational_exit_code_on_runtime_error(
    tmp_path,
    monkeypatch,
    caplog,
) -> None:
    monkeypatch.setattr(
        operations,
        "_load_runtime_settings",
        lambda env_file: _settings(tmp_path / "logs"),
    )
    monkeypatch.setattr(
        operations,
        "_build_operation_services",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    caplog.set_level("ERROR")

    result = operations._handle_run_once(
        argparse.Namespace(
            env_file=tmp_path / ".env",
            strategy=operations.StrategyKind.THIRTY_MINUTE_TREND.value,
            bar_root=None,
            paper_cash=None,
        )
    )

    assert result == operations.EXIT_CODE_OPERATION_FAILED
    assert "run-once 실행에 실패했습니다: boom" in caplog.text


def test_handle_run_continuous_returns_operational_exit_code_on_runtime_error(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        operations,
        "_load_runtime_settings",
        lambda env_file: _settings(tmp_path / "logs"),
    )
    monkeypatch.setattr(
        operations,
        "_build_operation_services",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    result = operations._handle_run_continuous(
        argparse.Namespace(
            env_file=tmp_path / ".env",
            strategy=operations.StrategyKind.THIRTY_MINUTE_TREND.value,
            bar_root=None,
            paper_cash=None,
            max_iterations=None,
        )
    )

    assert result == operations.EXIT_CODE_OPERATION_FAILED


def test_handle_control_pause_writes_runner_control_state(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        operations,
        "_load_runtime_settings",
        lambda env_file: _settings(tmp_path / "logs"),
    )

    result = operations._handle_control_pause(
        argparse.Namespace(env_file=tmp_path / ".env")
    )

    assert result == operations.EXIT_CODE_SUCCESS
    state = operations.FileRunnerControlStore(
        tmp_path / "logs" / "runner_control.json"
    ).load()
    assert state.mode.value == "paused"
    assert state.paused_by == "cli"
    assert "runner control 상태: paused" in capsys.readouterr().out


def test_handle_control_resume_writes_runner_control_state(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        operations,
        "_load_runtime_settings",
        lambda env_file: _settings(tmp_path / "logs"),
    )
    store = operations.FileRunnerControlStore(tmp_path / "logs" / "runner_control.json")
    store.pause(timestamp=datetime(2026, 4, 10, 9, 0, tzinfo=KST), source="cli")

    result = operations._handle_control_resume(
        argparse.Namespace(env_file=tmp_path / ".env")
    )

    assert result == operations.EXIT_CODE_SUCCESS
    state = store.load()
    assert state.mode.value == "running"
    assert state.resumed_by == "cli"


def test_handle_market_close_returns_operational_exit_code_on_runtime_error(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        operations,
        "_load_runtime_settings",
        lambda env_file: _settings(tmp_path / "logs"),
    )
    monkeypatch.setattr(
        operations,
        "_build_broker_clients",
        lambda settings, paper_cash_override: (_ for _ in ()).throw(
            RuntimeError("boom")
        ),
    )

    result = operations._handle_market_close(
        argparse.Namespace(
            env_file=tmp_path / ".env",
            paper_cash=None,
        )
    )

    assert result == operations.EXIT_CODE_OPERATION_FAILED


def test_handle_weekly_review_returns_operational_exit_code_on_runtime_error(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        operations,
        "_load_environment",
        lambda env_file: {"AUTOTRADE_LOG_DIR": str(tmp_path / "logs")},
    )
    monkeypatch.setattr(
        operations,
        "load_telegram_settings",
        lambda env: TelegramSettings(enabled=False),
    )
    monkeypatch.setattr(
        operations,
        "_build_and_write_weekly_review",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    result = operations._handle_weekly_review(
        argparse.Namespace(env_file=tmp_path / ".env")
    )

    assert result == operations.EXIT_CODE_OPERATION_FAILED


def test_handle_weekly_recommendation_returns_operational_exit_code_on_runtime_error(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        operations,
        "_load_environment",
        lambda env_file: {"AUTOTRADE_LOG_DIR": str(tmp_path / "logs")},
    )
    monkeypatch.setattr(
        operations,
        "_build_and_write_weekly_recommendation",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    result = operations._handle_weekly_recommendation(
        argparse.Namespace(
            env_file=tmp_path / ".env",
            universe_file=tmp_path / "seed.csv",
            bar_root=None,
            candidate_count=20,
            minimum_history_days=121,
            minimum_average_trading_value=Decimal("1000000000"),
            max_candidates_per_sector=2,
            exclude_symbol=[],
            exclude_sector=[],
        )
    )

    assert result == operations.EXIT_CODE_OPERATION_FAILED


def test_handle_collect_daily_bars_uses_default_bar_root_and_prints_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = _settings(tmp_path / "logs")
    universe_file = tmp_path / "universe.csv"
    captured: dict[str, object] = {}

    def fake_collect_daily_bars_for_universe(
        *,
        settings: AppSettings,
        universe_file: Path,
        bar_root: Path,
        generated_at: datetime,
    ) -> Path:
        captured["settings"] = settings
        captured["universe_file"] = universe_file
        captured["bar_root"] = bar_root
        captured["generated_at"] = generated_at
        return bar_root / "1d"

    monkeypatch.setattr(
        operations,
        "_load_runtime_settings",
        lambda env_file: settings,
    )
    monkeypatch.setattr(
        operations,
        "_collect_daily_bars_for_universe",
        fake_collect_daily_bars_for_universe,
    )

    result = operations._handle_collect_daily_bars(
        argparse.Namespace(
            env_file=tmp_path / ".env",
            universe_file=universe_file,
            bar_root=None,
        )
    )

    assert result == operations.EXIT_CODE_SUCCESS
    assert captured["settings"] is settings
    assert captured["universe_file"] == universe_file
    assert captured["bar_root"] == settings.log_dir / "bars"
    assert isinstance(captured["generated_at"], datetime)
    assert capsys.readouterr().out == f"{settings.log_dir / 'bars' / '1d'}\n"


def test_require_log_dir_expands_environment_variables(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PWD", str(tmp_path))

    log_dir = operations._require_log_dir({"AUTOTRADE_LOG_DIR": "$PWD/logs"})

    assert log_dir == tmp_path / "logs"


def test_build_weekly_recommendation_uses_latest_cached_daily_bar_as_as_of(
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / "logs"
    bar_root = log_dir / "bars"
    universe_file = tmp_path / "universe.csv"
    universe_file.write_text(
        "\n".join(
            [
                "symbol,name,asset_type,sector,is_etf,is_inverse,is_leveraged,active",
                "069500,KODEX 200,ETF,ETF,1,0,0,1",
            ]
        ),
        encoding="utf-8",
    )
    bars = _make_daily_bars("069500", length=130)
    CsvBarStore(bar_root).store_bars(bars)

    artifacts = operations._build_and_write_weekly_recommendation(
        log_dir=log_dir,
        universe_file=universe_file,
        bar_root=bar_root,
        generated_at=datetime(2026, 12, 1, 12, 0, tzinfo=KST),
        policy=RecommendationPolicy(
            min_history_days=120,
            min_average_traded_value=Decimal("1"),
            top_n=1,
            max_per_sector=1,
        ),
    )

    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["as_of"] == bars[-1].timestamp.astimezone(KST).date().isoformat()
    assert payload["summary"]["selected"] == 1
    assert "stale_daily_bars" not in payload["filter_reason_counts"]


def test_build_weekly_recommendation_reports_missing_bars_when_cache_empty(
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / "logs"
    universe_file = tmp_path / "universe.csv"
    universe_file.write_text(
        "\n".join(
            [
                "symbol,name,asset_type,sector,is_etf,is_inverse,is_leveraged,active",
                "069500,KODEX 200,ETF,ETF,1,0,0,1",
            ]
        ),
        encoding="utf-8",
    )
    generated_at = datetime(2026, 12, 1, 12, 0, tzinfo=KST)

    artifacts = operations._build_and_write_weekly_recommendation(
        log_dir=log_dir,
        universe_file=universe_file,
        bar_root=log_dir / "bars",
        generated_at=generated_at,
        policy=RecommendationPolicy(),
    )

    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["as_of"] == "2026-12-01"
    assert payload["summary"]["selected"] == 0
    assert payload["filter_reason_counts"]["missing_daily_bars"] == 1


def test_collect_daily_bars_for_universe_uses_seed_symbols(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_collect_strategy_bars(
        settings: AppSettings,
        *,
        bar_root: Path,
        timeframe: Timeframe,
        generated_at: datetime,
    ) -> None:
        captured["target_symbols"] = settings.target_symbols
        captured["bar_root"] = bar_root
        captured["timeframe"] = timeframe
        captured["generated_at"] = generated_at

    monkeypatch.setattr(
        operations,
        "_collect_strategy_bars",
        fake_collect_strategy_bars,
    )
    universe_file = tmp_path / "universe.csv"
    universe_file.write_text(
        "\n".join(
            [
                "symbol,name,asset_type,sector,is_etf,is_inverse,is_leveraged,active",
                "069500,KODEX 200,ETF,ETF,1,0,0,1",
                "357870,TIGER CD금리투자KIS,ETF,ETF,1,0,0,1",
            ]
        ),
        encoding="utf-8",
    )
    generated_at = datetime(2026, 4, 24, 12, 0, tzinfo=KST)
    bar_root = tmp_path / "logs" / "bars"

    output_dir = operations._collect_daily_bars_for_universe(
        settings=_settings(tmp_path / "logs"),
        universe_file=universe_file,
        bar_root=bar_root,
        generated_at=generated_at,
    )

    assert output_dir == bar_root / "1d"
    assert captured == {
        "target_symbols": ("069500", "357870"),
        "bar_root": bar_root,
        "timeframe": Timeframe.DAY,
        "generated_at": generated_at,
    }


def test_handle_approve_symbols_returns_operational_exit_code_on_runtime_error(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        operations,
        "_load_environment",
        lambda env_file: {"AUTOTRADE_LOG_DIR": str(tmp_path / "logs")},
    )
    monkeypatch.setattr(
        operations,
        "_load_candidate_report_for_approval",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    result = operations._handle_approve_symbols(
        argparse.Namespace(
            env_file=tmp_path / ".env",
            symbols="069500,005930,000660",
            candidate_json=None,
        )
    )

    assert result == operations.EXIT_CODE_OPERATION_FAILED


def test_collect_strategy_bars_fetches_and_writes_csv(tmp_path, monkeypatch) -> None:
    calls: list[tuple[str, Timeframe, datetime, datetime]] = []

    class FakeBarSource:
        def __init__(self, settings: BrokerSettings) -> None:
            assert settings.provider == "koreainvestment"

        def load_bars(
            self,
            symbol: str,
            timeframe: Timeframe,
            start: datetime | None = None,
            end: datetime | None = None,
        ) -> tuple[Bar, ...]:
            assert start is not None
            assert end is not None
            calls.append((symbol, timeframe, start, end))
            return (
                Bar(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=datetime(
                        2026,
                        4,
                        10,
                        9,
                        0,
                        tzinfo=ZoneInfo("Asia/Seoul"),
                    ),
                    open=Decimal("100"),
                    high=Decimal("105"),
                    low=Decimal("99"),
                    close=Decimal("104"),
                    volume=10,
                ),
            )

    monkeypatch.setattr(operations, "KoreaInvestmentBarSource", FakeBarSource)
    settings = AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
        ),
        target_symbols=("069500",),
        log_dir=tmp_path / "logs",
    )
    generated_at = datetime(2026, 4, 10, 21, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    bar_root = tmp_path / "logs" / "bars"

    operations._collect_strategy_bars(
        settings,
        bar_root=bar_root,
        timeframe=Timeframe.MINUTE_30,
        generated_at=generated_at,
    )

    assert calls == [
        (
            "069500",
            Timeframe.MINUTE_30,
            generated_at - operations.timedelta(days=21),
            generated_at,
        )
    ]
    assert (bar_root / "30m" / "069500.csv").read_text(encoding="utf-8") == (
        "symbol,timeframe,timestamp,open,high,low,close,volume\n"
        "069500,30m,2026-04-10T09:00:00+09:00,100,105,99,104,10\n"
    )


def test_collect_strategy_bars_fetches_only_missing_range_from_cached_tail(
    tmp_path,
    monkeypatch,
) -> None:
    calls: list[tuple[str, Timeframe, datetime, datetime]] = []
    bar_root = tmp_path / "logs" / "bars"
    CsvBarStore(bar_root).store_bars(
        (
            _make_bar("069500", Timeframe.MINUTE_30, "2026-04-10T09:00:00+09:00"),
            _make_bar("069500", Timeframe.MINUTE_30, "2026-04-10T09:30:00+09:00"),
        )
    )

    class FakeBarSource:
        def __init__(self, settings: BrokerSettings) -> None:
            assert settings.provider == "koreainvestment"

        def load_bars(
            self,
            symbol: str,
            timeframe: Timeframe,
            start: datetime | None = None,
            end: datetime | None = None,
        ) -> tuple[Bar, ...]:
            assert start is not None
            assert end is not None
            calls.append((symbol, timeframe, start, end))
            return (
                _make_bar("069500", Timeframe.MINUTE_30, "2026-04-10T10:00:00+09:00"),
            )

    monkeypatch.setattr(operations, "KoreaInvestmentBarSource", FakeBarSource)
    settings = _settings(tmp_path / "logs")
    generated_at = datetime(2026, 4, 10, 10, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    operations._collect_strategy_bars(
        settings,
        bar_root=bar_root,
        timeframe=Timeframe.MINUTE_30,
        generated_at=generated_at,
    )

    assert calls == [
        (
            "069500",
            Timeframe.MINUTE_30,
            datetime(2026, 4, 10, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
            generated_at,
        )
    ]
    assert (bar_root / "30m" / "069500.csv").read_text(encoding="utf-8") == (
        "symbol,timeframe,timestamp,open,high,low,close,volume\n"
        "069500,30m,2026-04-10T09:00:00+09:00,100,105,99,104,10\n"
        "069500,30m,2026-04-10T09:30:00+09:00,100,105,99,104,10\n"
        "069500,30m,2026-04-10T10:00:00+09:00,100,105,99,104,10\n"
    )


def test_collect_strategy_bars_skips_request_when_cache_already_covers_target(
    tmp_path,
    monkeypatch,
) -> None:
    bar_root = tmp_path / "logs" / "bars"
    CsvBarStore(bar_root).store_bars(
        (
            _make_bar("069500", Timeframe.MINUTE_30, "2026-04-10T09:00:00+09:00"),
            _make_bar("069500", Timeframe.MINUTE_30, "2026-04-10T09:30:00+09:00"),
            _make_bar("069500", Timeframe.MINUTE_30, "2026-04-10T10:00:00+09:00"),
        )
    )

    class FakeBarSource:
        def __init__(self, settings: BrokerSettings) -> None:
            assert settings.provider == "koreainvestment"

        def load_bars(
            self,
            symbol: str,
            timeframe: Timeframe,
            start: datetime | None = None,
            end: datetime | None = None,
        ) -> tuple[Bar, ...]:
            raise AssertionError(
                "network fetch should be skipped when cache is current"
            )

    monkeypatch.setattr(operations, "KoreaInvestmentBarSource", FakeBarSource)

    operations._collect_strategy_bars(
        _settings(tmp_path / "logs"),
        bar_root=bar_root,
        timeframe=Timeframe.MINUTE_30,
        generated_at=datetime(2026, 4, 10, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )


def test_build_scheduled_cycle_job_uses_context_scheduled_at(
    tmp_path,
    monkeypatch,
) -> None:
    captured: list[datetime] = []

    class FakeResult:
        def render_summary(self) -> str:
            return "summary"

    class FakeRuntime:
        def build_job(self) -> ScheduledJob:
            return ScheduledJob(
                name="live_cycle",
                phase=MarketSessionPhase.INTRADAY,
                handler=lambda context: "unused",
            )

    def fake_execute_live_cycle(runtime, *, settings, bar_root, generated_at):
        assert settings.target_symbols == ("069500",)
        assert bar_root == tmp_path / "bars"
        captured.append(generated_at)
        return FakeResult()

    monkeypatch.setattr(operations, "_execute_live_cycle", fake_execute_live_cycle)
    job = operations._build_scheduled_cycle_job(
        FakeRuntime(),
        settings=_settings(tmp_path / "logs"),
        bar_root=tmp_path / "bars",
    )

    summary = job.handler(
        JobContext(
            phase=MarketSessionPhase.INTRADAY,
            trading_day=datetime(2026, 4, 10, 0, 0, tzinfo=KST).date(),
            scheduled_at=datetime(2026, 4, 10, 9, 30, tzinfo=KST),
            triggered_at=datetime(2026, 4, 10, 9, 35, tzinfo=KST),
        )
    )

    assert summary == "summary"
    assert captured == [datetime(2026, 4, 10, 9, 30, tzinfo=KST)]


def test_run_resume_maintenance_catches_up_syncs_and_runs_missed_close(
    tmp_path,
    monkeypatch,
) -> None:
    calls: list[tuple[str, datetime]] = []

    class FakeSyncResult:
        total_fills = 1
        total_notifications = 2

        def render_summary(self) -> str:
            return "sync summary"

    class FakeRuntime:
        timeframe = Timeframe.MINUTE_30

        def sync_open_orders(self, *, timestamp):
            calls.append(("sync", timestamp))
            return FakeSyncResult()

    class FakeMarketCloseRuntime:
        calendar = KrxRegularSessionCalendar()

    def fake_collect_strategy_bars(settings, *, bar_root, timeframe, generated_at):
        calls.append(("collect", generated_at))

    def fake_run_market_close_flow(
        runtime,
        *,
        notifier,
        telegram_settings,
        timestamp,
        triggered_at=None,
        safe_stop_reason=None,
        safe_stop_detail=None,
    ):
        assert safe_stop_reason is None
        assert safe_stop_detail is None
        calls.append(("market_close", timestamp))
        calls.append(("triggered", triggered_at))
        return object(), None

    monkeypatch.setattr(
        operations,
        "_collect_strategy_bars",
        fake_collect_strategy_bars,
    )
    monkeypatch.setattr(
        operations,
        "_run_market_close_flow",
        fake_run_market_close_flow,
    )
    monkeypatch.setattr(
        operations,
        "_render_market_close_summary",
        lambda result, weekly_review: "close summary",
    )
    context = operations.ResumeContext(
        paused_at=datetime(2026, 4, 10, 15, 0, tzinfo=KST),
        resumed_at=datetime(2026, 4, 10, 16, 0, tzinfo=KST),
        source="cli",
        state=operations.RunnerControlState(),
        runs=(),
    )

    detail = operations._run_resume_maintenance(
        context,
        runtime=FakeRuntime(),
        settings=_settings(tmp_path / "logs"),
        bar_root=tmp_path / "bars",
        market_close_runtime=FakeMarketCloseRuntime(),
        notifier=RecordingNotifier(),
        telegram_settings=TelegramSettings(enabled=False),
    )

    assert calls == [
        ("collect", datetime(2026, 4, 10, 16, 0, tzinfo=KST)),
        ("sync", datetime(2026, 4, 10, 16, 0, tzinfo=KST)),
        ("market_close", datetime(2026, 4, 10, 15, 30, tzinfo=KST)),
        ("triggered", datetime(2026, 4, 10, 16, 0, tzinfo=KST)),
    ]
    assert "data_catch_up=completed" in detail
    assert "sync_fills=1" in detail
    assert "market_close=close summary" in detail


def test_build_paper_broker_uses_kis_order_capacity_cash(
    tmp_path,
    monkeypatch,
) -> None:
    calls: list[tuple[str, str, Decimal | None]] = []

    class FakeReader:
        def __init__(self, settings: BrokerSettings) -> None:
            assert settings.environment == "paper"

        def get_quote(self, symbol: str) -> Quote:
            calls.append(("quote", symbol, None))
            return Quote(
                symbol=symbol,
                price=Decimal("12345"),
                as_of=datetime(2026, 4, 10, 8, 50, tzinfo=KST),
            )

        def get_order_capacity(
            self,
            symbol: str,
            order_price: Decimal,
        ) -> OrderCapacity:
            calls.append(("capacity", symbol, order_price))
            return OrderCapacity(
                symbol=symbol,
                order_price=order_price,
                max_orderable_quantity=8,
                cash_available=Decimal("7654321"),
            )

    monkeypatch.setattr(operations, "KoreaInvestmentBrokerReader", FakeReader)
    settings = _settings(tmp_path / "logs")

    broker, initial_cash = operations._build_paper_broker(
        settings,
        paper_cash_override=None,
    )

    capacity = broker.get_order_capacity("069500", Decimal("1000"))
    assert calls == [
        ("quote", "069500", None),
        ("capacity", "069500", Decimal("12345")),
    ]
    assert initial_cash == Decimal("7654321")
    assert capacity.cash_available == Decimal("7654321")


def test_build_broker_clients_uses_kis_clients_for_paper_broker_mode(
    tmp_path,
    monkeypatch,
) -> None:
    class FakeReader:
        def __init__(self, settings: BrokerSettings) -> None:
            self.settings = settings

    class FakeTrader:
        def __init__(self, settings: BrokerSettings) -> None:
            self.settings = settings

    monkeypatch.setattr(operations, "KoreaInvestmentBrokerReader", FakeReader)
    monkeypatch.setattr(operations, "KoreaInvestmentBrokerTrader", FakeTrader)
    settings = AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
            paper_trading_mode="broker",
        ),
        target_symbols=("069500",),
        log_dir=tmp_path / "logs",
    )

    reader, trader = operations._build_broker_clients(
        settings,
        paper_cash_override=None,
    )

    assert isinstance(reader, FakeReader)
    assert isinstance(trader, FakeTrader)
    assert reader.settings.paper_trading_mode == "broker"
    assert trader.settings.paper_trading_mode == "broker"


def test_build_broker_clients_rejects_paper_cash_for_paper_broker_mode(
    tmp_path,
) -> None:
    settings = AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
            paper_trading_mode="broker",
        ),
        target_symbols=("069500",),
        log_dir=tmp_path / "logs",
    )

    with pytest.raises(ValueError):
        operations._build_broker_clients(
            settings,
            paper_cash_override=Decimal("5000000"),
        )


def test_build_paper_broker_uses_override_without_kis_lookup(
    tmp_path,
    monkeypatch,
) -> None:
    class FakeReader:
        def __init__(self, settings: BrokerSettings) -> None:
            raise AssertionError("KIS lookup should be skipped when paper cash is set")

    monkeypatch.setattr(operations, "KoreaInvestmentBrokerReader", FakeReader)
    settings = _settings(tmp_path / "logs")

    broker, initial_cash = operations._build_paper_broker(
        settings,
        paper_cash_override=Decimal("5000000"),
    )

    capacity = broker.get_order_capacity("069500", Decimal("1000"))
    assert initial_cash == Decimal("5000000")
    assert capacity.cash_available == Decimal("5000000")


def test_build_paper_broker_restores_persisted_simulated_state(tmp_path) -> None:
    settings = _settings(tmp_path / "logs")
    state_path = settings.log_dir / "paper_broker_state.json"
    broker, initial_cash = operations._build_paper_broker(
        settings,
        paper_cash_override=Decimal("1000000"),
        state_path=state_path,
    )
    assert initial_cash == Decimal("1000000")

    broker.advance_bar(
        Bar(
            symbol="069500",
            timeframe=Timeframe.DAY,
            timestamp=datetime(2026, 4, 10, 15, 30, tzinfo=KST),
            open=Decimal("100000"),
            high=Decimal("100000"),
            low=Decimal("100000"),
            close=Decimal("100000"),
            volume=1,
        )
    )
    broker.submit_order(
        OrderRequest(
            request_id="paper-buy-1",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=2,
            limit_price=Decimal("100000"),
            requested_at=datetime(2026, 4, 10, 15, 30, tzinfo=KST),
            order_type=OrderType.LIMIT,
        )
    )

    restored, restored_cash = operations._build_paper_broker(
        settings,
        paper_cash_override=None,
        state_path=state_path,
    )

    performance = restored.get_account_performance()
    assert restored_cash == Decimal("800000")
    assert performance.cash_available == Decimal("800000")
    assert len(performance.holdings) == 1
    assert performance.holdings[0].symbol == "069500"
    assert performance.holdings[0].quantity == 2


def test_build_paper_broker_backs_up_corrupt_persisted_state(
    tmp_path,
    monkeypatch,
) -> None:
    calls: list[tuple[str, str, Decimal | None]] = []

    class FakeReader:
        def __init__(self, settings: BrokerSettings) -> None:
            assert settings.environment == "paper"

        def get_quote(self, symbol: str) -> Quote:
            calls.append(("quote", symbol, None))
            return Quote(
                symbol=symbol,
                price=Decimal("1000"),
                as_of=datetime(2026, 4, 10, 8, 50, tzinfo=KST),
            )

        def get_order_capacity(
            self,
            symbol: str,
            order_price: Decimal,
        ) -> OrderCapacity:
            calls.append(("capacity", symbol, order_price))
            return OrderCapacity(
                symbol=symbol,
                order_price=order_price,
                max_orderable_quantity=8,
                cash_available=Decimal("123000"),
            )

    monkeypatch.setattr(operations, "KoreaInvestmentBrokerReader", FakeReader)
    settings = _settings(tmp_path / "logs")
    state_path = settings.log_dir / "paper_broker_state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text("{not-json", encoding="utf-8")

    broker, initial_cash = operations._build_paper_broker(
        settings,
        paper_cash_override=None,
        state_path=state_path,
    )

    capacity = broker.get_order_capacity("069500", Decimal("1000"))
    assert initial_cash == Decimal("123000")
    assert capacity.cash_available == Decimal("123000")
    assert calls == [
        ("quote", "069500", None),
        ("capacity", "069500", Decimal("1000")),
    ]
    assert tuple(state_path.parent.glob("paper_broker_state.json.corrupt-*"))


def test_build_safe_stop_cleanup_handler_uses_safe_stop_context(monkeypatch) -> None:
    captured: list[tuple[datetime, str, str]] = []

    class FakeResult:
        trading_day = date(2026, 4, 10)
        generated_at = datetime(2026, 4, 10, 11, 0, tzinfo=KST)

        def render_summary(self) -> str:
            return "cleanup_summary"

    class FakeRuntime:
        settings = _settings(Path("/tmp/logs"))
        calendar = operations.KrxRegularSessionCalendar()

        def run_safe_stop_cleanup(self, *, timestamp, reason, detail):
            captured.append((timestamp, reason, detail))
            return FakeResult()

    monkeypatch.setattr(
        operations, "_maybe_create_weekly_review", lambda **kwargs: None
    )
    handler = operations._build_safe_stop_cleanup_handler(
        FakeRuntime(),
        notifier=RecordingNotifier(),
        telegram_settings=TelegramSettings(enabled=False),
    )

    summary = handler(
        type(
            "SafeStopContextStub",
            (),
            {
                "triggered_at": datetime(2026, 4, 10, 11, 0, tzinfo=KST),
                "reason": "runner_exception",
                "detail": "scheduler loop crashed",
            },
        )()
    )

    assert summary == "cleanup_summary weekly_review=skipped"
    assert captured == [
        (
            datetime(2026, 4, 10, 11, 0, tzinfo=KST),
            "runner_exception",
            "scheduler loop crashed",
        )
    ]


def test_build_notifier_returns_file_notifier_when_telegram_disabled(tmp_path) -> None:
    notifier = operations._build_notifier(_settings(tmp_path / "logs"))

    assert isinstance(notifier, operations.FileNotifier)


def test_build_notifier_returns_composite_when_telegram_enabled(tmp_path) -> None:
    settings = AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
        ),
        target_symbols=("069500",),
        log_dir=tmp_path / "logs",
        telegram=TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-10012345",
        ),
    )

    notifier = operations._build_notifier(settings)

    assert isinstance(notifier, operations.CompositeNotifier)
    assert isinstance(notifier.notifiers[0], operations.FileNotifier)
    assert isinstance(notifier.notifiers[1], operations.BackgroundNotifier)
    assert isinstance(notifier.notifiers[1].notifier, operations.TelegramNotifier)


def test_build_telegram_control_poller_uses_dedicated_no_retry_notifier(
    tmp_path,
) -> None:
    control_store = operations.FileRunnerControlStore(tmp_path / "runner_control.json")
    telegram_settings = TelegramSettings(
        enabled=True,
        bot_token="bot-token",
        chat_id="-10012345",
        max_retries=3,
    )

    def account_status_provider() -> str:
        return "계좌 수익률: +1.23%"

    poller = operations._build_telegram_control_poller(
        telegram_settings,
        control_store=control_store,
        account_status_provider=account_status_provider,
    )

    assert poller is not None
    assert isinstance(poller, operations.BackgroundTelegramControlPoller)
    assert isinstance(poller.poller.notifier, operations.TelegramNotifier)
    assert poller.poller.notifier.settings.max_retries == 0
    assert poller.poller.account_status_provider is account_status_provider


def test_build_telegram_control_poller_keeps_account_query_when_control_disabled(
    tmp_path,
) -> None:
    control_store = operations.FileRunnerControlStore(tmp_path / "runner_control.json")
    telegram_settings = TelegramSettings(
        enabled=True,
        bot_token="bot-token",
        chat_id="-10012345",
        control_enabled=False,
    )

    poller = operations._build_telegram_control_poller(
        telegram_settings,
        control_store=control_store,
    )

    assert poller is not None
    assert poller.poller.runner_control_enabled is False


def test_is_last_trading_day_of_week_handles_friday_holiday() -> None:
    calendar = operations.KrxRegularSessionCalendar(
        holiday_dates=frozenset({date(2026, 4, 10)})
    )

    assert operations._is_last_trading_day_of_week(date(2026, 4, 9), calendar)
    assert not operations._is_last_trading_day_of_week(date(2026, 4, 8), calendar)


def test_run_market_close_flow_skips_weekly_review_before_last_trading_day(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    generated_at = datetime(2026, 4, 9, 15, 30, tzinfo=KST)
    runtime = MarketCloseRuntime(
        settings=_settings(log_dir),
        broker_reader=operations.PaperBroker(initial_cash=Decimal("1000000")),
        notifier=RecordingNotifier(),
        state_store=operations.FileExecutionStateStore(
            log_dir / "execution_state.json"
        ),
        clock=lambda: generated_at,
    )

    result, weekly_review = operations._run_market_close_flow(
        runtime,
        notifier=RecordingNotifier(),
        telegram_settings=TelegramSettings(enabled=False),
        timestamp=generated_at,
        triggered_at=generated_at,
    )

    assert result.daily_run_report_path.exists()
    assert weekly_review is None


def test_run_market_close_flow_generates_weekly_review_on_last_trading_day(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    generated_at = datetime(2026, 4, 10, 15, 30, tzinfo=KST)
    runtime = MarketCloseRuntime(
        settings=_settings(log_dir),
        broker_reader=operations.PaperBroker(initial_cash=Decimal("1000000")),
        notifier=RecordingNotifier(),
        state_store=operations.FileExecutionStateStore(
            log_dir / "execution_state.json"
        ),
        clock=lambda: generated_at,
    )

    result, weekly_review = operations._run_market_close_flow(
        runtime,
        notifier=RecordingNotifier(),
        telegram_settings=TelegramSettings(enabled=False),
        timestamp=generated_at,
        triggered_at=generated_at,
    )

    assert result.daily_run_report_path.exists()
    assert weekly_review is not None
    assert weekly_review.report_path.exists()
    assert weekly_review.report_path.name == "weekly_review_20260406_20260412.txt"


def test_run_market_close_flow_generates_weekly_review_on_safe_stop_last_day(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    generated_at = datetime(2026, 4, 10, 11, 0, tzinfo=KST)
    runtime = MarketCloseRuntime(
        settings=_settings(log_dir),
        broker_reader=operations.PaperBroker(initial_cash=Decimal("1000000")),
        notifier=RecordingNotifier(),
        state_store=operations.FileExecutionStateStore(
            log_dir / "execution_state.json"
        ),
        clock=lambda: generated_at,
    )

    result, weekly_review = operations._run_market_close_flow(
        runtime,
        notifier=RecordingNotifier(),
        telegram_settings=TelegramSettings(enabled=False),
        timestamp=generated_at,
        safe_stop_reason="runner_exception",
        safe_stop_detail="scheduler loop crashed",
    )

    assert result.daily_run_report_path.exists()
    assert weekly_review is not None
    assert weekly_review.report_path.exists()


class RecordingNotifier:
    def __init__(self) -> None:
        self.notifications: list[NotificationMessage] = []

    def send(self, notification: NotificationMessage) -> None:
        self.notifications.append(notification)


def _make_bar(symbol: str, timeframe: Timeframe, timestamp: str) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe=timeframe,
        timestamp=datetime.fromisoformat(timestamp),
        open=Decimal("100"),
        high=Decimal("105"),
        low=Decimal("99"),
        close=Decimal("104"),
        volume=10,
    )


def _make_daily_bars(symbol: str, *, length: int) -> tuple[Bar, ...]:
    calendar = KrxRegularSessionCalendar()
    timestamp = datetime(2026, 1, 2, 15, 30, tzinfo=KST)
    bars: list[Bar] = []
    for index in range(length):
        close = Decimal("100") + Decimal(index)
        bars.append(
            Bar(
                symbol=symbol,
                timeframe=Timeframe.DAY,
                timestamp=timestamp,
                open=close - Decimal("1"),
                high=close + Decimal("1"),
                low=close - Decimal("2"),
                close=close,
                volume=1000,
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
