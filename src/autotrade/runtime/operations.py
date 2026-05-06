from __future__ import annotations

import argparse
from csv import writer
import json
import logging
import os
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import replace
from datetime import date
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from autotrade.broker import KoreaInvestmentBarSource
from autotrade.broker import KoreaInvestmentBrokerReader
from autotrade.broker import KoreaInvestmentBrokerTrader
from autotrade.broker import PaperBroker
from autotrade.common import AccountPerformance
from autotrade.common import HoldingPerformance
from autotrade.config import AppSettings
from autotrade.config import ConfigError
from autotrade.config import TelegramSettings
from autotrade.config import load_telegram_settings
from autotrade.data import Bar
from autotrade.data import CsvBarSource
from autotrade.data import CsvBarStore
from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe
from autotrade.data import validate_bar_series
from autotrade.data.validation import normalize_symbol
from autotrade.data.validation import normalize_symbols
from autotrade.execution import BacktestConfig
from autotrade.execution import BacktestCostModel
from autotrade.execution import BacktestEngine
from autotrade.execution import BacktestResult
from autotrade.execution import FileExecutionStateStore
from autotrade.recommendation import ApprovedSymbolsRecord
from autotrade.recommendation import RecommendationArtifacts
from autotrade.recommendation import RecommendationPolicy
from autotrade.recommendation import RECOMMENDATION_DIR
from autotrade.recommendation import build_recommendation_report
from autotrade.recommendation import load_seed_universe_csv
from autotrade.recommendation import write_approved_symbols_bundle
from autotrade.recommendation import write_recommendation_bundle
from autotrade.report import build_daily_inspection_report
from autotrade.report import build_backtest_report
from autotrade.report import build_weekly_review_report
from autotrade.report import BackgroundNotifier
from autotrade.report import CompositeNotifier
from autotrade.report import FileNotifier
from autotrade.report import Notifier
from autotrade.report import publish_weekly_review_alert
from autotrade.report import render_backtest_report
from autotrade.report import TelegramNotifier
from autotrade.report import write_daily_inspection_report
from autotrade.report import write_weekly_review_report
from autotrade.report import load_daily_inspection_reports
from autotrade.report import load_daily_run_reports
from autotrade.runtime.live_cycle import LiveCycleRuntime
from autotrade.runtime.live_cycle import strategy_timeframe_for
from autotrade.runtime.market_close import MarketCloseResult
from autotrade.runtime.market_close import MarketCloseRuntime
from autotrade.runtime.market_open import MarketOpenPreparationRuntime
from autotrade.runtime.operation_environment import load_env_file as _load_env_file_impl
from autotrade.runtime.operation_environment import (
    load_environment as _load_environment_impl,
)
from autotrade.runtime.operation_environment import (
    load_runtime_settings as _load_runtime_settings_impl,
)
from autotrade.runtime.operation_environment import (
    resolve_environment as _resolve_environment_impl,
)
from autotrade.runtime.operation_flows import WeeklyReviewExecution
from autotrade.runtime.operation_flows import (
    build_and_write_weekly_review as _build_and_write_weekly_review_impl,
)
from autotrade.runtime.operation_flows import (
    build_market_close_job as _build_market_close_job_impl,
)
from autotrade.runtime.operation_flows import (
    build_safe_stop_cleanup_handler as _build_safe_stop_cleanup_handler_impl,
)
from autotrade.runtime.operation_flows import (
    build_scheduled_cycle_job as _build_scheduled_cycle_job_impl,
)
from autotrade.runtime.operation_flows import (
    collect_strategy_bars as _collect_strategy_bars_impl,
)
from autotrade.runtime.operation_flows import (
    collection_window_start as _collection_window_start_impl,
)
from autotrade.runtime.operation_flows import (
    execute_live_cycle as _execute_live_cycle_impl,
)
from autotrade.runtime.operation_flows import (
    is_last_trading_day_of_week as _is_last_trading_day_of_week_impl,
)
from autotrade.runtime.operation_flows import (
    maybe_create_weekly_review as _maybe_create_weekly_review_impl,
)
from autotrade.runtime.operation_flows import merge_bar_series as _merge_bar_series_impl
from autotrade.runtime.operation_flows import (
    render_market_close_summary as _render_market_close_summary_impl,
)
from autotrade.runtime.operation_flows import (
    resolve_incremental_collection_start as _resolve_incremental_collection_start_impl,
)
from autotrade.runtime.operation_flows import (
    run_market_close_flow as _run_market_close_flow_impl,
)
from autotrade.runtime.operation_flows import (
    run_resume_maintenance as _run_resume_maintenance_impl,
)
from autotrade.runtime.operation_services import OperationServices
from autotrade.runtime.operation_services import (
    build_broker_clients as _build_broker_clients_impl,
)
from autotrade.runtime.operation_services import build_notifier as _build_notifier_impl
from autotrade.runtime.operation_services import (
    build_operation_services as _build_operation_services_impl,
)
from autotrade.runtime.operation_services import (
    build_paper_broker as _build_paper_broker_impl,
)
from autotrade.runtime.operation_services import (
    build_weekly_review_notifier as _build_weekly_review_notifier_impl,
)
from autotrade.runtime.control import FileRunnerControlStore
from autotrade.runtime.control import RunnerControlState
from autotrade.runtime.runner import ResumeContext
from autotrade.runtime.runner import RunnerStatus
from autotrade.runtime.runner import ScheduledRunner
from autotrade.runtime.telegram_control import BackgroundTelegramControlPoller
from autotrade.runtime.telegram_control import TelegramControlPoller
from autotrade.strategy import create_strategy
from autotrade.strategy import StrategyKind

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ENV_FILE = ROOT / ".env"
ENV_TEMPLATE_FILE = ROOT / "docs" / "autotrade.env.example"
logger = logging.getLogger(__name__)
EXIT_CODE_SUCCESS = 0
EXIT_CODE_OPERATION_FAILED = 1
EXIT_CODE_CONFIGURATION_ERROR = 2


@dataclass(frozen=True, slots=True)
class BacktestArtifacts:
    report_path: Path
    trades_path: Path
    equity_path: Path


def _log_operation_failure(command_name: str, error: Exception) -> None:
    logger.error("%s 실행에 실패했습니다: %s", command_name, error)


def _close_notifier(notifier: Notifier | None) -> None:
    if notifier is None:
        return
    close = getattr(notifier, "close", None)
    if callable(close):
        close()


def _handle_run_once(args: argparse.Namespace) -> int:
    logger.info("AutoTrade 운영 사이클 실행을 준비합니다.")
    settings = _load_runtime_settings(args.env_file)
    if settings is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    services: OperationServices | None = None
    try:
        services = _build_operation_services(
            settings,
            strategy_kind=StrategyKind(args.strategy),
            bar_root=args.bar_root,
            paper_cash_override=args.paper_cash,
        )
        generated_at = datetime.now(KST)
        logger.info("운영 사이클을 실행합니다.")
        result = _execute_live_cycle(
            services.runtime,
            settings=services.settings,
            bar_root=services.bar_root,
            generated_at=generated_at,
        )
    except Exception as exc:
        _log_operation_failure("run-once", exc)
        return EXIT_CODE_OPERATION_FAILED
    finally:
        if services is not None:
            _close_notifier(services.notifier)
    logger.info("운영 사이클 실행이 끝났습니다.")

    print(result.render_korean_summary())
    print(f"알림 파일: {services.notification_log_path}")
    print(f"주문 상태 파일: {services.state_store.path}")
    return EXIT_CODE_SUCCESS


def _handle_run_continuous(args: argparse.Namespace) -> int:
    logger.info("AutoTrade 연속 운영 실행을 준비합니다.")
    settings = _load_runtime_settings(args.env_file)
    if settings is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    services: OperationServices | None = None
    telegram_control_poller: BackgroundTelegramControlPoller | None = None
    try:
        services = _build_operation_services(
            settings,
            strategy_kind=StrategyKind(args.strategy),
            bar_root=args.bar_root,
            paper_cash_override=args.paper_cash,
        )
        preparation_runtime = MarketOpenPreparationRuntime(
            settings=settings,
            strategy_kind=services.strategy_kind.value,
            timeframe=services.runtime.timeframe,
            bar_root=services.bar_root,
            broker_reader=services.broker_reader,
            notifier=services.notifier,
            state_store=services.state_store,
            collect_strategy_bars=_collect_strategy_bars,
        )
        market_close_runtime = MarketCloseRuntime(
            settings=settings,
            broker_reader=services.broker_reader,
            notifier=services.notifier,
            state_store=services.state_store,
        )
        telegram_control_poller = _build_telegram_control_poller(
            settings.telegram,
            control_store=services.control_store,
            account_status_provider=lambda: render_account_performance(
                services.broker_reader.get_account_performance()
            ),
        )
        if telegram_control_poller is not None:
            telegram_control_poller.start()
        runner = ScheduledRunner(
            jobs=(
                preparation_runtime.build_job(),
                _build_scheduled_cycle_job(
                    services.runtime,
                    settings=settings,
                    bar_root=services.bar_root,
                ),
                _build_market_close_job(
                    market_close_runtime,
                    notifier=services.notifier,
                    telegram_settings=settings.telegram,
                ),
            ),
            state_store=services.scheduler_state_store,
            notifier=services.notifier,
            log_dir=settings.log_dir,
            safe_stop_handler=_build_safe_stop_cleanup_handler(
                market_close_runtime,
                notifier=services.notifier,
                telegram_settings=settings.telegram,
            ),
            control_store=services.control_store,
            resume_handler=_build_resume_maintenance_handler(
                services.runtime,
                settings=settings,
                bar_root=services.bar_root,
                market_close_runtime=market_close_runtime,
                notifier=services.notifier,
                telegram_settings=settings.telegram,
            ),
        )
        logger.info("연속 운영 runner를 시작합니다.")
        runner_result = runner.run_forever(max_iterations=args.max_iterations)
    except Exception as exc:
        _log_operation_failure("run-continuous", exc)
        return EXIT_CODE_OPERATION_FAILED
    finally:
        if telegram_control_poller is not None:
            telegram_control_poller.stop()
        if services is not None:
            _close_notifier(services.notifier)
    logger.info(
        "연속 운영 runner가 종료되었습니다. 상태=%s",
        runner_result.status.value,
    )
    print(f"runner 상태: {runner_result.status.value}")
    if runner_result.stop_reason is not None:
        print(f"중지 사유: {runner_result.stop_reason}")
    print(f"알림 파일: {services.notification_log_path}")
    print(f"주문 상태 파일: {services.state_store.path}")
    print(f"scheduler 상태 파일: {services.scheduler_state_store.path}")
    print(f"runner control 상태 파일: {services.control_store.path}")
    if runner_result.status is RunnerStatus.SAFE_STOP:
        return EXIT_CODE_OPERATION_FAILED
    return EXIT_CODE_SUCCESS


def _handle_control_pause(args: argparse.Namespace) -> int:
    return _handle_control_mode(args, mode="pause")


def _handle_control_resume(args: argparse.Namespace) -> int:
    return _handle_control_mode(args, mode="resume")


def _handle_control_mode(args: argparse.Namespace, *, mode: str) -> int:
    settings = _load_runtime_settings(args.env_file)
    if settings is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    store = FileRunnerControlStore(settings.log_dir / "runner_control.json")
    try:
        timestamp = datetime.now(KST)
        if mode == "pause":
            state = store.pause(timestamp=timestamp, source="cli")
        elif mode == "resume":
            state = store.resume(timestamp=timestamp, source="cli")
        else:  # pragma: no cover - defensive branch
            raise ValueError(f"unsupported control mode: {mode}")
    except Exception as exc:
        _log_operation_failure(f"control {mode}", exc)
        return EXIT_CODE_OPERATION_FAILED

    _print_runner_control_state(state, path=store.path)
    return EXIT_CODE_SUCCESS


def _print_runner_control_state(
    state: RunnerControlState,
    *,
    path: Path,
) -> None:
    print(f"runner control 상태: {state.mode.value}")
    if state.paused_at is not None:
        print(f"pause 시각: {state.paused_at.isoformat()}")
    if state.resumed_at is not None:
        print(f"resume 시각: {state.resumed_at.isoformat()}")
    print(f"runner control 상태 파일: {path}")


def _handle_market_open(args: argparse.Namespace) -> int:
    logger.info("장전 준비 실행을 준비합니다.")
    settings = _load_runtime_settings(args.env_file)
    if settings is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    strategy_kind = StrategyKind(args.strategy)
    services: OperationServices | None = None
    try:
        services = _build_operation_services(
            settings,
            strategy_kind=strategy_kind,
            bar_root=None,
            paper_cash_override=None,
        )
        runtime = MarketOpenPreparationRuntime(
            settings=settings,
            strategy_kind=strategy_kind.value,
            timeframe=strategy_timeframe_for(strategy_kind),
            bar_root=services.bar_root,
            broker_reader=services.broker_reader,
            notifier=services.notifier,
            state_store=services.state_store,
            collect_strategy_bars=_collect_strategy_bars,
        )
        result = runtime.run()
    except Exception as exc:
        _log_operation_failure("market-open", exc)
        return EXIT_CODE_OPERATION_FAILED
    finally:
        if services is not None:
            _close_notifier(services.notifier)
    print(result.render_summary())
    print(f"스모크 리포트 파일: {result.smoke_report_path}")
    print(f"점검 리포트 파일: {result.inspection_report_path}")
    if not result.success:
        return EXIT_CODE_OPERATION_FAILED
    return EXIT_CODE_SUCCESS


def _handle_market_close(args: argparse.Namespace) -> int:
    logger.info("장종료 정리 실행을 준비합니다.")
    settings = _load_runtime_settings(args.env_file)
    if settings is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    notifier: Notifier | None = None
    try:
        broker_reader, _ = _build_broker_clients(
            settings,
            paper_cash_override=args.paper_cash,
        )
        notifier = _build_notifier(settings)
        state_store = FileExecutionStateStore(settings.log_dir / "execution_state.json")
        runtime = MarketCloseRuntime(
            settings=settings,
            broker_reader=broker_reader,
            notifier=notifier,
            state_store=state_store,
        )
        generated_at = datetime.now(KST)
        result, weekly_review = _run_market_close_flow(
            runtime,
            notifier=notifier,
            telegram_settings=settings.telegram,
            timestamp=generated_at,
            triggered_at=generated_at,
        )
    except Exception as exc:
        _log_operation_failure("market-close", exc)
        return EXIT_CODE_OPERATION_FAILED
    finally:
        _close_notifier(notifier)

    print(result.render_summary())
    print(f"일일 실행 리포트 파일: {result.daily_run_report_path}")
    print(f"점검 리포트 파일: {result.inspection_report_path}")
    print(f"다음 거래일 준비 파일: {result.next_day_preparation_path}")
    if weekly_review is not None:
        print(f"주간 리뷰 파일: {weekly_review.report_path}")
    return EXIT_CODE_SUCCESS


def _handle_account_performance(args: argparse.Namespace) -> int:
    logger.info("계좌 수익률 조회를 준비합니다.")
    settings = _load_runtime_settings(args.env_file)
    if settings is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    try:
        broker_reader, _ = _build_broker_clients(
            settings,
            paper_cash_override=args.paper_cash,
        )
        account_performance = broker_reader.get_account_performance()
    except Exception as exc:
        _log_operation_failure("account-performance", exc)
        return EXIT_CODE_OPERATION_FAILED

    print(render_account_performance(account_performance))
    return EXIT_CODE_SUCCESS


def _handle_weekly_review(args: argparse.Namespace) -> int:
    environment = _load_environment(args.env_file)
    if environment is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    log_dir = _require_log_dir(environment)
    if log_dir is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    generated_at = datetime.now(KST)
    try:
        telegram_settings = load_telegram_settings(environment)
    except ConfigError as exc:
        logger.error("설정 로딩에 실패했습니다: %s", exc)
        return EXIT_CODE_CONFIGURATION_ERROR

    notifier: Notifier | None = None
    try:
        weekly_review = _build_and_write_weekly_review(
            log_dir=log_dir,
            generated_at=generated_at,
        )
        if telegram_settings.enabled:
            notifier = _build_weekly_review_notifier(log_dir, telegram_settings)
            publish_weekly_review_alert(
                notifier,
                weekly_review.report,
                created_at=generated_at,
            )
    except Exception as exc:
        _log_operation_failure("weekly-review", exc)
        return EXIT_CODE_OPERATION_FAILED
    finally:
        _close_notifier(notifier)
    print(weekly_review.report_path)
    return EXIT_CODE_SUCCESS


def render_account_performance(account_performance: AccountPerformance) -> str:
    lines = [
        f"계좌 수익률: {_format_rate(account_performance.total_profit_loss_rate)}",
        f"평가손익: {_format_krw(account_performance.total_profit_loss, signed=True)}",
        f"총매입금액: {_format_krw(account_performance.total_purchase_amount)}",
        f"총평가금액: {_format_krw(account_performance.total_evaluation_amount)}",
        f"주문가능금액: {_format_krw(account_performance.cash_available)}",
        f"보유종목수: {len(account_performance.holdings)}",
    ]
    if account_performance.holdings:
        lines.append("보유종목:")
        for holding in account_performance.holdings:
            lines.append(
                " ".join(
                    (
                        f"- {_holding_display_name(holding)}",
                        f"quantity={holding.quantity}",
                        f"average_price={_format_krw(holding.average_price)}",
                        f"current_price={_format_krw(holding.current_price)}",
                        "profit_loss="
                        f"{_format_krw(holding.profit_loss, signed=True)}",
                        f"profit_loss_rate={_format_rate(holding.profit_loss_rate)}",
                    )
                )
            )
    return "\n".join(lines)


def _holding_display_name(holding: HoldingPerformance) -> str:
    name = holding.name
    if isinstance(name, str) and name.strip():
        return f"{name.strip()}({holding.symbol})"
    return holding.symbol


def _format_krw(value: Decimal, *, signed: bool = False) -> str:
    rounded = value.quantize(Decimal("1"))
    sign = "+" if signed and rounded > 0 else ""
    return f"{sign}{rounded:,.0f}원"


def _format_rate(value: Decimal) -> str:
    rounded = value.quantize(Decimal("0.01"))
    sign = "+" if rounded > 0 else ""
    return f"{sign}{rounded:.2f}%"


def _handle_backtest(args: argparse.Namespace) -> int:
    logger.info("백테스트 실행을 준비합니다.")
    environment = _load_environment(args.env_file)
    if environment is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    try:
        timeframe = Timeframe(args.timeframe)
        start = _parse_optional_datetime_argument(args.start, field_name="start")
        end = _parse_optional_datetime_argument(args.end, field_name="end")
        log_dir = _resolve_backtest_log_dir(environment)
        resolved_bar_root = args.bar_root or (log_dir / "bars")
        output_dir = args.output_dir or (log_dir / "backtests")
        bars = CsvBarSource(resolved_bar_root).load_bars(
            args.symbol,
            timeframe,
            start=start,
            end=end,
        )
        if not bars:
            raise ValueError(
                "backtest bars not found. "
                f"symbol={normalize_symbol(args.symbol)} "
                f"timeframe={timeframe.value} "
                f"bar_root={resolved_bar_root}"
            )
        result = BacktestEngine().run(
            create_strategy(StrategyKind(args.strategy)),
            bars,
            BacktestConfig(
                initial_cash=args.initial_cash,
                cost_model=BacktestCostModel(
                    commission_rate=args.commission_rate,
                    tax_rate=args.tax_rate,
                    slippage_rate=args.slippage_rate,
                ),
                in_sample_ratio=(
                    None
                    if args.in_sample_ratio == Decimal("0")
                    else args.in_sample_ratio
                ),
                close_open_position_on_finish=args.close_open_position_on_finish,
            ),
        )
        artifacts = _write_backtest_artifacts(
            result,
            output_dir=output_dir,
            generated_at=datetime.now(KST),
        )
    except Exception as exc:
        _log_operation_failure("backtest", exc)
        return EXIT_CODE_OPERATION_FAILED

    print(_render_backtest_stdout(result, artifacts))
    return EXIT_CODE_SUCCESS


def _resolve_backtest_log_dir(environment: Mapping[str, str]) -> Path:
    raw_log_dir = environment.get("AUTOTRADE_LOG_DIR", "./logs")
    if not raw_log_dir.strip():
        return Path("./logs")
    return Path(raw_log_dir)


def _parse_optional_datetime_argument(
    value: str | None,
    *,
    field_name: str,
) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return parsed


def _write_backtest_artifacts(
    result: BacktestResult,
    *,
    output_dir: Path,
    generated_at: datetime,
) -> BacktestArtifacts:
    output_dir.mkdir(parents=True, exist_ok=True)
    report = build_backtest_report(result)
    stem = (
        f"backtest_{normalize_symbol(result.symbol)}_{result.timeframe.value}_"
        f"{generated_at.strftime('%Y%m%dT%H%M%S')}"
    )
    report_path = output_dir / f"{stem}.txt"
    trades_path = output_dir / f"{stem}_trades.csv"
    equity_path = output_dir / f"{stem}_equity.csv"

    report_path.write_text(render_backtest_report(report), encoding="utf-8")
    _write_backtest_trades_csv(result, trades_path)
    _write_backtest_equity_csv(result, equity_path)
    return BacktestArtifacts(
        report_path=report_path,
        trades_path=trades_path,
        equity_path=equity_path,
    )


def _write_backtest_trades_csv(result: BacktestResult, path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        csv_writer = writer(handle)
        csv_writer.writerow(
            (
                "symbol",
                "entered_at",
                "exited_at",
                "quantity",
                "entry_price",
                "exit_price",
                "entry_fees",
                "exit_fees",
                "gross_pnl",
                "net_pnl",
                "holding_period_bars",
                "exit_reason",
            )
        )
        for trade in result.trades:
            csv_writer.writerow(
                (
                    trade.symbol,
                    trade.entered_at.isoformat(),
                    trade.exited_at.isoformat(),
                    trade.quantity,
                    str(trade.entry_price),
                    str(trade.exit_price),
                    str(trade.entry_fees),
                    str(trade.exit_fees),
                    str(trade.gross_pnl),
                    str(trade.net_pnl),
                    trade.holding_period_bars,
                    trade.exit_reason,
                )
            )


def _write_backtest_equity_csv(result: BacktestResult, path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        csv_writer = writer(handle)
        csv_writer.writerow(
            (
                "symbol",
                "timestamp",
                "close_price",
                "cash",
                "position_quantity",
                "position_average_price",
                "position_market_value",
                "realized_pnl",
                "unrealized_pnl",
                "total_pnl",
                "total_equity",
            )
        )
        for snapshot in result.snapshots:
            csv_writer.writerow(
                (
                    snapshot.symbol,
                    snapshot.timestamp.isoformat(),
                    str(snapshot.close_price),
                    str(snapshot.cash),
                    snapshot.position_quantity,
                    str(snapshot.position_average_price),
                    str(snapshot.position_market_value),
                    str(snapshot.realized_pnl),
                    str(snapshot.unrealized_pnl),
                    str(snapshot.total_pnl),
                    str(snapshot.total_equity),
                )
            )


def _render_backtest_stdout(
    result: BacktestResult,
    artifacts: BacktestArtifacts,
) -> str:
    report = build_backtest_report(result)
    combined = report.combined
    lines = [
        f"symbol={report.symbol}",
        f"timeframe={report.timeframe}",
        f"period={result.started_at.isoformat()}..{result.finished_at.isoformat()}",
        f"initial_cash={report.initial_cash}",
        f"final_equity={combined.final_equity}",
        f"net_profit={combined.net_profit}",
        f"total_return={_format_backtest_ratio(combined.total_return)}",
        f"cagr={_format_optional_backtest_ratio(combined.cagr)}",
        f"max_drawdown={_format_backtest_ratio(-combined.max_drawdown)}",
        f"trade_count={combined.trade_count}",
        f"win_rate={_format_optional_backtest_ratio(combined.win_rate)}",
        f"profit_factor={_format_optional_backtest_decimal(combined.profit_factor)}",
        f"report={artifacts.report_path}",
        f"trades={artifacts.trades_path}",
        f"equity={artifacts.equity_path}",
    ]
    return "\n".join(lines)


def _format_backtest_ratio(value: Decimal) -> str:
    return f"{(value * Decimal('100')).quantize(Decimal('0.01'))}%"


def _format_optional_backtest_ratio(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    if value.is_infinite():
        return "inf"
    return _format_backtest_ratio(value)


def _format_optional_backtest_decimal(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    if value.is_infinite():
        return "inf"
    return str(value.normalize())


def _handle_collect_daily_bars(args: argparse.Namespace) -> int:
    logger.info("추천용 일봉 수집을 준비합니다.")
    settings = _load_runtime_settings(args.env_file)
    if settings is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    generated_at = datetime.now(KST)
    resolved_bar_root = args.bar_root or (settings.log_dir / "bars")
    try:
        output_dir = _collect_daily_bars_for_universe(
            settings=settings,
            universe_file=args.universe_file,
            bar_root=resolved_bar_root,
            generated_at=generated_at,
        )
    except Exception as exc:
        _log_operation_failure("collect-daily-bars", exc)
        return EXIT_CODE_OPERATION_FAILED
    print(output_dir)
    return EXIT_CODE_SUCCESS


def _handle_weekly_recommendation(args: argparse.Namespace) -> int:
    environment = _load_environment(args.env_file)
    if environment is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    log_dir = _require_log_dir(environment)
    if log_dir is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    generated_at = datetime.now(KST)
    resolved_bar_root = args.bar_root or (log_dir / "bars")
    policy = RecommendationPolicy(
        min_history_days=args.minimum_history_days,
        min_average_traded_value=args.minimum_average_trading_value,
        top_n=args.candidate_count,
        max_per_sector=args.max_candidates_per_sector,
        excluded_symbols=tuple(args.exclude_symbol),
        excluded_sectors=tuple(args.exclude_sector),
    )
    try:
        artifacts = _build_and_write_weekly_recommendation(
            log_dir=log_dir,
            universe_file=args.universe_file,
            bar_root=resolved_bar_root,
            generated_at=generated_at,
            policy=policy,
        )
    except Exception as exc:
        _log_operation_failure("weekly-recommendation", exc)
        return EXIT_CODE_OPERATION_FAILED
    print(artifacts.markdown_path)
    print(artifacts.csv_path)
    print(artifacts.json_path)
    return EXIT_CODE_SUCCESS


def _handle_approve_symbols(args: argparse.Namespace) -> int:
    environment = _load_environment(args.env_file)
    if environment is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    log_dir = _require_log_dir(environment)
    if log_dir is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    generated_at = datetime.now(KST)
    try:
        candidate_report, candidate_report_path = _load_candidate_report_for_approval(
            log_dir=log_dir,
            candidate_json=args.candidate_json,
        )
        approved_symbols = tuple(
            symbol.strip() for symbol in _strip_optional_quotes(args.symbols).split(",")
        )
        approved_record = _approve_symbols(
            log_dir=log_dir,
            approved_symbols=approved_symbols,
            candidate_payload=candidate_report,
            candidate_report_path=candidate_report_path,
            created_at=generated_at,
        )
    except Exception as exc:
        _log_operation_failure("approve-symbols", exc)
        return EXIT_CODE_OPERATION_FAILED
    print(approved_record.latest_path)
    return EXIT_CODE_SUCCESS


def _handle_daily_inspection(args: argparse.Namespace) -> int:
    environment = _load_environment(args.env_file)
    if environment is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    log_dir = _require_log_dir(environment)
    if log_dir is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    generated_at = datetime.now(KST)
    report = build_daily_inspection_report(
        generated_at.date(),
        generated_at=generated_at,
    )
    report_path = write_daily_inspection_report(log_dir, report)
    print(report_path)
    return EXIT_CODE_SUCCESS


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def _load_runtime_settings(env_file: Path) -> AppSettings | None:
    return _load_runtime_settings_impl(
        env_file,
        env_template_file=ENV_TEMPLATE_FILE,
        logger=logger,
    )


def _load_environment(env_file: Path) -> dict[str, str] | None:
    return _load_environment_impl(
        env_file,
        env_template_file=ENV_TEMPLATE_FILE,
        logger=logger,
    )


def _build_operation_services(
    settings: AppSettings,
    *,
    strategy_kind: StrategyKind,
    bar_root: Path | None,
    paper_cash_override: Decimal | None,
) -> OperationServices:
    return _build_operation_services_impl(
        settings,
        strategy_kind=strategy_kind,
        bar_root=bar_root,
        paper_cash_override=paper_cash_override,
        logger=logger,
        build_notifier=_build_notifier,
        build_broker_clients=_build_broker_clients,
    )


def _build_broker_clients(
    settings: AppSettings,
    *,
    paper_cash_override: Decimal | None,
):
    return _build_broker_clients_impl(
        settings,
        paper_cash_override=paper_cash_override,
        logger=logger,
        build_paper_broker=_build_paper_broker,
        reader_cls=KoreaInvestmentBrokerReader,
        trader_cls=KoreaInvestmentBrokerTrader,
    )


def _build_notifier(settings: AppSettings) -> Notifier:
    return _build_notifier_impl(
        settings,
        file_notifier_cls=FileNotifier,
        composite_notifier_cls=CompositeNotifier,
        telegram_notifier_cls=TelegramNotifier,
        background_notifier_cls=BackgroundNotifier,
    )


def _build_weekly_review_notifier(
    log_dir: Path,
    telegram_settings: TelegramSettings,
) -> Notifier:
    return _build_weekly_review_notifier_impl(
        log_dir,
        telegram_settings,
        file_notifier_cls=FileNotifier,
        composite_notifier_cls=CompositeNotifier,
        telegram_notifier_cls=TelegramNotifier,
        background_notifier_cls=BackgroundNotifier,
    )


def _resolve_environment(
    base_environment: Mapping[str, str],
    *,
    env_file: Path,
) -> dict[str, str]:
    return _resolve_environment_impl(
        base_environment,
        env_file=env_file,
        env_template_file=ENV_TEMPLATE_FILE,
        logger=logger,
    )


def _load_env_file(path: Path) -> dict[str, str]:
    return _load_env_file_impl(path)


def _strip_optional_quotes(value: str) -> str:
    return (
        value[1:-1]
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}
        else value
    )


def _collect_strategy_bars(
    settings: AppSettings,
    *,
    bar_root: Path,
    timeframe: Timeframe,
    generated_at: datetime,
) -> None:
    return _collect_strategy_bars_impl(
        settings,
        bar_root=bar_root,
        timeframe=timeframe,
        generated_at=generated_at,
        logger=logger,
        bar_source_factory=KoreaInvestmentBarSource,
        bar_store_factory=CsvBarStore,
        cached_bar_source_factory=CsvBarSource,
        calendar_factory=KrxRegularSessionCalendar,
        validate_bars=validate_bar_series,
    )


def _execute_live_cycle(
    runtime: LiveCycleRuntime,
    *,
    settings: AppSettings,
    bar_root: Path,
    generated_at: datetime,
):
    return _execute_live_cycle_impl(
        runtime,
        settings=settings,
        bar_root=bar_root,
        generated_at=generated_at,
        collect_strategy_bars=_collect_strategy_bars,
    )


def _resolve_incremental_collection_start(
    cached_bars: tuple[Bar, ...],
    *,
    timeframe: Timeframe,
    window_start: datetime,
    generated_at: datetime,
    calendar: KrxRegularSessionCalendar,
    symbol: str,
) -> datetime | None:
    return _resolve_incremental_collection_start_impl(
        cached_bars,
        timeframe=timeframe,
        window_start=window_start,
        generated_at=generated_at,
        calendar=calendar,
        symbol=symbol,
        logger=logger,
        validate_bars=validate_bar_series,
    )


def _merge_bar_series(
    cached_bars: tuple[Bar, ...],
    fetched_bars: tuple[Bar, ...],
) -> tuple[Bar, ...]:
    return _merge_bar_series_impl(cached_bars, fetched_bars)


def _build_scheduled_cycle_job(
    runtime: LiveCycleRuntime,
    *,
    settings: AppSettings,
    bar_root: Path,
):
    return _build_scheduled_cycle_job_impl(
        runtime,
        settings=settings,
        bar_root=bar_root,
        execute_live_cycle=_execute_live_cycle,
    )


def _build_market_close_job(
    runtime: MarketCloseRuntime,
    *,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
):
    return _build_market_close_job_impl(
        runtime,
        notifier=notifier,
        telegram_settings=telegram_settings,
        run_market_close_flow=_run_market_close_flow,
        render_market_close_summary=_render_market_close_summary,
    )


def _build_safe_stop_cleanup_handler(
    runtime: MarketCloseRuntime,
    *,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
):
    return _build_safe_stop_cleanup_handler_impl(
        runtime,
        notifier=notifier,
        telegram_settings=telegram_settings,
        run_market_close_flow=_run_market_close_flow,
        render_market_close_summary=_render_market_close_summary,
    )


def _build_resume_maintenance_handler(
    runtime: LiveCycleRuntime,
    *,
    settings: AppSettings,
    bar_root: Path,
    market_close_runtime: MarketCloseRuntime,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
):
    def handler(context: ResumeContext) -> str:
        return _run_resume_maintenance(
            context,
            runtime=runtime,
            settings=settings,
            bar_root=bar_root,
            market_close_runtime=market_close_runtime,
            notifier=notifier,
            telegram_settings=telegram_settings,
        )

    return handler


def _run_resume_maintenance(
    context: ResumeContext,
    *,
    runtime: LiveCycleRuntime,
    settings: AppSettings,
    bar_root: Path,
    market_close_runtime: MarketCloseRuntime,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
) -> str:
    return _run_resume_maintenance_impl(
        context,
        runtime=runtime,
        settings=settings,
        bar_root=bar_root,
        market_close_runtime=market_close_runtime,
        notifier=notifier,
        telegram_settings=telegram_settings,
        collect_strategy_bars=_collect_strategy_bars,
        run_market_close_flow=_run_market_close_flow,
        render_market_close_summary=_render_market_close_summary,
    )


def _build_telegram_control_poller(
    telegram_settings: TelegramSettings,
    *,
    control_store: FileRunnerControlStore,
    account_status_provider: Callable[[], str] | None = None,
) -> BackgroundTelegramControlPoller | None:
    if not telegram_settings.enabled:
        return None
    return BackgroundTelegramControlPoller(
        TelegramControlPoller(
            settings=telegram_settings,
            control_store=control_store,
            notifier=TelegramNotifier(replace(telegram_settings, max_retries=0)),
            clock=lambda: datetime.now(KST),
            account_status_provider=account_status_provider,
            runner_control_enabled=telegram_settings.control_enabled,
        )
    )


def _run_market_close_flow(
    runtime: MarketCloseRuntime,
    *,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
    timestamp: datetime,
    triggered_at: datetime | None = None,
    safe_stop_reason: str | None = None,
    safe_stop_detail: str | None = None,
) -> tuple[MarketCloseResult, WeeklyReviewExecution | None]:
    return _run_market_close_flow_impl(
        runtime,
        notifier=notifier,
        telegram_settings=telegram_settings,
        timestamp=timestamp,
        maybe_create_weekly_review=_maybe_create_weekly_review,
        triggered_at=triggered_at,
        safe_stop_reason=safe_stop_reason,
        safe_stop_detail=safe_stop_detail,
    )


def _maybe_create_weekly_review(
    *,
    log_dir: Path,
    trading_day: date,
    generated_at: datetime,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
    calendar: KrxRegularSessionCalendar,
) -> WeeklyReviewExecution | None:
    return _maybe_create_weekly_review_impl(
        log_dir=log_dir,
        trading_day=trading_day,
        generated_at=generated_at,
        notifier=notifier,
        telegram_settings=telegram_settings,
        calendar=calendar,
        logger=logger,
        is_last_trading_day_of_week=_is_last_trading_day_of_week,
        build_and_write_weekly_review=_build_and_write_weekly_review,
        publish_weekly_review_alert=publish_weekly_review_alert,
    )


def _build_and_write_weekly_review(
    *,
    log_dir: Path,
    generated_at: datetime,
) -> WeeklyReviewExecution:
    return _build_and_write_weekly_review_impl(
        log_dir=log_dir,
        generated_at=generated_at,
        build_weekly_review_report=build_weekly_review_report,
        load_daily_run_reports=load_daily_run_reports,
        load_daily_inspection_reports=load_daily_inspection_reports,
        write_weekly_review_report=write_weekly_review_report,
    )


def _build_and_write_weekly_recommendation(
    *,
    log_dir: Path,
    universe_file: Path,
    bar_root: Path,
    generated_at: datetime,
    policy: RecommendationPolicy,
) -> RecommendationArtifacts:
    universe = load_seed_universe_csv(universe_file)
    bar_source = CsvBarSource(bar_root)
    bars_by_symbol = {
        member.symbol: bar_source.load_bars(
            member.symbol,
            Timeframe.DAY,
            end=generated_at,
        )
        for member in universe
    }
    report = build_recommendation_report(
        universe,
        bars_by_symbol,
        policy,
        as_of=_resolve_weekly_recommendation_as_of(
            bars_by_symbol,
            generated_at=generated_at,
        ),
        generated_at=generated_at,
    )
    return write_recommendation_bundle(log_dir, report)


def _resolve_weekly_recommendation_as_of(
    bars_by_symbol: Mapping[str, tuple[Bar, ...]],
    *,
    generated_at: datetime,
) -> date:
    latest_dates = [
        series[-1].timestamp.astimezone(KST).date()
        for series in bars_by_symbol.values()
        if series
    ]
    if latest_dates:
        return max(latest_dates)
    return generated_at.astimezone(KST).date()


def _collect_daily_bars_for_universe(
    *,
    settings: AppSettings,
    universe_file: Path,
    bar_root: Path,
    generated_at: datetime,
) -> Path:
    universe = load_seed_universe_csv(universe_file)
    collection_settings = replace(
        settings,
        target_symbols=tuple(member.symbol for member in universe),
    )
    _collect_strategy_bars(
        collection_settings,
        bar_root=bar_root,
        timeframe=Timeframe.DAY,
        generated_at=generated_at,
    )
    return bar_root / Timeframe.DAY.value


def _load_candidate_report_for_approval(
    *,
    log_dir: Path,
    candidate_json: Path | None,
):
    if candidate_json is not None:
        return _load_candidate_payload(candidate_json), candidate_json
    latest_path = log_dir / RECOMMENDATION_DIR / "weekly_candidates_latest.json"
    payload = _load_candidate_payload(latest_path)
    if payload is None:
        raise ValueError("latest weekly recommendation report was not found")
    return payload, latest_path


def _approve_symbols(
    *,
    log_dir: Path,
    approved_symbols: tuple[str, ...],
    candidate_payload: dict[str, object],
    candidate_report_path: Path,
    created_at: datetime,
):
    selected = candidate_payload.get("selected")
    if not isinstance(selected, list):
        raise ValueError("candidate report payload must contain a selected list")
    candidate_symbols = {_extract_candidate_symbol(candidate) for candidate in selected}
    resolved_symbols = normalize_symbols(approved_symbols)
    unknown_symbols = sorted(
        symbol for symbol in resolved_symbols if symbol not in candidate_symbols
    )
    if unknown_symbols:
        raise ValueError(
            "approved symbols must exist in the candidate report: "
            + ",".join(unknown_symbols)
        )
    return write_approved_symbols_bundle(
        log_dir,
        ApprovedSymbolsRecord(
            as_of=created_at.date(),
            approved_at=created_at,
            symbols=resolved_symbols,
            source_report=str(candidate_report_path),
        ),
    )


def _is_last_trading_day_of_week(
    trading_day: date,
    calendar: KrxRegularSessionCalendar,
) -> bool:
    return _is_last_trading_day_of_week_impl(trading_day, calendar)


def _render_market_close_summary(
    result: MarketCloseResult,
    weekly_review: WeeklyReviewExecution | None,
) -> str:
    return _render_market_close_summary_impl(result, weekly_review)


def _collection_window_start(timeframe: Timeframe, generated_at: datetime) -> datetime:
    return _collection_window_start_impl(timeframe, generated_at)


def _build_paper_broker(
    settings: AppSettings,
    *,
    paper_cash_override: Decimal | None,
    state_path: Path | None = None,
) -> tuple[PaperBroker, Decimal]:
    return _build_paper_broker_impl(
        settings,
        paper_cash_override=paper_cash_override,
        state_path=state_path,
        reader_cls=KoreaInvestmentBrokerReader,
        broker_cls=PaperBroker,
    )


def _require_log_dir(environment: Mapping[str, str]) -> Path | None:
    raw_log_dir = environment.get("AUTOTRADE_LOG_DIR")
    if raw_log_dir is None or not raw_log_dir.strip():
        logger.error(
            "설정 로딩에 실패했습니다: Missing required setting AUTOTRADE_LOG_DIR"
        )
        return None
    return Path(os.path.expandvars(raw_log_dir)).expanduser()


def _load_candidate_payload(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("candidate report payload must be an object")
    return payload


def _extract_candidate_symbol(payload: object) -> str:
    if not isinstance(payload, dict):
        raise ValueError("candidate entry must be an object")
    symbol = payload.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("candidate entry must include a symbol")
    return symbol


__all__ = [
    "DEFAULT_ENV_FILE",
    "ENV_TEMPLATE_FILE",
    "EXIT_CODE_CONFIGURATION_ERROR",
    "EXIT_CODE_OPERATION_FAILED",
    "EXIT_CODE_SUCCESS",
    "OperationServices",
    "WeeklyReviewExecution",
    "_build_notifier",
    "_build_paper_broker",
    "_build_safe_stop_cleanup_handler",
    "_build_scheduled_cycle_job",
    "_build_resume_maintenance_handler",
    "_build_telegram_control_poller",
    "_build_and_write_weekly_recommendation",
    "_build_weekly_review_notifier",
    "_collection_window_start",
    "_configure_logging",
    "_collect_daily_bars_for_universe",
    "_collect_strategy_bars",
    "_execute_live_cycle",
    "_handle_approve_symbols",
    "_handle_collect_daily_bars",
    "_handle_control_pause",
    "_handle_control_resume",
    "_handle_daily_inspection",
    "_handle_account_performance",
    "_handle_market_close",
    "_handle_market_open",
    "_handle_run_continuous",
    "_handle_run_once",
    "_handle_weekly_recommendation",
    "_handle_weekly_review",
    "_is_last_trading_day_of_week",
    "_load_env_file",
    "_load_environment",
    "_load_runtime_settings",
    "_maybe_create_weekly_review",
    "_merge_bar_series",
    "_render_market_close_summary",
    "_resolve_environment",
    "_resolve_incremental_collection_start",
    "_run_market_close_flow",
    "_run_resume_maintenance",
    "render_account_performance",
    "timedelta",
]
