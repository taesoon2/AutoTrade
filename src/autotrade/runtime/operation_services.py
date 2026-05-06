from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from autotrade.broker import BrokerReader
from autotrade.broker import BrokerTrader
from autotrade.broker import FilePaperBrokerSnapshotStore
from autotrade.broker import KoreaInvestmentBrokerReader
from autotrade.broker import KoreaInvestmentBrokerTrader
from autotrade.broker import PaperBroker
from autotrade.broker import PersistentPaperBroker
from autotrade.config import AppSettings
from autotrade.config import TelegramSettings
from autotrade.data import CsvBarSource
from autotrade.execution import FileExecutionStateStore
from autotrade.report import BackgroundNotifier
from autotrade.report import CompositeNotifier
from autotrade.report import FileNotifier
from autotrade.report import Notifier
from autotrade.report import TelegramNotifier
from autotrade.runtime.control import FileRunnerControlStore
from autotrade.runtime.live_cycle import LiveCycleRuntime
from autotrade.runtime.live_cycle import strategy_timeframe_for
from autotrade.scheduler import FileSchedulerStateStore
from autotrade.strategy import StrategyKind
from autotrade.strategy import create_strategy


@dataclass(frozen=True, slots=True)
class OperationServices:
    settings: AppSettings
    strategy_kind: StrategyKind
    bar_root: Path
    notification_log_path: Path
    notifier: Notifier
    state_store: FileExecutionStateStore
    scheduler_state_store: FileSchedulerStateStore
    control_store: FileRunnerControlStore
    broker_reader: BrokerReader
    broker_trader: BrokerTrader
    runtime: LiveCycleRuntime


def build_operation_services(
    settings: AppSettings,
    *,
    strategy_kind: StrategyKind,
    bar_root: Path | None,
    paper_cash_override: Decimal | None,
    logger: logging.Logger,
    build_notifier: Callable[[AppSettings], Notifier],
    build_broker_clients: Callable[..., tuple[BrokerReader, BrokerTrader]],
) -> OperationServices:
    resolved_bar_root = bar_root or (settings.log_dir / "bars")
    notification_log_path = settings.log_dir / "notifications.jsonl"
    notifier = build_notifier(settings)
    state_store = FileExecutionStateStore(settings.log_dir / "execution_state.json")
    scheduler_state_store = FileSchedulerStateStore(
        settings.log_dir / "scheduler_state.json"
    )
    control_store = FileRunnerControlStore(settings.log_dir / "runner_control.json")
    logger.info(
        "설정을 불러왔습니다. 환경=%s 전략=%s 대상종목=%s",
        settings.broker.environment,
        strategy_kind.value,
        ",".join(settings.target_symbols),
    )
    logger.info("바 데이터 경로: %s", resolved_bar_root)
    logger.info("알림 파일 경로: %s", notification_log_path)
    logger.info("주문 상태 파일 경로: %s", state_store.path)
    logger.info("scheduler 상태 파일 경로: %s", scheduler_state_store.path)
    logger.info("runner control 상태 파일 경로: %s", control_store.path)

    broker_reader, broker_trader = build_broker_clients(
        settings,
        paper_cash_override=paper_cash_override,
    )
    runtime = LiveCycleRuntime(
        settings=settings,
        strategy=create_strategy(strategy_kind),
        timeframe=strategy_timeframe_for(strategy_kind),
        bar_source=CsvBarSource(resolved_bar_root),
        broker_reader=broker_reader,
        broker_trader=broker_trader,
        notifier=notifier,
        state_store=state_store,
    )
    return OperationServices(
        settings=settings,
        strategy_kind=strategy_kind,
        bar_root=resolved_bar_root,
        notification_log_path=notification_log_path,
        notifier=notifier,
        state_store=state_store,
        scheduler_state_store=scheduler_state_store,
        control_store=control_store,
        broker_reader=broker_reader,
        broker_trader=broker_trader,
        runtime=runtime,
    )


def build_broker_clients(
    settings: AppSettings,
    *,
    paper_cash_override: Decimal | None,
    logger: logging.Logger,
    build_paper_broker: Callable[..., tuple[PaperBroker, Decimal]],
    reader_cls: type[KoreaInvestmentBrokerReader] = KoreaInvestmentBrokerReader,
    trader_cls: type[KoreaInvestmentBrokerTrader] = KoreaInvestmentBrokerTrader,
) -> tuple[BrokerReader, BrokerTrader]:
    if _uses_simulated_paper_broker(settings):
        broker, paper_cash = build_paper_broker(
            settings,
            paper_cash_override=paper_cash_override,
            state_path=settings.log_dir / "paper_broker_state.json",
        )
        if paper_cash_override is None:
            logger.info(
                "KIS paper 주문가능현금으로 내부 PaperBroker를 초기화합니다. 초기 현금=%s",
                paper_cash,
            )
        else:
            logger.info(
                "사용자 지정 초기 현금으로 내부 PaperBroker를 실행합니다. 초기 현금=%s",
                paper_cash,
            )
        return broker, broker

    if settings.broker.environment == "paper":
        if paper_cash_override is not None:
            raise ValueError(
                "paper_cash_override is only supported when "
                "AUTOTRADE_PAPER_TRADING_MODE=simulate"
            )
        logger.info("KIS paper 브로커 연동 객체를 초기화합니다.")
        return (
            reader_cls(settings.broker),
            trader_cls(settings.broker),
        )

    logger.info("실브로커(KIS) 연동 객체를 초기화합니다.")
    return (
        reader_cls(settings.broker),
        trader_cls(settings.broker),
    )


def _uses_simulated_paper_broker(settings: AppSettings) -> bool:
    return (
        settings.broker.environment == "paper"
        and settings.broker.paper_trading_mode == "simulate"
    )


def build_notifier(
    settings: AppSettings,
    *,
    file_notifier_cls: type[FileNotifier] = FileNotifier,
    composite_notifier_cls: type[CompositeNotifier] = CompositeNotifier,
    telegram_notifier_cls: type[TelegramNotifier] = TelegramNotifier,
    background_notifier_cls: type[BackgroundNotifier] = BackgroundNotifier,
) -> Notifier:
    file_notifier = file_notifier_cls(settings.log_dir / "notifications.jsonl")
    if not settings.telegram.enabled:
        return file_notifier
    return composite_notifier_cls(
        (
            file_notifier,
            background_notifier_cls(telegram_notifier_cls(settings.telegram)),
        )
    )


def build_weekly_review_notifier(
    log_dir: Path,
    telegram_settings: TelegramSettings,
    *,
    file_notifier_cls: type[FileNotifier] = FileNotifier,
    composite_notifier_cls: type[CompositeNotifier] = CompositeNotifier,
    telegram_notifier_cls: type[TelegramNotifier] = TelegramNotifier,
    background_notifier_cls: type[BackgroundNotifier] = BackgroundNotifier,
) -> Notifier:
    return composite_notifier_cls(
        (
            file_notifier_cls(log_dir / "notifications.jsonl"),
            background_notifier_cls(telegram_notifier_cls(telegram_settings)),
        )
    )


def build_paper_broker(
    settings: AppSettings,
    *,
    paper_cash_override: Decimal | None,
    state_path: Path | None = None,
    reader_cls: type[KoreaInvestmentBrokerReader] = KoreaInvestmentBrokerReader,
    broker_cls: type[PaperBroker] = PaperBroker,
) -> tuple[PaperBroker, Decimal]:
    if state_path is not None and broker_cls is PaperBroker:
        snapshot_store = FilePaperBrokerSnapshotStore(state_path)
        snapshot = snapshot_store.load()
        if snapshot is not None:
            broker = PersistentPaperBroker.restore(
                snapshot,
                snapshot_store=snapshot_store,
            )
            return broker, snapshot.cash

    if paper_cash_override is not None:
        if state_path is not None and broker_cls is PaperBroker:
            snapshot_store = FilePaperBrokerSnapshotStore(state_path)
            broker = PersistentPaperBroker(
                paper_cash_override,
                snapshot_store=snapshot_store,
            )
            broker.persist()
            return broker, paper_cash_override
        return broker_cls(initial_cash=paper_cash_override), paper_cash_override

    reader = reader_cls(settings.broker)
    target_symbol = settings.target_symbols[0]
    quote = reader.get_quote(target_symbol)
    capacity = reader.get_order_capacity(target_symbol, quote.price)
    if state_path is not None and broker_cls is PaperBroker:
        snapshot_store = FilePaperBrokerSnapshotStore(state_path)
        persistent_broker = PersistentPaperBroker(
            capacity.cash_available,
            snapshot_store=snapshot_store,
        )
        persistent_broker.persist()
        return persistent_broker, capacity.cash_available
    return broker_cls(initial_cash=capacity.cash_available), capacity.cash_available
