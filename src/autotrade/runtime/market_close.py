from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import date
from datetime import datetime
from decimal import Decimal
import logging
from pathlib import Path

from autotrade.broker import BrokerReader
from autotrade.common import AccountPerformance
from autotrade.common import ExecutionFill
from autotrade.common import OrderStatus
from autotrade.config import AppSettings
from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.execution import FileExecutionStateStore
from autotrade.execution import OrderExecutionSnapshot
from autotrade.report import DailyInspectionItem
from autotrade.report import DailyInspectionReport
from autotrade.report import DailyRunReport
from autotrade.report import InspectionStatus
from autotrade.report import InspectionWindow
from autotrade.report import Notifier
from autotrade.report import build_daily_inspection_report
from autotrade.report import build_daily_run_report
from autotrade.report import load_daily_inspection_report
from autotrade.report import load_job_run_results
from autotrade.report import publish_daily_run_alert
from autotrade.report import write_daily_inspection_report
from autotrade.report import write_daily_run_report
from autotrade.scheduler import JobContext
from autotrade.scheduler import JobRunResult
from autotrade.scheduler import MarketSessionPhase
from autotrade.scheduler import ScheduledJob
from autotrade.runtime.intraday_risk_state import FileIntradayRiskStateStore
from autotrade.runtime.intraday_risk_state import IntradayRiskState

logger = logging.getLogger(__name__)

_OPEN_ORDER_STATUSES = {
    OrderStatus.PENDING,
    OrderStatus.ACKNOWLEDGED,
    OrderStatus.PARTIALLY_FILLED,
    OrderStatus.CANCEL_PENDING,
}


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class NextTradingDayPreparation:
    trading_day: date
    next_trading_day: date
    generated_at: datetime
    items: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_aware_datetime("generated_at", self.generated_at)

    def render(self) -> str:
        lines = [
            f"trading_day={self.trading_day.isoformat()}",
            f"next_trading_day={self.next_trading_day.isoformat()}",
            f"generated_at={self.generated_at.isoformat()}",
        ]
        lines.extend(f"item={item}" for item in self.items)
        return "\n".join(lines) + "\n"


@dataclass(frozen=True, slots=True)
class MarketCloseResult:
    generated_at: datetime
    trading_day: date
    total_jobs: int
    failed_jobs: int
    order_snapshots: int
    daily_fills: int
    holdings: int
    open_orders: int
    rejected_orders: int
    daily_run_report_path: Path
    inspection_report_path: Path
    next_day_preparation_path: Path
    account_profit_loss: Decimal | None = None
    account_profit_loss_rate: Decimal | None = None

    def __post_init__(self) -> None:
        _require_aware_datetime("generated_at", self.generated_at)

    def render_summary(self) -> str:
        parts = [
            f"trading_day={self.trading_day.isoformat()}",
            f"total_jobs={self.total_jobs}",
            f"failed_jobs={self.failed_jobs}",
            f"order_snapshots={self.order_snapshots}",
            f"daily_fills={self.daily_fills}",
            f"holdings={self.holdings}",
            f"open_orders={self.open_orders}",
            f"rejected_orders={self.rejected_orders}",
        ]
        if self.account_profit_loss is not None:
            parts.append(f"account_profit_loss={self.account_profit_loss}")
        if self.account_profit_loss_rate is not None:
            parts.append(f"account_profit_loss_rate={self.account_profit_loss_rate}")
        parts.extend(
            [
                f"daily_report={self.daily_run_report_path.name}",
                f"inspection_report={self.inspection_report_path.name}",
                f"next_day_preparation={self.next_day_preparation_path.name}",
            ]
        )
        return " ".join(parts)


@dataclass(slots=True)
class MarketCloseRuntime:
    settings: AppSettings
    broker_reader: BrokerReader
    notifier: Notifier
    state_store: FileExecutionStateStore
    calendar: KrxRegularSessionCalendar = field(
        default_factory=KrxRegularSessionCalendar
    )
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(KST))

    def run(
        self,
        *,
        timestamp: datetime | None = None,
        triggered_at: datetime | None = None,
        safe_stop_reason: str | None = None,
        safe_stop_detail: str | None = None,
        additional_job_results: Sequence[JobRunResult] = (),
    ) -> MarketCloseResult:
        generated_at = timestamp or self.clock()
        _require_aware_datetime("generated_at", generated_at)
        started_at = triggered_at or generated_at
        _require_aware_datetime("started_at", started_at)
        logger.info(
            "장 종료 후 결과 정리를 시작합니다. 시각=%s", generated_at.isoformat()
        )
        try:
            result = self._run_success(
                generated_at=generated_at,
                started_at=started_at,
                safe_stop_reason=safe_stop_reason,
                safe_stop_detail=safe_stop_detail,
                additional_job_results=tuple(additional_job_results),
            )
        except Exception as exc:
            logger.exception("장 종료 정리 중 오류가 발생했습니다.")
            try:
                self._write_failure_artifacts(
                    generated_at=generated_at,
                    started_at=started_at,
                    error=str(exc),
                    safe_stop_reason=safe_stop_reason,
                    safe_stop_detail=safe_stop_detail,
                    additional_job_results=tuple(additional_job_results),
                )
            except Exception:  # pragma: no cover - defensive reporting path
                logger.exception("장 종료 실패 산출물 생성에도 실패했습니다.")
            raise
        logger.info("장 종료 후 결과 정리를 마쳤습니다. %s", result.render_summary())
        return result

    def run_safe_stop_cleanup(
        self,
        *,
        timestamp: datetime,
        reason: str,
        detail: str,
    ) -> MarketCloseResult:
        return self.run(
            timestamp=timestamp,
            triggered_at=timestamp,
            safe_stop_reason=reason,
            safe_stop_detail=detail,
            additional_job_results=_safe_stop_additional_job_results(
                timestamp=timestamp,
                reason=reason,
                detail=detail,
            ),
        )

    def build_job(
        self,
        *,
        name: str = "market_close_cleanup",
    ) -> ScheduledJob:
        def handler(context: JobContext) -> str:
            result = self.run(
                timestamp=context.scheduled_at,
                triggered_at=context.triggered_at,
            )
            return result.render_summary()

        return ScheduledJob(
            name=name,
            phase=MarketSessionPhase.MARKET_CLOSE,
            handler=handler,
        )

    def _run_success(
        self,
        *,
        generated_at: datetime,
        started_at: datetime,
        safe_stop_reason: str | None,
        safe_stop_detail: str | None,
        additional_job_results: tuple[JobRunResult, ...],
    ) -> MarketCloseResult:
        trading_day = generated_at.astimezone(KST).date()
        job_results = load_job_run_results(self.settings.log_dir, trading_day)
        snapshots = self.state_store.list_snapshots()
        holdings_error: str | None = None
        account_performance: AccountPerformance | None = None
        try:
            account_performance = self.broker_reader.get_account_performance()
            holdings = tuple(
                holding
                for holding in account_performance.holdings
                if holding.quantity > 0
            )
        except Exception:
            try:
                holdings = self.broker_reader.get_holdings()
            except Exception as holdings_exc:
                holdings = ()
                holdings_error = str(holdings_exc)
                logger.exception("장 종료 정리용 계좌 성과/잔고 조회에 실패했습니다.")
        daily_snapshots = _daily_order_snapshots(snapshots, trading_day=trading_day)
        daily_fills = _daily_fills(snapshots, trading_day=trading_day)
        open_orders = _open_order_snapshots(snapshots)
        rejected_orders = _rejected_order_snapshots(daily_snapshots)

        finished_at = self.clock()
        detail = _market_close_detail(
            order_snapshots=len(daily_snapshots),
            daily_fills=len(daily_fills),
            open_orders=len(open_orders),
            rejected_orders=len(rejected_orders),
            holdings=len(holdings),
            safe_stop_reason=safe_stop_reason,
            safe_stop_detail=safe_stop_detail,
        )
        current_job_result = JobRunResult(
            job_name="market_close_cleanup",
            phase=MarketSessionPhase.MARKET_CLOSE,
            scheduled_at=generated_at,
            started_at=started_at,
            finished_at=finished_at,
            success=holdings_error is None,
            detail=detail,
            error=holdings_error,
        )
        merged_job_results = _merge_job_results(
            job_results,
            *additional_job_results,
            current_job_result,
        )
        daily_report = build_daily_run_report(
            trading_day,
            merged_job_results,
            generated_at=finished_at,
        )
        daily_report_path = write_daily_run_report(self.settings.log_dir, daily_report)
        publish_daily_run_alert(
            self.notifier,
            daily_report,
            created_at=finished_at,
            account_performance=account_performance,
        )
        next_day = _next_trading_day(trading_day, calendar=self.calendar)
        next_day_preparation = _build_next_day_preparation(
            trading_day=trading_day,
            next_trading_day=next_day,
            generated_at=finished_at,
            failed_job_results=tuple(
                result for result in daily_report.job_results if not result.success
            ),
            open_orders=open_orders,
            rejected_orders=rejected_orders,
            safe_stop_reason=safe_stop_reason,
            safe_stop_detail=safe_stop_detail,
        )
        next_day_preparation_path = _write_next_day_preparation(
            self.settings.log_dir,
            next_day_preparation,
        )
        inspection_report = _build_market_close_inspection_report(
            log_dir=self.settings.log_dir,
            trading_day=trading_day,
            generated_at=finished_at,
            max_loss=self.settings.risk.max_loss,
            daily_report_path=daily_report_path,
            next_day_preparation_path=next_day_preparation_path,
            daily_report=daily_report,
            order_snapshots=daily_snapshots,
            daily_fills=daily_fills,
            open_orders=open_orders,
            rejected_orders=rejected_orders,
            safe_stop_reason=safe_stop_reason,
            safe_stop_detail=safe_stop_detail,
            account_performance=account_performance,
        )
        inspection_report_path = write_daily_inspection_report(
            self.settings.log_dir,
            inspection_report,
        )
        return MarketCloseResult(
            generated_at=finished_at,
            trading_day=trading_day,
            total_jobs=daily_report.total_jobs,
            failed_jobs=daily_report.failed_jobs,
            order_snapshots=len(daily_snapshots),
            daily_fills=len(daily_fills),
            holdings=len(holdings),
            open_orders=len(open_orders),
            rejected_orders=len(rejected_orders),
            daily_run_report_path=daily_report_path,
            inspection_report_path=inspection_report_path,
            next_day_preparation_path=next_day_preparation_path,
            account_profit_loss=(
                None
                if account_performance is None
                else account_performance.total_profit_loss
            ),
            account_profit_loss_rate=(
                None
                if account_performance is None
                else account_performance.total_profit_loss_rate
            ),
        )

    def _write_failure_artifacts(
        self,
        *,
        generated_at: datetime,
        started_at: datetime,
        error: str,
        safe_stop_reason: str | None,
        safe_stop_detail: str | None,
        additional_job_results: tuple[JobRunResult, ...],
    ) -> None:
        trading_day = generated_at.astimezone(KST).date()
        finished_at = self.clock()
        current_job_result = JobRunResult(
            job_name="market_close_cleanup",
            phase=MarketSessionPhase.MARKET_CLOSE,
            scheduled_at=generated_at,
            started_at=started_at,
            finished_at=finished_at,
            success=False,
            error=error,
        )
        merged_job_results = _merge_job_results(
            load_job_run_results(self.settings.log_dir, trading_day),
            *additional_job_results,
            current_job_result,
        )
        daily_report = build_daily_run_report(
            trading_day,
            merged_job_results,
            generated_at=finished_at,
        )
        daily_report_path = write_daily_run_report(self.settings.log_dir, daily_report)
        publish_daily_run_alert(
            self.notifier,
            daily_report,
            created_at=finished_at,
        )
        next_day = _next_trading_day(trading_day, calendar=self.calendar)
        next_day_preparation = NextTradingDayPreparation(
            trading_day=trading_day,
            next_trading_day=next_day,
            generated_at=finished_at,
            items=(
                *(
                    (_safe_stop_item(safe_stop_reason, safe_stop_detail),)
                    if safe_stop_reason is not None and safe_stop_detail is not None
                    else ()
                ),
                f"시장 마감 정리 실패 원인 확인: {error}",
                "daily report와 inspection report를 검토한 뒤 수동 복구 여부 판단",
            ),
        )
        next_day_preparation_path = _write_next_day_preparation(
            self.settings.log_dir,
            next_day_preparation,
        )
        inspection_report = _build_failure_inspection_report(
            log_dir=self.settings.log_dir,
            trading_day=trading_day,
            generated_at=finished_at,
            daily_report_path=daily_report_path,
            next_day_preparation_path=next_day_preparation_path,
            error=error,
            safe_stop_reason=safe_stop_reason,
            safe_stop_detail=safe_stop_detail,
        )
        write_daily_inspection_report(self.settings.log_dir, inspection_report)


def build_market_close_job(
    runtime: MarketCloseRuntime,
    *,
    name: str = "market_close_cleanup",
) -> ScheduledJob:
    return runtime.build_job(name=name)


def _market_close_detail(
    *,
    order_snapshots: int,
    daily_fills: int,
    open_orders: int,
    rejected_orders: int,
    holdings: int,
    safe_stop_reason: str | None = None,
    safe_stop_detail: str | None = None,
) -> str:
    parts = [
        f"order_snapshots={order_snapshots}",
        f"daily_fills={daily_fills}",
        f"open_orders={open_orders}",
        f"rejected_orders={rejected_orders}",
        f"holdings={holdings}",
    ]
    if safe_stop_reason is not None and safe_stop_detail is not None:
        parts.append(f"safe_stop_reason={safe_stop_reason}")
        parts.append(f"safe_stop_detail={safe_stop_detail}")
    return " ".join(parts)


def _merge_job_results(
    existing: tuple[JobRunResult, ...],
    *new_results: JobRunResult,
) -> tuple[JobRunResult, ...]:
    merged: dict[tuple[str, MarketSessionPhase, datetime], JobRunResult] = {
        (result.job_name, result.phase, result.scheduled_at): result
        for result in existing
    }
    for result in new_results:
        merged[(result.job_name, result.phase, result.scheduled_at)] = result
    return tuple(
        sorted(
            merged.values(),
            key=lambda result: (result.scheduled_at, result.job_name),
        )
    )


def _daily_order_snapshots(
    snapshots: tuple[OrderExecutionSnapshot, ...],
    *,
    trading_day: date,
) -> tuple[OrderExecutionSnapshot, ...]:
    return tuple(
        snapshot
        for snapshot in snapshots
        if snapshot.order.created_at.astimezone(KST).date() == trading_day
    )


def _daily_fills(
    snapshots: tuple[OrderExecutionSnapshot, ...],
    *,
    trading_day: date,
) -> tuple[ExecutionFill, ...]:
    return tuple(
        fill
        for snapshot in snapshots
        for fill in snapshot.fills
        if fill.filled_at.astimezone(KST).date() == trading_day
    )


def _open_order_snapshots(
    snapshots: tuple[OrderExecutionSnapshot, ...],
) -> tuple[OrderExecutionSnapshot, ...]:
    return tuple(
        snapshot
        for snapshot in snapshots
        if snapshot.order.status in _OPEN_ORDER_STATUSES
    )


def _rejected_order_snapshots(
    snapshots: tuple[OrderExecutionSnapshot, ...],
) -> tuple[OrderExecutionSnapshot, ...]:
    return tuple(
        snapshot
        for snapshot in snapshots
        if snapshot.order.status is OrderStatus.REJECTED
    )


def _build_market_close_inspection_report(
    *,
    log_dir: Path,
    trading_day: date,
    generated_at: datetime,
    max_loss: Decimal | None,
    daily_report_path: Path,
    next_day_preparation_path: Path,
    daily_report: DailyRunReport,
    order_snapshots: tuple[OrderExecutionSnapshot, ...],
    daily_fills: tuple[ExecutionFill, ...],
    open_orders: tuple[OrderExecutionSnapshot, ...],
    rejected_orders: tuple[OrderExecutionSnapshot, ...],
    safe_stop_reason: str | None,
    safe_stop_detail: str | None,
    account_performance: AccountPerformance | None = None,
) -> DailyInspectionReport:
    items = _merged_inspection_items(
        log_dir=log_dir,
        trading_day=trading_day,
        generated_at=generated_at,
    )
    failure_details = ",".join(
        f"{result.job_name}:{result.error}"
        for result in daily_report.job_results
        if not result.success and result.error is not None
    )
    safe_stop_note = _safe_stop_note(safe_stop_reason, safe_stop_detail)
    updates = {
        (InspectionWindow.INTRADAY, "스케줄러 정상 동작 확인"): DailyInspectionItem(
            window=InspectionWindow.INTRADAY,
            label="스케줄러 정상 동작 확인",
            status=(
                InspectionStatus.FAILED
                if daily_report.failed_jobs > 0 or safe_stop_note is not None
                else InspectionStatus.PASSED
            ),
            detail=(
                f"total_jobs={daily_report.total_jobs} "
                f"failed_jobs={daily_report.failed_jobs}"
                + (f" {safe_stop_note}" if safe_stop_note is not None else "")
            ),
        ),
        (InspectionWindow.INTRADAY, "주문/체결 이벤트 확인"): DailyInspectionItem(
            window=InspectionWindow.INTRADAY,
            label="주문/체결 이벤트 확인",
            status=InspectionStatus.PASSED,
            detail=(f"order_snapshots={len(order_snapshots)} fills={len(daily_fills)}"),
        ),
        (InspectionWindow.INTRADAY, "미체결 건수 확인"): DailyInspectionItem(
            window=InspectionWindow.INTRADAY,
            label="미체결 건수 확인",
            status=(
                InspectionStatus.FAILED if open_orders else InspectionStatus.PASSED
            ),
            detail=f"open_orders={len(open_orders)}",
        ),
        (InspectionWindow.INTRADAY, "이상 주문 여부 확인"): DailyInspectionItem(
            window=InspectionWindow.INTRADAY,
            label="이상 주문 여부 확인",
            status=(
                InspectionStatus.FAILED if rejected_orders else InspectionStatus.PASSED
            ),
            detail=f"rejected_orders={len(rejected_orders)}",
        ),
        (
            InspectionWindow.INTRADAY,
            "손실 상한 도달 여부 확인",
        ): _build_loss_limit_inspection_item(
            log_dir=log_dir,
            trading_day=trading_day,
            max_loss=max_loss,
        ),
        (InspectionWindow.POST_MARKET, "당일 주문 내역 저장"): DailyInspectionItem(
            window=InspectionWindow.POST_MARKET,
            label="당일 주문 내역 저장",
            status=InspectionStatus.PASSED,
            detail=f"order_snapshots={len(order_snapshots)}",
        ),
        (InspectionWindow.POST_MARKET, "체결 내역 저장"): DailyInspectionItem(
            window=InspectionWindow.POST_MARKET,
            label="체결 내역 저장",
            status=InspectionStatus.PASSED,
            detail=f"fills={len(daily_fills)}",
        ),
        (InspectionWindow.POST_MARKET, "손익 요약 리포트 생성"): DailyInspectionItem(
            window=InspectionWindow.POST_MARKET,
            label="손익 요약 리포트 생성",
            status=InspectionStatus.PASSED,
            detail=(
                f"daily_report={daily_report_path.name} "
                f"failed_jobs={daily_report.failed_jobs}"
                + (
                    ""
                    if account_performance is None
                    else (
                        f" account_profit_loss={account_performance.total_profit_loss}"
                        " account_profit_loss_rate="
                        f"{account_performance.total_profit_loss_rate}"
                    )
                )
            ),
        ),
        (InspectionWindow.POST_MARKET, "오류 로그 점검"): DailyInspectionItem(
            window=InspectionWindow.POST_MARKET,
            label="오류 로그 점검",
            status=(
                InspectionStatus.FAILED
                if daily_report.failed_jobs > 0 or safe_stop_note is not None
                else InspectionStatus.PASSED
            ),
            detail=(
                f"failed_jobs={daily_report.failed_jobs}"
                + (f" failure_details={failure_details}" if failure_details else "")
                + (f" {safe_stop_note}" if safe_stop_note is not None else "")
            ),
        ),
        (
            InspectionWindow.POST_MARKET,
            "다음 거래일 준비 상태 확인",
        ): DailyInspectionItem(
            window=InspectionWindow.POST_MARKET,
            label="다음 거래일 준비 상태 확인",
            status=InspectionStatus.PASSED,
            detail=f"next_day_preparation={next_day_preparation_path.name}",
        ),
    }
    return build_daily_inspection_report(
        trading_day,
        generated_at=generated_at,
        items=_apply_inspection_updates(items, updates),
    )


def _build_loss_limit_inspection_item(
    *,
    log_dir: Path,
    trading_day: date,
    max_loss: Decimal | None,
) -> DailyInspectionItem:
    state = FileIntradayRiskStateStore(log_dir / "intraday_risk_state.json").load()
    if max_loss is None:
        return DailyInspectionItem(
            window=InspectionWindow.INTRADAY,
            label="손실 상한 도달 여부 확인",
            status=InspectionStatus.PASSED,
            detail=_loss_limit_detail(
                max_loss="not_configured",
                state=state,
                state_status="available" if state is not None else "missing",
            ),
        )

    if state is None:
        return DailyInspectionItem(
            window=InspectionWindow.INTRADAY,
            label="손실 상한 도달 여부 확인",
            status=InspectionStatus.PENDING,
            detail=_loss_limit_detail(max_loss=str(max_loss), state_status="missing"),
        )
    if state.trading_day != trading_day:
        return DailyInspectionItem(
            window=InspectionWindow.INTRADAY,
            label="손실 상한 도달 여부 확인",
            status=InspectionStatus.PENDING,
            detail=_loss_limit_detail(
                max_loss=str(max_loss),
                state=state,
                state_status="stale",
            ),
        )

    missing_fields = tuple(
        field_name
        for field_name, value in (
            ("session_start_equity", state.session_start_equity),
            ("latest_equity", state.latest_equity),
        )
        if value is None
    )
    if missing_fields:
        return DailyInspectionItem(
            window=InspectionWindow.INTRADAY,
            label="손실 상한 도달 여부 확인",
            status=InspectionStatus.PENDING,
            detail=_loss_limit_detail(
                max_loss=str(max_loss),
                state=state,
                state_status="incomplete",
                missing_fields=missing_fields,
            ),
        )

    session_start_equity = state.session_start_equity
    latest_equity = state.latest_equity
    assert session_start_equity is not None
    assert latest_equity is not None
    loss_amount = max(session_start_equity - latest_equity, Decimal("0"))
    return DailyInspectionItem(
        window=InspectionWindow.INTRADAY,
        label="손실 상한 도달 여부 확인",
        status=(
            InspectionStatus.FAILED
            if loss_amount >= max_loss
            else InspectionStatus.PASSED
        ),
        detail=_loss_limit_detail(
            max_loss=str(max_loss),
            state=state,
            state_status="available",
            loss_amount=loss_amount,
        ),
    )


def _loss_limit_detail(
    *,
    max_loss: str,
    state: IntradayRiskState | None = None,
    state_status: str,
    missing_fields: tuple[str, ...] = (),
    loss_amount: Decimal | None = None,
) -> str:
    parts = [f"max_loss={max_loss}", f"state={state_status}"]
    if state is not None:
        parts.extend(_intraday_risk_state_detail_parts(state))
    if missing_fields:
        parts.append(f"missing_fields={','.join(missing_fields)}")
    if loss_amount is not None:
        parts.append(f"loss_amount={loss_amount}")
    return " ".join(parts)


def _intraday_risk_state_detail_parts(state: IntradayRiskState) -> list[str]:
    parts = [f"state_trading_day={state.trading_day.isoformat()}"]
    if state.session_start_equity is not None:
        parts.append(f"session_start_equity={state.session_start_equity}")
    if state.peak_equity is not None:
        parts.append(f"peak_equity={state.peak_equity}")
    if state.latest_equity is not None:
        parts.append(f"latest_equity={state.latest_equity}")
    return parts


def _build_failure_inspection_report(
    *,
    log_dir: Path,
    trading_day: date,
    generated_at: datetime,
    daily_report_path: Path,
    next_day_preparation_path: Path,
    error: str,
    safe_stop_reason: str | None,
    safe_stop_detail: str | None,
) -> DailyInspectionReport:
    items = _merged_inspection_items(
        log_dir=log_dir,
        trading_day=trading_day,
        generated_at=generated_at,
    )
    safe_stop_note = _safe_stop_note(safe_stop_reason, safe_stop_detail)
    updates = {
        (InspectionWindow.POST_MARKET, "당일 주문 내역 저장"): DailyInspectionItem(
            window=InspectionWindow.POST_MARKET,
            label="당일 주문 내역 저장",
            status=InspectionStatus.FAILED,
            detail=(
                error + (f" {safe_stop_note}" if safe_stop_note is not None else "")
            ),
        ),
        (InspectionWindow.POST_MARKET, "체결 내역 저장"): DailyInspectionItem(
            window=InspectionWindow.POST_MARKET,
            label="체결 내역 저장",
            status=InspectionStatus.FAILED,
            detail=(
                error + (f" {safe_stop_note}" if safe_stop_note is not None else "")
            ),
        ),
        (InspectionWindow.POST_MARKET, "손익 요약 리포트 생성"): DailyInspectionItem(
            window=InspectionWindow.POST_MARKET,
            label="손익 요약 리포트 생성",
            status=InspectionStatus.PASSED,
            detail=f"daily_report={daily_report_path.name}",
        ),
        (InspectionWindow.POST_MARKET, "오류 로그 점검"): DailyInspectionItem(
            window=InspectionWindow.POST_MARKET,
            label="오류 로그 점검",
            status=InspectionStatus.FAILED,
            detail=(
                error + (f" {safe_stop_note}" if safe_stop_note is not None else "")
            ),
        ),
        (
            InspectionWindow.POST_MARKET,
            "다음 거래일 준비 상태 확인",
        ): DailyInspectionItem(
            window=InspectionWindow.POST_MARKET,
            label="다음 거래일 준비 상태 확인",
            status=InspectionStatus.PASSED,
            detail=f"next_day_preparation={next_day_preparation_path.name}",
        ),
    }
    return build_daily_inspection_report(
        trading_day,
        generated_at=generated_at,
        items=_apply_inspection_updates(items, updates),
    )


def _merged_inspection_items(
    *,
    log_dir: Path,
    trading_day: date,
    generated_at: datetime,
) -> tuple[DailyInspectionItem, ...]:
    default_items = build_daily_inspection_report(
        trading_day,
        generated_at=generated_at,
    ).items
    previous_report = load_daily_inspection_report(log_dir, trading_day)
    previous_items = (
        {}
        if previous_report is None
        else {(item.window, item.label): item for item in previous_report.items}
    )
    return tuple(
        previous_items.get((item.window, item.label), item) for item in default_items
    )


def _apply_inspection_updates(
    items: tuple[DailyInspectionItem, ...],
    updates: dict[tuple[InspectionWindow, str], DailyInspectionItem],
) -> tuple[DailyInspectionItem, ...]:
    return tuple(updates.get((item.window, item.label), item) for item in items)


def _build_next_day_preparation(
    *,
    trading_day: date,
    next_trading_day: date,
    generated_at: datetime,
    failed_job_results: tuple[JobRunResult, ...],
    open_orders: tuple[OrderExecutionSnapshot, ...],
    rejected_orders: tuple[OrderExecutionSnapshot, ...],
    safe_stop_reason: str | None,
    safe_stop_detail: str | None,
) -> NextTradingDayPreparation:
    items: list[str] = []
    if safe_stop_reason is not None and safe_stop_detail is not None:
        items.append(_safe_stop_item(safe_stop_reason, safe_stop_detail))
    if failed_job_results:
        items.append(
            "실패 job 검토: "
            + ", ".join(
                f"{result.job_name}:{result.error}"
                for result in failed_job_results
                if result.error is not None
            )
        )
    if open_orders:
        items.append(
            "미체결 주문 확인: "
            + ", ".join(snapshot.order.order_id for snapshot in open_orders)
        )
    if rejected_orders:
        items.append(
            "거부 주문 원인 확인: "
            + ", ".join(snapshot.order.order_id for snapshot in rejected_orders)
        )
    if not items:
        items.append("특이 사항 없음: 다음 거래일 장전 점검만 수행")
    return NextTradingDayPreparation(
        trading_day=trading_day,
        next_trading_day=next_trading_day,
        generated_at=generated_at,
        items=tuple(items),
    )


def _write_next_day_preparation(
    log_dir: Path,
    preparation: NextTradingDayPreparation,
) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / (
        f"next_day_preparation_{preparation.next_trading_day.strftime('%Y%m%d')}.txt"
    )
    path.write_text(preparation.render(), encoding="utf-8")
    return path


def _next_trading_day(
    trading_day: date,
    *,
    calendar: KrxRegularSessionCalendar,
) -> date:
    current = trading_day
    while True:
        current = current.fromordinal(current.toordinal() + 1)
        if calendar.is_trading_day(current):
            return current


def _safe_stop_note(reason: str | None, detail: str | None) -> str | None:
    if reason is None or detail is None:
        return None
    return f"safe_stop_reason={reason} safe_stop_detail={detail}"


def _safe_stop_item(reason: str, detail: str) -> str:
    return f"safe stop 원인 확인: reason={reason} detail={detail}"


def _safe_stop_additional_job_results(
    *,
    timestamp: datetime,
    reason: str,
    detail: str,
) -> tuple[JobRunResult, ...]:
    if reason == "job_failure":
        return ()
    return (
        JobRunResult(
            job_name="runner_safe_stop",
            phase=MarketSessionPhase.MARKET_CLOSE,
            scheduled_at=timestamp,
            started_at=timestamp,
            finished_at=timestamp,
            success=False,
            error=f"{reason}:{detail}",
        ),
    )
