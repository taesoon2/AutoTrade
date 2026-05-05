from __future__ import annotations

from datetime import datetime

from autotrade.common import AccountPerformance
from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import OrderStatus
from autotrade.data import KST
from autotrade.report.operation_models import AlertSeverity
from autotrade.report.operation_models import DailyRunReport
from autotrade.report.operation_models import NotificationMessage
from autotrade.report.operation_models import Notifier
from autotrade.report.operation_models import WeeklyReviewReport
from autotrade.report.operation_models import _require_aware_datetime
from autotrade.report.operation_renderers import render_weekly_review_report


def build_order_alert(
    order: ExecutionOrder,
    *,
    created_at: datetime,
) -> NotificationMessage:
    _require_aware_datetime("created_at", created_at)

    severity = AlertSeverity.INFO
    if order.status is OrderStatus.CANCELED:
        severity = AlertSeverity.WARNING
    if order.status is OrderStatus.REJECTED:
        severity = AlertSeverity.ERROR

    lines = [
        f"order_id={order.order_id}",
        f"symbol={order.symbol}",
        f"status={order.status}",
        f"quantity={order.quantity}",
        f"filled_quantity={order.filled_quantity}",
        f"limit_price={order.limit_price}",
        f"updated_at={order.updated_at.isoformat()}",
    ]
    return NotificationMessage(
        created_at=created_at,
        severity=severity,
        subject=f"AutoTrade order {order.symbol} [{order.status}]",
        body="\n".join(lines),
    )


def publish_order_alert(
    notifier: Notifier,
    order: ExecutionOrder,
    *,
    created_at: datetime | None = None,
) -> NotificationMessage:
    notification = build_order_alert(
        order,
        created_at=created_at or datetime.now(KST),
    )
    notifier.send(notification)
    return notification


def build_fill_alert(
    fill: ExecutionFill,
    *,
    created_at: datetime,
) -> NotificationMessage:
    _require_aware_datetime("created_at", created_at)

    return NotificationMessage(
        created_at=created_at,
        severity=AlertSeverity.INFO,
        subject=f"AutoTrade fill {fill.symbol} [{fill.quantity}@{fill.price}]",
        body="\n".join(
            [
                f"fill_id={fill.fill_id}",
                f"order_id={fill.order_id}",
                f"symbol={fill.symbol}",
                f"quantity={fill.quantity}",
                f"price={fill.price}",
                f"filled_at={fill.filled_at.isoformat()}",
            ]
        ),
    )


def publish_fill_alert(
    notifier: Notifier,
    fill: ExecutionFill,
    *,
    created_at: datetime | None = None,
) -> NotificationMessage:
    notification = build_fill_alert(
        fill,
        created_at=created_at or datetime.now(KST),
    )
    notifier.send(notification)
    return notification


def build_daily_run_alert(
    report: DailyRunReport,
    *,
    created_at: datetime,
    account_performance: AccountPerformance | None = None,
) -> NotificationMessage:
    _require_aware_datetime("created_at", created_at)

    severity = AlertSeverity.INFO
    status = "OK"
    if report.failed_jobs > 0:
        severity = AlertSeverity.ERROR
        status = "FAILED"
    elif report.total_jobs == 0:
        severity = AlertSeverity.WARNING
        status = "NO_RUNS"

    lines = [
        f"trading_day={report.trading_day.isoformat()}",
        f"total_jobs={report.total_jobs}",
        f"successful_jobs={report.successful_jobs}",
        f"failed_jobs={report.failed_jobs}",
    ]
    failures = [result.job_name for result in report.job_results if not result.success]
    if failures:
        lines.append(f"failed_job_names={','.join(failures)}")
    if account_performance is not None:
        lines.extend(
            (
                f"account_profit_loss={account_performance.total_profit_loss}",
                "account_profit_loss_rate="
                f"{account_performance.total_profit_loss_rate}",
                f"account_holdings={len(account_performance.holdings)}",
            )
        )

    return NotificationMessage(
        created_at=created_at,
        severity=severity,
        subject=f"AutoTrade daily report {report.trading_day.isoformat()} [{status}]",
        body="\n".join(lines),
    )


def publish_daily_run_alert(
    notifier: Notifier,
    report: DailyRunReport,
    *,
    created_at: datetime | None = None,
    account_performance: AccountPerformance | None = None,
) -> NotificationMessage:
    notification = build_daily_run_alert(
        report,
        created_at=created_at or datetime.now(KST),
        account_performance=account_performance,
    )
    notifier.send(notification)
    return notification


def build_weekly_review_alert(
    report: WeeklyReviewReport,
    *,
    created_at: datetime,
) -> NotificationMessage:
    _require_aware_datetime("created_at", created_at)

    severity = AlertSeverity.INFO
    status = "OK"
    if (
        report.failed_jobs > 0
        or report.failed_inspection_items > 0
        or report.missing_run_report_days
        or report.missing_inspection_report_days
    ):
        severity = AlertSeverity.ERROR
        status = "FAILED"
    elif report.pending_inspection_items > 0 or report.total_jobs == 0:
        severity = AlertSeverity.WARNING
        status = "ATTENTION"

    lines = [
        f"week_start={report.week_start.isoformat()}",
        f"week_end={report.week_end.isoformat()}",
        f"total_jobs={report.total_jobs}",
        f"failed_jobs={report.failed_jobs}",
        f"failed_inspection_items={report.failed_inspection_items}",
        f"pending_inspection_items={report.pending_inspection_items}",
    ]
    if report.missing_run_report_days:
        lines.append(
            "missing_run_report_days="
            + ",".join(day.isoformat() for day in report.missing_run_report_days)
        )
    if report.missing_inspection_report_days:
        lines.append(
            "missing_inspection_report_days="
            + ",".join(day.isoformat() for day in report.missing_inspection_report_days)
        )
    if report.repeated_failure_jobs:
        lines.append("repeated_failure_jobs=" + ",".join(report.repeated_failure_jobs))
    lines.append("")
    lines.append(render_weekly_review_report(report).rstrip())

    return NotificationMessage(
        created_at=created_at,
        severity=severity,
        subject=(
            "AutoTrade weekly review "
            f"{report.week_start.isoformat()}~{report.week_end.isoformat()} [{status}]"
        ),
        body="\n".join(lines),
    )


def publish_weekly_review_alert(
    notifier: Notifier,
    report: WeeklyReviewReport,
    *,
    created_at: datetime | None = None,
) -> NotificationMessage:
    notification = build_weekly_review_alert(
        report,
        created_at=created_at or datetime.now(KST),
    )
    notifier.send(notification)
    return notification
