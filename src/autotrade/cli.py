from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path

import autotrade.runtime.operations as operations
from autotrade.strategy import StrategyKind


def main(argv: Sequence[str] | None = None) -> int:
    operations._configure_logging()
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 2
    return handler(args)


def main_live_cycle_compat(argv: Sequence[str] | None = None) -> int:
    resolved_argv = list(sys.argv[1:] if argv is None else argv)
    if "--continuous" in resolved_argv:
        forwarded = [arg for arg in resolved_argv if arg != "--continuous"]
        return main(["run-continuous", *forwarded])
    return main(["run-once", *resolved_argv])


def main_weekly_review_compat(argv: Sequence[str] | None = None) -> int:
    resolved_argv = list(sys.argv[1:] if argv is None else argv)
    return main(["weekly-review", *resolved_argv])


def main_daily_inspection_compat(argv: Sequence[str] | None = None) -> int:
    resolved_argv = list(sys.argv[1:] if argv is None else argv)
    return main(["daily-inspection", *resolved_argv])


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AutoTrade 운영 작업을 실행합니다.")
    subparsers = parser.add_subparsers(dest="command")

    run_once_parser = subparsers.add_parser(
        "run-once",
        help="장중 운영 사이클을 한 번 실행합니다.",
    )
    _add_runtime_arguments(run_once_parser)
    run_once_parser.set_defaults(handler=operations._handle_run_once)

    run_continuous_parser = subparsers.add_parser(
        "run-continuous",
        help="장전 준비, 장중 매매, 장종료 정리를 scheduler로 연속 실행합니다.",
    )
    _add_runtime_arguments(run_continuous_parser)
    run_continuous_parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="continuous 모드에서 scheduler 평가 횟수를 제한합니다.",
    )
    run_continuous_parser.set_defaults(handler=operations._handle_run_continuous)

    control_parser = subparsers.add_parser(
        "control",
        help="실행 중인 run-continuous runner를 제어합니다.",
    )
    control_subparsers = control_parser.add_subparsers(dest="control_command")
    control_pause_parser = control_subparsers.add_parser(
        "pause",
        help="run-continuous runner를 일시정지합니다.",
    )
    _add_control_arguments(control_pause_parser)
    control_pause_parser.set_defaults(handler=operations._handle_control_pause)
    control_resume_parser = control_subparsers.add_parser(
        "resume",
        help="일시정지된 run-continuous runner를 재개합니다.",
    )
    _add_control_arguments(control_resume_parser)
    control_resume_parser.set_defaults(handler=operations._handle_control_resume)

    market_open_parser = subparsers.add_parser(
        "market-open",
        help="장전 준비 점검만 실행합니다.",
    )
    market_open_parser.add_argument(
        "--strategy",
        default=StrategyKind.THIRTY_MINUTE_TREND.value,
        choices=[kind.value for kind in StrategyKind],
        help="장전 준비에서 사용할 전략 종류입니다.",
    )
    market_open_parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    market_open_parser.set_defaults(handler=operations._handle_market_open)

    market_close_parser = subparsers.add_parser(
        "market-close",
        help="장종료 정리와 주간 리뷰 후처리를 실행합니다.",
    )
    market_close_parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    market_close_parser.add_argument(
        "--paper-cash",
        type=Decimal,
        default=None,
        help=(
            "AUTOTRADE_BROKER_ENV=paper 이고 "
            "AUTOTRADE_PAPER_TRADING_MODE=simulate 일 때만 내부 PaperBroker "
            "초기 현금을 수동 지정합니다. 지정하지 않으면 KIS paper 주문가능현금을 사용합니다."
        ),
    )
    market_close_parser.set_defaults(handler=operations._handle_market_close)

    account_performance_parser = subparsers.add_parser(
        "account-performance",
        help="현재 계좌의 평가손익과 수익률을 조회합니다.",
    )
    account_performance_parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    account_performance_parser.add_argument(
        "--paper-cash",
        type=Decimal,
        default=None,
        help=(
            "AUTOTRADE_BROKER_ENV=paper 이고 "
            "AUTOTRADE_PAPER_TRADING_MODE=simulate 일 때만 내부 PaperBroker "
            "초기 현금을 수동 지정합니다."
        ),
    )
    account_performance_parser.set_defaults(
        handler=operations._handle_account_performance
    )

    backtest_parser = subparsers.add_parser(
        "backtest",
        help="저장된 바 데이터로 단일 종목 백테스트를 실행합니다.",
    )
    backtest_parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    backtest_parser.add_argument(
        "--symbol",
        required=True,
        help="백테스트할 종목 코드입니다.",
    )
    backtest_parser.add_argument(
        "--strategy",
        default=StrategyKind.DAILY_TREND_FOLLOWING.value,
        choices=[kind.value for kind in StrategyKind],
        help="백테스트할 전략 종류입니다.",
    )
    backtest_parser.add_argument(
        "--timeframe",
        default="1d",
        choices=[timeframe.value for timeframe in operations.Timeframe],
        help="읽을 바 데이터 주기입니다.",
    )
    backtest_parser.add_argument(
        "--bar-root",
        type=Path,
        default=None,
        help="CSV 바 데이터 루트 경로입니다. 기본값은 AUTOTRADE_LOG_DIR/bars 입니다.",
    )
    backtest_parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="백테스트 산출물 저장 경로입니다. 기본값은 AUTOTRADE_LOG_DIR/backtests 입니다.",
    )
    backtest_parser.add_argument(
        "--initial-cash",
        type=Decimal,
        default=Decimal("10000000"),
        help="초기 현금입니다.",
    )
    backtest_parser.add_argument(
        "--commission-rate",
        type=Decimal,
        default=Decimal("0"),
        help="매수/매도 수수료율입니다. 예: 0.00015",
    )
    backtest_parser.add_argument(
        "--tax-rate",
        type=Decimal,
        default=Decimal("0"),
        help="매도 세율입니다. 예: 0.0018",
    )
    backtest_parser.add_argument(
        "--slippage-rate",
        type=Decimal,
        default=Decimal("0"),
        help="체결 슬리피지율입니다. 예: 0.0005",
    )
    backtest_parser.add_argument(
        "--in-sample-ratio",
        type=Decimal,
        default=Decimal("0.7"),
        help="in/out sample 분할 비율입니다. 비활성화하려면 0을 지정합니다.",
    )
    backtest_parser.add_argument(
        "--start",
        default=None,
        help="백테스트 시작 시각입니다. ISO 8601 timezone-aware 형식입니다.",
    )
    backtest_parser.add_argument(
        "--end",
        default=None,
        help="백테스트 종료 시각입니다. ISO 8601 timezone-aware 형식입니다.",
    )
    backtest_parser.add_argument(
        "--keep-open-position",
        action="store_false",
        dest="close_open_position_on_finish",
        help="마지막 바에서 열린 포지션을 강제 청산하지 않습니다.",
    )
    backtest_parser.set_defaults(handler=operations._handle_backtest)

    weekly_review_parser = subparsers.add_parser(
        "weekly-review",
        help="주간 리뷰 파일을 생성하고 필요하면 알림을 발행합니다.",
    )
    weekly_review_parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    weekly_review_parser.set_defaults(handler=operations._handle_weekly_review)

    collect_daily_bars_parser = subparsers.add_parser(
        "collect-daily-bars",
        help="추천용 일봉 CSV 바 데이터를 수집합니다.",
    )
    collect_daily_bars_parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    collect_daily_bars_parser.add_argument(
        "--universe-file",
        type=Path,
        required=True,
        help="일봉을 수집할 seed universe CSV 파일 경로입니다.",
    )
    collect_daily_bars_parser.add_argument(
        "--bar-root",
        type=Path,
        default=None,
        help="일봉 CSV 바 데이터 루트 경로입니다. 기본값은 AUTOTRADE_LOG_DIR/bars 입니다.",
    )
    collect_daily_bars_parser.set_defaults(
        handler=operations._handle_collect_daily_bars
    )

    weekly_recommendation_parser = subparsers.add_parser(
        "weekly-recommendation",
        help="주간 종목 후보 리포트를 생성합니다.",
    )
    weekly_recommendation_parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    weekly_recommendation_parser.add_argument(
        "--universe-file",
        type=Path,
        required=True,
        help="추천 대상 seed universe CSV 파일 경로입니다.",
    )
    weekly_recommendation_parser.add_argument(
        "--bar-root",
        type=Path,
        default=None,
        help="일봉 CSV 바 데이터 루트 경로입니다. 기본값은 AUTOTRADE_LOG_DIR/bars 입니다.",
    )
    weekly_recommendation_parser.add_argument(
        "--candidate-count",
        type=int,
        default=20,
        help="최종 후보 개수입니다.",
    )
    weekly_recommendation_parser.add_argument(
        "--minimum-history-days",
        type=int,
        default=121,
        help="종목별 최소 일봉 히스토리 개수입니다.",
    )
    weekly_recommendation_parser.add_argument(
        "--minimum-average-trading-value",
        type=Decimal,
        default=Decimal("1000000000"),
        help="최근 20일 평균 거래대금 최소값입니다.",
    )
    weekly_recommendation_parser.add_argument(
        "--max-candidates-per-sector",
        type=int,
        default=2,
        help="섹터별 최대 후보 개수입니다.",
    )
    weekly_recommendation_parser.add_argument(
        "--exclude-symbol",
        action="append",
        default=[],
        help="추천에서 제외할 심볼입니다. 여러 번 지정할 수 있습니다.",
    )
    weekly_recommendation_parser.add_argument(
        "--exclude-sector",
        action="append",
        default=[],
        help="추천에서 제외할 섹터입니다. 여러 번 지정할 수 있습니다.",
    )
    weekly_recommendation_parser.set_defaults(
        handler=operations._handle_weekly_recommendation
    )

    approve_symbols_parser = subparsers.add_parser(
        "approve-symbols",
        help="주간 후보 중 승인된 종목 목록을 기록합니다.",
    )
    approve_symbols_parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    approve_symbols_parser.add_argument(
        "--symbols",
        required=True,
        help="승인할 종목 코드 목록입니다. 예: 069500,005930,000660",
    )
    approve_symbols_parser.add_argument(
        "--candidate-json",
        type=Path,
        default=None,
        help="검증에 사용할 주간 후보 JSON 파일 경로입니다. 기본값은 최신 후보 리포트입니다.",
    )
    approve_symbols_parser.set_defaults(handler=operations._handle_approve_symbols)

    daily_inspection_parser = subparsers.add_parser(
        "daily-inspection",
        help="수동 일일 점검 체크리스트 파일만 생성합니다.",
    )
    daily_inspection_parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    daily_inspection_parser.set_defaults(handler=operations._handle_daily_inspection)

    return parser


def _add_runtime_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--strategy",
        default=StrategyKind.THIRTY_MINUTE_TREND.value,
        choices=[kind.value for kind in StrategyKind],
        help="실행할 전략 종류입니다.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    parser.add_argument(
        "--bar-root",
        type=Path,
        default=None,
        help="CSV 바 데이터 루트 경로입니다. 기본값은 AUTOTRADE_LOG_DIR/bars 입니다.",
    )
    parser.add_argument(
        "--paper-cash",
        type=Decimal,
        default=None,
        help=(
            "AUTOTRADE_BROKER_ENV=paper 이고 "
            "AUTOTRADE_PAPER_TRADING_MODE=simulate 일 때만 내부 PaperBroker "
            "초기 현금을 수동 지정합니다. 지정하지 않으면 KIS paper 주문가능현금을 사용합니다."
        ),
    )


def _add_control_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )


__all__ = [
    "main",
    "main_daily_inspection_compat",
    "main_live_cycle_compat",
    "main_weekly_review_compat",
]


if __name__ == "__main__":
    raise SystemExit(main())
