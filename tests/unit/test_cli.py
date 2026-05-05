from __future__ import annotations

import autotrade.cli as cli


def test_build_parser_exposes_expected_subcommands() -> None:
    parser = cli._build_parser()
    subparsers_action = next(
        action for action in parser._actions if action.dest == "command"
    )

    assert set(subparsers_action.choices) == {
        "approve-symbols",
        "collect-daily-bars",
        "control",
        "daily-inspection",
        "account-performance",
        "backtest",
        "run-once",
        "run-continuous",
        "market-open",
        "market-close",
        "weekly-recommendation",
        "weekly-review",
    }


def test_main_live_cycle_compat_routes_continuous_to_new_subcommand(
    monkeypatch,
) -> None:
    captured: list[list[str]] = []

    monkeypatch.setattr(
        cli,
        "main",
        lambda argv: captured.append(list(argv)) or 0,
    )

    assert cli.main_live_cycle_compat(["--continuous", "--max-iterations", "2"]) == 0
    assert captured == [["run-continuous", "--max-iterations", "2"]]


def test_main_weekly_review_compat_routes_to_new_subcommand(monkeypatch) -> None:
    captured: list[list[str]] = []

    monkeypatch.setattr(
        cli,
        "main",
        lambda argv: captured.append(list(argv)) or 0,
    )

    assert cli.main_weekly_review_compat(["--env-file", "/tmp/.env"]) == 0
    assert captured == [["weekly-review", "--env-file", "/tmp/.env"]]


def test_main_daily_inspection_compat_routes_to_new_subcommand(monkeypatch) -> None:
    captured: list[list[str]] = []

    monkeypatch.setattr(
        cli,
        "main",
        lambda argv: captured.append(list(argv)) or 0,
    )

    assert cli.main_daily_inspection_compat(["--env-file", "/tmp/.env"]) == 0
    assert captured == [["daily-inspection", "--env-file", "/tmp/.env"]]
