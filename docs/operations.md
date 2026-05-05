# Operations

The repository includes baseline regular-session `scheduler/report` support.

- `scheduler`: runs market-open, intraday, and market-close jobs on the KRX regular-session calendar.
- `report`: turns run results into operation logs, daily reports, and alerts.
- `AUTOTRADE_BROKER_ENV=paper` uses internal `PaperBroker` by default. To send real KIS paper-account orders, also set `AUTOTRADE_PAPER_TRADING_MODE=broker`.

## Small Live-Operation Rules

- Live mode requires `AUTOTRADE_BROKER_ENV=live` and `AUTOTRADE_RISK_MAX_OPERATING_CAPITAL`.
- KIS paper real-order tests use `AUTOTRADE_BROKER_ENV=paper` + `AUTOTRADE_PAPER_TRADING_MODE=broker`.
- Order/fill alerts come from `build_order_alert`, `build_fill_alert`, `publish_order_alert`, and `publish_fill_alert`.
- `AUTOTRADE_TELEGRAM_ENABLED=true` makes `python -m autotrade.cli ...` fan out to file and Telegram notifiers.
- `weekly-review` reads repo-root `.env` by default, saves the weekly review, and sends Telegram if enabled.
- Telegram defaults to `AUTOTRADE_TELEGRAM_CHAT_ID`; warning/error channels can use `AUTOTRADE_TELEGRAM_WARNING_CHAT_ID` and `AUTOTRADE_TELEGRAM_ERROR_CHAT_ID`.
- Set `AUTOTRADE_TELEGRAM_FORCE_IPV4=true` when the host has a broken IPv6 route to Telegram; this limits Telegram HTTP requests and control polling to IPv4.
- Telegram alert sends run through a bounded background queue. `run-continuous` polls Telegram control commands in a separate background worker and the scheduler reads only `runner_control.json`.

## CLI Behavior

- `run-once`: collects bars into `AUTOTRADE_LOG_DIR/bars`, then runs signals, risk checks, orders, and order/fill alerts once.
- `run-continuous`: runs the scheduler, sleeps until `next_run_at`, and uses `AUTOTRADE_LOG_DIR/scheduler_state.json` to avoid duplicate slots after restart.
- `control pause|resume`: updates `AUTOTRADE_LOG_DIR/runner_control.json` so an active `run-continuous` process can pause before starting later jobs and resume cooperatively.
- `market_close`: writes daily run/inspection reports and, on the last trading day of the week, a weekly review.
- `account-performance`: reads the current broker account balance and prints account-level purchase amount, evaluation amount, profit/loss, profit/loss rate, cash available, and symbol-level holding performance.
- Official CLI: `src/autotrade/cli.py`. In a local `src` checkout, use `PYTHONPATH=src python -m autotrade.cli ...`; compatibility path `python tools/operations.py ...` remains available.
- CLI reads repo-root `.env` by default; template: `docs/autotrade.env.example`.
- Default inputs/outputs: `AUTOTRADE_LOG_DIR/bars`, `notifications.jsonl`, `execution_state.json`, `scheduler_state.json`.
- Runner control state: `AUTOTRADE_LOG_DIR/runner_control.json`.
- Exit codes: `0=success`, `1=operation failure or safe stop`, `2=config/input error`.
- Runtime state files (`execution_state.json`, `scheduler_state.json`, `intraday_risk_state.json`, and `runner_control.json`) are saved by temp file + replace; corrupt files are backed up as `*.corrupt-*` and reset. Runner control reads/writes use a sidecar lock file so the runner and Telegram control poller can share the file safely.
- KIS timeout, URL, and connection failures are normalized to `KoreaInvestmentBrokerError`; retryable read-side requests retry before failing, while order submissions fail fast and let the runner safe-stop.
- `--paper-cash` only applies when `AUTOTRADE_PAPER_TRADING_MODE=simulate`.
- `tools/daily_inspection.py` and `tools/weekly_review.py` create text artifacts under `AUTOTRADE_LOG_DIR`; orchestration connects live executors and external alerts.

## Run Order

1. `cp docs/autotrade.env.example .env`
2. Fill account, symbols, and log path in `.env`.
3. If not installed, run `PYTHONPATH=src python -m autotrade.cli run-once`.
4. Check Korean stdout logs plus `AUTOTRADE_LOG_DIR/bars`, `notifications.jsonl`, and `execution_state.json`.

Useful commands:

- Custom env: `PYTHONPATH=src python -m autotrade.cli run-once --env-file /path/to/custom.env`
- Continuous: `PYTHONPATH=src python -m autotrade.cli run-continuous`
- Pause continuous runner: `PYTHONPATH=src python -m autotrade.cli control pause`
- Resume continuous runner: `PYTHONPATH=src python -m autotrade.cli control resume`
- Market open only: `PYTHONPATH=src python -m autotrade.cli market-open`
- Market close only: `PYTHONPATH=src python -m autotrade.cli market-close`
- Account performance only: `PYTHONPATH=src python -m autotrade.cli account-performance`
- Weekly review only: `PYTHONPATH=src python -m autotrade.cli weekly-review --env-file /path/to/custom.env`
- Compatibility: `python tools/operations.py ...`

## Account Performance

`account-performance` uses the configured broker reader and does not submit orders.
For KIS, it calls domestic `inquire-balance`, normalizes `output1` holdings and
`output2` account totals, and prints:

- account profit/loss rate
- total profit/loss
- total purchase amount
- total evaluation amount
- available cash
- holding count and per-symbol quantity, average price, current price, profit/loss, and profit/loss rate

In simulated paper mode, the same fields are calculated from `PaperBroker` cash,
positions, and the latest market bars. With no position purchase amount, the
profit/loss rate is reported as `0`.

## Settings

Required:

- `AUTOTRADE_BROKER_ENV`
- `AUTOTRADE_BROKER_API_KEY`
- `AUTOTRADE_BROKER_API_SECRET`
- `AUTOTRADE_BROKER_ACCOUNT`
- `AUTOTRADE_TARGET_SYMBOLS`
- `AUTOTRADE_LOG_DIR`

Optional:

- `AUTOTRADE_PAPER_TRADING_MODE`
- `AUTOTRADE_TELEGRAM_ENABLED`
- `AUTOTRADE_TELEGRAM_BOT_TOKEN`
- `AUTOTRADE_TELEGRAM_CHAT_ID`
- `AUTOTRADE_TELEGRAM_WARNING_CHAT_ID`
- `AUTOTRADE_TELEGRAM_ERROR_CHAT_ID`
- `AUTOTRADE_TELEGRAM_FORCE_IPV4`
- `AUTOTRADE_TELEGRAM_MAX_RETRIES`
- `AUTOTRADE_TELEGRAM_TIMEOUT_SECONDS`
- `AUTOTRADE_TELEGRAM_CONTROL_TIMEOUT_SECONDS`

## Before Open

- Check settings and environment variables.
- At the 08:00 pre-open slot, verify and refresh strategy input bars.
- Check account, orderability, and data freshness.
- Confirm target symbols and strategy parameters.
- Preview symbol-level signals and expected buy/sell quantities from current price.
- Publish the preparation summary to notifier/Telegram.

## Intraday

- Periodically check order/fill state.
- Record exceptions and warnings.
- Stop automatic entries if risk limits are exceeded.
- Normalize limit prices to the KRX tick grid before submission. ETF tick
  handling uses `universe/universe_etf.csv` when available; otherwise the stock
  tick grid is used.
- Control cadence with `SchedulerConfig.intraday_interval`.
- Record each job success/failure and details.
- On a failed job, publish an alert, safe-stop, and resume from the next unrun slot using `scheduler_state.json`.
- While paused, already running jobs finish, but later scheduler jobs do not start.
- On resume, the runner refreshes strategy bars, syncs open orders/fills without submitting new orders, runs missed market-close cleanup once if the pause window crossed the close slot, and skips delayed trading slots scheduled at or before the resume time.

## Market Close

- Summarize orders, fills, holdings, and account performance when broker account performance lookup is available.
- Write reports and next-trading-day checklist items.
- Generate daily run report and alerts after close. Daily run alerts include account profit/loss, profit/loss rate, and holding count when account performance was collected.

## Artifacts

- Run log: phase, scheduled time, success/failure, details.
- Daily report: counts and failures by open/intraday/close phase.
- Account performance: account-level and symbol-level profit/loss summary from the current broker account.
- Alerts: `error` on failure, `warning` if nothing ran, otherwise `info`.
- Telegram: retries `429`, `5xx`, and network errors; splits long messages; sends through a bounded background queue so network retry waits do not block the active operation path.
- Telegram control: when Telegram is enabled, `/pause` and `/resume` commands are accepted only from `AUTOTRADE_TELEGRAM_CHAT_ID`; warning/error chat ids are output-only for v1. Control polling uses `AUTOTRADE_TELEGRAM_CONTROL_TIMEOUT_SECONDS`, separate from alert send timeout.
- Daily inspection: pre-open, intraday, and post-close checks as `passed/failed/pending`.
- Weekly review: weekly summary of daily run and inspection results plus retrospection prompt.
