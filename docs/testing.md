# Testing

## Direction

- Unit tests: strategy contracts, risk limits, order state transitions.
- Replay tests: historical price and order-flow reproduction.
- Paper trading: integration behavior before live orders.
- Backtests: long-term strategy tendency and drawdown resilience.

## Priority

Start with small pure-logic tests, then expand into data and broker integration.

## Paper Trading Manual Check

Before live trading, run this minimum paper-account check:

1. Set environment variables; verify paper account, app key, and log path.
2. Run read-only smoke for quote, balance, and orderable quantity. If a recent
   order id is available, add `--order-history-order-id <order-id>` to also
   verify order history parsing.
3. Run `PYTHONPATH=src python -m autotrade.cli account-performance`; verify
   account-level profit/loss, profit/loss rate, cash, and holding details are
   printed without submitting orders.
4. Submit one tiny limit order; verify modify and cancel are accepted.
5. Submit one aggressive order to induce fill; verify order state and holdings change.
6. Cross-check stdout, manual check log, account performance output, and raw KIS log.

### Pass Criteria

- `manual_paper_check_*.log` includes order submit, modify, cancel, and final balance lookup.
- Raw KIS log at `AUTOTRADE_LOG_DIR/kis_raw_YYYYMMDD.log` confirms `order-cash`, `order-rvsecncl`, and `inquire-balance`.
- `account-performance` output matches the KIS balance response totals and
  includes per-symbol holding profit/loss rates.
- `backtest --symbol <code>` can replay stored bars and writes report, trades,
  and equity CSV artifacts without broker writes.
- KIS paper `inquire-daily-ccld` may omit per-order `output1`; confirm fills with holdings increase and cancel-impossible response.
- After an aggressive order, `40330000 - 모의투자 정정/취소할 수량이 없습니다.` plus increased holdings means the paper environment treated it as fully filled.
- Unit broker contract tests include recorded KIS fixtures for quote, holdings,
  order capacity, and order history endpoint/TR regression detection.
