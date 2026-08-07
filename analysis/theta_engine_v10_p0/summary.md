# Phase 0 — scale vs overlap diagnose

## Goal
Decide whether naked Mode C greek breaches are mostly per-trade size or stacked overlap.

DATE_RANGE=('2025-04-11', '2026-08-01')  capital=100,000

## Scorecard (greek_limits_mode=off)

- **RichForce16**: ann=53.7%  maxDD=6.7%  PnL=$75,231  Vbreach=56.3%  Dbreach=79.7%
- **Daily15**: ann=84.1%  maxDD=13.4%  PnL=$121,491  Vbreach=89.8%  Dbreach=88.4%

## Decision for Phase 1 Pareto read-order
Emphasize first: **max_concurrent (breaches concentrate at high n_open)**

P1 still sweeps both `qty_per_1btc_equity` and `max_concurrent`.

## Next
Phase 1: `greek_limits_mode=scale` + qty × concurrent grid.
