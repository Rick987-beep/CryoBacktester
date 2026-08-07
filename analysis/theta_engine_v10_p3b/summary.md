# Phase 3b — breach-gated sticky wings (naked Mode C)

## Goal
Long OTM wings only on D/G (or D/G/V) breach; Axis A expiry×δ; episode hold (no bar flip); theta ignored for wing decisions.

DATE_RANGE=('2025-04-11', '2026-08-01')  capital=100,000

Grid: 2 controls + 24 sticky cells (2 expiry × 3 δ × 2 trigger × 2 entries).

## Naked controls
- **RichForce16**: ann=53.7%  maxDD=6.7%  D/G/V breach=79.7/0.0/56.3%  PnL=$75,231
- **Daily15**: ann=84.1%  maxDD=13.4%  D/G/V breach=88.4/0.1/89.8%  PnL=$121,491

**DG breach improved (best sticky vs control): True**
**Any sticky cell near 20%/5% DD: True**

## Champion (lowest DG breach vs control, then return)
- **RichForce16** expiry=same δ=0.15 trigger=dg: ann=46.7% DD=5.4% Δ(D+G)breach=-4.7pp opens=86

## Top 5 by DG improvement

- RichForce16 same/δ0.15/dg: ΔDG=-4.7pp ann=46.7% opens=86
- RichForce16 same/δ0.15/dgv: ΔDG=-4.4pp ann=49.4% opens=59
- Daily15 same/δ0.15/dg: ΔDG=-3.7pp ann=79.8% opens=145
- RichForce16 next_listed/δ0.15/dgv: ΔDG=-3.6pp ann=50.3% opens=91
- RichForce16 next_listed/δ0.1/dgv: ΔDG=-2.9pp ann=50.8% opens=100

## Next
Read scorecard; if DG still stuck, combine with P2 perp on champion size.
