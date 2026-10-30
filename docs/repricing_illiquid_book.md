# Repricing: illiquid / absurd book quotes

**Status:** shipped 2026-10-30  
**Code:** `backtester/core/strategy_base.py` → `price_legs` / `_reprice_legs`  
**Config:** `backtester/core/config.toml` → `[repricing]`  
**Knob:** `book_vs_mark_min_frac` (default `0.25`; `0.0` = legacy)

---

## Why this exists

Open-position MTM (NAV / max DD) and most executable exits call
`price_legs(..., mode="executable")`:

| Leg side | Book used | Intent |
|----------|-----------|--------|
| Short (sell) | **ask** (buy-to-close) | Conservative close cost |
| Long (buy) | **bid** (sell-to-close) | Conservative proceeds |

When the book is empty, we already fell back to **mark ± slip**. The bug was
that a **non-zero but absurd** bid skipped that fallback.

That matters most for **defined-risk multi-leg** books (credit verticals,
winged shorts): the short can mark near full intrinsic while the wing is
marked near zero → NAV drawdown far beyond width × qty.

Mark-mode SL (`price_mode="mark"`) is **unchanged** by this patch.

---

## Guideline

The backtester should simulate **what you could actually trade**:

1. **Exits:** never assume better liquidity than a *credible* book shows
   (prefer conservative: harder / more expensive to exit).
2. **Do not invent disasters** from junk quotes (stale micro-bids, etc.).
3. A missing **row** for any leg still returns `None` for the whole package
   (NAV carry-forward) — we never mark the short alone while dropping the wing.

---

## Before / after (real case)

**Source:** run 810 combo `32d58bb79da6`, trade Aug 17 → Sep 1 2026  
**Structure:** short `BTC-4SEP26-66000-C` / long `BTC-4SEP26-70000-C`, qty **1.6**  
**Width:** $4 000 × 1.6 = **$6 400** structural max loss (credit ~$694 → ~$5.7k net)  
**Snapshot:** `data/market/options_2026-08-25.parquet` @ **2026-08-25 09:15 UTC**  
**Spot:** ~79 822

| Leg | bid | ask | mark |
|-----|-----|-----|------|
| Short 66000C | — | **0** | 0.1746 |
| Long 70000C | **0.01** | — | **0.1253** |

Config used: `slip_pct_zero_price = 0.25`, `min_mark_usd = 15`.

### BEFORE (`book_vs_mark_min_frac` effectively ignored — only `bid == 0`)

| Leg | Rule fired | BTC used | USD (× spot × 1.6) |
|-----|------------|----------|---------------------|
| Short | `ask == 0` → mark × 1.25 | 0.21825 | **~$27 874** liability |
| Long | `bid == 0.01` → **trust bid** | 0.01 | **~$1 277** asset |
| **Net mark-to-close** | | | **~$26 597** debit |
| **Unrealized PnL** | | | **~−$25 945** |
| **DD vs ~$101.5k peak** | | | **~−25.6%** |

Same bar **intrinsic / mark package** ≈ **−$5.6k to −$6k** (matches width).  
Bundle `nav_low` that day: **~75.5k** — matches the broken executable path.

Closed trade later flattened at **~−$6 135** (wing did its job on realized PnL).

### AFTER (`book_vs_mark_min_frac = 0.25`)

Wing: `bid / mark = 0.01 / 0.1253 ≈ 0.08` **&lt; 0.25** → treat bid as unusable  
→ bid fallback `mark × (1 − 0.25) = 0.093975`

| Leg | Rule fired | BTC used | USD (× spot × 1.6) |
|-----|------------|----------|---------------------|
| Short | unchanged (`ask == 0` → mark×1.25) | 0.21825 | **~$27 874** |
| Long | absurd bid → mark×0.75 | 0.093975 | **~$12 002** |
| **Net mark-to-close** | | | **~$15 872** |
| **Unrealized (order of magnitude)** | | | **~−$15k** (still conservative vs width because short ask slip remains) |

Notes:

- After-fix MTM is **still not** clamped to width; short zero-ask still pays
  **+25% slip** on mark. That is intentional conservatism on the short side.
- The **fantasy −$26k / −26% DD** from understating the wing is removed.
- Fair mark-mark vertical that bar remains ~−$5.6k; EOD/`nav_close` was
  already near that.

---

## Config knobs

| Key | Default | Role |
|-----|---------|------|
| `min_mark_usd` | `15` | Mark must be large enough for ±slip fallback |
| `slip_pct_zero_price` | `0.25` | ask←mark×(1+s), bid←mark×(1−s) |
| `book_vs_mark_min_frac` | `0.25` | bid &lt; mark×frac ⇒ unusable; `0` = legacy |

---

## Blast radius

Shared by **all** strategies via NAV executable reprice and any
`price_mode="executable"` / `"bid"` exit.

| Path | Effect |
|------|--------|
| `mode="mark"` (typical SL) | **None** |
| NAV / max DD | Less false panic when long bids are junk |
| Executable TP | Does **not** get easier (junk bid made packages look *worse*) |
| Old bundles | Unchanged on disk; **re-runs** can differ |

Research tip: before/after a locked Starnberg (or similar winged) cell if
comparing to pre-2026-10-30 artefacts.

---

## Tests

`tests/test_price_legs_book_sanity.py` — Aug-25-shaped quotes + frac=0 legacy.
