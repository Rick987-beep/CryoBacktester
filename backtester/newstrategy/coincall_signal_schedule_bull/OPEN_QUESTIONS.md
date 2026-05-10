# Open Questions — Long-Gamma Bull/Bear Strategy Spec

**Date:** 8 May 2026
**Re:** Signal methodology package `coincall_signal_schedule_bull/`

This document identifies three points where the delivered specification is
either ambiguous or internally inconsistent when cross-checked against the
reference signal schedules and trade ledger. Each item is stated precisely,
with supporting evidence, and ends with a concrete question that needs a
definitive answer before the strategy can be implemented without ambiguity.

---

## 1. Concurrency rule contradicts the reference signal data

### What the spec says

The pseudocode in the Executive Summary describes a **mutex chain**: at any
given moment only one entry can be open per sleeve. An entry event is only
registered if no prior entry is pending.

```
if pending is None and entry_fire:
    pending = t          # only arms if slot is free
elif pending is not None and exit_fire:
    emit pair; clear pending
```

Under this rule, two entries can never share the same exit timestamp —
there is always at most one open position.

### What the reference data shows

`coincall_signal_schedule_bear.csv` contains multiple pairs that **open while
a prior pair is still live**, all converging on the same exit bar. Examples:

| Pairs | Entry bars | Shared exit bar |
|---|---|---|
| 1, 2, 3 | Dec 24 16:00, Dec 25 00:00, Dec 25 16:00 | Dec 26 12:00 |
| 8, 9 | Feb 14 20:00, Feb 15 04:00 | Feb 17 00:00 |
| 12, 13, 14 | Mar 14 16:00, Mar 15 04:00, Mar 15 16:00 | Mar 16 12:00 |
| 20, 21 | Aug 22 16:00, Aug 23 08:00 | Aug 24 20:00 |
| 23, 24 | Sep 02 04:00, Sep 02 16:00 | Sep 04 16:00 |
| 28, 29 | Jan 28 00:00, Jan 28 12:00 | Jan 29 04:00 |
| 32, 33, 34 | Feb 25 16:00, Feb 26 20:00, Feb 27 04:00 | Feb 27 20:00 |
| 35, 36 | Mar 23 20:00, Mar 24 08:00 | Mar 24 20:00 |
| 37, 38 | Mar 25 08:00, Mar 26 00:00 | Mar 26 12:00 |

This pattern is systematic and consistent, accounting for roughly a third of
all bear trades. The mutex pseudocode cannot produce this output.

### What the actual pairing rule appears to be

Each RSI entry event is independently paired with the **next RSI exit event
that occurs at or after it**, regardless of how many other entry events are
also pending. Result: multiple positions can accumulate before the next exit
fires, and all of them close at that exit bar.

### Questions

**1a.** Is the above interpretation correct — each entry event independently
pairs with the next exit event, with no limit on how many can be open
simultaneously?

**1b.** Is there a maximum concurrent open position limit per sleeve (e.g. 3),
or is it uncapped?

**1c.** Does the same fan-out rule apply to the bull sleeve (EMA crossover
pairs), or is the bull sleeve strictly sequential? (In practice EMA crossings
alternate up/down so fan-out cannot occur mechanically, but the rule should be
stated explicitly.)

---

## 2. Close timing when option expires before the exit signal fires

### What the spec says

"Close at bar close of `exit_ts` (the trigger reversal), **or** at expiry,
whichever comes first." This is mentioned in the spec body but listed as an
open question.

### What the reference data confirms

The `fill_exit_utc` column in `trades_combined_bullbear_chrono_mai26.csv`
differs from `signal_exit_utc` in two bull trades:

| Trade | signal_exit_utc | fill_exit_utc | exit_kind |
|---|---|---|---|
| bull-1 | 2024-11-25 20:00 | 2024-11-22 08:00 | `expiry` |
| bull-2 | 2025-05-04 12:00 | 2025-05-02 08:00 | `expiry` |

The option expired several days before the EMA exit signal fired. The actual
close occurred at expiry. All other trades have `fill_exit_utc = signal_exit_utc`.

The `min(option_expiry_ts, signal_exit_ts)` rule is therefore confirmed by the
data for the expiry path. However, two details remain unspecified:

### Questions

**2a.** When an option expires before the trigger exit fires, does the chain
re-arm immediately (the pending entry slot is cleared at expiry), or does the
chain wait for the trigger exit event before it can accept a new entry?
This matters if a new RSI/EMA entry event occurs in the window between expiry
and the trigger exit.

**2b.** The two expiry trades are both bull (long call, ~11 DTE). Given the
~21 DTE horizon of the bear sleeve, expiry-before-trigger is less likely there
but remains theoretically possible. Confirm the `min(expiry, trigger)` rule
applies uniformly to both sleeves.

---

## 3. Delta selection tolerance

### What the spec says

Target delta is **+0.70** for calls (bull) and **−0.60** for puts (bear).
No acceptable range or fallback is specified.

### The problem

Option chains are discrete. On any given entry bar the nearest available
strike may have a delta materially different from the target. The spec gives
no rule for:

- The maximum delta deviation from target that is still acceptable.
- What to do when no option falls within tolerance (skip the trade? use the
  nearest available regardless?).

### Questions

**3a.** What is the acceptable delta tolerance window around the target?
(e.g., ±0.05, ±0.10, ±0.15 — or simply "always pick the nearest, no skip"?)

**3b.** If no option is within tolerance, should the trade be skipped entirely,
or should the nearest available option be used with a warning logged?

---

## Summary checklist for the author

| # | Topic | Blocking? | Questions |
|---|---|---|---|
| 1a | Concurrency rule — fan-out semantics | **Yes** | Is each entry independently paired with next exit? |
| 1b | Max concurrent positions per sleeve | **Yes** | Is there a cap? |
| 1c | Bull sleeve concurrency rule | **Yes** | Confirm sequential or same fan-out rule |
| 2a | Chain re-arm after expiry | **Yes** | Clear pending slot at expiry, or wait for trigger? |
| 2b | `min(expiry, trigger)` applies to both sleeves | No (assumed yes) | Confirm |
| 3a | Delta tolerance window | **Yes** | Specify acceptable range |
| 3b | Behaviour when no option in tolerance | **Yes** | Skip or use nearest? |

All items marked **Yes** in the Blocking column must be resolved before an
unambiguous implementation can be built.
