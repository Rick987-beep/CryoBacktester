"""
extract_regime_from_image.py

Reads whitelist_coverage_timeline_oct2024.png pixel-by-pixel and extracts
a daily regime classification (BULL / BEAR / NEITHER / BOTH) for every UTC
calendar date in the image's date range.

Color mapping (confirmed by operator):
  Green  RGB≈(20, 128, 74)  → BULL
  Crimson RGB≈(139, 41, 66) → BEAR
  Dark brown RGB≈(61, 43, 43) → NEITHER
  Brownish yellow             → BOTH  (not observed in this image)

Output: daily_regime.csv  (date_utc, regime)

Usage:
  python backtester/newstrategy/coincall_signal_schedule_bull/extract_regime_from_image.py
"""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

import numpy as np
from PIL import Image

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HERE = Path(__file__).parent
IMG_PATH = HERE / "whitelist_coverage_timeline_oct2024.png"
OUT_PATH = HERE / "daily_regime.csv"

# Date range (inclusive) — confirmed from image axis labels
START_DATE = date(2024, 10, 1)
END_DATE   = date(2026, 4, 24)

# Chart pixel boundaries (determined by pixel probe)
CHART_X_START = 53    # first colored column
CHART_X_END   = 1897  # last colored column  (inclusive)
CHART_Y_MID   = 87    # middle of the colored band (y=36..138)

# Reference colors (RGB) for each regime
COLORS = {
    "BULL":    np.array([20,  128, 74]),
    "BEAR":    np.array([139,  41, 66]),
    "NEITHER": np.array([61,   43, 43]),
    "BOTH":    np.array([200, 160, 60]),  # brownish yellow — not in this image
}

# Maximum RGB Euclidean distance to accept a color match.
# If the nearest color is further than this, label as NEITHER (conservative).
MAX_MATCH_DIST = 40

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def nearest_regime(pixel: np.ndarray) -> str:
    best_name, best_dist = "NEITHER", float("inf")
    for name, ref in COLORS.items():
        dist = float(np.linalg.norm(pixel.astype(float) - ref.astype(float)))
        if dist < best_dist:
            best_dist = dist
            best_name = name
    if best_dist > MAX_MATCH_DIST:
        return "NEITHER"
    return best_name


def x_to_date(x: int, total_days: int, chart_width: int) -> date:
    """Map pixel column to calendar date using linear interpolation."""
    frac = (x - CHART_X_START) / (chart_width - 1)
    day_offset = round(frac * total_days)
    return START_DATE + timedelta(days=day_offset)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    img = Image.open(IMG_PATH).convert("RGB")
    arr = np.array(img)

    total_days = (END_DATE - START_DATE).days   # 570
    chart_width = CHART_X_END - CHART_X_START   # 1844

    print(f"Image shape: {arr.shape}")
    print(f"Date range: {START_DATE} → {END_DATE} ({total_days} days)")
    print(f"Chart x: {CHART_X_START}–{CHART_X_END}  ({chart_width} px)")
    print(f"Chart y sample row: {CHART_Y_MID}")
    print()

    # --- Pass 1: for each date, collect all x-pixels that map to it ----------
    # Build a dict: date → list of regime votes (one per x pixel)
    from collections import defaultdict
    votes: dict[date, list[str]] = defaultdict(list)

    # Sample 3 y rows in the chart band to reduce noise at band edges
    sample_ys = [60, 87, 115]

    for x in range(CHART_X_START, CHART_X_END + 1):
        for y in sample_ys:
            pixel = arr[y, x, :]
            regime = nearest_regime(pixel)
            d = x_to_date(x, total_days, chart_width)
            votes[d].append(regime)

    # --- Pass 2: majority vote per date, fill gaps --------------------------
    all_dates = [START_DATE + timedelta(days=i) for i in range(total_days + 1)]
    rows: list[tuple[date, str]] = []

    for d in all_dates:
        if d in votes:
            day_votes = votes[d]
            # Count, ignoring white/background (should not appear inside chart)
            from collections import Counter
            c = Counter(day_votes)
            # If both BULL and BEAR appear with meaningful weight → BOTH.
            # In practice this only occurs at color-boundary pixels (anti-aliasing
            # artefacts). Since BOTH is not used in this image, snap to whichever
            # of BULL/BEAR has more votes.
            bull_frac = c.get("BULL", 0) / len(day_votes)
            bear_frac = c.get("BEAR", 0) / len(day_votes)
            if bull_frac >= 0.25 and bear_frac >= 0.25:
                regime = "BULL" if bull_frac >= bear_frac else "BEAR"
            else:
                regime = c.most_common(1)[0][0]
        else:
            regime = "NEITHER"

        rows.append((d, regime))
        print(f"  {d}  {regime}")

    # --- Pass 3: hard-override dates known from signal schedule CSVs ---------
    # The signal entry dates are ground truth: a trade was opened that day,
    # therefore the sleeve was definitely armed. Override any misclassification.
    import csv as _csv
    known_overrides: dict[date, str] = {}
    for fname, regime_label in [
        ("coincall_signal_schedule_bull.csv", "BULL"),
        ("coincall_signal_schedule_bear.csv", "BEAR"),
    ]:
        with open(HERE / fname) as f:
            for row2 in _csv.DictReader(f):
                d2 = date.fromisoformat(row2["schedule_entry_bar_utc"][:10])
                known_overrides[d2] = regime_label

    rows = [
        (d, known_overrides.get(d, regime))
        for d, regime in rows
    ]

    # --- Write CSV ----------------------------------------------------------
    with open(OUT_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date_utc", "regime"])
        for d, regime in rows:
            writer.writerow([d.isoformat(), regime])

    print()
    print(f"Written {len(rows)} rows → {OUT_PATH}")

    # Summary
    from collections import Counter
    summary = Counter(r for _, r in rows)
    print("Regime counts:")
    for k, v in sorted(summary.items()):
        print(f"  {k}: {v} days")


if __name__ == "__main__":
    main()
