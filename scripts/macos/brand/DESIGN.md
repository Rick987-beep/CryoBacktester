# Cryo product family — visual design language

Shared brand system for **CryoBacktester**, **CryoTrader**, **CryoQuant**, and **CryoExecute**.
Master PNG assets live in this folder (`icons/`). Wire each product’s icon into its
macOS `.app` as `Contents/Resources/AppIcon.icns` + `CFBundleIconFile = AppIcon`.

## Mark (shape)

- **Monogram:** a continuous-stroke **C** that opens into an **options convexity**
  payoff swoosh (call-like upward curve).
- Reads as both “Cryo” and “options gamma / convexity” without chart clutter.
- **Color of the mark:** white / off-white on the coloured field (high contrast).
- No secondary logos, tickers, candlesticks, or numbers in the icon.

## Layout (app icon)

| Zone | Share | Content |
|------|-------|---------|
| **Top** | ~70% | Saturated product colour + white C→convexity mark |
| **Bottom** | ~30% | Near-black band (`#0B1220` family) + white product name |

- Product name only (not “Cryo …”): `Backtester`, `Trader`, `Quant`, `Execute`.
- Title case, centered, geometric sans (SF Pro / Inter / Neue Haas–like).
- Flat vector, Apple-style square master (1024×1024 PNG → `.icns`).

## Product colour palette

Top-field colours — **lighter, saturated** (not near-black):

| Product | Role | Field colour (intent) | Hex guide |
|---------|------|------------------------|-----------|
| **Backtester** | Research / replay | Green | `#22C55E` |
| **Trader** | Live trading | Red | `#EF4444` |
| **Quant** | Analysis / research tools | Blue | `#3B82F6` |
| **Execute** | Execution / routing | Violet | `#8B5CF6` |

Shared chrome:

| Token | Use | Hex guide |
|-------|-----|-----------|
| **Ink / band** | Bottom 30%, wordmark ground | `#0B1220` |
| **Mark / type** | C monogram + product name | `#FFFFFF` |

Hexes are guides — match the locked PNG masters in `icons/` if regenerating.

## Typography

- **Icon wordmark:** geometric sans, regular/medium weight, title case, generous
  tracking only if needed for “Backtester”.
- **In-app UI:** keep existing Research UI / product UI type; do not force the
  icon wordmark font into dense tables.

## Tone

- Clean, quantitative, cold-precise (“cryo”) — not neon, not skeuomorphic.
- Family cohesion = **same mark + same two-zone layout**; differentiation =
  **top-field hue** only.

## Asset inventory

| File | Use |
|------|-----|
| `icons/backtester.png` | Master 1024² for CryoBacktester |
| `icons/trader.png` | Master for future CryoTrader.app |
| `icons/quant.png` | Master for future CryoQuant.app |
| `icons/execute.png` | Master for future CryoExecute.app |
| `icons/backtester.icns` | Built macOS icon (also in the `.app` Resources) |
| `build_app_icon.sh` | Rebuilds `AppIcon.icns` into `CryoBacktester.app` |

Rebuild Backtester Dock icon after editing the PNG:

```bash
# from repo root
./scripts/macos/brand/build_app_icon.sh
# if Dock caches the old glyph: quit the app, then `killall Dock`
```
