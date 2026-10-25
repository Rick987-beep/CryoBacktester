# CryoBacktester Demo

Public demo of the **CryoBacktester** BTC options backtester: engine + UI + one
reference strategy (`blueprint_howto`) + ~3 months of Deribit sample market data.

This is **not** a live trading system. It does not include private research strategies.

| Channel | Link |
|---------|------|
| GitHub | https://github.com/Rick987-beep/CryoBacktester-demo |
| Zip (non-devs) | https://apps.aureas.xyz/backtester-demo/CryoBacktester-demo-latest.zip |

## Quick start (developers)

```bash
git clone https://github.com/Rick987-beep/CryoBacktester-demo.git
cd CryoBacktester-demo
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m backtester.run --strategy blueprint_howto
python -m pytest tests/ -v
```

Or use `./run_demo.sh` after cloning / unzipping.

Optional research UI (native window):

```bash
python -m backtester.ui.desktop
```

## What’s included

- `backtester/` — replay engine, grid runner, reporting, jobd, research UI
- `backtester/strategies/blueprint_howto.py` — annotated reference strategy
- `data/market/` — options + spot parquet snapshots for the demo window
- `docs/strategy_howto.md` — how to write a strategy against this engine
- `tests/` — product tests (no private strategy suite)

## What’s not included

- Private strategy books / research workspace
- Full multi-year market history
- Exchange API credentials or live execution

## License

See [LICENSE](LICENSE) — evaluation / educational use only.
