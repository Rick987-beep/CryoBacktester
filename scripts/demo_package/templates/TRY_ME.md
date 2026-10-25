# Try CryoBacktester (no git required)

This zip is a **demo**: the backtester engine, one reference strategy (`blueprint_howto`),
and about three months of BTC options sample data.

## Prerequisites

- macOS or Linux
- **Python 3.12** (`python3.12 --version`)

Windows: install Python 3.12, then follow the manual steps in `README.md`.

## Steps

1. Unzip this archive.
2. Open a terminal in the unzipped folder.
3. Run:

```bash
chmod +x run_demo.sh
./run_demo.sh
```

That creates a local `.venv`, installs dependencies, and runs the blueprint
strategy. When it finishes, open the HTML report path printed at the end
(under `data/runs/`).

## Download

Latest package:

**https://apps.aureas.xyz/backtester-demo/CryoBacktester-demo-latest.zip**

Developers who prefer git: see `README.md`.
