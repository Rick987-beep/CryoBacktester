#!/usr/bin/env bash
# CryoBacktester demo — create venv, install, run blueprint_howto.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if ! command -v python3.12 >/dev/null 2>&1; then
  echo "Python 3.12 is required. Install it, then re-run." >&2
  exit 1
fi

if [[ ! -d .venv ]]; then
  echo "Creating .venv …"
  python3.12 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate

echo "Installing dependencies …"
pip install -q -r requirements.txt

echo "Running blueprint_howto …"
python -m backtester.run --strategy blueprint_howto

echo
echo "Done. HTML reports (if any) are under data/runs/."
echo "Optional UI: python -m backtester.ui.desktop"
