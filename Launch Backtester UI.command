#!/bin/zsh
# Launch Backtester UI — double-click to start
cd "$(dirname "$0")"
source .venv/bin/activate
open http://localhost:5007
python -m backtester.ui.app --port 5007
