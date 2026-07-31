#!/usr/bin/env bash
# Pull prod slot-02 trade blotter from CryoTrader VPS.
set -euo pipefail

HOST="${CT_HOST:-root@46.225.137.92}"
REMOTE="${CT_BLOTTER:-/opt/ct/trade_history/slot-02.jsonl}"
OUT="$(cd "$(dirname "$0")/.." && pwd)/data/slot-02.jsonl"

echo "Pulling $HOST:$REMOTE -> $OUT"
scp "$HOST:$REMOTE" "$OUT"
wc -l "$OUT"
