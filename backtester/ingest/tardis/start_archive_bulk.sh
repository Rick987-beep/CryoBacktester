#!/usr/bin/env bash
# start_archive_bulk.sh — Launch Deribit archive in tmux on apps server.
set -euo pipefail

cd "$(dirname "$0")"
set -a
source .env
set +a
source .venv/bin/activate

: "${STORAGE_BOX_USER:?Set STORAGE_BOX_USER in .env}"
: "${STORAGE_BOX_HOST:?Set STORAGE_BOX_HOST in .env}"
: "${TARDIS_API_KEY:?Set TARDIS_API_KEY in .env}"
STORAGE_BOX_BASE="${STORAGE_BOX_BASE:-tardis_raw}"

UPLOAD_BASE="${STORAGE_BOX_USER}@${STORAGE_BOX_HOST}:${STORAGE_BOX_BASE}/deribit/"
SSH_PORT_ARGS=()
if [[ -n "${STORAGE_BOX_PORT:-}" ]]; then
  SSH_PORT_ARGS=(--ssh-port "${STORAGE_BOX_PORT}")
fi
SSH_KEY_ARGS=()
if [[ -n "${STORAGE_BOX_SSH_KEY:-}" ]]; then
  SSH_KEY_ARGS=(--ssh-key "${STORAGE_BOX_SSH_KEY}")
fi

mkdir -p logs staging

if tmux has-session -t tardis-archive 2>/dev/null; then
  echo "tmux session 'tardis-archive' already exists. Attach with: tmux attach -t tardis-archive"
  exit 1
fi

# Inner shell must export .env vars and run unbuffered for live logs in tmux.
tmux new-session -d -s tardis-archive \
  "cd '$(pwd)' && set -a && source .env && set +a && \
   source .venv/bin/activate && export PYTHONUNBUFFERED=1 && \
   python archive_fetch.py \
     --exchange deribit \
     --from 2025-04-11 --to 2026-07-12 \
     --upload-base '${UPLOAD_BASE}' \
     ${SSH_PORT_ARGS[*]:-} ${SSH_KEY_ARGS[*]:-} \
     --day-retries 3 \
     2>&1 | tee -a logs/archive.log; \
   echo exit=\$? >> logs/archive.log"

echo "Started Deribit archive in tmux session 'tardis-archive'"
echo "Attach: tmux attach -t tardis-archive"
echo "Log:    tail -f $(pwd)/logs/archive.log"
