#!/usr/bin/env bash
# start_archive_catchup.sh — Launch a short Deribit archive catch-up in tmux.
#
# Usage:
#   bash start_archive_catchup.sh [FROM_DATE] [TO_DATE]
# Default: 2026-07-02 2026-07-03
#
# Uses the same archive_fetch.py pipeline as start_archive_bulk.sh (manifest
# resume, retries, rsync to Storage Box). Skips days already in manifest.
set -euo pipefail

FROM_DATE="${1:-2026-07-02}"
TO_DATE="${2:-2026-07-03}"
SESSION="tardis-archive-catchup"
LOG="logs/archive_catchup.log"

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

if tmux has-session -t "${SESSION}" 2>/dev/null; then
  echo "tmux session '${SESSION}' already exists. Attach with: tmux attach -t ${SESSION}"
  exit 1
fi

tmux new-session -d -s "${SESSION}" \
  "cd '$(pwd)' && set -a && source .env && set +a && \
   source .venv/bin/activate && export PYTHONUNBUFFERED=1 && \
   python archive_fetch.py \
     --exchange deribit \
     --from '${FROM_DATE}' --to '${TO_DATE}' \
     --upload-base '${UPLOAD_BASE}' \
     ${SSH_PORT_ARGS[*]:-} ${SSH_KEY_ARGS[*]:-} \
     --day-retries 3 \
     2>&1 | tee -a '${LOG}'; \
   echo exit=\$? >> '${LOG}'"

echo "Started Deribit catch-up (${FROM_DATE} → ${TO_DATE}) in tmux session '${SESSION}'"
echo "Attach: tmux attach -t ${SESSION}"
echo "Log:    tail -f $(pwd)/${LOG}"
