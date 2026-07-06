#!/usr/bin/env bash
# provision_storage_box.sh — Verify Storage Box rsync from apps server.
#
# Prerequisites (Hetzner console):
#   1. Create BX21 (5 TB) Storage Box in Nuremberg — sufficient for Deribit ~2.9 TB
#   2. Enable SSH / external access
#   3. Note username (uXXXX) and host (uXXXX.your-storagebox.de)
#
# Usage on apps server:
#   cd /apps/tardis-archive && source .env
#   bash provision_storage_box.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${SCRIPT_DIR}/.env"
  set +a
fi

: "${STORAGE_BOX_USER:?Set STORAGE_BOX_USER in .env}"
: "${STORAGE_BOX_HOST:?Set STORAGE_BOX_HOST in .env}"
STORAGE_BOX_BASE="${STORAGE_BOX_BASE:-tardis_raw}"
STORAGE_BOX_PORT="${STORAGE_BOX_PORT:-23}"

SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=accept-new -p "${STORAGE_BOX_PORT}")
if [[ -n "${STORAGE_BOX_SSH_KEY:-}" ]]; then
  SSH_OPTS+=(-i "${STORAGE_BOX_SSH_KEY}")
fi

REMOTE="${STORAGE_BOX_USER}@${STORAGE_BOX_HOST}"
TEST_FILE="/tmp/tardis_rsync_test_$$"

echo "=== Storage Box rsync test ==="
echo "Remote: ${REMOTE}:${STORAGE_BOX_BASE}/"
echo "SSH port: ${STORAGE_BOX_PORT}"

echo "provision test $(date -u)" > "${TEST_FILE}"
rsync -av -e "ssh ${SSH_OPTS[*]}" "${TEST_FILE}" "${REMOTE}:${STORAGE_BOX_BASE}/"
ssh "${SSH_OPTS[@]}" "${REMOTE}" "ls -la ${STORAGE_BOX_BASE}/ && du -sh ${STORAGE_BOX_BASE} 2>/dev/null || true"
rm -f "${TEST_FILE}"

echo "=== OK: rsync to Storage Box works ==="
