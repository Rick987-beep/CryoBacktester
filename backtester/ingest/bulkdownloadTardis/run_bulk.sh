#!/usr/bin/env bash
# run_bulk.sh — Launch 4 parallel bulk_fetch workers in a tmux session.
#
# Each worker covers a ~91-day block, processed newest-first. Workers run
# independently — no cross-worker coordination needed.
#
# A 5th tmux window (Monitor) polls every 60s for all 4 workers to finish,
# then automatically runs fixup_midnight.py. You do not need to babysit it.
#
# Usage:
#   bash run_bulk.sh               # start all 4 workers + monitor
#   bash run_bulk.sh --dry-run     # preview commands without launching
#
# Reattach after detaching (Ctrl-B D):
#   tmux attach -t bulk
#
# Monitor progress:
#   Ctrl-B 0/1/2/3 to switch worker windows
#   Ctrl-B 4       to watch the Monitor + fixup window
#   ls data/options_*.parquet | wc -l    # total days done
#   df -h /                              # disk usage
#
# Requirements:
#   - TARDIS_API_KEY environment variable must be set
#   - tmux installed (apt-get install -y tmux)
#   - .venv/ in the same directory as this script (python bulk_fetch.py)
#
# ── Configuration ─────────────────────────────────────────────────────────────
# Edit these if your date range or block splits differ.

SESSION="bulk"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
LOGS_DIR="${SCRIPT_DIR}/logs"
VENV="${SCRIPT_DIR}/.venv/bin/activate"
MAX_DTE=700

# Worker blocks — newest-first within each block.
# Total: 371 days (2025-04-11 → 2026-04-16), split 4×~93 days.
# All DTE included (MAX_DTE=700). Academic key covers 2025-04-11 to 2026-07-12.
WORKER_A_FROM="2026-01-15"
WORKER_A_TO="2026-04-16"

WORKER_B_FROM="2025-10-14"
WORKER_B_TO="2026-01-14"

WORKER_C_FROM="2025-07-13"
WORKER_C_TO="2025-10-13"

WORKER_D_FROM="2025-04-11"
WORKER_D_TO="2025-07-12"

# ── Preflight checks ──────────────────────────────────────────────────────────

DRY_RUN=0
if [[ "${1}" == "--dry-run" ]]; then
    DRY_RUN=1
fi

if [[ -z "${TARDIS_API_KEY}" ]]; then
    echo "ERROR: TARDIS_API_KEY is not set."
    echo "  export TARDIS_API_KEY=your_key_here"
    exit 1
fi

if ! command -v tmux &>/dev/null; then
    echo "ERROR: tmux not found. Install with: apt-get install -y tmux"
    exit 1
fi

if [[ ! -f "${VENV}" ]]; then
    echo "ERROR: virtualenv not found at ${VENV}"
    echo "  Create it with: python3 -m venv .venv && source .venv/bin/activate && pip install requests pyarrow"
    exit 1
fi

mkdir -p "${DATA_DIR}"
mkdir -p "${LOGS_DIR}"

# ── Build worker commands ─────────────────────────────────────────────────────

_cmd() {
    local worker=$1 from=$2 to=$3
    local logfile="${LOGS_DIR}/worker_${worker}.log"
    echo "source ${VENV} &&" \
        "python ${SCRIPT_DIR}/bulk_fetch.py" \
        "--from ${from} --to ${to}" \
        "--worker ${worker}" \
        "--data-dir ${DATA_DIR}" \
        "--max-dte ${MAX_DTE}" \
        "2>&1 | tee ${logfile};" \
        "echo '=== Worker ${worker} finished ===' | tee -a ${logfile}; bash"
}

CMD_A=$(_cmd A "${WORKER_A_FROM}" "${WORKER_A_TO}")
CMD_B=$(_cmd B "${WORKER_B_FROM}" "${WORKER_B_TO}")
CMD_C=$(_cmd C "${WORKER_C_FROM}" "${WORKER_C_TO}")
CMD_D=$(_cmd D "${WORKER_D_FROM}" "${WORKER_D_TO}")

# ── Dry-run mode ──────────────────────────────────────────────────────────────

if [[ "${DRY_RUN}" -eq 1 ]]; then
    echo "DRY RUN — would launch tmux session '${SESSION}' with 5 windows:"
    echo ""
    echo "  Window 0 (Worker A): ${WORKER_A_TO} → ${WORKER_A_FROM}"
    echo "  Window 1 (Worker B): ${WORKER_B_TO} → ${WORKER_B_FROM}"
    echo "  Window 2 (Worker C): ${WORKER_C_TO} → ${WORKER_C_FROM}"
    echo "  Window 3 (Worker D): ${WORKER_D_TO} → ${WORKER_D_FROM}"
    echo "  Window 4 (Monitor):  polls every 60s → runs fixup_midnight.py when all done"
    echo ""
    echo "  data_dir: ${DATA_DIR}"
    echo "  venv:     ${VENV}"
    echo ""
    echo "Commands:"
    for label in A B C D; do
        eval "echo \"  Worker ${label}: \${CMD_${label}}\""
        echo ""
    done
    exit 0
fi

# ── Launch or attach ──────────────────────────────────────────────────────────

if tmux has-session -t "${SESSION}" 2>/dev/null; then
    echo "tmux session '${SESSION}' already exists — attaching."
    echo "To restart from scratch: tmux kill-session -t ${SESSION} && bash run_bulk.sh"
    tmux attach -t "${SESSION}"
    exit 0
fi

echo "Launching tmux session '${SESSION}' with 4 workers + monitor..."
echo "  Data dir: ${DATA_DIR}"
echo ""

# Create session with Worker A in window 0
tmux new-session  -d -s "${SESSION}" -n "Worker-A" -e "TARDIS_API_KEY=${TARDIS_API_KEY}"
tmux send-keys    -t "${SESSION}:Worker-A" "${CMD_A}" Enter

# Worker B in window 1
tmux new-window   -t "${SESSION}" -n "Worker-B" -e "TARDIS_API_KEY=${TARDIS_API_KEY}"
tmux send-keys    -t "${SESSION}:Worker-B" "${CMD_B}" Enter

# Worker C in window 2
tmux new-window   -t "${SESSION}" -n "Worker-C" -e "TARDIS_API_KEY=${TARDIS_API_KEY}"
tmux send-keys    -t "${SESSION}:Worker-C" "${CMD_C}" Enter

# Worker D in window 3
tmux new-window   -t "${SESSION}" -n "Worker-D" -e "TARDIS_API_KEY=${TARDIS_API_KEY}"
tmux send-keys    -t "${SESSION}:Worker-D" "${CMD_D}" Enter

# Window 4 — Monitor: waits for all 4 workers, then runs fixup_midnight.py
# Polls every 60s. Checks for the completion sentinel written by _cmd().
FIXUP_CMD="source ${VENV} && python ${SCRIPT_DIR}/fixup_midnight.py --data-dir ${DATA_DIR} 2>&1 | tee ${LOGS_DIR}/fixup.log"
MONITOR_CMD="bash -c '
  LOGS=${LOGS_DIR}
  DATA=${DATA_DIR}
  VENV=${VENV}
  SCRIPT=${SCRIPT_DIR}
  FIXUP=${LOGS_DIR}/fixup.log
  echo \"[monitor] Started — polling every 60s for all 4 workers to finish\"
  while true; do
    done=0
    grep -q \"=== Worker A finished ===\" \"\${LOGS}/worker_A.log\" 2>/dev/null && done=\$((done+1))
    grep -q \"=== Worker B finished ===\" \"\${LOGS}/worker_B.log\" 2>/dev/null && done=\$((done+1))
    grep -q \"=== Worker C finished ===\" \"\${LOGS}/worker_C.log\" 2>/dev/null && done=\$((done+1))
    grep -q \"=== Worker D finished ===\" \"\${LOGS}/worker_D.log\" 2>/dev/null && done=\$((done+1))
    parquets=\$(ls \"\${DATA}\"/options_*.parquet 2>/dev/null | wc -l)
    echo \"[monitor] \$(date +%H:%M)  workers done: \${done}/4  |  parquets: \${parquets}\"
    if [[ \${done} -eq 4 ]]; then
      echo \"[monitor] All 4 workers complete — running fixup_midnight.py ...\"
      source \"\${VENV}\" && python \"\${SCRIPT}/fixup_midnight.py\" --data-dir \"\${DATA}\" 2>&1 | tee \"\${FIXUP}\"
      echo \"[monitor] ALL DONE. Fixup complete. Check \${FIXUP} for results.\"
      break
    fi
    sleep 60
  done
  exec bash
'"
tmux new-window   -t "${SESSION}" -n "Monitor"
tmux send-keys    -t "${SESSION}:Monitor" "${MONITOR_CMD}" Enter

# Focus window 0 and attach
tmux select-window -t "${SESSION}:Worker-A"

echo "Session started. Attaching now..."
echo "  Detach:  Ctrl-B D"
echo "  Switch:  Ctrl-B 0/1/2/3 = workers  |  Ctrl-B 4 = monitor"
echo "  Reattach: tmux attach -t ${SESSION}"
echo ""

tmux attach -t "${SESSION}"
