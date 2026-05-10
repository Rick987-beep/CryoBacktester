#!/usr/bin/env bash
# retry_missing.sh — One-time fix for a specific Tardis availability event.
#
# During the original bulk download, these 20 specific dates returned empty gzip
# files from Tardis (HTTP 200, ~20 bytes, no S3 redirect). This was a transient
# server-side issue — Tardis had not yet populated those dates' files. This script
# was written to check back later and re-download only those dates once available.
#
# This is NOT a general-purpose retry utility. The 20 dates are hardcoded. If you
# need to re-run a different set of dates, use bulk_fetch.py directly with --date.
#
# Strategy: probe each date with a cheap curl check first (takes ~1s).
#   - HTTP 302 → Wasabi S3 = data is now available → run full bulk_fetch download
#   - HTTP 200 + <=20 bytes = still missing on Tardis → log and skip immediately
#
# This avoids burning 4×retry cycles (~25s) on dates still missing at Tardis.
#
# Usage (on the Hetzner server):
#   cd /root/tardis
#   source .env && export TARDIS_API_KEY
#   bash retry_missing.sh
#
# Or with a custom data dir:
#   DATA_DIR=/root/tardis/data bash retry_missing.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${DATA_DIR:-${SCRIPT_DIR}/data}"
LOGS_DIR="${SCRIPT_DIR}/logs"
VENV="${SCRIPT_DIR}/.venv/bin/activate"
MAX_DTE=700
TARDIS_BASE="https://datasets.tardis.dev/v1/deribit/options_chain"

MISSING_DATES=(
    2025-05-12
    2025-05-19
    2025-08-05
    2025-08-16
    2025-08-17
    2025-08-19
    2025-08-25
    2025-08-26
    2025-09-23
    2025-10-13
    2025-10-14
    2025-10-16
    2025-10-17
    2025-10-22
    2025-10-24
    2025-10-30
    2026-01-30
    2026-02-02
    2026-02-03
    2026-03-03
)

# Preflight
if [[ -z "${TARDIS_API_KEY:-}" ]]; then
    echo "ERROR: TARDIS_API_KEY is not set. Run: source .env && export TARDIS_API_KEY"
    exit 1
fi
if [[ ! -f "${VENV}" ]]; then
    echo "ERROR: virtualenv not found at ${VENV}"
    exit 1
fi

source "${VENV}"
mkdir -p "${LOGS_DIR}"

LOGFILE="${LOGS_DIR}/retry_missing.log"
echo "=== retry_missing.sh started $(date) ===" | tee "${LOGFILE}"
echo "Dates to check: ${#MISSING_DATES[@]}" | tee -a "${LOGFILE}"
echo "" | tee -a "${LOGFILE}"

SUCCEEDED=()
STILL_MISSING=()
FAILED=()

# probe_date DATE
# Checks Tardis for the date without downloading.
# Returns:
#   0 — HTTP 302 redirect found, real data available
#   1 — HTTP 200 with <=20 bytes, still empty placeholder
#   2 — unexpected response
probe_date() {
    local date="$1"
    local year="${date:0:4}" month="${date:5:2}" day="${date:8:2}"
    local url="${TARDIS_BASE}/${year}/${month}/${day}/OPTIONS.csv.gz"

    read -r http_code size <<< "$(curl -sS -o /dev/null \
        -w "%{http_code} %{size_download}" \
        -H "Authorization: Bearer ${TARDIS_API_KEY}" \
        --max-time 15 \
        "${url}")"

    echo "[probe]  ${date} → HTTP ${http_code}, ${size} bytes received" | tee -a "${LOGFILE}"

    if [[ "${http_code}" == "200" ]] && [[ "${size}" -gt 1000 ]]; then
        return 0
    elif [[ "${http_code}" == "200" ]] && [[ "${size}" -le 20 ]]; then
        return 1
    elif [[ "${http_code}" == "302" ]]; then
        return 0
    else
        echo "[probe]  ${date} → unexpected response (HTTP ${http_code}, ${size} bytes)" | tee -a "${LOGFILE}"
        return 2
    fi
}

for DATE in "${MISSING_DATES[@]}"; do
    echo "─── [$(date +%H:%M:%S)] ${DATE} ───" | tee -a "${LOGFILE}"

    probe_result=0
    probe_date "${DATE}" || probe_result=$?

    if [[ "${probe_result}" -eq 1 ]]; then
        echo "[STILL MISSING] ${DATE} — Tardis still returning empty gzip, skipping" | tee -a "${LOGFILE}"
        STILL_MISSING+=("${DATE}")
        echo "" | tee -a "${LOGFILE}"
        continue
    elif [[ "${probe_result}" -eq 2 ]]; then
        echo "[ERROR] ${DATE} — unexpected probe response, skipping" | tee -a "${LOGFILE}"
        FAILED+=("${DATE}")
        echo "" | tee -a "${LOGFILE}"
        continue
    fi

    # Data is available — run full download
    echo "[DOWNLOAD] ${DATE} — data available on Tardis, starting bulk_fetch..." | tee -a "${LOGFILE}"

    dl_result=0
    python "${SCRIPT_DIR}/bulk_fetch.py" \
        --from "${DATE}" --to "${DATE}" \
        --worker RETRY \
        --data-dir "${DATA_DIR}" \
        --max-dte "${MAX_DTE}" \
        --force \
        2>&1 | tee -a "${LOGFILE}" || dl_result=$?

    PARQUET="${DATA_DIR}/options_${DATE}.parquet"
    if [[ "${dl_result}" -eq 0 ]] && [[ -f "${PARQUET}" ]] && \
       [[ $(stat -c%s "${PARQUET}" 2>/dev/null || stat -f%z "${PARQUET}") -gt 1024 ]]; then
        echo "[OK] ${DATE} — parquet written ($(du -sh "${PARQUET}" | cut -f1))" | tee -a "${LOGFILE}"
        SUCCEEDED+=("${DATE}")
    else
        echo "[FAIL] ${DATE} — download failed or parquet missing/empty (exit=${dl_result})" | tee -a "${LOGFILE}"
        FAILED+=("${DATE}")
    fi

    echo "" | tee -a "${LOGFILE}"
done

echo "================================================================" | tee -a "${LOGFILE}"
echo "=== Summary $(date) ===" | tee -a "${LOGFILE}"
echo "================================================================" | tee -a "${LOGFILE}"
echo "Downloaded OK   (${#SUCCEEDED[@]}): ${SUCCEEDED[*]:-none}" | tee -a "${LOGFILE}"
echo "Still missing   (${#STILL_MISSING[@]}): ${STILL_MISSING[*]:-none}" | tee -a "${LOGFILE}"
echo "Errors          (${#FAILED[@]}): ${FAILED[*]:-none}" | tee -a "${LOGFILE}"
echo "================================================================" | tee -a "${LOGFILE}"
echo "=== retry_missing.sh done $(date) ===" | tee -a "${LOGFILE}"
