#!/usr/bin/env bash
# CryoBacktester — Cloud Agent install script.
# Idempotent: safe to re-run. Prepares the Python 3.12 venv, the private
# workspace submodule (strategies/experiments/tests), and all dependencies.
set -euo pipefail

# Resolve repo root (this file lives in <repo>/.cursor/).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# 1. System dependency: the stdlib venv module is not bundled with the
#    base image's python3.12. Install it once (idempotent via dpkg check).
if ! dpkg -s python3.12-venv >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends python3.12-venv
fi

# 2. Private workspace submodule (full strategy registry + tests). The
#    repo is listed under repositoryDependencies so the generated token
#    can fetch it. No-op if already checked out.
git submodule update --init --recursive

# 3. Python virtual environment at .venv/ (matches AGENTS.md / pyproject).
if [ ! -x ".venv/bin/python" ]; then
  python3.12 -m venv .venv
fi

# 4. Dependencies (idempotent: pip skips already-satisfied requirements).
# shellcheck disable=SC1091
. .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt

echo "CryoBacktester install complete: venv=.venv python=$(python --version 2>&1)"
