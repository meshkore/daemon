#!/usr/bin/env bash
# DM0 — test runner. One command from a fresh clone.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet -r tests/requirements.txt
pytest tests/ --cov=. --cov-branch --cov-report=term --cov-fail-under=25 "$@"
