#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PYTHON=${PYTHON:-.venv/bin/python}
if [[ ! -x "$PYTHON" ]]; then
  echo "缺少虚拟环境，请先运行: python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt" >&2
  exit 1
fi
exec "$PYTHON" -m uvicorn lakehouse.app:app --host "${HOST:-127.0.0.1}" --port "${PORT:-8765}"
