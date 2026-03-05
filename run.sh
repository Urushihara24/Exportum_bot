#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PYTHON="$ROOT_DIR/.venv/bin/python"

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "Error: Python not found in .venv. Create venv first."
  exit 1
fi

cd "$ROOT_DIR"
exec "$VENV_PYTHON" "$ROOT_DIR/main.py"
