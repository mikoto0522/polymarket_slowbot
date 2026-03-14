#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ ! -d ".venv" ]; then
  echo ".venv not found. Run scripts/server_setup.sh first."
  exit 1
fi

if [ ! -f ".env" ]; then
  echo ".env not found. Create it from .env.server.example first."
  exit 1
fi

source .venv/bin/activate
python -m src.main
