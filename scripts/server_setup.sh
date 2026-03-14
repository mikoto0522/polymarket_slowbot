#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found. Please install Python 3.10+ first."
  exit 1
fi

run_privileged() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    return 1
  fi
}

install_venv_dependency_if_needed() {
  if ! command -v apt-get >/dev/null 2>&1; then
    return 1
  fi

  local py_mm
  py_mm="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
  local version_pkg="python${py_mm}-venv"
  local fallback_pkg="python3-venv"
  local target_pkg="$fallback_pkg"

  if apt-cache show "$version_pkg" >/dev/null 2>&1; then
    target_pkg="$version_pkg"
  fi

  echo "Installing missing venv dependency: $target_pkg"
  run_privileged apt-get update
  run_privileged apt-get install -y "$target_pkg"
}

create_venv() {
  if python3 -m venv .venv; then
    return 0
  fi
  echo "python3 -m venv failed, trying to install system venv package..."
  if install_venv_dependency_if_needed; then
    python3 -m venv .venv
    return 0
  fi
  echo "Failed to create .venv. Please install python3-venv (or pythonX.Y-venv) manually."
  return 1
}

if [ ! -d ".venv" ]; then
  create_venv
fi

source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

if [ ! -f ".env" ]; then
  cp .env.server.example .env
  echo "Created .env from .env.server.example. Please edit .env before running."
else
  echo ".env already exists, skipped."
fi

mkdir -p data/logs data/reports
echo "Setup completed at: $ROOT_DIR"
