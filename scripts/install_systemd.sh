#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_NAME="polymarket-slowbot"
SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
TIMER_PATH="/etc/systemd/system/${SERVICE_NAME}.timer"
RUN_USER="${SUDO_USER:-$USER}"

cat <<EOF >/tmp/${SERVICE_NAME}.service
[Unit]
Description=Polymarket Slowbot Daily Runner
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=${RUN_USER}
WorkingDirectory=${ROOT_DIR}
ExecStart=/bin/bash ${ROOT_DIR}/scripts/server_run.sh
StandardOutput=append:${ROOT_DIR}/data/logs/systemd.log
StandardError=append:${ROOT_DIR}/data/logs/systemd.err.log
EOF

cat <<EOF >/tmp/${SERVICE_NAME}.timer
[Unit]
Description=Run Polymarket Slowbot once daily (UTC)

[Timer]
OnCalendar=*-*-* 01:00:00
Persistent=true
Unit=${SERVICE_NAME}.service

[Install]
WantedBy=timers.target
EOF

sudo mv /tmp/${SERVICE_NAME}.service "${SERVICE_PATH}"
sudo mv /tmp/${SERVICE_NAME}.timer "${TIMER_PATH}"

sudo systemctl daemon-reload
sudo systemctl enable --now "${SERVICE_NAME}.timer"

echo "Installed:"
echo "  ${SERVICE_PATH}"
echo "  ${TIMER_PATH}"
echo "Timer status:"
sudo systemctl status "${SERVICE_NAME}.timer" --no-pager
