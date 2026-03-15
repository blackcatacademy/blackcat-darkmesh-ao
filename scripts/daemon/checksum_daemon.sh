#!/usr/bin/env bash
set -euo pipefail
INTERVAL=${AO_CHECKSUM_INTERVAL:-60}
while true; do
  timestamp=$(date -Iseconds)
  echo "[checksum] $timestamp running" >&2
  scripts/verify/checksum_alert.sh || true
  sleep "$INTERVAL"
done
