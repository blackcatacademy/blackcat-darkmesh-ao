#!/usr/bin/env bash
# Periodically run checksum_alert for audit/queue/wal
set -euo pipefail
INTERVAL=${CHECKSUM_INTERVAL_SEC:-300}
if [ "$INTERVAL" -le 0 ]; then
  echo "CHECKSUM_INTERVAL_SEC must be >0" >&2
  exit 1
fi
while true; do
  ./scripts/verify/checksum_alert.sh || true
  sleep "$INTERVAL"
done
