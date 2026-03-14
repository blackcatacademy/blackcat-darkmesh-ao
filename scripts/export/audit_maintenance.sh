#!/usr/bin/env bash
# Combined audit maintenance: prune rotated logs then archive current logs.
# Intended for cron/CI; safe when AUDIT_LOG_DIR is missing (no-op).

set -euo pipefail

LOG_DIR="${AUDIT_LOG_DIR:-arweave/manifests}"
ARCHIVE_DIR="${AUDIT_ARCHIVE_DIR:-/tmp}"
RETAIN="${AUDIT_RETAIN_FILES:-10}"

if [ ! -d "$LOG_DIR" ]; then
  echo "[audit-maintenance] Log dir not found, skipping: $LOG_DIR"
  exit 0
fi

AUDIT_LOG_DIR="$LOG_DIR" AUDIT_RETAIN_FILES="$RETAIN" bash "$(dirname "$0")/audit_prune.sh"
AUDIT_LOG_DIR="$LOG_DIR" AUDIT_ARCHIVE_DIR="$ARCHIVE_DIR" bash "$(dirname "$0")/audit_archive.sh"
echo "[audit-maintenance] done"
