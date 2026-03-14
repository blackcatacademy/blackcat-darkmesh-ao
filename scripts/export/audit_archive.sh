#!/usr/bin/env bash
# Archive audit logs into a timestamped tar.gz in /tmp (or AUDIT_ARCHIVE_DIR).
# Usage: AUDIT_LOG_DIR=... scripts/export/audit_archive.sh

set -euo pipefail

LOG_DIR="${AUDIT_LOG_DIR:-arweave/manifests}"
ARCHIVE_DIR="${AUDIT_ARCHIVE_DIR:-/tmp}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE="${ARCHIVE_DIR}/audit-${STAMP}.tar.gz"

if [ ! -d "$LOG_DIR" ]; then
  echo "Log dir not found: $LOG_DIR" >&2
  exit 1
fi

mkdir -p "$ARCHIVE_DIR"
tar -czf "$ARCHIVE" -C "$LOG_DIR" .
echo "Archived audit logs to $ARCHIVE"
