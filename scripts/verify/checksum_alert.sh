#!/usr/bin/env bash
set -euo pipefail
AUDIT_DIR=${AUDIT_LOG_DIR:-arweave/manifests}
AUDIT_MAX=${AUDIT_MAX_BYTES:-5242880}
status=0
if [ -d "$AUDIT_DIR" ]; then
  size=$(du -sb "$AUDIT_DIR" | awk '{print $1}')
  echo "audit.size=$size"
  if [ "$AUDIT_MAX" -gt 0 ] && [ "$size" -gt "$AUDIT_MAX" ]; then
    echo "audit size exceeded" >&2
    status=2
  fi
  find "$AUDIT_DIR" -type f -maxdepth 1 -printf "%p %s\n" | sort -k2 -nr | head -n 5
else
  echo "audit dir missing: $AUDIT_DIR"
fi
exit $status
