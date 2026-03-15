#!/usr/bin/env bash
set -euo pipefail
AUDIT_DIR=${AUDIT_LOG_DIR:-arweave/manifests}
AUDIT_MAX=${AUDIT_MAX_BYTES:-5242880}
QUEUE=${AO_QUEUE_PATH:-}
WAL=${AO_WAL_PATH:-}
QUEUE_MAX=${AO_QUEUE_MAX_BYTES:-2097152}
WAL_MAX=${AO_WAL_MAX_BYTES:-5242880}
status=0

check_dir() {
  local path=$1
  local max=$2
  local label=$3
  if [ -z "$path" ] || [ ! -d "$path" ]; then
    echo "$label: skip"
    return
  fi
  local size=$(du -sb "$path" | awk '{print $1}')
  echo "$label.size=$size"
  if [ "$max" -gt 0 ] && [ "$size" -gt "$max" ]; then
    echo "$label size exceeded" >&2
    status=2
  fi
  find "$path" -maxdepth 1 -type f -printf "%p %s\n" | sort -k2 -nr | head -n 5
}

check_file() {
  local file=$1
  local max=$2
  local label=$3
  if [ -z "$file" ] || [ ! -f "$file" ]; then
    echo "$label: skip"
    return
  fi
  local size=$(stat -c%s "$file")
  local hash=$(sha256sum "$file" | awk '{print $1}')
  echo "$label.size=$size hash=$hash"
  if [ "$max" -gt 0 ] && [ "$size" -gt "$max" ]; then
    echo "$label size exceeded" >&2
    status=2
  fi
}

check_dir "$AUDIT_DIR" "$AUDIT_MAX" "audit"
check_file "$QUEUE" "$QUEUE_MAX" "queue"
check_file "$WAL" "$WAL_MAX" "wal"
exit $status
