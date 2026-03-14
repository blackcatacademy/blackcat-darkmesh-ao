#!/usr/bin/env bash
# Simple health check for Arweave endpoint (optional).
# Usage: ARWEAVE_HTTP_ENDPOINT=https://... scripts/verify/arweave_health.sh

set -euo pipefail

ENDPOINT="${ARWEAVE_HEALTH_ENDPOINT:-${ARWEAVE_HTTP_ENDPOINT:-}}"
TIMEOUT="${ARWEAVE_HTTP_TIMEOUT:-10}"

if [ -z "$ENDPOINT" ]; then
  echo "ARWEAVE_HTTP_ENDPOINT not set; skipping health check." >&2
  exit 0
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl not available; cannot run health check." >&2
  exit 1
fi

STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time "$TIMEOUT" "$ENDPOINT")
if [ "$STATUS" = "200" ] || [ "$STATUS" = "204" ]; then
  echo "[arweave] OK $STATUS at $ENDPOINT"
  exit 0
else
  echo "[arweave] FAIL status=$STATUS endpoint=$ENDPOINT" >&2
  exit 1
fi
