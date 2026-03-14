#!/usr/bin/env bash
# Verifies signer file exists and is readable when ARWEAVE_HTTP_REAL=1.

set -euo pipefail

if [ "${ARWEAVE_HTTP_REAL:-0}" != "1" ]; then
  echo "[signer] Skipping (ARWEAVE_HTTP_REAL!=1)"
  exit 0
fi

if [ -z "${ARWEAVE_HTTP_SIGNER:-}" ]; then
  echo "[signer] Missing ARWEAVE_HTTP_SIGNER" >&2
  exit 1
fi

if [ ! -r "${ARWEAVE_HTTP_SIGNER}" ]; then
  echo "[signer] Signer not readable: ${ARWEAVE_HTTP_SIGNER}" >&2
  exit 1
fi

echo "[signer] OK ${ARWEAVE_HTTP_SIGNER}"
