#!/usr/bin/env bash
# Validates signer JSON structure for Arweave when ARWEAVE_HTTP_SIGNER is set.
# Requires python3.

set -euo pipefail

if [ -z "${ARWEAVE_HTTP_SIGNER:-}" ]; then
  echo "[signer-validate] Skipping (no ARWEAVE_HTTP_SIGNER)"
  exit 0
fi

if [ ! -r "${ARWEAVE_HTTP_SIGNER}" ]; then
  echo "[signer-validate] Signer not readable: ${ARWEAVE_HTTP_SIGNER}" >&2
  exit 1
fi

python3 - <<'PY'
import json, os, sys
path = os.environ["ARWEAVE_HTTP_SIGNER"]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)

required = ["kty", "n", "e", "d"]
missing = [k for k in required if k not in data]
if missing:
    sys.stderr.write(f"[signer-validate] Missing fields: {missing}\n")
    sys.exit(1)

print(f"[signer-validate] OK {path} (kty={data.get('kty')})")
PY
