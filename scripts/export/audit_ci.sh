#!/usr/bin/env bash
# CI-friendly audit export helper. No-op if audit logs are absent.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="${1:-$ROOT/artifacts/audit.ndjson}"
LOG_DIR="${AUDIT_LOG_DIR:-$ROOT/arweave/manifests}"

if [ ! -d "$LOG_DIR" ]; then
  echo "[audit-ci] No audit logs under $LOG_DIR, skipping."
  exit 0
fi

mkdir -p "$(dirname "$OUT")"

lua_bin=${LUA_BIN:-}
if [ -z "$lua_bin" ]; then
  if command -v lua5.4 >/dev/null 2>&1; then
    lua_bin=lua5.4
  elif command -v lua >/dev/null 2>&1; then
    lua_bin=lua
  else
    echo "[audit-ci] No Lua interpreter found, skipping." && exit 0
  fi
fi

LUA_PATH="$ROOT/?.lua;$ROOT/?/init.lua;$ROOT/ao/?.lua;$ROOT/ao/?/init.lua;;" \
"$lua_bin" "$ROOT/scripts/export/audit_export.lua" all ndjson "$OUT" || {
  echo "[audit-ci] Export failed (non-fatal)"; exit 0;
}

echo "[audit-ci] Audit logs exported to $OUT"
