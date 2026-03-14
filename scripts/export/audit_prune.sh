#!/usr/bin/env bash
# Delete rotated audit logs beyond retention (uses AUDIT_RETAIN_FILES).

set -euo pipefail

LOG_DIR="${AUDIT_LOG_DIR:-arweave/manifests}"
RETAIN="${AUDIT_RETAIN_FILES:-10}"

if [ ! -d "$LOG_DIR" ]; then
  echo "Log dir not found: $LOG_DIR" >&2
  exit 1
fi

find "$LOG_DIR" -maxdepth 1 -type f -name 'audit*.log.*' | while read -r file; do
  base=$(echo "$file" | sed 's/\\.log\\..*/.log/')
  prefix=$(basename "$base"). 
done

# Simpler approach: use lua to mirror retention logic
lua - <<'LUA'
local lfs = require("lfs")
local log_dir = os.getenv("AUDIT_LOG_DIR") or "arweave/manifests"
local retain = tonumber(os.getenv("AUDIT_RETAIN_FILES") or "10")

local groups = {}
for file in lfs.dir(log_dir) do
  local base, ts = file:match("^(audit[^%s]+%.log)%.(%d+)$")
  if base and ts then
    groups[base] = groups[base] or {}
    table.insert(groups[base], {file=file, ts=ts})
  end
end

for base, arr in pairs(groups) do
  table.sort(arr, function(a,b) return a.ts > b.ts end)
  for i = retain+1, #arr do
    os.remove(log_dir .. "/" .. arr[i].file)
    print("pruned", arr[i].file)
  end
end
LUA
echo "Prune complete"
