#!/usr/bin/env bash
set -euo pipefail

lua_versions=(lua5.4 lua)
found_lua=""
for l in "${lua_versions[@]}"; do
  if command -v "$l" >/dev/null 2>&1; then
    found_lua="$l"
    break
  fi
done

if [ -z "$found_lua" ]; then
  echo "Lua interpreter not found for libsodium check" >&2
  exit 1
fi

"$found_lua" - <<'LUA'
local function require_any(mods)
  for _, m in ipairs(mods) do
    local ok, mod = pcall(require, m)
    if ok then return true end
  end
  return false
end

local sodium_ok = require_any({ "sodium", "luasodium" })
local ed_ok = require_any({ "ed25519", "sodium", "luasodium" })

if not sodium_ok or not ed_ok then
  error(string.format("Missing sodium/ed25519 (sodium:%s ed25519:%s)", tostring(sodium_ok), tostring(ed_ok)))
end
LUA
