#!/usr/bin/env lua

-- Best-effort check that local schema bundle matches expected Arweave tx/hash.
-- In mock mode it reads arweave/manifests/<tx>.json; in real mode you would curl the tx.

local cjson = require("cjson.safe")

local tx = arg[1] or os.getenv("SCHEMA_MANIFEST_TX")
local expected_hash = arg[2] or os.getenv("SCHEMA_HASH")

if not tx or tx == "" then
  io.stderr:write("usage: lua scripts/setup/schema_fetch.lua <tx> [expected_hash]\n")
  os.exit(1)
end

local path = string.format("arweave/manifests/%s.json", tx)
local f = io.open(path, "r")
if not f then
  io.stderr:write("manifest not found locally (mock mode): " .. path .. "\n")
  os.exit(2)
end
local data = f:read("*a")
f:close()
local manifest = cjson.decode(data) or {}

local function sha256(str)
  local p = io.popen("printf %s \"" .. str:gsub("\"","\\\"") .. "\" | openssl dgst -sha256 2>/dev/null")
  if not p then return nil end
  local out = p:read("*a") or ""
  p:close()
  return out:match("= (%w+)")
end

local actual_hash = sha256(data)

if expected_hash and expected_hash ~= "" then
  if actual_hash ~= expected_hash then
    io.stderr:write(string.format("hash mismatch: expected %s got %s\n", expected_hash, tostring(actual_hash)))
    os.exit(3)
  end
  print(string.format("ok: tx=%s hash=%s", tx, actual_hash))
else
  print(string.format("computed hash=%s for tx=%s", tostring(actual_hash), tx))
end

-- emit modules summary if present
if manifest.modules then
  print(string.format("modules: %d", #manifest.modules))
end
