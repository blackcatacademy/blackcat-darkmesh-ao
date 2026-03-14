-- Arweave adapter for publish flow.
-- Default mode: file-backed mock under arweave/snapshots (deterministic, hash checked).
-- If ARWEAVE_MODE=mock (default), nothing leaves the machine.

local Ar = {}

local counter = 0
local manifests = {}

local MODE = os.getenv("ARWEAVE_MODE") or "mock"
local SNAPSHOT_DIR = os.getenv("ARWEAVE_STORAGE_DIR") or "arweave/snapshots"
local REQUEST_LOG = os.getenv("ARWEAVE_REQUEST_LOG") or "arweave/manifests"
local ENDPOINT = os.getenv("ARWEAVE_HTTP_ENDPOINT")
local API_KEY = os.getenv("ARWEAVE_HTTP_API_KEY")
local SIGNER = os.getenv("ARWEAVE_HTTP_SIGNER") -- path to key or wallet JSON
local HTTP_TIMEOUT = tonumber(os.getenv("ARWEAVE_HTTP_TIMEOUT") or "10")

local function next_tx()
  counter = counter + 1
  return string.format("mock-tx-%06d", counter)
end

local function ensure_dir(path)
  os.execute(string.format('mkdir -p "%s"', path))
end

local function sha256(str)
  local p = io.popen("openssl dgst -sha256 2>/dev/null", "w")
  if p then
    p:write(str)
    p:close()
  end
  local r = io.popen("echo -n \"" .. str:gsub("\"", "\\\"") .. "\" | openssl dgst -sha256 2>/dev/null")
  if r then
    local out = r:read("*a")
    r:close()
    if out and out:match("= (%w+)$") then
      return out:match("= (%w+)$")
    end
  end
  return nil
end

local function fallback_checksum(str)
  local sum = 0
  for i = 1, #str do
    sum = (sum + string.byte(str, i)) % 0xFFFFFFFF
  end
  return string.format("%08x", sum)
end

local function is_array(tbl)
  local i = 0
  for _ in pairs(tbl) do
    i = i + 1
    if tbl[i] == nil then return false end
  end
  return true
end

local function json_encode(value)
  local t = type(value)
  if t == "nil" then return "null" end
  if t == "boolean" then return value and "true" or "false" end
  if t == "number" then return tostring(value) end
  if t == "string" then
    return string.format("%q", value)
  end
  if t == "table" then
    if is_array(value) then
      local parts = {}
      for _, v in ipairs(value) do
        table.insert(parts, json_encode(v))
      end
      return "[" .. table.concat(parts, ",") .. "]"
    else
      local parts = {}
      for k, v in pairs(value) do
        table.insert(parts, string.format("%q:%s", k, json_encode(v)))
      end
      return "{" .. table.concat(parts, ",") .. "}"
    end
  end
  return "\"<unsupported>\""
end

local function persist_manifest(tx, content)
  ensure_dir(SNAPSHOT_DIR)
  local path = SNAPSHOT_DIR .. "/" .. tx .. ".json"
  local f = io.open(path, "w")
  if f then
    f:write(content)
    f:close()
  end
end

-- Stores a snapshot payload and returns a manifest transaction id and hash.
function Ar.put_snapshot(payload)
  local tx = next_tx()
  local serialized = json_encode(payload)
  local hash = sha256(serialized) or fallback_checksum(serialized)

  manifests[tx] = {
    payload = payload,
    hash = hash,
    storedAt = os.date("!%Y-%m-%dT%H:%M:%SZ"),
  }

  if MODE == "mock" then
    persist_manifest(tx, serialized)
  end

  return tx, hash
end

function Ar.get_snapshot(tx)
  return manifests[tx]
end

function Ar.verify_snapshot(tx, expected_hash)
  local m = manifests[tx]
  if not m then return false, "not_found" end
  if expected_hash and m.hash ~= expected_hash then return false, "hash_mismatch" end
  return true
end

-- HTTP mode placeholder: log outbound request; real network disabled here.
local function log_request(tx, payload, hash)
  ensure_dir(REQUEST_LOG)
  local path = string.format("%s/%s-request.json", REQUEST_LOG, tx)
  local f = io.open(path, "w")
  if f then
    f:write(json_encode({ tx = tx, hash = hash, payload = payload, mode = MODE }))
    f:close()
  end
end

if MODE == "http" then
  -- Simulated HTTP call: writes request + simulated response status to manifests log.
  -- Still offline/off-chain; safe for local runs.
  function Ar.put_snapshot(payload)
    local tx = next_tx()
    local serialized = json_encode(payload)
    local hash = sha256(serialized) or fallback_checksum(serialized)
    log_request(tx, {
      endpoint = ENDPOINT or "<missing-endpoint>",
      apiKey = API_KEY and "<redacted>",
      signer = SIGNER and "<redacted>",
      timeout = HTTP_TIMEOUT,
      body = payload,
      simulated = true,
    }, hash)
    return tx, hash
  end
end

-- Expose for tests
Ar._manifests = manifests

return Ar
