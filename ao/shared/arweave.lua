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
local HTTP_REAL = os.getenv("ARWEAVE_HTTP_REAL") == "1"
local HTTP_SIGNER_HEADER = os.getenv("ARWEAVE_HTTP_SIGNER_HEADER") or "X-Arweave-Signer"
local HTTP_RETRIES = tonumber(os.getenv("ARWEAVE_HTTP_RETRIES") or "3")
local HTTP_BACKOFF_MS = tonumber(os.getenv("ARWEAVE_HTTP_BACKOFF_MS") or "200")
local MAX_MANIFEST_BYTES = tonumber(os.getenv("ARWEAVE_MAX_MANIFEST_BYTES") or "262144") -- 256 KiB
local HTTP_MAX_BODY = tonumber(os.getenv("ARWEAVE_HTTP_MAX_BODY") or "1048576") -- 1 MiB
local EXPECT_RESPONSE_HASH = os.getenv("ARWEAVE_EXPECT_RESPONSE_HASH")
local FORCE_ERROR = os.getenv("ARWEAVE_FORCE_ERROR") == "1"
local RESPONSE_PATTERN = os.getenv("ARWEAVE_RESPONSE_PATTERN") or "^%s*%{\""
local ok_cjson_safe, cjson_safe = pcall(require, "cjson.safe")
local cjson = cjson_safe or require("cjson") -- required dependency
local schema = require("ao.shared.schema")
local openssl_ok, openssl = pcall(require, "openssl")
local sodium_ok, sodium = pcall(require, "sodium")

local function next_tx()
  counter = counter + 1
  return string.format("mock-tx-%06d", counter)
end

local function ensure_dir(path)
  os.execute(string.format('mkdir -p "%s"', path))
end

local function bin_to_hex(bytes)
  return (bytes:gsub(".", function(c) return string.format("%02x", string.byte(c)) end))
end

local function sha256(str)
  if openssl_ok and openssl.digest then
    local d = openssl.digest.new("sha256")
    d:update(str)
    return bin_to_hex(d:final())
  elseif sodium_ok and sodium.crypto_hash_sha256 then
    return bin_to_hex(sodium.crypto_hash_sha256(str))
  else
    local r = io.popen("printf %s \"" .. str:gsub("\"", "\\\"") .. "\" | openssl dgst -sha256 -binary 2>/dev/null | xxd -p", "r")
    if r then
      local out = r:read("*a") or ""
      r:close()
      out = out:gsub("%s+", "")
      if #out > 0 then return out end
    end
  end
  return nil
end

local function file_sha256(path)
  local f = io.open(path, "rb")
  if not f then return nil end
  local content = f:read("*a")
  f:close()
  return sha256(content)
end

local function has_curl()
  local ok = os.execute("command -v curl >/dev/null 2>&1")
  return ok == true or ok == 0
end

local function http_post(serialized, tx)
  ensure_dir(REQUEST_LOG)
  local response_path = string.format("%s/%s-response.json", REQUEST_LOG, tx)
  local auth_header = API_KEY and (" -H \"Authorization: Bearer " .. API_KEY .. "\"") or ""
  local signer_header = SIGNER and (" -H \"" .. HTTP_SIGNER_HEADER .. ": " .. SIGNER .. "\"") or ""
  local status
  for attempt = 1, HTTP_RETRIES do
    local cmd = string.format("echo %q | curl -s -o \"%s\" -w \"%%{http_code}\" -H \"Content-Type: application/json\"%s%s --max-time %d -X POST \"%s\" --data-binary @-",
      serialized,
      response_path,
      auth_header,
      signer_header,
      HTTP_TIMEOUT,
      ENDPOINT or "")
    local pipe = io.popen(cmd, "r")
    if pipe then
      status = pipe:read("*a")
      pipe:close()
      status = status and status:match("(%d+)")
      if status then status = tonumber(status) end
      if status and status < 500 then
        break
      end
    end
    if attempt < HTTP_RETRIES then
      local jitter = math.random() * 0.5 + 0.75 -- 0.75-1.25x
      os.execute(string.format("sleep %.3f", (HTTP_BACKOFF_MS * jitter) / 1000))
    end
  end
  return status, response_path
end

local function signer_exists()
  if not SIGNER or SIGNER == "" then return true end
  local f = io.open(SIGNER, "r")
  if f then f:close(); return true end
  return false
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

local function sorted_keys(tbl)
  local keys = {}
  for k in pairs(tbl) do
    table.insert(keys, k)
  end
  table.sort(keys, function(a, b)
    return tostring(a) < tostring(b)
  end)
  return keys
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
      for _, k in ipairs(sorted_keys(value)) do
        local v = value[k]
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
  if MAX_MANIFEST_BYTES and #serialized > MAX_MANIFEST_BYTES then
    return nil, "too_large"
  end
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
    if MAX_MANIFEST_BYTES and #serialized > MAX_MANIFEST_BYTES then
      return nil, "too_large"
    end
    local hash = sha256(serialized) or fallback_checksum(serialized)
    local httpStatus, response_path
    if FORCE_ERROR then
      httpStatus = 500
    elseif HTTP_REAL and ENDPOINT and has_curl() and not (os.getenv("ARWEAVE_HTTP_DRYRUN") == "1") then
      if not signer_exists() then
        log_request(tx, {
          endpoint = ENDPOINT or "<missing-endpoint>",
          apiKey = API_KEY and "<redacted>",
          signer = SIGNER or "<missing>",
          timeout = HTTP_TIMEOUT,
          body = payload,
          simulated = true,
          error = "signer_missing"
        }, hash)
        return tx, hash
      end
      httpStatus, response_path = http_post(serialized, tx)
    else
      -- offline simulated response body so schema validation/path logic still runs
      ensure_dir(REQUEST_LOG)
      response_path = string.format("%s/%s-response.json", REQUEST_LOG, tx)
      local body = os.getenv("ARWEAVE_HTTP_SIM_BODY") or string.format('{"status":"ok","tx":"%s"}', tx)
      local f = io.open(response_path, "w"); if f then f:write(body); f:close() end
      httpStatus = tonumber(os.getenv("ARWEAVE_HTTP_SIM_STATUS") or "200")
    end
    local signerHash = SIGNER and file_sha256(SIGNER) or nil
    if httpStatus and httpStatus >= 400 then
      log_request(tx, { error = "http_error", status = httpStatus })
      return nil, "http_error"
    end
    if response_path then
      local f = io.open(response_path, "r")
      if f then
        local body = f:read("*a") or ""
        f:close()
        if #body == 0 then
          log_request(tx, { warning = "empty_response" })
        elseif HTTP_MAX_BODY and #body > HTTP_MAX_BODY then
          log_request(tx, { error = "response_too_large", size = #body })
          return nil, "http_response_too_large"
        else
          if RESPONSE_PATTERN and not body:match(RESPONSE_PATTERN) then
            log_request(tx, { warning = "response_unexpected_pattern" })
            return nil, "http_response_invalid"
          end
          local parsed = cjson.decode(body)
          if not parsed then
            return nil, "http_response_invalid_json"
          end
          local ok_schema, err_schema = schema.validate("arweaveResponse", parsed)
          if not ok_schema then
            log_request(tx, { warning = "response_schema_invalid" })
            return nil, "http_response_schema_invalid"
          end
          local resp_hash = sha256(body)
          if not resp_hash then
            log_request(tx, { warning = "response_hash_failed" })
          else
            log_request(tx, { responseHash = resp_hash })
            if EXPECT_RESPONSE_HASH and resp_hash ~= EXPECT_RESPONSE_HASH then
              return nil, "response_hash_mismatch"
            end
          end
        end
      end
    end
    log_request(tx, {
      endpoint = ENDPOINT or "<missing-endpoint>",
      apiKey = API_KEY and "<redacted>",
      signer = SIGNER and "<redacted>",
      signerHash = signerHash,
      timeout = HTTP_TIMEOUT,
      body = payload,
      simulated = not HTTP_REAL,
      httpStatus = httpStatus,
      responsePath = response_path,
    }, hash)
    return tx, hash
  end
end

-- Expose for tests
Ar._manifests = manifests

return Ar
