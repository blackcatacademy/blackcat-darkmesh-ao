-- Append-only audit stub for local testing.

local Audit = {}
local records = {}
local LOG_DIR = os.getenv("AUDIT_LOG_DIR") or "arweave/manifests"
local MAX_IN_MEMORY = tonumber(os.getenv("AUDIT_MAX_RECORDS") or "1000")
local FORMAT = os.getenv("AUDIT_FORMAT") or "line" -- line | ndjson
local ROTATE_MAX = tonumber(os.getenv("AUDIT_ROTATE_MAX") or "1048576") -- bytes

local function ensure_dir(path)
  os.execute(string.format('mkdir -p "%s"', path))
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
  if t == "string" then return string.format("%q", value) end
  if t == "table" then
    if is_array(value) then
      local parts = {}
      for _, v in ipairs(value) do table.insert(parts, json_encode(v)) end
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

local function rotate_if_needed(path)
  local f = io.open(path, "r")
  if not f then return end
  local content = f:read("*a")
  f:close()
  if #content >= ROTATE_MAX then
    local rotated = path .. "." .. os.date("!%Y%m%d%H%M%S")
    os.rename(path, rotated)
  end
end

function Audit.append(entry)
  if not entry.ts then
    entry.ts = os.date("!%Y-%m-%dT%H:%M:%SZ")
  end
  table.insert(records, entry)
  if #records > MAX_IN_MEMORY then
    table.remove(records, 1)
  end
  if LOG_DIR then
    ensure_dir(LOG_DIR)
    local path = string.format("%s/audit.log", LOG_DIR)
    rotate_if_needed(path)
    local f = io.open(path, "a")
    if f then
      if FORMAT == "ndjson" then
        f:write(json_encode(entry), "\n")
      else
        f:write(tostring(entry.action or "event"), " ", json_encode(entry), "\n")
      end
      f:close()
    end
  end
end

-- Helper to record a normalized event
-- fields: process, action, requestId, actorRole, siteId, resultCode
function Audit.record(process, action, msg, resp, extra)
  local entry = {
    process = process,
    action = action,
    requestId = msg and msg["Request-Id"],
    actorRole = msg and (msg["Actor-Role"] or msg.actorRole),
    siteId = msg and (msg["Site-Id"] or msg.siteId),
    resultCode = resp and resp.code or resp and resp.status,
  }
  if extra then
    for k, v in pairs(extra) do
      entry[k] = v
    end
  end
  Audit.append(entry)
end

function Audit.all()
  return records
end

function Audit._clear()
  records = {}
end

return Audit
