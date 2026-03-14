-- Append-only audit stub for local testing.

local Audit = {}
local records = {}
local LOG_DIR = os.getenv("AUDIT_LOG_DIR") or "arweave/manifests"

local function ensure_dir(path)
  os.execute(string.format('mkdir -p "%s"', path))
end

local function json_encode(obj)
  if type(obj) == "table" then
    local parts = {}
    for k, v in pairs(obj) do
      table.insert(parts, string.format("%q:%q", k, tostring(v)))
    end
    return "{" .. table.concat(parts, ",") .. "}"
  end
  return tostring(obj)
end

function Audit.append(entry)
  table.insert(records, entry)
  if LOG_DIR then
    ensure_dir(LOG_DIR)
    local path = string.format("%s/audit.log", LOG_DIR)
    local f = io.open(path, "a")
    if f then
      f:write(json_encode(entry), "\n")
      f:close()
    end
  end
end

function Audit.all()
  return records
end

function Audit._clear()
  records = {}
end

return Audit
