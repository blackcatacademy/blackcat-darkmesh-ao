-- Simple in-memory idempotency registry for handler scaffolding.
-- Stores response per Request-Id within process lifetime with TTL and bounded size.

local Idem = {}
local seen = {}
local ttl = tonumber(os.getenv("IDEM_TTL_SECONDS") or "300") -- 5 minutes default
local max_entries = tonumber(os.getenv("IDEM_MAX_ENTRIES") or "1024")

local function now()
  return os.time()
end

local function prune()
  -- drop expired
  local count = 0
  for k, v in pairs(seen) do
    if v.expire_at and v.expire_at < now() then
      seen[k] = nil
    else
      count = count + 1
    end
  end
  -- cap size by removing oldest if over limit
  if count > max_entries then
    local oldest_key, oldest_ts
    for k, v in pairs(seen) do
      if not oldest_ts or v.recorded_at < oldest_ts then
        oldest_ts = v.recorded_at
        oldest_key = k
      end
    end
    if oldest_key then
      seen[oldest_key] = nil
    end
  end
end

function Idem.check(request_id)
  prune()
  local entry = seen[request_id]
  if not entry then return nil end
  if entry.expire_at and entry.expire_at < now() then
    seen[request_id] = nil
    return nil
  end
  return entry.response
end

function Idem.record(request_id, response)
  if not request_id then return end
  seen[request_id] = {
    response = response,
    recorded_at = now(),
    expire_at = ttl > 0 and (now() + ttl) or nil,
  }
  prune()
end

-- reset for tests if needed
function Idem._clear()
  seen = {}
end

function Idem._size()
  local c = 0
  for _ in pairs(seen) do c = c + 1 end
  return c
end

return Idem
