-- Minimal metrics stub: counts and durations written to NDJSON file (mock-friendly).

local Metrics = {}

local LOG_PATH = os.getenv("METRICS_LOG") or "metrics/metrics.log"
local ENABLED = os.getenv("METRICS_ENABLED") ~= "0"
local counters = {}

local function ensure_dir(path)
  local dir = path:match("(.+)/[^/]+$")
  if dir then
    os.execute(string.format('mkdir -p "%s"', dir))
  end
end

local function log(event)
  if not ENABLED or not LOG_PATH then return end
  ensure_dir(LOG_PATH)
  local f = io.open(LOG_PATH, "a")
  if not f then return end
  f:write(string.format('{"ts":"%s","event":"%s","value":%s}\n',
    os.date("!%Y-%m-%dT%H:%M:%SZ"),
    event.name or "metric",
    event.value or 0))
  f:close()
end

function Metrics.inc(name, value)
  value = value or 1
  counters[name] = (counters[name] or 0) + value
  log({ name = name, value = counters[name] })
end

function Metrics.get(name)
  return counters[name] or 0
end

function Metrics._reset()
  counters = {}
end

return Metrics
