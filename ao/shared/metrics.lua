-- Minimal metrics stub: counts and durations written to NDJSON file (mock-friendly).

local Metrics = {}

local LOG_PATH = os.getenv("METRICS_LOG") or "metrics/metrics.log"
local ENABLED = os.getenv("METRICS_ENABLED") ~= "0"
local PROM_PATH = os.getenv("METRICS_PROM_PATH")
local FLUSH_EVERY = tonumber(os.getenv("METRICS_FLUSH_EVERY") or "0")
local FLUSH_INTERVAL = tonumber(os.getenv("METRICS_FLUSH_INTERVAL_SEC") or "0")
local counters = {}
local since_flush = 0
local last_flush = os.time()
local last_tick = os.time()
local bg_running = false

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
  since_flush = since_flush + 1
  if FLUSH_EVERY > 0 and since_flush >= FLUSH_EVERY then
    Metrics.flush_prom()
    since_flush = 0
  elseif FLUSH_EVERY == 0 then
    Metrics.flush_prom()
  end
end

function Metrics.tick()
  local now = os.time()
  if FLUSH_INTERVAL > 0 and (now - last_flush) >= FLUSH_INTERVAL then
    Metrics.flush_prom()
    last_flush = now
    since_flush = 0
  end
  last_tick = now
end

function Metrics.flush_prom()
  if not PROM_PATH then return end
  ensure_dir(PROM_PATH)
  local f = io.open(PROM_PATH, "w")
  if not f then return end
  for k, v in pairs(counters) do
    f:write(string.format("%s %d\n", k:gsub("[^%w_]", "_"), v))
  end
  f:close()
end

-- Background flush using simple shell loop; optional, best-effort.
function Metrics.start_bg()
  if bg_running or FLUSH_INTERVAL <= 0 then return end
  bg_running = true
  local cmd = string.format("(while true; do sleep %d; LUA_PATH='%s' lua -e \"require('ao.shared.metrics').flush_prom()\"; done) >/dev/null 2>&1 &",
    math.max(1, FLUSH_INTERVAL),
    package.path)
  os.execute(cmd)
end

function Metrics._bg_running()
  return bg_running
end

function Metrics.get(name)
  return counters[name] or 0
end

function Metrics._reset()
  counters = {}
end

return Metrics
