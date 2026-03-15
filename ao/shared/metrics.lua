-- Minimal metrics stub: counts and durations written to NDJSON file (mock-friendly).

local Metrics = {}

local LOG_PATH = os.getenv "METRICS_LOG" or "metrics/metrics.log"
local ENABLED = os.getenv "METRICS_ENABLED" ~= "0"
local PROM_PATH = os.getenv "METRICS_PROM_PATH"
local FLUSH_EVERY = tonumber(os.getenv "METRICS_FLUSH_EVERY" or "0")
local FLUSH_INTERVAL = tonumber(os.getenv "METRICS_FLUSH_INTERVAL_SEC" or "0")
local counters = {}
local since_flush = 0
local last_flush = os.time()
local timer = require "ao.shared.timer"
local started = false

local function ensure_dir(path)
  local dir = path:match "(.+)/[^/]+$"
  if dir then
    os.execute(string.format('mkdir -p "%s"', dir))
  end
end

local function log(event)
  if not ENABLED or not LOG_PATH then
    return
  end
  ensure_dir(LOG_PATH)
  local f = io.open(LOG_PATH, "a")
  if not f then
    return
  end
  f:write(
    string.format(
      '{"ts":"%s","event":"%s","value":%s}\n',
      os.date "!%Y-%m-%dT%H:%M:%SZ",
      event.name or "metric",
      event.value or 0
    )
  )
  f:close()
end

function Metrics.inc(name, value)
  value = value or 1
  counters[name] = (counters[name] or 0) + value
  log { name = name, value = counters[name] }
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
  if FLUSH_INTERVAL > 0 then
    timer.start(FLUSH_INTERVAL, Metrics.flush_prom)
  end
end

function Metrics.flush_prom()
  if not PROM_PATH then
    return
  end
  ensure_dir(PROM_PATH)
  local f = io.open(PROM_PATH, "w")
  if not f then
    return
  end
  for k, v in pairs(counters) do
    f:write(string.format("%s %d\n", k:gsub("[^%w_]", "_"), v))
  end
  f:close()
end

function Metrics.last_flush_ts()
  return last_flush
end

function Metrics.get(name)
  return counters[name] or 0
end

function Metrics._reset()
  counters = {}
end

function Metrics.start_background()
  if started then
    return
  end
  started = true
  if FLUSH_INTERVAL > 0 then
    timer.start(FLUSH_INTERVAL, Metrics.flush_prom)
  end
end

-- auto-start if interval specified
Metrics.start_background()

return Metrics
