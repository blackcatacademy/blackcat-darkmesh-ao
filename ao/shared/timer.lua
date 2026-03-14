-- Minimal timer abstraction using luv if available.
-- Returns no-op functions when luv is absent.

local ok, uv = pcall(require, "luv")

local Timer = {}
local started = false

function Timer.start(interval_sec, fn)
  if not ok or not uv or started then return end
  if not interval_sec or interval_sec <= 0 then return end
  local t = uv.new_timer()
  if not t then return end
  started = true
  t:start(interval_sec * 1000, interval_sec * 1000, function()
    pcall(fn)
  end)
end

function Timer.is_started()
  return started
end

return Timer
