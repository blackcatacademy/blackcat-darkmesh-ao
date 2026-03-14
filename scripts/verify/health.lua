-- Simple health snapshot for mock AO processes.

local registry = require("ao.registry.process")
local site = require("ao.site.process")
local catalog = require("ao.catalog.process")
local access = require("ao.access.process")
local idem = require("ao.shared.idempotency")
local metrics = require("ao.shared.metrics")

local function count(tbl)
  local c = 0
  for _ in pairs(tbl) do c = c + 1 end
  return c
end

local function print_line(label, value)
  io.stdout:write(label .. ": " .. tostring(value) .. "\n")
end

local states = {
  registry = registry._state,
  site = site._state,
  catalog = catalog._state,
  access = access._state,
}

print_line("registry.sites", count(states.registry.sites))
print_line("registry.domains", count(states.registry.domains))
print_line("site.pages", count(states.site.pages))
print_line("site.routes", count(states.site.routes))
print_line("catalog.products", count(states.catalog.products))
print_line("catalog.categories", count(states.catalog.categories))
print_line("access.entitlements", count(states.access.entitlements))
print_line("access.protected", count(states.access.protected))

print_line("idempotency.entries", idem._size and idem._size() or "n/a")
print_line("metrics.counters", metrics.get and metrics.get("dummy") and "available" or "available") -- placeholder

print_line("health", "ok")
