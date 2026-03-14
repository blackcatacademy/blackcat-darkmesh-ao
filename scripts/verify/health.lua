-- Simple health snapshot for mock AO processes.

local registry = require("ao.registry.process")
local site = require("ao.site.process")
local catalog = require("ao.catalog.process")
local access = require("ao.access.process")
local idem = require("ao.shared.idempotency")
local metrics = require("ao.shared.metrics")
local lfs_ok, lfs = pcall(require, "lfs")

local function count(tbl)
  local c = 0
  for _ in pairs(tbl) do c = c + 1 end
  return c
end

local function dir_size(path)
  if not lfs_ok then return "n/a" end
  local total = 0
  for file in lfs.dir(path) do
    if file ~= "." and file ~= ".." then
      local full = path .. "/" .. file
      local attr = lfs.attributes(full)
      if attr then total = total + attr.size end
    end
  end
  return total
end

local function rotated_count(path)
  if not lfs_ok then return "n/a" end
  local count = 0
  for file in lfs.dir(path) do
    if file:match("^audit.*%.log%.") then
      count = count + 1
    end
  end
  return count
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
print_line("metrics.flush_mode", os.getenv("METRICS_FLUSH_EVERY") or "immediate")
local audit_dir = os.getenv("AUDIT_LOG_DIR") or "arweave/manifests"
print_line("audit.dir.size", dir_size(audit_dir))
print_line("audit.dir.path", audit_dir)
print_line("audit.rotated.count", rotated_count(audit_dir))

print_line("health", "ok")
