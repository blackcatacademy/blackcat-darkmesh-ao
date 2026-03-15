-- Simple health snapshot for mock AO processes.

local registry = require "ao.registry.process"
local site = require "ao.site.process"
local catalog = require "ao.catalog.process"
local access = require "ao.access.process"
local idem = require "ao.shared.idempotency"
local metrics = require "ao.shared.metrics"
local lfs_ok, lfs = pcall(require, "lfs")
local sqlite_ok, sqlite = pcall(require, "lsqlite3")

local function count(tbl)
  local c = 0
  for _ in pairs(tbl) do
    c = c + 1
  end
  return c
end

local function dir_size(path)
  if not lfs_ok then
    return "n/a"
  end
  local total = 0
  for file in lfs.dir(path) do
    if file ~= "." and file ~= ".." then
      local full = path .. "/" .. file
      local attr = lfs.attributes(full)
      if attr then
        total = total + attr.size
      end
    end
  end
  return total
end

local function rotated_count(path)
  if not lfs_ok then
    return "n/a"
  end
  local count = 0
  for file in lfs.dir(path) do
    if file:match "^audit.*%.log%." then
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
print_line("metrics.flush_mode", os.getenv "METRICS_FLUSH_EVERY" or "immediate")
print_line("metrics.last_flush_ts", metrics.last_flush_ts and metrics.last_flush_ts() or "n/a")
local audit_dir = os.getenv "AUDIT_LOG_DIR" or "arweave/manifests"
print_line("audit.dir.size", dir_size(audit_dir))
print_line("audit.dir.path", audit_dir)
print_line("audit.rotated.count", rotated_count(audit_dir))
print_line("deps.luv", pcall(require, "luv") and "yes" or "no")
print_line("deps.ed25519", pcall(require, "ed25519") and "yes" or "no")
print_line("deps.lsqlite3", pcall(require, "lsqlite3") and "yes" or "no")
print_line("deps.cjson", pcall(require, "cjson.safe") and "yes" or "no")
print_line("deps.luaossl", pcall(require, "openssl") and "yes" or "no")
print_line("deps.sodium", pcall(require, "sodium") and "yes" or "no")

local function check_rate_db()
  local path = os.getenv "AUTH_RATE_LIMIT_SQLITE"
  if not path or path == "" then
    print_line("rate_db", "unset")
    return
  end
  if not sqlite_ok then
    print_line("rate_db", "missing lsqlite3")
    return
  end
  local db = sqlite.open(path)
  if not db then
    print_line("rate_db", "open_failed")
    return
  end
  local ok = db:exec "CREATE TABLE IF NOT EXISTS ratelimit_dummy(k TEXT PRIMARY KEY, v INTEGER);"
  if ok ~= sqlite.OK then
    print_line("rate_db", "create_failed")
  else
    local stmt =
      db:prepare "INSERT OR REPLACE INTO ratelimit_dummy(k,v) VALUES('ping', strftime('%s','now'));"
    local res = stmt and stmt:step()
    if res == sqlite.DONE then
      print_line("rate_db", "rw_ok")
    else
      print_line("rate_db", "write_failed")
    end
    if stmt then
      stmt:finalize()
    end
  end
  db:close()
end

local function check_prom()
  local prom = os.getenv "METRICS_PROM_PATH"
  if not prom or prom == "" then
    print_line("prom_path", "unset")
    return
  end
  local f = io.open(prom, "a")
  if f then
    f:write "# health probe\n"
    f:close()
    print_line("prom_path", "rw_ok")
  else
    print_line("prom_path", "write_failed")
  end
end

local function check_manifest()
  local tx = os.getenv "SCHEMA_MANIFEST_TX" or ""
  local hash = os.getenv "SCHEMA_HASH" or ""
  local trust_tx = os.getenv "TRUST_MANIFEST_TX" or ""
  if tx ~= "" then
    print_line("schema_manifest_tx", tx)
  end
  if hash ~= "" then
    print_line("schema_hash", hash)
  end
  if trust_tx ~= "" then
    print_line("trust_manifest_tx", trust_tx)
  end
end

check_rate_db()
check_prom()
check_manifest()
print_line("health", "ok")
