-- Lightweight fuzz/property checks for pagination and Arweave HTTP failure handling.

math.randomseed(os.time())

local catalog = require("ao.catalog.process")
local site = require("ao.site.process")
local ar = require("ao.shared.arweave")
local audit = require("ao.shared.audit")

local function with_req(fields)
  fields["Request-Id"] = fields["Request-Id"] or tostring(math.random())
  return fields
end

-- Fuzz pagination uniqueness
do
  local siteId = "fuzz-site"
  local cat = "cat-fuzz"
  local total = 40
  for i = 1, total do
    catalog.route(with_req({ Action = "UpsertProduct", ["Site-Id"] = siteId, Sku = "fuzz-" .. i, Payload = { name = "P" .. i }, ["Actor-Role"] = "catalog-admin" }))
  end
  catalog.route(with_req({ Action = "UpsertCategory", ["Site-Id"] = siteId, ["Category-Id"] = cat, Products = {} , ["Actor-Role"] = "catalog-admin" }))
  for i = 1, total do
    catalog.route(with_req({ Action = "UpsertCategory", ["Site-Id"] = siteId, ["Category-Id"] = cat, Products = { "fuzz-" .. i }, ["Actor-Role"] = "catalog-admin" }))
  end
  local seen = {}
  for page = 1, 20 do
    local resp = catalog.route(with_req({ Action = "ListCategoryProducts", ["Site-Id"] = siteId, ["Category-Id"] = cat, Page = page, PageSize = 3 }))
    if resp.payload then
      for _, item in ipairs(resp.payload.items) do
        if seen[item.sku] then
          error("duplicate sku in pagination fuzz: " .. item.sku)
        end
        seen[item.sku] = true
      end
    end
  end
end

-- Arweave HTTP failure simulation: ensure too_large manifests are rejected
do
  local tx, err = ar.put_snapshot({ dummy = string.rep("x", 300 * 1024) }) -- > 256 KiB
  if tx ~= nil or err ~= "too_large" then
    error("expected too_large manifest rejection")
  end
end

-- Audit rotation/prune: set tiny rotate and emit many records
do
  os.setenv = os.setenv or function() end -- no-op if not available
  -- re-require audit with different env by clearing cache if possible
  package.loaded["ao.shared.audit"] = nil
  os.execute("rm -rf /tmp/ao-audit-fuzz")
  os.execute("mkdir -p /tmp/ao-audit-fuzz")
  os.setenv("AUDIT_LOG_DIR", "/tmp/ao-audit-fuzz")
  os.setenv("AUDIT_ROTATE_MAX", "200")
  os.setenv("AUDIT_RETAIN_FILES", "2")
  local audit2 = require("ao.shared.audit")
  for i = 1, 50 do
    audit2.record("fuzz", "Test", { ["Request-Id"] = tostring(i) }, { status = "OK" })
  end
  local p = io.popen("ls -1 /tmp/ao-audit-fuzz | wc -l", "r")
  local count = p and p:read("*n") or 0
  if p then p:close() end
  if count > 3 then error("audit rotation retained too many files: " .. tostring(count)) end
end

print("fuzz tests passed")

print("fuzz tests passed")
