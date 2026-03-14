-- Lightweight fuzz/property checks for pagination and Arweave HTTP failure handling.

math.randomseed(os.time())

local catalog = require("ao.catalog.process")
local site = require("ao.site.process")
local ar = require("ao.shared.arweave")

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

print("fuzz tests passed")
