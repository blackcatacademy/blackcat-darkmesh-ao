-- Lightweight contract smoke tests for AO handlers.
-- Uses in-memory handler state; ensures deterministic behavior matches contracts.

local function assert_eq(actual, expected, label)
  if actual ~= expected then
    error(string.format("%s expected %s, got %s", label, tostring(expected), tostring(actual)))
  end
end

local function assert_truthy(val, label)
  if not val then error(label .. " expected truthy") end
end

local function with_req(fields)
  fields["Request-Id"] = fields["Request-Id"] or tostring(math.random())
  return fields
end

-- Registry tests
do
  local registry = require("ao.registry.process")
  registry.route(with_req({ Action = "RegisterSite", ["Site-Id"] = "site-1", Config = { version = "v1" } }))
  local bind = registry.route(with_req({ Action = "BindDomain", ["Site-Id"] = "site-1", Host = "example.com" }))
  assert_eq(bind.status, "OK", "bind status")
  local lookup = registry.route(with_req({ Action = "GetSiteByHost", Host = "example.com" }))
  assert_eq(lookup.status, "OK", "get site by host status")
  assert_eq(lookup.payload.siteId, "site-1", "domain->siteId")
end

-- Site tests
do
  local site = require("ao.site.process")
  site.route(with_req({ Action = "PutDraft", ["Site-Id"] = "site-1", ["Page-Id"] = "home", Content = { title = "Hello" } }))
  site.route(with_req({ Action = "UpsertRoute", ["Site-Id"] = "site-1", Path = "/", ["Page-Id"] = "home", ["Layout-Id"] = "layout-1" }))
  local publish = site.route(with_req({ Action = "PublishVersion", ["Site-Id"] = "site-1", Version = "v2" }))
  assert_eq(publish.status, "OK", "publish status")
  assert_truthy(publish.payload.manifestTx, "publish manifestTx")
  assert_truthy(publish.payload.manifestHash, "publish manifestHash")
  local route = site.route(with_req({ Action = "ResolveRoute", ["Site-Id"] = "site-1", Path = "/" }))
  assert_eq(route.status, "OK", "resolve route status")
  local page = site.route(with_req({ Action = "GetPage", ["Site-Id"] = "site-1", ["Page-Id"] = "home" }))
  assert_eq(page.status, "OK", "get page status")
  assert_eq(page.payload.version, "v2", "page version active")
end

-- Catalog tests
do
  local catalog = require("ao.catalog.process")
  for i = 1, 60 do
    catalog.route(with_req({ Action = "UpsertProduct", ["Site-Id"] = "site-1", Sku = "sku-" .. i, Payload = { name = "Prod" .. i } }))
  end
  catalog.route(with_req({ Action = "UpsertCategory", ["Site-Id"] = "site-1", ["Category-Id"] = "cat-1", Products = { "sku-1", "sku-2", "sku-3", "sku-55" } }))
  local product = catalog.route(with_req({ Action = "GetProduct", ["Site-Id"] = "site-1", Sku = "sku-1" }))
  assert_eq(product.status, "OK", "get product status")
  local listing = catalog.route(with_req({ Action = "ListCategoryProducts", ["Site-Id"] = "site-1", ["Category-Id"] = "cat-1" }))
  assert_eq(listing.status, "OK", "list category products")
  assert_eq(listing.payload.total, 4, "category total")
  local search = catalog.route(with_req({ Action = "SearchCatalog", ["Site-Id"] = "site-1", Query = "Prod" }))
  assert_eq(search.status, "OK", "search status")
  assert_eq(search.payload.total, 60, "search total")
  local paged = catalog.route(with_req({ Action = "ListCategoryProducts", ["Site-Id"] = "site-1", ["Category-Id"] = "cat-1", Page = 2, PageSize = 2 }))
  assert_eq(paged.payload.page, 2, "second page number")
  assert_eq(#paged.payload.items, 2, "second page size")
end

-- Access tests
do
  local access = require("ao.access.process")
  access.route(with_req({ Action = "GrantEntitlement", Subject = "user-1", Asset = "asset-1", Policy = "view" }))
  access.route(with_req({ Action = "PutProtectedAssetRef", Asset = "asset-1", Ref = "ar://tx123", Visibility = "protected" }))
  local check = access.route(with_req({ Action = "HasEntitlement", Subject = "user-1", Asset = "asset-1" }))
  assert_eq(check.status, "OK", "has entitlement status")
  assert_truthy(check.payload.hasEntitlement, "entitlement flag")
  local asset = access.route(with_req({ Action = "GetProtectedAssetRef", Asset = "asset-1" }))
  assert_eq(asset.status, "OK", "get asset ref")
  assert_eq(asset.payload.ref, "ar://tx123", "asset ref matches")
end

print("contract tests passed")
