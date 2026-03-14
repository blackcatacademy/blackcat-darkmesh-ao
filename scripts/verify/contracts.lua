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

local function assert_falsy(val, label)
  if val then error(label .. " expected falsy") end
end

local function assert_code(resp, code, label)
  assert_eq(resp.code, code, label .. " code")
end

local function assert_status(resp, status, label)
  assert_eq(resp.status, status, label .. " status")
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

  -- conflict: binding another site to same host should overwrite in stub (and keep deterministic)
  registry.route(with_req({ Action = "RegisterSite", ["Site-Id"] = "site-2" }))
  local rebind = registry.route(with_req({ Action = "BindDomain", ["Site-Id"] = "site-2", Host = "example.com" }))
  assert_status(rebind, "OK", "rebind status")
  local lookup2 = registry.route(with_req({ Action = "GetSiteByHost", Host = "example.com" }))
  assert_eq(lookup2.payload.siteId, "site-2", "rebinding took effect")
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

-- Site edge cases
do
  local site = require("ao.site.process")
  local missing = site.route(with_req({ Action = "ResolveRoute", ["Site-Id"] = "site-1" })) -- missing Path
  assert_eq(missing.status, "ERROR", "missing field status")
  assert_code(missing, "INVALID_INPUT", "missing field code")

  local notfound = site.route(with_req({ Action = "ResolveRoute", ["Site-Id"] = "site-1", Path = "/nope" }))
  assert_eq(notfound.status, "ERROR", "resolve unknown status")
  assert_code(notfound, "NOT_FOUND", "resolve unknown code")

  local publish_empty = site.route(with_req({ Action = "PublishVersion", ["Site-Id"] = "site-2", Version = "v1" }))
  assert_eq(publish_empty.status, "OK", "publish empty status")
  assert_falsy(publish_empty.payload.manifestTx, "publish empty manifest")

  -- Archive then fetch
  site.route(with_req({ Action = "PutDraft", ["Site-Id"] = "site-2", ["Page-Id"] = "old", Content = { title = "Old" } }))
  site.route(with_req({ Action = "PublishVersion", ["Site-Id"] = "site-2", Version = "v1" }))
  site.route(with_req({ Action = "ArchivePage", ["Site-Id"] = "site-2", ["Page-Id"] = "old" }))
  local archived = site.route(with_req({ Action = "GetPage", ["Site-Id"] = "site-2", ["Page-Id"] = "old" }))
  assert_status(archived, "ERROR", "archived page status")
  assert_code(archived, "NOT_FOUND", "archived page code")
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

  local paged_out = catalog.route(with_req({ Action = "ListCategoryProducts", ["Site-Id"] = "site-1", ["Category-Id"] = "cat-1", Page = 10, PageSize = 10 }))
  assert_eq(#paged_out.payload.items, 0, "empty page")

  local missing = catalog.route(with_req({ Action = "GetProduct", ["Site-Id"] = "site-1", Sku = "nope" }))
  assert_eq(missing.status, "ERROR", "missing product status")
  assert_code(missing, "NOT_FOUND", "missing product code")

  -- search miss
  local search_miss = catalog.route(with_req({ Action = "SearchCatalog", ["Site-Id"] = "site-1", Query = "zzz" }))
  assert_eq(search_miss.payload.total, 0, "search miss total")
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

  access.route(with_req({ Action = "RevokeEntitlement", Subject = "user-1", Asset = "asset-1" }))
  local check2 = access.route(with_req({ Action = "HasEntitlement", Subject = "user-1", Asset = "asset-1" }))
  assert_eq(check2.status, "OK", "has entitlement revoked status")
  assert_falsy(check2.payload.hasEntitlement, "entitlement revoked flag")

  local missing_asset = access.route(with_req({ Action = "GetProtectedAssetRef", Asset = "asset-missing" }))
  assert_eq(missing_asset.status, "ERROR", "missing asset ref status")
  assert_code(missing_asset, "NOT_FOUND", "missing asset ref code")

  -- revoke idempotency
  local rev2 = access.route(with_req({ Action = "RevokeEntitlement", Subject = "user-1", Asset = "asset-1" }))
  assert_status(rev2, "OK", "second revoke status")
end

-- Unknown action test
do
  local registry = require("ao.registry.process")
  local resp = registry.route(with_req({ Action = "NopeAction" }))
  assert_eq(resp.status, "ERROR", "unknown action status")
  assert_code(resp, "UNKNOWN_ACTION", "unknown action code")
end

print("contract tests passed")
