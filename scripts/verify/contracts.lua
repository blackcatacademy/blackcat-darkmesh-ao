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
  registry.route(with_req({ Action = "RegisterSite", ["Site-Id"] = "site-1", Config = { version = "v1" }, ["Actor-Role"] = "admin" }))
  local bind = registry.route(with_req({ Action = "BindDomain", ["Site-Id"] = "site-1", Host = "example.com", ["Actor-Role"] = "registry-admin" }))
  assert_eq(bind.status, "OK", "bind status")
  local lookup = registry.route(with_req({ Action = "GetSiteByHost", Host = "example.com" }))
  assert_eq(lookup.status, "OK", "get site by host status")
  assert_eq(lookup.payload.siteId, "site-1", "domain->siteId")

  -- conflict: binding another site to same host should overwrite in stub (and keep deterministic)
  registry.route(with_req({ Action = "RegisterSite", ["Site-Id"] = "site-2", ["Actor-Role"] = "admin" }))
  local rebind = registry.route(with_req({ Action = "BindDomain", ["Site-Id"] = "site-2", Host = "example.com", ["Actor-Role"] = "registry-admin" }))
  assert_status(rebind, "OK", "rebind status")
  local lookup2 = registry.route(with_req({ Action = "GetSiteByHost", Host = "example.com" }))
  assert_eq(lookup2.payload.siteId, "site-2", "rebinding took effect")

  -- forbidden bind
  local denied = registry.route(with_req({ Action = "BindDomain", ["Site-Id"] = "site-2", Host = "forbidden.com", ["Actor-Role"] = "viewer" }))
  assert_status(denied, "ERROR", "bind forbidden status")
  assert_code(denied, "FORBIDDEN", "bind forbidden code")

  -- ExpectedVersion conflict
  registry.route(with_req({ Action = "SetActiveVersion", ["Site-Id"] = "site-2", Version = "v10", ["Actor-Role"] = "registry-admin" }))
  local conflict = registry.route(with_req({ Action = "SetActiveVersion", ["Site-Id"] = "site-2", Version = "v11", ExpectedVersion = "v0", ["Actor-Role"] = "registry-admin" }))
  assert_status(conflict, "ERROR", "set version conflict status")
  assert_code(conflict, "VERSION_CONFLICT", "set version conflict code")

  -- Idempotent bind (same Request-Id)
  local idem_req = { Action = "BindDomain", ["Site-Id"] = "site-2", Host = "example.com", ["Actor-Role"] = "registry-admin", ["Request-Id"] = "rid-bind" }
  local b1 = registry.route(idem_req)
  local b2 = registry.route(idem_req)
  assert_eq(b1.payload.host, b2.payload.host, "idempotent bind host")

  -- Conflict on GrantRole with different payload same Request-Id returns original
  local g1 = registry.route({ Action = "GrantRole", ["Site-Id"] = "site-2", Subject = "userA", Role = "editor", ["Actor-Role"] = "registry-admin", ["Request-Id"] = "rid-grant" })
  local g2 = registry.route({ Action = "GrantRole", ["Site-Id"] = "site-2", Subject = "userA", Role = "admin", ["Actor-Role"] = "registry-admin", ["Request-Id"] = "rid-grant" })
  assert_eq(g2.payload.role, g1.payload.role, "grant role idempotent keeps first role")

  -- Unexpected field in register
  local extra = registry.route(with_req({ Action = "RegisterSite", ["Site-Id"] = "site-extra", Config = {}, Foo = "bar", ["Actor-Role"] = "admin" }))
  assert_status(extra, "ERROR", "register extra status")
  assert_code(extra, "UNSUPPORTED_FIELD", "register extra code")
end

-- Site tests
do
  local site = require("ao.site.process")
  site.route(with_req({ Action = "PutDraft", ["Site-Id"] = "site-1", ["Page-Id"] = "home", Content = { title = "Hello" }, ["Actor-Role"] = "editor" }))
  site.route(with_req({ Action = "UpsertRoute", ["Site-Id"] = "site-1", Path = "/", ["Page-Id"] = "home", ["Layout-Id"] = "layout-1", ["Actor-Role"] = "editor" }))
  local publish = site.route(with_req({ Action = "PublishVersion", ["Site-Id"] = "site-1", Version = "v2", ["Actor-Role"] = "publisher" }))
  assert_eq(publish.status, "OK", "publish status")
  assert_truthy(publish.payload.manifestTx, "publish manifestTx")
  assert_truthy(publish.payload.manifestHash, "publish manifestHash")
  local ar = require("ao.shared.arweave")
  local ok_hash = ar.verify_snapshot(publish.payload.manifestTx, publish.payload.manifestHash)
  assert_truthy(ok_hash, "verify manifest hash")
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

  local publish_empty = site.route(with_req({ Action = "PublishVersion", ["Site-Id"] = "site-2", Version = "v1", ["Actor-Role"] = "publisher" }))
  assert_eq(publish_empty.status, "OK", "publish empty status")
  assert_falsy(publish_empty.payload.manifestTx, "publish empty manifest")

  -- Archive then fetch
  site.route(with_req({ Action = "PutDraft", ["Site-Id"] = "site-2", ["Page-Id"] = "old", Content = { title = "Old" }, ["Actor-Role"] = "editor" }))
  site.route(with_req({ Action = "PublishVersion", ["Site-Id"] = "site-2", Version = "v1", ["Actor-Role"] = "publisher" }))
  site.route(with_req({ Action = "ArchivePage", ["Site-Id"] = "site-2", ["Page-Id"] = "old", ["Actor-Role"] = "publisher" }))
  local archived = site.route(with_req({ Action = "GetPage", ["Site-Id"] = "site-2", ["Page-Id"] = "old" }))
  assert_status(archived, "ERROR", "archived page status")
  assert_code(archived, "NOT_FOUND", "archived page code")

  -- Unexpected field
  local extra = site.route(with_req({ Action = "PutDraft", ["Site-Id"] = "site-1", ["Page-Id"] = "x", Content = { title = "X" }, Foo = "bar", ["Actor-Role"] = "editor" }))
  assert_status(extra, "ERROR", "putdraft extra status")
  assert_code(extra, "UNSUPPORTED_FIELD", "putdraft extra code")

  -- Forbidden publish
  local denied = site.route(with_req({ Action = "PublishVersion", ["Site-Id"] = "site-3", Version = "v1", ["Actor-Role"] = "viewer" }))
  assert_status(denied, "ERROR", "publish forbidden status")
  assert_code(denied, "FORBIDDEN", "publish forbidden code")

  -- Idempotent publish with same Request-Id returns same manifest
  local req = { Action = "PublishVersion", ["Site-Id"] = "site-4", Version = "v1", ["Actor-Role"] = "publisher", ["Request-Id"] = "rid-same" }
  local first = site.route(req)
  local second = site.route(req)
  assert_eq(first.payload.manifestTx, second.payload.manifestTx, "idempotent publish manifest")

  -- Conflicting publish payload (different drafts) same Request-Id keeps original
  site.route(with_req({ Action = "PutDraft", ["Site-Id"] = "site-4", ["Page-Id"] = "extra", Content = { title = "Extra" }, ["Actor-Role"] = "editor", ["Request-Id"] = "rid-extra1" }))
  local third = site.route(req)
  assert_eq(third.payload.manifestTx, first.payload.manifestTx, "idempotent publish ignores new drafts")

  -- ExpectedVersion conflict on publish
  local conflict = site.route(with_req({ Action = "PublishVersion", ["Site-Id"] = "site-1", Version = "v3", ExpectedVersion = "old", ["Actor-Role"] = "publisher" }))
  assert_status(conflict, "ERROR", "publish expected version conflict")
  assert_code(conflict, "VERSION_CONFLICT", "publish conflict code")
end

-- Catalog tests
do
  local catalog = require("ao.catalog.process")
  for i = 1, 60 do
    catalog.route(with_req({ Action = "UpsertProduct", ["Site-Id"] = "site-1", Sku = "sku-" .. i, Payload = { name = "Prod" .. i }, ["Actor-Role"] = "catalog-admin" }))
  end
  catalog.route(with_req({ Action = "UpsertCategory", ["Site-Id"] = "site-1", ["Category-Id"] = "cat-1", Products = { "sku-1", "sku-2", "sku-3", "sku-55" }, ["Actor-Role"] = "catalog-admin" }))
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

  -- page bounds
  local capped = catalog.route(with_req({ Action = "ListCategoryProducts", ["Site-Id"] = "site-1", ["Category-Id"] = "cat-1", Page = -1, PageSize = 500 }))
  assert_eq(capped.payload.page, 1, "page capped to 1")
  assert_eq(capped.payload.pageSize, 200, "pageSize capped to 200")

  local missing = catalog.route(with_req({ Action = "GetProduct", ["Site-Id"] = "site-1", Sku = "nope" }))
  assert_eq(missing.status, "ERROR", "missing product status")
  assert_code(missing, "NOT_FOUND", "missing product code")

  -- search miss
  local search_miss = catalog.route(with_req({ Action = "SearchCatalog", ["Site-Id"] = "site-1", Query = "zzz" }))
  assert_eq(search_miss.payload.total, 0, "search miss total")

  -- Forbidden upsert
  local denied = catalog.route(with_req({ Action = "UpsertProduct", ["Site-Id"] = "site-1", Sku = "bad", Payload = {}, ["Actor-Role"] = "viewer" }))
  assert_status(denied, "ERROR", "catalog forbidden status")
  assert_code(denied, "FORBIDDEN", "catalog forbidden code")

  -- Idempotent upsert
  local idem_req = { Action = "UpsertProduct", ["Site-Id"] = "site-1", Sku = "sku-idem", Payload = {}, ["Actor-Role"] = "catalog-admin", ["Request-Id"] = "rid-upsert" }
  local first = catalog.route(idem_req)
  local second = catalog.route(idem_req)
  assert_eq(first.payload.sku, second.payload.sku, "idempotent upsert sku")

  -- Conflicting payload same Request-Id keeps original
  local conflict = catalog.route({ Action = "UpsertProduct", ["Site-Id"] = "site-1", Sku = "sku-idem", Payload = { name = "changed" }, ["Actor-Role"] = "catalog-admin", ["Request-Id"] = "rid-upsert" })
  assert_status(conflict, "OK", "idempotent conflict status")
  assert_eq(conflict.payload.sku, first.payload.sku, "idempotent conflict ignores change")

  -- ExpectedVersion conflict on PublishCatalogVersion
  catalog.route(with_req({ Action = "PublishCatalogVersion", ["Site-Id"] = "site-1", Version = "cat-v1", ["Actor-Role"] = "catalog-admin" }))
  local publish_conflict = catalog.route(with_req({ Action = "PublishCatalogVersion", ["Site-Id"] = "site-1", Version = "cat-v2", ExpectedVersion = "different", ["Actor-Role"] = "catalog-admin" }))
  assert_status(publish_conflict, "ERROR", "catalog publish conflict status")
  assert_code(publish_conflict, "VERSION_CONFLICT", "catalog publish conflict code")

  -- Unexpected field in upsert
  local extra = catalog.route(with_req({ Action = "UpsertProduct", ["Site-Id"] = "site-1", Sku = "sku-extra", Payload = {}, Extra = true, ["Actor-Role"] = "catalog-admin" }))
  assert_status(extra, "ERROR", "upsert extra status")
  assert_code(extra, "UNSUPPORTED_FIELD", "upsert extra code")
end

-- Access tests
do
  local access = require("ao.access.process")
  access.route(with_req({ Action = "GrantEntitlement", Subject = "user-1", Asset = "asset-1", Policy = "view", ["Actor-Role"] = "admin" }))
  access.route(with_req({ Action = "PutProtectedAssetRef", Asset = "asset-1", Ref = "ar://tx123", Visibility = "protected", ["Actor-Role"] = "access-admin" }))
  local check = access.route(with_req({ Action = "HasEntitlement", Subject = "user-1", Asset = "asset-1" }))
  assert_eq(check.status, "OK", "has entitlement status")
  assert_truthy(check.payload.hasEntitlement, "entitlement flag")
  local asset = access.route(with_req({ Action = "GetProtectedAssetRef", Asset = "asset-1" }))
  assert_eq(asset.status, "OK", "get asset ref")
  assert_eq(asset.payload.ref, "ar://tx123", "asset ref matches")

  access.route(with_req({ Action = "RevokeEntitlement", Subject = "user-1", Asset = "asset-1", ["Actor-Role"] = "admin" }))
  local check2 = access.route(with_req({ Action = "HasEntitlement", Subject = "user-1", Asset = "asset-1" }))
  assert_eq(check2.status, "OK", "has entitlement revoked status")
  assert_falsy(check2.payload.hasEntitlement, "entitlement revoked flag")

  local missing_asset = access.route(with_req({ Action = "GetProtectedAssetRef", Asset = "asset-missing" }))
  assert_eq(missing_asset.status, "ERROR", "missing asset ref status")
  assert_code(missing_asset, "NOT_FOUND", "missing asset ref code")

  -- revoke idempotency
  local rev2 = access.route(with_req({ Action = "RevokeEntitlement", Subject = "user-1", Asset = "asset-1", ["Actor-Role"] = "admin" }))
  assert_status(rev2, "OK", "second revoke status")

  -- Forbidden grant
  local deny = access.route(with_req({ Action = "GrantEntitlement", Subject = "u2", Asset = "asset-2", Policy = "view", ["Actor-Role"] = "viewer" }))
  assert_status(deny, "ERROR", "grant forbidden status")
  assert_code(deny, "FORBIDDEN", "grant forbidden code")

  -- Idempotent grant (same Request-Id returns same ref)
  local idem_req = { Action = "GrantEntitlement", Subject = "u3", Asset = "asset-3", Policy = "view", ["Actor-Role"] = "admin", ["Request-Id"] = "idem-grant" }
  local first = access.route(idem_req)
  local second = access.route(idem_req)
  assert_eq(first.payload.subject, second.payload.subject, "idempotent grant subject")

  -- Conflicting payload with same Request-Id returns original
  local conflict = access.route({ Action = "GrantEntitlement", Subject = "u3", Asset = "asset-3", Policy = "edit", ["Actor-Role"] = "admin", ["Request-Id"] = "idem-grant" })
  assert_eq(conflict.payload.policy, first.payload.policy, "idempotent ignores new payload")

  -- Unexpected field in grant
  local extra = access.route(with_req({ Action = "GrantEntitlement", Subject = "u4", Asset = "asset-4", Policy = "view", Foo = "bar", ["Actor-Role"] = "admin" }))
  assert_status(extra, "ERROR", "grant extra status")
  assert_code(extra, "UNSUPPORTED_FIELD", "grant extra code")
end

-- Unknown action test
do
  local registry = require("ao.registry.process")
  local resp = registry.route(with_req({ Action = "NopeAction" }))
  assert_eq(resp.status, "ERROR", "unknown action status")
  assert_code(resp, "UNKNOWN_ACTION", "unknown action code")
end

print("contract tests passed")
