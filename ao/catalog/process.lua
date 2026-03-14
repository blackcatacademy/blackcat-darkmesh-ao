-- Catalog process handlers: products, categories, listings.

local codec = require("ao.shared.codec")
local validation = require("ao.shared.validation")
local ids = require("ao.shared.ids")
local auth = require("ao.shared.auth")
local idem = require("ao.shared.idempotency")

local handlers = {}
local allowed_actions = {
  "GetProduct",
  "ListCategoryProducts",
  "SearchCatalog",
  "UpsertProduct",
  "UpsertCategory",
  "PublishCatalogVersion",
}

local role_policy = {
  UpsertProduct = { "catalog-admin", "editor", "admin" },
  UpsertCategory = { "catalog-admin", "editor", "admin" },
  PublishCatalogVersion = { "publisher", "admin", "catalog-admin" },
}

local state = {
  products = {},      -- product:<site>:<sku> -> { payload }
  categories = {},    -- category:<site>:<id> -> { payload, products = {sku}} 
  active_versions = {} -- site -> version
}

local function ensure(fields, msg)
  for _, f in ipairs(fields) do
    if msg[f] == nil then return false, f end
  end
  return true
end

function handlers.GetProduct(msg)
  local ok, missing = ensure({ "Site-Id", "Sku" }, msg)
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local key = ids.product_key(msg["Site-Id"], msg.Sku)
  local product = state.products[key]
  if not product then
    return codec.error("NOT_FOUND", "Product not found", { sku = msg.Sku })
  end
  return codec.ok({
    siteId = msg["Site-Id"],
    sku = msg.Sku,
    payload = product.payload,
    version = product.version or state.active_versions[msg["Site-Id"]] or "active",
  })
end

function handlers.ListCategoryProducts(msg)
  local ok, missing = ensure({ "Site-Id", "Category-Id" }, msg)
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local key = ids.category_key(msg["Site-Id"], msg["Category-Id"])
  local category = state.categories[key]
  if not category then
    return codec.error("NOT_FOUND", "Category not found", { category = msg["Category-Id"] })
  end
  local page = msg.Page or 1
  local page_size = msg.PageSize or 50
  local start = (page - 1) * page_size + 1
  local finish = start + page_size - 1
  local products = {}
  for i = start, math.min(finish, #category.products) do
    local sku = category.products[i]
    local pkey = ids.product_key(msg["Site-Id"], sku)
    if state.products[pkey] then
      table.insert(products, { sku = sku, payload = state.products[pkey].payload })
    end
  end
  return codec.ok({
    siteId = msg["Site-Id"],
    categoryId = msg["Category-Id"],
    page = page,
    pageSize = page_size,
    items = products,
    total = #category.products,
  })
end

function handlers.SearchCatalog(msg)
  local ok, missing = ensure({ "Site-Id" }, msg)
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local q = msg.Query and msg.Query:lower() or ""
  local results = {}
  local prefix = "product:" .. msg["Site-Id"] .. ":"
  for key, product in pairs(state.products) do
    if key:sub(1, #prefix) == prefix then
      local sku = key:match("product:[^:]+:(.+)")
      local text = (product.payload.name or ""):lower() .. " " .. (product.payload.description or ""):lower()
      if q == "" or text:find(q, 1, true) then
        table.insert(results, { sku = sku, payload = product.payload })
      end
    end
  end
  return codec.ok({
    siteId = msg["Site-Id"],
    query = q,
    items = results,
    total = #results,
  })
end

function handlers.UpsertProduct(msg)
  local ok, missing = ensure({ "Site-Id", "Sku", "Payload" }, msg)
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local key = ids.product_key(msg["Site-Id"], msg.Sku)
  state.products[key] = { payload = msg.Payload, version = msg.Version }
  return codec.ok({ sku = msg.Sku })
end

function handlers.UpsertCategory(msg)
  local ok, missing = ensure({ "Site-Id", "Category-Id" }, msg)
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local key = ids.category_key(msg["Site-Id"], msg["Category-Id"])
  state.categories[key] = {
    payload = msg.Payload or {},
    products = msg.Products or state.categories[key] and state.categories[key].products or {},
  }
  return codec.ok({ categoryId = msg["Category-Id"] })
end

function handlers.PublishCatalogVersion(msg)
  local ok, missing = ensure({ "Site-Id", "Version" }, msg)
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  state.active_versions[msg["Site-Id"]] = msg.Version
  return codec.ok({ siteId = msg["Site-Id"], activeVersion = msg.Version })
end

local function route(msg)
  local ok, missing = validation.require_tags(msg, { "Action" })
  if not ok then
    return codec.missing_tags(missing)
  end

  local seen = idem.check(msg["Request-Id"])
  if seen then return seen end

  local ok_action, err = validation.require_action(msg, allowed_actions)
  if not ok_action then
    if err == "unknown_action" then
      return codec.unknown_action(msg.Action)
    end
    return codec.error("MISSING_ACTION", "Action is required")
  end

  local ok_role, role_err = auth.require_role_for_action(msg, role_policy)
  if not ok_role then
    return codec.error("FORBIDDEN", role_err)
  end

  local handler = handlers[msg.Action]
  if not handler then
    return codec.unknown_action(msg.Action)
  end

  local resp = handler(msg)
  idem.record(msg["Request-Id"], resp)
  return resp
end

return {
  route = route,
  _state = state,
}
