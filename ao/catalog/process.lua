-- Catalog process handlers: products, categories, listings.

local codec = require("ao.shared.codec")
local validation = require("ao.shared.validation")
local ids = require("ao.shared.ids")
local auth = require("ao.shared.auth")
local idem = require("ao.shared.idempotency")
local audit = require("ao.shared.audit")
local schema = require("ao.shared.schema")
local metrics = require("ao.shared.metrics")
local metrics = require("ao.shared.metrics")

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

local MAX_PAYLOAD_BYTES = tonumber(os.getenv("CATALOG_MAX_PAYLOAD_BYTES") or "") or (64 * 1024)

function handlers.GetProduct(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Sku" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Sku", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" }) end
  local ok_len_sku, err_sku = validation.check_length(msg.Sku, 128, "Sku")
  if not ok_len_sku then return codec.error("INVALID_INPUT", err_sku, { field = "Sku" }) end
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
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Category-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Category-Id", "Page", "PageSize", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" }) end
  local ok_len_cat, err_cat = validation.check_length(msg["Category-Id"], 128, "Category-Id")
  if not ok_len_cat then return codec.error("INVALID_INPUT", err_cat, { field = "Category-Id" }) end
  local key = ids.category_key(msg["Site-Id"], msg["Category-Id"])
  local category = state.categories[key]
  if not category then
    return codec.error("NOT_FOUND", "Category not found", { category = msg["Category-Id"] })
  end
  local page = msg.Page or 1
  local page_size = msg.PageSize or 50
  if page < 1 then page = 1 end
  if page_size < 1 then page_size = 1 end
  if page_size > 200 then page_size = 200 end
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
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Query", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" }) end
  if msg.Query then
    local ok_len_query, err_query = validation.check_length(msg.Query, 1024, "Query")
    if not ok_len_query then return codec.error("INVALID_INPUT", err_query, { field = "Query" }) end
  end
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
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Sku", "Payload" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Sku", "Payload", "Version", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" }) end
  local ok_len_sku, err_sku = validation.check_length(msg.Sku, 128, "Sku")
  if not ok_len_sku then return codec.error("INVALID_INPUT", err_sku, { field = "Sku" }) end
  if msg.Version then
    local ok_len_ver, err_ver = validation.check_length(msg.Version, 128, "Version")
    if not ok_len_ver then return codec.error("INVALID_INPUT", err_ver, { field = "Version" }) end
  end
  local ok_type_payload, err_type_payload = validation.assert_type(msg.Payload, "table", "Payload")
  if not ok_type_payload then return codec.error("INVALID_INPUT", err_type_payload, { field = "Payload" }) end
  if not msg.Payload.sku then msg.Payload.sku = msg.Sku end
  if msg.Payload.sku ~= msg.Sku then
    return codec.error("INVALID_INPUT", "Payload sku must match Sku field", { field = "Sku" })
  end
  local payload_len = validation.estimate_json_length(msg.Payload)
  local ok_size, err_size = validation.check_size(payload_len, MAX_PAYLOAD_BYTES, "Payload")
  if not ok_size then return codec.error("INVALID_INPUT", err_size, { field = "Payload" }) end
  local ok_schema, schema_err = schema.validate("product", msg.Payload)
  if not ok_schema then return codec.error("INVALID_INPUT", "Payload failed schema", { errors = schema_err }) end
  local key = ids.product_key(msg["Site-Id"], msg.Sku)
  state.products[key] = { payload = msg.Payload, version = msg.Version }
  audit.record("catalog", "UpsertProduct", msg, nil, { sku = msg.Sku })
  return codec.ok({ sku = msg.Sku })
end

function handlers.UpsertCategory(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Category-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Category-Id", "Payload", "Products", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" }) end
  local ok_len_cat, err_cat = validation.check_length(msg["Category-Id"], 128, "Category-Id")
  if not ok_len_cat then return codec.error("INVALID_INPUT", err_cat, { field = "Category-Id" }) end
  if msg.Payload then
    local ok_type_payload, err_type_payload = validation.assert_type(msg.Payload, "table", "Payload")
    if not ok_type_payload then return codec.error("INVALID_INPUT", err_type_payload, { field = "Payload" }) end
  end
  if msg.Products then
    local ok_type_products, err_type_products = validation.assert_type(msg.Products, "table", "Products")
    if not ok_type_products then return codec.error("INVALID_INPUT", err_type_products, { field = "Products" }) end
  end
  local payload_len = validation.estimate_json_length(msg.Payload or {})
  local ok_size, err_size = validation.check_size(payload_len, MAX_PAYLOAD_BYTES, "Payload")
  if not ok_size then return codec.error("INVALID_INPUT", err_size, { field = "Payload" }) end
  local key = ids.category_key(msg["Site-Id"], msg["Category-Id"])
  state.categories[key] = {
    payload = msg.Payload or {},
    products = msg.Products or state.categories[key] and state.categories[key].products or {},
  }
  return codec.ok({ categoryId = msg["Category-Id"] })
end

function handlers.PublishCatalogVersion(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Version" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Version", "ExpectedVersion", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" }) end
  local ok_len_ver, err_ver = validation.check_length(msg.Version, 128, "Version")
  if not ok_len_ver then return codec.error("INVALID_INPUT", err_ver, { field = "Version" }) end
  if msg.ExpectedVersion then
    local ok_len_exp, err_exp = validation.check_length(msg.ExpectedVersion, 128, "ExpectedVersion")
    if not ok_len_exp then return codec.error("INVALID_INPUT", err_exp, { field = "ExpectedVersion" }) end
  end
  local current = state.active_versions[msg["Site-Id"]]
  if msg.ExpectedVersion and current and current ~= msg.ExpectedVersion then
    return codec.error("VERSION_CONFLICT", "ExpectedVersion mismatch", { expected = msg.ExpectedVersion, current = current })
  end
  state.active_versions[msg["Site-Id"]] = msg.Version
  local resp = codec.ok({ siteId = msg["Site-Id"], activeVersion = msg.Version })
  audit.record("catalog", "PublishCatalogVersion", msg, resp)
  return resp
end

local function route(msg)
  local ok, missing = validation.require_tags(msg, { "Action" })
  if not ok then
    return codec.missing_tags(missing)
  end

  local ok_sec, sec_err = auth.enforce(msg)
  if not ok_sec then
    return codec.error("FORBIDDEN", sec_err)
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
  metrics.inc("catalog." .. msg.Action .. ".count")
  metrics.tick()
  idem.record(msg["Request-Id"], resp)
  return resp
end

return {
  route = route,
  _state = state,
}
