-- Catalog process handlers: products, categories, listings.

local codec = require("ao.shared.codec")
local validation = require("ao.shared.validation")
local ids = require("ao.shared.ids")
local auth = require("ao.shared.auth")
local idem = require("ao.shared.idempotency")
local audit = require("ao.shared.audit")
local schema = require("ao.shared.schema")
local metrics = require("ao.shared.metrics")
local json_ok, cjson = pcall(require, "cjson.safe")

local handlers = {}
local allowed_actions = {
  "GetProduct",
  "ListCategoryProducts",
  "SearchCatalog",
  "GetOrder",
  "ListOrders",
  "ApplyOrderEvent",
  "UpsertProduct",
  "UpsertCategory",
  "PublishCatalogVersion",
  "SetInventoryReservation",
  "SyncShipment",
  "SyncReturn",
  "ApplyShipmentEvent",
  "ApplyTrackingEvent",
  "GetShippingRates",
  "GetTaxRates",
  "ValidateAddress",
  "GetShipment",
}

local role_policy = {
  UpsertProduct = { "catalog-admin", "editor", "admin" },
  UpsertCategory = { "catalog-admin", "editor", "admin" },
  PublishCatalogVersion = { "publisher", "admin", "catalog-admin" },
  SetInventoryReservation = { "catalog-admin", "admin" },
  SyncShipment = { "catalog-admin", "admin" },
  SyncReturn = { "catalog-admin", "admin" },
  ApplyOrderEvent = { "admin", "catalog-admin" },
  ApplyShipmentEvent = { "admin", "catalog-admin", "support" },
  ApplyTrackingEvent = { "admin", "catalog-admin", "support" },
  GetShippingRates = { "support", "admin", "catalog-admin" },
  GetTaxRates = { "support", "admin", "catalog-admin" },
  ValidateAddress = { "support", "admin" },
  GetShipment = { "support", "admin" },
}

local state = {
  products = {},      -- product:<site>:<sku> -> { payload }
  categories = {},    -- category:<site>:<id> -> { payload, products = {sku}} 
  active_versions = {}, -- site -> version
  inventory = {},       -- site -> sku -> { quantity }
  reservations = {},    -- orderId -> { siteId, items = { { sku, qty } }, released=false }
  orders = {},          -- orderId -> { siteId, status, totals, currency, customerId, coupons, address, shipping, paymentStatus, updatedAt }
  shipments = {},       -- shipmentId -> { status, tracking, carrier, eta, orderId }
  returns = {},         -- returnId -> { status, reason, orderId }
  shipping_rates = {},  -- siteId -> list of rate rows
  tax_rates = {},       -- siteId -> list of tax rows
}

local MAX_PAYLOAD_BYTES = tonumber(os.getenv("CATALOG_MAX_PAYLOAD_BYTES") or "") or (64 * 1024)
local SHIPPING_RATES_PATH = os.getenv("AO_SHIPPING_RATES_PATH")
local TAX_RATES_PATH = os.getenv("AO_TAX_RATES_PATH")

local function load_ndjson(path)
  if not path or path == "" or not json_ok then return {} end
  local f = io.open(path, "r")
  if not f then return {} end
  local out = {}
  for line in f:lines() do
    local ok, obj = pcall(cjson.decode, line)
    if ok and obj then
      table.insert(out, obj)
    end
  end
  f:close()
  return out
end

local function load_rates()
  local ship = load_ndjson(SHIPPING_RATES_PATH)
  for _, r in ipairs(ship) do
    if r.siteId then
      state.shipping_rates[r.siteId] = state.shipping_rates[r.siteId] or {}
      table.insert(state.shipping_rates[r.siteId], r)
    end
  end
  local tax = load_ndjson(TAX_RATES_PATH)
  for _, t in ipairs(tax) do
    if t.siteId then
      state.tax_rates[t.siteId] = state.tax_rates[t.siteId] or {}
      table.insert(state.tax_rates[t.siteId], t)
    end
  end
end

load_rates()

-- tiny Levenshtein for typo tolerance on short queries
local function levenshtein(a, b)
  if not a or not b then return 99 end
  local la, lb = #a, #b
  if la == 0 then return lb end
  if lb == 0 then return la end
  local prev = {}
  for j = 0, lb do prev[j] = j end
  for i = 1, la do
    local cur = {}
    cur[0] = i
    for j = 1, lb do
      local cost = (a:byte(i) == b:byte(j)) and 0 or 1
      cur[j] = math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost)
    end
    prev = cur
  end
  return prev[lb]
end

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
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Query", "MinPrice", "MaxPrice", "Locale", "Available", "Category-Id", "Sort", "Currency", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" }) end
  if msg.Query then
    local ok_len_query, err_query = validation.check_length(msg.Query, 1024, "Query")
    if not ok_len_query then return codec.error("INVALID_INPUT", err_query, { field = "Query" }) end
  end
  local min_price = msg.MinPrice
  local max_price = msg.MaxPrice
  if min_price and type(min_price) ~= "number" then return codec.error("INVALID_INPUT", "MinPrice must be number") end
  if max_price and type(max_price) ~= "number" then return codec.error("INVALID_INPUT", "MaxPrice must be number") end
  local q = msg.Query and msg.Query:lower() or ""
  local sort = msg.Sort or "relevance"
  local results = {}
  local facets = {
    categories = {},
    availability = { available = 0, unavailable = 0 },
    price = { lt25 = 0, lt100 = 0, gte100 = 0 },
    currency = {},
    locales = {},
  }
  local prefix = "product:" .. msg["Site-Id"] .. ":"
  for key, product in pairs(state.products) do
    if key:sub(1, #prefix) == prefix then
      local sku = key:match("product:[^:]+:(.+)")
      local text = (product.payload.name or ""):lower() .. " " .. (product.payload.description or ""):lower()
      local matched = (q == "") or text:find(q, 1, true)
      local fuzzy_hit = false
      if (not matched) and q ~= "" and #q <= 16 then
        local d = levenshtein((product.payload.name or ""):lower(), q)
        fuzzy_hit = d <= 2
      end
      if matched or fuzzy_hit then
        local payload = product.payload or {}
        local price = payload.price
        local locale = payload.locale or payload.Locale
        local available = payload.is_available or payload.available
        if not available and state.inventory[msg["Site-Id"]] then
          local inv = state.inventory[msg["Site-Id"]][sku]
          available = inv and inv.quantity and inv.quantity > 0 or false
        end
        local ok_price = true
        if min_price and price and price < min_price then ok_price = false end
        if max_price and price and price > max_price then ok_price = false end
        local ok_locale = (not msg.Locale) or (locale == msg.Locale)
        local ok_currency = (not msg.Currency) or (payload.currency == msg.Currency)
        local ok_available = (msg.Available == nil) or (available == msg.Available)
        if available then facets.availability.available = facets.availability.available + 1 else facets.availability.unavailable = facets.availability.unavailable + 1 end
        if payload.categoryId then
          facets.categories[payload.categoryId] = (facets.categories[payload.categoryId] or 0) + 1
        end
        local price_num = tonumber(price or 0) or 0
        if price_num < 25 then facets.price.lt25 = facets.price.lt25 + 1
        elseif price_num < 100 then facets.price.lt100 = facets.price.lt100 + 1
        else facets.price.gte100 = facets.price.gte100 + 1 end
        if payload.currency then
          facets.currency[payload.currency] = (facets.currency[payload.currency] or 0) + 1
        end
        if locale then
          facets.locales[locale] = (facets.locales[locale] or 0) + 1
        end
        local ok_cat = (not msg["Category-Id"]) or (payload.categoryId == msg["Category-Id"]) or (payload.category and payload.category.id == msg["Category-Id"]) or false
        if ok_price and ok_locale and ok_currency and ok_available and ok_cat then
          local score = 0
          if q ~= "" then
            if sku:lower():find("^" .. q, 1, false) then score = score + 5 end
            if (payload.name or ""):lower():find(q, 1, true) then score = score + 3 end
            if (payload.description or ""):lower():find(q, 1, true) then score = score + 1 end
            -- typo tolerance for short queries (distance <=1)
            if #q <= 6 then
              local d = levenshtein((payload.name or ""):lower(), q)
              if d == 1 then score = score + 2 end
            end
            if fuzzy_hit then score = score + 1 end
          end
          if msg.Locale and locale == msg.Locale then score = score + 1 end
          table.insert(results, { sku = sku, payload = payload, price = price, name = payload.name or sku, score = score, available = available })
        end
      end
    end
  end
  table.sort(results, function(a, b)
    if sort == "price" or sort == "price_asc" then return (a.price or 0) < (b.price or 0) end
    if sort == "-price" or sort == "price_desc" then return (a.price or 0) > (b.price or 0) end
    if sort == "name" then return tostring(a.name) < tostring(b.name) end
    if sort == "available" then
      if a.available ~= b.available then return a.available and not b.available end
      -- fall back to relevance if availability matches
      if a.score ~= b.score then return (a.score or 0) > (b.score or 0) end
      return (a.price or 0) < (b.price or 0)
    end
    if sort == "newest" then
      return (a.payload.updatedAt or 0) > (b.payload.updatedAt or 0)
    end
    -- default relevance
    if a.score ~= b.score then return (a.score or 0) > (b.score or 0) end
    return (a.price or 0) < (b.price or 0)
  end)
  return codec.ok({
    siteId = msg["Site-Id"],
    query = q,
    items = results,
    total = #results,
    facets = facets,
  })
end

function handlers.ApplyOrderEvent(msg)
  local ok, missing = validation.require_fields(msg, { "Event" })
  if not ok then return codec.error("INVALID_INPUT", "Missing Event", { missing = missing }) end
  local ev = msg.Event
  if type(ev) ~= "table" or not ev.type then
    return codec.error("INVALID_INPUT", "Event.type required")
  end
  -- allow verification with hmac if present
  msg["Order-Id"] = msg["Order-Id"] or ev.orderId or ev["Order-Id"]
  local ok_hmac, hmac_err = auth.verify_outbox_hmac(msg)
  if not ok_hmac then return codec.error("FORBIDDEN", hmac_err) end

  if ev.type == "OrderCreated" then
    state.orders[ev.orderId] = {
      siteId = ev.siteId,
      customerId = ev.customerId,
      currency = ev.currency,
      totals = ev.totals,
      coupon = ev.coupon,
      coupons = ev.coupons,
      vatRate = ev.vatRate,
      shipping = ev.shipping,
      address = ev.address,
      status = ev.status or "pending",
      updatedAt = os.time(),
    }
  elseif ev.type == "OrderStatusUpdated" then
    local o = state.orders[ev.orderId] or { siteId = ev.siteId }
    o.status = ev.status or o.status
    o.reason = ev.reason or o.reason
    o.updatedAt = os.time()
    state.orders[ev.orderId] = o
  elseif ev.type == "PaymentStatusChanged" then
    if ev.orderId then
      local o = state.orders[ev.orderId] or {}
      o.paymentStatus = ev.status or o.paymentStatus
      if ev.status == "disputed" then
        o.status = o.status or "disputed"
      end
      o.updatedAt = os.time()
      state.orders[ev.orderId] = o
    end
  elseif ev.type == "ShipmentTrackingUpdated" then
    state.shipments[ev.shipmentId] = {
      tracking = ev.tracking,
      carrier = ev.carrier,
      eta = ev.eta,
      status = ev.status,
      orderId = ev.orderId,
      updatedAt = os.time(),
    }
  elseif ev.type == "ReturnUpdated" then
    state.returns[ev.returnId] = {
      status = ev.status,
      reason = ev.reason,
      orderId = ev.orderId,
      updatedAt = os.time(),
    }
  end
  audit.record("catalog", "ApplyOrderEvent", msg, nil, { type = ev.type, orderId = ev.orderId })
  metrics.inc("catalog.ApplyOrderEvent.count")
  metrics.tick()
  return codec.ok({ applied = ev.type, orderId = ev.orderId })
end

function handlers.GetOrder(msg)
  local ok, missing = validation.require_fields(msg, { "Order-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Order-Id required", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Order-Id", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local order = state.orders[msg["Order-Id"]]
  if not order then return codec.error("NOT_FOUND", "order not found") end
  return codec.ok({ orderId = msg["Order-Id"], order = order })
end

function handlers.ListOrders(msg)
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Customer-Id", "Status", "Limit", "Offset", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local limit = tonumber(msg.Limit) or 50
  local offset = tonumber(msg.Offset) or 0
  local items = {}
  for oid, o in pairs(state.orders) do
    if (not msg["Site-Id"] or o.siteId == msg["Site-Id"]) and
       (not msg["Customer-Id"] or o.customerId == msg["Customer-Id"]) and
       (not msg.Status or o.status == msg.Status) then
      table.insert(items, { orderId = oid, order = o })
    end
  end
  table.sort(items, function(a, b) return (a.order.updatedAt or 0) > (b.order.updatedAt or 0) end)
  local slice = {}
  for i = offset + 1, math.min(#items, offset + limit) do
    table.insert(slice, items[i])
  end
  return codec.ok({ total = #items, items = slice })
end

function handlers.SetInventoryReservation(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Order-Id", "Items" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Order-Id", "Items", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_items, err_items = validation.assert_type(msg.Items, "table", "Items")
  if not ok_items then return codec.error("INVALID_INPUT", err_items, { field = "Items" }) end
  for _, item in ipairs(msg.Items) do
    if not (item.sku and item.qty) then
      return codec.error("INVALID_INPUT", "Item must have sku and qty")
    end
  end
  state.reservations[msg["Order-Id"]] = { siteId = msg["Site-Id"], items = msg.Items, released = false }
  return codec.ok({ orderId = msg["Order-Id"], reserved = #msg.Items })
end

local function adjust_inventory(siteId, items, sign)
  state.inventory[siteId] = state.inventory[siteId] or {}
  for _, item in ipairs(items or {}) do
    local inv = state.inventory[siteId][item.sku] or { quantity = 0 }
    inv.quantity = math.max(0, inv.quantity + sign * (item.qty or 0))
    state.inventory[siteId][item.sku] = inv
  end
end

function handlers.SyncShipment(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Order-Id", "Status" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local res = state.reservations[msg["Order-Id"]]
  if res and not res.released and (msg.Status == "shipped" or msg.Status == "delivered") then
    adjust_inventory(res.siteId, res.items, -1)
    res.released = true
  end
  return codec.ok({ orderId = msg["Order-Id"], released = res and res.released or false })
end

function handlers.SyncReturn(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Order-Id", "Status" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local res = state.reservations[msg["Order-Id"]]
  if res and (msg.Status == "approved" or msg.Status == "refunded") then
    adjust_inventory(res.siteId, res.items, 1)
  end
  return codec.ok({ orderId = msg["Order-Id"], restocked = res ~= nil })
end

function handlers.GetShippingRates(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local rates = state.shipping_rates[msg["Site-Id"]] or {}
  return codec.ok({ siteId = msg["Site-Id"], rates = rates })
end

function handlers.GetTaxRates(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local rates = state.tax_rates[msg["Site-Id"]] or {}
  return codec.ok({ siteId = msg["Site-Id"], rates = rates })
end

function handlers.ValidateAddress(msg)
  local ok, missing = validation.require_fields(msg, { "Country" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Country", "Region", "City", "Postal", "Line1", "Line2", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  if #msg.Country ~= 2 then return codec.error("INVALID_INPUT", "Country must be ISO2") end
  local cmd = os.getenv("ADDRESS_VALIDATE_CMD")
  if cmd and cmd ~= "" then
    os.execute(cmd .. " >/dev/null 2>&1")
  end
  return codec.ok({
    valid = true,
    normalized = {
      country = msg.Country:upper(),
      region = msg.Region,
      city = msg.City,
      postal = msg.Postal,
      line1 = msg.Line1,
      line2 = msg.Line2,
    },
  })
end

function handlers.GetShipment(msg)
  local ok, missing = validation.require_fields(msg, { "Shipment-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Shipment-Id", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local sh = state.shipments[msg["Shipment-Id"]]
  if not sh then return codec.error("NOT_FOUND", "Shipment not found") end
  return codec.ok(sh)
end

function handlers.ApplyShipmentEvent(msg)
  local ok, missing = validation.require_fields(msg, { "Shipment-Id", "Order-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Shipment-Id", "Order-Id", "Carrier", "Service", "Label-Url", "Status", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  state.shipments[msg["Shipment-Id"]] = state.shipments[msg["Shipment-Id"]] or {}
  local sh = state.shipments[msg["Shipment-Id"]]
  sh.orderId = msg["Order-Id"]
  sh.carrier = msg.Carrier or sh.carrier
  sh.service = msg.Service or sh.service
  sh.labelUrl = msg["Label-Url"] or sh.labelUrl
  sh.status = msg.Status or sh.status or "pending"
  audit.record("catalog", "ApplyShipmentEvent", msg, nil, { shipment = msg["Shipment-Id"] })
  return codec.ok({ shipmentId = msg["Shipment-Id"], status = sh.status, carrier = sh.carrier, service = sh.service, labelUrl = sh.labelUrl })
end

function handlers.ApplyTrackingEvent(msg)
  local ok, missing = validation.require_fields(msg, { "Shipment-Id", "Tracking" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Shipment-Id", "Tracking", "Carrier", "Eta", "Tracking-Url", "Status", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  state.shipments[msg["Shipment-Id"]] = state.shipments[msg["Shipment-Id"]] or {}
  local sh = state.shipments[msg["Shipment-Id"]]
  sh.tracking = msg.Tracking
  sh.trackingUrl = msg["Tracking-Url"] or sh.trackingUrl
  sh.eta = msg.Eta or sh.eta
  sh.carrier = msg.Carrier or sh.carrier
  sh.status = msg.Status or sh.status
  audit.record("catalog", "ApplyTrackingEvent", msg, nil, { shipment = msg["Shipment-Id"], tracking = msg.Tracking })
  return codec.ok({ shipmentId = msg["Shipment-Id"], tracking = sh.tracking, trackingUrl = sh.trackingUrl, eta = sh.eta, status = sh.status })
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

  local ok_hmac, hmac_err = auth.verify_outbox_hmac(msg)
  if not ok_hmac then
    return codec.error("FORBIDDEN", hmac_err)
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
