-- Catalog process handlers: products, categories, listings.

local codec = require "ao.shared.codec"
local validation = require "ao.shared.validation"
local ids = require "ao.shared.ids"
local auth = require "ao.shared.auth"
local idem = require "ao.shared.idempotency"
local audit = require "ao.shared.audit"
local schema = require "ao.shared.schema"
local metrics = require "ao.shared.metrics"
local json_ok, cjson = pcall(require, "cjson.safe")
local RECENT_LIMIT = tonumber(os.getenv "CATALOG_RECENT_LIMIT" or "") or 20
local SCA_FORCE = os.getenv "CATALOG_SCA_FORCE" == "1"
local MAX_RATE_OPTIONS = tonumber(os.getenv "CATALOG_MAX_RATE_OPTIONS" or "") or 5
local RISK_THRESHOLD = tonumber(os.getenv "CATALOG_MANUAL_REVIEW_THRESHOLD" or "") or 90
local TELEMETRY_EXPORT_PATH = os.getenv "CATALOG_TELEMETRY_PATH"
local INVOICE_EXPORT_PATH = os.getenv "CATALOG_INVOICE_PATH"
local CARRIER_LABEL_BASE = os.getenv "CATALOG_CARRIER_LABEL_BASE" or "https://labels.example/"
local CARRIER_TRACK_BASE = os.getenv "CATALOG_CARRIER_TRACK_BASE" or "https://track.example/"
local CARRIER_API_URL = os.getenv "CATALOG_CARRIER_API_URL" -- optional external rate/label stub
local CARRIER_API_TOKEN = os.getenv "CATALOG_CARRIER_API_TOKEN"
local INVOICE_PDF_DIR = os.getenv "CATALOG_INVOICE_PDF_DIR"
local INVOICE_NUMBER_WITH_YEAR = os.getenv "CATALOG_INVOICE_YEAR" ~= "0"
local INVOICE_S3_BUCKET = os.getenv "CATALOG_INVOICE_S3_BUCKET"
local HTTP_TIMEOUT = tonumber(os.getenv "CATALOG_HTTP_TIMEOUT" or "") or 5
local S3_TIMEOUT = tonumber(os.getenv "CATALOG_S3_TIMEOUT" or "") or 10
local S3_RETRIES = tonumber(os.getenv "CATALOG_S3_RETRIES" or "") or 2
local EVENT_LOG_LIMIT = tonumber(os.getenv "CATALOG_EVENT_LOG_LIMIT" or "") or 5000
local GA4_ENDPOINT = os.getenv "CATALOG_GA4_ENDPOINT"
local GA4_API_SECRET = os.getenv "CATALOG_GA4_API_SECRET"
local GA4_MEASUREMENT_ID = os.getenv "CATALOG_GA4_MEASUREMENT_ID"
local PAYMENT_WEBHOOK_SECRET = os.getenv "CATALOG_PAYMENT_WEBHOOK_SECRET"
local CARRIER_WEBHOOK_SECRET = os.getenv "CATALOG_CARRIER_WEBHOOK_SECRET"
local RETURN_LABEL_BASE = os.getenv "CATALOG_RETURN_LABEL_BASE" or CARRIER_LABEL_BASE
local JWT_HMAC_SECRET = os.getenv "CATALOG_JWT_SECRET" or os.getenv "JWT_SECRET"
local INVOICE_SIGN_SECRET = os.getenv "CATALOG_INVOICE_SIGN_SECRET"
local PDF_RENDER_CMD = os.getenv "CATALOG_PDF_RENDER_CMD" or "cat" -- expects: CMD input.html output.pdf
local FEED_EXPORT_PATH = os.getenv "CATALOG_FEED_EXPORT_PATH"
local MERCHANT_CENTER_PATH = os.getenv "CATALOG_MERCHANT_CENTER_PATH"
local MERCHANT_CENTER_COUNTRY = os.getenv "CATALOG_MERCHANT_CENTER_COUNTRY" or "US"
local MERCHANT_CENTER_CURRENCY = os.getenv "CATALOG_MERCHANT_CENTER_CURRENCY" or "USD"
local STOCK_ALERT_WEBHOOK = os.getenv "CATALOG_STOCK_ALERT_WEBHOOK"
local CDN_PURGE_CMD = os.getenv "CATALOG_CDN_PURGE_CMD" or os.getenv "CDN_PURGE_CMD"
local RMA_WEBHOOK = os.getenv "CATALOG_RMA_WEBHOOK"
local STRIPE_SECRET = os.getenv "CATALOG_STRIPE_SECRET"
local STRIPE_WEBHOOK_SECRET = os.getenv "CATALOG_STRIPE_WEBHOOK_SECRET"
local PAYPAL_WEBHOOK_ID = os.getenv "CATALOG_PAYPAL_WEBHOOK_ID"
local PAYPAL_WEBHOOK_SECRET = os.getenv "CATALOG_PAYPAL_WEBHOOK_SECRET"
local ADYEN_HMAC_KEY = os.getenv "CATALOG_ADYEN_HMAC_KEY"
local CDN_PURGE_CMD = os.getenv "CATALOG_CDN_PURGE_CMD" -- optional, e.g. "curl -X POST https://api.fastly.com/service/... -H 'Fastly-Key: ...' -H 'Surrogate-Key: %s'"
local RETENTION_DAYS = tonumber(os.getenv "CATALOG_RETENTION_DAYS" or "") or 30
local SEARCH_SYNONYMS_PATH = os.getenv "CATALOG_SEARCH_SYNONYMS_PATH"
local SEARCH_STOPWORDS_PATH = os.getenv "CATALOG_SEARCH_STOPWORDS_PATH"

local handlers = {}
local allowed_actions = {
  "GetProduct",
  "ListCategoryProducts",
  "SearchCatalog",
  "GetOrder",
  "ListOrders",
  "ApplyOrderEvent",
  "UpsertProduct",
  "UpsertVariants",
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
  "SetPriceList",
  "AddPromo",
  "QuotePrice",
  "SetTaxRules",
  "SetShippingRules",
  "QuoteOrder",
  "StartCheckout",
  "CompleteCheckout",
  "SetInventory",
  "GetInventory",
  "TrackCatalogEvent",
  "RelatedProducts",
  "RecentlyViewed",
  "CreatePaymentIntent",
  "CapturePayment",
  "RefundPayment",
  "RequestReturn",
  "ApproveReturn",
  "RefundReturn",
  "CalculateTax",
  "RateShopCarriers",
  "ExportTelemetry",
  "CreateCompanyAccount",
  "AddCompanyUser",
  "CreatePurchaseOrder",
  "ApprovePurchaseOrder",
  "RejectPurchaseOrder",
  "CheckoutPurchaseOrder",
  "CreateShippingLabel",
  "CreateInvoice",
  "GetInvoice",
  "ListInvoices",
  "Bestsellers",
  "TrendingProducts",
  "ExportEventLog",
  "StreamTelemetry",
  "HandlePaymentWebhook",
  "HandleCarrierWebhook",
  "UpdateReturnStatus",
  "CreateReturnLabel",
  "CreateWebhook",
  "GetWebhook",
  "ListWebhooks",
  "DeleteWebhook",
  "SignPayload",
  "VerifySignature",
  "ExportCatalogFeed",
  "ExportSearchFeed",
  "ExportCategoryFeed",
  "DeleteProduct",
  "DeleteCategory",
  "PurgeCache",
  "ExportMerchantFeed",
  "SetStockPolicy",
  "ListLowStock",
  "GetCategory",
  "ListCategories",
  "DeliverLowStockAlerts",
  "ForgetSubject",
  "ListBackorders",
  "TokenizePaymentMethod",
  "HandlePaymentProviderWebhook",
  "CleanupRetention",
}

local role_policy = {
  UpsertProduct = { "catalog-admin", "editor", "admin" },
  UpsertVariants = { "catalog-admin", "editor", "admin" },
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
  SetPriceList = { "catalog-admin", "admin" },
  AddPromo = { "catalog-admin", "admin" },
  QuotePrice = { "catalog-admin", "support", "admin" },
  SetTaxRules = { "catalog-admin", "admin" },
  SetShippingRules = { "catalog-admin", "admin" },
  QuoteOrder = { "catalog-admin", "support", "admin" },
  StartCheckout = { "catalog-admin", "support", "admin" },
  CompleteCheckout = { "catalog-admin", "support", "admin" },
  SetInventory = { "catalog-admin", "admin" },
  GetInventory = { "catalog-admin", "support", "admin" },
  TrackCatalogEvent = { "catalog-admin", "support", "admin", "viewer" },
  RelatedProducts = { "catalog-admin", "support", "admin", "viewer" },
  RecentlyViewed = { "catalog-admin", "support", "admin", "viewer" },
  CreatePaymentIntent = { "catalog-admin", "support", "admin" },
  CapturePayment = { "catalog-admin", "support", "admin" },
  RefundPayment = { "catalog-admin", "support", "admin" },
  RequestReturn = { "support", "catalog-admin", "admin" },
  ApproveReturn = { "support", "catalog-admin", "admin" },
  RefundReturn = { "support", "catalog-admin", "admin" },
  UpdateReturnStatus = { "support", "catalog-admin", "admin" },
  CalculateTax = { "catalog-admin", "support", "admin" },
  RateShopCarriers = { "catalog-admin", "support", "admin" },
  ExportTelemetry = { "admin", "catalog-admin" },
  CreateCompanyAccount = { "catalog-admin", "admin" },
  AddCompanyUser = { "catalog-admin", "admin" },
  CreatePurchaseOrder = { "buyer", "approver", "catalog-admin", "admin" },
  ApprovePurchaseOrder = { "approver", "catalog-admin", "admin" },
  RejectPurchaseOrder = { "approver", "catalog-admin", "admin" },
  CheckoutPurchaseOrder = { "catalog-admin", "admin", "approver" },
  CreateShippingLabel = { "catalog-admin", "admin", "support" },
  CreateInvoice = { "catalog-admin", "admin", "support" },
  GetInvoice = { "support", "admin", "catalog-admin" },
  ListInvoices = { "support", "admin", "catalog-admin" },
  Bestsellers = { "catalog-admin", "support", "admin", "viewer" },
  TrendingProducts = { "catalog-admin", "support", "admin", "viewer" },
  ExportEventLog = { "admin", "catalog-admin" },
  StreamTelemetry = { "admin", "catalog-admin" },
  HandlePaymentWebhook = { "admin", "catalog-admin" },
  HandleCarrierWebhook = { "admin", "catalog-admin", "support" },
  CreateReturnLabel = { "support", "catalog-admin", "admin" },
  CreateWebhook = { "admin", "catalog-admin" },
  GetWebhook = { "admin", "catalog-admin" },
  ListWebhooks = { "admin", "catalog-admin" },
  DeleteWebhook = { "admin", "catalog-admin" },
  SignPayload = { "admin", "catalog-admin" },
  VerifySignature = { "admin", "catalog-admin" },
  ExportCatalogFeed = { "catalog-admin", "support", "admin", "viewer" },
  ExportSearchFeed = { "catalog-admin", "support", "admin", "viewer" },
  ExportCategoryFeed = { "catalog-admin", "support", "admin", "viewer" },
  DeleteProduct = { "catalog-admin", "editor", "admin" },
  DeleteCategory = { "catalog-admin", "editor", "admin" },
  PurgeCache = { "catalog-admin", "admin", "support" },
  ExportMerchantFeed = { "catalog-admin", "support", "admin" },
  SetStockPolicy = { "catalog-admin", "admin" },
  ListLowStock = { "catalog-admin", "admin", "support" },
  GetCategory = { "catalog-admin", "support", "admin", "viewer" },
  ListCategories = { "catalog-admin", "support", "admin", "viewer" },
  DeliverLowStockAlerts = { "catalog-admin", "admin", "support" },
  ForgetSubject = { "admin", "catalog-admin", "support" },
  ListBackorders = { "catalog-admin", "admin", "support" },
  TokenizePaymentMethod = { "catalog-admin", "support", "admin" },
  HandlePaymentProviderWebhook = { "catalog-admin", "support", "admin" },
  CleanupRetention = { "admin", "catalog-admin" },
}

local state = {
  products = {}, -- product:<site>:<sku> -> { payload }
  categories = {}, -- category:<site>:<id> -> { payload, products = {sku}}
  active_versions = {}, -- site -> version
  inventory = {}, -- siteId -> warehouse -> { sku -> qty }
  reservations = {}, -- orderId -> { siteId, items = { { sku, qty } }, released=false }
  orders = {}, -- orderId -> order record
  shipments = {}, -- shipmentId -> { status, tracking, carrier, eta, orderId }
  returns = {}, -- returnId -> { status, reason, orderId }
  shipping_rates = {}, -- siteId -> list of rate rows
  tax_rates = {}, -- siteId -> list of tax rows
  price_lists = {}, -- siteId -> currency -> { sku -> price }
  promos = {}, -- code -> { type = "percent"|"amount", value, skus }
  variants = {}, -- siteId -> parentSku -> { variants = { { sku, attrs, price } } }
  tax_rules = {}, -- siteId -> list { country, region?, rate }
  shipping_rules = {}, -- siteId -> list { country, min_total, max_total, rate, carrier, service }
  checkouts = {}, -- checkoutId -> { siteId, items, address, quote, status }
  events = {}, -- siteId -> sku -> { views, add_to_cart, purchases }
  recent = {}, -- subject -> list of { siteId, sku } (most recent first, capped)
  payments = {}, -- paymentId -> { status, amount, currency, method, siteId, orderId, checkoutId, requiresAction }
  telemetry = {}, -- buffered events for export
  companies = {}, -- companyId -> { name, users = { [userId] = role } }
  purchase_orders = {}, -- poId -> { siteId, companyId, items, address, currency, subtotal, tax, shipping, total, status, approvals = {} }
  invoices = {}, -- invoiceId -> { orderId, siteId, total, currency, lines, issuedAt, status }
  invoice_seq = {}, -- siteId -> last number
  invoice_seq_year = {}, -- siteId -> year -> last number
  event_log = {}, -- siteId -> list of { ts, sku, event }
  webhooks = {}, -- siteId -> id -> { url, secret, events }
  deletions = {}, -- siteId -> list of { key, deletedAt }
  category_deletions = {}, -- siteId -> list of { key, deletedAt }
  stock_policies = {}, -- siteId -> sku -> { allow_backorder, preorder_at, low_stock_threshold }
  stock_alerts = {}, -- siteId -> list of { sku, total, threshold, ts }
  backorders = {}, -- siteId -> list of { sku, qty, preorder_at, eta_days, createdAt, source, ref }
  shipment_events = {}, -- shipmentId -> list of { ts, status, meta }
  search_synonyms = {}, -- siteId -> map term -> {synonyms}
  search_stopwords = {}, -- siteId -> set of stopwords
}

local function gen_id(prefix)
  return string.format("%s-%d-%04d", prefix, os.time(), math.random(0, 9999))
end

local MAX_PAYLOAD_BYTES = tonumber(os.getenv "CATALOG_MAX_PAYLOAD_BYTES" or "") or (64 * 1024)
local SHIPPING_RATES_PATH = os.getenv "AO_SHIPPING_RATES_PATH"
local TAX_RATES_PATH = os.getenv "AO_TAX_RATES_PATH"

local function load_ndjson(path)
  if not path or path == "" or not json_ok then
    return {}
  end
  local f = io.open(path, "r")
  if not f then
    return {}
  end
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

local function load_synonyms()
  if not SEARCH_SYNONYMS_PATH or SEARCH_SYNONYMS_PATH == "" or not json_ok then
    return
  end
  local f = io.open(SEARCH_SYNONYMS_PATH, "r")
  if not f then
    return
  end
  local ok, data = pcall(cjson.decode, f:read "*a")
  f:close()
  if not ok or type(data) ~= "table" then
    return
  end
  -- expected format: { siteId = "...", synonyms = { { term = "tv", words = {"television","oled"} }, ... } }
  for _, entry in ipairs(data) do
    if entry.siteId and entry.synonyms and type(entry.synonyms) == "table" then
      state.search_synonyms[entry.siteId] = {}
      for _, row in ipairs(entry.synonyms) do
        if row.term and row.words then
          state.search_synonyms[entry.siteId][row.term:lower()] = {}
          for _, w in ipairs(row.words) do
            table.insert(state.search_synonyms[entry.siteId][row.term:lower()], w:lower())
          end
        end
      end
    end
  end
end

local function load_stopwords()
  if not SEARCH_STOPWORDS_PATH or SEARCH_STOPWORDS_PATH == "" or not json_ok then
    return
  end
  local f = io.open(SEARCH_STOPWORDS_PATH, "r")
  if not f then
    return
  end
  local ok, data = pcall(cjson.decode, f:read "*a")
  f:close()
  if not ok or type(data) ~= "table" then
    return
  end
  -- expected format: [ { siteId="...", words=["a","the","and"] }, ... ]
  for _, entry in ipairs(data) do
    if entry.siteId and entry.words and type(entry.words) == "table" then
      state.search_stopwords[entry.siteId] = {}
      for _, w in ipairs(entry.words) do
        state.search_stopwords[entry.siteId][w:lower()] = true
      end
    end
  end
end

load_rates()
load_synonyms()
load_stopwords()

local function track_event(site_id, subject, sku, event)
  state.events[site_id] = state.events[site_id] or {}
  local stats = state.events[site_id][sku] or { views = 0, add_to_cart = 0, purchases = 0 }
  if event == "view" then
    stats.views = stats.views + 1
  elseif event == "add_to_cart" then
    stats.add_to_cart = stats.add_to_cart + 1
  elseif event == "purchase" then
    stats.purchases = stats.purchases + 1
  end
  state.events[site_id][sku] = stats

  if subject then
    state.recent[subject] = state.recent[subject] or {}
    local list = state.recent[subject]
    for i = #list, 1, -1 do
      if list[i].sku == sku and list[i].siteId == site_id then
        table.remove(list, i)
      end
    end
    table.insert(list, 1, { siteId = site_id, sku = sku })
    while #list > RECENT_LIMIT do
      table.remove(list)
    end
  end

  state.event_log[site_id] = state.event_log[site_id] or {}
  local log = state.event_log[site_id]
  table.insert(log, 1, { ts = os.time(), sku = sku, event = event })
  while #log > EVENT_LOG_LIMIT do
    table.remove(log)
  end

  return stats
end

local function create_payment_intent_internal(args)
  -- args: siteId, checkoutId?, orderId?, amount, currency, method, require3ds?, provider?, token?
  local payment_id = gen_id "pay"
  local requires_action = (args.require3ds == true) or SCA_FORCE
  local status = requires_action and "requires_action" or "authorized"
  local record = {
    paymentId = payment_id,
    siteId = args.siteId,
    checkoutId = args.checkoutId,
    orderId = args.orderId,
    amount = args.amount,
    currency = args.currency,
    method = args.method,
    provider = args.provider or "internal",
    token = args.token,
    status = status,
    requiresAction = requires_action,
    clientSecret = "sec_" .. payment_id,
    createdAt = os.time(),
  }
  state.payments[payment_id] = record
  if args.checkoutId and state.checkouts[args.checkoutId] then
    local chk = state.checkouts[args.checkoutId]
    chk.paymentIntent = payment_id
    chk.paymentStatus = status
  end
  if args.orderId and state.orders[args.orderId] then
    state.orders[args.orderId].paymentStatus = status
  end
  return record
end

local function record_telemetry(kind, data)
  table.insert(state.telemetry, {
    ts = os.time(),
    kind = kind,
    data = data,
  })
end

local function http_post_json(url, payload)
  if not json_ok then
    return nil, "JSON_ENCODE_DISABLED"
  end
  local ok_enc, body = pcall(cjson.encode, payload)
  if not ok_enc or not body then
    return nil, "ENCODE_FAILED"
  end
  local tmp = os.tmpname()
  local f = io.open(tmp, "w")
  if not f then
    return nil, "TMP_OPEN_FAILED"
  end
  f:write(body)
  f:close()
  local auth = ""
  if CARRIER_API_TOKEN and CARRIER_API_TOKEN ~= "" then
    auth = "-H 'Authorization: Bearer " .. CARRIER_API_TOKEN .. "' "
  end
  local cmd = string.format(
    "curl -sS --max-time %d -X POST %s -H 'Content-Type: application/json' %s --data-binary @%s",
    HTTP_TIMEOUT,
    url,
    auth,
    tmp
  )
  local reader = io.popen(cmd, "r")
  if not reader then
    os.remove(tmp)
    return nil, "CURL_READ_FAILED"
  end
  local out = reader:read "*a"
  local ok_close, why, code = reader:close()
  os.remove(tmp)
  if not ok_close then
    return nil, "CURL_EXIT_" .. tostring(code or why)
  end
  return out, nil
end

local function s3_copy_with_retry(path, bucket)
  if not bucket or bucket == "" then
    return false
  end
  for attempt = 1, (S3_RETRIES + 1) do
    local cmd = string.format(
      "aws s3 cp %s s3://%s/ --no-progress --expected-size %d --cli-read-timeout %d --cli-connect-timeout %d",
      path,
      bucket,
      0,
      S3_TIMEOUT,
      S3_TIMEOUT
    )
    local rc = os.execute(cmd)
    if rc == true or rc == 0 then
      return true
    end
  end
  return false
end

local function render_invoice_pdf(inv)
  if not INVOICE_PDF_DIR or INVOICE_PDF_DIR == "" then
    return nil
  end
  os.execute("mkdir -p " .. INVOICE_PDF_DIR)
  local html_path = string.format("%s/%s.html", INVOICE_PDF_DIR, inv.invoiceId)
  local pdf_path = string.format("%s/%s.pdf", INVOICE_PDF_DIR, inv.invoiceId)
  local f = io.open(html_path, "w")
  if f then
    f:write "<html><body>"
    f:write(string.format("<h1>Invoice %s</h1>", inv.invoiceNumber or inv.invoiceId))
    f:write(string.format("<p>Order: %s</p>", inv.orderId or "-"))
    f:write(string.format("<p>Total: %.2f %s</p>", inv.total or 0, inv.currency or ""))
    f:write "<ul>"
    for _, line in ipairs(inv.lines or {}) do
      f:write(
        string.format(
          "<li>%s x%s @ %s</li>",
          line.sku or line.Sku or "item",
          line.qty or line.Qty or "1",
          line.unit_price or line.price or "?"
        )
      )
    end
    f:write "</ul>"
    f:write(string.format("<p>Tax: %.2f Shipping: %.2f</p>", inv.tax or 0, inv.shipping or 0))
    f:write "</body></html>"
    f:close()
  end
  -- best-effort render command
  local cmd = string.format("%s %s %s", PDF_RENDER_CMD, html_path, pdf_path)
  os.execute(cmd)
  local pdf_exists = io.open(pdf_path, "r")
  if pdf_exists then
    pdf_exists:close()
    return pdf_path
  end
  return nil
end

local function risk_score(checkout)
  local score = 0
  if checkout.quote.total and checkout.quote.total > 500 then
    score = score + 20
  end
  if checkout.address and checkout.address.Country and checkout.address.Country ~= "US" then
    score = score + 10
  end
  if checkout.email and checkout.email:match "@(mailinator|10minutemail|tempmail)" then
    score = score + 25
  end
  if checkout.quote and checkout.quote.shipping and checkout.quote.shipping.rate == 0 then
    score = score + 5
  end
  if checkout.quote and checkout.quote.promo then
    score = score + 5
  end
  return math.min(100, score)
end

local function build_label(carrier, service, weight)
  local shipment_id = gen_id "ship"
  local tracking = string.format("trk-%s-%04d", carrier or "std", math.random(0, 9999))
  local label_url = string.format("%s%s.pdf", CARRIER_LABEL_BASE, shipment_id)
  local track_url = string.format("%s%s", CARRIER_TRACK_BASE, tracking)
  local label = {
    shipmentId = shipment_id,
    tracking = tracking,
    trackingUrl = track_url,
    labelUrl = label_url,
    carrier = carrier,
    service = service,
    weight = weight,
    status = "label_created",
  }
  -- optional remote label creation
  if CARRIER_API_URL and json_ok then
    local payload = {
      carrier = carrier,
      service = service,
      tracking = tracking,
      shipmentId = shipment_id,
      weight = weight,
    }
    http_post_json(CARRIER_API_URL .. "/label", payload)
  end
  return label
end

-- B2B helpers -------------------------------------------------------------
local function ensure_company(company_id)
  if not state.companies[company_id] then
    return false, "Company not found"
  end
  return true
end

local function require_company_role(company_id, user_id, roles)
  local comp = state.companies[company_id]
  if not comp then
    return false, "Company not found"
  end
  local role = comp.users and comp.users[user_id]
  for _, r in ipairs(roles) do
    if role == r then
      return true
    end
  end
  return false, "User not authorized for company"
end

-- tiny Levenshtein for typo tolerance on short queries
local function levenshtein(a, b)
  if not a or not b then
    return 99
  end
  local la, lb = #a, #b
  if la == 0 then
    return lb
  end
  if lb == 0 then
    return la
  end
  local prev = {}
  for j = 0, lb do
    prev[j] = j
  end
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
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Sku", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then
    return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" })
  end
  local ok_len_sku, err_sku = validation.check_length(msg.Sku, 128, "Sku")
  if not ok_len_sku then
    return codec.error("INVALID_INPUT", err_sku, { field = "Sku" })
  end
  local key = ids.product_key(msg["Site-Id"], msg.Sku)
  local product = state.products[key]
  if not product then
    return codec.error("NOT_FOUND", "Product not found", { sku = msg.Sku })
  end
  return codec.ok {
    siteId = msg["Site-Id"],
    sku = msg.Sku,
    payload = product.payload,
    version = product.version or state.active_versions[msg["Site-Id"]] or "active",
    variants = state.variants[msg["Site-Id"]] and state.variants[msg["Site-Id"]][msg.Sku],
    stats = state.events[msg["Site-Id"]] and state.events[msg["Site-Id"]][msg.Sku],
  }
end

function handlers.ListCategoryProducts(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Category-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Category-Id",
    "Page",
    "PageSize",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then
    return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" })
  end
  local ok_len_cat, err_cat = validation.check_length(msg["Category-Id"], 128, "Category-Id")
  if not ok_len_cat then
    return codec.error("INVALID_INPUT", err_cat, { field = "Category-Id" })
  end
  local key = ids.category_key(msg["Site-Id"], msg["Category-Id"])
  local category = state.categories[key]
  if not category then
    return codec.error("NOT_FOUND", "Category not found", { category = msg["Category-Id"] })
  end
  local page = msg.Page or 1
  local page_size = msg.PageSize or 50
  if page < 1 then
    page = 1
  end
  if page_size < 1 then
    page_size = 1
  end
  if page_size > 200 then
    page_size = 200
  end
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
  return codec.ok {
    siteId = msg["Site-Id"],
    categoryId = msg["Category-Id"],
    page = page,
    pageSize = page_size,
    items = products,
    total = #category.products,
  }
end

function handlers.SearchCatalog(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Query",
    "MinPrice",
    "MaxPrice",
    "Locale",
    "Available",
    "Category-Id",
    "Sort",
    "Currency",
    "Carrier",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then
    return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" })
  end
  if msg.Query then
    local ok_len_query, err_query = validation.check_length(msg.Query, 1024, "Query")
    if not ok_len_query then
      return codec.error("INVALID_INPUT", err_query, { field = "Query" })
    end
  end
  local min_price = msg.MinPrice
  local max_price = msg.MaxPrice
  if min_price and type(min_price) ~= "number" then
    return codec.error("INVALID_INPUT", "MinPrice must be number")
  end
  if max_price and type(max_price) ~= "number" then
    return codec.error("INVALID_INPUT", "MaxPrice must be number")
  end
  local q = msg.Query and msg.Query:lower() or ""
  local sort = msg.Sort or "relevance"
  local tokens = {}
  for t in q:gmatch "%S+" do
    table.insert(tokens, t)
  end
  local syn = state.search_synonyms[msg["Site-Id"]] or {}
  local stopwords = state.search_stopwords[msg["Site-Id"]] or {}
  local filtered_tokens = {}
  for _, t in ipairs(tokens) do
    if not stopwords[t] then
      table.insert(filtered_tokens, t)
    end
  end
  local expanded_tokens = {}
  for _, t in ipairs(filtered_tokens) do
    table.insert(expanded_tokens, t)
    if syn[t] then
      for _, s in ipairs(syn[t]) do
        table.insert(expanded_tokens, s)
      end
    end
  end
  local results = {}
  local suggestions = {}
  local facets = {
    categories = {},
    availability = { available = 0, unavailable = 0 },
    shippingStatus = {},
    price = { lt25 = 0, lt100 = 0, gte100 = 0 },
    currency = {},
    locales = {},
    carriers = {},
  }
  local prefix = "product:" .. msg["Site-Id"] .. ":"
  for key, product in pairs(state.products) do
    if key:sub(1, #prefix) == prefix then
      local sku = key:match "product:[^:]+:(.+)"
      local text = (product.payload.name or ""):lower()
        .. " "
        .. (product.payload.description or ""):lower()
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
          local inv = state.inventory[msg["Site-Id"]] or {}
          local qty = 0
          for _, wh in pairs(inv) do
            qty = qty + (wh[sku] or 0)
          end
          available = qty > 0
        end
        local ok_price = true
        if min_price and price and price < min_price then
          ok_price = false
        end
        if max_price and price and price > max_price then
          ok_price = false
        end
        local ok_locale = (not msg.Locale) or (locale == msg.Locale)
        local ok_currency = (not msg.Currency) or (payload.currency == msg.Currency)
        local ok_available = (msg.Available == nil) or (available == msg.Available)
        local ok_carrier = (not msg.Carrier) or (payload.carrier == msg.Carrier)
        if available then
          facets.availability.available = facets.availability.available + 1
        else
          facets.availability.unavailable = facets.availability.unavailable + 1
        end
        if payload.categoryId then
          facets.categories[payload.categoryId] = (facets.categories[payload.categoryId] or 0) + 1
        end
        if payload.carrier then
          facets.carriers[payload.carrier] = (facets.carriers[payload.carrier] or 0) + 1
        end
        if payload.shippingStatus then
          facets.shippingStatus[payload.shippingStatus] = (
            facets.shippingStatus[payload.shippingStatus] or 0
          ) + 1
        end
        local price_num = tonumber(price or 0) or 0
        if price_num < 25 then
          facets.price.lt25 = facets.price.lt25 + 1
        elseif price_num < 100 then
          facets.price.lt100 = facets.price.lt100 + 1
        else
          facets.price.gte100 = facets.price.gte100 + 1
        end
        if payload.currency then
          facets.currency[payload.currency] = (facets.currency[payload.currency] or 0) + 1
        end
        if locale then
          facets.locales[locale] = (facets.locales[locale] or 0) + 1
        end
        local ok_cat = not msg["Category-Id"]
          or (payload.categoryId == msg["Category-Id"])
          or (payload.category and payload.category.id == msg["Category-Id"])
          or false
        if ok_price and ok_locale and ok_currency and ok_available and ok_carrier and ok_cat then
          local score = 0
          local events = state.events[msg["Site-Id"]] and state.events[msg["Site-Id"]][sku] or {}
          if q ~= "" then
            if sku:lower():find("^" .. q, 1, false) then
              score = score + 5
            end
            if (payload.name or ""):lower():find(q, 1, true) then
              score = score + 3
            end
            if (payload.description or ""):lower():find(q, 1, true) then
              score = score + 1
            end
            -- typo tolerance for short queries (distance <=1)
            if #q <= 6 then
              local d = levenshtein((payload.name or ""):lower(), q)
              if d == 1 then
                score = score + 2
              end
            end
            if fuzzy_hit then
              score = score + 1
            end
            for _, tok in ipairs(expanded_tokens) do
              if (payload.brand or ""):lower():find(tok, 1, true) then
                score = score + 2
              end
              if payload.tags and type(payload.tags) == "table" then
                for _, tag in ipairs(payload.tags) do
                  if type(tag) == "string" and tag:lower() == tok then
                    score = score + 1
                  end
                end
              end
              -- lightweight token typo for short tokens
              if #tok <= 4 then
                local d = levenshtein((payload.name or ""):lower(), tok)
                if d == 1 then
                  score = score + 1
                end
              end
            end
          end
          score = score + (events.purchases or 0) * 2 + (events.views or 0) * 0.1
          if msg.Locale and locale == msg.Locale then
            score = score + 1
          end
          table.insert(results, {
            sku = sku,
            payload = payload,
            price = price,
            name = payload.name or sku,
            score = score,
            available = available,
            category = payload.categoryId or (payload.category and payload.category.id),
          })
        elseif q ~= "" and not matched then
          local name = (product.payload.name or ""):lower()
          local d = levenshtein(name, q)
          if d <= 2 then
            table.insert(suggestions, product.payload.name or sku)
          end
        end
      end
    end
  end
  table.sort(results, function(a, b)
    if sort == "price" or sort == "price_asc" then
      return (a.price or 0) < (b.price or 0)
    end
    if sort == "-price" or sort == "price_desc" then
      return (a.price or 0) > (b.price or 0)
    end
    if sort == "name" then
      return tostring(a.name) < tostring(b.name)
    end
    if sort == "popularity" then
      if (a.score or 0) ~= (b.score or 0) then
        return (a.score or 0) > (b.score or 0)
      end
      return (a.price or 0) < (b.price or 0)
    end
    if sort == "available" then
      if a.available ~= b.available then
        return a.available and not b.available
      end
      -- fall back to relevance if availability matches
      if a.score ~= b.score then
        return (a.score or 0) > (b.score or 0)
      end
      return (a.price or 0) < (b.price or 0)
    end
    if sort == "newest" then
      return (a.payload.updatedAt or 0) > (b.payload.updatedAt or 0)
    end
    -- default relevance
    if a.score ~= b.score then
      return (a.score or 0) > (b.score or 0)
    end
    return (a.price or 0) < (b.price or 0)
  end)
  return codec.ok {
    siteId = msg["Site-Id"],
    query = q,
    items = results,
    total = #results,
    facets = facets,
    suggestions = suggestions,
  }
end

function handlers.TrackCatalogEvent(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Sku", "Event" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Sku",
    "Event",
    "Subject",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then
    return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" })
  end
  local ok_len_sku, err_sku = validation.check_length(msg.Sku, 128, "Sku")
  if not ok_len_sku then
    return codec.error("INVALID_INPUT", err_sku, { field = "Sku" })
  end
  if msg.Subject then
    local ok_len_sub, err_sub = validation.check_length(msg.Subject, 128, "Subject")
    if not ok_len_sub then
      return codec.error("INVALID_INPUT", err_sub, { field = "Subject" })
    end
  end
  local ev = msg.Event
  if ev ~= "view" and ev ~= "add_to_cart" and ev ~= "purchase" then
    return codec.error("INVALID_INPUT", "Event must be view|add_to_cart|purchase")
  end
  local key = ids.product_key(msg["Site-Id"], msg.Sku)
  if not state.products[key] then
    return codec.error("NOT_FOUND", "Product not found", { sku = msg.Sku })
  end
  local stats = track_event(msg["Site-Id"], msg.Subject, msg.Sku, ev)
  record_telemetry("catalog_event", {
    siteId = msg["Site-Id"],
    sku = msg.Sku,
    subject = msg.Subject,
    event = ev,
  })
  audit.record("catalog", "TrackCatalogEvent", msg, nil, { event = ev, sku = msg.Sku })
  metrics.inc("catalog.TrackCatalogEvent." .. ev)
  metrics.tick()
  return codec.ok {
    siteId = msg["Site-Id"],
    sku = msg.Sku,
    stats = stats,
    recent = state.recent[msg.Subject],
  }
end

function handlers.RelatedProducts(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Sku" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Sku",
    "Limit",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local key = ids.product_key(msg["Site-Id"], msg.Sku)
  if not state.products[key] then
    return codec.error("NOT_FOUND", "Product not found", { sku = msg.Sku })
  end
  local limit = tonumber(msg.Limit) or 5
  if limit < 1 then
    limit = 1
  end
  if limit > 20 then
    limit = 20
  end

  local scores = state.events[msg["Site-Id"]] or {}
  local ranked = {}
  for sku, s in pairs(scores) do
    if sku ~= msg.Sku then
      local score = (s.views or 0) + 3 * (s.add_to_cart or 0) + 5 * (s.purchases or 0)
      local pkey = ids.product_key(msg["Site-Id"], sku)
      if state.products[pkey] then
        table.insert(ranked, { sku = sku, score = score, payload = state.products[pkey].payload })
      end
    end
  end
  table.sort(ranked, function(a, b)
    if a.score == b.score then
      return tostring(a.sku) < tostring(b.sku)
    end
    return a.score > b.score
  end)
  while #ranked > limit do
    table.remove(ranked)
  end
  return codec.ok { siteId = msg["Site-Id"], sku = msg.Sku, items = ranked, total = #ranked }
end

function handlers.RecentlyViewed(msg)
  local ok, missing = validation.require_fields(msg, { "Subject" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Subject",
    "Site-Id",
    "Limit",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local list = state.recent[msg.Subject] or {}
  local limit = tonumber(msg.Limit) or 10
  if limit < 1 then
    limit = 1
  end
  if limit > RECENT_LIMIT then
    limit = RECENT_LIMIT
  end
  local items = {}
  for _, entry in ipairs(list) do
    if #items >= limit then
      break
    end
    if (not msg["Site-Id"]) or entry.siteId == msg["Site-Id"] then
      local pkey = ids.product_key(entry.siteId, entry.sku)
      local product = state.products[pkey]
      if product then
        table.insert(items, { siteId = entry.siteId, sku = entry.sku, payload = product.payload })
      end
    end
  end
  return codec.ok { subject = msg.Subject, items = items, total = #items }
end

function handlers.Bestsellers(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Limit",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local limit = tonumber(msg.Limit) or 10
  if limit < 1 then
    limit = 1
  end
  if limit > 50 then
    limit = 50
  end
  local scores = state.events[msg["Site-Id"]] or {}
  local ranked = {}
  for sku, s in pairs(scores) do
    local score = (s.purchases or 0)
    if score > 0 then
      local pkey = ids.product_key(msg["Site-Id"], sku)
      if state.products[pkey] then
        table.insert(ranked, { sku = sku, score = score, payload = state.products[pkey].payload })
      end
    end
  end
  table.sort(ranked, function(a, b)
    if a.score == b.score then
      return tostring(a.sku) < tostring(b.sku)
    end
    return a.score > b.score
  end)
  while #ranked > limit do
    table.remove(ranked)
  end
  return codec.ok { siteId = msg["Site-Id"], items = ranked, total = #ranked }
end

function handlers.TrendingProducts(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Limit",
    "WindowSec",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local limit = tonumber(msg.Limit) or 10
  if limit < 1 then
    limit = 1
  end
  if limit > 50 then
    limit = 50
  end
  local window = tonumber(msg.WindowSec) or (7 * 24 * 3600)
  local cutoff = os.time() - window
  local log = state.event_log[msg["Site-Id"]] or {}
  local scores = {}
  for _, ev in ipairs(log) do
    if ev.ts >= cutoff then
      local w = (ev.event == "view" and 1)
        or (ev.event == "add_to_cart" and 3)
        or (ev.event == "purchase" and 5)
        or 0
      scores[ev.sku] = (scores[ev.sku] or 0) + w
    else
      break
    end
  end
  local ranked = {}
  for sku, score in pairs(scores) do
    local pkey = ids.product_key(msg["Site-Id"], sku)
    if state.products[pkey] then
      table.insert(ranked, { sku = sku, score = score, payload = state.products[pkey].payload })
    end
  end
  table.sort(ranked, function(a, b)
    if a.score == b.score then
      return tostring(a.sku) < tostring(b.sku)
    end
    return a.score > b.score
  end)
  while #ranked > limit do
    table.remove(ranked)
  end
  return codec.ok { siteId = msg["Site-Id"], items = ranked, total = #ranked, window = window }
end

function handlers.ExportEventLog(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Limit",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local limit = tonumber(msg.Limit) or 500
  if limit < 1 then
    limit = 1
  end
  if limit > EVENT_LOG_LIMIT then
    limit = EVENT_LOG_LIMIT
  end
  local log = state.event_log[msg["Site-Id"]] or {}
  local slice = {}
  for i = 1, math.min(limit, #log) do
    table.insert(slice, log[i])
  end
  return codec.ok { siteId = msg["Site-Id"], events = slice, total = #slice }
end

function handlers.StreamTelemetry(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Events",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  if not (GA4_ENDPOINT and GA4_API_SECRET and GA4_MEASUREMENT_ID) then
    return codec.error("PROVIDER_ERROR", "GA4 not configured")
  end
  local events = msg.Events or state.telemetry
  if type(events) ~= "table" then
    return codec.error("INVALID_INPUT", "Events must be array")
  end
  if #events == 0 then
    return codec.ok { streamed = 0 }
  end
  local payload = {
    client_id = "ao-catalog",
    measurement_id = GA4_MEASUREMENT_ID,
    api_secret = GA4_API_SECRET,
    events = {},
  }
  for _, ev in ipairs(events) do
    table.insert(payload.events, {
      name = ev.kind or "catalog_event",
      params = ev.data or ev,
    })
  end
  local out, err = http_post_json(GA4_ENDPOINT, payload)
  if err then
    return codec.error("PROVIDER_ERROR", err)
  end
  state.telemetry = {}
  return codec.ok { streamed = #payload.events, response = out }
end

local function verify_shared_secret(msg, secret)
  if not secret or secret == "" then
    return true
  end
  local sig = msg.Signature or msg.signature or msg.auth or msg["X-Signature"]
  if not sig then
    return false
  end
  return sig == secret
end

function handlers.HandlePaymentWebhook(msg)
  -- Accepts payload: Payment-Id, Status (authorized|captured|failed|disputed|refunded), Amount?
  local ok, missing = validation.require_fields(msg, { "Payment-Id", "Status" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  if not verify_shared_secret(msg, PAYMENT_WEBHOOK_SECRET) then
    return codec.error("FORBIDDEN", "Invalid webhook signature")
  end
  local pay = state.payments[msg["Payment-Id"]]
  if not pay then
    return codec.error("NOT_FOUND", "Payment not found")
  end
  local status = msg.Status
  local allowed = {
    authorized = true,
    requires_action = true,
    captured = true,
    failed = true,
    disputed = true,
    refunded = true,
  }
  if not allowed[status] then
    return codec.error("INVALID_INPUT", "Unsupported status")
  end
  pay.status = status
  pay.updatedAt = os.time()
  pay.gatewayPayload = msg.Payload or msg.payload
  if status == "captured" then
    pay.capturedAt = os.time()
  end
  if status == "refunded" then
    pay.refundedAt = os.time()
    pay.refundAmount = msg.Amount or pay.amount
  end
  if pay.orderId and state.orders[pay.orderId] then
    state.orders[pay.orderId].paymentStatus = status
    if status == "refunded" then
      state.orders[pay.orderId].refundAmount = msg.Amount or pay.amount
    end
    if status == "disputed" then
      state.orders[pay.orderId].status = "disputed"
    end
  end
  if pay.checkoutId and state.checkouts[pay.checkoutId] then
    state.checkouts[pay.checkoutId].paymentStatus = status
    state.checkouts[pay.checkoutId].status = status == "captured" and "paid" or status
  end
  audit.record(
    "catalog",
    "HandlePaymentWebhook",
    msg,
    nil,
    { paymentId = pay.paymentId, status = status }
  )
  return codec.ok { paymentId = pay.paymentId, status = status }
end

function handlers.HandleCarrierWebhook(msg)
  local ok, missing = validation.require_fields(msg, { "Shipment-Id", "Status" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  if not verify_shared_secret(msg, CARRIER_WEBHOOK_SECRET) then
    return codec.error("FORBIDDEN", "Invalid webhook signature")
  end
  state.shipments[msg["Shipment-Id"]] = state.shipments[msg["Shipment-Id"]] or {}
  local sh = state.shipments[msg["Shipment-Id"]]
  sh.status = msg.Status or sh.status
  sh.tracking = msg.Tracking or sh.tracking
  sh.eta = msg.Eta or sh.eta
  sh.updatedAt = os.time()
  record_shipment_event(
    msg["Shipment-Id"],
    sh.status,
    { source = "carrier", tracking = sh.tracking }
  )
  if sh.orderId and state.orders[sh.orderId] then
    if sh.status == "delivered" then
      state.orders[sh.orderId].status = "delivered"
    elseif sh.status == "exception" or sh.status == "delayed" then
      state.orders[sh.orderId].status = "shipment_issue"
    end
  end
  audit.record(
    "catalog",
    "HandleCarrierWebhook",
    msg,
    nil,
    { shipmentId = msg["Shipment-Id"], status = sh.status }
  )
  return codec.ok(sh)
end

local function sign_hmac(body)
  if not JWT_HMAC_SECRET then
    return nil, "SECRET_MISSING"
  end
  return auth.hmac(body, JWT_HMAC_SECRET)
end

function handlers.SignPayload(msg)
  local ok, missing = validation.require_fields(msg, { "Payload" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Payload",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  if not json_ok then
    return codec.error("PROVIDER_ERROR", "json not available")
  end
  local ok_enc, body = pcall(cjson.encode, msg.Payload)
  if not ok_enc then
    return codec.error("INVALID_INPUT", "Payload not encodable")
  end
  local sig, err = sign_hmac(body)
  if not sig then
    return codec.error("PROVIDER_ERROR", err)
  end
  return codec.ok { signature = sig }
end

function handlers.VerifySignature(msg)
  local ok, missing = validation.require_fields(msg, { "Payload", "Signature" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  if not json_ok then
    return codec.error("PROVIDER_ERROR", "json not available")
  end
  local ok_enc, body = pcall(cjson.encode, msg.Payload)
  if not ok_enc then
    return codec.error("INVALID_INPUT", "Payload not encodable")
  end
  local expected, err = sign_hmac(body)
  if not expected then
    return codec.error("PROVIDER_ERROR", err)
  end
  if expected ~= msg.Signature then
    return codec.error("FORBIDDEN", "Signature mismatch")
  end
  return codec.ok { valid = true }
end

function handlers.CreateWebhook(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Url", "Events" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Url",
    "Events",
    "Secret",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_events, err_events = validation.assert_type(msg.Events, "table", "Events")
  if not ok_events then
    return codec.error("INVALID_INPUT", err_events, { field = "Events" })
  end
  local id = gen_id "wh"
  state.webhooks[msg["Site-Id"]] = state.webhooks[msg["Site-Id"]] or {}
  state.webhooks[msg["Site-Id"]][id] = {
    url = msg.Url,
    secret = msg.Secret,
    events = msg.Events,
    createdAt = os.time(),
  }
  audit.record("catalog", "CreateWebhook", msg, nil, { siteId = msg["Site-Id"], webhookId = id })
  return codec.ok { webhookId = id }
end

function handlers.GetWebhook(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Webhook-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Webhook-Id",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local wh = state.webhooks[msg["Site-Id"]] and state.webhooks[msg["Site-Id"]][msg["Webhook-Id"]]
  if not wh then
    return codec.error("NOT_FOUND", "Webhook not found")
  end
  return codec.ok { webhookId = msg["Webhook-Id"], webhook = wh }
end

function handlers.ListWebhooks(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local list = {}
  for id, wh in pairs(state.webhooks[msg["Site-Id"]] or {}) do
    table.insert(list, { webhookId = id, webhook = wh })
  end
  return codec.ok { siteId = msg["Site-Id"], items = list, total = #list }
end

function handlers.DeleteWebhook(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Webhook-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Webhook-Id",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  if state.webhooks[msg["Site-Id"]] then
    state.webhooks[msg["Site-Id"]][msg["Webhook-Id"]] = nil
  end
  audit.record("catalog", "DeleteWebhook", msg, nil, { webhookId = msg["Webhook-Id"] })
  return codec.ok { deleted = msg["Webhook-Id"] }
end

function handlers.ExportCatalogFeed(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Cursor",
    "Limit",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local site = msg["Site-Id"]
  if not site then
    return codec.error("INVALID_INPUT", "Site-Id required")
  end
  local cursor = msg.Cursor or ""
  local limit = tonumber(msg.Limit) or 200
  if limit < 1 then
    limit = 1
  end
  if limit > 500 then
    limit = 500
  end
  local prefix = "product:" .. site .. ":"
  local items = {}
  local keys = {}
  for key, _ in pairs(state.products) do
    if key:sub(1, #prefix) == prefix then
      table.insert(keys, key)
    end
  end
  table.sort(keys)
  local start_index = 1
  if cursor ~= "" then
    for i, k in ipairs(keys) do
      if k == cursor then
        start_index = i + 1
        break
      end
    end
  end
  for i = start_index, math.min(#keys, start_index + limit - 1) do
    local key = keys[i]
    table.insert(items, { key = key, payload = state.products[key].payload })
  end
  local next_cursor = (#keys > start_index + limit - 1) and keys[start_index + limit - 1] or nil
  return codec.ok { siteId = site, items = items, nextCursor = next_cursor, total = #items }
end

function handlers.ExportCategoryFeed(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Cursor",
    "Limit",
    "UpdatedAfter",
    "IncludeDeleted",
    "Path",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local site = msg["Site-Id"]
  if not site then
    return codec.error("INVALID_INPUT", "Site-Id required")
  end
  local updated_after = tonumber(msg.UpdatedAfter) or 0
  local cursor = msg.Cursor or ""
  local limit = tonumber(msg.Limit) or 200
  if limit < 1 then
    limit = 1
  end
  if limit > 500 then
    limit = 500
  end
  local prefix = "category:" .. site .. ":"
  local keys = {}
  for key, cat in pairs(state.categories) do
    if key:sub(1, #prefix) == prefix and (cat.updatedAt or 0) >= updated_after then
      table.insert(keys, key)
    end
  end
  table.sort(keys)
  local start_index = 1
  if cursor ~= "" then
    for i, k in ipairs(keys) do
      if k == cursor then
        start_index = i + 1
        break
      end
    end
  end
  local items = {}
  for i = start_index, math.min(#keys, start_index + limit - 1) do
    local key = keys[i]
    local cat = state.categories[key]
    table.insert(items, {
      key = key,
      payload = cat.payload,
      products = cat.products,
      updatedAt = cat.updatedAt,
      categoryId = key:match "category:[^:]+:(.+)",
    })
  end
  if msg.IncludeDeleted and state.category_deletions[site] then
    for _, d in ipairs(state.category_deletions[site]) do
      if (d.deletedAt or 0) >= updated_after then
        table.insert(items, { key = d.key, deletedAt = d.deletedAt, deleted = true })
      end
    end
  end
  local next_cursor = (#keys > start_index + limit - 1) and keys[start_index + limit - 1] or nil
  if msg.Path then
    local f = io.open(msg.Path, "w")
    if f and json_ok then
      for _, item in ipairs(items) do
        local ok_line, line = pcall(cjson.encode, item)
        if ok_line and line then
          f:write(line)
          f:write "\n"
        end
      end
      f:close()
    end
  end
  return codec.ok {
    siteId = site,
    items = items,
    nextCursor = next_cursor,
    total = #items,
    updatedAfter = updated_after,
    includeDeleted = msg.IncludeDeleted or false,
  }
end

function handlers.ExportSearchFeed(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Cursor",
    "Limit",
    "UpdatedAfter",
    "Path",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local site = msg["Site-Id"]
  if not site then
    return codec.error("INVALID_INPUT", "Site-Id required")
  end
  local updated_after = tonumber(msg.UpdatedAfter) or 0
  local limit = tonumber(msg.Limit) or 500
  if limit < 1 then
    limit = 1
  end
  if limit > 2000 then
    limit = 2000
  end
  local cursor = msg.Cursor or ""
  local prefix = "product:" .. site .. ":"
  local keys = {}
  for key, product in pairs(state.products) do
    if key:sub(1, #prefix) == prefix then
      local updated = product.payload.updatedAt or product.payload.updated_at or 0
      if updated >= updated_after then
        table.insert(keys, { key = key, updated = updated })
      end
    end
  end
  table.sort(keys, function(a, b)
    if a.updated == b.updated then
      return a.key < b.key
    end
    return (a.updated or 0) > (b.updated or 0)
  end)
  local start_index = 1
  if cursor ~= "" then
    for i, row in ipairs(keys) do
      if row.key == cursor then
        start_index = i + 1
        break
      end
    end
  end
  local items = {}
  for i = start_index, math.min(#keys, start_index + limit - 1) do
    local row = keys[i]
    table.insert(
      items,
      { key = row.key, updatedAt = row.updated, payload = state.products[row.key].payload }
    )
  end
  -- include deletions as tombstones if requested
  if msg.IncludeDeleted and state.deletions[site] then
    for _, d in ipairs(state.deletions[site]) do
      if (d.deletedAt or 0) >= updated_after then
        table.insert(items, { key = d.key, deletedAt = d.deletedAt, deleted = true })
      end
    end
  end
  local next_cursor = (#keys > start_index + limit - 1) and keys[start_index + limit - 1].key or nil
  -- optional NDJSON export
  if msg.Path or FEED_EXPORT_PATH then
    local path = msg.Path or FEED_EXPORT_PATH
    local f = io.open(path, "w")
    if f and json_ok then
      for _, item in ipairs(items) do
        local ok_line, line = pcall(cjson.encode, item)
        if ok_line and line then
          f:write(line)
          f:write "\n"
        end
      end
      f:close()
    end
  end
  return codec.ok {
    siteId = site,
    items = items,
    nextCursor = next_cursor,
    total = #items,
    updatedAfter = updated_after,
    includeDeleted = msg.IncludeDeleted or false,
  }
end

function handlers.ExportMerchantFeed(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Limit",
    "Cursor",
    "Path",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local site = msg["Site-Id"]
  if not site then
    return codec.error("INVALID_INPUT", "Site-Id required")
  end
  local updated_after = tonumber(msg.UpdatedAfter) or 0
  local limit = tonumber(msg.Limit) or 1000
  if limit < 1 then
    limit = 1
  end
  if limit > 5000 then
    limit = 5000
  end
  local cursor = msg.Cursor or ""
  local prefix = "product:" .. site .. ":"
  local keys = {}
  for key, product in pairs(state.products) do
    if key:sub(1, #prefix) == prefix then
      local updated = product.payload.updatedAt or product.payload.updated_at or 0
      if updated >= updated_after then
        table.insert(keys, { key = key, updated = updated })
      end
    end
  end
  table.sort(keys, function(a, b)
    if a.updated == b.updated then
      return a.key < b.key
    end
    return (a.updated or 0) > (b.updated or 0)
  end)
  local start_index = 1
  if cursor ~= "" then
    for i, row in ipairs(keys) do
      if row.key == cursor then
        start_index = i + 1
        break
      end
    end
  end
  local rows = {}
  for i = start_index, math.min(#keys, start_index + limit - 1) do
    local row = keys[i]
    local p = state.products[row.key].payload
    table.insert(rows, {
      id = p.sku or row.key,
      title = p.name,
      description = p.description,
      link = p.url or p.Link,
      image_link = (p.assets and p.assets[1]) or nil,
      availability = p.available and "in stock" or "out of stock",
      price = string.format("%.2f %s", p.price or 0, p.currency or MERCHANT_CENTER_CURRENCY),
      brand = p.brand,
      gtin = p.gtin,
      mpn = p.mpn,
      condition = p.condition or "new",
      shipping = {
        country = MERCHANT_CENTER_COUNTRY,
        service = p.shippingService or "Standard",
        price = string.format(
          "%.2f %s",
          (p.shipping and p.shipping.price) or 0,
          p.currency or MERCHANT_CENTER_CURRENCY
        ),
      },
      updatedAt = row.updated,
    })
  end
  if msg.IncludeDeleted and state.deletions[site] then
    for _, d in ipairs(state.deletions[site]) do
      if (d.deletedAt or 0) >= updated_after then
        table.insert(rows, { id = d.key, deleted = true, deletedAt = d.deletedAt })
      end
    end
  end
  local next_cursor = (#keys > start_index + limit - 1) and keys[start_index + limit - 1].key or nil
  if msg.Path or MERCHANT_CENTER_PATH then
    local path = msg.Path or MERCHANT_CENTER_PATH
    local f = io.open(path, "w")
    if f then
      for _, r in ipairs(rows) do
        f:write(table.concat({
          r.id,
          r.title or "",
          r.description or "",
          r.link or "",
          r.image_link or "",
          r.availability or "",
          r.price or "",
          r.brand or "",
          r.gtin or "",
          r.mpn or "",
          r.condition or "",
          r.shipping.country or "",
          r.shipping.service or "",
          r.shipping.price or "",
        }, ","))
        f:write "\n"
      end
      f:close()
    end
  end
  return codec.ok {
    siteId = site,
    items = rows,
    nextCursor = next_cursor,
    total = #rows,
    updatedAfter = updated_after,
    includeDeleted = msg.IncludeDeleted or false,
  }
end

-- Cache purge stub -------------------------------------------------------
function handlers.PurgeCache(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Path",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local path = msg.Path or "/*"
  local result = { purged = path }
  if CDN_PURGE_CMD and CDN_PURGE_CMD ~= "" then
    local cmd = string.format(CDN_PURGE_CMD, path)
    local rc = os.execute(cmd)
    result.command = cmd
    result.success = (rc == true or rc == 0)
  end
  audit.record("catalog", "PurgeCache", msg, nil, { siteId = msg["Site-Id"], path = path })
  return codec.ok(result)
end

function handlers.ApplyOrderEvent(msg)
  local ok, missing = validation.require_fields(msg, { "Event" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing Event", { missing = missing })
  end
  local ev = msg.Event
  if type(ev) ~= "table" or not ev.type then
    return codec.error("INVALID_INPUT", "Event.type required")
  end
  -- allow verification with hmac if present
  msg["Order-Id"] = msg["Order-Id"] or ev.orderId or ev["Order-Id"]
  local ok_hmac, hmac_err = auth.verify_outbox_hmac(msg)
  if not ok_hmac then
    return codec.error("FORBIDDEN", hmac_err)
  end

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
  metrics.inc "catalog.ApplyOrderEvent.count"
  metrics.tick()
  return codec.ok { applied = ev.type, orderId = ev.orderId }
end

function handlers.GetOrder(msg)
  local ok, missing = validation.require_fields(msg, { "Order-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Order-Id required", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Order-Id", "Actor-Role", "Schema-Version" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local order = state.orders[msg["Order-Id"]]
  if not order then
    return codec.error("NOT_FOUND", "order not found")
  end
  return codec.ok { orderId = msg["Order-Id"], order = order }
end

function handlers.ListOrders(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Customer-Id",
    "Status",
    "Limit",
    "Offset",
    "Actor-Role",
    "Schema-Version",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local limit = tonumber(msg.Limit) or 50
  local offset = tonumber(msg.Offset) or 0
  local items = {}
  for oid, o in pairs(state.orders) do
    if
      (not msg["Site-Id"] or o.siteId == msg["Site-Id"])
      and (not msg["Customer-Id"] or o.customerId == msg["Customer-Id"])
      and (not msg.Status or o.status == msg.Status)
    then
      table.insert(items, { orderId = oid, order = o })
    end
  end
  table.sort(items, function(a, b)
    return (a.order.updatedAt or 0) > (b.order.updatedAt or 0)
  end)
  local slice = {}
  for i = offset + 1, math.min(#items, offset + limit) do
    table.insert(slice, items[i])
  end
  return codec.ok { total = #items, items = slice }
end

function handlers.SetInventoryReservation(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Order-Id", "Items" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Order-Id", "Items", "Actor-Role", "Schema-Version" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_items, err_items = validation.assert_type(msg.Items, "table", "Items")
  if not ok_items then
    return codec.error("INVALID_INPUT", err_items, { field = "Items" })
  end
  for _, item in ipairs(msg.Items) do
    if not (item.sku and item.qty) then
      return codec.error("INVALID_INPUT", "Item must have sku and qty")
    end
  end
  state.reservations[msg["Order-Id"]] =
    { siteId = msg["Site-Id"], items = msg.Items, released = false }
  return codec.ok { orderId = msg["Order-Id"], reserved = #msg.Items }
end

local function adjust_inventory(siteId, items, sign)
  state.inventory[siteId] = state.inventory[siteId] or {}
  local inv = state.inventory[siteId]
  for _, item in ipairs(items or {}) do
    local wh = item.warehouse or "default"
    inv[wh] = inv[wh] or {}
    inv[wh][item.sku] = math.max(0, (inv[wh][item.sku] or 0) + sign * (item.qty or 0))
  end
end

function handlers.SyncShipment(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Order-Id", "Status" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local res = state.reservations[msg["Order-Id"]]
  if res and not res.released and (msg.Status == "shipped" or msg.Status == "delivered") then
    adjust_inventory(res.siteId, res.items, -1)
    res.released = true
  end
  return codec.ok { orderId = msg["Order-Id"], released = res and res.released or false }
end

function handlers.SyncReturn(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Order-Id", "Status" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local res = state.reservations[msg["Order-Id"]]
  if res and (msg.Status == "approved" or msg.Status == "refunded") then
    adjust_inventory(res.siteId, res.items, 1)
  end
  return codec.ok { orderId = msg["Order-Id"], restocked = res ~= nil }
end

function handlers.GetShippingRates(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Actor-Role", "Schema-Version" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local rates = state.shipping_rates[msg["Site-Id"]] or {}
  return codec.ok { siteId = msg["Site-Id"], rates = rates }
end

function handlers.GetTaxRates(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Actor-Role", "Schema-Version" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local rates = state.tax_rates[msg["Site-Id"]] or {}
  return codec.ok { siteId = msg["Site-Id"], rates = rates }
end

function handlers.ValidateAddress(msg)
  local ok, missing = validation.require_fields(msg, { "Country" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Country",
    "Region",
    "City",
    "Postal",
    "Line1",
    "Line2",
    "Actor-Role",
    "Schema-Version",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  if #msg.Country ~= 2 then
    return codec.error("INVALID_INPUT", "Country must be ISO2")
  end
  local postal_re = os.getenv "ADDRESS_POSTAL_REGEX"
  if postal_re and msg.Postal and not tostring(msg.Postal):match(postal_re) then
    return codec.error("INVALID_INPUT", "Postal format invalid", { field = "Postal" })
  end

  local validated = {
    country = msg.Country:upper(),
    region = msg.Region,
    city = msg.City,
    postal = msg.Postal,
    line1 = msg.Line1,
    line2 = msg.Line2,
  }

  local cmd = os.getenv "ADDRESS_VALIDATE_CMD"
  if cmd and cmd ~= "" then
    local pipe = io.popen(cmd, "r")
    if pipe then
      local out = pipe:read "*a"
      pipe:close()
      if out and #out > 0 then
        local ok_json, obj = pcall(cjson.decode, out)
        if ok_json and obj and obj.normalized then
          validated = obj.normalized
        elseif os.getenv "ADDRESS_VALIDATE_STRICT" == "1" then
          return codec.error("PROVIDER_ERROR", "address_validate_failed", { output = out })
        end
      elseif os.getenv "ADDRESS_VALIDATE_STRICT" == "1" then
        return codec.error("PROVIDER_ERROR", "address_validate_empty")
      end
    elseif os.getenv "ADDRESS_VALIDATE_STRICT" == "1" then
      return codec.error("PROVIDER_ERROR", "address_validate_io")
    end
  end

  return codec.ok {
    valid = true,
    normalized = validated,
  }
end

function handlers.GetShipment(msg)
  local ok, missing = validation.require_fields(msg, { "Shipment-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Shipment-Id", "Actor-Role", "Schema-Version" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local sh = state.shipments[msg["Shipment-Id"]]
  if not sh then
    return codec.error("NOT_FOUND", "Shipment not found")
  end
  return codec.ok(sh)
end

function handlers.ApplyShipmentEvent(msg)
  local ok, missing = validation.require_fields(msg, { "Shipment-Id", "Order-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Shipment-Id",
    "Order-Id",
    "Carrier",
    "Service",
    "Label-Url",
    "Tracking",
    "Tracking-Url",
    "Eta",
    "Status",
    "Actor-Role",
    "Schema-Version",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  state.shipments[msg["Shipment-Id"]] = state.shipments[msg["Shipment-Id"]] or {}
  local sh = state.shipments[msg["Shipment-Id"]]
  sh.orderId = msg["Order-Id"]
  sh.carrier = msg.Carrier or sh.carrier
  sh.service = msg.Service or sh.service
  sh.labelUrl = msg["Label-Url"] or sh.labelUrl
  sh.tracking = msg.Tracking or sh.tracking
  sh.trackingUrl = msg["Tracking-Url"] or sh.trackingUrl
  sh.eta = msg.Eta or sh.eta
  sh.status = msg.Status or sh.status or "pending"
  record_shipment_event(msg["Shipment-Id"], sh.status, { source = "apply", tracking = sh.tracking })
  audit.record("catalog", "ApplyShipmentEvent", msg, nil, { shipment = msg["Shipment-Id"] })
  return codec.ok {
    shipmentId = msg["Shipment-Id"],
    status = sh.status,
    carrier = sh.carrier,
    service = sh.service,
    labelUrl = sh.labelUrl,
    tracking = sh.tracking,
    trackingUrl = sh.trackingUrl,
    eta = sh.eta,
  }
end

function handlers.ApplyTrackingEvent(msg)
  local ok, missing = validation.require_fields(msg, { "Shipment-Id", "Tracking" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Shipment-Id",
    "Tracking",
    "Carrier",
    "Eta",
    "Tracking-Url",
    "Status",
    "Actor-Role",
    "Schema-Version",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  state.shipments[msg["Shipment-Id"]] = state.shipments[msg["Shipment-Id"]] or {}
  local sh = state.shipments[msg["Shipment-Id"]]
  sh.tracking = msg.Tracking
  sh.trackingUrl = msg["Tracking-Url"] or sh.trackingUrl
  sh.eta = msg.Eta or sh.eta
  sh.carrier = msg.Carrier or sh.carrier
  sh.status = msg.Status or sh.status
  record_shipment_event(
    msg["Shipment-Id"],
    sh.status or "in_transit",
    { source = "track", tracking = sh.tracking }
  )
  audit.record(
    "catalog",
    "ApplyTrackingEvent",
    msg,
    nil,
    { shipment = msg["Shipment-Id"], tracking = msg.Tracking }
  )
  return codec.ok {
    shipmentId = msg["Shipment-Id"],
    tracking = sh.tracking,
    trackingUrl = sh.trackingUrl,
    eta = sh.eta,
    status = sh.status,
  }
end

function handlers.CreateShippingLabel(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Order-Id", "Carrier" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Order-Id",
    "Carrier",
    "Service",
    "Address",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local carrier = msg.Carrier
  local service = msg.Service or "standard"
  local order = state.orders[msg["Order-Id"]] or {}
  local ship_to = msg.Address or order.address
  if type(ship_to) ~= "table" or not ship_to.Country then
    return codec.error("INVALID_INPUT", "Address.Country required for label")
  end
  local weight = 0
  if order.items then
    for _, it in ipairs(order.items) do
      local pkey = ids.product_key(msg["Site-Id"], it.sku or it.Sku or "")
      local payload = state.products[pkey] and state.products[pkey].payload or {}
      weight = weight + (payload.weight or payload.Weight or 0) * (it.qty or it.Qty or 1)
    end
  end
  local label = build_label(carrier, service, weight)
  label.orderId = msg["Order-Id"]
  label.siteId = msg["Site-Id"]
  label.address = ship_to
  label.eta = order.eta
  label.createdAt = os.time()
  state.shipments[label.shipmentId] = label
  audit.record(
    "catalog",
    "CreateShippingLabel",
    msg,
    nil,
    { shipmentId = label.shipmentId, orderId = msg["Order-Id"], carrier = carrier }
  )
  return codec.ok(label)
end

function handlers.UpsertProduct(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Sku", "Payload" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Sku",
    "Payload",
    "Version",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then
    return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" })
  end
  local ok_len_sku, err_sku = validation.check_length(msg.Sku, 128, "Sku")
  if not ok_len_sku then
    return codec.error("INVALID_INPUT", err_sku, { field = "Sku" })
  end
  if msg.Version then
    local ok_len_ver, err_ver = validation.check_length(msg.Version, 128, "Version")
    if not ok_len_ver then
      return codec.error("INVALID_INPUT", err_ver, { field = "Version" })
    end
  end
  local ok_type_payload, err_type_payload = validation.assert_type(msg.Payload, "table", "Payload")
  if not ok_type_payload then
    return codec.error("INVALID_INPUT", err_type_payload, { field = "Payload" })
  end
  if not msg.Payload.sku then
    msg.Payload.sku = msg.Sku
  end
  if msg.Payload.sku ~= msg.Sku then
    return codec.error("INVALID_INPUT", "Payload sku must match Sku field", { field = "Sku" })
  end
  local payload_len = validation.estimate_json_length(msg.Payload)
  local ok_size, err_size = validation.check_size(payload_len, MAX_PAYLOAD_BYTES, "Payload")
  if not ok_size then
    return codec.error("INVALID_INPUT", err_size, { field = "Payload" })
  end
  if msg.Payload.taxClass and type(msg.Payload.taxClass) ~= "string" then
    return codec.error("INVALID_INPUT", "taxClass must be string", { field = "Payload.taxClass" })
  end
  if msg.Payload.taxClass and #msg.Payload.taxClass > 64 then
    return codec.error("INVALID_INPUT", "taxClass too long", { field = "Payload.taxClass" })
  end
  local ok_schema, schema_err = schema.validate("product", msg.Payload)
  if not ok_schema then
    return codec.error("INVALID_INPUT", "Payload failed schema", { errors = schema_err })
  end
  local key = ids.product_key(msg["Site-Id"], msg.Sku)
  state.products[key] = { payload = msg.Payload, version = msg.Version }
  audit.record("catalog", "UpsertProduct", msg, nil, { sku = msg.Sku })
  purge_paths {
    "/p/" .. msg.Sku,
    "/api/catalog/" .. msg.Sku,
  }
  return codec.ok { sku = msg.Sku }
end

function handlers.DeleteProduct(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Sku" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Sku", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local key = ids.product_key(msg["Site-Id"], msg.Sku)
  state.products[key] = nil
  state.deletions[msg["Site-Id"]] = state.deletions[msg["Site-Id"]] or {}
  table.insert(state.deletions[msg["Site-Id"]], { key = key, deletedAt = os.time() })
  audit.record("catalog", "DeleteProduct", msg, nil, { sku = msg.Sku })
  purge_paths {
    "/p/" .. msg.Sku,
    "/api/catalog/" .. msg.Sku,
  }
  return codec.ok { deleted = msg.Sku }
end

function handlers.UpsertVariants(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Parent-Sku", "Variants" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Parent-Sku",
    "Variants",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then
    return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" })
  end
  local ok_len_parent, err_parent = validation.check_length(msg["Parent-Sku"], 128, "Parent-Sku")
  if not ok_len_parent then
    return codec.error("INVALID_INPUT", err_parent, { field = "Parent-Sku" })
  end
  local ok_type, err_type = validation.assert_type(msg.Variants, "table", "Variants")
  if not ok_type then
    return codec.error("INVALID_INPUT", err_type, { field = "Variants" })
  end
  if #msg.Variants == 0 then
    return codec.error("INVALID_INPUT", "Variants must be non-empty", { field = "Variants" })
  end
  state.variants[msg["Site-Id"]] = state.variants[msg["Site-Id"]] or {}
  state.variants[msg["Site-Id"]][msg["Parent-Sku"]] = { variants = {} }
  for _, v in ipairs(msg.Variants) do
    if not v.sku or not v.attrs then
      return codec.error("INVALID_INPUT", "Variant requires sku and attrs", { variant = v })
    end
    local payload_len = validation.estimate_json_length(v)
    local ok_size, err_size = validation.check_size(payload_len, MAX_PAYLOAD_BYTES, "Variant")
    if not ok_size then
      return codec.error("INVALID_INPUT", err_size, { field = "Variants" })
    end
    table.insert(state.variants[msg["Site-Id"]][msg["Parent-Sku"]].variants, v)
  end
  audit.record(
    "catalog",
    "UpsertVariants",
    msg,
    nil,
    { parent = msg["Parent-Sku"], count = #msg.Variants }
  )
  return codec.ok {
    parentSku = msg["Parent-Sku"],
    variants = state.variants[msg["Site-Id"]][msg["Parent-Sku"]],
  }
end

function handlers.UpsertCategory(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Category-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Category-Id",
    "Payload",
    "Products",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then
    return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" })
  end
  local ok_len_cat, err_cat = validation.check_length(msg["Category-Id"], 128, "Category-Id")
  if not ok_len_cat then
    return codec.error("INVALID_INPUT", err_cat, { field = "Category-Id" })
  end
  if msg.Payload then
    local ok_type_payload, err_type_payload =
      validation.assert_type(msg.Payload, "table", "Payload")
    if not ok_type_payload then
      return codec.error("INVALID_INPUT", err_type_payload, { field = "Payload" })
    end
  end
  if msg.Products then
    local ok_type_products, err_type_products =
      validation.assert_type(msg.Products, "table", "Products")
    if not ok_type_products then
      return codec.error("INVALID_INPUT", err_type_products, { field = "Products" })
    end
  end
  local payload_len = validation.estimate_json_length(msg.Payload or {})
  local ok_size, err_size = validation.check_size(payload_len, MAX_PAYLOAD_BYTES, "Payload")
  if not ok_size then
    return codec.error("INVALID_INPUT", err_size, { field = "Payload" })
  end
  local key = ids.category_key(msg["Site-Id"], msg["Category-Id"])
  state.categories[key] = {
    payload = msg.Payload or {},
    products = msg.Products or state.categories[key] and state.categories[key].products or {},
    updatedAt = os.time(),
  }
  purge_paths {
    "/c/" .. msg["Category-Id"],
    "/api/catalog/category/" .. msg["Category-Id"],
  }
  return codec.ok { categoryId = msg["Category-Id"] }
end

function handlers.DeleteCategory(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Category-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Category-Id",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then
    return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" })
  end
  local ok_len_cat, err_cat = validation.check_length(msg["Category-Id"], 128, "Category-Id")
  if not ok_len_cat then
    return codec.error("INVALID_INPUT", err_cat, { field = "Category-Id" })
  end
  local key = ids.category_key(msg["Site-Id"], msg["Category-Id"])
  state.categories[key] = nil
  state.category_deletions[msg["Site-Id"]] = state.category_deletions[msg["Site-Id"]] or {}
  table.insert(state.category_deletions[msg["Site-Id"]], { key = key, deletedAt = os.time() })
  audit.record("catalog", "DeleteCategory", msg, nil, { categoryId = msg["Category-Id"] })
  purge_paths {
    "/c/" .. msg["Category-Id"],
    "/api/catalog/category/" .. msg["Category-Id"],
  }
  return codec.ok { deleted = msg["Category-Id"] }
end

function handlers.GetCategory(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Category-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Category-Id",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local key = ids.category_key(msg["Site-Id"], msg["Category-Id"])
  local cat = state.categories[key]
  if not cat then
    return codec.error("NOT_FOUND", "Category not found", { categoryId = msg["Category-Id"] })
  end
  return codec.ok {
    siteId = msg["Site-Id"],
    categoryId = msg["Category-Id"],
    payload = cat.payload,
    products = cat.products,
    updatedAt = cat.updatedAt,
  }
end

function handlers.ListCategories(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Cursor",
    "Limit",
    "UpdatedAfter",
    "IncludeDeleted",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local site = msg["Site-Id"]
  local updated_after = tonumber(msg.UpdatedAfter) or 0
  local cursor = msg.Cursor or ""
  local limit = tonumber(msg.Limit) or 200
  if limit < 1 then
    limit = 1
  end
  if limit > 500 then
    limit = 500
  end
  local prefix = "category:" .. site .. ":"
  local keys = {}
  for key, cat in pairs(state.categories) do
    if key:sub(1, #prefix) == prefix and (cat.updatedAt or 0) >= updated_after then
      table.insert(keys, key)
    end
  end
  table.sort(keys)
  local start_index = 1
  if cursor ~= "" then
    for i, k in ipairs(keys) do
      if k == cursor then
        start_index = i + 1
        break
      end
    end
  end
  local items = {}
  for i = start_index, math.min(#keys, start_index + limit - 1) do
    local key = keys[i]
    local cat = state.categories[key]
    table.insert(items, {
      key = key,
      categoryId = key:match "category:[^:]+:(.+)",
      payload = cat.payload,
      products = cat.products,
      updatedAt = cat.updatedAt,
    })
  end
  if msg.IncludeDeleted and state.category_deletions[site] then
    for _, d in ipairs(state.category_deletions[site]) do
      if (d.deletedAt or 0) >= updated_after then
        table.insert(items, { key = d.key, deletedAt = d.deletedAt, deleted = true })
      end
    end
  end
  local next_cursor = (#keys > start_index + limit - 1) and keys[start_index + limit - 1] or nil
  return codec.ok { siteId = site, items = items, nextCursor = next_cursor, total = #items }
end

function handlers.PublishCatalogVersion(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Version" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Version",
    "ExpectedVersion",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then
    return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" })
  end
  local ok_len_ver, err_ver = validation.check_length(msg.Version, 128, "Version")
  if not ok_len_ver then
    return codec.error("INVALID_INPUT", err_ver, { field = "Version" })
  end
  if msg.ExpectedVersion then
    local ok_len_exp, err_exp = validation.check_length(msg.ExpectedVersion, 128, "ExpectedVersion")
    if not ok_len_exp then
      return codec.error("INVALID_INPUT", err_exp, { field = "ExpectedVersion" })
    end
  end
  local current = state.active_versions[msg["Site-Id"]]
  if msg.ExpectedVersion and current and current ~= msg.ExpectedVersion then
    return codec.error(
      "VERSION_CONFLICT",
      "ExpectedVersion mismatch",
      { expected = msg.ExpectedVersion, current = current }
    )
  end
  state.active_versions[msg["Site-Id"]] = msg.Version
  local resp = codec.ok { siteId = msg["Site-Id"], activeVersion = msg.Version }
  audit.record("catalog", "PublishCatalogVersion", msg, resp)
  return resp
end

-- Price lists per currency ------------------------------------------------
function handlers.SetPriceList(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Currency", "Prices" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Currency",
    "Prices",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local currency = msg.Currency:upper()
  if not currency:match "^[A-Z][A-Z][A-Z]$" then
    return codec.error("INVALID_INPUT", "Currency must be ISO 4217 code", { field = "Currency" })
  end
  local ok_type, err_type = validation.assert_type(msg.Prices, "table", "Prices")
  if not ok_type then
    return codec.error("INVALID_INPUT", err_type, { field = "Prices" })
  end
  state.price_lists[msg["Site-Id"]] = state.price_lists[msg["Site-Id"]] or {}
  state.price_lists[msg["Site-Id"]][currency] = msg.Prices
  audit.record(
    "catalog",
    "SetPriceList",
    msg,
    nil,
    { siteId = msg["Site-Id"], currency = currency }
  )
  return codec.ok { siteId = msg["Site-Id"], currency = currency, count = #msg.Prices }
end

-- Promos ------------------------------------------------------------------
function handlers.AddPromo(msg)
  local ok, missing = validation.require_fields(msg, { "Code", "Type", "Value" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Code",
    "Type",
    "Value",
    "Skus",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local typ = msg.Type
  if typ ~= "percent" and typ ~= "amount" then
    return codec.error("INVALID_INPUT", "Type must be percent|amount", { field = "Type" })
  end
  local value = tonumber(msg.Value)
  if not value or value <= 0 then
    return codec.error("INVALID_INPUT", "Value must be positive number", { field = "Value" })
  end
  local skus = msg.Skus or {}
  state.promos[msg.Code] = { type = typ, value = value, skus = skus }
  audit.record("catalog", "AddPromo", msg, nil, { code = msg.Code, type = typ })
  return codec.ok { code = msg.Code, type = typ, value = value }
end

local function apply_pricing(site_id, sku, currency, promo_code)
  local product_key = ids.product_key(site_id, sku)
  local product = state.products[product_key]
  if not product then
    return nil, "NOT_FOUND"
  end
  local price = product.payload.price
  local base_currency = product.payload.currency or currency
  local price_lists = state.price_lists[site_id]
  if price_lists and price_lists[currency] and price_lists[currency][sku] then
    price = price_lists[currency][sku]
    base_currency = currency
  end
  if promo_code and state.promos[promo_code] then
    local promo = state.promos[promo_code]
    if #promo.skus == 0 then
      -- applies to all
    else
      local applies = false
      for _, s in ipairs(promo.skus) do
        if s == sku then
          applies = true
          break
        end
      end
      if not applies then
        return { price = price, currency = base_currency }, nil
      end
    end
    if promo.type == "percent" then
      price = price * (1 - promo.value / 100)
    elseif promo.type == "amount" then
      price = math.max(0, price - promo.value)
    end
  end
  return { price = price, currency = base_currency }, nil
end

function handlers.QuotePrice(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Sku" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Sku",
    "Currency",
    "Promo",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local currency = msg.Currency or "USD"
  local quote, err = apply_pricing(msg["Site-Id"], msg.Sku, currency, msg.Promo)
  if not quote then
    return codec.error("NOT_FOUND", "Product not found", { sku = msg.Sku })
  end
  return codec.ok { sku = msg.Sku, price = quote.price, currency = quote.currency, promo = msg.Promo }
end

-- Tax & shipping rules ----------------------------------------------------
function handlers.SetTaxRules(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Rules" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Rules", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_type, err_type = validation.assert_type(msg.Rules, "table", "Rules")
  if not ok_type then
    return codec.error("INVALID_INPUT", err_type, { field = "Rules" })
  end
  for _, r in ipairs(msg.Rules) do
    if r.taxClass and type(r.taxClass) ~= "string" then
      return codec.error("INVALID_INPUT", "taxClass must be string", { rule = r })
    end
    if r.taxClass and #r.taxClass > 64 then
      return codec.error("INVALID_INPUT", "taxClass too long", { rule = r })
    end
    if r.priority and type(r.priority) ~= "number" then
      return codec.error("INVALID_INPUT", "priority must be number", { rule = r })
    end
    if r.rate and (type(r.rate) ~= "number" or r.rate < 0 or r.rate > 100) then
      return codec.error("INVALID_INPUT", "rate must be 0-100 percent", { rule = r })
    end
    if r.Rate and (type(r.Rate) ~= "number" or r.Rate < 0 or r.Rate > 100) then
      return codec.error("INVALID_INPUT", "Rate must be 0-100 percent", { rule = r })
    end
    if r.country and (type(r.country) ~= "string" or #r.country ~= 2) then
      return codec.error("INVALID_INPUT", "country must be ISO2", { rule = r })
    end
    if r.region and type(r.region) ~= "string" then
      return codec.error("INVALID_INPUT", "region must be string", { rule = r })
    end
  end
  state.tax_rules[msg["Site-Id"]] = msg.Rules
  audit.record("catalog", "SetTaxRules", msg, nil, { siteId = msg["Site-Id"], count = #msg.Rules })
  return codec.ok { siteId = msg["Site-Id"], count = #msg.Rules }
end

function handlers.SetShippingRules(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Rules" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Rules", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_type, err_type = validation.assert_type(msg.Rules, "table", "Rules")
  if not ok_type then
    return codec.error("INVALID_INPUT", err_type, { field = "Rules" })
  end
  state.shipping_rules[msg["Site-Id"]] = msg.Rules
  audit.record(
    "catalog",
    "SetShippingRules",
    msg,
    nil,
    { siteId = msg["Site-Id"], count = #msg.Rules }
  )
  return codec.ok { siteId = msg["Site-Id"], count = #msg.Rules }
end

local function pick_tax_rate(site_id, address, tax_class)
  local rules = state.tax_rules[site_id] or state.tax_rates[site_id] or {}
  local best_rate = 0
  local best_score = -1
  for _, r in ipairs(rules) do
    local match = (not r.country or r.country == address.Country)
      and (not r.region or r.region == address.Region)
      and (not r.taxClass or r.taxClass == tax_class)
    if match then
      local priority = tonumber(r.priority or r.Priority) or 0
      local specificity = (r.country and 1 or 0) + (r.region and 1 or 0) + (r.taxClass and 1 or 0)
      local score = priority * 10 + specificity
      if score > best_score then
        best_score = score
        best_rate = tonumber(r.rate or r.Rate) or 0
      end
    end
  end
  return best_rate
end

local function pick_shipping(site_id, address, total, weight, dims)
  local rules = state.shipping_rules[site_id] or state.shipping_rates[site_id] or {}
  local billable_weight = weight or 0
  if dims and dims.length and dims.width and dims.height then
    billable_weight =
      math.max(weight or 0, dimensional_weight(dims.length, dims.width, dims.height, 5000))
  end
  local best = nil
  for _, r in ipairs(rules) do
    if
      (not r.country or r.country == address.Country)
      and (not r.region or r.region == address.Region)
      and (not r.min_total or total >= r.min_total)
      and (not r.max_total or total <= r.max_total)
      and (not r.min_weight or billable_weight >= r.min_weight)
      and (not r.max_weight or billable_weight <= r.max_weight)
    then
      if not best or (r.rate or 0) < (best.rate or 0) then
        best = r
      end
    end
  end
  return best or { rate = 0, carrier = "standard", service = "ground" }
end

local function dimensional_weight(l, w, h, divisor)
  if not l or not w or not h then
    return 0
  end
  return (l * w * h) / (divisor or 5000)
end

local function shop_shipping(site_id, address, total, weight, dims)
  local rules = state.shipping_rules[site_id] or state.shipping_rates[site_id] or {}
  local billable_weight = weight or 0
  if dims and dims.length and dims.width and dims.height then
    billable_weight =
      math.max(weight or 0, dimensional_weight(dims.length, dims.width, dims.height, 5000))
  end
  local options = {}
  for _, r in ipairs(rules) do
    if
      (not r.country or r.country == address.Country)
      and (not r.region or r.region == address.Region)
      and (not r.min_total or total >= r.min_total)
      and (not r.max_total or total <= r.max_total)
      and (not r.min_weight or billable_weight >= r.min_weight)
      and (not r.max_weight or billable_weight <= r.max_weight)
    then
      table.insert(options, {
        carrier = r.carrier or "standard",
        service = r.service or "ground",
        rate = r.rate or 0,
        transitDays = r.transit_days or r.transitDays,
        currency = r.currency or "USD",
      })
    end
  end
  table.sort(options, function(a, b)
    return (a.rate or 0) < (b.rate or 0)
  end)
  while #options > MAX_RATE_OPTIONS do
    table.remove(options)
  end
  if CARRIER_API_URL and CARRIER_API_URL ~= "" then
    local payload = {
      siteId = site_id,
      address = address,
      total = total,
      weight = weight,
      currency = address.Currency or "USD",
    }
    local out = http_post_json(CARRIER_API_URL .. "/rates", payload)
    if out and out ~= "" then
      local ok, arr = pcall(cjson.decode, out)
      if ok and type(arr) == "table" then
        for _, o in ipairs(arr) do
          if o.rate then
            table.insert(options, {
              carrier = o.carrier or "external",
              service = o.service or "standard",
              rate = o.rate,
              transitDays = o.transitDays,
              currency = o.currency or payload.currency,
            })
          end
        end
      end
    end
  end
  if #options == 0 then
    table.insert(options, { carrier = "standard", service = "ground", rate = 0, currency = "USD" })
  end
  return options
end

local function compute_cart(site_id, items, currency, promo)
  local subtotal = 0
  local weight = 0
  local lines = {}
  for _, it in ipairs(items) do
    local qty = tonumber(it.Qty or it.qty) or 0
    if qty <= 0 then
      return nil, "INVALID_QTY"
    end
    local sku = it.Sku or it.sku
    local quote, err = apply_pricing(site_id, sku, currency, promo)
    if not quote then
      return nil, err or "NOT_FOUND"
    end
    local line_total = quote.price * qty
    subtotal = subtotal + line_total
    local pkey = ids.product_key(site_id, sku)
    local payload = state.products[pkey] and state.products[pkey].payload or {}
    weight = weight + (payload.weight or payload.Weight or 0) * qty
    table.insert(lines, {
      sku = sku,
      qty = qty,
      unit_price = quote.price,
      currency = quote.currency,
      line_total = line_total,
      taxClass = payload.taxClass or payload.TaxClass,
    })
  end
  return { subtotal = subtotal, weight = weight, lines = lines }, nil
end

function handlers.QuoteOrder(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Items", "Address" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Items",
    "Address",
    "Currency",
    "Promo",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_type_items, err_items = validation.assert_type(msg.Items, "table", "Items")
  if not ok_type_items or #msg.Items == 0 then
    return codec.error(
      "INVALID_INPUT",
      err_items or "Items must be non-empty array",
      { field = "Items" }
    )
  end
  local address = msg.Address
  if type(address) ~= "table" or not address.Country then
    return codec.error("INVALID_INPUT", "Address.Country required", { field = "Address" })
  end
  local currency = msg.Currency or "USD"
  local cart, cart_err = compute_cart(msg["Site-Id"], msg.Items, currency, msg.Promo)
  if not cart then
    return codec.error("INVALID_INPUT", cart_err or "Pricing failed")
  end
  -- inventory check across warehouses
  for _, line in ipairs(cart.lines) do
    local inv = state.inventory[msg["Site-Id"]] or {}
    local available = 0
    for _, wh in pairs(inv) do
      available = available + (wh[line.sku] or 0)
    end
    if available < line.qty then
      return codec.error(
        "OUT_OF_STOCK",
        "Insufficient inventory",
        { sku = line.sku, available = available }
      )
    end
  end
  local ship = pick_shipping(msg["Site-Id"], address, cart.subtotal, cart.weight)
  local tax = 0
  local line_taxes = {}
  for _, line in ipairs(cart.lines) do
    local tr = pick_tax_rate(msg["Site-Id"], address, line.taxClass)
    local lt = line.line_total * tr / 100
    tax = tax + lt
    table.insert(line_taxes, { sku = line.sku, tax = lt, taxRate = tr })
  end
  local total = cart.subtotal + ship.rate + tax
  return codec.ok {
    siteId = msg["Site-Id"],
    currency = currency,
    items = cart.lines,
    subtotal = cart.subtotal,
    weight = cart.weight,
    shipping = ship,
    tax = tax,
    lineTaxes = line_taxes,
    total = total,
    promo = msg.Promo,
  }
end

function handlers.CalculateTax(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Items", "Address" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Items",
    "Address",
    "Currency",
    "Promo",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local address = msg.Address
  if type(address) ~= "table" or not address.Country then
    return codec.error("INVALID_INPUT", "Address.Country required", { field = "Address" })
  end
  local currency = msg.Currency or "USD"
  local cart, cart_err = compute_cart(msg["Site-Id"], msg.Items, currency, msg.Promo)
  if not cart then
    return codec.error("INVALID_INPUT", cart_err or "Pricing failed")
  end
  local tax = 0
  local line_taxes = {}
  for _, line in ipairs(cart.lines) do
    local tr = pick_tax_rate(msg["Site-Id"], address, line.taxClass)
    local lt = line.line_total * tr / 100
    tax = tax + lt
    table.insert(line_taxes, { sku = line.sku, tax = lt, taxRate = tr })
  end
  return codec.ok {
    siteId = msg["Site-Id"],
    subtotal = cart.subtotal,
    tax = tax,
    lineTaxes = line_taxes,
    currency = currency,
  }
end

function handlers.RateShopCarriers(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Items", "Address" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Items",
    "Address",
    "Currency",
    "Promo",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local address = msg.Address
  if type(address) ~= "table" or not address.Country then
    return codec.error("INVALID_INPUT", "Address.Country required", { field = "Address" })
  end
  local currency = msg.Currency or "USD"
  local cart, cart_err = compute_cart(msg["Site-Id"], msg.Items, currency, msg.Promo)
  if not cart then
    return codec.error("INVALID_INPUT", cart_err or "Pricing failed")
  end
  local dims = nil
  if msg.Dimensions then
    dims = {
      length = tonumber(msg.Dimensions.Length),
      width = tonumber(msg.Dimensions.Width),
      height = tonumber(msg.Dimensions.Height),
    }
  end
  local options = shop_shipping(msg["Site-Id"], address, cart.subtotal, cart.weight, dims)
  return codec.ok {
    siteId = msg["Site-Id"],
    subtotal = cart.subtotal,
    weight = cart.weight,
    currency = currency,
    options = options,
  }
end

-- Inventory per warehouse -------------------------------------------------
function handlers.SetInventory(msg)
  local ok, missing =
    validation.require_fields(msg, { "Site-Id", "Warehouse-Id", "Sku", "Quantity" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Warehouse-Id",
    "Sku",
    "Quantity",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local qty = tonumber(msg.Quantity)
  if not qty or qty < 0 then
    return codec.error("INVALID_INPUT", "Quantity must be >= 0", { field = "Quantity" })
  end
  state.inventory[msg["Site-Id"]] = state.inventory[msg["Site-Id"]] or {}
  state.inventory[msg["Site-Id"]][msg["Warehouse-Id"]] = state.inventory[msg["Site-Id"]][msg["Warehouse-Id"]]
    or {}
  state.inventory[msg["Site-Id"]][msg["Warehouse-Id"]][msg.Sku] = qty
  -- compute total for alerts
  local total = 0
  for _, skus in pairs(state.inventory[msg["Site-Id"]]) do
    total = total + (skus[msg.Sku] or 0)
  end
  local policy = state.stock_policies[msg["Site-Id"]]
      and state.stock_policies[msg["Site-Id"]][msg.Sku]
    or {}
  push_low_stock(msg["Site-Id"], msg.Sku, total, policy.low_stock_threshold)
  audit.record(
    "catalog",
    "SetInventory",
    msg,
    nil,
    { siteId = msg["Site-Id"], warehouse = msg["Warehouse-Id"], sku = msg.Sku, quantity = qty }
  )
  return codec.ok {
    siteId = msg["Site-Id"],
    warehouse = msg["Warehouse-Id"],
    sku = msg.Sku,
    quantity = qty,
  }
end

function handlers.GetInventory(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Sku" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Sku", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local inv = state.inventory[msg["Site-Id"]] or {}
  local warehouses = {}
  local total = 0
  for wh, skus in pairs(inv) do
    local q = skus[msg.Sku] or 0
    if q > 0 then
      warehouses[wh] = q
      total = total + q
    end
  end
  local policy = state.stock_policies[msg["Site-Id"]]
    and state.stock_policies[msg["Site-Id"]][msg.Sku]
  return codec.ok {
    siteId = msg["Site-Id"],
    sku = msg.Sku,
    total = total,
    warehouses = warehouses,
    policy = policy,
  }
end

-- Stock policy (backorder/preorder thresholds) ----------------------------
function handlers.SetStockPolicy(msg)
  local ok, missing =
    validation.require_fields(msg, { "Site-Id", "Sku", "Allow-Backorder", "Low-Stock-Threshold" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Sku",
    "Allow-Backorder",
    "Preorder-At",
    "ETA-Days",
    "Low-Stock-Threshold",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local allow_backorder = msg["Allow-Backorder"] == true or msg["Allow-Backorder"] == "true"
  local threshold = tonumber(msg["Low-Stock-Threshold"]) or 0
  if threshold < 0 then
    return codec.error(
      "INVALID_INPUT",
      "Low-Stock-Threshold must be >=0",
      { field = "Low-Stock-Threshold" }
    )
  end
  local preorder_at = msg["Preorder-At"]
  if preorder_at and type(preorder_at) ~= "string" then
    return codec.error(
      "INVALID_INPUT",
      "Preorder-At must be ISO date string",
      { field = "Preorder-At" }
    )
  end
  local eta_days = msg["ETA-Days"] and tonumber(msg["ETA-Days"]) or nil
  if eta_days and eta_days < 0 then
    return codec.error("INVALID_INPUT", "ETA-Days must be >=0", { field = "ETA-Days" })
  end
  state.stock_policies[msg["Site-Id"]] = state.stock_policies[msg["Site-Id"]] or {}
  state.stock_policies[msg["Site-Id"]][msg.Sku] = {
    allow_backorder = allow_backorder,
    preorder_at = preorder_at,
    low_stock_threshold = threshold,
    eta_days = eta_days,
  }
  audit.record(
    "catalog",
    "SetStockPolicy",
    msg,
    nil,
    { sku = msg.Sku, allow_backorder = allow_backorder, threshold = threshold }
  )
  return codec.ok {
    siteId = msg["Site-Id"],
    sku = msg.Sku,
    allowBackorder = allow_backorder,
    preorderAt = preorder_at,
    lowStockThreshold = threshold,
    etaDays = eta_days,
  }
end

local function push_low_stock(site_id, sku, total, threshold)
  if threshold and threshold > 0 and total <= threshold then
    state.stock_alerts[site_id] = state.stock_alerts[site_id] or {}
    local alert = {
      sku = sku,
      total = total,
      threshold = threshold,
      ts = os.time(),
    }
    table.insert(state.stock_alerts[site_id], alert)
    deliver_stock_alert(site_id, alert)
  end
end

local function record_backorder(site_id, sku, qty, source, ref, preorder_at, eta_days)
  state.backorders[site_id] = state.backorders[site_id] or {}
  table.insert(state.backorders[site_id], {
    sku = sku,
    qty = qty,
    source = source,
    ref = ref,
    preorder_at = preorder_at,
    eta_days = eta_days,
    createdAt = os.time(),
  })
end

function handlers.ListLowStock(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Clear", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local alerts = state.stock_alerts[msg["Site-Id"]] or {}
  local clear = msg.Clear == true or msg.Clear == "true"
  if clear then
    state.stock_alerts[msg["Site-Id"]] = {}
  end
  return codec.ok { siteId = msg["Site-Id"], alerts = alerts }
end

function handlers.DeliverLowStockAlerts(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local alerts = state.stock_alerts[msg["Site-Id"]] or {}
  for _, alert in ipairs(alerts) do
    deliver_stock_alert(msg["Site-Id"], alert)
  end
  return codec.ok { siteId = msg["Site-Id"], delivered = #alerts }
end

function handlers.ListBackorders(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Sku",
    "Source",
    "Cursor",
    "Limit",
    "Clear",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local limit = tonumber(msg.Limit) or 200
  if limit < 1 then
    limit = 1
  end
  if limit > 1000 then
    limit = 1000
  end
  local cursor = tonumber(msg.Cursor) or 0
  local out = {}
  local all = state.backorders[msg["Site-Id"]] or {}
  local filtered = {}
  for _, bo in ipairs(all) do
    if (not msg.Sku or msg.Sku == bo.sku) and (not msg.Source or msg.Source == bo.source) then
      table.insert(filtered, bo)
    end
  end
  table.sort(filtered, function(a, b)
    return (a.createdAt or 0) > (b.createdAt or 0)
  end)
  for i = cursor + 1, math.min(#filtered, cursor + limit) do
    table.insert(out, filtered[i])
  end
  local next_cursor = (#filtered > cursor + limit) and (cursor + limit) or nil
  if msg.Clear == true or msg.Clear == "true" then
    state.backorders[msg["Site-Id"]] = {}
  end
  return codec.ok {
    siteId = msg["Site-Id"],
    items = out,
    nextCursor = next_cursor,
    total = #out,
    filterSku = msg.Sku,
    filterSource = msg.Source,
  }
end

function handlers.ForgetSubject(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Subject" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Subject", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local count = forget_subject(msg["Site-Id"], msg.Subject)
  audit.record(
    "catalog",
    "ForgetSubject",
    msg,
    nil,
    { siteId = msg["Site-Id"], subject = msg.Subject }
  )
  return codec.ok { siteId = msg["Site-Id"], subject = msg.Subject, scrubbed = count }
end

-- Checkout skeleton -------------------------------------------------------
local function reserve_inventory(site_id, items)
  local inv = state.inventory[site_id] or {}
  local changes = {}
  local backorders = {}
  for _, item in ipairs(items) do
    local needed = item.qty
    for wh, skus in pairs(inv) do
      local available = skus[item.sku] or 0
      if available > 0 then
        local take = math.min(available, needed)
        skus[item.sku] = available - take
        needed = needed - take
        table.insert(changes, { warehouse = wh, sku = item.sku, qty = take })
        if needed == 0 then
          break
        end
      end
    end
    if needed > 0 then
      local policy = state.stock_policies[site_id] and state.stock_policies[site_id][item.sku] or {}
      if policy.allow_backorder then
        table.insert(backorders, {
          sku = item.sku,
          qty = needed,
          preorder_at = policy.preorder_at,
          eta_days = policy.eta_days,
        })
      else
        -- rollback
        for _, c in ipairs(changes) do
          inv[c.warehouse][c.sku] = (inv[c.warehouse][c.sku] or 0) + c.qty
        end
        return false, "INSUFFICIENT_STOCK"
      end
    end
    -- low stock alert after deduction
    local total_after = 0
    for _, skus in pairs(inv) do
      total_after = total_after + (skus[item.sku] or 0)
    end
    local policy = state.stock_policies[site_id] and state.stock_policies[site_id][item.sku] or {}
    push_low_stock(site_id, item.sku, total_after, policy.low_stock_threshold)
    if needed > 0 then
      record_backorder(
        site_id,
        item.sku,
        needed,
        "reserve",
        nil,
        policy.preorder_at,
        policy.eta_days
      )
    end
  end
  return true, changes, backorders
end

local function restore_inventory(site_id, changes)
  local inv = state.inventory[site_id] or {}
  for _, c in ipairs(changes or {}) do
    inv[c.warehouse] = inv[c.warehouse] or {}
    inv[c.warehouse][c.sku] = (inv[c.warehouse][c.sku] or 0) + c.qty
  end
end

local function purge_paths(paths)
  if not CDN_PURGE_CMD or CDN_PURGE_CMD == "" then
    return
  end
  for _, p in ipairs(paths or {}) do
    local cmd = string.format(CDN_PURGE_CMD, p)
    os.execute(cmd .. " >/dev/null 2>&1")
  end
end

local function forget_subject(site_id, subject)
  if not subject or subject == "" then
    return 0
  end
  local scrubbed = 0
  -- remove from recent list
  for sub, items in pairs(state.recent) do
    if sub == subject then
      state.recent[sub] = nil
      scrubbed = scrubbed + 1
    end
  end
  -- scrub checkouts
  for id, chk in pairs(state.checkouts) do
    if chk.siteId == site_id and chk.email == subject then
      chk.email = nil
      chk.address = nil
      scrubbed = scrubbed + 1
    end
  end
  -- scrub orders
  for _, ord in pairs(state.orders) do
    if ord.siteId == site_id and ord.email == subject then
      ord.email = nil
      ord.address = nil
      scrubbed = scrubbed + 1
    end
  end
  -- scrub telemetry buffered events
  if state.telemetry[site_id] then
    local filtered = {}
    for _, ev in ipairs(state.telemetry[site_id]) do
      if ev.subject ~= subject then
        table.insert(filtered, ev)
      end
    end
    state.telemetry[site_id] = filtered
  end
  return scrubbed
end

local function notify_rma(site_id, return_id, event, payload)
  if not RMA_WEBHOOK or RMA_WEBHOOK == "" or not json_ok then
    return
  end
  local body = {
    type = "rma." .. event,
    siteId = site_id,
    returnId = return_id,
    payload = payload,
  }
  local ok, json_body = pcall(cjson.encode, body)
  if not ok then
    return
  end
  local cmd = string.format(
    "curl -sS -m %d -H 'Content-Type: application/json' -d '%s' %s >/dev/null 2>&1",
    HTTP_TIMEOUT,
    json_body:gsub("'", "'\\''"),
    RMA_WEBHOOK
  )
  os.execute(cmd)
end

local function record_shipment_event(shipment_id, status, meta)
  state.shipment_events[shipment_id] = state.shipment_events[shipment_id] or {}
  table.insert(state.shipment_events[shipment_id], {
    ts = os.time(),
    status = status,
    meta = meta,
  })
end

local function cleanup_retention()
  local cutoff = os.time() - (RETENTION_DAYS * 86400)
  for site, evs in pairs(state.telemetry) do
    local filtered = {}
    for _, ev in ipairs(evs) do
      if not ev.ts or ev.ts >= cutoff then
        table.insert(filtered, ev)
      end
    end
    state.telemetry[site] = filtered
  end
  for site, log in pairs(state.event_log) do
    local filtered = {}
    for _, ev in ipairs(log) do
      if (ev.ts or 0) >= cutoff then
        table.insert(filtered, ev)
      end
    end
    state.event_log[site] = filtered
  end
  for site, alerts in pairs(state.stock_alerts) do
    local filtered = {}
    for _, a in ipairs(alerts) do
      if (a.ts or 0) >= cutoff then
        table.insert(filtered, a)
      end
    end
    state.stock_alerts[site] = filtered
  end
  for site, list in pairs(state.backorders) do
    local filtered = {}
    for _, bo in ipairs(list) do
      if (bo.createdAt or 0) >= cutoff then
        table.insert(filtered, bo)
      end
    end
    state.backorders[site] = filtered
  end
  for ship, events in pairs(state.shipment_events) do
    local filtered = {}
    for _, e in ipairs(events) do
      if (e.ts or 0) >= cutoff then
        table.insert(filtered, e)
      end
    end
    state.shipment_events[ship] = filtered
  end
end

local function deliver_stock_alert(site_id, alert)
  if not STOCK_ALERT_WEBHOOK or STOCK_ALERT_WEBHOOK == "" or not json_ok then
    return
  end
  local body = {
    type = "low_stock",
    siteId = site_id,
    sku = alert.sku,
    total = alert.total,
    threshold = alert.threshold,
    ts = alert.ts,
  }
  local ok, payload = pcall(cjson.encode, body)
  if not ok then
    return
  end
  local cmd = string.format(
    "curl -sS -m %d -H 'Content-Type: application/json' -d '%s' %s >/dev/null 2>&1",
    HTTP_TIMEOUT,
    payload:gsub("'", "'\\''"),
    STOCK_ALERT_WEBHOOK
  )
  os.execute(cmd)
end

function handlers.StartCheckout(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Items", "Address", "Email" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Items",
    "Address",
    "Email",
    "Currency",
    "Promo",
    "Payment-Method",
    "Require3DS",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local currency = msg.Currency or "USD"
  local cart, cart_err = compute_cart(msg["Site-Id"], msg.Items, currency, msg.Promo)
  if not cart then
    return codec.error("INVALID_INPUT", cart_err or "Pricing failed")
  end
  local address = msg.Address
  if type(address) ~= "table" or not address.Country then
    return codec.error("INVALID_INPUT", "Address.Country required", { field = "Address" })
  end
  local dims = nil
  if msg.Dimensions then
    dims = {
      length = tonumber(msg.Dimensions.Length),
      width = tonumber(msg.Dimensions.Width),
      height = tonumber(msg.Dimensions.Height),
    }
  end
  local shipping = pick_shipping(msg["Site-Id"], address, cart.subtotal, cart.weight, dims)
  local tax_rate = pick_tax_rate(msg["Site-Id"], address)
  local tax = cart.subtotal * tax_rate / 100
  local total = cart.subtotal + shipping.rate + tax
  local items = {}
  for _, item in ipairs(msg.Items) do
    table.insert(items, { sku = item.Sku, qty = tonumber(item.Qty) or 0 })
  end
  local ok_reserve, changes, backorders = reserve_inventory(msg["Site-Id"], items)
  if not ok_reserve then
    return codec.error("OUT_OF_STOCK", "Insufficient inventory during reserve")
  end
  local checkout_id = string.format("chk-%d", os.time() * 1000 + math.random(0, 999))
  state.checkouts[checkout_id] = {
    siteId = msg["Site-Id"],
    items = items,
    address = msg.Address,
    email = msg.Email,
    quote = {
      subtotal = cart.subtotal,
      weight = cart.weight,
      taxRate = tax_rate,
      tax = tax,
      shipping = shipping,
      total = total,
      currency = currency,
      promo = msg.Promo,
    },
    status = "pending_payment",
    reserve = changes,
    backorders = backorders,
    risk = risk_score {
      quote = {
        subtotal = cart.subtotal,
        shipping = shipping,
        total = total,
        promo = msg.Promo,
      },
      address = msg.Address,
      email = msg.Email,
    },
  }
  local payment
  if msg["Payment-Method"] then
    payment = create_payment_intent_internal {
      siteId = msg["Site-Id"],
      checkoutId = checkout_id,
      amount = total,
      currency = currency,
      method = msg["Payment-Method"],
      require3ds = msg.Require3DS,
    }
  end
  for _, bo in ipairs(backorders or {}) do
    record_backorder(
      msg["Site-Id"],
      bo.sku,
      bo.qty,
      "checkout",
      checkout_id,
      bo.preorder_at,
      bo.eta_days
    )
  end
  return codec.ok {
    checkoutId = checkout_id,
    total = total,
    currency = currency,
    tax = tax,
    taxRate = tax_rate,
    shipping = shipping,
    paymentIntent = payment and payment.paymentId,
    paymentStatus = payment and payment.status or "pending_payment",
    risk = state.checkouts[checkout_id].risk,
    backorders = backorders,
  }
end

function handlers.CompleteCheckout(msg)
  local ok, missing = validation.require_fields(msg, { "Checkout-Id", "Payment-Method" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Checkout-Id",
    "Payment-Method",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local chk = state.checkouts[msg["Checkout-Id"]]
  if not chk then
    return codec.error("NOT_FOUND", "Checkout not found", { checkoutId = msg["Checkout-Id"] })
  end
  if chk.status ~= "pending_payment" then
    return codec.error("INVALID_STATE", "Checkout already completed", { status = chk.status })
  end
  local payment_id = chk.paymentIntent
  if not payment_id then
    local created = create_payment_intent_internal {
      siteId = chk.siteId,
      checkoutId = msg["Checkout-Id"],
      amount = chk.quote.total,
      currency = chk.quote.currency or "USD",
      method = msg["Payment-Method"],
      require3ds = msg.Require3DS,
    }
    payment_id = created.paymentId
  end
  local pay = state.payments[payment_id]
  if not pay then
    return codec.error("NOT_FOUND", "Payment intent missing", { paymentId = payment_id })
  end
  if pay.requiresAction and not msg.ChallengeCompleted then
    return codec.error("REQUIRES_ACTION", "3DS challenge not completed")
  end
  if chk.risk and chk.risk >= RISK_THRESHOLD and not msg.OverrideRisk then
    chk.status = "manual_review"
    return codec.error("REVIEW_REQUIRED", "Checkout flagged for manual review", { risk = chk.risk })
  end
  pay.status = "captured"
  pay.capturedAt = os.time()
  chk.status = "paid"
  chk.payment = {
    method = msg["Payment-Method"],
    status = pay.status,
    paidAt = os.date "!%Y-%m-%dT%H:%M:%SZ",
    paymentId = pay.paymentId,
  }
  audit.record("catalog", "CompleteCheckout", msg, nil, { checkoutId = msg["Checkout-Id"] })
  return codec.ok {
    checkoutId = msg["Checkout-Id"],
    status = chk.status,
    payment = chk.payment,
    risk = chk.risk,
  }
end

function handlers.CreatePaymentIntent(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Amount", "Currency", "Method" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Checkout-Id",
    "Order-Id",
    "Amount",
    "Currency",
    "Method",
    "Require3DS",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  if type(msg.Amount) ~= "number" or msg.Amount <= 0 then
    return codec.error("INVALID_INPUT", "Amount must be positive number")
  end
  if not msg.Currency or #msg.Currency ~= 3 then
    return codec.error("INVALID_INPUT", "Currency must be ISO 4217")
  end
  local method = msg.Method
  local allowed = { card = true, paypal = true, applepay = true, googlepay = true, ideal = true }
  if not allowed[method] then
    return codec.error("INVALID_INPUT", "Unsupported payment method")
  end
  if msg["Checkout-Id"] and not state.checkouts[msg["Checkout-Id"]] then
    return codec.error("NOT_FOUND", "Checkout not found", { checkoutId = msg["Checkout-Id"] })
  end
  if msg["Order-Id"] and not state.orders[msg["Order-Id"]] then
    return codec.error("NOT_FOUND", "Order not found", { orderId = msg["Order-Id"] })
  end
  local provider = msg.Provider or "internal"
  local record = create_payment_intent_internal {
    siteId = msg["Site-Id"],
    checkoutId = msg["Checkout-Id"],
    orderId = msg["Order-Id"],
    amount = msg.Amount,
    currency = msg.Currency,
    method = method,
    require3ds = msg.Require3DS,
    provider = provider,
    token = msg.Token,
  }
  audit.record(
    "catalog",
    "CreatePaymentIntent",
    msg,
    nil,
    { paymentId = record.paymentId, status = record.status }
  )
  metrics.inc "catalog.CreatePaymentIntent.count"
  metrics.tick()
  return codec.ok {
    paymentId = record.paymentId,
    status = record.status,
    provider = record.provider,
    clientSecret = record.clientSecret,
    nextAction = record.requiresAction
        and { type = "3ds_redirect", token = "3ds-" .. record.paymentId }
      or nil,
  }
end

function handlers.TokenizePaymentMethod(msg)
  local ok, missing = validation.require_fields(msg, { "Provider", "Payload" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Provider",
    "Payload",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  if type(msg.Payload) ~= "table" then
    return codec.error("INVALID_INPUT", "Payload must be object")
  end
  local provider = msg.Provider
  if provider ~= "stripe" and provider ~= "paypal" and provider ~= "adyen" then
    return codec.error("INVALID_INPUT", "Unsupported provider")
  end
  local token = string.format("%s_tok_%s", provider, gen_id "pm")
  audit.record("catalog", "TokenizePaymentMethod", msg, nil, { provider = provider })
  return codec.ok { token = token, provider = provider }
end

function handlers.CapturePayment(msg)
  local ok, missing = validation.require_fields(msg, { "Payment-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Payment-Id",
    "ChallengeCompleted",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local pay = state.payments[msg["Payment-Id"]]
  if not pay then
    return codec.error("NOT_FOUND", "Payment not found")
  end
  if pay.status == "captured" or pay.status == "refunded" then
    return codec.ok { paymentId = pay.paymentId, status = pay.status }
  end
  if pay.requiresAction and not msg.ChallengeCompleted then
    return codec.error("REQUIRES_ACTION", "3DS challenge not completed")
  end
  if pay.provider ~= "internal" and pay.token then
    pay.providerCaptureId = "cap_" .. pay.paymentId
  end
  pay.status = "captured"
  pay.capturedAt = os.time()
  if pay.orderId and state.orders[pay.orderId] then
    state.orders[pay.orderId].paymentStatus = "paid"
  end
  if pay.checkoutId and state.checkouts[pay.checkoutId] then
    state.checkouts[pay.checkoutId].paymentStatus = "paid"
    state.checkouts[pay.checkoutId].status = "paid"
  end
  audit.record("catalog", "CapturePayment", msg, nil, { paymentId = pay.paymentId })
  metrics.inc "catalog.CapturePayment.count"
  metrics.tick()
  return codec.ok { paymentId = pay.paymentId, status = pay.status }
end

function handlers.RefundPayment(msg)
  local ok, missing = validation.require_fields(msg, { "Payment-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Payment-Id",
    "Amount",
    "Reason",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local pay = state.payments[msg["Payment-Id"]]
  if not pay then
    return codec.error("NOT_FOUND", "Payment not found")
  end
  local amount = msg.Amount or pay.amount
  if type(amount) ~= "number" or amount <= 0 then
    return codec.error("INVALID_INPUT", "Amount must be positive number")
  end
  pay.status = "refunded"
  pay.refundAmount = amount
  pay.refundedAt = os.time()
  if pay.provider ~= "internal" then
    pay.providerRefundId = "rf_" .. pay.paymentId
  end
  if pay.orderId and state.orders[pay.orderId] then
    state.orders[pay.orderId].refundAmount = amount
    state.orders[pay.orderId].paymentStatus = "refunded"
    state.orders[pay.orderId].status = state.orders[pay.orderId].status or "refunded"
  end
  audit.record("catalog", "RefundPayment", msg, nil, { paymentId = pay.paymentId, amount = amount })
  metrics.inc "catalog.RefundPayment.count"
  metrics.tick()
  return codec.ok { paymentId = pay.paymentId, status = pay.status, amount = amount }
end

local function verify_provider_webhook(provider)
  if provider == "stripe" then
    return STRIPE_WEBHOOK_SECRET ~= nil
  elseif provider == "paypal" then
    return PAYPAL_WEBHOOK_ID ~= nil
  elseif provider == "adyen" then
    return ADYEN_HMAC_KEY ~= nil
  end
  return false
end

function handlers.HandlePaymentProviderWebhook(msg)
  local ok, missing = validation.require_fields(msg, { "Provider", "Event" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Provider",
    "Event",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  if not verify_provider_webhook(msg.Provider) then
    return codec.error("FORBIDDEN", "Signature verification failed")
  end
  local ev = msg.Event
  if type(ev) ~= "table" then
    return codec.error("INVALID_INPUT", "Event must be object")
  end
  local pid = ev.paymentId or ev.payment_id
  if not pid then
    return codec.error("INVALID_INPUT", "paymentId missing in event")
  end
  local pay = state.payments[pid]
  if not pay then
    return codec.error("NOT_FOUND", "Payment not found")
  end
  if ev.type == "payment_succeeded" then
    pay.status = "captured"
    pay.capturedAt = os.time()
  elseif ev.type == "payment_failed" then
    pay.status = "failed"
  elseif ev.type == "refund_succeeded" then
    pay.status = "refunded"
    pay.refundAmount = ev.amount or pay.amount
  end
  audit.record(
    "catalog",
    "HandlePaymentProviderWebhook",
    msg,
    nil,
    { paymentId = pid, status = pay.status }
  )
  return codec.ok { paymentId = pid, status = pay.status }
end

function handlers.CleanupRetention(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  cleanup_retention()
  audit.record("catalog", "CleanupRetention", msg, nil, { retentionDays = RETENTION_DAYS })
  return codec.ok { retentionDays = RETENTION_DAYS }
end

function handlers.RequestReturn(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Order-Id", "Items" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Order-Id",
    "Items",
    "Reason",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_items, err_items = validation.assert_type(msg.Items, "table", "Items")
  if not ok_items then
    return codec.error("INVALID_INPUT", err_items, { field = "Items" })
  end
  local items = {}
  for _, it in ipairs(msg.Items) do
    if not (it.Sku and it.Qty) then
      return codec.error("INVALID_INPUT", "Item requires Sku and Qty")
    end
    table.insert(items, { sku = it.Sku, qty = tonumber(it.Qty) or 0, reason = it.Reason })
  end
  local return_id = gen_id "ret"
  state.returns[return_id] = {
    returnId = return_id,
    siteId = msg["Site-Id"],
    orderId = msg["Order-Id"],
    items = items,
    status = "requested",
    reason = msg.Reason,
    createdAt = os.time(),
    restockFee = msg.RestockFee,
    method = msg.Method or "dropoff",
  }
  audit.record("catalog", "RequestReturn", msg, nil, { returnId = return_id })
  notify_rma(msg["Site-Id"], return_id, "requested", state.returns[return_id])
  metrics.inc "catalog.RequestReturn.count"
  metrics.tick()
  return codec.ok { returnId = return_id, status = "requested" }
end

function handlers.UpdateReturnStatus(msg)
  local ok, missing = validation.require_fields(msg, { "Return-Id", "Status" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Return-Id",
    "Status",
    "Reason",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ret = state.returns[msg["Return-Id"]]
  if not ret then
    return codec.error("NOT_FOUND", "Return not found")
  end
  local allowed = {
    requested = true,
    authorized = true,
    in_transit = true,
    received = true,
    inspected = true,
    refunded = true,
    rejected = true,
  }
  if not allowed[msg.Status] then
    return codec.error("INVALID_INPUT", "Unsupported status")
  end
  ret.status = msg.Status
  ret.reason = msg.Reason or ret.reason
  ret.updatedAt = os.time()
  audit.record(
    "catalog",
    "UpdateReturnStatus",
    msg,
    nil,
    { returnId = ret.returnId, status = ret.status }
  )
  notify_rma(ret.siteId, ret.returnId, "status", { status = ret.status, reason = ret.reason })
  return codec.ok { returnId = ret.returnId, status = ret.status }
end

function handlers.ApproveReturn(msg)
  local ok, missing = validation.require_fields(msg, { "Return-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Return-Id",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ret = state.returns[msg["Return-Id"]]
  if not ret then
    return codec.error("NOT_FOUND", "Return not found")
  end
  ret.status = "approved"
  ret.approvedAt = os.time()
  audit.record("catalog", "ApproveReturn", msg, nil, { returnId = ret.returnId })
  notify_rma(ret.siteId, ret.returnId, "approved", { status = ret.status })
  metrics.inc "catalog.ApproveReturn.count"
  metrics.tick()
  return codec.ok { returnId = ret.returnId, status = ret.status }
end

function handlers.RefundReturn(msg)
  local ok, missing = validation.require_fields(msg, { "Return-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Return-Id",
    "Amount",
    "Restock",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ret = state.returns[msg["Return-Id"]]
  if not ret then
    return codec.error("NOT_FOUND", "Return not found")
  end
  local amount = msg.Amount
  if amount and (type(amount) ~= "number" or amount <= 0) then
    return codec.error("INVALID_INPUT", "Amount must be positive number")
  end
  ret.status = "refunded"
  ret.refundAmount = amount
  ret.refundedAt = os.time()
  local restock = msg.Restock ~= false
  if restock then
    adjust_inventory(ret.siteId, ret.items, 1)
  end
  if ret.orderId and state.orders[ret.orderId] then
    local o = state.orders[ret.orderId]
    o.status = o.status or "returned"
    o.returnStatus = ret.status
    o.refundAmount = amount or o.refundAmount
  end
  audit.record("catalog", "RefundReturn", msg, nil, { returnId = ret.returnId, amount = amount })
  notify_rma(ret.siteId, ret.returnId, "refunded", { amount = amount, restocked = restock })
  metrics.inc "catalog.RefundReturn.count"
  metrics.tick()
  return codec.ok { returnId = ret.returnId, status = ret.status, restocked = restock }
end

function handlers.CreateReturnLabel(msg)
  local ok, missing = validation.require_fields(msg, { "Return-Id", "Carrier" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Return-Id",
    "Carrier",
    "Service",
    "Address",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ret = state.returns[msg["Return-Id"]]
  if not ret then
    return codec.error("NOT_FOUND", "Return not found")
  end
  local carrier = msg.Carrier
  local service = msg.Service or "standard"
  local address = msg.Address or ret.address
  if not address or not address.Country then
    return codec.error("INVALID_INPUT", "Address.Country required", { field = "Address" })
  end
  local label = build_label(carrier, service, ret.weight or 0)
  label.returnId = ret.returnId
  label.address = address
  label.base = RETURN_LABEL_BASE
  state.shipments[label.shipmentId] = label
  ret.returnLabel = label
  notify_rma(
    ret.siteId,
    ret.returnId,
    "label",
    { shipmentId = label.shipmentId, carrier = carrier }
  )
  audit.record(
    "catalog",
    "CreateReturnLabel",
    msg,
    nil,
    { returnId = ret.returnId, shipmentId = label.shipmentId }
  )
  return codec.ok(label)
end

function handlers.ExportTelemetry(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local events = state.telemetry
  state.telemetry = {}
  if TELEMETRY_EXPORT_PATH and TELEMETRY_EXPORT_PATH ~= "" and json_ok then
    local f = io.open(TELEMETRY_EXPORT_PATH, "a")
    if f then
      for _, ev in ipairs(events) do
        local ok, line = pcall(cjson.encode, ev)
        if ok and line then
          f:write(line)
          f:write "\n"
        end
      end
      f:close()
    end
  end
  return codec.ok { events = events, count = #events, path = TELEMETRY_EXPORT_PATH }
end

-- B2B / Purchase Orders ---------------------------------------------------
function handlers.CreateCompanyAccount(msg)
  local ok, missing = validation.require_fields(msg, { "Name" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Company-Id",
    "Name",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local cid = msg["Company-Id"] or gen_id "co"
  state.companies[cid] = state.companies[cid] or { name = msg.Name, users = {} }
  audit.record("catalog", "CreateCompanyAccount", msg, nil, { companyId = cid })
  return codec.ok { companyId = cid, name = msg.Name }
end

function handlers.AddCompanyUser(msg)
  local ok, missing = validation.require_fields(msg, { "Company-Id", "User-Id", "Role" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Company-Id",
    "User-Id",
    "Role",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_comp, err_comp = ensure_company(msg["Company-Id"])
  if not ok_comp then
    return codec.error("NOT_FOUND", err_comp)
  end
  local role = msg.Role
  if role ~= "buyer" and role ~= "approver" and role ~= "admin" then
    return codec.error("INVALID_INPUT", "Role must be buyer|approver|admin")
  end
  state.companies[msg["Company-Id"]].users[msg["User-Id"]] = role
  audit.record(
    "catalog",
    "AddCompanyUser",
    msg,
    nil,
    { companyId = msg["Company-Id"], userId = msg["User-Id"], role = role }
  )
  return codec.ok { companyId = msg["Company-Id"], userId = msg["User-Id"], role = role }
end

function handlers.CreatePurchaseOrder(msg)
  local ok, missing =
    validation.require_fields(msg, { "Site-Id", "Company-Id", "Items", "Address" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Company-Id",
    "Items",
    "Address",
    "Currency",
    "Promo",
    "Buyer-Id",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_comp, err_comp = ensure_company(msg["Company-Id"])
  if not ok_comp then
    return codec.error("NOT_FOUND", err_comp)
  end
  if
    msg["Buyer-Id"]
    and not require_company_role(msg["Company-Id"], msg["Buyer-Id"], { "buyer", "admin" })
  then
    return codec.error("FORBIDDEN", "Buyer not allowed for company")
  end
  local currency = msg.Currency or "USD"
  local cart, cart_err = compute_cart(msg["Site-Id"], msg.Items, currency, msg.Promo)
  if not cart then
    return codec.error("INVALID_INPUT", cart_err or "Pricing failed")
  end
  local address = msg.Address
  if type(address) ~= "table" or not address.Country then
    return codec.error("INVALID_INPUT", "Address.Country required", { field = "Address" })
  end
  local dims = nil
  if msg.Dimensions then
    dims = {
      length = tonumber(msg.Dimensions.Length),
      width = tonumber(msg.Dimensions.Width),
      height = tonumber(msg.Dimensions.Height),
    }
  end
  local shipping = pick_shipping(msg["Site-Id"], address, cart.subtotal, cart.weight, dims)
  local tax_rate = pick_tax_rate(msg["Site-Id"], address)
  local tax = cart.subtotal * tax_rate / 100
  local total = cart.subtotal + shipping.rate + tax
  local po_id = gen_id "po"
  state.purchase_orders[po_id] = {
    poId = po_id,
    siteId = msg["Site-Id"],
    companyId = msg["Company-Id"],
    items = cart.lines,
    address = address,
    currency = currency,
    subtotal = cart.subtotal,
    weight = cart.weight,
    tax = tax,
    taxRate = tax_rate,
    shipping = shipping,
    total = total,
    promo = msg.Promo,
    status = "pending_approval",
    approvals = {},
  }
  audit.record("catalog", "CreatePurchaseOrder", msg, nil, { poId = po_id, total = total })
  return codec.ok { poId = po_id, status = "pending_approval", total = total, currency = currency }
end

function handlers.ApprovePurchaseOrder(msg)
  local ok, missing = validation.require_fields(msg, { "PO-Id", "Approver-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "PO-Id",
    "Approver-Id",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local po = state.purchase_orders[msg["PO-Id"]]
  if not po then
    return codec.error("NOT_FOUND", "Purchase order not found")
  end
  if not require_company_role(po.companyId, msg["Approver-Id"], { "approver", "admin" }) then
    return codec.error("FORBIDDEN", "Approver not allowed")
  end
  po.status = "approved"
  po.approvals[msg["Approver-Id"]] = "approved"
  audit.record("catalog", "ApprovePurchaseOrder", msg, nil, { poId = po.poId })
  return codec.ok { poId = po.poId, status = po.status }
end

function handlers.RejectPurchaseOrder(msg)
  local ok, missing = validation.require_fields(msg, { "PO-Id", "Approver-Id", "Reason" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "PO-Id",
    "Approver-Id",
    "Reason",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local po = state.purchase_orders[msg["PO-Id"]]
  if not po then
    return codec.error("NOT_FOUND", "Purchase order not found")
  end
  if not require_company_role(po.companyId, msg["Approver-Id"], { "approver", "admin" }) then
    return codec.error("FORBIDDEN", "Approver not allowed")
  end
  po.status = "rejected"
  po.approvals[msg["Approver-Id"]] = "rejected"
  po.rejectionReason = msg.Reason
  audit.record("catalog", "RejectPurchaseOrder", msg, nil, { poId = po.poId })
  return codec.ok { poId = po.poId, status = po.status, reason = msg.Reason }
end

function handlers.CheckoutPurchaseOrder(msg)
  local ok, missing = validation.require_fields(msg, { "PO-Id", "Payment-Method" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "PO-Id",
    "Payment-Method",
    "Require3DS",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local po = state.purchase_orders[msg["PO-Id"]]
  if not po then
    return codec.error("NOT_FOUND", "Purchase order not found")
  end
  if po.status ~= "approved" then
    return codec.error("INVALID_STATE", "PO not approved", { status = po.status })
  end
  -- create checkout-like record without re-quoting
  local items = {}
  for _, line in ipairs(po.items) do
    table.insert(items, { sku = line.sku, qty = line.qty })
  end
  local ok_reserve, changes, backorders = reserve_inventory(po.siteId, items)
  if not ok_reserve then
    return codec.error("OUT_OF_STOCK", "Insufficient inventory")
  end
  local checkout_id = gen_id "chk"
  state.checkouts[checkout_id] = {
    siteId = po.siteId,
    items = items,
    address = po.address,
    email = po.email,
    quote = {
      subtotal = po.subtotal,
      weight = po.weight,
      taxRate = po.taxRate,
      tax = po.tax,
      shipping = po.shipping,
      total = po.total,
      currency = po.currency,
      promo = po.promo,
    },
    status = "pending_payment",
    reserve = changes,
    backorders = backorders,
    poId = po.poId,
    risk = risk_score { quote = { total = po.total, shipping = po.shipping }, address = po.address },
  }
  for _, bo in ipairs(backorders or {}) do
    record_backorder(po.siteId, bo.sku, bo.qty, "po", checkout_id, bo.preorder_at, bo.eta_days)
  end
  local payment = create_payment_intent_internal {
    siteId = po.siteId,
    checkoutId = checkout_id,
    amount = po.total,
    currency = po.currency,
    method = msg["Payment-Method"],
    require3ds = msg.Require3DS,
  }
  po.status = "in_checkout"
  po.checkoutId = checkout_id
  audit.record(
    "catalog",
    "CheckoutPurchaseOrder",
    msg,
    nil,
    { poId = po.poId, checkoutId = checkout_id }
  )
  return codec.ok {
    poId = po.poId,
    checkoutId = checkout_id,
    paymentId = payment.paymentId,
    paymentStatus = payment.status,
    total = po.total,
    currency = po.currency,
  }
end

-- Invoicing ---------------------------------------------------------------
local function persist_invoice(inv)
  if INVOICE_EXPORT_PATH and INVOICE_EXPORT_PATH ~= "" and json_ok then
    local f = io.open(INVOICE_EXPORT_PATH, "a")
    if f then
      local ok, line = pcall(cjson.encode, inv)
      if ok and line then
        f:write(line)
        f:write "\n"
      end
      f:close()
    end
  end
  if INVOICE_PDF_DIR and INVOICE_PDF_DIR ~= "" then
    os.execute("mkdir -p " .. INVOICE_PDF_DIR)
    local path = string.format("%s/%s.pdf", INVOICE_PDF_DIR, inv.invoiceId)
    local f = io.open(path, "w")
    if f then
      f:write(string.format("INVOICE %s (%s)\n", inv.invoiceNumber or inv.invoiceId, inv.siteId))
      f:write(
        string.format(
          "Order: %s\nCurrency: %s\nTotal: %.2f\n",
          inv.orderId or "-",
          inv.currency,
          inv.total or 0
        )
      )
      f:write "Lines:\n"
      for _, line in ipairs(inv.lines or {}) do
        f:write(
          string.format(
            "- %s x%s @ %s\n",
            line.sku or line.Sku or "item",
            line.qty or line.Qty or "1",
            line.unit_price or line.price or "?"
          )
        )
      end
      f:write(
        string.format(
          "Tax: %.2f\nShipping: %.2f\nIssued: %s\n",
          inv.tax or 0,
          inv.shipping or 0,
          inv.issuedAt or ""
        )
      )
      f:close()
      inv.pdfPath = path
    end
  end
  if INVOICE_S3_BUCKET and INVOICE_S3_BUCKET ~= "" and inv.pdfPath then
    local ok = s3_copy_with_retry(inv.pdfPath, INVOICE_S3_BUCKET)
    if ok then
      inv.s3Url = string.format("s3://%s/%s", INVOICE_S3_BUCKET, inv.invoiceId .. ".pdf")
    end
  end
  -- render HTML->PDF via external tool if configured
  local rendered = render_invoice_pdf(inv)
  if rendered then
    inv.pdfPath = rendered
    if INVOICE_S3_BUCKET and INVOICE_S3_BUCKET ~= "" then
      local ok = s3_copy_with_retry(inv.pdfPath, INVOICE_S3_BUCKET)
      if ok then
        inv.s3Url = string.format("s3://%s/%s", INVOICE_S3_BUCKET, inv.invoiceId .. ".pdf")
      end
    end
  end
  -- attach signature if secret present
  if INVOICE_SIGN_SECRET and json_ok then
    local ok_enc, body = pcall(cjson.encode, inv)
    if ok_enc then
      inv.signature = auth.hmac(body, INVOICE_SIGN_SECRET)
    end
  end
end

function handlers.CreateInvoice(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Order-Id", "Lines" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Order-Id",
    "Lines",
    "Currency",
    "Total",
    "Tax",
    "Shipping",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_lines, err_lines = validation.assert_type(msg.Lines, "table", "Lines")
  if not ok_lines or #msg.Lines == 0 then
    return codec.error("INVALID_INPUT", err_lines or "Lines must be non-empty", { field = "Lines" })
  end
  local currency = msg.Currency or "USD"
  local total = msg.Total
  local lines_total = 0
  for _, line in ipairs(msg.Lines) do
    local qty = tonumber(line.qty or line.Qty or line.quantity or 0) or 0
    local unit = tonumber(line.unit_price or line.Unit or line.price or 0) or 0
    if qty <= 0 or unit < 0 then
      return codec.error("INVALID_INPUT", "Line qty/unit_price invalid", { line = line })
    end
    lines_total = lines_total + qty * unit
  end
  if total == nil then
    total = lines_total + (msg.Tax or 0) + (msg.Shipping or 0)
  elseif type(total) ~= "number" or total < 0 then
    return codec.error("INVALID_INPUT", "Total must be non-negative number")
  end
  local inv_id = gen_id "inv"
  local year = os.date "%Y"
  local seq
  if INVOICE_NUMBER_WITH_YEAR then
    state.invoice_seq_year[msg["Site-Id"]] = state.invoice_seq_year[msg["Site-Id"]] or {}
    seq = (state.invoice_seq_year[msg["Site-Id"]][year] or 0) + 1
    state.invoice_seq_year[msg["Site-Id"]][year] = seq
  else
    seq = (state.invoice_seq[msg["Site-Id"]] or 0) + 1
    state.invoice_seq[msg["Site-Id"]] = seq
  end
  local invoice_number = INVOICE_NUMBER_WITH_YEAR
      and string.format("%s-%s-%06d", msg["Site-Id"], year, seq)
    or string.format("%s-%06d", msg["Site-Id"], seq)
  local inv = {
    invoiceId = inv_id,
    invoiceNumber = invoice_number,
    siteId = msg["Site-Id"],
    orderId = msg["Order-Id"],
    currency = currency,
    lines = msg.Lines,
    tax = msg.Tax or 0,
    shipping = msg.Shipping or 0,
    total = total,
    issuedAt = os.date "!%Y-%m-%dT%H:%M:%SZ",
    status = "issued",
    pdfUrl = string.format("%s%s.pdf", CARRIER_LABEL_BASE, inv_id),
  }
  state.invoices[inv_id] = inv
  persist_invoice(inv)
  audit.record("catalog", "CreateInvoice", msg, nil, { invoiceId = inv_id, orderId = inv.orderId })
  return codec.ok(inv)
end

function handlers.GetInvoice(msg)
  local ok, missing = validation.require_fields(msg, { "Invoice-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Invoice-Id", "Actor-Role", "Schema-Version" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local inv = state.invoices[msg["Invoice-Id"]]
  if not inv then
    return codec.error("NOT_FOUND", "Invoice not found")
  end
  return codec.ok(inv)
end

function handlers.ListInvoices(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Order-Id",
    "Actor-Role",
    "Schema-Version",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local items = {}
  for id, inv in pairs(state.invoices) do
    if not msg["Site-Id"] or inv.siteId == msg["Site-Id"] then
      if not msg["Order-Id"] or inv.orderId == msg["Order-Id"] then
        table.insert(items, inv)
      end
    end
  end
  table.sort(items, function(a, b)
    return (a.issuedAt or "") > (b.issuedAt or "")
  end)
  return codec.ok { total = #items, items = items }
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
  if seen then
    return seen
  end

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
