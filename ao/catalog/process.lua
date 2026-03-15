-- Catalog process handlers: products, categories, listings.
-- luacheck: ignore parse_header mark_webhook_seen record_shipment_event notify_customer purge_cache
-- luacheck: ignore resize_and_store add_price_window parse_set is_eu is_vat_id_valid dimensional_weight
-- luacheck: ignore push_low_stock deliver_stock_alert forget_subject pick_tax_rule

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
local TELEMETRY_KAFKA_PATH = os.getenv "CATALOG_TELEMETRY_KAFKA" -- mock sink file
local TELEMETRY_S3_PATH = os.getenv "CATALOG_TELEMETRY_S3" -- mock sink file
local INVOICE_EXPORT_PATH = os.getenv "CATALOG_INVOICE_PATH"
local CARRIER_LABEL_BASE = os.getenv "CATALOG_CARRIER_LABEL_BASE" or "https://labels.example/"
local CARRIER_TRACK_BASE = os.getenv "CATALOG_CARRIER_TRACK_BASE" or "https://track.example/"
local CARRIER_API_URL = os.getenv "CATALOG_CARRIER_API_URL" -- optional external rate/label stub
local CARRIER_API_TOKEN = os.getenv "CATALOG_CARRIER_API_TOKEN"
local INVOICE_PDF_DIR = os.getenv "CATALOG_INVOICE_PDF_DIR"
local INVOICE_NUMBER_WITH_YEAR = os.getenv "CATALOG_INVOICE_YEAR" ~= "0"
local INVOICE_S3_BUCKET = os.getenv "CATALOG_INVOICE_S3_BUCKET"
local HTTP_TIMEOUT = tonumber(os.getenv "CATALOG_HTTP_TIMEOUT" or "") or 5
local HTTP_CONNECT_TIMEOUT = tonumber(os.getenv "CATALOG_HTTP_CONNECT_TIMEOUT" or "") or 2
local S3_TIMEOUT = tonumber(os.getenv "CATALOG_S3_TIMEOUT" or "") or 10
local S3_RETRIES = tonumber(os.getenv "CATALOG_S3_RETRIES" or "") or 2
local EVENT_LOG_LIMIT = tonumber(os.getenv "CATALOG_EVENT_LOG_LIMIT" or "") or 5000
local RATE_LIMIT_WINDOW = tonumber(os.getenv "CATALOG_RATE_LIMIT_WINDOW" or "") or 60
local RATE_LIMIT_MAX = tonumber(os.getenv "CATALOG_RATE_LIMIT_MAX" or "") or 120
local GA4_ENDPOINT = os.getenv "CATALOG_GA4_ENDPOINT"
local GA4_API_SECRET = os.getenv "CATALOG_GA4_API_SECRET"
local GA4_MEASUREMENT_ID = os.getenv "CATALOG_GA4_MEASUREMENT_ID"
local PAYMENT_WEBHOOK_SECRET = os.getenv "CATALOG_PAYMENT_WEBHOOK_SECRET"
local CARRIER_WEBHOOK_SECRET = os.getenv "CATALOG_CARRIER_WEBHOOK_SECRET"
local RETURN_LABEL_BASE = os.getenv "CATALOG_RETURN_LABEL_BASE" or CARRIER_LABEL_BASE
local CARRIER_LABEL_API_URL = os.getenv "CATALOG_CARRIER_LABEL_API_URL"
local CARRIER_LABEL_API_KEY = os.getenv "CATALOG_CARRIER_LABEL_API_KEY"
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
local STRIPE_WEBHOOK_ID = os.getenv "CATALOG_STRIPE_WEBHOOK_ID"
local STRIPE_VERIFY_EVENT = os.getenv "CATALOG_STRIPE_VERIFY_EVENT" == "1"
local APPLE_PAY_MERCHANT_ID = os.getenv "CATALOG_APPLE_PAY_MERCHANT_ID" -- luacheck: ignore
local GOOGLE_PAY_MERCHANT_ID = os.getenv "CATALOG_GOOGLE_PAY_MERCHANT_ID" -- luacheck: ignore
local ADYEN_MERCHANT_ACCOUNT = os.getenv "CATALOG_ADYEN_MERCHANT_ACCOUNT" -- luacheck: ignore
local PSP_MODE = os.getenv "CATALOG_PSP_MODE" or "sandbox" -- sandbox|live
local PSP_ALLOW_STUB = os.getenv "CATALOG_PSP_ALLOW_STUB" ~= "0"
local PAYPAL_WEBHOOK_ID = os.getenv "CATALOG_PAYPAL_WEBHOOK_ID"
local PAYPAL_WEBHOOK_SECRET = os.getenv "CATALOG_PAYPAL_WEBHOOK_SECRET"
local PAYPAL_CERT_HOST = os.getenv "CATALOG_PAYPAL_CERT_HOST" or "paypal.com"
local PAYPAL_CERT_CACHE_SEC = tonumber(os.getenv "CATALOG_PAYPAL_CERT_CACHE_SEC" or "") or 3600
local CARRIER_WEBHOOK_TOLERANCE = tonumber(os.getenv "CATALOG_CARRIER_WEBHOOK_TOLERANCE" or "")
  or 600
local ADYEN_HMAC_KEY = os.getenv "CATALOG_ADYEN_HMAC_KEY"
local CDN_SURROGATE_CMD = os.getenv "CATALOG_CDN_SURROGATE_CMD"
-- optional, e.g. "curl -sS -X POST https://api.fastly.com/service/... -H 'Fastly-Key: ...' -H 'Surrogate-Key: %s'"
local IMAGE_RESIZE_CMD = os.getenv "CATALOG_IMAGE_RESIZE_CMD" -- e.g. "vipsthumbnail %s --size %dx%d -o %s"
local IMAGE_STORE_DIR = os.getenv "CATALOG_IMAGE_STORE_DIR"
local IMAGE_FORMATS = os.getenv "CATALOG_IMAGE_FORMATS" or "webp,avif,jpg"
local IMAGE_SIZES = os.getenv "CATALOG_IMAGE_SIZES" or "320x320,640x640,1280x1280"
local IMAGE_S3_BUCKET = os.getenv "CATALOG_IMAGE_S3_BUCKET"
local IMAGE_S3_PREFIX = os.getenv "CATALOG_IMAGE_S3_PREFIX" or ""
local IMAGE_PUBLIC_BASE = os.getenv "CATALOG_IMAGE_PUBLIC_BASE"
local US_NEXUS_STATES = os.getenv "CATALOG_US_NEXUS_STATES" or ""
local RETENTION_DAYS = tonumber(os.getenv "CATALOG_RETENTION_DAYS" or "") or 30
local SEARCH_SYNONYMS_PATH = os.getenv "CATALOG_SEARCH_SYNONYMS_PATH"
local SEARCH_STOPWORDS_PATH = os.getenv "CATALOG_SEARCH_STOPWORDS_PATH"
local CUSTOMER_WEBHOOK = os.getenv "CATALOG_CUSTOMER_WEBHOOK"
local NOTIFY_RETRIES = tonumber(os.getenv "CATALOG_NOTIFY_RETRIES" or "") or 2
local NOTIFY_BACKOFF_MS = tonumber(os.getenv "CATALOG_NOTIFY_BACKOFF_MS" or "") or 200
local IMPORT_MAX_ROWS = tonumber(os.getenv "CATALOG_IMPORT_MAX_ROWS" or "") or 5000
local THREE_DS_URL = os.getenv "CATALOG_3DS_URL" or "https://3ds.example.com/challenge/"
local WEBHOOK_REPLAY_WINDOW = tonumber(os.getenv "CATALOG_WEBHOOK_REPLAY_WINDOW" or "") or 600
local CHALLENGE_TTL = tonumber(os.getenv "CATALOG_3DS_TTL" or "") or 900
local MERCHANT_COUNTRY = (os.getenv "CATALOG_MERCHANT_COUNTRY" or "US"):upper()

local openssl_ok, openssl = pcall(require, "openssl")
local sodium_ok, sodium = pcall(require, "sodium")
if not sodium_ok then
  sodium_ok, sodium = pcall(require, "luasodium")
end

-- forward declarations to satisfy luacheck
local parse_header
local mark_webhook_seen
local record_shipment_event
local notify_customer
local purge_cache
local resize_and_store
local add_price_window
local parse_set
local is_eu
local is_vat_id_valid
local dimensional_weight
local push_low_stock
local deliver_stock_alert
local forget_subject

local handlers = {}
local allowed_actions = {
  "GetProduct",
  "ListCategoryProducts",
  "SearchCatalog",
  "FacetSearch",
  "GetRecommendations",
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
  "ApplyCoupon",
  "UpsertPriceList",
  "SetPriceList",
  "QuotePrice",
  "SetTaxRules",
  "SetShippingRules",
  "QuoteOrder",
  "StartCheckout",
  "CompleteCheckout",
  "SetInventory",
  "GetInventory",
  "TrackCatalogEvent",
  "ExportEvents",
  "RelatedProducts",
  "RecentlyViewed",
  "GetRecommendations",
  "CreatePaymentIntent",
  "CapturePayment",
  "RefundPayment",
  "SavePaymentToken",
  "AddStoreCredit",
  "ApplyStoreCredit",
  "SaveAddress",
  "ListAddresses",
  "SetConsents",
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
  "SetCompanyTerms",
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
  "SetEdgeCachePolicy",
  "SetFeatureFlags",
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
  "ExportRecommendations",
  "ListNotificationFailures",
  "ImportCatalogCSV",
  "BulkPriceUpdate",
  "Complete3DSChallenge",
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
  UpsertPriceList = { "catalog-admin", "admin" },
  AddPromo = { "catalog-admin", "admin" },
  ApplyCoupon = { "catalog-admin", "support", "admin", "viewer" },
  QuotePrice = { "catalog-admin", "support", "admin" },
  SetTaxRules = { "catalog-admin", "admin" },
  SetShippingRules = { "catalog-admin", "admin" },
  QuoteOrder = { "catalog-admin", "support", "admin" },
  StartCheckout = { "catalog-admin", "support", "admin" },
  CompleteCheckout = { "catalog-admin", "support", "admin" },
  SetInventory = { "catalog-admin", "admin" },
  GetInventory = { "catalog-admin", "support", "admin" },
  TrackCatalogEvent = { "catalog-admin", "support", "admin", "viewer" },
  ExportEvents = { "admin", "catalog-admin", "support" },
  RelatedProducts = { "catalog-admin", "support", "admin", "viewer" },
  RecentlyViewed = { "catalog-admin", "support", "admin", "viewer" },
  GetRecommendations = { "catalog-admin", "support", "admin", "viewer" },
  CreatePaymentIntent = { "catalog-admin", "support", "admin" },
  CapturePayment = { "catalog-admin", "support", "admin" },
  RefundPayment = { "catalog-admin", "support", "admin" },
  SavePaymentToken = { "catalog-admin", "support", "admin", "viewer" },
  AddStoreCredit = { "support", "catalog-admin", "admin" },
  ApplyStoreCredit = { "catalog-admin", "support", "admin", "viewer" },
  SaveAddress = { "catalog-admin", "support", "admin", "viewer" },
  ListAddresses = { "catalog-admin", "support", "admin", "viewer" },
  SetConsents = { "catalog-admin", "support", "admin", "viewer" },
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
  SetCompanyTerms = { "b2b-admin", "admin" },
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
  SetEdgeCachePolicy = { "catalog-admin", "admin" },
  SetFeatureFlags = { "catalog-admin", "admin" },
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
  ExportRecommendations = { "catalog-admin", "support", "admin", "viewer" },
  ListNotificationFailures = { "admin", "catalog-admin", "support" },
  ImportCatalogCSV = { "catalog-admin", "admin" },
  BulkPriceUpdate = { "catalog-admin", "admin" },
  Complete3DSChallenge = { "catalog-admin", "support", "admin" },
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
  assets = {}, -- siteId -> sku -> { original, variants = { {url, w, h, fmt} } }
  shipping_rates = {}, -- siteId -> list of rate rows
  tax_rates = {}, -- siteId -> list of tax rows
  price_lists = {}, -- siteId -> currency -> { sku -> price }
  price_windows = {}, -- siteId -> currency -> { { region, valid_from, valid_to, prices = {sku=price} } }
  promos = {}, -- code -> { type = "percent"|"amount", value, skus }
  coupons = {}, -- code -> { type, value, applies_to, free_shipping }
  variants = {}, -- siteId -> parentSku -> { variants = { { sku, attrs, price } } }
  tax_rules = {}, -- siteId -> list { country, region?, rate }
  shipping_rules = {}, -- siteId -> list { country, min_total, max_total, rate, carrier, service }
  checkouts = {}, -- checkoutId -> { siteId, items, address, quote, status }
  events = {}, -- siteId -> sku -> { views, add_to_cart, purchases }
  recent = {}, -- subject -> list of { siteId, sku } (most recent first, capped)
  payments = {}, -- paymentId -> { status, amount, currency, method, siteId, orderId, checkoutId, requiresAction }
  payment_tokens = {}, -- subject -> { { provider, token, last4, brand, exp, default=true? } }
  store_credit = {}, -- subject -> { balance, currency }
  address_book = {}, -- subject -> { entries = { ... } }
  consents = {}, -- subject -> map of consent flags
  telemetry = {}, -- buffered events for export
  companies = {}, -- companyId -> { name, users = { [userId] = role } }
  purchase_orders = {}, -- poId -> { siteId, companyId, items, totals, status, approvals = {} }
  company_terms = {}, -- companyId -> { credit_limit, net_terms, currency, balance }
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
  rate_limits = {}, -- key -> { count, window_start }
  search_synonyms = {}, -- siteId -> map term -> {synonyms}
  search_stopwords = {}, -- siteId -> set of stopwords
  notification_failures = {}, -- siteId -> list of { type, target, payload, attempts, ts }
  payment_attempts = {}, -- paymentId -> list of events
  webhook_seen = {}, -- id -> ts for replay protection
  provider_events = {}, -- provider -> id -> ts
  stripe_idempotency = {}, -- idemKey -> { paymentId, status }
  paypal_certs = {}, -- url -> { pem, fetchedAt }
}

local function gen_id(prefix)
  return string.format("%s-%d-%04d", prefix, os.time(), math.random(0, 9999))
end

local function hex_encode(bytes)
  if not bytes then
    return nil
  end
  if openssl_ok and openssl.hex then
    return openssl.hex(bytes)
  end
  if sodium_ok then
    if sodium.to_hex then
      return sodium.to_hex(bytes)
    end
    if sodium.bin2hex then
      return sodium.bin2hex(bytes)
    end
  end
  return (bytes:gsub(".", function(c)
    return string.format("%02x", string.byte(c))
  end))
end

local function hmac_sha256_hex(data, key)
  if not key or key == "" then
    return nil, "missing_key"
  end
  if openssl_ok and openssl.hmac then
    local raw = openssl.hmac.digest("sha256", data, key, true)
    return hex_encode(raw)
  end
  if sodium_ok and sodium.crypto_auth then
    local raw = sodium.crypto_auth(data, key)
    return hex_encode(raw)
  end
  return nil, "hmac_unavailable"
end

-- PSP adapter shim -------------------------------------------------------
local function psp_call(provider, action, payload)
  -- Pluggable PSP adapters. Currently sandbox stubs; replace with real REST/SDK calls when keys are provided.
  -- Contract:
  --   create_intent -> ok, { providerPaymentId, clientSecret, requiresAction?, nextActionUrl? } or nil, err
  --   capture       -> ok, { status = "captured", providerCaptureId? } or nil, err
  --   refund        -> ok, { status = "refunded", refundedAt?, amount? } or nil, err

  local adapters = {}

  adapters.stripe = function(act, p)
    if not STRIPE_SECRET or STRIPE_SECRET == "" then
      return nil, "PSP_NOT_CONFIGURED"
    end
    if act == "create_intent" then
      return true,
        {
          providerPaymentId = "pi_" .. gen_id "stripe",
          clientSecret = "cs_" .. gen_id "stripe",
          requiresAction = p.require3ds == true,
          nextActionUrl = p.require3ds and (THREE_DS_URL .. "?pid=" .. p.id) or nil,
        }
    elseif act == "capture" then
      return true, { status = "captured", capturedAt = os.time() }
    elseif act == "refund" then
      return true, { status = "refunded", refundedAt = os.time(), amount = p.amount }
    end
    return nil, "UNSUPPORTED_ACTION"
  end

  adapters.adyen = function(act, p)
    if not ADYEN_HMAC_KEY or ADYEN_HMAC_KEY == "" then
      return nil, "PSP_NOT_CONFIGURED"
    end
    if act == "create_intent" then
      return true,
        {
          providerPaymentId = "adyen_" .. gen_id "adyen",
          clientSecret = "sec_" .. gen_id "adyen",
          requiresAction = false,
        }
    elseif act == "capture" then
      return true, { status = "captured", capturedAt = os.time() }
    elseif act == "refund" then
      return true, { status = "refunded", refundedAt = os.time(), amount = p.amount }
    end
    return nil, "UNSUPPORTED_ACTION"
  end

  adapters.paypal = function(act, p)
    if not PAYPAL_WEBHOOK_ID or not PAYPAL_WEBHOOK_SECRET then
      return nil, "PSP_NOT_CONFIGURED"
    end
    if act == "create_intent" then
      return true,
        {
          providerPaymentId = "pp_" .. gen_id "pp",
          clientSecret = "sec_" .. gen_id "pp",
          requiresAction = false,
        }
    elseif act == "capture" then
      return true, { status = "captured", capturedAt = os.time() }
    elseif act == "refund" then
      return true, { status = "refunded", refundedAt = os.time(), amount = p.amount }
    end
    return nil, "UNSUPPORTED_ACTION"
  end

  adapters.default = function(act, p)
    if not PSP_ALLOW_STUB then
      return nil, "PSP_NOT_CONFIGURED"
    end
    if act == "create_intent" then
      return true,
        {
          providerPaymentId = "int_" .. gen_id "psp",
          clientSecret = "sec_" .. gen_id "psp",
          requiresAction = p.require3ds == true,
          nextActionUrl = p.require3ds and (THREE_DS_URL .. "?pid=" .. p.id) or nil,
        }
    elseif act == "capture" then
      return true, { status = "captured", capturedAt = os.time() }
    elseif act == "refund" then
      return true, { status = "refunded", refundedAt = os.time(), amount = p.amount }
    end
    return nil, "UNSUPPORTED_ACTION"
  end

  local adapter = adapters[provider] or adapters.default
  return adapter(action, payload)
end

local function mark_event_seen(provider, event_id, ts)
  if not provider or not event_id then
    return true
  end
  ts = ts or os.time()
  state.provider_events[provider] = state.provider_events[provider] or {}
  local last = state.provider_events[provider][event_id]
  if last and (ts - last) <= WEBHOOK_REPLAY_WINDOW then
    return false, "event_replayed"
  end
  state.provider_events[provider][event_id] = ts
  return true
end

local function stripe_fetch_event(event_id)
  if not STRIPE_SECRET or STRIPE_SECRET == "" or not event_id then
    return nil, "missing_secret"
  end
  local url = string.format("https://api.stripe.com/v1/events/%s", event_id)
  local cmd = string.format(
    "curl -sS --max-time %d --connect-timeout %d -u '%s:' %s",
    HTTP_TIMEOUT,
    HTTP_CONNECT_TIMEOUT,
    STRIPE_SECRET,
    url
  )
  local reader = io.popen(cmd, "r")
  if not reader then
    return nil, "curl_failed"
  end
  local body = reader:read "*a"
  local ok_close = reader:close()
  if not ok_close then
    return nil, "curl_exit"
  end
  if not json_ok then
    return body
  end
  local ok_dec, obj = pcall(cjson.decode, body)
  if ok_dec then
    return obj
  end
  return nil, "decode_failed"
end

local function hostname_from_url(url)
  if not url or url == "" then
    return nil
  end
  return url:match "^https?://([^/]+)"
end

local function fetch_paypal_cert(cert_url)
  if not cert_url or cert_url == "" then
    return nil, "no_cert_url"
  end
  local cached = state.paypal_certs[cert_url]
  if cached and (os.time() - cached.fetchedAt) < PAYPAL_CERT_CACHE_SEC then
    return cached.pem
  end
  local host = hostname_from_url(cert_url)
  if not host or not host:match(PAYPAL_CERT_HOST:gsub("%.", "%%.") .. "$") then
    return nil, "cert_host_blocked"
  end
  local cmd = string.format(
    "curl -sS --max-time %d --connect-timeout %d '%s'",
    HTTP_TIMEOUT,
    HTTP_CONNECT_TIMEOUT,
    cert_url
  )
  local reader = io.popen(cmd, "r")
  if not reader then
    return nil, "curl_failed"
  end
  local pem = reader:read "*a"
  local ok_close = reader:close()
  if not ok_close or not pem or pem == "" then
    return nil, "curl_exit"
  end
  state.paypal_certs[cert_url] = { pem = pem, fetchedAt = os.time() }
  return pem
end

local function verify_paypal_cert_signature(signed, signature_b64, cert_pem)
  if not signature_b64 or signature_b64 == "" then
    return false, "missing_signature"
  end
  local tmp_sig = os.tmpname()
  local tmp_cert = os.tmpname()
  local tmp_data = os.tmpname()
  local fdata = io.open(tmp_data, "w")
  if not fdata then
    return false, "tmp_data_failed"
  end
  fdata:write(signed)
  fdata:close()
  local tmp_b64 = os.tmpname()
  local fb = io.open(tmp_b64, "w")
  if not fb then
    os.remove(tmp_data)
    return false, "tmp_b64_failed"
  end
  fb:write(signature_b64)
  fb:close()
  local dec_rc = os.execute(string.format("base64 -d %s > %s", tmp_b64, tmp_sig))
  os.remove(tmp_b64)
  if dec_rc ~= true and dec_rc ~= 0 then
    os.remove(tmp_data)
    os.remove(tmp_sig)
    return false, "base64_decode_failed"
  end
  local fcert = io.open(tmp_cert, "w")
  if not fcert then
    os.remove(tmp_data)
    if tmp_sig then
      os.remove(tmp_sig)
    end
    return false, "tmp_cert_failed"
  end
  fcert:write(cert_pem)
  fcert:close()
  local cmd =
    string.format("openssl dgst -sha256 -verify %s -signature %s %s", tmp_cert, tmp_sig, tmp_data)
  local rc = os.execute(cmd)
  os.remove(tmp_data)
  if tmp_sig then
    os.remove(tmp_sig)
  end
  os.remove(tmp_cert)
  return rc == true or rc == 0, rc
end

local function cache_stripe_idempotency(msg, result)
  local idem_key = msg.IdempotencyKey or parse_header(msg.Headers, "Idempotency-Key")
  if idem_key and idem_key ~= "" then
    state.stripe_idempotency[idem_key] = result
  end
end

local function check_stripe_idempotency(msg)
  local idem_key = msg.IdempotencyKey or parse_header(msg.Headers, "Idempotency-Key")
  if idem_key and state.stripe_idempotency[idem_key] then
    return state.stripe_idempotency[idem_key]
  end
end

local function validate_payment_event(ev, pay)
  if
    ev.currency
    and pay.currency
    and tostring(ev.currency):upper() ~= tostring(pay.currency):upper()
  then
    return false, "currency_mismatch"
  end
  if ev.amount and pay.amount and ev.amount > (pay.amount + 0.01) then
    return false, "amount_exceeds"
  end
  if ev.type == "refund_succeeded" or ev.refundAmount then
    local refund_amt = ev.amount or ev.refundAmount or pay.refundAmount or 0
    if refund_amt > pay.amount then
      return false, "refund_gt_payment"
    end
  end
  if ev.orderId and pay.orderId and ev.orderId ~= pay.orderId then
    return false, "order_mismatch"
  end
  return true
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

-- PII redaction for audit logs ------------------------------------------
local pii_keys = {
  email = true,
  Email = true,
  phone = true,
  Phone = true,
  Address = true,
  address = true,
  subject = true,
  Subject = true,
}

local function scrub_pii(obj, depth)
  if depth > 3 then
    return obj
  end
  if type(obj) ~= "table" then
    return obj
  end
  local copy = {}
  for k, v in pairs(obj) do
    if pii_keys[k] then
      copy[k] = "[redacted]"
    else
      copy[k] = scrub_pii(v, depth + 1)
    end
  end
  return copy
end

local _audit_record = audit.record
audit.record = function(actor, action, msg, resp, meta)
  return _audit_record(actor, action, scrub_pii(msg, 0), scrub_pii(resp, 0), scrub_pii(meta, 0))
end

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

local function typo_match(text, tokens)
  text = text:lower()
  for _, t in ipairs(tokens) do
    if text:find(t, 1, true) then
      return true
    end
  end
  return false
end

local function check_rate_limit(key)
  local now = os.time()
  local bucket = state.rate_limits[key]
  if not bucket or now - bucket.window_start >= RATE_LIMIT_WINDOW then
    state.rate_limits[key] = { count = 1, window_start = now }
    return true
  end
  bucket.count = bucket.count + 1
  if bucket.count > RATE_LIMIT_MAX then
    return false
  end
  return true
end

local function normalize_provider(p)
  if p == "apple_pay" or p == "google_pay" then
    return "stripe"
  end
  return p or "internal"
end

local function create_payment_intent_internal(args)
  -- args: siteId, checkoutId?, orderId?, amount, currency, method, require3ds?, provider?, token?, subject?
  local payment_id = gen_id "pay"
  local requires_action = (args.require3ds == true) or SCA_FORCE
  local status = requires_action and "requires_action" or "authorized"
  local ok_psp, provider_payload = psp_call(normalize_provider(args.provider), "create_intent", {
    id = payment_id,
    amount = args.amount,
    currency = args.currency,
    token = args.token,
    require3ds = args.require3ds,
    subject = args.subject,
    mode = PSP_MODE,
  })
  if not ok_psp then
    -- fallback to internal stub if allowed
    if PSP_ALLOW_STUB then
      ok_psp, provider_payload = psp_call("internal", "create_intent", {
        id = payment_id,
        amount = args.amount,
        currency = args.currency,
        token = args.token,
        require3ds = args.require3ds,
        subject = args.subject,
        mode = PSP_MODE,
      })
    end
    if not ok_psp then
      return nil, provider_payload or "PSP_ERROR"
    end
  end
  local record = {
    paymentId = payment_id,
    siteId = args.siteId,
    checkoutId = args.checkoutId,
    orderId = args.orderId,
    amount = args.amount,
    currency = args.currency,
    method = args.method,
    provider = ok_psp and normalize_provider(args.provider) or "internal",
    token = args.token,
    subject = args.subject,
    status = status,
    requiresAction = provider_payload.requiresAction or requires_action,
    providerPaymentId = provider_payload.providerPaymentId,
    clientSecret = provider_payload.clientSecret or ("sec_" .. payment_id),
    nextActionUrl = provider_payload.nextActionUrl
      or (requires_action and (THREE_DS_URL .. payment_id) or nil),
    createdAt = os.time(),
  }
  state.payments[payment_id] = record
  state.payment_attempts[payment_id] = {
    {
      ts = os.time(),
      event = "created",
      status = status,
      amount = args.amount,
      provider = record.provider,
    },
  }
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

local function http_post_json(url, payload, opts)
  opts = opts or {}
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

  local header_flags = "-H 'Content-Type: application/json'"
  if opts.headers then
    for k, v in pairs(opts.headers) do
      if v and v ~= "" then
        header_flags = header_flags .. string.format(" -H '%s: %s'", k, v)
      end
    end
  end
  if opts.Authorization then
    header_flags = header_flags .. string.format(" -H 'Authorization: %s'", opts.Authorization)
  end
  if opts.bearer then
    header_flags = header_flags .. string.format(" -H 'Authorization: Bearer %s'", opts.bearer)
  end
  if
    not opts.Authorization
    and not opts.bearer
    and CARRIER_API_TOKEN
    and CARRIER_API_URL
    and url:find(CARRIER_API_URL, 1, true)
  then
    header_flags = header_flags
      .. string.format(" -H 'Authorization: Bearer %s'", CARRIER_API_TOKEN)
  end

  local timeout = opts.timeout or HTTP_TIMEOUT
  local connect_timeout = opts.connect_timeout or HTTP_CONNECT_TIMEOUT
  local cmd = string.format(
    "curl -sS --max-time %d --connect-timeout %d -X POST %s %s --data-binary @%s",
    timeout,
    connect_timeout,
    header_flags,
    url,
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
  if opts.decode == false then
    return out, nil
  end
  if json_ok then
    local ok_dec, obj = pcall(cjson.decode, out)
    if ok_dec then
      return obj, nil
    end
  end
  return out, nil
end

local function s3_copy_with_retry(path, bucket)
  if not bucket or bucket == "" then
    return false
  end
  for _ = 1, (S3_RETRIES + 1) do
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

local function build_label(carrier, service, weight, dims)
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
    dimensions = dims,
    status = "label_created",
  }
  -- optional remote label creation
  if CARRIER_LABEL_API_URL and json_ok then
    local payload = {
      carrier = carrier,
      service = service,
      tracking = tracking,
      shipmentId = shipment_id,
      weight = weight,
    }
    local resp = http_post_json(CARRIER_LABEL_API_URL, payload, {
      Authorization = CARRIER_LABEL_API_KEY and ("Bearer " .. CARRIER_LABEL_API_KEY) or nil,
    })
    if resp and type(resp) == "table" then
      label.labelUrl = resp.labelUrl or label.labelUrl
      label.tracking = resp.tracking or label.tracking
      label.trackingUrl = resp.trackingUrl or label.trackingUrl
    end
  elseif CARRIER_API_URL and json_ok then
    http_post_json(CARRIER_API_URL .. "/label", {
      carrier = carrier,
      service = service,
      tracking = tracking,
      shipmentId = shipment_id,
      weight = weight,
    })
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

local function calculate_tax_breakdown(site_id, address, cart, shipping_rate)
  local tax = 0
  local line_taxes = {}
  local subtotal_ex = 0
  local reverse_charge = false
  local nexus_states = parse_set(US_NEXUS_STATES)
  local us_taxable = true
  if address.Country == "US" and next(nexus_states) and address.Region then
    us_taxable = nexus_states[address.Region:upper()] == true
  end
  if
    is_eu(MERCHANT_COUNTRY)
    and is_eu(address.Country)
    and address.Country:upper() ~= MERCHANT_COUNTRY
    and is_vat_id_valid(address.VatId or address.VAT or address.VATID)
  then
    reverse_charge = true
  end
  for _, line in ipairs(cart.lines) do
    local rule, rate = pick_tax_rule(site_id, address, line.taxClass)
    local incl = (line.taxInclusive == true) or (rule and rule.taxInclusive == true)
    local line_net = line.line_total
    local lt = 0
    if rate > 0 and not reverse_charge and us_taxable then
      if incl then
        local divisor = 1 + rate / 100
        line_net = line.line_total / divisor
        lt = line.line_total - line_net
      else
        lt = line.line_total * rate / 100
      end
    end
    subtotal_ex = subtotal_ex + line_net
    tax = tax + lt
    table.insert(line_taxes, {
      sku = line.sku,
      tax = lt,
      taxRate = rate,
      taxInclusive = incl,
    })
  end
  local shipping_tax = 0
  if shipping_rate and shipping_rate > 0 then
    local rule, rate = pick_tax_rule(site_id, address, nil)
    local taxable = not (rule and rule.shippingTaxable == false)
    if rate > 0 and taxable and not reverse_charge and us_taxable then
      shipping_tax = shipping_rate * rate / 100
    end
  end
  return tax, line_taxes, subtotal_ex, shipping_tax, reverse_charge
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
  if not check_rate_limit("search:" .. (msg.Subject or msg["Site-Id"])) then
    return codec.error("RATE_LIMITED", "Too many search requests")
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Query",
    "Segment",
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
    brands = {},
    tags = {},
  }
  local prefix = "product:" .. msg["Site-Id"] .. ":"
  local flags = state.feature_flags and state.feature_flags[msg["Site-Id"]] or {}
  local segment = msg.Segment
  for key, product in pairs(state.products) do
    if key:sub(1, #prefix) == prefix then
      local sku = key:match "product:[^:]+:(.+)"
      local text = (product.payload.name or ""):lower()
        .. " "
        .. (product.payload.description or ""):lower()
      local matched = (q == "") or text:find(q, 1, true) or typo_match(text, expanded_tokens)
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
        local required_flags = payload.flags and payload.flags.required
        local segments = payload.segments
        if required_flags and type(required_flags) == "table" then
          local all_on = true
          for _, f in ipairs(required_flags) do
            if not (flags and flags[f]) then
              all_on = false
            end
          end
          if not all_on then
            goto continue
          end
        end
        if segments and segment then
          local ok_seg = false
          for _, s in ipairs(segments) do
            if s == segment then
              ok_seg = true
            end
          end
          if not ok_seg then
            goto continue
          end
        end
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
        if payload.brand then
          facets.brands[payload.brand] = (facets.brands[payload.brand] or 0) + 1
        end
        if payload.tags and type(payload.tags) == "table" then
          for _, tag in ipairs(payload.tags) do
            if type(tag) == "string" then
              facets.tags[tag] = (facets.tags[tag] or 0) + 1
            end
          end
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
              if #tok <= 5 then
                local d2 = levenshtein((payload.name or ""):lower(), tok)
                if d2 == 1 then
                  score = score + 1
                end
              end
            end
          end
          score = score + (events.purchases or 0) * 2 + (events.views or 0) * 0.1
          if msg.Locale and locale == msg.Locale then
            score = score + 1
          end
          if msg.Segment and segments then
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
    ::continue::
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
  if not check_rate_limit("event:" .. (msg.Subject or msg["Site-Id"])) then
    return codec.error("RATE_LIMITED", "Too many events")
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
    "Format",
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
  if msg.Format == "csv" then
    local lines = { "sku,score" }
    for _, r in ipairs(ranked) do
      table.insert(lines, string.format("%s,%s", r.sku, r.score or 0))
    end
    return codec.ok {
      siteId = msg["Site-Id"],
      sku = msg.Sku,
      format = "csv",
      body = table.concat(lines, "\n"),
    }
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

function handlers.GetRecommendations(msg)
  local limit = msg.Limit or 10
  return handlers.RelatedProducts { ["Site-Id"] = msg["Site-Id"], Sku = msg.Sku, Limit = limit }
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
    "Format",
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
    local score = (s.purchases or 0) * 4 + (s.add_to_cart or 0) * 2 + (s.views or 0) * 0.2
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
  if msg.Format == "csv" then
    local lines = { "sku,score" }
    for _, r in ipairs(ranked) do
      table.insert(lines, string.format("%s,%s", r.sku, r.score or 0))
    end
    return codec.ok { siteId = msg["Site-Id"], format = "csv", body = table.concat(lines, "\n") }
  end
  return codec.ok { siteId = msg["Site-Id"], items = ranked, total = #ranked, format = "json" }
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

-- Addresses, consents, tokens, credit -----------------------------------
function handlers.SaveAddress(msg)
  local ok, missing = validation.require_fields(msg, { "Subject", "Address" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Subject",
    "Address",
    "Label",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  state.address_book[msg.Subject] = state.address_book[msg.Subject] or {}
  table.insert(
    state.address_book[msg.Subject],
    { label = msg.Label or "default", address = msg.Address }
  )
  return codec.ok { subject = msg.Subject, count = #state.address_book[msg.Subject] }
end

function handlers.ListAddresses(msg)
  local ok, missing = validation.require_fields(msg, { "Subject" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Subject", "Actor-Role", "Schema-Version" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  return codec.ok { subject = msg.Subject, addresses = state.address_book[msg.Subject] or {} }
end

function handlers.SetConsents(msg)
  local ok, missing = validation.require_fields(msg, { "Subject", "Consents" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Subject", "Consents", "Actor-Role" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  state.consents[msg.Subject] = msg.Consents
  return codec.ok { subject = msg.Subject, consents = msg.Consents }
end

function handlers.SavePaymentToken(msg)
  local ok, missing = validation.require_fields(msg, { "Subject", "Provider", "Token", "Last4" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  state.payment_tokens[msg.Subject] = state.payment_tokens[msg.Subject] or {}
  table.insert(state.payment_tokens[msg.Subject], {
    provider = msg.Provider,
    token = msg.Token,
    last4 = msg.Last4,
    brand = msg.Brand,
    exp = msg.Exp,
    default = msg.Default == true,
  })
  return codec.ok { subject = msg.Subject, count = #state.payment_tokens[msg.Subject] }
end

function handlers.AddStoreCredit(msg)
  local ok, missing = validation.require_fields(msg, { "Subject", "Amount", "Currency" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local balance = state.store_credit[msg.Subject] or { balance = 0, currency = msg.Currency }
  balance.balance = balance.balance + tonumber(msg.Amount)
  balance.currency = msg.Currency
  state.store_credit[msg.Subject] = balance
  return codec.ok(balance)
end

function handlers.ApplyStoreCredit(msg)
  local ok, missing = validation.require_fields(msg, { "Subject", "Checkout-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local credit = state.store_credit[msg.Subject]
  if not credit or credit.balance <= 0 then
    return codec.error("INSUFFICIENT_CREDIT", "No store credit available")
  end
  local checkout = state.checkouts[msg["Checkout-Id"]]
  if not checkout then
    return codec.error("NOT_FOUND", "Checkout not found")
  end
  local apply = math.min(credit.balance, checkout.total or 0)
  credit.balance = credit.balance - apply
  checkout.total = (checkout.total or 0) - apply
  checkout.storeCredit = apply
  return codec.ok { remaining = credit.balance, applied = apply, currency = credit.currency }
end

function handlers.ExportEvents(msg)
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id" })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local data = msg["Site-Id"] and (state.event_log[msg["Site-Id"]] or {}) or state.event_log
  -- mock sinks: append newline-delimited JSON to paths if configured
  if TELEMETRY_KAFKA_PATH and json_ok then
    local f = io.open(TELEMETRY_KAFKA_PATH, "a")
    if f then
      f:write(cjson.encode { ts = os.time(), events = data }, "\n")
      f:close()
    end
  end
  if TELEMETRY_S3_PATH and json_ok then
    local f = io.open(TELEMETRY_S3_PATH, "a")
    if f then
      f:write(cjson.encode { ts = os.time(), events = data }, "\n")
      f:close()
    end
  end
  return codec.ok(data)
end

function handlers.SetFeatureFlags(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Flags" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  state.feature_flags = state.feature_flags or {}
  state.feature_flags[msg["Site-Id"]] = msg.Flags
  return codec.ok { siteId = msg["Site-Id"], flags = msg.Flags }
end

function handlers.SetEdgeCachePolicy(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Path", "Cache-Control" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  state.edge_cache = state.edge_cache or {}
  state.edge_cache[msg["Site-Id"]] = state.edge_cache[msg["Site-Id"]] or {}
  state.edge_cache[msg["Site-Id"]][msg.Path] = {
    cache_control = msg["Cache-Control"],
    etag = msg.ETag,
    ttl = msg.TTL,
  }
  return codec.ok { siteId = msg["Site-Id"], path = msg.Path }
end

local function verify_shared_secret(msg, secret)
  if not secret or secret == "" then
    return true
  end
  local sig = msg.Signature or msg.signature or msg.auth or msg["X-Signature"]
  local ts = msg.Timestamp or msg["X-Timestamp"]
  local raw = msg.RawBody
  if raw and sig then
    local expected = hmac_sha256_hex((ts or "") .. "." .. raw, secret)
    if not expected or expected:lower() ~= tostring(sig):lower() then
      return false
    end
    if ts then
      local tnum = tonumber(ts) or 0
      if math.abs(os.time() - tnum) > CARRIER_WEBHOOK_TOLERANCE then
        return false
      end
      local ok_seen = mark_webhook_seen("carrier:" .. (sig or "") .. ":" .. (ts or ""), tnum)
      if not ok_seen then
        return false
      end
    end
    return true
  end
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
  if
    msg.Timestamp
    and math.abs(os.time() - (tonumber(msg.Timestamp) or 0)) > CARRIER_WEBHOOK_TOLERANCE
  then
    return codec.error("FORBIDDEN", "Stale webhook")
  end
  if msg.EventId then
    local ok_seen, err = mark_event_seen("carrier", msg.EventId, tonumber(msg.Timestamp))
    if not ok_seen then
      return codec.error("CONFLICT", "Duplicate webhook", { reason = err })
    end
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
    local order = state.orders[sh.orderId]
    if sh.status == "delivered" then
      order.status = "delivered"
      notify_customer(
        "shipment.delivered",
        { orderId = sh.orderId, shipmentId = msg["Shipment-Id"] }
      )
    elseif sh.status == "out_for_delivery" then
      order.status = order.status or "out_for_delivery"
      notify_customer("shipment.out_for_delivery", {
        orderId = sh.orderId,
        shipmentId = msg["Shipment-Id"],
        tracking = sh.tracking,
      })
    elseif sh.status == "exception" or sh.status == "delayed" or sh.status == "lost" then
      order.status = "shipment_issue"
      notify_customer("shipment.issue", {
        orderId = sh.orderId,
        shipmentId = msg["Shipment-Id"],
        status = sh.status,
        tracking = sh.tracking,
      })
      state.notification_failures[sh.orderId] = state.notification_failures[sh.orderId] or {}
      table.insert(state.notification_failures[sh.orderId], {
        type = "shipment_issue",
        target = CUSTOMER_WEBHOOK,
        payload = sh,
        attempts = 0,
        ts = os.time(),
        error = "carrier_" .. sh.status,
      })
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
  local keys = msg.SurrogateKeys or {}
  purge_cache { paths = { path }, keys = keys }
  local result = { purged = path, surrogateKeys = keys }
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
  if sh.orderId and sh.status == "in_transit" then
    notify_customer("shipment.in_transit", {
      orderId = sh.orderId,
      shipmentId = msg["Shipment-Id"],
      tracking = sh.tracking,
      eta = sh.eta,
    })
  end
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
    "Dimensions",
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
  local dims
  if msg.Dimensions then
    local d = msg.Dimensions
    local function dim(name)
      local v = d[name] or d[name:lower()] or d[name:upper()]
      return v and tonumber(v) or nil
    end
    local L, W, H = dim "Length", dim "Width", dim "Height"
    if not (L and W and H) then
      return codec.error("INVALID_INPUT", "Dimensions require Length/Width/Height numbers")
    end
    dims = { length = L, width = W, height = H }
  end
  local weight = 0
  if order.items then
    for _, it in ipairs(order.items) do
      local pkey = ids.product_key(msg["Site-Id"], it.sku or it.Sku or "")
      local payload = state.products[pkey] and state.products[pkey].payload or {}
      weight = weight + (payload.weight or payload.Weight or 0) * (it.qty or it.Qty or 1)
    end
  end
  local label = build_label(carrier, service, weight, dims)
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
  purge_cache {
    paths = {
      "/p/" .. msg.Sku,
      "/api/catalog/" .. msg.Sku,
    },
    keys = { "product:" .. msg.Sku },
  }
  -- image pipeline
  if msg.Payload.assets and msg.Payload.assets[1] and IMAGE_RESIZE_CMD then
    local src = msg.Payload.assets[1]
    resize_and_store(msg["Site-Id"], msg.Sku, src)
  end
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
  purge_cache {
    paths = {
      "/p/" .. msg.Sku,
      "/api/catalog/" .. msg.Sku,
    },
    keys = { "product:" .. msg.Sku },
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
  purge_cache {
    paths = {
      "/c/" .. msg["Category-Id"],
      "/api/catalog/category/" .. msg["Category-Id"],
    },
    keys = { "category:" .. msg["Category-Id"] },
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
  purge_cache {
    paths = {
      "/c/" .. msg["Category-Id"],
      "/api/catalog/category/" .. msg["Category-Id"],
    },
    keys = { "category:" .. msg["Category-Id"] },
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
  local window = {
    region = msg.Region,
    valid_from = msg.ValidFrom,
    valid_to = msg.ValidTo,
    prices = msg.Prices,
  }
  add_price_window(msg["Site-Id"], currency, window)
  audit.record(
    "catalog",
    "SetPriceList",
    msg,
    nil,
    { siteId = msg["Site-Id"], currency = currency, region = msg.Region }
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

function handlers.UpsertPriceList(msg)
  return handlers.SetPriceList(msg)
end

function handlers.ApplyCoupon(msg)
  local ok, missing = validation.require_fields(msg, { "Code" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Code",
    "Type",
    "Value",
    "Applies-To",
    "FreeShipping",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local typ = msg.Type or "percent"
  local value = tonumber(msg.Value or 0) or 0
  state.coupons[msg.Code] = {
    type = typ,
    value = value,
    applies_to = msg["Applies-To"],
    free_shipping = msg.FreeShipping == true,
  }
  audit.record("catalog", "ApplyCoupon", msg, nil, { code = msg.Code, type = typ })
  return codec.ok { code = msg.Code, type = typ, freeShipping = msg.FreeShipping }
end

local function price_match_region(window_region, target_region)
  if not window_region or window_region == "" then
    return true
  end
  if not target_region or target_region == "" then
    return false
  end
  return window_region:upper() == target_region:upper()
end

local EU_COUNTRIES = {
  AT = true,
  BE = true,
  BG = true,
  CY = true,
  CZ = true,
  DE = true,
  DK = true,
  EE = true,
  ES = true,
  FI = true,
  FR = true,
  GR = true,
  HR = true,
  HU = true,
  IE = true,
  IT = true,
  LT = true,
  LU = true,
  LV = true,
  MT = true,
  NL = true,
  PL = true,
  PT = true,
  RO = true,
  SE = true,
  SI = true,
  SK = true,
}

local function is_eu(country)
  if not country then
    return false
  end
  return EU_COUNTRIES[country:upper()] == true
end

local function is_vat_id_valid(id)
  if not id or id == "" then
    return false
  end
  -- basic check: country prefix + 8-12 alnum
  return id:match "^[A-Z]{2}[A-Z0-9]{8,12}$" ~= nil
end

local function select_price(site_id, sku, currency, region)
  local now = os.time()
  local windows = state.price_windows[site_id]
  if windows and windows[currency] then
    local best = nil
    for _, w in ipairs(windows[currency]) do
      if price_match_region(w.region, region) then
        local vf = w.valid_from
          and validation.parse_iso8601
          and validation.parse_iso8601(w.valid_from)
        local vt = w.valid_to and validation.parse_iso8601 and validation.parse_iso8601(w.valid_to)
        local ok_time = true
        if vf and now < vf then
          ok_time = false
        end
        if vt and now > vt then
          ok_time = false
        end
        if ok_time and w.prices and w.prices[sku] then
          best = w.prices[sku]
          break
        end
      end
    end
    if best then
      return best, currency
    end
  end
  local pl = state.price_lists[site_id]
  if pl and pl[currency] and pl[currency][sku] then
    return pl[currency][sku], currency
  end
end

local function apply_pricing(site_id, sku, currency, promo_code, region)
  local product_key = ids.product_key(site_id, sku)
  local product = state.products[product_key]
  if not product then
    return nil, "NOT_FOUND"
  end
  local price = product.payload.price
  local base_currency = product.payload.currency or currency
  local override, o_cur = select_price(site_id, sku, currency, region)
  if override then
    price = override
    base_currency = o_cur
  end
  local free_shipping = false
  local bogo = false
  if promo_code then
    local coupon = state.coupons[promo_code]
    if coupon then
      local applies = not coupon.applies_to or #coupon.applies_to == 0
      if coupon.applies_to then
        for _, s in ipairs(coupon.applies_to) do
          if s == sku then
            applies = true
            break
          end
        end
      end
      if applies then
        if coupon.type == "percent" then
          price = price * (1 - coupon.value / 100)
        elseif coupon.type == "amount" then
          price = math.max(0, price - coupon.value)
        elseif coupon.type == "bogo" then
          bogo = true
        end
        if coupon.free_shipping then
          free_shipping = true
        end
      end
    end
    local promo = state.promos[promo_code]
    if promo then
      local applies = #promo.skus == 0
      if not applies then
        for _, s in ipairs(promo.skus) do
          if s == sku then
            applies = true
            break
          end
        end
      end
      if applies then
        if promo.type == "percent" then
          price = price * (1 - promo.value / 100)
        elseif promo.type == "amount" then
          price = math.max(0, price - promo.value)
        end
      end
    end
  end
  return { price = price, currency = base_currency, free_shipping = free_shipping, bogo = bogo },
    nil
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
    "Region",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local currency = msg.Currency or "USD"
  local quote = apply_pricing(msg["Site-Id"], msg.Sku, currency, msg.Promo, msg.Region)
  if not quote then
    return codec.error("NOT_FOUND", "Product not found", { sku = msg.Sku })
  end
  return codec.ok {
    sku = msg.Sku,
    price = quote.price,
    currency = quote.currency,
    promo = msg.Promo,
    region = msg.Region,
  }
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
    if r.taxInclusive ~= nil and type(r.taxInclusive) ~= "boolean" then
      return codec.error("INVALID_INPUT", "taxInclusive must be boolean", { rule = r })
    end
    if r.shippingTaxable ~= nil and type(r.shippingTaxable) ~= "boolean" then
      return codec.error("INVALID_INPUT", "shippingTaxable must be boolean", { rule = r })
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
    if r.zipPrefix and type(r.zipPrefix) ~= "string" then
      return codec.error("INVALID_INPUT", "zipPrefix must be string", { rule = r })
    end
    if (r.zipFrom or r.zipTo) and (type(r.zipFrom) ~= "number" or type(r.zipTo) ~= "number") then
      return codec.error("INVALID_INPUT", "zipFrom/zipTo must be number", { rule = r })
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

local function pick_tax_rule(site_id, address, tax_class)
  local rules = state.tax_rules[site_id] or state.tax_rates[site_id] or {}
  local best_rule = nil
  local best_score = -1
  for _, r in ipairs(rules) do
    local match = (not r.country or r.country == address.Country)
      and (not r.region or r.region == address.Region)
      and (not r.taxClass or r.taxClass == tax_class)
    if match and r.zipPrefix and address.PostalCode then
      match = address.PostalCode:sub(1, #r.zipPrefix) == r.zipPrefix
    end
    if match and r.zipFrom and r.zipTo and address.PostalCode then
      local z = tonumber(address.PostalCode:match "%d+")
      local zf, zt = tonumber(r.zipFrom), tonumber(r.zipTo)
      if z and zf and zt then
        match = z >= zf and z <= zt
      end
    end
    if match then
      local priority = tonumber(r.priority or r.Priority) or 0
      local specificity = (r.country and 1 or 0) + (r.region and 1 or 0) + (r.taxClass and 1 or 0)
      local score = priority * 10 + specificity
      if score > best_score then
        best_score = score
        best_rule = r
      end
    end
  end
  return best_rule, best_rule and (tonumber(best_rule.rate or best_rule.Rate) or 0) or 0
end

local function pick_tax_rate(site_id, address, tax_class)
  local _, rate = pick_tax_rule(site_id, address, tax_class)
  return rate
end

local function pick_shipping(site_id, address, total, weight, dims, opts)
  opts = opts or {}
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
  local chosen = best or { rate = 0, carrier = "standard", service = "ground" }
  if opts.free_shipping then
    chosen.rate = 0
    chosen.service = "free"
  end
  return chosen
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
  local free_shipping = false
  for _, it in ipairs(items) do
    local qty = tonumber(it.Qty or it.qty) or 0
    if qty <= 0 then
      return nil, "INVALID_QTY"
    end
    local sku = it.Sku or it.sku
    local quote, err = apply_pricing(site_id, sku, currency, promo, it.Region)
    if not quote then
      return nil, err or "NOT_FOUND"
    end
    if quote.free_shipping then
      free_shipping = true
    end
    local free_units = quote.bogo and math.floor(qty / 2) or 0
    local charge_qty = qty - free_units
    local line_total = quote.price * charge_qty
    subtotal = subtotal + line_total
    local pkey = ids.product_key(site_id, sku)
    local payload = state.products[pkey] and state.products[pkey].payload or {}
    weight = weight + (payload.weight or payload.Weight or 0) * qty
    table.insert(lines, {
      sku = sku,
      qty = qty,
      free_units = free_units,
      unit_price = quote.price,
      currency = quote.currency,
      line_total = line_total,
      taxClass = payload.taxClass or payload.TaxClass,
      taxInclusive = payload.taxInclusive or payload.TaxInclusive,
    })
  end
  return { subtotal = subtotal, weight = weight, lines = lines, free_shipping = free_shipping }, nil
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
  local tax, line_taxes, subtotal_ex, ship_tax, reverse_charge =
    calculate_tax_breakdown(msg["Site-Id"], address, cart, ship.rate)
  local total = subtotal_ex + ship.rate + ship_tax + tax
  return codec.ok {
    siteId = msg["Site-Id"],
    currency = currency,
    items = cart.lines,
    subtotal = cart.subtotal,
    subtotalExcl = subtotal_ex,
    weight = cart.weight,
    shipping = ship,
    tax = tax,
    shippingTax = ship_tax,
    lineTaxes = line_taxes,
    total = total,
    promo = msg.Promo,
    reverseCharge = reverse_charge,
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
  local shipping = pick_shipping(msg["Site-Id"], address, cart.subtotal, cart.weight)
  local tax, line_taxes, subtotal_ex, ship_tax, reverse_charge =
    calculate_tax_breakdown(msg["Site-Id"], address, cart, shipping.rate)
  return codec.ok {
    siteId = msg["Site-Id"],
    subtotal = cart.subtotal,
    subtotalExcl = subtotal_ex,
    tax = tax,
    shippingTax = ship_tax,
    lineTaxes = line_taxes,
    shipping = shipping,
    total = subtotal_ex + shipping.rate + ship_tax + tax,
    currency = currency,
    reverseCharge = reverse_charge,
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

local function purge_cache(opts)
  opts = opts or {}
  local paths = opts.paths or opts
  local keys = opts.keys or {}
  if CDN_PURGE_CMD and CDN_PURGE_CMD ~= "" then
    for _, p in ipairs(paths or {}) do
      local cmd = string.format(CDN_PURGE_CMD, p)
      os.execute(cmd .. " >/dev/null 2>&1")
    end
  end
  if CDN_SURROGATE_CMD and CDN_SURROGATE_CMD ~= "" then
    for _, k in ipairs(keys) do
      local cmd = string.format(CDN_SURROGATE_CMD, k)
      os.execute(cmd .. " >/dev/null 2>&1")
    end
  end
end

local function parse_sizes(str)
  local sizes = {}
  for token in tostring(str or ""):gmatch "[^,]+" do
    local w, h = token:match "(%d+)x(%d+)"
    if w and h then
      table.insert(sizes, { tonumber(w), tonumber(h) })
    end
  end
  return sizes
end

local function parse_formats(str)
  local fmts = {}
  for token in tostring(str or ""):gmatch "[^,]+" do
    table.insert(fmts, token)
  end
  return fmts
end

local function parse_set(str)
  local out = {}
  for token in tostring(str or ""):gmatch "[^,; ]+" do
    out[token:upper()] = true
  end
  return out
end

local function add_price_window(site_id, currency, window)
  if not window or not currency or not site_id then
    return
  end
  state.price_windows[site_id] = state.price_windows[site_id] or {}
  state.price_windows[site_id][currency] = state.price_windows[site_id][currency] or {}
  table.insert(state.price_windows[site_id][currency], window)
end

local function ensure_dir(path)
  if not path or path == "" then
    return false
  end
  os.execute(string.format("mkdir -p '%s'", path))
  return true
end

local function fetch_file(src)
  if not src or src == "" then
    return nil, "missing_source"
  end
  if src:match "^https?://" then
    local tmp = os.tmpname()
    local cmd = string.format(
      "curl -sS --max-time %d --connect-timeout %d '%s' -o %s",
      HTTP_TIMEOUT,
      HTTP_CONNECT_TIMEOUT,
      src,
      tmp
    )
    local rc = os.execute(cmd)
    if rc == true or rc == 0 then
      return tmp, nil, true
    end
    os.remove(tmp)
    return nil, "download_failed"
  end
  -- local path
  local f = io.open(src, "rb")
  if not f then
    return nil, "source_not_found"
  end
  f:close()
  return src, nil, false
end

local function upload_image(path, relkey)
  if not IMAGE_S3_BUCKET or IMAGE_S3_BUCKET == "" then
    return nil
  end
  local prefix = IMAGE_S3_PREFIX
  if prefix ~= "" and not prefix:match "/$" then
    prefix = prefix .. "/"
  end
  local key = prefix .. relkey
  local cmd = string.format(
    "aws s3 cp %s s3://%s/%s --no-progress --cli-read-timeout %d --cli-connect-timeout %d",
    path,
    IMAGE_S3_BUCKET,
    key,
    S3_TIMEOUT,
    S3_TIMEOUT
  )
  local rc = os.execute(cmd)
  if rc == true or rc == 0 then
    if IMAGE_PUBLIC_BASE and IMAGE_PUBLIC_BASE ~= "" then
      local base = IMAGE_PUBLIC_BASE
      if base:sub(-1) == "/" then
        base = base:sub(1, -2)
      end
      return base .. "/" .. key
    end
    return "https://" .. IMAGE_S3_BUCKET .. ".s3.amazonaws.com/" .. key
  end
  return nil, "upload_failed"
end

local function resize_and_store(site_id, sku, src_path)
  if not IMAGE_RESIZE_CMD or not IMAGE_STORE_DIR or IMAGE_STORE_DIR == "" then
    return nil, "IMAGE_PIPELINE_DISABLED"
  end
  local local_path, ferr, tmp = fetch_file(src_path)
  if not local_path then
    return nil, ferr or "SOURCE_NOT_FOUND"
  end
  ensure_dir(IMAGE_STORE_DIR)
  local sizes = parse_sizes(IMAGE_SIZES)
  local fmts = parse_formats(IMAGE_FORMATS)
  local dests = {}
  for _, sz in ipairs(sizes) do
    local w, h = sz[1], sz[2]
    for _, fmt in ipairs(fmts) do
      local out_dir = string.format("%s/%s/%s", IMAGE_STORE_DIR, fmt, sku)
      ensure_dir(out_dir)
      local outfile = string.format("%s/%dx%d.%s", out_dir, w, h, fmt)
      local cmd = string.format(IMAGE_RESIZE_CMD, local_path, w, h, outfile)
      os.execute(cmd .. " >/dev/null 2>&1")
      local rel = outfile:gsub("^" .. IMAGE_STORE_DIR .. "/?", "")
      local url, up_err = upload_image(outfile, rel)
      table.insert(dests, {
        url = url or outfile,
        width = w,
        height = h,
        format = fmt,
        uploaded = up_err == nil,
      })
    end
  end
  state.assets[site_id] = state.assets[site_id] or {}
  state.assets[site_id][sku] = state.assets[site_id][sku] or {}
  state.assets[site_id][sku].variants = dests
  state.assets[site_id][sku].original = src_path
  -- purge cached variants
  local purge_list = { src_path }
  for _, d in ipairs(dests) do
    table.insert(purge_list, d.url)
  end
  purge_cache { paths = purge_list, keys = { "product:" .. sku, "images:" .. sku } }
  if tmp then
    os.remove(local_path)
  end
  return dests
end

local function forget_subject(site_id, subject)
  if not subject or subject == "" then
    return 0
  end
  local scrubbed = 0
  -- remove from recent list
  for sub in pairs(state.recent) do
    if sub == subject then
      state.recent[sub] = nil
      scrubbed = scrubbed + 1
    end
  end
  -- scrub checkouts
  for _, chk in pairs(state.checkouts) do
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
  -- scrub shipments linked to subject's orders
  for _, sh in pairs(state.shipments) do
    if sh.orderId and state.orders[sh.orderId] and state.orders[sh.orderId].email == subject then
      sh.address = nil
      scrubbed = scrubbed + 1
    end
  end
  -- scrub returns linked to subject's orders
  for _, ret in pairs(state.returns) do
    if ret.orderId and state.orders[ret.orderId] and state.orders[ret.orderId].email == subject then
      ret.address = nil
      ret.reason = nil
      scrubbed = scrubbed + 1
    end
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

local function send_with_retry(site_id, target, body, kind)
  if not target or target == "" or not json_ok then
    return false, "target_missing"
  end
  local ok, json_body = pcall(cjson.encode, body)
  if not ok then
    return false, "encode_failed"
  end
  local safe = json_body:gsub("'", "'\\''")
  local attempts = 0
  local success = false
  local err = nil
  while attempts <= NOTIFY_RETRIES do
    attempts = attempts + 1
    local cmd = string.format(
      "curl -sS -m %d -H 'Content-Type: application/json' -d '%s' %s >/dev/null 2>&1",
      HTTP_TIMEOUT,
      safe,
      target
    )
    local rc = os.execute(cmd)
    success = (rc == true or rc == 0)
    if success then
      break
    end
    err = "curl_failed"
    if attempts <= NOTIFY_RETRIES then
      -- backoff
      os.execute(string.format("sleep %.3f", NOTIFY_BACKOFF_MS / 1000))
    end
  end
  if not success then
    state.notification_failures[site_id or "global"] = state.notification_failures[site_id or "global"]
      or {}
    table.insert(state.notification_failures[site_id or "global"], {
      type = kind,
      target = target,
      payload = body,
      attempts = attempts,
      ts = os.time(),
      error = err,
    })
  end
  return success, err
end

local function notify_customer(event, payload)
  return send_with_retry(
    payload.siteId,
    CUSTOMER_WEBHOOK,
    { type = event, payload = payload },
    event
  )
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
  for site, fails in pairs(state.notification_failures) do
    local filtered = {}
    for _, f in ipairs(fails) do
      if (f.ts or 0) >= cutoff then
        table.insert(filtered, f)
      end
    end
    state.notification_failures[site] = filtered
  end
  -- delete old shipment event logs
  for ship, events in pairs(state.shipment_events) do
    local filtered = {}
    for _, e in ipairs(events) do
      if (e.ts or 0) >= cutoff then
        table.insert(filtered, e)
      end
    end
    state.shipment_events[ship] = filtered
  end
  for key, ts in pairs(state.webhook_seen) do
    if (ts or 0) < os.time() - WEBHOOK_REPLAY_WINDOW then
      state.webhook_seen[key] = nil
    end
  end
  for provider, events in pairs(state.provider_events) do
    local filtered = {}
    for id, ts in pairs(events) do
      if (ts or 0) >= cutoff then
        filtered[id] = ts
      end
    end
    state.provider_events[provider] = filtered
  end
  for idem_key, res in pairs(state.stripe_idempotency) do
    if
      res
      and res.nextAction
      and res.nextAction.expiresAt
      and res.nextAction.expiresAt < os.time()
    then
      state.stripe_idempotency[idem_key] = nil
    end
  end
  for _, pay in pairs(state.payments) do
    if
      pay.status == "requires_action"
      and pay.challengeExpiresAt
      and pay.challengeExpiresAt < os.time()
    then
      pay.status = "failed"
    end
  end
end

local function parse_csv_line(line)
  local res = {}
  local i = 1
  local in_quote = false
  local field = ""
  while i <= #line do
    local c = line:sub(i, i)
    if c == '"' then
      if in_quote and line:sub(i + 1, i + 1) == '"' then
        field = field .. '"'
        i = i + 1
      else
        in_quote = not in_quote
      end
    elseif c == "," and not in_quote then
      table.insert(res, field)
      field = ""
    else
      field = field .. c
    end
    i = i + 1
  end
  table.insert(res, field)
  return res
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
  send_with_retry(site_id, STOCK_ALERT_WEBHOOK, body, "low_stock")
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
  local shipping = pick_shipping(
    msg["Site-Id"],
    address,
    cart.subtotal,
    cart.weight,
    dims,
    { free_shipping = cart.free_shipping }
  )
  local tax, _, subtotal_ex, ship_tax =
    calculate_tax_breakdown(msg["Site-Id"], address, cart, shipping.rate)
  local total = subtotal_ex + shipping.rate + ship_tax + tax
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
      subtotalExcl = subtotal_ex,
      weight = cart.weight,
      tax = tax,
      shippingTax = ship_tax,
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
    taxRate = subtotal_ex > 0 and (tax * 100 / subtotal_ex) or 0,
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

function handlers.Complete3DSChallenge(msg)
  local ok, missing = validation.require_fields(msg, { "Payment-Id", "Token" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Payment-Id",
    "Token",
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
  if pay.status ~= "requires_action" then
    return codec.error("INVALID_INPUT", "Payment not awaiting 3DS", { status = pay.status })
  end
  local expected = pay.challengeToken or ("3ds-" .. msg["Payment-Id"])
  if msg.Token ~= expected then
    return codec.error("FORBIDDEN", "Token mismatch")
  end
  if pay.challengeExpiresAt and os.time() > pay.challengeExpiresAt then
    return codec.error("FORBIDDEN", "3DS token expired")
  end
  pay.status = "captured"
  pay.capturedAt = os.time()
  audit.record("catalog", "Complete3DSChallenge", msg, nil, { paymentId = pay.paymentId })
  return codec.ok { paymentId = pay.paymentId, status = pay.status }
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
    "IdempotencyKey",
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
  if not check_rate_limit("pi:" .. (msg.Subject or msg["Site-Id"])) then
    return codec.error("RATE_LIMITED", "Too many payment attempts")
  end
  if msg["Order-Id"] and not state.orders[msg["Order-Id"]] then
    return codec.error("NOT_FOUND", "Order not found", { orderId = msg["Order-Id"] })
  end
  local provider = msg.Provider or "internal"
  local idem_key = msg.IdempotencyKey
  if provider == "stripe" and idem_key then
    local cached = state.stripe_idempotency[idem_key]
    if cached then
      return codec.ok(cached)
    end
  end
  local token = msg.Token
  if msg.Subject and not token and state.payment_tokens[msg.Subject] then
    local last = state.payment_tokens[msg.Subject][#state.payment_tokens[msg.Subject]]
    token = last and last.token
  end
  local record = create_payment_intent_internal {
    siteId = msg["Site-Id"],
    checkoutId = msg["Checkout-Id"],
    orderId = msg["Order-Id"],
    amount = msg.Amount,
    currency = msg.Currency,
    method = method,
    require3ds = msg.Require3DS,
    provider = provider,
    token = token,
    subject = msg.Subject,
  }
  if record.requiresAction then
    record.challengeToken = "3ds-" .. record.paymentId
    record.challengeExpiresAt = os.time() + CHALLENGE_TTL
    state.payments[record.paymentId].challengeToken = record.challengeToken
    state.payments[record.paymentId].challengeExpiresAt = record.challengeExpiresAt
  end
  audit.record(
    "catalog",
    "CreatePaymentIntent",
    msg,
    nil,
    { paymentId = record.paymentId, status = record.status }
  )
  metrics.inc "catalog.CreatePaymentIntent.count"
  metrics.tick()
  local resp = {
    paymentId = record.paymentId,
    status = record.status,
    provider = record.provider,
    clientSecret = record.clientSecret,
    nextAction = record.requiresAction and {
      type = "3ds_redirect",
      token = record.challengeToken,
      url = THREE_DS_URL .. "?pid=" .. record.paymentId .. "&token=" .. record.challengeToken,
      expiresAt = record.challengeExpiresAt,
    } or nil,
  }
  if provider == "stripe" and idem_key then
    state.stripe_idempotency[idem_key] = resp
  end
  return codec.ok(resp)
end

function handlers.TokenizePaymentMethod(msg)
  local ok, missing = validation.require_fields(msg, { "Provider", "Payload" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Subject",
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
  if
    provider ~= "stripe"
    and provider ~= "paypal"
    and provider ~= "adyen"
    and provider ~= "apple_pay"
    and provider ~= "google_pay"
  then
    return codec.error("INVALID_INPUT", "Unsupported provider")
  end
  local token = string.format("%s_tok_%s", provider, gen_id "pm")
  if msg.Subject then
    state.payment_tokens[msg.Subject] = state.payment_tokens[msg.Subject] or {}
    table.insert(state.payment_tokens[msg.Subject], {
      provider = provider,
      token = token,
      label = msg.Payload.label,
      last4 = msg.Payload.last4 or msg.Payload.Last4,
      brand = msg.Payload.brand,
      exp = msg.Payload.exp,
      default = msg.Payload.default == true,
    })
  end
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
  if pay.provider ~= "internal" then
    local ok_cap, resp = psp_call(
      pay.provider,
      "capture",
      { id = pay.providerPaymentId or pay.paymentId, amount = pay.amount }
    )
    if not ok_cap or (resp and resp.status ~= "captured") then
      return codec.error(
        "PROVIDER_ERROR",
        "Capture failed",
        { provider = pay.provider, reason = resp }
      )
    end
    pay.providerCaptureId = resp.providerCaptureId
  end
  pay.status = "captured"
  pay.capturedAt = os.time()
  table.insert(state.payment_attempts[pay.paymentId] or {}, {
    ts = os.time(),
    event = "captured",
    status = pay.status,
    provider = pay.provider,
  })
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
  table.insert(state.payment_attempts[pay.paymentId] or {}, {
    ts = os.time(),
    event = "refunded",
    status = pay.status,
    amount = amount,
  })
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

local function parse_header(headers, key)
  if not headers then
    return nil
  end
  return headers[key] or headers[key:lower()] or headers[key:upper()]
end

local function mark_webhook_seen(cache_key, ts)
  ts = ts or os.time()
  local existing = state.webhook_seen[cache_key]
  if existing and (ts - existing) <= WEBHOOK_REPLAY_WINDOW then
    return false, "replay"
  end
  state.webhook_seen[cache_key] = ts
  return true
end

local function verify_provider_webhook(provider, msg, raw_body)
  raw_body = raw_body or ""
  if provider == "stripe" then
    if not STRIPE_WEBHOOK_SECRET or STRIPE_WEBHOOK_SECRET == "" then
      return false, "stripe_secret_missing"
    end
    if STRIPE_WEBHOOK_ID and STRIPE_WEBHOOK_ID ~= "" then
      local hook = msg["Webhook-Id"] or parse_header(msg.Headers, "Stripe-Webhook-Id")
      if hook ~= STRIPE_WEBHOOK_ID then
        return false, "stripe_webhook_id_mismatch"
      end
    end
    local sig_header = msg.Signature or parse_header(msg.Headers, "Stripe-Signature")
    if not sig_header then
      return false, "missing_signature"
    end
    local t = sig_header:match "t=(%d+)"
    local v1 = sig_header:match "v1=([0-9a-fA-F]+)"
    if not t or not v1 then
      return false, "signature_format"
    end
    local expected = hmac_sha256_hex(t .. "." .. raw_body, STRIPE_WEBHOOK_SECRET)
    if not expected or expected:lower() ~= v1:lower() then
      return false, "signature_mismatch"
    end
    local ts_num = tonumber(t) or 0
    if math.abs(os.time() - ts_num) > WEBHOOK_REPLAY_WINDOW then
      return false, "timestamp_out_of_window"
    end
    local ok_seen, err_seen = mark_webhook_seen("stripe:" .. v1, ts_num)
    if not ok_seen then
      return false, err_seen
    end
    return true
  elseif provider == "paypal" then
    if not PAYPAL_WEBHOOK_SECRET or PAYPAL_WEBHOOK_SECRET == "" then
      return false, "paypal_secret_missing"
    end
    if PAYPAL_WEBHOOK_ID and PAYPAL_WEBHOOK_ID ~= "" then
      local hook = msg["Webhook-Id"] or parse_header(msg.Headers, "Webhook-Id")
      if hook ~= PAYPAL_WEBHOOK_ID then
        return false, "paypal_webhook_id_mismatch"
      end
    end
    local sig = msg.Signature or parse_header(msg.Headers, "PayPal-Transmission-Sig")
    if not sig then
      return false, "missing_signature"
    end
    local expected = hmac_sha256_hex(raw_body, PAYPAL_WEBHOOK_SECRET)
    if not expected or expected:lower() ~= tostring(sig):lower() then
      return false, "signature_mismatch"
    end
    local transmission_id = msg["Transmission-Id"]
      or parse_header(msg.Headers, "PayPal-Transmission-Id")
    local ts = tonumber(msg.Timestamp or parse_header(msg.Headers, "PayPal-Transmission-Time"))
      or os.time()
    local replay_key = transmission_id or sig
    local ok_seen, err_seen = mark_webhook_seen("paypal:" .. replay_key, ts)
    if not ok_seen then
      return false, err_seen
    end
    return true
  elseif provider == "adyen" then
    if not ADYEN_HMAC_KEY or ADYEN_HMAC_KEY == "" then
      return false, "adyen_secret_missing"
    end
    local sig = msg.Signature or parse_header(msg.Headers, "Hmac-Signature")
    if not sig then
      return false, "missing_signature"
    end
    local expected = hmac_sha256_hex(raw_body, ADYEN_HMAC_KEY)
    if not expected or expected:lower() ~= tostring(sig):lower() then
      return false, "signature_mismatch"
    end
    local ok_seen, err_seen = mark_webhook_seen("adyen:" .. sig, os.time())
    if not ok_seen then
      return false, err_seen
    end
    return true
  end
  return false, "provider_not_supported"
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
    "RawBody",
    "Headers",
    "Timestamp",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local raw_body = msg.RawBody
  if not raw_body and json_ok then
    local ok_enc, body = pcall(cjson.encode, msg.Event)
    raw_body = ok_enc and body or ""
  end
  local sig_ok, sig_err = verify_provider_webhook(msg.Provider, msg, raw_body or "")
  if not sig_ok then
    metrics.inc "catalog.HandlePaymentProviderWebhook.verify_failed"
    return codec.error("FORBIDDEN", "Signature verification failed", { reason = sig_err })
  end
  if msg.Provider:lower() == "paypal" then
    -- PayPal deterministic signature with webhook id
    local tid = msg["Transmission-Id"] or parse_header(msg.Headers, "PayPal-Transmission-Id") or ""
    local tts = msg["Transmission-Time"]
      or parse_header(msg.Headers, "PayPal-Transmission-Time")
      or ""
    local wid = msg["Webhook-Id"]
      or parse_header(msg.Headers, "Webhook-Id")
      or PAYPAL_WEBHOOK_ID
      or ""
    local signed = table.concat({ tid, tts, wid, raw_body or "" }, "|")
    local expected = hmac_sha256_hex(signed, PAYPAL_WEBHOOK_SECRET)
    local provided = msg.Signature or parse_header(msg.Headers, "PayPal-Transmission-Sig")
    if not expected or not provided or expected:lower() ~= tostring(provided):lower() then
      return codec.error(
        "FORBIDDEN",
        "Signature verification failed",
        { reason = "paypal_sig_mismatch" }
      )
    end
    local cert_url = parse_header(msg.Headers, "PayPal-Cert-Url")
    if cert_url then
      local host = hostname_from_url(cert_url)
      if not host or not host:match(PAYPAL_CERT_HOST:gsub("%.", "%%.") .. "$") then
        return codec.error("FORBIDDEN", "Cert host not allowed")
      end
      local cert_pem, cerr = fetch_paypal_cert(cert_url)
      if not cert_pem then
        return codec.error("FORBIDDEN", "Cert fetch failed", { reason = cerr })
      end
      local ok_cert, cert_err = verify_paypal_cert_signature(signed, provided, cert_pem)
      if not ok_cert then
        return codec.error("FORBIDDEN", "Cert signature invalid", { reason = cert_err })
      end
    end
  end
  local ev = msg.Event
  if type(ev) ~= "table" then
    return codec.error("INVALID_INPUT", "Event must be object")
  end
  local provider = msg.Provider:lower()
  -- normalize PayPal resource wrapper
  if provider == "paypal" and ev.resource and type(ev.resource) == "table" then
    ev = ev.resource
  end
  -- basic freshness check if provider sends creation time
  if ev.created and math.abs(os.time() - (tonumber(ev.created) or 0)) > WEBHOOK_REPLAY_WINDOW then
    return codec.error("FORBIDDEN", "Event too old", { created = ev.created })
  end
  if provider == "adyen" and ev.additionalData and ev.additionalData["hmacSignature"] then
    -- Adyen already verified at webhook layer; prefer pspReference as id
    ev.pspReference = ev.pspReference or ev.additionalData["pspReference"]
    if ev.success == false or ev.success == "false" then
      return codec.error("FORBIDDEN", "Adyen event not successful")
    end
  end
  -- Ensure paymentId present early for idempotency cache write
  local allowed_types = {
    stripe = {
      ["payment_intent.succeeded"] = "payment_succeeded",
      ["payment_intent.payment_failed"] = "payment_failed",
      ["charge.refunded"] = "refund_succeeded",
    },
    paypal = {
      ["CHECKOUT.ORDER.APPROVED"] = "payment_succeeded",
      ["PAYMENT.CAPTURE.COMPLETED"] = "payment_succeeded",
      ["PAYMENT.CAPTURE.DENIED"] = "payment_failed",
      ["PAYMENT.CAPTURE.REFUNDED"] = "refund_succeeded",
    },
    adyen = {
      ["AUTHORISATION"] = "payment_succeeded",
      ["CANCELLATION"] = "payment_failed",
      ["REFUND"] = "refund_succeeded",
    },
  }
  if allowed_types[provider] and ev.type then
    ev.type = allowed_types[provider][ev.type] or ev.type
  end
  if allowed_types[provider] and not allowed_types[provider][ev.type or ""] then
    return codec.error("INVALID_INPUT", "Event type not allowed", { type = ev.type })
  end
  if provider == "stripe" then
    local cached = check_stripe_idempotency(msg)
    if cached then
      return codec.ok(cached)
    end
  end
  local pid = ev.paymentId or ev.payment_id
  if not pid then
    return codec.error("INVALID_INPUT", "paymentId missing in event")
  end
  if
    not ev.id
    and not ev.eventId
    and not ev.event_id
    and not ev.pspReference
    and not ev.resourceId
  then
    return codec.error("INVALID_INPUT", "event id missing")
  end
  local event_id = ev.id or ev.eventId or ev.event_id or ev.pspReference or ev.resourceId
  local ts = tonumber(msg.Timestamp) or os.time()
  if event_id then
    local ok_seen, err_seen = mark_event_seen(msg.Provider, event_id, ts)
    if not ok_seen then
      return codec.error("CONFLICT", "Duplicate webhook", { reason = err_seen, eventId = event_id })
    end
  end
  if provider == "adyen" and ev.pspReference then
    local ok_seen, err_seen = mark_event_seen("adyen_psp", ev.pspReference, ts)
    if not ok_seen then
      return codec.error("CONFLICT", "Duplicate PSP reference", { reason = err_seen })
    end
  end
  local pay = state.payments[pid]
  if not pay then
    return codec.error("NOT_FOUND", "Payment not found")
  end
  if msg.Provider:lower() == "stripe" then
    pay.stripeIdempotencyKey = msg.IdempotencyKey or parse_header(msg.Headers, "Idempotency-Key")
  end
  local ok_evt, evt_err = validate_payment_event(ev, pay)
  if not ok_evt then
    return codec.error("INVALID_INPUT", "Event rejected", { reason = evt_err })
  end
  if provider == "stripe" and STRIPE_VERIFY_EVENT and ev.id then
    local fetched, ferr = stripe_fetch_event(ev.id)
    if not fetched or type(fetched) ~= "table" then
      return codec.error("FORBIDDEN", "Stripe event fetch failed", { reason = ferr })
    end
    local obj = fetched.data and fetched.data.object or {}
    local pi = obj.id or obj.payment_intent or obj.payment_intent_id
    if pi and pi ~= pid then
      return codec.error(
        "FORBIDDEN",
        "Stripe event payment mismatch",
        { fetchedPayment = pi, expected = pid }
      )
    end
    if obj.amount_received and obj.amount_received / 100 > pay.amount + 0.01 then
      return codec.error("FORBIDDEN", "Stripe event amount mismatch")
    end
    if obj.currency and pay.currency and obj.currency:upper() ~= pay.currency:upper() then
      return codec.error("FORBIDDEN", "Stripe event currency mismatch")
    end
  end
  local before = pay.status
  if ev.type == "payment_succeeded" then
    if before ~= "refunded" then
      pay.status = "captured"
      pay.capturedAt = os.time()
    end
  elseif ev.type == "payment_failed" then
    if before ~= "captured" and before ~= "refunded" then
      pay.status = "failed"
    end
  elseif ev.type == "refund_succeeded" then
    pay.status = "refunded"
    pay.refundAmount = ev.amount or ev.refundAmount or pay.amount
  end
  audit.record("catalog", "HandlePaymentProviderWebhook", msg, nil, {
    paymentId = pid,
    status = pay.status,
    provider = msg.Provider,
    eventId = event_id,
    statusBefore = before,
  })
  if provider == "stripe" then
    cache_stripe_idempotency(msg, { paymentId = pid, status = pay.status })
  end
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

function handlers.ListNotificationFailures(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Clear",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local site = msg["Site-Id"] or "global"
  local items = state.notification_failures[site] or {}
  if msg.Clear == true or msg.Clear == "true" then
    state.notification_failures[site] = {}
  end
  return codec.ok { siteId = site, failures = items, total = #items }
end

function handlers.ExportRecommendations(msg)
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
  local limit = tonumber(msg.Limit) or 20
  if limit < 1 then
    limit = 1
  end
  if limit > 200 then
    limit = 200
  end
  local events = state.events[msg["Site-Id"]] or {}
  local list = {}
  for sku, stats in pairs(events) do
    local score = (stats.purchases or 0) * 3
      + (stats.add_to_cart or 0) * 1.5
      + (stats.views or 0) * 0.2
    table.insert(list, { sku = sku, score = score, stats = stats })
  end
  table.sort(list, function(a, b)
    return (a.score or 0) > (b.score or 0)
  end)
  while #list > limit do
    table.remove(list)
  end
  return codec.ok { siteId = msg["Site-Id"], items = list, total = #list }
end

function handlers.ImportCatalogCSV(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Path" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Path",
    "DryRun",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local f = io.open(msg.Path, "r")
  if not f then
    return codec.error("NOT_FOUND", "File not found", { path = msg.Path })
  end
  local header = f:read "*l"
  if not header then
    f:close()
    return codec.error("INVALID_INPUT", "Empty file")
  end
  local cols = parse_csv_line(header)
  local idx = {}
  for i, col in ipairs(cols) do
    idx[col:lower()] = i
  end
  local required = { "sku", "name", "price", "currency" }
  for _, r in ipairs(required) do
    if not idx[r] then
      f:close()
      return codec.error("INVALID_INPUT", "Missing column " .. r)
    end
  end
  local imported = 0
  local dry = msg.DryRun == true or msg.DryRun == "true"
  for line in f:lines() do
    if line ~= "" then
      local fields = parse_csv_line(line)
      local sku = fields[idx.sku]
      local name = fields[idx.name]
      local price = tonumber(fields[idx.price])
      local currency = fields[idx.currency]
      if not (sku and name and price and currency) then
        f:close()
        return codec.error("INVALID_INPUT", "Missing required fields on line", { line = line })
      end
      imported = imported + 1
      if imported > IMPORT_MAX_ROWS then
        f:close()
        return codec.error("INVALID_INPUT", "Import too large", { limit = IMPORT_MAX_ROWS })
      end
      if not dry then
        local payload = {
          sku = sku,
          name = name,
          price = price,
          currency = currency,
          description = idx.description and fields[idx.description] or nil,
          categoryId = idx.category and fields[idx.category] or nil,
          taxClass = idx.taxclass and fields[idx.taxclass] or nil,
        }
        if idx.weight and fields[idx.weight] then
          payload.weight = tonumber(fields[idx.weight])
        end
        if idx.assets and fields[idx.assets] then
          payload.assets = {}
          for token in fields[idx.assets]:gmatch "[^,; ]+" do
            table.insert(payload.assets, token)
          end
        end
        if idx.attributes and fields[idx.attributes] and json_ok then
          local ok_attr, attrs = pcall(cjson.decode, fields[idx.attributes])
          if ok_attr and type(attrs) == "table" then
            payload.attributes = attrs
          end
        end
        local key = ids.product_key(msg["Site-Id"], sku)
        state.products[key] = { payload = payload }
        if idx.stock and fields[idx.stock] then
          local qty = tonumber(fields[idx.stock]) or 0
          state.inventory[msg["Site-Id"]] = state.inventory[msg["Site-Id"]] or {}
          state.inventory[msg["Site-Id"]]["default"] = state.inventory[msg["Site-Id"]]["default"]
            or {}
          state.inventory[msg["Site-Id"]]["default"][sku] = qty
        end
        if (idx.region or idx.valid_from or idx.valid_to) and price then
          add_price_window(msg["Site-Id"], currency, {
            region = idx.region and fields[idx.region] or nil,
            valid_from = idx.valid_from and fields[idx.valid_from] or nil,
            valid_to = idx.valid_to and fields[idx.valid_to] or nil,
            prices = { [sku] = price },
          })
        end
      end
    end
  end
  f:close()
  audit.record(
    "catalog",
    "ImportCatalogCSV",
    msg,
    nil,
    { siteId = msg["Site-Id"], imported = imported, dryRun = dry }
  )
  return codec.ok { imported = imported, dryRun = dry }
end

function handlers.BulkPriceUpdate(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Updates" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Updates",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_updates, err_updates = validation.assert_type(msg.Updates, "table", "Updates")
  if not ok_updates then
    return codec.error("INVALID_INPUT", err_updates, { field = "Updates" })
  end
  local count = 0
  for _, row in ipairs(msg.Updates) do
    if not row.Sku or not row.Price then
      return codec.error("INVALID_INPUT", "Update requires Sku and Price")
    end
    local key = ids.product_key(msg["Site-Id"], row.Sku)
    if state.products[key] and state.products[key].payload then
      local price = tonumber(row.Price)
      if row.Region or row.ValidFrom or row.ValidTo then
        add_price_window(msg["Site-Id"], row.Currency or state.products[key].payload.currency, {
          region = row.Region,
          valid_from = row.ValidFrom,
          valid_to = row.ValidTo,
          prices = { [row.Sku] = price },
        })
      else
        state.products[key].payload.price = price
        if row.Currency then
          state.products[key].payload.currency = row.Currency
        end
      end
      count = count + 1
    end
  end
  audit.record("catalog", "BulkPriceUpdate", msg, nil, { siteId = msg["Site-Id"], count = count })
  return codec.ok { updated = count }
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
  state.company_terms[cid] = state.company_terms[cid]
    or {
      credit_limit = msg["Credit-Limit"],
      currency = msg.Currency or "USD",
      net_terms = msg["Net-Terms"] or "NET30",
      balance = 0,
    }
  audit.record("catalog", "CreateCompanyAccount", msg, nil, { companyId = cid })
  return codec.ok { companyId = cid, name = msg.Name }
end

function handlers.SetCompanyTerms(msg)
  local ok, missing =
    validation.require_fields(msg, { "Company-Id", "Credit-Limit", "Currency", "Net-Terms" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  state.company_terms[msg["Company-Id"]] = state.company_terms[msg["Company-Id"]] or { balance = 0 }
  local t = state.company_terms[msg["Company-Id"]]
  t.credit_limit = msg["Credit-Limit"]
  t.currency = msg.Currency
  t.net_terms = msg["Net-Terms"]
  return codec.ok {
    companyId = msg["Company-Id"],
    creditLimit = t.credit_limit,
    netTerms = t.net_terms,
    balance = t.balance or 0,
  }
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
  local terms = state.company_terms[po.companyId]
  if terms and terms.credit_limit and terms.balance then
    if (terms.balance + po.total) > terms.credit_limit then
      return codec.error("CREDIT_LIMIT_EXCEEDED", "PO exceeds credit limit", {
        creditLimit = terms.credit_limit,
        balance = terms.balance,
      })
    end
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
    provider = msg.Provider,
  }
  po.status = "in_checkout"
  po.checkoutId = checkout_id
  if terms then
    terms.balance = (terms.balance or 0) + po.total
  end
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
  for _, inv in pairs(state.invoices) do
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
