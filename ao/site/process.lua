-- Site process handlers: routes, pages, layouts, navigation.

local codec = require "ao.shared.codec"
local validation = require "ao.shared.validation"
local ids = require "ao.shared.ids"
local ar = require "ao.shared.arweave"
local auth = require "ao.shared.auth"
local idem = require "ao.shared.idempotency"
local audit = require "ao.shared.audit"
local metrics = require "ao.shared.metrics"
local schema = require "ao.shared.schema"
local assets = require "ao.shared.assets"
local a11y = require "ao.shared.a11y"
local i18n = require "ao.shared.i18n"
local layout_components = require "ao.shared.layout_components"

local handlers = {}
local allowed_actions = {
  "ResolveRoute",
  "GetPage",
  "GetLayout",
  "GetNavigation",
  "PutDraft",
  "AddDraftComment",
  "RequestPublish",
  "ApprovePublish",
  "SchedulePublish",
  "RunPublishScheduler",
  "LockDraft",
  "UnlockDraft",
  "ForceUnlockDraft",
  "RenewDraftLock",
  "GetDraftAudit",
  "RegisterContentType",
  "ListContentTypes",
  "SetPerfBudgets",
  "RecordWebVital",
  "UpsertRoute",
  "UpsertLayout",
  "RegisterAsset",
  "GetAsset",
  "SetLocales",
  "PublishVersion",
  "ArchivePage",
  "RecordOrder",
  "GetOrder",
  "ListOrders",
  "GetPublishLog",
  "ExportPublishLog",
  "GetPublishStatus",
}

local role_policy = {
  PutDraft = { "editor", "publisher", "admin" },
  AddDraftComment = { "editor", "publisher", "admin" },
  RequestPublish = { "editor", "publisher", "admin" },
  ApprovePublish = { "publisher", "admin" },
  SchedulePublish = { "publisher", "admin" },
  RunPublishScheduler = { "publisher", "admin" },
  LockDraft = { "editor", "publisher", "admin" },
  UnlockDraft = { "editor", "publisher", "admin" },
  ForceUnlockDraft = { "publisher", "admin" },
  RenewDraftLock = { "editor", "publisher", "admin" },
  GetDraftAudit = { "editor", "publisher", "admin", "support" },
  ExportPublishLog = { "admin" },
  RegisterContentType = { "admin" },
  ListContentTypes = { "editor", "publisher", "admin" },
  SetPerfBudgets = { "admin" },
  RecordWebVital = { "viewer", "support", "editor", "publisher", "admin" },
  UpsertRoute = { "editor", "publisher", "admin" },
  UpsertLayout = { "editor", "publisher", "admin" },
  RegisterAsset = { "editor", "publisher", "admin" },
  GetAsset = { "editor", "publisher", "admin", "support" },
  SetLocales = { "admin", "publisher" },
  PublishVersion = { "publisher", "admin" },
  ArchivePage = { "publisher", "admin" },
  RecordOrder = { "support", "admin" },
  GetOrder = { "support", "admin" },
  ListOrders = { "support", "admin" },
  GetPublishLog = { "publisher", "admin", "support" },
  ExportPublishLog = { "admin" },
  GetPublishStatus = { "publisher", "admin", "support" },
}

-- pseudo-state for scaffolding
local state = {
  routes = {}, -- route:<site>:<path>[:locale] -> { pageId, layoutId, type }
  pages = {}, -- page:<site>:<page>:<version>[:locale] -> { content, manifestTx, archived }
  layouts = {}, -- layout:<id>:<version>[:locale] -> { content }
  menus = {}, -- menu:<site>:<menu>:<version>[:locale] -> { items }
  drafts = {}, -- page:<site>:<page>:draft[:locale] -> { content }
  active_versions = {}, -- siteId -> versionId
  orders = {}, -- siteId -> orderId -> { status, totalAmount, currency, vatRate, updatedAt }
  assets = {}, -- siteId -> assetId -> metadata
  locales = {}, -- siteId -> { default = "en", supported = { "en" } }
  draft_comments = {}, -- draftKey -> { { author, body, ts } }
  draft_locks = {}, -- draftKey -> { subject, ts, ttl }
  publish_schedules = {}, -- siteId -> list { pageId, version, locale, publishAt, expireAt }
  content_types = {}, -- siteId -> { name -> schema }
  perf_budgets = {}, -- siteId -> { lcp_ms, cls, tbt_ms }
  perf_vitals = {}, -- siteId -> { last = { metric, value, ts } }
  publish_log = {}, -- list of publish/expire actions for observability
  draft_audit = {}, -- draftId -> { { ts, fields, actor } }
}

local MAX_CONTENT_BYTES = tonumber(os.getenv "SITE_MAX_CONTENT_BYTES" or "") or (64 * 1024)
local MAX_PUBLISH_RETRY = tonumber(os.getenv "SITE_MAX_PUBLISH_RETRY" or "") or 5
local PUBLISH_LOG_LIMIT = tonumber(os.getenv "SITE_PUBLISH_LOG_LIMIT" or "") or 1000
local PUBLISH_ALERT_PATH = os.getenv "SITE_PUBLISH_ALERT_PATH"
local PUBLISH_ALERT_WEBHOOK = os.getenv "SITE_PUBLISH_ALERT_WEBHOOK"

local function publish_alert(entry, msg)
  local payload = require("cjson").encode {
    ts = os.date "!%Y-%m-%dT%H:%M:%SZ",
    entry = entry,
    message = msg,
  }
  if PUBLISH_ALERT_PATH then
    local f = io.open(PUBLISH_ALERT_PATH, "a")
    if f then
      f:write(payload, "\n")
      f:close()
    end
  end
  if PUBLISH_ALERT_WEBHOOK and PUBLISH_ALERT_WEBHOOK ~= "" then
    local cmd = string.format(
      "curl -s -X POST -H 'Content-Type: application/json' --data %q %s >/dev/null 2>&1",
      payload,
      PUBLISH_ALERT_WEBHOOK
    )
    os.execute(cmd)
  end
end

local function get_locale_cfg(site_id)
  return state.locales[site_id] or { default = "en", supported = { "en" } }
end

local function pick_locale(site_id, requested)
  local cfg = get_locale_cfg(site_id)
  if not requested or requested == "" then
    return cfg.default
  end
  for _, loc in ipairs(cfg.supported or {}) do
    if loc:lower() == requested:lower() then
      return loc:lower()
    end
  end
  return cfg.default
end

local function validate_locales(msg)
  local supported = msg.Locales or { msg["Default-Locale"] or "en" }
  local default_locale = (msg["Default-Locale"] or supported[1]):lower()
  if #supported == 0 or #supported > 16 then
    return nil, nil, "Locales must contain 1-16 entries"
  end
  for _, loc in ipairs(supported) do
    local ok_len_loc, err_loc = validation.check_length(loc, 10, "Locales")
    if not ok_len_loc then
      return nil, nil, err_loc
    end
    if not loc:match "^[A-Za-z][A-Za-z%-]*$" then
      return nil, nil, "Locale must be alpha/alpha-dash"
    end
  end
  local found_default = false
  for _, loc in ipairs(supported) do
    if loc:lower() == default_locale then
      found_default = true
      break
    end
  end
  if not found_default then
    return nil, nil, "Default-Locale must be listed in Locales"
  end
  return supported, default_locale
end

function handlers.ResolveRoute(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Path" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Path", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then
    return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" })
  end
  local ok_len_path, err_path = validation.check_length(msg.Path, 2048, "Path")
  if not ok_len_path then
    return codec.error("INVALID_INPUT", err_path, { field = "Path" })
  end
  local locale_cfg = get_locale_cfg(msg["Site-Id"])
  local locale, normalized_path =
    i18n.detect_locale(msg.Path, locale_cfg.supported, locale_cfg.default)
  local key_locale = ids.route_key(msg["Site-Id"], normalized_path, locale)
  local key_default = ids.route_key(msg["Site-Id"], normalized_path, locale_cfg.default)
  local key_plain = ids.route_key(msg["Site-Id"], normalized_path)
  local route = state.routes[key_locale] or state.routes[key_default] or state.routes[key_plain]
  if not route then
    return codec.error("NOT_FOUND", "Route not found", { path = msg.Path })
  end
  local perf = state.perf_vitals[msg["Site-Id"]]
  local budgets = state.perf_budgets[msg["Site-Id"]]
  if perf and budgets then
    if perf.metric == "LCP" and budgets.lcp_ms and perf.value > budgets.lcp_ms then
      return codec.error("PERF_BUDGET_EXCEEDED", "LCP over budget", { lcp = perf.value })
    end
    if perf.metric == "CLS" and budgets.cls and perf.value > budgets.cls then
      return codec.error("PERF_BUDGET_EXCEEDED", "CLS over budget", { cls = perf.value })
    end
    if perf.metric == "TBT" and budgets.tbt_ms and perf.value > budgets.tbt_ms then
      return codec.error("PERF_BUDGET_EXCEEDED", "TBT over budget", { tbt = perf.value })
    end
  end
  local cache_policy = state.edge_cache
    and state.edge_cache[msg["Site-Id"]]
    and state.edge_cache[msg["Site-Id"]][route.path or msg.Path]
  return codec.ok {
    siteId = msg["Site-Id"],
    path = msg.Path,
    locale = locale,
    pageId = route.pageId,
    layoutId = route.layoutId,
    type = route.type or "page",
    cache = cache_policy,
  }
end

function handlers.GetPage(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Page-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Page-Id",
    "Version",
    "Locale",
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
  local ok_len_page, err_page = validation.check_length(msg["Page-Id"], 128, "Page-Id")
  if not ok_len_page then
    return codec.error("INVALID_INPUT", err_page, { field = "Page-Id" })
  end
  if msg.Version then
    local ok_len_ver, err_ver = validation.check_length(msg.Version, 128, "Version")
    if not ok_len_ver then
      return codec.error("INVALID_INPUT", err_ver, { field = "Version" })
    end
  end
  local version = msg.Version or state.active_versions[msg["Site-Id"]] or "active"
  local locale = pick_locale(msg["Site-Id"], msg.Locale)
  local key = ids.page_key(msg["Site-Id"], msg["Page-Id"], version, locale)
  local fallback = ids.page_key(msg["Site-Id"], msg["Page-Id"], version)
  local page = state.pages[key] or state.pages[fallback]
  if not page or page.archived then
    return codec.error(
      "NOT_FOUND",
      "Page not found",
      { pageId = msg["Page-Id"], version = version }
    )
  end
  return codec.ok {
    siteId = msg["Site-Id"],
    pageId = msg["Page-Id"],
    version = version,
    locale = locale,
    content = page.content,
  }
end

function handlers.GetLayout(msg)
  local ok, missing = validation.require_fields(msg, { "Layout-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Layout-Id",
    "Version",
    "Locale",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_layout, err_layout = validation.check_length(msg["Layout-Id"], 128, "Layout-Id")
  if not ok_len_layout then
    return codec.error("INVALID_INPUT", err_layout, { field = "Layout-Id" })
  end
  if msg.Version then
    local ok_len_ver, err_ver = validation.check_length(msg.Version, 128, "Version")
    if not ok_len_ver then
      return codec.error("INVALID_INPUT", err_ver, { field = "Version" })
    end
  end
  local version = msg.Version or "active"
  local locale = msg.Locale and msg.Locale:lower() or nil
  local key = ids.layout_key(msg["Layout-Id"], version, locale)
  local fallback = ids.layout_key(msg["Layout-Id"], version)
  local layout = state.layouts[key] or state.layouts[fallback]
  if not layout then
    return codec.error(
      "NOT_FOUND",
      "Layout not found",
      { layoutId = msg["Layout-Id"], version = version }
    )
  end
  return codec.ok {
    layoutId = msg["Layout-Id"],
    version = version,
    locale = locale or nil,
    content = layout.content,
    warnings = layout.warnings,
  }
end

function handlers.GetNavigation(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Menu-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Menu-Id",
    "Version",
    "Locale",
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
  local ok_len_menu, err_menu = validation.check_length(msg["Menu-Id"], 128, "Menu-Id")
  if not ok_len_menu then
    return codec.error("INVALID_INPUT", err_menu, { field = "Menu-Id" })
  end
  if msg.Version then
    local ok_len_ver, err_ver = validation.check_length(msg.Version, 128, "Version")
    if not ok_len_ver then
      return codec.error("INVALID_INPUT", err_ver, { field = "Version" })
    end
  end
  local version = msg.Version or state.active_versions[msg["Site-Id"]] or "active"
  local locale = pick_locale(msg["Site-Id"], msg.Locale)
  local key = ids.menu_key(msg["Site-Id"], msg["Menu-Id"], version, locale)
  local fallback = ids.menu_key(msg["Site-Id"], msg["Menu-Id"], version)
  local menu = state.menus[key] or state.menus[fallback]
  if not menu then
    return codec.error(
      "NOT_FOUND",
      "Navigation not found",
      { menuId = msg["Menu-Id"], version = version }
    )
  end
  return codec.ok {
    siteId = msg["Site-Id"],
    menuId = msg["Menu-Id"],
    version = version,
    locale = locale,
    items = menu.items,
  }
end

function handlers.PutDraft(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Page-Id", "Content" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Page-Id",
    "Content",
    "Locale",
    "Content-Type",
    "Actor-Role",
    "Schema-Version",
    "ExpectedVersion",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_site, err_site = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_site then
    return codec.error("INVALID_INPUT", err_site, { field = "Site-Id" })
  end
  local ok_len_page, err_page = validation.check_length(msg["Page-Id"], 128, "Page-Id")
  if not ok_len_page then
    return codec.error("INVALID_INPUT", err_page, { field = "Page-Id" })
  end
  local ok_type, err_type = validation.assert_type(msg.Content, "table", "Content")
  if not ok_type then
    return codec.error("INVALID_INPUT", err_type, { field = "Content" })
  end
  -- normalize content against schema expectations
  if not msg.Content.id then
    msg.Content.id = msg["Page-Id"]
  end
  if not msg.Content.blocks then
    msg.Content.blocks = {}
  end
  local content_type = msg["Content-Type"] or "page"
  if
    state.content_types[msg["Site-Id"]] and not state.content_types[msg["Site-Id"]][content_type]
  then
    return codec.error("INVALID_INPUT", "Unknown content type", { contentType = content_type })
  end
  -- enforce lazy/blur on block images by default
  if msg.Content.blocks then
    for _, block in ipairs(msg.Content.blocks) do
      if type(block) == "table" and block.image and type(block.image) == "table" then
        block.image.loading = block.image.loading or "lazy"
        block.image.placeholder = block.image.placeholder or "blur"
      end
    end
  end
  local content_len = validation.estimate_json_length(msg.Content)
  local ok_size, err_size = validation.check_size(content_len, MAX_CONTENT_BYTES, "Content")
  if not ok_size then
    return codec.error("INVALID_INPUT", err_size, { field = "Content" })
  end
  local ok_schema, schema_err
  if content_type == "page" then
    ok_schema, schema_err = schema.validate("page", msg.Content)
  else
    local custom_schema = state.content_types[msg["Site-Id"]]
      and state.content_types[msg["Site-Id"]][content_type]
    ok_schema, schema_err = schema.validate_custom(custom_schema, msg.Content)
  end
  if not ok_schema then
    return codec.error("INVALID_INPUT", "Content failed schema", { errors = schema_err })
  end
  local ok_a11y, a11y_warnings = a11y.validate_page(msg.Content)
  if not ok_a11y and os.getenv "A11Y_STRICT" == "1" then
    return codec.error(
      "INVALID_INPUT",
      "Accessibility validation failed",
      { warnings = a11y_warnings }
    )
  end
  local locale = pick_locale(msg["Site-Id"], msg.Locale)
  local key = ids.page_key(msg["Site-Id"], msg["Page-Id"], "draft", locale)
  local previous = state.drafts[key]
  if previous and previous.content then
    local changed_fields = {}
    for k, v in pairs(msg.Content) do
      if previous.content[k] ~= v then
        table.insert(changed_fields, k)
      end
    end
    local conflicts = {}
    if msg.Merge == true and type(previous.content) == "table" then
      for k, v in pairs(msg.Content) do
        if type(v) == "table" and type(previous.content[k]) == "table" then
          for subk, subv in pairs(v) do
            previous.content[k][subk] = subv
          end
          msg.Content[k] = previous.content[k]
        elseif previous.content[k] ~= v and previous.content[k] ~= nil then
          conflicts[k] = { incoming = v, existing = previous.content[k] }
        end
      end
    end
    state.draft_audit[key] = state.draft_audit[key] or {}
    table.insert(state.draft_audit[key], {
      ts = os.date "!%Y-%m-%dT%H:%M:%SZ",
      actor = msg.Subject or msg["Actor-Role"],
      fields = changed_fields,
      conflicts = conflicts,
    })
  end
  state.drafts[key] = {
    content = msg.Content,
    updatedAt = os.date "!%Y-%m-%dT%H:%M:%SZ",
    locale = locale,
    status = "draft",
    publishAt = msg.PublishAt,
    expireAt = msg.ExpireAt,
    contentType = content_type,
  }
  return codec.ok {
    draftId = key,
    warnings = a11y_warnings,
    locale = locale,
    contentType = content_type,
  }
end

function handlers.AddDraftComment(msg)
  local ok, missing = validation.require_fields(msg, { "Draft-Id", "Author", "Body" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Draft-Id",
    "Author",
    "Body",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  state.draft_comments[msg["Draft-Id"]] = state.draft_comments[msg["Draft-Id"]] or {}
  table.insert(state.draft_comments[msg["Draft-Id"]], {
    author = msg.Author,
    body = msg.Body,
    ts = os.date "!%Y-%m-%dT%H:%M:%SZ",
  })
  return codec.ok { draftId = msg["Draft-Id"], count = #state.draft_comments[msg["Draft-Id"]] }
end

function handlers.UpsertRoute(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Path", "Page-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Path",
    "Page-Id",
    "Layout-Id",
    "Type",
    "Locale",
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
  local ok_len_path, err_path = validation.check_length(msg.Path, 2048, "Path")
  if not ok_len_path then
    return codec.error("INVALID_INPUT", err_path, { field = "Path" })
  end
  local ok_len_page, err_page = validation.check_length(msg["Page-Id"], 128, "Page-Id")
  if not ok_len_page then
    return codec.error("INVALID_INPUT", err_page, { field = "Page-Id" })
  end
  if msg["Layout-Id"] then
    local ok_len_layout, err_layout = validation.check_length(msg["Layout-Id"], 128, "Layout-Id")
    if not ok_len_layout then
      return codec.error("INVALID_INPUT", err_layout, { field = "Layout-Id" })
    end
  end
  if msg.Type then
    local ok_len_type, err_type = validation.check_length(msg.Type, 64, "Type")
    if not ok_len_type then
      return codec.error("INVALID_INPUT", err_type, { field = "Type" })
    end
  end
  local locale = pick_locale(msg["Site-Id"], msg.Locale)
  local key = ids.route_key(msg["Site-Id"], msg.Path, locale)
  state.routes[key] = {
    pageId = msg["Page-Id"],
    layoutId = msg["Layout-Id"],
    type = msg.Type or "page",
    locale = locale,
  }
  return codec.ok { path = msg.Path, pageId = msg["Page-Id"], locale = locale }
end

function handlers.UpsertLayout(msg)
  local ok, missing = validation.require_fields(msg, { "Layout-Id", "Version", "Components" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Layout-Id",
    "Version",
    "Components",
    "Locale",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_layout, err_layout = validation.check_length(msg["Layout-Id"], 128, "Layout-Id")
  if not ok_len_layout then
    return codec.error("INVALID_INPUT", err_layout, { field = "Layout-Id" })
  end
  local ok_len_ver, err_ver = validation.check_length(msg.Version, 128, "Version")
  if not ok_len_ver then
    return codec.error("INVALID_INPUT", err_ver, { field = "Version" })
  end
  local ok_type, err_type = validation.assert_type(msg.Components, "table", "Components")
  if not ok_type then
    return codec.error("INVALID_INPUT", err_type, { field = "Components" })
  end
  local ok_layout, layout_warnings = layout_components.validate(msg.Components)
  if not ok_layout and os.getenv "LAYOUT_STRICT" == "1" then
    return codec.error("INVALID_INPUT", "Layout components invalid", { warnings = layout_warnings })
  end
  local locale = msg.Locale and msg.Locale:lower() or nil
  local key = ids.layout_key(msg["Layout-Id"], msg.Version, locale)
  state.layouts[key] = { content = msg.Components, locale = locale, warnings = layout_warnings }
  audit.record(
    "site",
    "UpsertLayout",
    msg,
    nil,
    { layoutId = msg["Layout-Id"], version = msg.Version, locale = locale }
  )
  return codec.ok {
    layoutId = msg["Layout-Id"],
    version = msg.Version,
    locale = locale,
    warnings = layout_warnings,
  }
end

function handlers.RegisterAsset(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Asset-Id", "Url" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Asset-Id",
    "Url",
    "Type",
    "Formats",
    "Sizes",
    "Base-Url",
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
  local ok_len_id, err_id = validation.check_length(msg["Asset-Id"], 256, "Asset-Id")
  if not ok_len_id then
    return codec.error("INVALID_INPUT", err_id, { field = "Asset-Id" })
  end
  local ok_len_url, err_url = validation.check_length(msg.Url, 2048, "Url")
  if not ok_len_url then
    return codec.error("INVALID_INPUT", err_url, { field = "Url" })
  end
  local typ = msg.Type or "image"
  if typ ~= "image" and typ ~= "video" then
    return codec.error("INVALID_INPUT", "Type must be image|video", { field = "Type" })
  end
  state.assets[msg["Site-Id"]] = state.assets[msg["Site-Id"]] or {}
  local meta = { type = typ, url = msg.Url }
  if typ == "image" then
    local manifest = assets.build_image_variants(msg.Url, {
      sizes = msg.Sizes,
      formats = msg.Formats,
      base_url = msg["Base-Url"],
    })
    meta.variants = manifest.variants
    meta.srcset = manifest.srcset
    meta.formats = manifest.formats
    meta.sizes = manifest.sizes
    meta.src = manifest.src
    meta.loading = manifest.loading
    meta.placeholder = manifest.placeholder
  end
  state.assets[msg["Site-Id"]][msg["Asset-Id"]] = meta
  audit.record(
    "site",
    "RegisterAsset",
    msg,
    nil,
    { siteId = msg["Site-Id"], assetId = msg["Asset-Id"], type = typ }
  )
  return codec.ok { siteId = msg["Site-Id"], assetId = msg["Asset-Id"], asset = meta }
end

function handlers.GetAsset(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Asset-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Asset-Id", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local assets_for_site = state.assets[msg["Site-Id"]] or {}
  local asset = assets_for_site[msg["Asset-Id"]]
  if not asset then
    return codec.error("NOT_FOUND", "Asset not found", { assetId = msg["Asset-Id"] })
  end
  return codec.ok { siteId = msg["Site-Id"], assetId = msg["Asset-Id"], asset = asset }
end

function handlers.SetLocales(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Locales" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Locales",
    "Default-Locale",
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
  local supported, default_locale, err_loc = validate_locales(msg)
  if not supported then
    return codec.error("INVALID_INPUT", err_loc, { field = "Locales" })
  end
  state.locales[msg["Site-Id"]] = { supported = supported, default = default_locale }
  audit.record(
    "site",
    "SetLocales",
    msg,
    nil,
    { siteId = msg["Site-Id"], default = default_locale }
  )
  return codec.ok { siteId = msg["Site-Id"], defaultLocale = default_locale, locales = supported }
end

function handlers.PublishVersion(msg)
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
  local site = msg["Site-Id"]
  local snapshots = {}
  local current = state.active_versions[site]
  if msg.ExpectedVersion and current and current ~= msg.ExpectedVersion then
    return codec.error(
      "VERSION_CONFLICT",
      "ExpectedVersion mismatch",
      { expected = msg.ExpectedVersion, current = current }
    )
  end
  -- promote drafts to versioned pages for this site and bundle snapshot (locale-aware)
  local prefix = "page:" .. site .. ":"
  for key, draft in pairs(state.drafts) do
    if key:sub(1, #prefix) == prefix then
      local parts = {}
      for part in key:gmatch "[^:]+" do
        table.insert(parts, part)
      end
      local page_id = parts[3]
      local locale = parts[5]
      local target_key = ids.page_key(site, page_id, msg.Version, locale)
      state.pages[target_key] = { content = draft.content, locale = locale }
      table.insert(snapshots, { pageId = page_id, content = draft.content, locale = locale })
    end
  end

  local manifestTx
  local manifestHash
  if #snapshots > 0 then
    manifestTx, manifestHash =
      ar.put_snapshot { siteId = site, version = msg.Version, pages = snapshots }
    if not manifestTx then
      return codec.error("INVALID_INPUT", "Snapshot too large for Arweave manifest")
    end
  end

  state.active_versions[site] = msg.Version
  local resp = codec.ok {
    siteId = site,
    activeVersion = msg.Version,
    manifestTx = manifestTx,
    manifestHash = manifestHash,
  }
  audit.record("site", "PublishVersion", msg, resp, { manifestTx = manifestTx })
  return resp
end

function handlers.ArchivePage(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Page-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Page-Id",
    "Version",
    "Locale",
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
  local ok_len_page, err_page = validation.check_length(msg["Page-Id"], 128, "Page-Id")
  if not ok_len_page then
    return codec.error("INVALID_INPUT", err_page, { field = "Page-Id" })
  end
  if msg.Version then
    local ok_len_ver, err_ver = validation.check_length(msg.Version, 128, "Version")
    if not ok_len_ver then
      return codec.error("INVALID_INPUT", err_ver, { field = "Version" })
    end
  end
  local version = msg.Version or state.active_versions[msg["Site-Id"]] or "active"
  local locale = pick_locale(msg["Site-Id"], msg.Locale)
  local key = ids.page_key(msg["Site-Id"], msg["Page-Id"], version, locale)
  local fallback = ids.page_key(msg["Site-Id"], msg["Page-Id"], version)
  if state.pages[key] then
    state.pages[key].archived = true
  elseif state.pages[fallback] then
    state.pages[fallback].archived = true
  end
  return codec.ok { pageId = msg["Page-Id"], version = version, locale = locale, archived = true }
end

-- Authoring workflow -----------------------------------------------------
local function assert_lock(draft_id, subject)
  local lock = state.draft_locks[draft_id]
  if not lock then
    return true
  end
  local ttl = lock.ttl or 900
  if os.time() - (lock.ts or 0) > ttl then
    state.draft_locks[draft_id] = nil
    return true
  end
  if lock.subject == subject then
    lock.ts = os.time()
    return true
  end
  return false, "LOCKED_BY_OTHER"
end

function handlers.LockDraft(msg)
  local ok, missing = validation.require_fields(msg, { "Draft-Id", "Subject" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local allowed, reason = assert_lock(msg["Draft-Id"], msg.Subject)
  if not allowed then
    return codec.error("CONFLICT", reason)
  end
  state.draft_locks[msg["Draft-Id"]] = { subject = msg.Subject, ts = os.time(), ttl = 900 }
  return codec.ok { draftId = msg["Draft-Id"], subject = msg.Subject }
end

function handlers.UnlockDraft(msg)
  local ok, missing = validation.require_fields(msg, { "Draft-Id", "Subject" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local lock = state.draft_locks[msg["Draft-Id"]]
  if lock and lock.subject ~= msg.Subject then
    return codec.error("FORBIDDEN", "Only lock owner can unlock")
  end
  state.draft_locks[msg["Draft-Id"]] = nil
  return codec.ok { draftId = msg["Draft-Id"], unlocked = true }
end

function handlers.ForceUnlockDraft(msg)
  local ok, missing = validation.require_fields(msg, { "Draft-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  state.draft_locks[msg["Draft-Id"]] = nil
  audit.record(
    "site",
    "ForceUnlockDraft",
    msg,
    nil,
    { draftId = msg["Draft-Id"], reason = msg.Reason or "unspecified" }
  )
  state.draft_audit[msg["Draft-Id"]] = state.draft_audit[msg["Draft-Id"]] or {}
  table.insert(state.draft_audit[msg["Draft-Id"]], {
    ts = os.date "!%Y-%m-%dT%H:%M:%SZ",
    actor = msg.Subject or msg["Actor-Role"],
    fields = { "lock" },
    action = "force_unlock",
    reason = msg.Reason,
    code = msg["Reason-Code"],
  })
  return codec.ok { draftId = msg["Draft-Id"], unlocked = true, forced = true, reason = msg.Reason }
end

function handlers.RenewDraftLock(msg)
  local ok, missing = validation.require_fields(msg, { "Draft-Id", "Subject" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local lock = state.draft_locks[msg["Draft-Id"]]
  if not lock or lock.subject ~= msg.Subject then
    return codec.error("FORBIDDEN", "Lock not held by subject")
  end
  lock.ts = os.time()
  return codec.ok { draftId = msg["Draft-Id"], renewed = true }
end

function handlers.RequestPublish(msg)
  local ok, missing = validation.require_fields(msg, { "Draft-Id", "Requested-By" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local draft = state.drafts[msg["Draft-Id"]]
  if not draft then
    return codec.error("NOT_FOUND", "Draft not found")
  end
  draft.status = "in_review"
  draft.requestedBy = msg["Requested-By"]
  draft.requestedAt = os.date "!%Y-%m-%dT%H:%M:%SZ"
  return codec.ok { draftId = msg["Draft-Id"], status = draft.status }
end

function handlers.ApprovePublish(msg)
  local ok, missing = validation.require_fields(msg, { "Draft-Id", "Approved-By" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local draft = state.drafts[msg["Draft-Id"]]
  if not draft then
    return codec.error("NOT_FOUND", "Draft not found")
  end
  draft.status = "approved"
  draft.approvedBy = msg["Approved-By"]
  draft.approvedAt = os.date "!%Y-%m-%dT%H:%M:%SZ"
  return codec.ok { draftId = msg["Draft-Id"], status = draft.status }
end

function handlers.SchedulePublish(msg)
  local ok, missing =
    validation.require_fields(msg, { "Site-Id", "Page-Id", "Version", "Publish-At" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local locale = pick_locale(msg["Site-Id"], msg.Locale)
  state.publish_schedules[msg["Site-Id"]] = state.publish_schedules[msg["Site-Id"]] or {}
  table.insert(state.publish_schedules[msg["Site-Id"]], {
    pageId = msg["Page-Id"],
    version = msg.Version,
    locale = locale,
    publishAt = msg["Publish-At"],
    expireAt = msg["Expire-At"],
    status = "pending",
    retryCount = 0,
    lastError = nil,
  })
  return codec.ok { siteId = msg["Site-Id"], count = #state.publish_schedules[msg["Site-Id"]] }
end

local function iso_to_ts(iso)
  if not iso or iso == "" then
    return nil
  end
  local year, mon, day, hour, min, sec =
    iso:match "^(%d%d%d%d)%-(%d%d)%-(%d%d)T(%d%d):(%d%d):(%d%d)Z$"
  if not year then
    return nil
  end
  return os.time {
    year = tonumber(year),
    month = tonumber(mon),
    day = tonumber(day),
    hour = tonumber(hour),
    min = tonumber(min),
    sec = tonumber(sec),
    isdst = false,
  }
end

function handlers.RunPublishScheduler(msg)
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

  local now_ts = os.time()
  local sites = {}
  if msg["Site-Id"] then
    sites = { msg["Site-Id"] }
  else
    for site_id in pairs(state.publish_schedules) do
      table.insert(sites, site_id)
    end
  end

  local published = {}
  local expired = {}

  for _, site_id in ipairs(sites) do
    local pending = {}
    for _, entry in ipairs(state.publish_schedules[site_id] or {}) do
      local publish_ts = iso_to_ts(entry.publishAt)
      local expire_ts = iso_to_ts(entry.expireAt)
      local should_publish = publish_ts and publish_ts <= now_ts
      local should_expire = expire_ts and expire_ts <= now_ts

      if entry.status == "failed" then
        table.insert(pending, entry)
        goto continue
      end

      if should_publish then
        local draft_key = ids.page_key(site_id, entry.pageId, "draft", entry.locale)
        local draft_fallback = ids.page_key(site_id, entry.pageId, "draft")
        local draft = state.drafts[draft_key] or state.drafts[draft_fallback]
        if draft then
          local target_key = ids.page_key(site_id, entry.pageId, entry.version, entry.locale)
          state.pages[target_key] = {
            content = draft.content,
            locale = entry.locale,
            publishedAt = entry.publishAt,
          }
          draft.status = "published"
          state.active_versions[site_id] = entry.version
          audit.record("site", "RunPublishScheduler", msg, nil, {
            siteId = site_id,
            pageId = entry.pageId,
            version = entry.version,
            locale = entry.locale,
            action = "publish",
          })
          table.insert(published, {
            siteId = site_id,
            pageId = entry.pageId,
            version = entry.version,
            locale = entry.locale,
          })
          table.insert(state.publish_log, {
            ts = os.date "!%Y-%m-%dT%H:%M:%SZ",
            siteId = site_id,
            pageId = entry.pageId,
            version = entry.version,
            locale = entry.locale,
            action = "publish",
          })
          entry.status = "published"
          entry.lastError = nil
        else
          table.insert(pending, entry) -- no draft yet; keep waiting
          table.insert(state.publish_log, {
            ts = os.date "!%Y-%m-%dT%H:%M:%SZ",
            siteId = site_id,
            pageId = entry.pageId,
            version = entry.version,
            locale = entry.locale,
            action = "missing_draft",
          })
          entry.retryCount = (entry.retryCount or 0) + 1
          entry.lastError = "draft_missing"
          if entry.retryCount >= MAX_PUBLISH_RETRY then
            entry.status = "failed"
            audit.record("site", "RunPublishScheduler", msg, nil, {
              siteId = site_id,
              pageId = entry.pageId,
              version = entry.version,
              locale = entry.locale,
              action = "failed_retry",
            })
            publish_alert(entry, "publish_failed_max_retry")
            table.insert(state.publish_log, {
              ts = os.date "!%Y-%m-%dT%H:%M:%SZ",
              siteId = site_id,
              pageId = entry.pageId,
              version = entry.version,
              locale = entry.locale,
              action = "failed_retry",
              retryCount = entry.retryCount,
            })
          end
        end
      end

      if should_expire then
        local page_key = ids.page_key(site_id, entry.pageId, entry.version, entry.locale)
        local page = state.pages[page_key]
        if page then
          page.archived = true
          audit.record("site", "RunPublishScheduler", msg, nil, {
            siteId = site_id,
            pageId = entry.pageId,
            version = entry.version,
            locale = entry.locale,
            action = "expire",
          })
          table.insert(expired, {
            siteId = site_id,
            pageId = entry.pageId,
            version = entry.version,
            locale = entry.locale,
          })
          table.insert(state.publish_log, {
            ts = os.date "!%Y-%m-%dT%H:%M:%SZ",
            siteId = site_id,
            pageId = entry.pageId,
            version = entry.version,
            locale = entry.locale,
            action = "expire",
          })
        end
      end

      if (not should_publish) and not should_expire then
        table.insert(pending, entry)
      end
    end
    state.publish_schedules[site_id] = pending
  end

  -- prune publish log to limit
  if #state.publish_log > PUBLISH_LOG_LIMIT then
    local drop = #state.publish_log - PUBLISH_LOG_LIMIT
    for _ = 1, drop do
      table.remove(state.publish_log, 1)
    end
  end

  return codec.ok {
    published = published,
    expired = expired,
    remaining = state.publish_schedules,
    logSize = #state.publish_log,
    statuses = state.publish_schedules,
  }
end

function handlers.GetPublishLog(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Limit",
    "Offset",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local limit = tonumber(msg.Limit or 100) or 100
  local offset = tonumber(msg.Offset or 0) or 0
  local items = {}
  local start = math.max(1, #state.publish_log - limit - offset + 1)
  for i = start, math.max(start, #state.publish_log - offset) do
    local entry = state.publish_log[i]
    if not msg["Site-Id"] or (entry and entry.siteId == msg["Site-Id"]) then
      table.insert(items, entry)
    end
  end
  return codec.ok { siteId = msg["Site-Id"], items = items, total = #items, offset = offset }
end

function handlers.ExportPublishLog(msg)
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
  local data = msg["Site-Id"] and {} or state.publish_log
  if msg["Site-Id"] then
    for _, item in ipairs(state.publish_log) do
      if item.siteId == msg["Site-Id"] then
        table.insert(data, item)
      end
    end
  end
  local path = os.getenv "SITE_PUBLISH_LOG_EXPORT"
  if path then
    local f = io.open(path, "a")
    if f then
      f:write(require("cjson").encode(data), "\n")
      f:close()
    end
  end
  return codec.ok { siteId = msg["Site-Id"], items = data, total = #data }
end

function handlers.GetPublishStatus(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Page-Id",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local items = {}
  for _, entry in ipairs(state.publish_schedules[msg["Site-Id"]] or {}) do
    if (not msg["Page-Id"]) or entry.pageId == msg["Page-Id"] then
      table.insert(items, entry)
    end
  end
  return codec.ok { siteId = msg["Site-Id"], items = items, total = #items }
end

function handlers.GetDraftAudit(msg)
  local ok, missing = validation.require_fields(msg, { "Draft-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Draft-Id",
    "Limit",
    "Offset",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local limit = tonumber(msg.Limit or 50) or 50
  local offset = tonumber(msg.Offset or 0) or 0
  local audit_log = state.draft_audit[msg["Draft-Id"]] or {}
  local items = {}
  for i = math.max(1, #audit_log - limit - offset + 1), math.max(0, #audit_log - offset) do
    items[#items + 1] = audit_log[i]
  end
  return codec.ok { draftId = msg["Draft-Id"], items = items, total = #items, offset = offset }
end

function handlers.RegisterContentType(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Name", "Schema" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  if type(msg.Schema) ~= "table" then
    return codec.error("INVALID_INPUT", "Schema must be object", { field = "Schema" })
  end
  state.content_types[msg["Site-Id"]] = state.content_types[msg["Site-Id"]] or {}
  state.content_types[msg["Site-Id"]][msg.Name] = msg.Schema
  return codec.ok { siteId = msg["Site-Id"], name = msg.Name }
end

function handlers.ListContentTypes(msg)
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Actor-Role", "Schema-Version" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  return codec.ok { siteId = msg["Site-Id"], types = state.content_types[msg["Site-Id"]] or {} }
end

function handlers.SetPerfBudgets(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Budgets" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  state.perf_budgets[msg["Site-Id"]] = msg.Budgets
  return codec.ok { siteId = msg["Site-Id"], budgets = msg.Budgets }
end

function handlers.RecordWebVital(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Metric", "Value" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local budgets = state.perf_budgets[msg["Site-Id"]] or {}
  local metric = msg.Metric
  local value = msg.Value
  if metric == "LCP" and budgets.lcp_ms and value > budgets.lcp_ms then
    return codec.error(
      "PERF_BUDGET_EXCEEDED",
      "LCP over budget",
      { lcp = value, budget = budgets.lcp_ms }
    )
  end
  if metric == "CLS" and budgets.cls and value > budgets.cls then
    return codec.error(
      "PERF_BUDGET_EXCEEDED",
      "CLS over budget",
      { cls = value, budget = budgets.cls }
    )
  end
  if metric == "TBT" and budgets.tbt_ms and value > budgets.tbt_ms then
    return codec.error(
      "PERF_BUDGET_EXCEEDED",
      "TBT over budget",
      { tbt = value, budget = budgets.tbt_ms }
    )
  end
  state.perf_vitals[msg["Site-Id"]] = {
    metric = metric,
    value = value,
    ts = os.time(),
  }
  return codec.ok { siteId = msg["Site-Id"], metric = metric }
end

function handlers.RecordOrder(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Order-Id", "Status" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Order-Id",
    "Status",
    "TotalAmount",
    "Currency",
    "VatRate",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  state.orders[msg["Site-Id"]] = state.orders[msg["Site-Id"]] or {}
  state.orders[msg["Site-Id"]][msg["Order-Id"]] = {
    status = msg.Status,
    totalAmount = msg.TotalAmount,
    currency = msg.Currency,
    vatRate = msg.VatRate,
    updatedAt = msg.Timestamp,
  }
  return codec.ok {
    siteId = msg["Site-Id"],
    orderId = msg["Order-Id"],
    status = msg.Status,
    totalAmount = msg.TotalAmount,
    currency = msg.Currency,
    vatRate = msg.VatRate,
  }
end

function handlers.GetOrder(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Order-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Order-Id", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local site_orders = state.orders[msg["Site-Id"]] or {}
  local order = site_orders[msg["Order-Id"]]
  if not order then
    return codec.error("NOT_FOUND", "Order not found", { orderId = msg["Order-Id"] })
  end
  return codec.ok {
    siteId = msg["Site-Id"],
    orderId = msg["Order-Id"],
    status = order.status,
    totalAmount = order.totalAmount,
    currency = order.currency,
    vatRate = order.vatRate,
    updatedAt = order.updatedAt,
    reason = order.reason,
  }
end

function handlers.ListOrders(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Missing field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Status",
    "Page",
    "PageSize",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local page = tonumber(msg.Page or 1) or 1
  local page_size = tonumber(msg.PageSize or 20) or 20
  local site_orders = state.orders[msg["Site-Id"]] or {}
  local items = {}
  for oid, o in pairs(site_orders) do
    if not msg.Status or msg.Status == o.status then
      table.insert(items, {
        orderId = oid,
        status = o.status,
        totalAmount = o.totalAmount,
        currency = o.currency,
        vatRate = o.vatRate,
        updatedAt = o.updatedAt,
      })
    end
  end
  table.sort(items, function(a, b)
    return tostring(a.updatedAt or "") > tostring(b.updatedAt or "")
  end)
  local start = (page - 1) * page_size + 1
  local slice = {}
  for i = start, math.min(#items, start + page_size - 1) do
    table.insert(slice, items[i])
  end
  return codec.ok {
    siteId = msg["Site-Id"],
    total = #items,
    page = page,
    pageSize = page_size,
    items = slice,
  }
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
  metrics.inc("site." .. msg.Action .. ".count")
  metrics.tick()
  idem.record(msg["Request-Id"], resp)
  return resp
end

return {
  route = route,
  _state = state, -- exposed for tests
}
