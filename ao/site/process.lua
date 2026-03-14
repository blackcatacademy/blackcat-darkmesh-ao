-- Site process handlers: routes, pages, layouts, navigation.

local codec = require("ao.shared.codec")
local validation = require("ao.shared.validation")
local ids = require("ao.shared.ids")
local ar = require("ao.shared.arweave")
local auth = require("ao.shared.auth")
local idem = require("ao.shared.idempotency")
local audit = require("ao.shared.audit")

local handlers = {}
local allowed_actions = {
  "ResolveRoute",
  "GetPage",
  "GetLayout",
  "GetNavigation",
  "PutDraft",
  "UpsertRoute",
  "PublishVersion",
  "ArchivePage",
}

local role_policy = {
  PutDraft = { "editor", "publisher", "admin" },
  UpsertRoute = { "editor", "publisher", "admin" },
  PublishVersion = { "publisher", "admin" },
  ArchivePage = { "publisher", "admin" },
}

-- pseudo-state for scaffolding
local state = {
  routes = {},        -- route:<site>:<path> -> { pageId, layoutId, type }
  pages = {},         -- page:<site>:<page>:<version> -> { content, manifestTx, archived }
  layouts = {},       -- layout:<id>:<version> -> { content }
  menus = {},         -- menu:<site>:<menu>:<version> -> { items }
  drafts = {},        -- page:<site>:<page>:draft -> { content }
  active_versions = {} -- siteId -> versionId
}

function handlers.ResolveRoute(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Path" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Path", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local key = ids.route_key(msg["Site-Id"], msg.Path)
  local route = state.routes[key]
  if not route then
    return codec.error("NOT_FOUND", "Route not found", { path = msg.Path })
  end
  return codec.ok({
    siteId = msg["Site-Id"],
    path = msg.Path,
    pageId = route.pageId,
    layoutId = route.layoutId,
    type = route.type or "page",
  })
end

function handlers.GetPage(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Page-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Page-Id", "Version", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local version = msg.Version or state.active_versions[msg["Site-Id"]] or "active"
  local key = ids.page_key(msg["Site-Id"], msg["Page-Id"], version)
  local page = state.pages[key]
  if not page or page.archived then
    return codec.error("NOT_FOUND", "Page not found", { pageId = msg["Page-Id"], version = version })
  end
  return codec.ok({
    siteId = msg["Site-Id"],
    pageId = msg["Page-Id"],
    version = version,
    content = page.content,
  })
end

function handlers.GetLayout(msg)
  local ok, missing = validation.require_fields(msg, { "Layout-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Layout-Id", "Version", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local version = msg.Version or "active"
  local key = ids.layout_key(msg["Layout-Id"], version)
  local layout = state.layouts[key]
  if not layout then
    return codec.error("NOT_FOUND", "Layout not found", { layoutId = msg["Layout-Id"], version = version })
  end
  return codec.ok({
    layoutId = msg["Layout-Id"],
    version = version,
    content = layout.content,
  })
end

function handlers.GetNavigation(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Menu-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Menu-Id", "Version", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local version = msg.Version or state.active_versions[msg["Site-Id"]] or "active"
  local key = ids.menu_key(msg["Site-Id"], msg["Menu-Id"], version)
  local menu = state.menus[key]
  if not menu then
    return codec.error("NOT_FOUND", "Navigation not found", { menuId = msg["Menu-Id"], version = version })
  end
  return codec.ok({
    siteId = msg["Site-Id"],
    menuId = msg["Menu-Id"],
    version = version,
    items = menu.items,
  })
end

function handlers.PutDraft(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Page-Id", "Content" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Page-Id", "Content", "Actor-Role", "Schema-Version", "ExpectedVersion" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local key = ids.page_key(msg["Site-Id"], msg["Page-Id"], "draft")
  state.drafts[key] = { content = msg.Content, updatedAt = os.date("!%Y-%m-%dT%H:%M:%SZ") }
  return codec.ok({ draftId = key })
end

function handlers.UpsertRoute(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Path", "Page-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Site-Id", "Path", "Page-Id", "Layout-Id", "Type", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local key = ids.route_key(msg["Site-Id"], msg.Path)
  state.routes[key] = {
    pageId = msg["Page-Id"],
    layoutId = msg["Layout-Id"],
    type = msg.Type or "page",
  }
  return codec.ok({ path = msg.Path, pageId = msg["Page-Id"] })
end

function handlers.PublishVersion(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Version" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local site = msg["Site-Id"]
  local snapshots = {}
  local current = state.active_versions[site]
  if msg.ExpectedVersion and current and current ~= msg.ExpectedVersion then
    return codec.error("VERSION_CONFLICT", "ExpectedVersion mismatch", { expected = msg.ExpectedVersion, current = current })
  end
  -- promote drafts to versioned pages for this site and bundle snapshot
  local prefix = "page:" .. site .. ":"
  for key, draft in pairs(state.drafts) do
    if key:sub(1, #prefix) == prefix then
      local parts = {}
      for part in key:gmatch("[^:]+") do table.insert(parts, part) end
      local page_id = parts[3]
      local target_key = ids.page_key(site, page_id, msg.Version)
      state.pages[target_key] = { content = draft.content }
      table.insert(snapshots, { pageId = page_id, content = draft.content })
    end
  end

  local manifestTx
  local manifestHash
  if #snapshots > 0 then
    manifestTx, manifestHash = ar.put_snapshot({ siteId = site, version = msg.Version, pages = snapshots })
  end

  state.active_versions[site] = msg.Version
  local resp = codec.ok({ siteId = site, activeVersion = msg.Version, manifestTx = manifestTx, manifestHash = manifestHash })
  audit.record("site", "PublishVersion", msg, resp, { manifestTx = manifestTx })
  return resp
end

function handlers.ArchivePage(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id", "Page-Id" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local version = msg.Version or state.active_versions[msg["Site-Id"]] or "active"
  local key = ids.page_key(msg["Site-Id"], msg["Page-Id"], version)
  if state.pages[key] then
    state.pages[key].archived = true
  end
  return codec.ok({ pageId = msg["Page-Id"], version = version, archived = true })
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
  _state = state, -- exposed for tests
}
