-- Registry process handlers: domains, sites, versions, roles.
-- Lightweight in-memory scaffolding to keep contracts testable.

local codec = require("ao.shared.codec")
local validation = require("ao.shared.validation")
local auth = require("ao.shared.auth")
local idem = require("ao.shared.idempotency")
local ids = require("ao.shared.ids")
local audit = require("ao.shared.audit")

local handlers = {}
local allowed_actions = {
  "GetSiteByHost",
  "GetSiteConfig",
  "RegisterSite",
  "BindDomain",
  "SetActiveVersion",
  "GrantRole",
}

local role_policy = {
  RegisterSite = { "admin", "registry-admin" },
  BindDomain = { "admin", "registry-admin" },
  SetActiveVersion = { "admin", "registry-admin" },
  GrantRole = { "admin", "registry-admin" },
}

-- pseudo-state kept in-memory for now; AO runtime would persist this.
local state = {
  sites = {},          -- siteId => {config = {}, createdAt = ts}
  domains = {},        -- host => siteId
  active_versions = {},-- siteId => versionId
  roles = {},          -- siteId => map[user] = role
}

local function now_iso()
  -- coarse timestamp for audit/debug; determinism is sufficient here.
  return os.date("!%Y-%m-%dT%H:%M:%SZ")
end

function handlers.GetSiteByHost(msg)
  local ok, missing = validation.require_fields(msg, { "Host" })
  if not ok then
    return codec.error("INVALID_INPUT", "Host is required", { missing = missing })
  end
  local site_id = state.domains[msg.Host]
  if not site_id then
    return codec.error("NOT_FOUND", "Domain not bound", { host = msg.Host })
  end
  return codec.ok({
    siteId = site_id,
    activeVersion = state.active_versions[site_id],
  })
end

function handlers.GetSiteConfig(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Site-Id is required", { missing = missing })
  end
  local site = state.sites[msg["Site-Id"]]
  if not site then
    return codec.error("NOT_FOUND", "Site not registered", { siteId = msg["Site-Id"] })
  end
  return codec.ok({
    siteId = msg["Site-Id"],
    config = site.config,
    activeVersion = state.active_versions[msg["Site-Id"]],
  })
end

function handlers.RegisterSite(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Site-Id is required", { missing = missing })
  end
  local config = msg.Config or {}
  local existing = state.sites[msg["Site-Id"]]
  if existing then
    return codec.ok({
      siteId = msg["Site-Id"],
      createdAt = existing.createdAt,
      config = existing.config,
      activeVersion = state.active_versions[msg["Site-Id"]],
      note = "already_registered",
    })
  end
  state.sites[msg["Site-Id"]] = {
    config = config,
    createdAt = now_iso(),
  }
  state.active_versions[msg["Site-Id"]] = config.version or msg.Version or nil
  audit.record("registry", "RegisterSite", msg, nil)
  return codec.ok({
    siteId = msg["Site-Id"],
    createdAt = state.sites[msg["Site-Id"]].createdAt,
    activeVersion = state.active_versions[msg["Site-Id"]],
  })
end

function handlers.BindDomain(msg)
  local required = { "Site-Id", "Host" }
  local ok, missing = validation.require_fields(msg, required)
  if not ok then
    return codec.error("INVALID_INPUT", "Missing required field", { missing = missing })
  end
  if not state.sites[msg["Site-Id"]] then
    return codec.error("NOT_FOUND", "Site not registered", { siteId = msg["Site-Id"] })
  end
  state.domains[msg.Host] = msg["Site-Id"]
  audit.record("registry", "BindDomain", msg, nil, { host = msg.Host })
  return codec.ok({
    host = msg.Host,
    siteId = msg["Site-Id"],
  })
end

function handlers.SetActiveVersion(msg)
  local required = { "Site-Id", "Version" }
  local ok, missing = validation.require_fields(msg, required)
  if not ok then
    return codec.error("INVALID_INPUT", "Missing required field", { missing = missing })
  end
  if not state.sites[msg["Site-Id"]] then
    return codec.error("NOT_FOUND", "Site not registered", { siteId = msg["Site-Id"] })
  end
  local current = state.active_versions[msg["Site-Id"]]
  if msg.ExpectedVersion and current and current ~= msg.ExpectedVersion then
    return codec.error("VERSION_CONFLICT", "ExpectedVersion mismatch", { expected = msg.ExpectedVersion, current = current })
  end
  state.active_versions[msg["Site-Id"]] = msg.Version
  local resp = codec.ok({
    siteId = msg["Site-Id"],
    activeVersion = msg.Version,
  })
  audit.record("registry", "SetActiveVersion", msg, resp, { version = msg.Version })
  return resp
end

function handlers.GrantRole(msg)
  local required = { "Site-Id", "Subject", "Role" }
  local ok, missing = validation.require_fields(msg, required)
  if not ok then
    return codec.error("INVALID_INPUT", "Missing required field", { missing = missing })
  end
  if not state.sites[msg["Site-Id"]] then
    return codec.error("NOT_FOUND", "Site not registered", { siteId = msg["Site-Id"] })
  end
  state.roles[msg["Site-Id"]] = state.roles[msg["Site-Id"]] or {}
  state.roles[msg["Site-Id"]][msg.Subject] = msg.Role
  audit.record("registry", "GrantRole", msg, nil, { subject = msg.Subject, role = msg.Role })
  return codec.ok({
    siteId = msg["Site-Id"],
    subject = msg.Subject,
    role = msg.Role,
  })
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
