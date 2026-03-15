-- Registry process handlers: domains, sites, versions, roles.
-- Lightweight in-memory scaffolding to keep contracts testable.

local codec = require "ao.shared.codec"
local validation = require "ao.shared.validation"
local auth = require "ao.shared.auth"
local idem = require "ao.shared.idempotency"
local ids = require "ao.shared.ids"
local audit = require "ao.shared.audit"
local metrics = require "ao.shared.metrics"
local schema = require "ao.shared.schema"
local json_ok, json = pcall(require, "cjson.safe")

local handlers = {}
local allowed_actions = {
  "GetSiteByHost",
  "GetSiteConfig",
  "RegisterSite",
  "BindDomain",
  "SetActiveVersion",
  "GrantRole",
  "UpdateTrustResolvers",
  "GetTrustedResolvers",
  "FlagResolver",
  "UnflagResolver",
  "GetResolverFlags",
}

local role_policy = {
  RegisterSite = { "admin", "registry-admin" },
  BindDomain = { "admin", "registry-admin" },
  SetActiveVersion = { "admin", "registry-admin" },
  GrantRole = { "admin", "registry-admin" },
  UpdateTrustResolvers = { "admin", "registry-admin" },
  GetTrustedResolvers = { "admin", "registry-admin" },
  FlagResolver = { "admin", "registry-admin" },
  UnflagResolver = { "admin", "registry-admin" },
  GetResolverFlags = { "admin", "registry-admin" },
}

-- pseudo-state kept in-memory for now; AO runtime would persist this.
local state = {
  sites = {}, -- siteId => {config = {}, createdAt = ts}
  domains = {}, -- host => siteId
  active_versions = {}, -- siteId => versionId
  roles = {}, -- siteId => map[user] = role
  trust = { resolvers = {}, manifestTx = nil, updatedAt = nil },
  resolver_flags = {}, -- resolverId => { flag = "suspicious"|"blocked"|"ok", reason, raisedAt, raisedBy }
}

local MAX_CONFIG_BYTES = tonumber(os.getenv "REGISTRY_MAX_CONFIG_BYTES" or "") or (16 * 1024)
local FLAGS_PATH = os.getenv "AO_FLAGS_PATH"
local WAL_PATH = os.getenv "AO_WAL_PATH"

local function now_iso()
  -- coarse timestamp for audit/debug; determinism is sufficient here.
  return os.date "!%Y-%m-%dT%H:%M:%SZ"
end

local function append_log(path, obj)
  if not path or path == "" or not json_ok then
    return
  end
  local line = json.encode(obj)
  if not line then
    return
  end
  local f = io.open(path, "a")
  if f then
    f:write(line)
    f:write "\n"
    f:close()
  end
end

local function persist_flag_event(ev)
  append_log(WAL_PATH, ev)
  append_log(FLAGS_PATH, ev)
end

function handlers.GetSiteByHost(msg)
  local ok, missing = validation.require_fields(msg, { "Host" })
  if not ok then
    return codec.error("INVALID_INPUT", "Host is required", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Host", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len, err = validation.check_length(msg.Host, 255, "Host")
  if not ok_len then
    return codec.error("INVALID_INPUT", err, { field = "Host" })
  end
  local site_id = state.domains[msg.Host]
  if not site_id then
    return codec.error("NOT_FOUND", "Domain not bound", { host = msg.Host })
  end
  return codec.ok {
    siteId = site_id,
    activeVersion = state.active_versions[site_id],
  }
end

function handlers.GetSiteConfig(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Site-Id is required", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len, err = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len then
    return codec.error("INVALID_INPUT", err, { field = "Site-Id" })
  end
  local site = state.sites[msg["Site-Id"]]
  if not site then
    return codec.error("NOT_FOUND", "Site not registered", { siteId = msg["Site-Id"] })
  end
  return codec.ok {
    siteId = msg["Site-Id"],
    config = site.config,
    activeVersion = state.active_versions[msg["Site-Id"]],
  }
end

function handlers.RegisterSite(msg)
  local ok, missing = validation.require_fields(msg, { "Site-Id" })
  if not ok then
    return codec.error("INVALID_INPUT", "Site-Id is required", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Config",
    "Version",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len, err = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len then
    return codec.error("INVALID_INPUT", err, { field = "Site-Id" })
  end
  local config = msg.Config or {}
  if msg.Config ~= nil then
    local ok_type_cfg, err_type_cfg = validation.assert_type(msg.Config, "table", "Config")
    if not ok_type_cfg then
      return codec.error("INVALID_INPUT", err_type_cfg, { field = "Config" })
    end
    local ok_schema, schema_err = schema.validate("registryConfig", msg.Config)
    if not ok_schema then
      return codec.error("INVALID_INPUT", "Config failed schema", { errors = schema_err })
    end
  end
  local config_len = validation.estimate_json_length(config)
  local ok_size, err_size = validation.check_size(config_len, MAX_CONFIG_BYTES, "Config")
  if not ok_size then
    return codec.error("INVALID_INPUT", err_size, { field = "Config" })
  end
  local existing = state.sites[msg["Site-Id"]]
  if existing then
    return codec.ok {
      siteId = msg["Site-Id"],
      createdAt = existing.createdAt,
      config = existing.config,
      activeVersion = state.active_versions[msg["Site-Id"]],
      note = "already_registered",
    }
  end
  state.sites[msg["Site-Id"]] = {
    config = config,
    createdAt = now_iso(),
  }
  state.active_versions[msg["Site-Id"]] = config.version or msg.Version or nil
  audit.record("registry", "RegisterSite", msg, nil)
  return codec.ok {
    siteId = msg["Site-Id"],
    createdAt = state.sites[msg["Site-Id"]].createdAt,
    activeVersion = state.active_versions[msg["Site-Id"]],
  }
end

function handlers.BindDomain(msg)
  local required = { "Site-Id", "Host" }
  local ok, missing = validation.require_fields(msg, required)
  if not ok then
    return codec.error("INVALID_INPUT", "Missing required field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Site-Id", "Host", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_id, err_id = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_id then
    return codec.error("INVALID_INPUT", err_id, { field = "Site-Id" })
  end
  local ok_len_host, err_host = validation.check_length(msg.Host, 255, "Host")
  if not ok_len_host then
    return codec.error("INVALID_INPUT", err_host, { field = "Host" })
  end
  if not state.sites[msg["Site-Id"]] then
    return codec.error("NOT_FOUND", "Site not registered", { siteId = msg["Site-Id"] })
  end
  state.domains[msg.Host] = msg["Site-Id"]
  audit.record("registry", "BindDomain", msg, nil, { host = msg.Host })
  return codec.ok {
    host = msg.Host,
    siteId = msg["Site-Id"],
  }
end

function handlers.SetActiveVersion(msg)
  local required = { "Site-Id", "Version" }
  local ok, missing = validation.require_fields(msg, required)
  if not ok then
    return codec.error("INVALID_INPUT", "Missing required field", { missing = missing })
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
  local ok_len_id, err_id = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_id then
    return codec.error("INVALID_INPUT", err_id, { field = "Site-Id" })
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
  if not state.sites[msg["Site-Id"]] then
    return codec.error("NOT_FOUND", "Site not registered", { siteId = msg["Site-Id"] })
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
  local resp = codec.ok {
    siteId = msg["Site-Id"],
    activeVersion = msg.Version,
  }
  audit.record("registry", "SetActiveVersion", msg, resp, { version = msg.Version })
  return resp
end

function handlers.GrantRole(msg)
  local required = { "Site-Id", "Subject", "Role" }
  local ok, missing = validation.require_fields(msg, required)
  if not ok then
    return codec.error("INVALID_INPUT", "Missing required field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Site-Id",
    "Subject",
    "Role",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_len_id, err_id = validation.check_length(msg["Site-Id"], 128, "Site-Id")
  if not ok_len_id then
    return codec.error("INVALID_INPUT", err_id, { field = "Site-Id" })
  end
  local ok_len_subj, err_subj = validation.check_length(msg.Subject, 128, "Subject")
  if not ok_len_subj then
    return codec.error("INVALID_INPUT", err_subj, { field = "Subject" })
  end
  local ok_len_role, err_role = validation.check_length(msg.Role, 64, "Role")
  if not ok_len_role then
    return codec.error("INVALID_INPUT", err_role, { field = "Role" })
  end
  if not state.sites[msg["Site-Id"]] then
    return codec.error("NOT_FOUND", "Site not registered", { siteId = msg["Site-Id"] })
  end
  state.roles[msg["Site-Id"]] = state.roles[msg["Site-Id"]] or {}
  state.roles[msg["Site-Id"]][msg.Subject] = msg.Role
  audit.record("registry", "GrantRole", msg, nil, { subject = msg.Subject, role = msg.Role })
  return codec.ok {
    siteId = msg["Site-Id"],
    subject = msg.Subject,
    role = msg.Role,
  }
end

function handlers.UpdateTrustResolvers(msg)
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Manifest-Tx",
    "Resolvers",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  if msg.Resolvers and type(msg.Resolvers) ~= "table" then
    return codec.error("INVALID_INPUT", "Resolvers must be array")
  end
  local list = msg.Resolvers or {}
  state.trust.resolvers = list
  state.trust.manifestTx = msg["Manifest-Tx"]
  state.trust.updatedAt = now_iso()
  audit.record("registry", "UpdateTrustResolvers", msg, nil, { count = #list })
  return codec.ok {
    updatedAt = state.trust.updatedAt,
    count = #list,
    manifestTx = state.trust.manifestTx,
  }
end

function handlers.GetTrustedResolvers(msg)
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  return codec.ok {
    manifestTx = state.trust.manifestTx,
    updatedAt = state.trust.updatedAt,
    resolvers = state.trust.resolvers,
  }
end

local function validate_resolver_id(id)
  local ok_len, err = validation.check_length(id, 256, "Resolver-Id")
  if not ok_len then
    return false, err
  end
  return true
end

function handlers.FlagResolver(msg)
  local required = { "Resolver-Id", "Flag" }
  local ok, missing = validation.require_fields(msg, required)
  if not ok then
    return codec.error("INVALID_INPUT", "Missing required field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(msg, {
    "Action",
    "Request-Id",
    "Resolver-Id",
    "Flag",
    "Reason",
    "Actor-Role",
    "Schema-Version",
    "Signature",
  })
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_id, err_id = validate_resolver_id(msg["Resolver-Id"])
  if not ok_id then
    return codec.error("INVALID_INPUT", err_id, { field = "Resolver-Id" })
  end
  local flag = msg.Flag
  if flag ~= "suspicious" and flag ~= "blocked" and flag ~= "ok" then
    return codec.error("INVALID_INPUT", "Flag must be suspicious|blocked|ok", { flag = flag })
  end
  local reason = msg.Reason or ""
  local ok_len_reason, err_reason = validation.check_length(reason, 512, "Reason")
  if not ok_len_reason then
    return codec.error("INVALID_INPUT", err_reason, { field = "Reason" })
  end
  state.resolver_flags[msg["Resolver-Id"]] = {
    flag = flag,
    reason = reason,
    raisedAt = now_iso(),
    raisedBy = msg["Actor-Role"],
  }
  persist_flag_event {
    ts = now_iso(),
    action = "FlagResolver",
    resolverId = msg["Resolver-Id"],
    flag = flag,
    reason = reason,
  }
  audit.record("registry", "FlagResolver", msg, nil, { resolver = msg["Resolver-Id"], flag = flag })
  return codec.ok { resolverId = msg["Resolver-Id"], flag = flag, reason = reason }
end

function handlers.UnflagResolver(msg)
  local required = { "Resolver-Id" }
  local ok, missing = validation.require_fields(msg, required)
  if not ok then
    return codec.error("INVALID_INPUT", "Missing required field", { missing = missing })
  end
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Resolver-Id", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  local ok_id, err_id = validate_resolver_id(msg["Resolver-Id"])
  if not ok_id then
    return codec.error("INVALID_INPUT", err_id, { field = "Resolver-Id" })
  end
  state.resolver_flags[msg["Resolver-Id"]] = nil
  persist_flag_event {
    ts = now_iso(),
    action = "UnflagResolver",
    resolverId = msg["Resolver-Id"],
    flag = "cleared",
  }
  audit.record("registry", "UnflagResolver", msg, nil, { resolver = msg["Resolver-Id"] })
  return codec.ok { resolverId = msg["Resolver-Id"], flag = "cleared" }
end

function handlers.GetResolverFlags(msg)
  local ok_extra, extras = validation.require_no_extras(
    msg,
    { "Action", "Request-Id", "Resolver-Id", "Actor-Role", "Schema-Version", "Signature" }
  )
  if not ok_extra then
    return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras })
  end
  if msg["Resolver-Id"] then
    local ok_id, err_id = validate_resolver_id(msg["Resolver-Id"])
    if not ok_id then
      return codec.error("INVALID_INPUT", err_id, { field = "Resolver-Id" })
    end
    local entry = state.resolver_flags[msg["Resolver-Id"]]
    if not entry then
      return codec.ok { resolverId = msg["Resolver-Id"], flag = "none" }
    end
    return codec.ok {
      resolverId = msg["Resolver-Id"],
      flag = entry.flag,
      reason = entry.reason,
      raisedAt = entry.raisedAt,
      raisedBy = entry.raisedBy,
    }
  end
  local cnt = 0
  for _ in pairs(state.resolver_flags) do
    cnt = cnt + 1
  end
  return codec.ok { flags = state.resolver_flags, count = cnt }
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
  metrics.inc("registry." .. msg.Action .. ".count")
  metrics.tick()
  idem.record(msg["Request-Id"], resp)
  return resp
end

return {
  route = route,
  _state = state, -- exposed for tests
}
