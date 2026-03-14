-- Access process handlers: entitlements and protected assets.

local codec = require("ao.shared.codec")
local validation = require("ao.shared.validation")
local ids = require("ao.shared.ids")
local auth = require("ao.shared.auth")
local idem = require("ao.shared.idempotency")
local audit = require("ao.shared.audit")

local handlers = {}
local allowed_actions = {
  "HasEntitlement",
  "GetProtectedAssetRef",
  "GrantEntitlement",
  "RevokeEntitlement",
  "PutProtectedAssetRef",
}

local role_policy = {
  GrantEntitlement = { "admin", "access-admin" },
  RevokeEntitlement = { "admin", "access-admin" },
  PutProtectedAssetRef = { "admin", "access-admin" },
}

local state = {
  entitlements = {},   -- entitlement:<subject>:<asset> -> policy
  protected = {},      -- asset:<id> -> { ref, visibility }
}

local function ensure(fields, msg)
  for _, f in ipairs(fields) do
    if msg[f] == nil then return false, f end
  end
  return true
end

function handlers.HasEntitlement(msg)
  local ok, missing = ensure({ "Subject", "Asset" }, msg)
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local key = ids.entitlement_key(msg.Subject, msg.Asset)
  local policy = state.entitlements[key]
  return codec.ok({
    subject = msg.Subject,
    asset = msg.Asset,
    hasEntitlement = policy ~= nil,
    policy = policy,
  })
end

function handlers.GetProtectedAssetRef(msg)
  local ok, missing = ensure({ "Asset" }, msg)
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local asset = state.protected[msg.Asset]
  if not asset then
    return codec.error("NOT_FOUND", "Asset ref not found", { asset = msg.Asset })
  end
  return codec.ok({
    asset = msg.Asset,
    ref = asset.ref,
    visibility = asset.visibility or "protected",
  })
end

function handlers.GrantEntitlement(msg)
  local ok, missing = ensure({ "Subject", "Asset", "Policy" }, msg)
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local key = ids.entitlement_key(msg.Subject, msg.Asset)
  state.entitlements[key] = msg.Policy
  audit.append({ action = "GrantEntitlement", subject = msg.Subject, asset = msg.Asset, policy = msg.Policy })
  return codec.ok({
    subject = msg.Subject,
    asset = msg.Asset,
    policy = msg.Policy,
  })
end

function handlers.RevokeEntitlement(msg)
  local ok, missing = ensure({ "Subject", "Asset" }, msg)
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local key = ids.entitlement_key(msg.Subject, msg.Asset)
  state.entitlements[key] = nil
  audit.append({ action = "RevokeEntitlement", subject = msg.Subject, asset = msg.Asset })
  return codec.ok({
    subject = msg.Subject,
    asset = msg.Asset,
    revoked = true,
  })
end

function handlers.PutProtectedAssetRef(msg)
  local ok, missing = ensure({ "Asset", "Ref" }, msg)
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  state.protected[msg.Asset] = { ref = msg.Ref, visibility = msg.Visibility or "protected" }
  audit.append({ action = "PutProtectedAssetRef", asset = msg.Asset, ref = msg.Ref })
  return codec.ok({ asset = msg.Asset, ref = msg.Ref })
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
