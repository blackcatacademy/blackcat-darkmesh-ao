-- Access process handlers: entitlements and protected assets.

local codec = require("ao.shared.codec")
local validation = require("ao.shared.validation")
local ids = require("ao.shared.ids")
local auth = require("ao.shared.auth")
local idem = require("ao.shared.idempotency")
local audit = require("ao.shared.audit")
local metrics = require("ao.shared.metrics")

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

local MAX_POLICY_BYTES = tonumber(os.getenv("ACCESS_MAX_POLICY_BYTES") or "") or (32 * 1024)

function handlers.HasEntitlement(msg)
  local ok, missing = validation.require_fields(msg, { "Subject", "Asset" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Subject", "Asset", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_len_sub, err_sub = validation.check_length(msg.Subject, 128, "Subject")
  if not ok_len_sub then return codec.error("INVALID_INPUT", err_sub, { field = "Subject" }) end
  local ok_len_asset, err_asset = validation.check_length(msg.Asset, 256, "Asset")
  if not ok_len_asset then return codec.error("INVALID_INPUT", err_asset, { field = "Asset" }) end
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
  local ok, missing = validation.require_fields(msg, { "Asset" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Asset", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_len_asset, err_asset = validation.check_length(msg.Asset, 256, "Asset")
  if not ok_len_asset then return codec.error("INVALID_INPUT", err_asset, { field = "Asset" }) end
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
  local ok, missing = validation.require_fields(msg, { "Subject", "Asset", "Policy" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Subject", "Asset", "Policy", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_len_sub, err_sub = validation.check_length(msg.Subject, 128, "Subject")
  if not ok_len_sub then return codec.error("INVALID_INPUT", err_sub, { field = "Subject" }) end
  local ok_len_asset, err_asset = validation.check_length(msg.Asset, 256, "Asset")
  if not ok_len_asset then return codec.error("INVALID_INPUT", err_asset, { field = "Asset" }) end
  local ok_len_policy, err_policy = validation.check_length(msg.Policy, 64, "Policy")
  if not ok_len_policy then return codec.error("INVALID_INPUT", err_policy, { field = "Policy" }) end
  local policy_size = validation.estimate_json_length(msg.Policy)
  local ok_size, err_size = validation.check_size(policy_size, MAX_POLICY_BYTES, "Policy")
  if not ok_size then return codec.error("INVALID_INPUT", err_size, { field = "Policy" }) end
  local key = ids.entitlement_key(msg.Subject, msg.Asset)
  state.entitlements[key] = msg.Policy
  audit.record("access", "GrantEntitlement", msg, nil, { subject = msg.Subject, asset = msg.Asset, policy = msg.Policy })
  return codec.ok({
    subject = msg.Subject,
    asset = msg.Asset,
    policy = msg.Policy,
  })
end

function handlers.RevokeEntitlement(msg)
  local ok, missing = validation.require_fields(msg, { "Subject", "Asset" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Subject", "Asset", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_len_sub, err_sub = validation.check_length(msg.Subject, 128, "Subject")
  if not ok_len_sub then return codec.error("INVALID_INPUT", err_sub, { field = "Subject" }) end
  local ok_len_asset, err_asset = validation.check_length(msg.Asset, 256, "Asset")
  if not ok_len_asset then return codec.error("INVALID_INPUT", err_asset, { field = "Asset" }) end
  local key = ids.entitlement_key(msg.Subject, msg.Asset)
  state.entitlements[key] = nil
  audit.record("access", "RevokeEntitlement", msg, nil, { subject = msg.Subject, asset = msg.Asset })
  return codec.ok({
    subject = msg.Subject,
    asset = msg.Asset,
    revoked = true,
  })
end

function handlers.PutProtectedAssetRef(msg)
  local ok, missing = validation.require_fields(msg, { "Asset", "Ref" })
  if not ok then return codec.error("INVALID_INPUT", "Missing field", { missing = missing }) end
  local ok_extra, extras = validation.require_no_extras(msg, { "Action", "Request-Id", "Asset", "Ref", "Visibility", "Actor-Role", "Schema-Version" })
  if not ok_extra then return codec.error("UNSUPPORTED_FIELD", "Unexpected fields", { unexpected = extras }) end
  local ok_len_asset, err_asset = validation.check_length(msg.Asset, 256, "Asset")
  if not ok_len_asset then return codec.error("INVALID_INPUT", err_asset, { field = "Asset" }) end
  local ok_len_ref, err_ref = validation.check_length(msg.Ref, 2048, "Ref")
  if not ok_len_ref then return codec.error("INVALID_INPUT", err_ref, { field = "Ref" }) end
  if msg.Visibility then
    local ok_len_vis, err_vis = validation.check_length(msg.Visibility, 32, "Visibility")
    if not ok_len_vis then return codec.error("INVALID_INPUT", err_vis, { field = "Visibility" }) end
  end
  state.protected[msg.Asset] = { ref = msg.Ref, visibility = msg.Visibility or "protected" }
  audit.record("access", "PutProtectedAssetRef", msg, nil, { asset = msg.Asset, ref = msg.Ref })
  return codec.ok({ asset = msg.Asset, ref = msg.Ref })
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
  metrics.inc("access." .. msg.Action .. ".count")
  idem.record(msg["Request-Id"], resp)
  return resp
end

return {
  route = route,
  _state = state,
}
