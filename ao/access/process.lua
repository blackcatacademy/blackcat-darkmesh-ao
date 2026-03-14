-- Access process handlers: entitlements and protected assets.

local codec = require("ao.shared.codec")
local validation = require("ao.shared.validation")

local handlers = {}
local allowed_actions = {
  "HasEntitlement",
  "GetProtectedAssetRef",
  "GrantEntitlement",
  "RevokeEntitlement",
}

function handlers.HasEntitlement(msg)
  return codec.not_implemented("HasEntitlement")
end

function handlers.GetProtectedAssetRef(msg)
  return codec.not_implemented("GetProtectedAssetRef")
end

function handlers.GrantEntitlement(msg)
  return codec.not_implemented("GrantEntitlement")
end

function handlers.RevokeEntitlement(msg)
  return codec.not_implemented("RevokeEntitlement")
end

local function route(msg)
  local ok, missing = validation.require_tags(msg, { "Action" })
  if not ok then
    return codec.missing_tags(missing)
  end

  local ok_action, err = validation.require_action(msg, allowed_actions)
  if not ok_action then
    if err == "unknown_action" then
      return codec.unknown_action(msg.Action)
    end
    return codec.error("MISSING_ACTION", "Action is required")
  end

  local handler = handlers[msg.Action]
  if not handler then
    return codec.unknown_action(msg.Action)
  end

  return handler(msg)
end

return {
  route = route,
}
