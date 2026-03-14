-- Registry process handlers: domains, sites, versions, roles.

local codec = require("ao.shared.codec")
local validation = require("ao.shared.validation")

local handlers = {}
local allowed_actions = {
  "GetSiteByHost",
  "GetSiteConfig",
  "RegisterSite",
  "BindDomain",
  "SetActiveVersion",
  "GrantRole",
}

function handlers.GetSiteByHost(msg)
  return codec.not_implemented("GetSiteByHost")
end

function handlers.GetSiteConfig(msg)
  return codec.not_implemented("GetSiteConfig")
end

function handlers.RegisterSite(msg)
  return codec.not_implemented("RegisterSite")
end

function handlers.BindDomain(msg)
  return codec.not_implemented("BindDomain")
end

function handlers.SetActiveVersion(msg)
  return codec.not_implemented("SetActiveVersion")
end

function handlers.GrantRole(msg)
  return codec.not_implemented("GrantRole")
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
