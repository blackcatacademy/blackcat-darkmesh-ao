-- Site process handlers: routes, pages, layouts, navigation.

local codec = require("ao.shared.codec")
local validation = require("ao.shared.validation")

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

function handlers.ResolveRoute(msg)
  return codec.not_implemented("ResolveRoute")
end

function handlers.GetPage(msg)
  return codec.not_implemented("GetPage")
end

function handlers.GetLayout(msg)
  return codec.not_implemented("GetLayout")
end

function handlers.GetNavigation(msg)
  return codec.not_implemented("GetNavigation")
end

function handlers.PutDraft(msg)
  return codec.not_implemented("PutDraft")
end

function handlers.UpsertRoute(msg)
  return codec.not_implemented("UpsertRoute")
end

function handlers.PublishVersion(msg)
  return codec.not_implemented("PublishVersion")
end

function handlers.ArchivePage(msg)
  return codec.not_implemented("ArchivePage")
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
