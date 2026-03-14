-- Catalog process handlers: products, categories, listings.

local codec = require("ao.shared.codec")
local validation = require("ao.shared.validation")

local handlers = {}
local allowed_actions = {
  "GetProduct",
  "ListCategoryProducts",
  "SearchCatalog",
  "UpsertProduct",
  "UpsertCategory",
  "PublishCatalogVersion",
}

function handlers.GetProduct(msg)
  return codec.not_implemented("GetProduct")
end

function handlers.ListCategoryProducts(msg)
  return codec.not_implemented("ListCategoryProducts")
end

function handlers.SearchCatalog(msg)
  return codec.not_implemented("SearchCatalog")
end

function handlers.UpsertProduct(msg)
  return codec.not_implemented("UpsertProduct")
end

function handlers.UpsertCategory(msg)
  return codec.not_implemented("UpsertCategory")
end

function handlers.PublishCatalogVersion(msg)
  return codec.not_implemented("PublishCatalogVersion")
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
