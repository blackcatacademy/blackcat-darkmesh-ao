-- Deterministic ID generation and namespacing helpers.
-- These keep key shapes consistent across processes.

local IDs = {}

local function normalize_path(path)
  if not path or path == "" then
    return "/"
  end
  if path:sub(1, 1) ~= "/" then
    path = "/" .. path
  end
  -- collapse duplicate slashes (lightweight)
  path = path:gsub("//+", "/")
  return path
end

function IDs.site_key(site_id)
  return ("site:%s"):format(site_id)
end

function IDs.domain_key(host)
  return ("domain:%s"):format(host)
end

function IDs.version_key(site_id, version_id)
  return ("version:%s:%s"):format(site_id, version_id)
end

function IDs.route_key(site_id, path, locale)
  local normalized = normalize_path(path)
  if locale and locale ~= "" then
    return ("route:%s:%s:%s"):format(site_id, normalized, locale:lower())
  end
  return ("route:%s:%s"):format(site_id, normalized)
end

function IDs.page_key(site_id, page_id, version_id, locale)
  if locale and locale ~= "" then
    return ("page:%s:%s:%s:%s"):format(site_id, page_id, version_id or "active", locale:lower())
  end
  return ("page:%s:%s:%s"):format(site_id, page_id, version_id or "active")
end

function IDs.layout_key(layout_id, version_id, locale)
  if locale and locale ~= "" then
    return ("layout:%s:%s:%s"):format(layout_id, version_id or "active", locale:lower())
  end
  return ("layout:%s:%s"):format(layout_id, version_id or "active")
end

function IDs.menu_key(site_id, menu_id, version_id, locale)
  if locale and locale ~= "" then
    return ("menu:%s:%s:%s:%s"):format(site_id, menu_id, version_id or "active", locale:lower())
  end
  return ("menu:%s:%s:%s"):format(site_id, menu_id, version_id or "active")
end

function IDs.product_key(site_id, sku)
  return ("product:%s:%s"):format(site_id, sku)
end

function IDs.category_key(site_id, category_id)
  return ("category:%s:%s"):format(site_id, category_id)
end

function IDs.entitlement_key(subject, asset)
  return ("entitlement:%s:%s"):format(subject, asset)
end

return IDs
