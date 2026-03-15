-- Locale helpers: detect locale from path prefixes and normalize to supported locales.

local I18n = {}

local function normalize_locale(locale)
  if not locale or locale == "" then
    return nil
  end
  return locale:lower()
end

---Detect locale prefix in a URL path and strip it.
-- @param path string (e.g. "/en/products/1")
-- @param supported table array of locales; if nil, no detection performed
-- @param default_locale string fallback locale
-- @return locale (string), stripped_path (string)
function I18n.detect_locale(path, supported, default_locale)
  local locale = normalize_locale(default_locale) or "en"
  local normalized_path = path or "/"
  if not supported or #supported == 0 or not path or path == "" then
    return locale, normalized_path
  end

  for _, candidate in ipairs(supported) do
    local lc = normalize_locale(candidate)
    local prefix = "/" .. lc
    if normalized_path == prefix then
      return lc, "/"
    end
    if normalized_path:sub(1, #prefix + 1) == prefix .. "/" then
      return lc, normalized_path:sub(#prefix + 1)
    end
  end

  return locale, normalized_path
end

return I18n
