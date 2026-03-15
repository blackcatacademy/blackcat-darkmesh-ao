-- Layout component validator for block-based layouts.

local Layout = {}

local function warn(list, msg)
  table.insert(list, msg)
end

local validators = {}

validators.hero = function(comp, warnings)
  if not comp.title or comp.title == "" then
    warn(warnings, "hero.title required")
  end
  if comp.image and (not comp.image.alt or comp.image.alt == "") then
    warn(warnings, "hero.image.alt required when image set")
  end
end

validators.grid = function(comp, warnings)
  if not comp.items or type(comp.items) ~= "table" or #comp.items == 0 then
    warn(warnings, "grid.items must be non-empty array")
  end
end

validators.carousel = function(comp, warnings)
  if not comp.slides or type(comp.slides) ~= "table" or #comp.slides == 0 then
    warn(warnings, "carousel.slides must be non-empty array")
    return
  end
  for _, slide in ipairs(comp.slides) do
    if not slide.image then
      warn(warnings, "carousel.slide.image required")
    elseif not slide.alt or slide.alt == "" then
      warn(warnings, "carousel.slide.alt required")
    end
  end
end

validators.rich_text = function(comp, warnings)
  if not comp.body or comp.body == "" then
    warn(warnings, "rich_text.body required")
  end
end

validators.form = function(comp, warnings)
  if not comp.fields or type(comp.fields) ~= "table" or #comp.fields == 0 then
    warn(warnings, "form.fields must be non-empty array")
    return
  end
  for _, f in ipairs(comp.fields) do
    if not f.name or not f.label then
      warn(warnings, "form.field name and label required")
    end
  end
end

local allowed_types = {
  hero = true,
  grid = true,
  carousel = true,
  rich_text = true,
  form = true,
}

---Validate array of components.
-- @return ok:boolean, warnings:table
function Layout.validate(components)
  local warnings = {}
  if not components or type(components) ~= "table" then
    return true, warnings
  end
  for _, comp in ipairs(components) do
    local typ = comp.type or comp.kind
    if not typ or not allowed_types[typ] then
      warn(warnings, "Unsupported component type")
    else
      local v = validators[typ]
      if v then
        v(comp, warnings)
      end
    end
  end
  return #warnings == 0, warnings
end

return Layout
