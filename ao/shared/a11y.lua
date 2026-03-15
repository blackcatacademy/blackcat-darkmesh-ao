-- Simple accessibility & performance lint for page content blocks.

local A11y = {}

local function warn(list, msg)
  table.insert(list, msg)
end

-- Validate a single block; return warnings appended to provided list.
local function validate_block(block, warnings, last_heading_level)
  local typ = block.type or block.kind
  if typ == "image" or typ == "hero" then
    if not block.alt or block.alt == "" then
      warn(warnings, "Image block missing alt text")
    end
  elseif typ == "link" then
    if not block.text or block.text == "" then
      warn(warnings, "Link block missing text")
    end
    if block.href and block.href:match "^javascript:" then
      warn(warnings, "Link uses javascript: URI, avoid for accessibility")
    end
  elseif typ == "heading" then
    local level = tonumber(block.level or block.depth or 0) or 0
    if level < 1 or level > 6 then
      warn(warnings, "Heading level must be 1-6")
    elseif last_heading_level and level > last_heading_level + 1 then
      warn(
        warnings,
        string.format("Heading level skips from h%d to h%d", last_heading_level, level)
      )
    end
    return level
  end
  return last_heading_level
end

---Validate a page content table (expects blocks array).
-- Returns ok:boolean, warnings:table
function A11y.validate_page(content)
  local warnings = {}
  if not content or type(content) ~= "table" then
    return true, warnings
  end
  local blocks = content.blocks or {}
  local last_heading_level = nil
  for _, block in ipairs(blocks) do
    last_heading_level = validate_block(block, warnings, last_heading_level)
  end
  return #warnings == 0, warnings
end

return A11y
