-- Minimal JSON Schema validator (draft-07 subset) for local use.
-- Supports: type (string, number, object, array), required, properties,
-- enums, pattern, minItems, minLength, maxLength, items.type, format=date-time (basic).

local Schema = {}

-- Schemas embedded as Lua tables (converted from schemas/*.json)
local SCHEMAS = {
  page = {
    type = "object",
    required = { "id", "title", "blocks" },
    properties = {
      id = { type = "string" },
      title = { type = "string" },
      locale = { type = "string" },
      layoutId = { type = "string" },
      blocks = { type = "array", items = { type = "object" } },
    },
  },
  product = {
    type = "object",
    required = { "sku", "name" },
    properties = {
      sku = { type = "string" },
      name = { type = "string" },
      description = { type = "string" },
      price = { type = "number" },
      assets = { type = "array", items = { type = "string" } },
    },
  },
  route = {
    type = "object",
    required = { "siteId", "path", "pageId" },
    properties = {
      siteId = { type = "string" },
      path = { type = "string" },
      locale = { type = "string" },
      pageId = { type = "string" },
      type = { type = "string" },
    },
  },
  publish = {
    type = "object",
    required = { "publishId", "versionId", "manifestTx" },
    properties = {
      publishId = { type = "string" },
      versionId = { type = "string" },
      manifestTx = { type = "string" },
      activatedAt = { type = "string" },
      rollbackTo = { type = "string" },
    },
  },
  entitlement = {
    type = "object",
    required = { "subject", "asset" },
    properties = {
      subject = { type = "string" },
      asset = { type = "string" },
      policy = { type = "string" },
    },
  },
}

local function type_of(value)
  local t = type(value)
  if t == "table" then
    local i = 0
    for _ in pairs(value) do
      i = i + 1
      if value[i] == nil then
        return "object"
      end
    end
    return "array"
  end
  return t
end

local function validate_properties(value, schema, path, errors)
  if schema.required then
    for _, req in ipairs(schema.required) do
      if value[req] == nil then
        table.insert(errors, path .. req .. " is required")
      end
    end
  end
  if schema.properties then
    for name, prop in pairs(schema.properties) do
      if value[name] ~= nil then
        local actual_type = type_of(value[name])
        if prop.type and actual_type ~= prop.type then
          table.insert(errors, path .. name .. " expected " .. prop.type .. ", got " .. actual_type)
        end
        if prop.enum then
          local ok_enum = false
          for _, ev in ipairs(prop.enum) do if ev == value[name] then ok_enum = true end end
          if not ok_enum then
            table.insert(errors, path .. name .. " not in enum")
          end
        end
        if prop.pattern and actual_type == "string" then
          if not tostring(value[name]):match(prop.pattern) then
            table.insert(errors, path .. name .. " does not match pattern")
          end
        end
        if prop.minLength and actual_type == "string" and #tostring(value[name]) < prop.minLength then
          table.insert(errors, path .. name .. " shorter than minLength")
        end
        if prop.maxLength and actual_type == "string" and #tostring(value[name]) > prop.maxLength then
          table.insert(errors, path .. name .. " longer than maxLength")
        end
        if prop.type == "array" and prop.items and value[name] ~= nil then
          for idx, item in ipairs(value[name]) do
            local item_type = type_of(item)
            if prop.items.type and item_type ~= prop.items.type then
              table.insert(errors, path .. name .. "[" .. idx .. "] expected " .. prop.items.type .. ", got " .. item_type)
            end
          end
          if prop.minItems and #value[name] < prop.minItems then
            table.insert(errors, path .. name .. " fewer than minItems")
          end
        end
        if prop.format == "date-time" and actual_type == "string" then
          if not tostring(value[name]):match("^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%dZ$") then
            table.insert(errors, path .. name .. " invalid date-time")
          end
        end
      end
    end
  end
end

local function validate_against(schema, value, path, errors)
  local actual = type_of(value)
  if schema.type and actual ~= schema.type then
    table.insert(errors, path .. "expected " .. schema.type .. ", got " .. actual)
    return
  end
  if schema.type == "object" and type(value) == "table" then
    validate_properties(value, schema, path, errors)
  elseif schema.type == "array" and type(value) == "table" then
    if schema.items then
      for idx, item in ipairs(value) do
        validate_against(schema.items, item, path .. "[" .. idx .. "].", errors)
      end
    end
  end
end

function Schema.validate(schema_name, value)
  local schema = SCHEMAS[schema_name]
  if not schema then
    return true
  end
  local errors = {}
  validate_against(schema, value, "", errors)
  if #errors > 0 then
    return false, errors
  end
  return true
end

return Schema
