-- Minimal JSON Schema validator with optional python/jsonschema backend.
-- If SCHEMA_VALIDATOR=python and python3+jsonschema are available,
-- uses that; otherwise falls back to the embedded validator below.

local Schema = {}
local SCHEMA_MODE = os.getenv "SCHEMA_VALIDATOR" or "auto" -- auto|python|embedded

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
      subject = { type = "string", minLength = 1, maxLength = 128 },
      asset = { type = "string", minLength = 1, maxLength = 256 },
      policy = { type = "string", minLength = 1, maxLength = 128 },
    },
  },
  accessAsset = {
    type = "object",
    required = { "asset", "ref" },
    properties = {
      asset = { type = "string", minLength = 1, maxLength = 256, pattern = "^[%w%-%._:/]+$" },
      ref = { type = "string", minLength = 1, maxLength = 2048, pattern = "^ar://[%w%-]+$" },
      visibility = { type = "string", enum = { "protected", "public", "private" } },
    },
  },
  registryConfig = {
    type = "object",
    required = {},
    properties = {
      version = { type = "string", minLength = 1, maxLength = 128 },
      metadata = { type = "object" },
      flags = {
        type = "object",
        properties = {
          cors = { type = "boolean" },
          corsAllowlist = {
            type = "array",
            minItems = 1,
            items = { type = "string", pattern = "^https?://[%w%.-]+(:%d+)?/?$" },
          },
          immutable = { type = "boolean" },
          allowUploads = { type = "boolean" },
          ttlSeconds = { type = "number", minimum = 0, maximum = 31536000 },
          rateLimitPerMinute = { type = "number", minimum = 0, maximum = 10000 },
          maxUploadBytes = { type = "number", minimum = 0, maximum = 104857600 },
          allowAnonRead = { type = "boolean" },
          requireMfa = { type = "boolean" },
        },
      },
      region = { type = "string", enum = { "eu", "us", "apac" } },
      tier = { type = "string", enum = { "dev", "staging", "prod" } },
      codeHash = { type = "string", pattern = "^[a-fA-F0-9]{64}$" },
      buildId = { type = "string", minLength = 1, maxLength = 128 },
      signerPubKey = { type = "string", pattern = "^[a-fA-F0-9]{64}$" },
      tableProfile = {
        type = "string",
        enum = {
          "minimal",
          "core-observability",
          "auth-rbac",
          "commerce-lite",
          "monitoring-outbox",
        },
      },
      schemaManifestTx = { type = "string", pattern = "^[A-Za-z0-9_-]{10,128}$" },
      schemaHash = { type = "string", pattern = "^[a-fA-F0-9]{64}$" },
      policies = {
        type = "object",
        properties = {
          allowAnonymousRead = { type = "boolean" },
          allowAnonymousWrite = { type = "boolean" },
          auditLevel = { type = "string", enum = { "none", "basic", "full" } },
          dataResidency = { type = "string", enum = { "eu", "us", "apac", "global" } },
          piiHandling = { type = "string", enum = { "deny", "mask", "allow" } },
          allowedOrigins = {
            type = "array",
            items = { type = "string", pattern = "^https?://[%w%.-]+(:%d+)?/?$" },
            minItems = 1,
          },
          ipAllowlist = {
            type = "array",
            items = { type = "string", pattern = "^%d+%.%d+%.%d+%.%d+/%d%d?$" },
            minItems = 0,
          },
          allowedMethods = {
            type = "array",
            items = {
              type = "string",
              enum = { "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS" },
            },
            minItems = 1,
          },
        },
      },
    },
  },
  arweaveResponse = {
    type = "object",
    required = { "status" },
    properties = {
      status = { type = "string" },
      message = { type = "string" },
      tx = { type = "string" },
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
          for _, ev in ipairs(prop.enum) do
            if ev == value[name] then
              ok_enum = true
            end
          end
          if not ok_enum then
            table.insert(errors, path .. name .. " not in enum")
          end
        end
        if prop.pattern and actual_type == "string" then
          if not tostring(value[name]):match(prop.pattern) then
            table.insert(errors, path .. name .. " does not match pattern")
          end
        end
        if
          prop.minLength
          and actual_type == "string"
          and #tostring(value[name]) < prop.minLength
        then
          table.insert(errors, path .. name .. " shorter than minLength")
        end
        if
          prop.maxLength
          and actual_type == "string"
          and #tostring(value[name]) > prop.maxLength
        then
          table.insert(errors, path .. name .. " longer than maxLength")
        end
        if prop.type == "array" and prop.items and value[name] ~= nil then
          for idx, item in ipairs(value[name]) do
            local item_type = type_of(item)
            if prop.items.type and item_type ~= prop.items.type then
              table.insert(
                errors,
                path
                  .. name
                  .. "["
                  .. idx
                  .. "] expected "
                  .. prop.items.type
                  .. ", got "
                  .. item_type
              )
            end
            if
              prop.items.pattern
              and type(item) == "string"
              and not tostring(item):match(prop.items.pattern)
            then
              table.insert(errors, path .. name .. "[" .. idx .. "] does not match pattern")
            end
            if prop.items.enum then
              local ok_enum = false
              for _, ev in ipairs(prop.items.enum) do
                if ev == item then
                  ok_enum = true
                end
              end
              if not ok_enum then
                table.insert(errors, path .. name .. "[" .. idx .. "] not in enum")
              end
            end
          end
          if prop.minItems and #value[name] < prop.minItems then
            table.insert(errors, path .. name .. " fewer than minItems")
          end
        elseif prop.type == "object" and prop.properties and type(value[name]) == "table" then
          validate_properties(value[name], prop, path .. name .. ".", errors)
        end
        if prop.format == "date-time" and actual_type == "string" then
          if not tostring(value[name]):match "^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%dZ$" then
            table.insert(errors, path .. name .. " invalid date-time")
          end
        end
        if prop.minimum and actual_type == "number" and value[name] < prop.minimum then
          table.insert(errors, path .. name .. " below minimum")
        end
        if prop.maximum and actual_type == "number" and value[name] > prop.maximum then
          table.insert(errors, path .. name .. " above maximum")
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
  if SCHEMA_MODE ~= "embedded" then
    local ok, err = Schema.validate_python(schema_name, value)
    if ok ~= nil then
      return ok, err
    end -- nil means fallback to embedded
  end
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

-- Validate against a schema table passed at runtime (same rules as embedded validator)
function Schema.validate_custom(schema_table, value)
  if not schema_table then
    return true
  end
  local errors = {}
  validate_against(schema_table, value, "", errors)
  if #errors > 0 then
    return false, errors
  end
  return true
end

-- Python/jsonschema validator (optional). Returns nil if not usable.
function Schema.validate_python(schema_name, value)
  local has_py = os.execute 'python3 -c "import jsonschema" >/dev/null 2>&1'
  if has_py ~= true and has_py ~= 0 then
    return nil, "python_jsonschema_missing"
  end
  local schema_path = "schemas/" .. schema_name .. ".schema.json"
  local f = io.open(schema_path, "r")
  if not f then
    return nil, "schema_not_found"
  end
  f:close()
  local tmp = os.tmpname() .. ".json"
  local jf = io.open(tmp, "w")
  if not jf then
    return nil, "tmp_write_failed"
  end
  local function json_encode(v)
    local t = type(v)
    if t == "nil" then
      return "null"
    end
    if t == "boolean" then
      return v and "true" or "false"
    end
    if t == "number" then
      return tostring(v)
    end
    if t == "string" then
      return string.format("%q", v)
    end
    if t == "table" then
      local is_array = true
      local i = 0
      for _, _ in pairs(v) do
        i = i + 1
        if v[i] == nil then
          is_array = false
        end
      end
      local parts = {}
      if is_array then
        for _, item in ipairs(v) do
          table.insert(parts, json_encode(item))
        end
        return "[" .. table.concat(parts, ",") .. "]"
      else
        for k, item in pairs(v) do
          table.insert(parts, string.format("%q:%s", tostring(k), json_encode(item)))
        end
        return "{" .. table.concat(parts, ",") .. "}"
      end
    end
    return '"<unsupported>"'
  end
  jf:write(json_encode(value))
  jf:close()
  local cmd = string.format(
    [[python3 - <<'PY'
import json,sys,jsonschema
with open(%q) as f: schema=json.load(f)
with open(%q) as f: inst=json.load(f)
try:
 jsonschema.validate(inst, schema)
 sys.exit(0)
except jsonschema.ValidationError:
 sys.exit(1)
PY]],
    schema_path,
    tmp
  )
  local ok = os.execute(cmd)
  os.remove(tmp)
  if ok == 0 or ok == true then
    return true
  end
  -- If validation fails, treat as schema error; otherwise fallback
  if ok == 256 or ok == false then
    return false, { "python_validator_failed" }
  end
  return nil, "python_validator_unavailable"
end

return Schema
