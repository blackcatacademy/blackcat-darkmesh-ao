-- Shared codecs and response normalization.

local Codec = {}

function Codec.ok(payload)
  return {
    status = "OK",
    payload = payload or {},
  }
end

function Codec.error(code, message, meta)
  return {
    status = "ERROR",
    code = code,
    message = message,
    meta = meta,
  }
end

function Codec.missing_tags(missing)
  return Codec.error("MISSING_TAGS", "Required tags are missing", { missing = missing })
end

function Codec.unknown_action(action)
  return Codec.error("UNKNOWN_ACTION", "Unsupported action", { action = action })
end

function Codec.not_implemented(action)
  return Codec.error("NOT_IMPLEMENTED", "Handler not implemented", { action = action })
end

return Codec
