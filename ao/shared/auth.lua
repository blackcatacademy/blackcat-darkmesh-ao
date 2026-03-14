-- Shared auth utilities: signature verification and role checks.
-- AO environment is expected to verify signatures; here we keep role/allowlist helpers.

local Auth = {}

local function contains(list, value)
  for _, v in ipairs(list) do
    if v == value then
      return true
    end
  end
  return false
end

-- Accepts either dash or camel case field names for flexibility with gateways.
local function extract_role(msg)
  return msg["Actor-Role"] or msg.actorRole or msg.role
end

function Auth.require_role(msg, allowed_roles)
  if not allowed_roles or #allowed_roles == 0 then
    return true
  end
  local role = extract_role(msg)
  if not role then
    return false, "missing_role"
  end
  if not contains(allowed_roles, role) then
    return false, "forbidden_role"
  end
  return true
end

return Auth
