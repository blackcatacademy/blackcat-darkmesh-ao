-- Simple in-memory idempotency registry for handler scaffolding.
-- Stores response per Request-Id within process lifetime.

local Idem = {}
local seen = {}

function Idem.check(request_id)
  return seen[request_id]
end

function Idem.record(request_id, response)
  seen[request_id] = response
end

-- reset for tests if needed
function Idem._clear()
  seen = {}
end

return Idem
