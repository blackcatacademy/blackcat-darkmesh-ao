-- Minimal Arweave adapter (stub) for publish flow.
-- Provides in-memory manifest storage for local testing.

local Ar = {}

local counter = 0
local manifests = {}

local function next_tx()
  counter = counter + 1
  return string.format("mock-tx-%06d", counter)
end

-- Stores a snapshot payload and returns a manifest transaction id.
function Ar.put_snapshot(payload)
  local tx = next_tx()
  manifests[tx] = {
    payload = payload,
    storedAt = os.date("!%Y-%m-%dT%H:%M:%SZ"),
  }
  return tx
end

function Ar.get_snapshot(tx)
  return manifests[tx]
end

-- Expose for tests
Ar._manifests = manifests

return Ar
