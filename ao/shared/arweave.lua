-- Minimal Arweave adapter (stub) for publish flow.
-- Provides in-memory manifest storage and a deterministic checksum.

local Ar = {}

local counter = 0
local manifests = {}

local function next_tx()
  counter = counter + 1
  return string.format("mock-tx-%06d", counter)
end

local function checksum(str)
  local sum = 0
  for i = 1, #str do
    sum = (sum + string.byte(str, i)) % 0xFFFFFFFF
  end
  return string.format("%08x", sum)
end

-- Stores a snapshot payload and returns a manifest transaction id and hash.
function Ar.put_snapshot(payload)
  local tx = next_tx()
  local serialized = type(payload) == "string" and payload or tostring(payload)
  local hash = checksum(serialized)
  manifests[tx] = {
    payload = payload,
    hash = hash,
    storedAt = os.date("!%Y-%m-%dT%H:%M:%SZ"),
  }
  return tx, hash
end

function Ar.get_snapshot(tx)
  return manifests[tx]
end

function Ar.verify_snapshot(tx, expected_hash)
  local m = manifests[tx]
  if not m then return false, "not_found" end
  if m.hash ~= expected_hash then return false, "hash_mismatch" end
  return true
end

-- Expose for tests
Ar._manifests = manifests

return Ar
