-- Load and verify a trust manifest from Arweave (or local file) and print active resolvers.
-- Usage:
--   TRUST_MANIFEST_HMAC=secret TRUST_MANIFEST_TX=<txid> lua scripts/verify/trust_manifest_loader.lua
--   TRUST_MANIFEST_HMAC=secret lua scripts/verify/trust_manifest_loader.lua path/to/manifest.signed.json

local secret = os.getenv("TRUST_MANIFEST_HMAC")
if not secret or secret == "" then
  io.stderr:write("TRUST_MANIFEST_HMAC not set\n")
  os.exit(1)
end

local ok_json, cjson = pcall(require, "cjson.safe")
local ok_crypto, crypto = pcall(require, "ao.shared.crypto")
if not (ok_json and ok_crypto) then
  io.stderr:write("missing deps: cjson.safe or ao.shared.crypto\n")
  os.exit(1)
end

local function fetch_arweave(tx)
  local endpoint = os.getenv("ARWEAVE_HTTP_ENDPOINT") or "https://arweave.net"
  local url = string.format("%s/%s", endpoint, tx)
  local cmd = string.format("curl -sS %q", url)
  local fh = io.popen(cmd)
  if not fh then return nil, "curl_failed" end
  local body = fh:read("*a")
  fh:close()
  return body
end

local function load_manifest()
  local tx = os.getenv("TRUST_MANIFEST_TX")
  local path = arg[1]
  local raw
  if path and path ~= "" then
    local f = assert(io.open(path, "r"))
    raw = f:read("*a")
    f:close()
  elseif tx and tx ~= "" then
    raw = fetch_arweave(tx)
    if not raw then
      io.stderr:write("failed to fetch arweave tx\n")
      os.exit(1)
    end
  else
    io.stderr:write("provide TRUST_MANIFEST_TX or path\n")
    os.exit(1)
  end
  return raw
end

local raw = load_manifest()
local signed, err = cjson.decode(raw)
if not signed then
  io.stderr:write("decode failed: " .. tostring(err) .. "\n")
  os.exit(1)
end

local manifest_json = cjson.encode(signed.manifest)
local expected = crypto.hmac_sha256_hex(manifest_json, secret)
if not expected then
  io.stderr:write("crypto backend missing\n")
  os.exit(1)
end

if expected:lower() ~= tostring(signed.signature or ""):lower() then
  io.stderr:write("signature mismatch\n")
  os.exit(2)
end

local now = os.time()
local active = {}
for _, r in ipairs(signed.manifest.resolvers or {}) do
  local vf = r.validFrom or 0
  local vt = r.validTo or (now + 31536000)
  if (vf == 0 or vf <= now) and (vt == 0 or vt >= now) and (r.status or "active") == "active" then
    table.insert(active, r)
  end
end

print(string.format("OK: signer=%s signed_at=%s resolvers=%d active=%d", signed.signer or "unknown", signed.signed_at or "?", #(signed.manifest.resolvers or {}), #active))
for _, r in ipairs(active) do
  print(string.format("- id=%s endpoint=%s pubkey=%s validTo=%s", r.id or "?", r.endpoint or "n/a", (r.pubkey or ""):sub(1,16) .. "...", r.validTo or ""))
end
