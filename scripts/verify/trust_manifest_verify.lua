-- Verify a resolver trust manifest signed with HMAC-SHA256.
-- Usage:
--   TRUST_MANIFEST_HMAC=secret lua scripts/verify/trust_manifest_verify.lua manifest.signed.json
--
-- manifest.signed.json format (produced by write/scripts/cli/trust_manifest_sign.lua):
-- {
--   "manifest": { "version": 1, "resolvers": [ ... ] },
--   "signature": "<hex>",
--   "signed_at": 1700000000,
--   "signer": "admin@example"
-- }

local secret = os.getenv("TRUST_MANIFEST_HMAC")
if not secret or secret == "" then
  io.stderr:write("TRUST_MANIFEST_HMAC not set\n")
  os.exit(1)
end

local path = arg[1]
if not path then
  io.stderr:write("usage: lua trust_manifest_verify.lua manifest.signed.json\n")
  os.exit(1)
end

local ok_json, cjson = pcall(require, "cjson.safe")
local ok_crypto, crypto = pcall(require, "ao.shared.crypto")
if not (ok_json and ok_crypto) then
  io.stderr:write("missing deps: cjson.safe or ao.shared.crypto\n")
  os.exit(1)
end

local f = assert(io.open(path, "r"))
local raw = f:read("*a")
f:close()

local signed, err = cjson.decode(raw)
if not signed then
  io.stderr:write("decode failed: " .. tostring(err) .. "\n")
  os.exit(1)
end

if type(signed) ~= "table" or not signed.manifest or not signed.signature then
  io.stderr:write("invalid signed manifest structure\n")
  os.exit(1)
end

local manifest_json = cjson.encode(signed.manifest)
local expected = crypto.hmac_sha256_hex(manifest_json, secret)
if not expected then
  io.stderr:write("crypto backend missing\n")
  os.exit(1)
end

if expected:lower() ~= tostring(signed.signature):lower() then
  io.stderr:write("signature mismatch\n")
  os.exit(2)
end

print(string.format("OK: signer=%s signed_at=%s resolvers=%d", signed.signer or "unknown", signed.signed_at or "?", #(signed.manifest.resolvers or {})))
