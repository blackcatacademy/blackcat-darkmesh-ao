-- Minimal JWT HS256 verifier (no clock skew handling).
local crypto_ok, crypto = pcall(require, "ao.shared.crypto")
local ok_mime, mime = pcall(require, "mime")
local ok_json, cjson = pcall(require, "cjson.safe")

local Jwt = {}

local function b64url_decode(input)
  input = input:gsub("-", "+"):gsub("_", "/")
  local pad = #input % 4
  if pad > 0 then
    input = input .. string.rep("=", 4 - pad)
  end
  if ok_mime and mime.unb64 then
    return mime.unb64(input)
  end
  return nil
end

function Jwt.verify_hs256(token, secret)
  if not token or token == "" or not secret then
    return false, "missing_token"
  end
  if not crypto_ok or not crypto.hmac_sha256_hex then
    return false, "crypto_missing"
  end
  local header_b64, payload_b64, sig_b64 = token:match "([^%.]+)%.([^%.]+)%.([^%.]+)"
  if not (header_b64 and payload_b64 and sig_b64) then
    return false, "invalid_format"
  end
  local signed = header_b64 .. "." .. payload_b64
  local signature = b64url_decode(sig_b64)
  if not signature then
    return false, "bad_signature_b64"
  end
  local expected_hex = crypto.hmac_sha256_hex(signed, secret)
  local expected = expected_hex
    and expected_hex:gsub("%x%x", function(x)
      return string.char(tonumber(x, 16))
    end)
  if not expected or expected ~= signature then
    return false, "signature_mismatch"
  end
  if not ok_json then
    return false, "json_missing"
  end
  local ok_h = pcall(cjson.decode, b64url_decode(header_b64) or "")
  local ok_p, payload = pcall(cjson.decode, b64url_decode(payload_b64) or "")
  if not (ok_h and ok_p) then
    return false, "decode_failed"
  end
  return true, payload
end

return Jwt
