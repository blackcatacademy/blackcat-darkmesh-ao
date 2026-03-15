-- Shared auth utilities: signature verification and role checks.
-- AO environment is expected to verify signatures; here we keep role/allowlist helpers.

local jwt_ok, jwt = pcall(require, "ao.shared.jwt")

local Auth = {}
local os_time = os.time

local NONCE_TTL = tonumber(os.getenv("AUTH_NONCE_TTL_SECONDS") or "300")
local NONCE_MAX = tonumber(os.getenv("AUTH_NONCE_MAX_ENTRIES") or "2048")
local REQUIRE_NONCE = os.getenv("AUTH_REQUIRE_NONCE") == "1"
local REQUIRE_SIGNATURE = os.getenv("AUTH_REQUIRE_SIGNATURE") == "1"
local RL_WINDOW = tonumber(os.getenv("AUTH_RATE_LIMIT_WINDOW_SECONDS") or "60")
local RL_MAX = tonumber(os.getenv("AUTH_RATE_LIMIT_MAX_REQUESTS") or "200")
local RL_STATE_FILE = os.getenv("AUTH_RATE_LIMIT_FILE")
local RL_SQLITE = os.getenv("AUTH_RATE_LIMIT_SQLITE")
local SIG_SECRET = os.getenv("AUTH_SIGNATURE_SECRET")
local SIG_PUBLIC = os.getenv("AUTH_SIGNATURE_PUBLIC")
local SIG_TYPE = os.getenv("AUTH_SIGNATURE_TYPE") or "hmac" -- hmac | ed25519
local JWT_SECRET = os.getenv("AUTH_JWT_HS_SECRET")
local REQUIRE_JWT = os.getenv("AUTH_REQUIRE_JWT") == "1"
local DEVICE_TOKEN = os.getenv("AUTH_DEVICE_TOKEN")
local REQUIRE_DEVICE = os.getenv("AUTH_REQUIRE_DEVICE_TOKEN") == "1"
local openssl_ok, openssl = pcall(require, "openssl")
local sodium_ok, sodium = pcall(require, "sodium")
if not sodium_ok then
  sodium_ok, sodium = pcall(require, "luasodium")
end
local ed25519_ok, ed25519 = pcall(require, "ed25519") -- pure-lua (MIT) if installed
local sqlite_ok, sqlite = pcall(require, "lsqlite3")
local SHELL_FALLBACK = os.getenv("AUTH_ALLOW_SHELL_FALLBACK") == "1" -- default now off
local json_ok, json = pcall(require, "cjson.safe")
local FLAGS_FILE = os.getenv("AUTH_RESOLVER_FLAGS_FILE") or os.getenv("AO_FLAGS_PATH")

local nonce_store = {}
local rate_store = {}
local rate_db_loaded = false
local resolver_flags = {}

-- load persisted rate store (simple CSV key,count,reset)
if RL_STATE_FILE then
  local f = io.open(RL_STATE_FILE, "r")
  if f then
    for line in f:lines() do
      local key, count, reset = line:match("^([^,]+),(%d+),(%d+)")
      if key and count and reset then
        rate_store[key] = { count = tonumber(count), reset = tonumber(reset) }
      end
    end
    f:close()
  end
end

local function contains(list, value)
  for _, v in ipairs(list) do
    if v == value then
      return true
    end
  end
  return false
end

local function extract_bearer(msg)
  if msg.jwt then return msg.jwt end
  if msg.JWT then return msg.JWT end
  if msg.token then return msg.token end
  local authz = msg.Authorization or msg.authorization or msg.auth
  if authz and type(authz) == "string" then
    return (authz:gsub("^%s*[Bb]earer%s+", ""))
  end
end

function Auth.consume_jwt(msg)
  if not JWT_SECRET or JWT_SECRET == "" then return true end
  if not jwt_ok then return not REQUIRE_JWT, "jwt_module_missing" end
  local token = extract_bearer(msg)
  if (not token or token == "") then
    if REQUIRE_JWT then return false, "missing_jwt" end
    return true
  end
  local ok, claims = jwt.verify_hs256(token, JWT_SECRET)
  if not ok then return false, claims or "jwt_invalid" end
  if claims.exp and os_time() > claims.exp then
    return false, "jwt_expired"
  end
  msg["Actor-Id"] = msg["Actor-Id"] or claims.sub or claims.actor
  msg["Actor-Role"] = msg["Actor-Role"] or claims.role
  msg["Tenant"] = msg["Tenant"] or claims.tenant
  msg.Nonce = msg.Nonce or claims.nonce
  msg.jwt_claims = claims
  return true
end

-- Accepts either dash or camel case field names for flexibility with gateways.
local function extract_role(msg)
  return msg["Actor-Role"] or msg.actorRole or msg.role
end

local function prune_nonces()
  local now = os_time()
  local count = 0
  for k, v in pairs(nonce_store) do
    if v < now then
      nonce_store[k] = nil
    else
      count = count + 1
    end
  end
  if count > NONCE_MAX then
    -- drop oldest
    local oldest_key, oldest_val
    for k, v in pairs(nonce_store) do
      if not oldest_val or v < oldest_val then
        oldest_val = v
        oldest_key = k
      end
    end
    if oldest_key then nonce_store[oldest_key] = nil end
  end
end

function Auth.require_nonce(msg)
  prune_nonces()
  local nonce = msg.Nonce or msg.nonce
  if not nonce then
    if REQUIRE_NONCE then
      return false, "missing_nonce"
    end
    return true
  end
  if nonce_store[nonce] then
    return false, "replay_nonce"
  end
  nonce_store[nonce] = os_time() + NONCE_TTL
  prune_nonces()
  return true
end

function Auth.require_signature(msg)
  local sig = msg.Signature or msg.signature or msg["Signature-Ref"]
  if not sig then
    if REQUIRE_SIGNATURE then
      return false, "missing_signature"
    end
    return true
  end
  local target = (msg.Action or "") .. "|" .. (msg["Site-Id"] or "") .. "|" .. (msg["Request-Id"] or "")
  if SIG_TYPE == "ed25519" and SIG_PUBLIC then
    if ed25519_ok and ed25519.verify then
      local pub = assert(io.open(SIG_PUBLIC, "rb")):read("*a")
      local raw_sig = ed25519.fromhex and ed25519.fromhex(sig) or sig
      if raw_sig and ed25519.verify(raw_sig, target, pub) then
        return true
      end
    end
    -- Prefer libsodium for detached ed25519 verification (hex signature expected)
    if sodium_ok and sodium.crypto_sign_verify_detached then
      local pub = assert(io.open(SIG_PUBLIC, "rb")):read("*a")
      local raw_sig
      if sodium.from_hex then
        raw_sig = sodium.from_hex(sig)
      else
        -- manual hex decode fallback
        local bytes = {}
        for byte in sig:gmatch("%x%x") do bytes[#bytes + 1] = string.char(tonumber(byte, 16)) end
        raw_sig = table.concat(bytes)
      end
      if raw_sig and sodium.crypto_sign_verify_detached(raw_sig, target, pub) then
        return true
      end
    end
    -- Try luaossl
    if openssl_ok and openssl.pkey and openssl.hex then
      local pub_pem = assert(io.open(SIG_PUBLIC, "r")):read("*a")
      local pkey = openssl.pkey.read(pub_pem, true, "public")
      local raw_sig = openssl.hex(sig)
      local ok, _ = pkey:verify(raw_sig, target, "NONE")
      if ok then return true end
    end
    if not SHELL_FALLBACK then
      return false, "bad_signature"
    end
    -- Fallback shell
    local tmp = os.tmpname()
    local f = io.open(tmp, "w"); if f then f:write(target); f:close() end
    local cmd = string.format("openssl pkeyutl -verify -pubin -inkey %q -rawin -in %q -sigfile %q 2>/dev/null", SIG_PUBLIC, tmp, tmp .. ".sig")
    local sf = io.open(tmp .. ".sig", "w")
    if sf then
      sf:write(sig)
      sf:close()
    end
    local ok = os.execute(cmd)
    os.remove(tmp); os.remove(tmp .. ".sig")
    if ok == true or ok == 0 then return true end
    return false, "bad_signature"
  else
    if not SIG_SECRET then
      return not REQUIRE_SIGNATURE, REQUIRE_SIGNATURE and "missing_signature_secret" or nil
    end
    if openssl_ok and openssl.hmac then
      local raw = openssl.hmac.digest("sha256", target, SIG_SECRET, true)
      if not raw then return false, "sig_verify_failed" end
      local hex = (openssl.hex and openssl.hex(raw)) or raw:gsub(".", function(c) return string.format("%02x", string.byte(c)) end)
      if hex:lower() ~= tostring(sig):lower() then
        return false, "bad_signature"
      end
      return true
    elseif sodium_ok and sodium.crypto_auth then
      local tag = sodium.crypto_auth(target, SIG_SECRET)
      local hex = sodium.to_hex(tag)
      if hex:lower() ~= tostring(sig):lower() then
        return false, "bad_signature"
      end
      return true
    else
      return false, "sig_verify_failed"
    end
  end
end

function Auth.verify_outbox_hmac(msg)
  local secret = os.getenv("OUTBOX_HMAC_SECRET")
  if not secret or secret == "" then return true end
  local provided = msg.hmac or msg.Hmac
  if not provided then return false, "missing_outbox_hmac" end
  local crypto_ok, crypto = pcall(require, "ao.shared.crypto")
  if not crypto_ok then return false, "crypto_missing" end
  local payload = (msg["Site-Id"] or "") .. "|" .. (msg["Page-Id"] or msg["Order-Id"] or "") .. "|" .. (msg.Version or msg["Manifest-Tx"] or msg.Amount or "")
  local expected = crypto.hmac_sha256_hex(payload, secret)
  if not expected or expected:lower() ~= tostring(provided):lower() then
    return false, "outbox_hmac_mismatch"
  end
  return true
end

local function rate_key(msg)
  local site = msg["Site-Id"] or "global"
  local actor = msg.Subject or msg["Actor-Id"] or msg["Actor-Role"] or "anon"
  return site .. ":" .. actor
end

local function prune_rate()
  local now = os_time()
  for k, v in pairs(rate_store) do
    if v.reset < now then
      rate_store[k] = nil
    end
  end
end

local function load_rate_store_sqlite()
  if not RL_SQLITE or not sqlite_ok or rate_db_loaded then return end
  Auth._db = sqlite.open(RL_SQLITE)
  Auth._db:exec("CREATE TABLE IF NOT EXISTS rate (k TEXT PRIMARY KEY, count INT, reset INT)")
  for row in Auth._db:nrows("SELECT k,count,reset FROM rate") do
    rate_store[row.k] = { count = tonumber(row.count) or 0, reset = tonumber(row.reset) or os_time() }
  end
  rate_db_loaded = true
end

function Auth.check_rate_limit(msg)
  load_rate_store_sqlite()
  prune_rate()
  local key = rate_key(msg)
  local now = os_time()
  local bucket = rate_store[key] or { count = 0, reset = now + RL_WINDOW }
  bucket.count = bucket.count + 1
  if bucket.reset < now then
    bucket.count = 1
    bucket.reset = now + RL_WINDOW
  end
  rate_store[key] = bucket
  if bucket.count > RL_MAX then
    return false, "rate_limited"
  end
  if RL_SQLITE and sqlite_ok then
    if not Auth._db then
      Auth._db = sqlite.open(RL_SQLITE)
      Auth._db:exec("CREATE TABLE IF NOT EXISTS rate (k TEXT PRIMARY KEY, count INT, reset INT)")
    end
    local stmt = Auth._db:prepare("INSERT OR REPLACE INTO rate (k,count,reset) VALUES (?, ?, ?)")
    stmt:bind_values(key, bucket.count, bucket.reset)
    stmt:step()
    stmt:finalize()
  elseif RL_STATE_FILE then
    local f = io.open(RL_STATE_FILE, "w")
    if f then
      for rk, rv in pairs(rate_store) do
        f:write(string.format("%s,%d,%d\n", rk, rv.count, rv.reset))
      end
      f:close()
    end
  end
  return true
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

-- Convenience: pick allowlist by action map { action = {roles...} }
function Auth.require_role_for_action(msg, policy_table)
  local roles = policy_table[msg.Action]
  if not roles then
    return true
  end
  return Auth.require_role(msg, roles)
end

local function load_resolver_flags()
  if not FLAGS_FILE or FLAGS_FILE == "" or not json_ok then return end
  local f = io.open(FLAGS_FILE, "r")
  if not f then return end
  local tmp = {}
  for line in f:lines() do
    local obj = json.decode(line)
    if obj and obj.resolverId and obj.flag then
      tmp[obj.resolverId] = obj
    end
  end
  f:close()
  resolver_flags = tmp
end

local function check_resolver_flag(msg)
  if not FLAGS_FILE then return true end
  local rid = msg["Resolver-Id"] or msg.ResolverId or msg.resolverId or msg.resolver
  if not rid then return true end
  load_resolver_flags()
  local entry = resolver_flags[rid]
  if not entry then return true end
  if entry.flag == "blocked" then
    return false, "resolver_blocked"
  elseif entry.flag == "suspicious" then
    local action = msg.Action or ""
    if action:match("^[Gg]et") or action:match("^[Ll]ist") then
      return true
    end
    return false, "resolver_suspicious_readonly"
  end
  return true
end

local function require_device_token(msg)
  local token = msg["Device-Token"] or msg.deviceToken or msg.device_token or msg.device
  if not token or token == "" then
    if REQUIRE_DEVICE then return false, "missing_device_token" end
    return true
  end
  if DEVICE_TOKEN and DEVICE_TOKEN ~= "" then
    if token ~= DEVICE_TOKEN then
      return false, "device_token_mismatch"
    end
  end
  return true
end

-- Combined security gate used by routes
function Auth.enforce(msg)
  local ok_jwt, err_jwt = Auth.consume_jwt(msg)
  if not ok_jwt then return false, err_jwt end
  local ok_nonce, err_nonce = Auth.require_nonce(msg)
  if not ok_nonce then return false, err_nonce end
  local ok_sig, err_sig = Auth.require_signature(msg)
  if not ok_sig then return false, err_sig end
  local ok_flag, err_flag = check_resolver_flag(msg)
  if not ok_flag then return false, err_flag end
  local ok_dev, err_dev = require_device_token(msg)
  if not ok_dev then return false, err_dev end
  local ok_rl, err_rl = Auth.check_rate_limit(msg)
  if not ok_rl then return false, err_rl end
  return true
end

return Auth
