-- Shared auth utilities: signature verification and role checks.
-- AO environment is expected to verify signatures; here we keep role/allowlist helpers.

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
local openssl_ok, openssl = pcall(require, "openssl")
local sodium_ok, sodium = pcall(require, "sodium")
local ed25519_ok, ed25519 = pcall(require, "ed25519") -- pure-lua (MIT) if installed
local sqlite_ok, sqlite = pcall(require, "lsqlite3")
local SHELL_FALLBACK = os.getenv("AUTH_ALLOW_SHELL_FALLBACK") == "1" -- default now off

local nonce_store = {}
local rate_store = {}
local rate_db_loaded = false

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
      local raw_sig = sodium.from_hex(sig)
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

-- Combined security gate used by routes
function Auth.enforce(msg)
  local ok_nonce, err_nonce = Auth.require_nonce(msg)
  if not ok_nonce then return false, err_nonce end
  local ok_sig, err_sig = Auth.require_signature(msg)
  if not ok_sig then return false, err_sig end
  local ok_rl, err_rl = Auth.check_rate_limit(msg)
  if not ok_rl then return false, err_rl end
  return true
end

return Auth
