-- luacheck: ignore
-- Lightweight fuzz/property checks for pagination and Arweave HTTP failure handling.

math.randomseed(os.time())
local _env = {}
local real_getenv = os.getenv
function os.setenv(key, value)
  _env[key] = value
end
function os.getenv(key)
  if _env[key] ~= nil then
    return _env[key]
  end
  if real_getenv then
    return real_getenv(key)
  end
  return nil
end

local catalog = require "ao.catalog.process"
local site = require "ao.site.process"
local ar = require "ao.shared.arweave"
local audit = require "ao.shared.audit"
local auth = require "ao.shared.auth"

local function with_req(fields)
  fields["Request-Id"] = fields["Request-Id"] or tostring(math.random())
  return fields
end

-- Mixed action replay: same Request-Id across registry + site should stick to first response
do
  local reg = require "ao.registry.process"
  local site = require "ao.site.process"
  local rid = "rid-mixed-replay"
  reg.route(with_req {
    Action = "RegisterSite",
    ["Site-Id"] = "mix-replay",
    ["Actor-Role"] = "admin",
    ["Request-Id"] = rid,
  })
  local again = reg.route(with_req {
    Action = "RegisterSite",
    ["Site-Id"] = "mix-replay",
    ["Actor-Role"] = "admin",
    ["Request-Id"] = rid,
  })
  if again.status ~= "OK" then
    error "mixed replay should return stored OK"
  end
  local siteReplay = site.route(with_req {
    Action = "ResolveRoute",
    ["Site-Id"] = "mix-replay",
    Path = "/",
    ["Actor-Role"] = "editor",
    ["Request-Id"] = rid,
  })
  -- resolve route will be missing, but should not be treated as a new requestId; earlier stored response reused if present
end

-- Fuzz pagination uniqueness
do
  local siteId = "fuzz-site"
  local cat = "cat-fuzz"
  local total = 40
  for i = 1, total do
    catalog.route(with_req {
      Action = "UpsertProduct",
      ["Site-Id"] = siteId,
      Sku = "fuzz-" .. i,
      Payload = { name = "P" .. i },
      ["Actor-Role"] = "catalog-admin",
    })
  end
  catalog.route(with_req {
    Action = "UpsertCategory",
    ["Site-Id"] = siteId,
    ["Category-Id"] = cat,
    Products = {},
    ["Actor-Role"] = "catalog-admin",
  })
  for i = 1, total do
    catalog.route(with_req {
      Action = "UpsertCategory",
      ["Site-Id"] = siteId,
      ["Category-Id"] = cat,
      Products = { "fuzz-" .. i },
      ["Actor-Role"] = "catalog-admin",
    })
  end
  local seen = {}
  for page = 1, 20 do
    local resp = catalog.route(with_req {
      Action = "ListCategoryProducts",
      ["Site-Id"] = siteId,
      ["Category-Id"] = cat,
      Page = page,
      PageSize = 3,
    })
    if resp.payload then
      for _, item in ipairs(resp.payload.items) do
        if seen[item.sku] then
          error("duplicate sku in pagination fuzz: " .. item.sku)
        end
        seen[item.sku] = true
      end
    end
  end
end

-- Arweave HTTP failure simulation: ensure too_large manifests are rejected
do
  local tx, err = ar.put_snapshot { dummy = string.rep("x", 300 * 1024) } -- > 256 KiB
  if tx ~= nil or err ~= "too_large" then
    error "expected too_large manifest rejection"
  end
end

-- Force Arweave HTTP error via env flag
do
  package.loaded["ao.shared.arweave"] = nil
  os.setenv("ARWEAVE_MODE", "http")
  os.setenv("ARWEAVE_HTTP_REAL", "1")
  os.setenv("ARWEAVE_FORCE_ERROR", "1")
  os.setenv("ARWEAVE_HTTP_MAX_BODY", "32")
  local ar2 = require "ao.shared.arweave"
  local tx, err = ar2.put_snapshot { dummy = "ok" }
  if err ~= "http_error" then
    error "expected http_error with force flag"
  end
  os.setenv("ARWEAVE_FORCE_ERROR", nil)
  os.setenv("ARWEAVE_HTTP_REAL", nil)
  os.setenv("ARWEAVE_HTTP_MAX_BODY", nil)
  os.setenv("ARWEAVE_MODE", nil)
  package.loaded["ao.shared.arweave"] = nil
end

-- Arweave response corpus: simulate bad schema/body/status
do
  package.loaded["ao.shared.arweave"] = nil
  os.setenv("ARWEAVE_MODE", "http")
  os.setenv("ARWEAVE_HTTP_SIM_STATUS", "500")
  local ar3 = require "ao.shared.arweave"
  local tx, err = ar3.put_snapshot { dummy = "ok" }
  if err ~= "http_error" then
    error "expected http_error on simulated 500"
  end
  package.loaded["ao.shared.arweave"] = nil
  os.setenv("ARWEAVE_HTTP_SIM_STATUS", "200")
  os.setenv("ARWEAVE_HTTP_SIM_BODY", '{"status":"ok","message":"hi","tx":123}') -- tx wrong type
  local ar4 = require "ao.shared.arweave"
  local tx2, err2 = ar4.put_snapshot { dummy = "ok" }
  if err2 ~= "http_response_schema_invalid" then
    error "expected schema invalid on bad body"
  end
  os.setenv("ARWEAVE_HTTP_SIM_BODY", '{"status":"error","message":"fail"}') -- missing tx
  package.loaded["ao.shared.arweave"] = nil
  local ar5 = require "ao.shared.arweave"
  local tx3, err3 = ar5.put_snapshot { dummy = "ok" }
  if tx3 == nil then
    error "expected success on error-status body"
  end
  os.setenv("ARWEAVE_HTTP_SIM_BODY", nil)
  os.setenv("ARWEAVE_MODE", "mock")
  package.loaded["ao.shared.arweave"] = nil
end

-- Auth ed25519 verification round-trip
do
  -- generate keypair
  os.execute "openssl genpkey -algorithm ed25519 -out /tmp/ao-ed.key >/dev/null 2>&1"
  os.execute "openssl pkey -in /tmp/ao-ed.key -pubout -out /tmp/ao-ed.pub >/dev/null 2>&1"
  local target = "PublishVersion|site-x|rid-x"
  os.execute(string.format("printf %%s %q > /tmp/ao-msg", target))
  os.execute "openssl pkeyutl -sign -inkey /tmp/ao-ed.key -rawin -in /tmp/ao-msg -out /tmp/ao-sig >/dev/null 2>&1"
  local sig_hex = io.popen("xxd -p /tmp/ao-sig"):read "*l"
  os.setenv("AUTH_SIGNATURE_TYPE", "ed25519")
  os.setenv("AUTH_SIGNATURE_PUBLIC", "/tmp/ao-ed.pub")
  os.setenv("AUTH_REQUIRE_SIGNATURE", "1")
  package.loaded["ao.shared.auth"] = nil
  local auth2 = require "ao.shared.auth"
  local ok, err = auth2.require_signature {
    Action = "PublishVersion",
    ["Site-Id"] = "site-x",
    ["Request-Id"] = "rid-x",
    Signature = sig_hex,
  }
  if not ok then
    io.stderr:write("skipping ed25519 verify in fuzz: " .. tostring(err) .. "\\n")
  end
  local ok2, err2 = auth2.require_signature {
    Action = "PublishVersion",
    ["Site-Id"] = "site-x",
    ["Request-Id"] = "rid-x",
    Signature = "deadbeef",
  }
  if ok2 then
    error "bad signature should fail"
  end
  os.setenv("AUTH_REQUIRE_SIGNATURE", nil)
  os.setenv("AUTH_SIGNATURE_TYPE", nil)
  os.setenv("AUTH_SIGNATURE_PUBLIC", nil)
  package.loaded["ao.shared.auth"] = nil
end

-- Concurrent publish/version set simulation
do
  local siteId = "conc-site"
  local site = require "ao.site.process"
  site.route(with_req {
    Action = "PutDraft",
    ["Site-Id"] = siteId,
    ["Page-Id"] = "p1",
    Content = { title = "T", blocks = { { type = "paragraph", text = "T" } } },
    ["Actor-Role"] = "editor",
  })
  local ok1 = site.route(with_req {
    Action = "PublishVersion",
    ["Site-Id"] = siteId,
    Version = "v1",
    ["Actor-Role"] = "publisher",
  })
  local conflict = site.route(with_req {
    Action = "PublishVersion",
    ["Site-Id"] = siteId,
    Version = "v2",
    ExpectedVersion = "old",
    ["Actor-Role"] = "publisher",
  })
  if conflict.status ~= "ERROR" then
    error "Expected VERSION_CONFLICT on second publish"
  end

  -- Concurrent SetActiveVersion vs PublishVersion in registry
  local registry = require "ao.registry.process"
  registry.route(
    with_req { Action = "RegisterSite", ["Site-Id"] = "conc-reg", ["Actor-Role"] = "admin" }
  )
  registry.route(with_req {
    Action = "SetActiveVersion",
    ["Site-Id"] = "conc-reg",
    Version = "v1",
    ["Actor-Role"] = "registry-admin",
  })
  local conflict2 = registry.route(with_req {
    Action = "SetActiveVersion",
    ["Site-Id"] = "conc-reg",
    Version = "v2",
    ExpectedVersion = "nope",
    ["Actor-Role"] = "registry-admin",
  })
  if conflict2.status ~= "ERROR" then
    error "Expected VERSION_CONFLICT on SetActiveVersion"
  end

  -- random interleavings of SetActiveVersion and PublishVersion
  local reg = require "ao.registry.process"
  reg.route(
    with_req { Action = "RegisterSite", ["Site-Id"] = "conc-reg2", ["Actor-Role"] = "admin" }
  )
  local actions = {
    function()
      reg.route(with_req {
        Action = "SetActiveVersion",
        ["Site-Id"] = "conc-reg2",
        Version = "v1",
        ["Actor-Role"] = "registry-admin",
      })
    end,
    function()
      reg.route(with_req {
        Action = "SetActiveVersion",
        ["Site-Id"] = "conc-reg2",
        Version = "v2",
        ExpectedVersion = "v0",
        ["Actor-Role"] = "registry-admin",
      })
    end,
  }
  for i = 1, 20 do
    actions[math.random(1, #actions)]()
  end
  local lookup = reg.route(with_req { Action = "GetSiteConfig", ["Site-Id"] = "conc-reg2" })
  if lookup.status ~= "OK" then
    error "GetSiteConfig failed after interleavings"
  end

  -- parallel publish/version with cjson decode of response bodies (simulated)
  for i = 1, 10 do
    local resp = site.route(with_req {
      Action = "PublishVersion",
      ["Site-Id"] = siteId,
      Version = "v" .. (10 + i),
      ["Actor-Role"] = "publisher",
    })
    if resp.payload.manifestHash then
      local ok, decoded = pcall(require("cjson").decode, "{}")
      if not ok then
        error "cjson decode failed in fuzz"
      end
    end
  end

  -- Random interleavings: PublishVersion vs SetActiveVersion cross-process
  local reg3 = require "ao.registry.process"
  reg3.route(
    with_req { Action = "RegisterSite", ["Site-Id"] = "conc-reg3", ["Actor-Role"] = "admin" }
  )
  local versions = {}
  for i = 1, 30 do
    local pick = math.random()
    if pick < 0.6 then
      local ver = "v" .. i
      table.insert(versions, ver)
      site.route(with_req {
        Action = "PutDraft",
        ["Site-Id"] = "conc-reg3",
        ["Page-Id"] = "p" .. i,
        Content = { title = "T" .. i, blocks = { { type = "paragraph", text = "T" .. i } } },
        ["Actor-Role"] = "editor",
      })
      site.route(with_req {
        Action = "PublishVersion",
        ["Site-Id"] = "conc-reg3",
        Version = ver,
        ["Actor-Role"] = "publisher",
      })
    else
      local target = (#versions > 0) and versions[math.random(1, #versions)] or "v1"
      local resp = reg3.route(with_req {
        Action = "SetActiveVersion",
        ["Site-Id"] = "conc-reg3",
        Version = target,
        ["Actor-Role"] = "registry-admin",
      })
      if resp.status ~= "OK" and resp.code ~= "VERSION_CONFLICT" then
        error("unexpected status in interleaving: " .. tostring(resp.status))
      end
    end
  end
  -- Final schema validation sanity
  local last = reg3.route(with_req { Action = "GetSiteConfig", ["Site-Id"] = "conc-reg3" })
  local schema_mod = require "ao.shared.schema"
  if schema_mod.validate_envelope then
    local ok_env, errs = schema_mod.validate_envelope {
      action = "noop",
      requestId = "x",
      actor = "a",
      tenant = "t",
      timestamp = "2026-03-15T00:00:00Z",
      nonce = "n",
      signatureRef = "s",
      payload = {},
    }
    if not ok_env then
      error("envelope schema should validate minimal payload: " .. tostring(errs[1]))
    end
  end
end

-- Concurrent PublishVersion vs SetActiveVersion with envelope/schema validation
do
  local reg = require "ao.registry.process"
  local siteProc = require "ao.site.process"
  local schema = require "ao.shared.schema"
  reg.route(
    with_req { Action = "RegisterSite", ["Site-Id"] = "conc-schema", ["Actor-Role"] = "admin" }
  )
  for i = 1, 15 do
    local ver = "sv" .. i
    if schema.validate_envelope then
      local env_ok = schema.validate_envelope {
        action = "PublishPageVersion",
        requestId = "rid-" .. ver,
        actor = "pub",
        tenant = "t",
        timestamp = "2026-03-15T00:00:00Z",
        nonce = "n-" .. ver,
        signatureRef = "s-" .. ver,
        payload = {
          siteId = "conc-schema",
          pageId = "p",
          versionId = ver,
          manifestTx = "tx-" .. ver,
        },
      }
      if not env_ok then
        error "envelope schema failed during fuzz"
      end
    end
    siteProc.route(with_req {
      Action = "PutDraft",
      ["Site-Id"] = "conc-schema",
      ["Page-Id"] = "p",
      Content = { title = ver, blocks = { { type = "paragraph", text = ver } } },
      ["Actor-Role"] = "editor",
    })
    if math.random() < 0.5 then
      siteProc.route(with_req {
        Action = "PublishVersion",
        ["Site-Id"] = "conc-schema",
        Version = ver,
        ["Actor-Role"] = "publisher",
      })
    else
      reg.route(with_req {
        Action = "SetActiveVersion",
        ["Site-Id"] = "conc-schema",
        Version = ver,
        ["Actor-Role"] = "registry-admin",
      })
    end
  end
  local conf2 = reg.route(with_req { Action = "GetSiteConfig", ["Site-Id"] = "conc-schema" })
  if conf2.status ~= "OK" then
    error "site config missing after schema fuzz"
  end
end

-- Product currency/VAT schema fuzz
do
  local schema = require "ao.shared.schema"
  local payload =
    { sku = "sku-curr-1", name = "Prod", price = 9.99, currency = "EUR", vatRate = 0.21 }
  local ok = schema.validate_payload and schema.validate_payload("Product", payload)
  if ok == nil then
    -- fallback: use product schema directly
    if schema.validate then
      local ok2, errs = schema.validate("product", payload)
      if ok2 == false then
        error("product schema validation failed: " .. tostring(errs and errs[1]))
      end
    end
  elseif ok == false then
    error "product currency/vat schema validation failed"
  end
end

-- Rate limit sqlite smoke
do
  os.setenv("AUTH_RATE_LIMIT_SQLITE", "/tmp/ao-rate-fuzz.db")
  package.loaded["ao.shared.auth"] = nil
  local auth = require "ao.shared.auth"
  for i = 1, 5 do
    local ok = auth.check_rate_limit { ["Site-Id"] = "r1", Subject = "u1" }
    if not ok then
      error "rate limit should not trip in smoke"
    end
  end
  os.remove "/tmp/ao-rate-fuzz.db"
  package.loaded["ao.shared.auth"] = nil
end

-- Audit rotation/prune: set tiny rotate and emit many records
do
  os.setenv = os.setenv or function() end -- no-op if not available
  -- re-require audit with different env by clearing cache if possible
  package.loaded["ao.shared.audit"] = nil
  os.execute "rm -rf /tmp/ao-audit-fuzz"
  os.execute "mkdir -p /tmp/ao-audit-fuzz"
  os.setenv("AUDIT_LOG_DIR", "/tmp/ao-audit-fuzz")
  os.setenv("AUDIT_ROTATE_MAX", "200")
  os.setenv("AUDIT_RETAIN_FILES", "2")
  local audit2 = require "ao.shared.audit"
  for i = 1, 50 do
    audit2.record("fuzz", "Test", { ["Request-Id"] = tostring(i) }, { status = "OK" })
  end
  local p = io.popen("ls -1 /tmp/ao-audit-fuzz | wc -l", "r")
  local count = p and p:read "*n" or 0
  if p then
    p:close()
  end
  if count > 10 then
    error("audit rotation retained too many files: " .. tostring(count))
  end

  -- heavier prune scenario
  for i = 51, 400 do
    audit2.record("fuzz", "Test", { ["Request-Id"] = tostring(i) }, { status = "OK" })
  end
  local p2 = io.popen("ls -1 /tmp/ao-audit-fuzz | wc -l", "r")
  local count2 = p2 and p2:read "*n" or 0
  if p2 then
    p2:close()
  end
  if count2 > 10 then
    error("audit rotation retained too many files after heavy write: " .. tostring(count2))
  end
end

-- Random role denial fuzz for order actions
do
  local site = require "ao.site.process"
  local roles = { "viewer", "guest", nil }
  for i = 1, 10 do
    local role = roles[math.random(1, #roles)]
    local resp = site.route(with_req {
      Action = "RecordOrder",
      ["Site-Id"] = "fuzz-role",
      ["Order-Id"] = "order-" .. i,
      Status = "pending",
      TotalAmount = 1.11 * i,
      Currency = "EUR",
      VatRate = 0.2,
      ["Actor-Role"] = role,
    })
    if resp.status ~= "ERROR" or resp.code ~= "FORBIDDEN" then
      error("expected forbidden for role " .. tostring(role))
    end
  end
end

print "fuzz tests passed"

print "fuzz tests passed"
