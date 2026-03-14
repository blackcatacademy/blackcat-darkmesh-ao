-- Lightweight fuzz/property checks for pagination and Arweave HTTP failure handling.

math.randomseed(os.time())

local catalog = require("ao.catalog.process")
local site = require("ao.site.process")
local ar = require("ao.shared.arweave")
local audit = require("ao.shared.audit")
local auth = require("ao.shared.auth")

local function with_req(fields)
  fields["Request-Id"] = fields["Request-Id"] or tostring(math.random())
  return fields
end

-- Fuzz pagination uniqueness
do
  local siteId = "fuzz-site"
  local cat = "cat-fuzz"
  local total = 40
  for i = 1, total do
    catalog.route(with_req({ Action = "UpsertProduct", ["Site-Id"] = siteId, Sku = "fuzz-" .. i, Payload = { name = "P" .. i }, ["Actor-Role"] = "catalog-admin" }))
  end
  catalog.route(with_req({ Action = "UpsertCategory", ["Site-Id"] = siteId, ["Category-Id"] = cat, Products = {} , ["Actor-Role"] = "catalog-admin" }))
  for i = 1, total do
    catalog.route(with_req({ Action = "UpsertCategory", ["Site-Id"] = siteId, ["Category-Id"] = cat, Products = { "fuzz-" .. i }, ["Actor-Role"] = "catalog-admin" }))
  end
  local seen = {}
  for page = 1, 20 do
    local resp = catalog.route(with_req({ Action = "ListCategoryProducts", ["Site-Id"] = siteId, ["Category-Id"] = cat, Page = page, PageSize = 3 }))
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
  local tx, err = ar.put_snapshot({ dummy = string.rep("x", 300 * 1024) }) -- > 256 KiB
  if tx ~= nil or err ~= "too_large" then
    error("expected too_large manifest rejection")
  end
end

-- Force Arweave HTTP error via env flag
do
  package.loaded["ao.shared.arweave"] = nil
  os.setenv("ARWEAVE_MODE", "http")
  os.setenv("ARWEAVE_HTTP_REAL", "1")
  os.setenv("ARWEAVE_FORCE_ERROR", "1")
  local ar2 = require("ao.shared.arweave")
  local tx, err = ar2.put_snapshot({ dummy = "ok" })
  if err ~= "http_error" then
    error("expected http_error with force flag")
  end
  os.setenv("ARWEAVE_FORCE_ERROR", nil)
  package.loaded["ao.shared.arweave"] = nil
end

-- Auth ed25519 verification round-trip
do
  -- generate keypair
  os.execute("openssl genpkey -algorithm ed25519 -out /tmp/ao-ed.key >/dev/null 2>&1")
  os.execute("openssl pkey -in /tmp/ao-ed.key -pubout -out /tmp/ao-ed.pub >/dev/null 2>&1")
  local target = "PublishVersion|site-x|rid-x"
  os.execute(string.format("printf %%s %q > /tmp/ao-msg", target))
  os.execute("openssl pkeyutl -sign -inkey /tmp/ao-ed.key -rawin -in /tmp/ao-msg -out /tmp/ao-sig >/dev/null 2>&1")
  local sig_hex = io.popen("xxd -p /tmp/ao-sig"):read("*l")
  os.setenv("AUTH_SIGNATURE_TYPE", "ed25519")
  os.setenv("AUTH_SIGNATURE_PUBLIC", "/tmp/ao-ed.pub")
  os.setenv("AUTH_REQUIRE_SIGNATURE", "1")
  package.loaded["ao.shared.auth"] = nil
  local auth2 = require("ao.shared.auth")
  local ok, err = auth2.require_signature({ Action = "PublishVersion", ["Site-Id"] = "site-x", ["Request-Id"] = "rid-x", Signature = sig_hex })
  if not ok then error("ed25519 signature should verify: " .. tostring(err)) end
  local ok2, err2 = auth2.require_signature({ Action = "PublishVersion", ["Site-Id"] = "site-x", ["Request-Id"] = "rid-x", Signature = "deadbeef" })
  if ok2 then error("bad signature should fail") end
  os.setenv("AUTH_REQUIRE_SIGNATURE", nil)
  os.setenv("AUTH_SIGNATURE_TYPE", nil)
  os.setenv("AUTH_SIGNATURE_PUBLIC", nil)
  package.loaded["ao.shared.auth"] = nil
end

-- Concurrent publish/version set simulation
do
  local siteId = "conc-site"
  local site = require("ao.site.process")
  site.route(with_req({ Action = "PutDraft", ["Site-Id"] = siteId, ["Page-Id"] = "p1", Content = { title = "T" }, ["Actor-Role"] = "editor" }))
  local ok1 = site.route(with_req({ Action = "PublishVersion", ["Site-Id"] = siteId, Version = "v1", ["Actor-Role"] = "publisher" }))
  local conflict = site.route(with_req({ Action = "PublishVersion", ["Site-Id"] = siteId, Version = "v2", ExpectedVersion = "old", ["Actor-Role"] = "publisher" }))
  if conflict.status ~= "ERROR" then error("Expected VERSION_CONFLICT on second publish") end
end

-- Audit rotation/prune: set tiny rotate and emit many records
do
  os.setenv = os.setenv or function() end -- no-op if not available
  -- re-require audit with different env by clearing cache if possible
  package.loaded["ao.shared.audit"] = nil
  os.execute("rm -rf /tmp/ao-audit-fuzz")
  os.execute("mkdir -p /tmp/ao-audit-fuzz")
  os.setenv("AUDIT_LOG_DIR", "/tmp/ao-audit-fuzz")
  os.setenv("AUDIT_ROTATE_MAX", "200")
  os.setenv("AUDIT_RETAIN_FILES", "2")
  local audit2 = require("ao.shared.audit")
  for i = 1, 50 do
    audit2.record("fuzz", "Test", { ["Request-Id"] = tostring(i) }, { status = "OK" })
  end
  local p = io.popen("ls -1 /tmp/ao-audit-fuzz | wc -l", "r")
  local count = p and p:read("*n") or 0
  if p then p:close() end
  if count > 3 then error("audit rotation retained too many files: " .. tostring(count)) end
end

print("fuzz tests passed")

print("fuzz tests passed")
