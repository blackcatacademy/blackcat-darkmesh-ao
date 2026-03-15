-- Verify HMAC on outbox NDJSON produced by write bridge
local secret = os.getenv("OUTBOX_HMAC_SECRET")
local path = os.getenv("OUTBOX_FILE") or "dev/outbox.ndjson"
if not secret or secret == "" then
  io.stderr:write("OUTBOX_HMAC_SECRET not set\n")
  os.exit(1)
end
local crypto = require("ao.shared.crypto")
local cjson = require("cjson")
local f = io.open(path, "r")
if not f then
  io.stderr:write("outbox file not found: "..path.."\n")
  os.exit(1)
end
local ok_all = true
for line in f:lines() do
  if line:match("%S") then
    local ev = cjson.decode(line)
    local msg = (ev.siteId or "") .. "|" .. (ev.pageId or ev.orderId or "") .. "|" .. (ev.versionId or ev.amount or "")
    local expected = crypto.hmac_sha256_hex(msg, secret)
    if not expected or not ev.hmac or expected:lower() ~= tostring(ev.hmac):lower() then
      ok_all = false
      io.stderr:write(string.format("hmac mismatch for requestId=%s\n", tostring(ev.requestId)))
    end
  end
end
f:close()
if ok_all then
  print("outbox hmac: ok")
  os.exit(0)
else
  os.exit(2)
end
