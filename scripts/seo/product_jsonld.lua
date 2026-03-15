#!/usr/bin/env lua
-- Generate minimal Product JSON-LD from payload
local json = require("cjson.safe")
local sku = arg[1]
local payload_file = arg[2]
if not sku or not payload_file then
  io.stderr:write("Usage: lua scripts/seo/product_jsonld.lua <sku> <payload.json>\n")
  os.exit(1)
end
local f = assert(io.open(payload_file, "r"))
local content = f:read("*a")
f:close()
local ok, payload = pcall(json.decode, content)
if not ok or type(payload) ~= "table" then
  io.stderr:write("Invalid JSON\n")
  os.exit(1)
end
local ld = {
  ["@context"] = "https://schema.org",
  ["@type"] = "Product",
  sku = sku,
  name = payload.name,
  description = payload.description,
  image = payload.image,
  brand = payload.brand,
  category = payload.categoryId,
  offers = {
    ["@type"] = "Offer",
    price = payload.price,
    priceCurrency = payload.currency,
    availability = payload.available and "https://schema.org/InStock" or "https://schema.org/OutOfStock",
  },
}
print(json.encode(ld))
