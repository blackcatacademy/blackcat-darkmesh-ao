-- Minimal SEO helpers (JSON-LD generators). Not wired by default.

local cjson_ok, cjson = pcall(require, "cjson.safe")

local SEO = {}

function SEO.product_ld(product)
  if not cjson_ok then
    return nil
  end
  local ld = {
    ["@context"] = "https://schema.org",
    ["@type"] = "Product",
    name = product.name,
    description = product.description,
    sku = product.sku,
    image = product.image,
    offers = {
      ["@type"] = "Offer",
      price = product.price,
      priceCurrency = product.currency,
      availability = product.available and "https://schema.org/InStock"
        or "https://schema.org/OutOfStock",
    },
  }
  return cjson.encode(ld)
end

function SEO.page_ld(page)
  if not cjson_ok then
    return nil
  end
  local ld = {
    ["@context"] = "https://schema.org",
    ["@type"] = "WebPage",
    name = page.title or page.name,
    description = page.description,
    url = page.url,
  }
  return cjson.encode(ld)
end

return SEO
