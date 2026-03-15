-- Minimal SEO helpers (JSON-LD generators). Not wired by default.

local cjson_ok, cjson = pcall(require, "cjson.safe")

local SEO = {}

local function encode(ld)
  if not cjson_ok then
    return nil
  end
  return cjson.encode(ld)
end

-- Products ---------------------------------------------------------------
function SEO.product_ld(product)
  return encode {
    ["@context"] = "https://schema.org",
    ["@type"] = "Product",
    name = product.name,
    description = product.description,
    sku = product.sku,
    image = product.image,
    brand = product.brand,
    category = product.category,
    offers = {
      ["@type"] = "Offer",
      price = product.price,
      priceCurrency = product.currency,
      availability = product.available and "https://schema.org/InStock"
        or "https://schema.org/OutOfStock",
      url = product.url,
      itemCondition = product.condition,
    },
  }
end

-- Articles / blog --------------------------------------------------------
function SEO.article_ld(article)
  return encode {
    ["@context"] = "https://schema.org",
    ["@type"] = "Article",
    headline = article.title,
    datePublished = article.publishedAt,
    dateModified = article.updatedAt or article.publishedAt,
    author = article.author and { ["@type"] = "Person", name = article.author } or nil,
    image = article.image,
    description = article.description,
    mainEntityOfPage = article.url,
  }
end

-- Breadcrumbs ------------------------------------------------------------
function SEO.breadcrumb_ld(crumbs)
  local item_list = {}
  for idx, crumb in ipairs(crumbs or {}) do
    table.insert(item_list, {
      ["@type"] = "ListItem",
      position = idx,
      name = crumb.name,
      item = crumb.url,
    })
  end
  return encode {
    ["@context"] = "https://schema.org",
    ["@type"] = "BreadcrumbList",
    itemListElement = item_list,
  }
end

-- FAQ --------------------------------------------------------------------
function SEO.faq_ld(items)
  local qas = {}
  for _, qa in ipairs(items or {}) do
    table.insert(qas, {
      ["@type"] = "Question",
      name = qa.question,
      acceptedAnswer = { ["@type"] = "Answer", text = qa.answer },
    })
  end
  return encode {
    ["@context"] = "https://schema.org",
    ["@type"] = "FAQPage",
    mainEntity = qas,
  }
end

-- Organization -----------------------------------------------------------
function SEO.organization_ld(org)
  return encode {
    ["@context"] = "https://schema.org",
    ["@type"] = "Organization",
    name = org.name,
    url = org.url,
    logo = org.logo,
    sameAs = org.sameAs,
    contactPoint = org.contact and {
      ["@type"] = "ContactPoint",
      telephone = org.contact.phone,
      contactType = org.contact.type or "customer support",
      areaServed = org.contact.areaServed,
      availableLanguage = org.contact.languages,
    } or nil,
  }
end

-- WebPage ----------------------------------------------------------------
function SEO.page_ld(page)
  return encode {
    ["@context"] = "https://schema.org",
    ["@type"] = "WebPage",
    name = page.title or page.name,
    description = page.description,
    url = page.url,
    inLanguage = page.locale,
  }
end

-- Canonical / hreflang helpers -------------------------------------------
function SEO.canonical(base_url, path)
  if not base_url or base_url == "" then
    return path
  end
  if base_url:sub(-1) == "/" then
    base_url = base_url:sub(1, -2)
  end
  if path:sub(1, 1) ~= "/" then
    path = "/" .. path
  end
  return base_url .. path
end

---Build hreflang link map.
-- @param base_url string e.g. https://example.com
-- @param path string normalized path without locale prefix
-- @param locales { supported = { "en", "de" }, default = "en" }
function SEO.hreflang_links(base_url, path, locales)
  if not locales or not locales.supported then
    return {}
  end
  local links = {}
  for _, loc in ipairs(locales.supported) do
    local href = SEO.canonical(base_url, "/" .. loc .. path)
    table.insert(links, { rel = "alternate", hreflang = loc:lower(), href = href })
  end
  -- x-default
  local default_href = SEO.canonical(base_url, "/" .. (locales.default or "en") .. path)
  table.insert(links, { rel = "alternate", hreflang = "x-default", href = default_href })
  return links
end

-- Sitemaps / robots.txt --------------------------------------------------
function SEO.sitemap(urls)
  local buffer = {
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
  }
  for _, u in ipairs(urls or {}) do
    table.insert(buffer, "<url>")
    table.insert(buffer, string.format("<loc>%s</loc>", u.loc))
    if u.lastmod then
      table.insert(buffer, string.format("<lastmod>%s</lastmod>", u.lastmod))
    end
    if u.changefreq then
      table.insert(buffer, string.format("<changefreq>%s</changefreq>", u.changefreq))
    end
    if u.priority then
      table.insert(buffer, string.format("<priority>%.1f</priority>", u.priority))
    end
    table.insert(buffer, "</url>")
  end
  table.insert(buffer, "</urlset>")
  return table.concat(buffer, "\n")
end

function SEO.robots_txt(opts)
  opts = opts or {}
  local lines = {
    "User-agent: *",
    string.format("Disallow: %s", opts.disallow or ""),
  }
  if opts.allow then
    table.insert(lines, string.format("Allow: %s", opts.allow))
  end
  if opts.sitemap then
    table.insert(lines, "Sitemap: " .. opts.sitemap)
  end
  return table.concat(lines, "\n")
end

return SEO
