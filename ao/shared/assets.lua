-- Asset helpers: generate responsive variants and minimal CDN invalidation hooks.

local Assets = {}

local DEFAULT_SIZES = { 320, 640, 960, 1280, 1920 }
local DEFAULT_FORMATS = { "avif", "webp", "jpg" }

local function normalize_formats(formats)
  if not formats or #formats == 0 then
    return DEFAULT_FORMATS
  end
  local out = {}
  local seen = {}
  for _, f in ipairs(formats) do
    local fmt = tostring(f):lower()
    if not seen[fmt] then
      table.insert(out, fmt)
      seen[fmt] = true
    end
  end
  return out
end

local function normalize_sizes(sizes)
  if not sizes or #sizes == 0 then
    return DEFAULT_SIZES
  end
  local out = {}
  for _, s in ipairs(sizes) do
    local n = tonumber(s)
    if n and n > 0 then
      table.insert(out, math.floor(n))
    end
  end
  table.sort(out)
  return out
end

local function build_url(base_url, path)
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

---Generate responsive variants for an image using a deterministic URL pattern.
-- The pattern is: {base}/{width}w/{basename}.{format}
function Assets.build_image_variants(src, opts)
  opts = opts or {}
  local sizes = normalize_sizes(opts.sizes)
  local formats = normalize_formats(opts.formats)
  local base_url = opts.base_url or os.getenv "ASSET_BASE_URL" or "/assets"

  local basename = src:gsub("^.*/", "")
  local variants = {}
  local srcset = {}

  for _, fmt in ipairs(formats) do
    srcset[fmt] = {}
    for _, w in ipairs(sizes) do
      local path = string.format("%dw/%s.%s", w, basename, fmt)
      local url = build_url(base_url, path)
      table.insert(srcset[fmt], string.format("%s %dw", url, w))
      table.insert(variants, { width = w, format = fmt, url = url })
    end
    srcset[fmt] = table.concat(srcset[fmt], ", ")
  end

  return {
    src = build_url(base_url, basename),
    sizes = sizes,
    formats = formats,
    variants = variants,
    srcset = srcset,
  }
end

-- Lightweight CDN purge hook; caller passes relative or absolute paths.
function Assets.cdn_invalidate(paths)
  if type(paths) ~= "table" or #paths == 0 then
    return { purged = 0 }
  end
  local purged = 0
  for _, path in ipairs(paths) do
    -- intentionally minimal: integrate real CDN API here (Fastly/Akamai/Cloudflare)
    os.execute(string.format('echo "PURGE %s" >/dev/null', path))
    purged = purged + 1
  end
  return { purged = purged }
end

return Assets
