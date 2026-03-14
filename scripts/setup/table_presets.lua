#!/usr/bin/env lua

-- Simple preset helper: list/suggest/dump presets defined in config/table-presets.json.
-- No external deps beyond cjson (already required in the project).
--
-- Usage:
--   lua scripts/setup/table_presets.lua list
--   lua scripts/setup/table_presets.lua suggest "prompt text"
--   lua scripts/setup/table_presets.lua dump <preset-id>

local cjson = require("cjson.safe")

local function load_presets()
  local f = io.open("config/table-presets.json", "r")
  if not f then
    io.stderr:write("config/table-presets.json not found\n")
    os.exit(1)
  end
  local data = f:read("*a")
  f:close()
  local doc = cjson.decode(data)
  if not doc or not doc.presets then
    io.stderr:write("invalid presets file\n")
    os.exit(1)
  end
  return doc.presets
end

local function list_presets(presets)
  for _, p in ipairs(presets) do
    print(string.format("%-20s engine=%-9s tags=%s modules=%d  %s", p.id, p.engine or "any", table.concat(p.tags or {}, ","), #(p.modules or {}), p.label or ""))
  end
end

local function tokens(str)
  local t = {}
  for w in string.gmatch(string.lower(str), "%w+") do
    t[w] = true
  end
  return t
end

local function score(preset, prompt_tokens)
  local s = 0
  local function bump(val)
    if val and prompt_tokens[string.lower(val)] then s = s + 2 end
  end
  bump(preset.id)
  bump(preset.engine)
  for _, tag in ipairs(preset.tags or {}) do
    bump(tag)
    if prompt_tokens[string.lower(tag)] then s = s + 1 end
  end
  if preset.label then
    for w in string.gmatch(string.lower(preset.label), "%w+") do
      if prompt_tokens[w] then s = s + 1 end
    end
  end
  return s
end

local function suggest(presets, prompt)
  local tk = tokens(prompt or "")
  local scored = {}
  for _, p in ipairs(presets) do
    table.insert(scored, { p = p, s = score(p, tk) })
  end
  table.sort(scored, function(a, b) return a.s > b.s end)
  for i = 1, math.min(3, #scored) do
    local entry = scored[i]
    print(string.format("%s (score=%d): %s", entry.p.id, entry.s, entry.p.label or ""))
  end
end

local function dump(presets, id)
  for _, p in ipairs(presets) do
    if p.id == id then
      print(string.format("# %s (%s) modules=%d", p.id, p.engine or "any", #(p.modules or {})))
      for _, m in ipairs(p.modules or {}) do
        print("- " .. m)
      end
      return
    end
  end
  io.stderr:write("preset not found: " .. tostring(id) .. "\n")
  os.exit(1)
end

local cmd = arg[1]
local presets = load_presets()

if cmd == "list" then
  list_presets(presets)
elseif cmd == "suggest" then
  suggest(presets, arg[2] or "")
elseif cmd == "dump" then
  dump(presets, arg[2])
else
  io.stderr:write("Usage: list | suggest <prompt> | dump <preset-id>\n")
  os.exit(1)
end
