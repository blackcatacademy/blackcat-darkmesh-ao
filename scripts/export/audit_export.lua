#!/usr/bin/env lua
-- Export audit logs to stdout or file.
-- Usage: AUDIT_LOG_DIR=... scripts/export/audit_export.lua [process|all] [format] [outfile]
-- format: ndjson (default) or raw (keeps original lines)

local audit = require("ao.shared.audit")
local lfs = require("lfs")

local process = arg[1] or ""
local format = arg[2] or "ndjson"
local outfile = arg[3]

local log_dir = os.getenv("AUDIT_LOG_DIR") or "arweave/manifests"
local files = {}

local function add_file(path)
  local f = io.open(path, "r")
  if f then
    table.insert(files, path)
    f:close()
  end
end

if process == "all" then
  for file in lfs.dir(log_dir) do
    if file:match("^audit.*%.log$") then
      add_file(log_dir .. "/" .. file)
    end
  end
elseif process ~= "" then
  add_file(audit.process_log_path(process))
else
  add_file(audit.log_path() or (log_dir .. "/audit.log"))
end

if #files == 0 then
  io.stderr:write("No audit logs found under ", log_dir, "\n")
  os.exit(1)
end

local out = outfile and assert(io.open(outfile, "w")) or io.stdout

for _, path in ipairs(files) do
  local f = io.open(path, "r")
  if f then
    for line in f:lines() do
      if format == "ndjson" and not line:match("^%s*{") then
        -- best effort: wrap raw line as JSON string
        out:write(string.format("%q\n", line))
      else
        out:write(line, "\n")
      end
    end
    f:close()
  end
end

if outfile then out:close() end
