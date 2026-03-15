#!/usr/bin/env lua
-- Audit log dumper (mock-friendly). Prints last N lines from audit log.
-- Usage: AUDIT_LOG_DIR=... scripts/export/audit_dump.lua [N] [process]

local audit = require "ao.shared.audit"

local count = tonumber(arg[1]) or 50
local process = arg[2]

local log_path = process and audit.process_log_path(process) or audit.log_path()
if not log_path then
  io.stderr:write "No AUDIT_LOG_DIR configured; nothing to dump\n"
  os.exit(1)
end

local f = io.open(log_path, "r")
if not f then
  io.stderr:write("Log not found: ", log_path, "\n")
  os.exit(1)
end

local lines = {}
for line in f:lines() do
  table.insert(lines, line)
end
f:close()

local start = #lines - count + 1
if start < 1 then
  start = 1
end
for i = start, #lines do
  print(lines[i])
end
