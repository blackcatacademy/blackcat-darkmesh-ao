local deps = {
  { name = "luv", mod = "luv" },
  { name = "ed25519", mod = "ed25519" },
  { name = "lsqlite3", mod = "lsqlite3" },
  { name = "cjson", mod = "cjson.safe" },
  { name = "luaossl", mod = "openssl" },
  { name = "sodium", mod = "sodium" },
}

for _, d in ipairs(deps) do
  local ok, _ = pcall(require, d.mod)
  io.stdout:write(string.format("%-10s: %s\n", d.name, ok and "available" or "missing"))
  if not ok then os.exit(1) end
end
