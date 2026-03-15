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

-- Fail-closed if sodium (for ed25519) is missing when signatures are required
if os.getenv("AUTH_REQUIRE_SIGNATURE") == "1" then
  local ok_sodium, _ = pcall(require, "sodium")
  if not ok_sodium then
    io.stderr:write("sodium module missing but AUTH_REQUIRE_SIGNATURE=1\n")
    os.exit(1)
  end
end
