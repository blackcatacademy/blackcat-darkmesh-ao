# Third-party dependencies (optional)

These libraries are MIT/BSD licensed and safe for commercial use. They are optional; code falls back gracefully if absent.

- **luv** (MIT): event loop/timers. Install via `luarocks install luv`.
- **ed25519** (MIT): pure-Lua Ed25519 verify. Install via `luarocks install ed25519`.
- **lsqlite3** (MIT): persistent rate-limit store. Install via `luarocks install lsqlite3`.
- **luaossl** (OpenSSL/SSLeay): alternate crypto backend. Install via `luarocks install luaossl`.
- **lua-cjson** (MIT): JSON decode for Arweave response validation. Install via `luarocks install lua-cjson`.

If not installed, features degrade safely (signature enforcement fails closed when required, timers stay tick-based, response JSON validation is pattern-only).
