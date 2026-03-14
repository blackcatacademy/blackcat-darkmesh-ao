# Verify scripts

This directory holds verification and smoke-check helpers for the AO layer.

Current tools:

- `preflight.sh` — validates JSON schemas and checks Lua sources for syntax errors (`lua5.4` or `luac` required).
- `contracts.lua` — lightweight contract smoke tests against handler scaffolding.
- `arweave_health.sh` — optional curl-based health check for `ARWEAVE_HTTP_ENDPOINT`.

Usage:

```bash
scripts/verify/preflight.sh
lua scripts/verify/contracts.lua
# optional
ARWEAVE_HTTP_ENDPOINT=https://arweave.net/health scripts/verify/arweave_health.sh
```

Run this locally before opening a PR to catch obvious issues early.
