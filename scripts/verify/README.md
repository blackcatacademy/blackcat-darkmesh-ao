# Verify scripts

This directory holds verification and smoke-check helpers for the AO layer.

Current tools:

- `preflight.sh` — validates JSON schemas and checks Lua sources for syntax errors (`lua5.4` or `luac` required).
- `contracts.lua` — lightweight contract smoke tests against handler scaffolding.

Usage:

```bash
scripts/verify/preflight.sh
lua scripts/verify/contracts.lua
```

Run this locally before opening a PR to catch obvious issues early.
