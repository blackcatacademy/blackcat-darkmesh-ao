# Verify scripts

This directory holds verification and smoke-check helpers for the AO layer.

Current tools:

- `preflight.sh` — validates JSON schemas and checks Lua sources for syntax errors (`lua5.4` or `luac` required).
- `contracts.lua` — lightweight contract smoke tests against handler scaffolding.
- `arweave_health.sh` — optional curl-based health check for `ARWEAVE_HTTP_ENDPOINT`.
- `signer_check.sh` — verifies `ARWEAVE_HTTP_SIGNER` exists when `ARWEAVE_HTTP_REAL=1`.
- `signer_validate.sh` — parses signer JSON (`kty/n/e/d` required) when `ARWEAVE_HTTP_SIGNER` is set.

Usage:

```bash
scripts/verify/preflight.sh
lua scripts/verify/contracts.lua
# optional
ARWEAVE_HTTP_ENDPOINT=https://arweave.net/health scripts/verify/arweave_health.sh
ARWEAVE_HTTP_REAL=1 ARWEAVE_HTTP_SIGNER=/path/to/signer.json bash scripts/verify/signer_check.sh
ARWEAVE_HTTP_SIGNER=/path/to/signer.json bash scripts/verify/signer_validate.sh
```

Run this locally before opening a PR to catch obvious issues early.
