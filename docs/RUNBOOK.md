# Ops Runbook (AO + Write bridge)

## Start / Stop
- Ensure env files are set from `ops/env.prod.example` (fail-closed signatures, rate DB path, metrics path).
- Start AO processes via your supervisor (e.g., `lua5.4 ao/registry/process.lua` etc.) ensuring `METRICS_PROM_PATH` is writable.
- Start write bridge with `WRITE_WAL_PATH`, `WRITE_OUTBOX_PATH`, `AO_QUEUE_PATH`, `AO_QUEUE_LOG_PATH`, `OUTBOX_HMAC_SECRET` set.

## Health Checks
- AO: `LUA_PATH="?.lua;?/init.lua;ao/?.lua;ao/?/init.lua" lua scripts/verify/health.lua` (reports rate DB RW, metrics flush, audit size, deps).
- Write: `WRITE_WAL_PATH=... WRITE_OUTBOX_PATH=... AO_QUEUE_PATH=... LUA_PATH="?.lua;?/init.lua;ao/?.lua;ao/?/init.lua" lua scripts/verify/health.lua` (reports WAL/queue size/hash, deps, warns on size caps).
- Deps: `RUN_DEPS_CHECK=1 scripts/verify/preflight.sh` (both repos, in CI).

## Key Management
- Store public keys under `/etc/ao/keys`; record `sha256sum /etc/ao/keys/*.pub` in your vault.
- Rotate keys on a schedule: deploy new pubkey, restart services with updated env (`AUTH_SIGNATURE_PUBLIC`, `WRITE_SIG_PUBLIC`), then deprecate old key.
- Never commit private keys or echo them in CI logs; use org secrets only.

## Rate-Limit Store
- AO uses `AUTH_RATE_LIMIT_SQLITE`; health check performs RW test on boot. Ensure path is on persistent storage and backed up/snapshotted if required.

## Outbox Integrity
- Write emits HMAC on outbox events when `OUTBOX_HMAC_SECRET` is set; queue forwarder verifies HMAC before delivery.
- Queue/WAL size alerts: set `WRITE_WAL_MAX_BYTES` (default 5 MiB) and `AO_QUEUE_MAX_BYTES` (default 2 MiB); health warns when exceeded.

## Replay/Idempotency
- Nonce TTL and requestId idempotency enabled; conflict tests cover cross-action replay. Keep `WRITE_REQUIRE_NONCE=1` and `WRITE_REQUIRE_SIGNATURE=1` in prod.

## Arweave Deploy Verification
- After `arkb` deploy, compute local SHA256 of bundle and compare to returned tx ID hash:
  - `sha256sum dev/schema-bundles/your.tar.gz`
  - Fetch from Arweave: `curl -sL https://arweave.net/<txid> | sha256sum`
  - Log txid + hash in ops journal.

## Incident Response
- Replay/rollback: use WAL hashes to detect tampering; re-run write fixtures with `batch_run.lua` for quick consistency check.
- Rate-limit exhaustion: inspect `AUTH_RATE_LIMIT_SQLITE` and adjust window/max via env; purge offending actor if necessary.
- HMAC failures: rotate `OUTBOX_HMAC_SECRET` and requeue undelivered events.

## Secret Scanning
- Run `gitleaks detect --no-git -v` locally before releases; keep CI secrets at org level; do not print secrets in workflows.

## Lint/Supply Chain
- Pin Lua rocks in your deploy image; run `luacheck`/`stylua` if available. Use `RUN_DEPS_CHECK=1` preflight to fail when critical rocks are missing.
