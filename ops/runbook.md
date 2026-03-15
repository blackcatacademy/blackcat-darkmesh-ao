# Ops Runbook (AO/Write)

## Key rotation (ed25519/HMAC)
- Keys live at `/etc/ao/keys/*.pub` (and private counterparts in secure store).
- Record checksum: `sha256sum /etc/ao/keys/write-ed25519.pub` (store in ops notes).
- Rotation steps:
  1) Generate new keypair (keep old active): `ssh-keygen -t ed25519 -f /etc/ao/keys/write-ed25519-new -N ''`.
  2) Update env: `AUTH_SIGNATURE_PUBLIC` (AO) / `WRITE_SIG_PUBLIC` (write) to new pub; deploy.
  3) Verify CI/env loads libsodium/openssl; run `scripts/verify/libsodium_strict.sh` and `scripts/verify/preflight.sh`.
  4) Once traffic confirmed, retire old key (remove from env, archive private securely).
- HMAC secrets (OUTBOX_HMAC_SECRET, OTP_HMAC_SECRET): rotate by adding new secret, deploy, then drop old after consumers updated.

## Checksums / WAL / queue
- Queue/WAL paths: `AO_WAL_PATH`, `AO_QUEUE_PATH`, `WRITE_WAL_PATH`, `WRITE_OUTBOX_PATH`.
- Health: `scripts/verify/checksum_alert.sh` (AO) and `scripts/verify/checksum_alert.sh` (write) warn on size/hash drift.
- Daemon: `ops/checksum-daemon.service` runs `scripts/verify/checksum_daemon.sh` with `CHECKSUM_INTERVAL_SEC`.
- For production, set alerts when WAL/queue exceed thresholds (`AO_WAL_MAX_BYTES`, `AO_QUEUE_MAX_BYTES`).

## Secrets handling
- CI uses org-level secrets; gitleaks runs in CI (fail on detection). Avoid echoing secrets in logs.
- Keep `ops/env.prod.example` free of real keys; use vault/secret manager for prod values.

## Start/stop
- AO: `LUA_PATH="?.lua;?/init.lua;ao/?.lua;ao/?/init.lua" lua scripts/verify/health.lua`
- Check metrics flush: `METRICS_PROM_PATH`, `METRICS_FLUSH_INTERVAL_SEC`.
- Run checksum daemon under systemd: `ops/checksum-daemon.service` (set env file `/etc/blackcat/ao.env`).

## Incident: replay/rollback
- Use WAL hashes to detect tamper. Re-run write fixtures (`scripts/verify/contracts.lua`, `scripts/verify/batch_run.lua` if added) and compare.
- For resolver trust issues: rotate trusted resolvers manifest (UpdateTrustResolvers) and flags file (`AO_FLAGS_PATH`).

## Dependency pinning
- Rocks pinned via `ops/rocks.lock`. CI installs from lockfile; ensure updates go through `luarocks` + lock refresh.
- No npm/pip runtime deps today; if added, pin versions and add lock files to ops/ (package-lock.json/pip-tools).
