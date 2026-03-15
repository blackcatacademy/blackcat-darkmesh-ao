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

## Periodic checksum monitoring
- AO: run `scripts/verify/checksum_daemon.sh` under your supervisor (set `CHECKSUM_INTERVAL_SEC`, optional `AO_QUEUE_PATH`, `AO_WAL_PATH`, `AUDIT_LOG_DIR`). Example systemd unit:
```
[Unit]
Description=AO checksum monitor
After=network.target

[Service]
WorkingDirectory=/opt/blackcat-darkmesh-ao
Environment=CHECKSUM_INTERVAL_SEC=300
Environment=AO_QUEUE_PATH=/var/lib/ao/outbox-queue.ndjson
Environment=AO_WAL_PATH=/var/lib/ao/registry-wal.ndjson
Environment=AUDIT_LOG_DIR=/var/log/ao/audit
ExecStart=/opt/blackcat-darkmesh-ao/scripts/verify/checksum_daemon.sh
Restart=always

[Install]
WantedBy=multi-user.target
```
- Write: a similar `scripts/verify/checksum_daemon.sh` exists in the write repo (set `WRITE_WAL_PATH`, `AO_QUEUE_PATH`), run via supervisor if desired.
## Write checksum daemon (deploy on write host)
```
[Unit]
Description=Write checksum monitor
After=network.target

[Service]
WorkingDirectory=/opt/blackcat-darkmesh-write
Environment=CHECKSUM_INTERVAL_SEC=300
Environment=WRITE_WAL_PATH=/var/log/ao/write-wal.ndjson
Environment=AO_QUEUE_PATH=/var/lib/ao/outbox-queue.ndjson
ExecStart=/opt/blackcat-darkmesh-write/scripts/verify/checksum_daemon.sh
Restart=always

[Install]
WantedBy=multi-user.target
```

## Outbox Integrity
- Write emits HMAC on outbox events when `OUTBOX_HMAC_SECRET` is set; queue forwarder verifies HMAC before delivery.
- Queue/WAL size alerts: set `WRITE_WAL_MAX_BYTES` (default 5 MiB) and `AO_QUEUE_MAX_BYTES` (default 2 MiB); health warns when exceeded.
- Disputes/chargebacks: Stripe `charge.dispute.*` and PayPal `CUSTOMER.DISPUTE.*` map in write to `paymentStatus=disputed`; AO consumes `PaymentStatusChanged` and surfaces `status=disputed` on the order.

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

## Key rotation SOP (ed25519)
- Schedule: rotate every 90 days or immediately on suspected compromise.
- Generate: `openssl genpkey -algorithm ed25519 -out /secure/write-ed25519.key` and `openssl pkey -in ... -pubout -out /etc/ao/keys/write-ed25519.pub` (similarly for registry).
- Record: store `sha256sum /etc/ao/keys/*.pub` in ops vault with date.
- Deploy: update env (`WRITE_SIG_PUBLIC` / `AUTH_SIGNATURE_PUBLIC`) and restart services.
- Validate: run health + signature tests; remove old pubkey only after validation.
- Never store private keys in repo/CI; keep in secure KMS or offline.
