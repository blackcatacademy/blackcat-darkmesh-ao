# blackcat-darkmesh-ao

AO-first backend layer for Blackcat Darkmesh. This repository hosts the AO processes, message contracts, schemas, Arweave manifests, and runbooks that power the permaweb runtime. Code is commercially restricted; non-commercial use is allowed for transparency.

## Scope
- In scope: AO processes (registry, site, catalog, access), message handlers, schemas, Arweave manifests/snapshots, deploy/verify/export scripts, fixtures, CI workflows.
- Out of scope: PHP/JS gateways, frontend builds, write bridge with secrets, admin studio. Those live in separate repos and only integrate via public contracts.

## Architecture Snapshot
- Process split: `registry` (domains, versions, roles), `site` (routes, pages, layouts, menus), `catalog` (products, categories, listings), `access` (entitlements, protected assets).
- Data model: document/NoSQL over process state; optional SQLite/WASM only for future specialized queries.
- Immutable assets: large HTML snapshots, media, catalog exports live on Arweave; AO state keeps normalized JSON and references.
- Write path: signed writes only (bridge/admin); read path: resolver pulls AO state and follows Arweave refs.
- Deterministic handlers and stable message names for public contracts.

## Repository Layout (blueprint)
```
docs/              # Architecture, runbooks, ADRs
ao/                # AO processes and shared libs
  registry|site|catalog|access/
  shared/          # auth, codec, validation, ids
arweave/           # manifests, snapshots, asset index, encryption policies
schemas/           # JSON schemas for pages, routes, products, publish, entitlements
scripts/           # deploy | verify | seed | export
fixtures/          # sample site, catalog, publish data
tests/             # integration, message-contracts, snapshots, security
.github/workflows/ # CI entrypoint
```

## Message Contract (minimal read/write set)
- Read: `GetSiteByHost`, `ResolveRoute`, `GetPage`, `GetLayout`, `GetNavigation`, `GetProduct`, `ListCategoryProducts`, `HasEntitlement`.
- Write: `RegisterSite`, `BindDomain`, `PutDraft`, `UpsertRoute`, `PublishVersion`, `UpsertProduct`, `GrantRole`, `GrantEntitlement`.
- Standard tags to carry: `Action`, `Site-Id`, `Version`, `Locale`, `Request-Id`, `Actor-Role`, `Schema-Version`, `Publish-Id`, `Nonce`, `Signature-Ref`.

## Publish Model (draft → publish → activate)
1) Draft stored and validated against JSON schemas.  
2) Publish snapshot created with `publishId` + `versionId`.  
3) Immutable payloads pinned to Arweave (`manifestTx`).  
4) Registry flips `activeVersion`; history stays append-only for rollback/audit.

## Initial Roadmap
- M0 Core skeleton: repo, docs, schemas, shared libs, fixtures.
- M1 Registry + Site: host lookup, route resolving, page returns, basic publish.
- M2 Catalog: product/category read/write and publish flow.
- M3 Access: protected asset refs and entitlement checks.
- M4 Scale hardening: process split tuning, verify/rollback scripts, runbooks.

## Usage Notes
- Keep hot state small; push large or historical payloads to Arweave.
- Never store private keys, seeds, or plaintext secrets in AO state or metadata; only references and hashes belong here.
- All comments and documentation in this repository stay in English.

## Development
- Prereqs: `python3` (3.8+) and Lua (`lua5.1`–`lua5.4`) with rocks `lua-cjson`, `lsqlite3`, `luv`, `luaossl`, and `libsodium` headers for native crypto. Run `lua scripts/verify/deps_check.lua` to verify.
- Run static checks before opening a PR: `scripts/verify/preflight.sh`.
- Contract smoke tests are bundled in the preflight script (runs under Lua 5.4).
- Branches: `main` (releasable), `develop` (integration), `feature/*`, `adr/*`, `release/*`.
- Message contracts and schemas are treated as public API; prefer additive changes over breaking ones.
- Role policy: write actions are gated by actor roles (registry/site/catalog/access); provide `Actor-Role` tag in messages to pass policy checks.
- Arweave config (mock-safe by default):
  - `ARWEAVE_MODE` (`mock`|`http`) — mock persists snapshots locally; http logs intended requests only.
  - `ARWEAVE_HTTP_ENDPOINT`, `ARWEAVE_HTTP_API_KEY`, `ARWEAVE_HTTP_SIGNER` — only logged in http mode.
  - `ARWEAVE_HTTP_TIMEOUT` seconds; requests are simulated/offline but logged with this value.
  - `ARWEAVE_HTTP_REAL=1` enables actual HTTP POST via curl (still logs responses); keep unset for offline.
  - `ARWEAVE_HTTP_SIGNER_HEADER` custom header name for signer path (default `X-Arweave-Signer`); signer file must exist when `*_REAL=1`.
- Audit config: `AUDIT_LOG_DIR` (default `arweave/manifests`), `AUDIT_MAX_RECORDS` (default 1000 in-memory).
  - `AUDIT_FORMAT` (`line`|`ndjson`), `AUDIT_ROTATE_MAX` bytes for log rotation.
  - `AUDIT_RETAIN_FILES` rotated log files per stream (default 10).
- Audit export tooling:
  - `scripts/export/audit_dump.lua [N] [process]` — tail audit logs (mock-safe).
  - `scripts/export/audit_export.lua [process|all] [format] [outfile]` — export NDJSON or raw.
  - `scripts/export/audit_ci.sh [outfile]` — CI-friendly helper; writes `artifacts/audit.ndjson` if logs exist (no-op otherwise).
- Payload caps (env overrides):
  - `SITE_MAX_CONTENT_BYTES` (draft/page content, default 64 KiB)
  - `CATALOG_MAX_PAYLOAD_BYTES` (product/category payloads, default 64 KiB)
  - `ACCESS_MAX_POLICY_BYTES` (entitlement policy payloads, default 32 KiB)
  - `REGISTRY_MAX_CONFIG_BYTES` (site config payload, default 16 KiB)
- Idempotency cache: `IDEM_TTL_SECONDS` (default 300s) and `IDEM_MAX_ENTRIES` (default 1024) bound the in-memory Request-Id store.
- Security hooks: nonce/signature optional enforcement (`AUTH_REQUIRE_NONCE`, `AUTH_REQUIRE_SIGNATURE`), nonce TTL (`AUTH_NONCE_TTL_SECONDS`), rate limit window (`AUTH_RATE_LIMIT_WINDOW_SECONDS`) and max (`AUTH_RATE_LIMIT_MAX_REQUESTS`).
- Signature check uses HMAC-SHA256 over `Action|Site-Id|Request-Id` with `AUTH_SIGNATURE_SECRET` (requires `openssl` when enforcement is on).
- Optional JWT gate: set `AUTH_JWT_HS_SECRET` (HS256) and optionally `AUTH_REQUIRE_JWT=1` to fail-closed; claims `sub/tenant/role/nonce` are mapped to `Actor-Id`/`Tenant`/`Actor-Role`/`Nonce`.
- Rate-limit state can persist to `AUTH_RATE_LIMIT_FILE`.
- Prefer libsodium/luaossl for ed25519 when present; set `AUTH_ALLOW_SHELL_FALLBACK=0` to forbid shell fallback.
- Metrics: set `METRICS_ENABLED=1` and `METRICS_LOG` path to emit NDJSON counters; see `ao/shared/metrics.lua`.
- Prometheus export: set `METRICS_PROM_PATH` to write text exposition on flush.
- Flush cadence: `METRICS_FLUSH_INTERVAL_SEC` (tick-based timer) or `METRICS_FLUSH_EVERY` (per-N increments); no shell/background dependency.
- Arweave HTTP: retries/backoff (`ARWEAVE_HTTP_RETRIES`, `ARWEAVE_HTTP_BACKOFF_MS`), manifest cap (`ARWEAVE_MAX_MANIFEST_BYTES`), signer hash logged when present.
- Arweave dry-run: `ARWEAVE_HTTP_DRYRUN=1` skips curl; errors on HTTP >=400 return `http_error`.
- Fuzz tests: set `RUN_FUZZ=1` to run `scripts/verify/fuzz.lua` during preflight.
- Production baseline env: see `ops/env.prod.example` (strict signatures, sqlite rate-limit, Prometheus path).

## Quickstart / Deploy (PowerShell vs shell)
The commands below ship the **schema bundle** (manifest-only), intended for dev/CI snapshots. For production, export with your curated presets and strict env (see below).

Use the built-in launcher; it detects Windows/WSL vs Linux and prints copy/paste commands:
```
python scripts/start.py
```

Deploy the latest bundle to Arweave via arkb:
- PowerShell (Windows):
```
npx arkb deploy "dev/schema-bundles/schema-bundle-*.tar.gz" --content-type application/gzip   # dev snapshot
```
- Bash (Linux/WSL):
```
./scripts/setup/build_schema_bundle.sh
npx arkb deploy ./dev/schema-bundles/schema-bundle-*.tar.gz --content-type application/gzip   # dev snapshot
```

Handy CLI helpers:
- List collections: `python scripts/setup/schema_helper.py list`
- Suggest presets from prompt: `python scripts/setup/schema_helper.py suggest --prompt "ebook shop with subscriptions"`
- Export bundle with presets: `python scripts/setup/schema_helper.py export --presets core,commerce,content,ebook,subscriptions --out dev/schema-bundles/custom.tar.gz`
- Wizard for a site-specific bundle (prompts for slug/presets): run `python scripts/start.py` and choose option **4**.
- Dependency check: `lua scripts/verify/deps_check.lua`

**Production reminders**
- Start from `ops/env.prod.example` and set:  
  - `AUTH_REQUIRE_SIGNATURE=1`  
  - `AUTH_SIGNATURE_TYPE=ed25519` and `AUTH_SIGNATURE_PUBLIC=/etc/ao/keys/registry-ed25519.pub` (or your path)  
  - `AUTH_ALLOW_SHELL_FALLBACK=0` (fail-closed if openssl/shell is missing)  
  - `AUTH_RATE_LIMIT_SQLITE=/var/lib/ao/rate.db` (persistent per-actor rate limit)  
  - `METRICS_PROM_PATH=/var/lib/ao/metrics.prom` and optionally `METRICS_FLUSH_INTERVAL_SEC=10`
- Key management: store public keys under `/etc/ao/keys`, record their `sha256sum` in your ops vault, and rotate on a schedule; never check private keys into the repo or CI artifacts.

### Ops runbook
See `docs/RUNBOOK.md` for start/stop, health checks, key rotation, outbox HMAC, Arweave deploy verification, and incident response procedures.

### Orders API (resolver-facing)
- `RecordOrder` (support/admin) — stores status, totalAmount, currency, vatRate, reason, updatedAt.
- `GetOrder` (support/admin) — returns stored order fields.
- `ListOrders` (support/admin) — paginated list filtered by status; returns `siteId,total,page,pageSize,items[]` with status/amount/currency/vatRate/updatedAt.
- Schemas: `schemas/order.schema.json` and `schemas/order-list.schema.json` are in the manifest.
- Export only the collections you need: `python scripts/setup/schema_helper.py export --presets core,commerce,content --out dev/schema-bundles/prod.tar.gz`.
- Deploy that prod bundle with arkb from a secured environment.

### Write-bridge observability (from `blackcat-darkmesh-write`)
When you deploy the write bridge alongside this repo, enable its logging so AO ops can audit downstream delivery:
- Queue: set `AO_QUEUE_PATH=/var/lib/ao/outbox-queue.ndjson`, `AO_QUEUE_LOG_PATH=/var/log/ao/queue-log.ndjson`, `AO_QUEUE_MAX_RETRIES=5`.
- Bridge hashes: optionally enforce downstream body hash with `AO_EXPECT_RESPONSE_HASH=<sha256>`.
- WAL on write-side: `WRITE_WAL_PATH=/var/log/ao/write-wal.ndjson` (stores request/response hashes for every command).
These live in the **write** service; keep paths under your ops log/data locations.

## Schemas (WeaveDB-first)
- Canonical table definitions (columns, types, constraints) live in `schemas/canonical-db/tables/` plus the map `schemas/canonical-db/schema-map.yaml`.
- WeaveDB-ready collections are in `schemas/weavedb/collections/*.yaml` (JSON Schema + indexes); manifest v3 carries them under `weavedb`.
- Generate manifest v3: `python3 scripts/setup/make_schema_manifest.py` → `schemas/manifest/schema-manifest.json`.
- Build bundle (manifest only): `./scripts/setup/build_schema_bundle.sh`; current deployed bundle: tx `iygsD6GhCXGI1cXrl2lw6VOpxbjwISZO5pqWmo7y8XM` (sha256 `b1ee8a00d4d2c989c4d7a88daf1ca45c0ea70fb0037dd8b688d44d05f9f534d5`).

## License
Blackcat Darkmesh AO Proprietary License (see `LICENSE`). External contributions require written permission from Black Cat Academy s. r. o.
