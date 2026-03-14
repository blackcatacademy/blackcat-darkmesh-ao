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
- Prereqs: `lua5.4` (or `luac`) and `python3`.
- Run static checks before opening a PR: `scripts/verify/preflight.sh`.
- Branches: `main` (releasable), `develop` (integration), `feature/*`, `adr/*`, `release/*`.
- Message contracts and schemas are treated as public API; prefer additive changes over breaking ones.
- Role policy: write actions are gated by actor roles (registry/site/catalog/access); provide `Actor-Role` tag in messages to pass policy checks.
- Arweave config (mock-safe by default):
  - `ARWEAVE_MODE` (`mock`|`http`) — mock persists snapshots locally; http logs intended requests only.
  - `ARWEAVE_HTTP_ENDPOINT`, `ARWEAVE_HTTP_API_KEY`, `ARWEAVE_HTTP_SIGNER` — only logged in http mode.
- Audit config: `AUDIT_LOG_DIR` (default `arweave/manifests`), `AUDIT_MAX_RECORDS` (default 1000 in-memory).

## License
Blackcat Darkmesh AO Proprietary License (see `LICENSE`). External contributions require written permission from Black Cat Academy s. r. o.
