# Gateway model (outline)

> A lightweight gateway layer that serves any tenant in a country/region, minimizing latency/energy and increasing censorship resistance. All content lives on AR; the gateway is replaceable and holds only cache + scoped write rights.

## Goals
- **Universal resolver:** gateway can serve any tenant (abc.cz → gateway.cz/abc), caching frontend + AR content; supports only tenant-scoped writes.
- **Latency:** a few gateways per country/region for near‑web2 RTT (e.g., users in India/China served locally).
- **Energy:** consolidation of compute instead of thousands of lightly used origin servers.
- **Censorship resistance:** AR content is permanent; gateways are replaceable. Local gateways make “fast reply” firewalls less effective; blocking requires killing the local node, not a distant origin.
- **Privacy/PSP:** card data goes straight to PSP; resolver holds minimal sensitive data (WeaveDB + one‑time tokens).

## Trust & permissions
- **Trusted list** (signed by an authority): which gateways may serve tenants and perform limited writes.
- **Scoped writes:** only the operations needed for normal site/eshop operation (orders, telemetry, inventory). No access to user secrets.
- **Auto‑flagging:** rate limits, replay window, checksum/manifest mismatch alerts; flagged gateway/tenant can be quarantined.

## Data flows
1) DNS/CNAME → gateway (e.g., gateway.cz/tenant).
2) Gateway → resolver (-ao) → AR/WeaveDB → cache → user.
3) Write path: `POST /t/{tenant}/write` with tenant signature/trusted token; audit to AR/WeaveDB.

## API sketch (v1)
- `GET /t/{tenant}/{path}` — serve content; headers: `X-Tenant`, `X-Signature`, `X-Timestamp`, `X-Nonce`.
- `POST /t/{tenant}/write` — restricted ops (order create, telemetry, inventory); same auth; rate limited.
- `GET /health`, `GET /metrics`.

### Signature scheme (proposal)
- Header fields:  
  - `X-Tenant` (tenant id/domain)  
  - `X-Timestamp` (unix seconds)  
  - `X-Nonce` (random, max 36 chars)  
  - `X-Signature` (hex HMAC-SHA256)  
- Message to sign: `X-Tenant || "\n" || X-Timestamp || "\n" || X-Nonce || "\n" || HTTP_METHOD || "\n" || PATH || "\n" || SHA256(body or "")`
- Key: per-gateway shared secret derived from trusted list entry.  
- Replay window: 300s; nonces cached per tenant to prevent replay.  
- Required for both GET and POST; GET uses empty body hash.

## Cache / invalidation
- Per-tenant cache (HTML/assets); TTL + event-based purge after publish (hook from -write/-ao).
- Honors Cache-Control/ETag from -ao; optional CDN purge command.

## Security
- TLS required; per-tenant rate limits/bot guard; replay window + nonce cache; audit hashes optionally pinned to AR/WeaveDB.
- PSP: tokenization only; card numbers never traverse gateway.

## Deployment checklist
- Geo-pin (place gateway physically in target country/region).
- Resource limits (ulimit/conntrack), basic DoS protection.
- Log shipping to S3/Kafka (optional), retention policy.
- Key rotation and trusted list refresh; optional AR mirror for connectivity loss.

## Reference implementation (todo)
- Reverse proxy (e.g., nginx/openresty or lightweight Go/Rust service) with:  
  - auth middleware (signature verify, replay window, rate limit)  
  - cache layer (per-tenant, path-aware)  
  - backend resolver client (-ao) with retries/backoff  
  - write proxy restricted to allowed actions  
  - metrics (Prometheus) + health endpoints
- CI/CD template (container + systemd/k8s edge deployment).

## Open items
- Finalize trusted list format (AR-hosted manifest?).
- Provide sample config + env for sandbox deployment.
- Add integration tests for signature verification and replay defence.
