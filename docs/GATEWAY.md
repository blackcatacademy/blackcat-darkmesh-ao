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
- **Auto-flagging:** rate limits, replay window, checksum/manifest mismatch alerts; flagged gateway/tenant can be quarantined.
- **Trusted list format (proposal):**
  ```json
  {
    "version": 1,
    "issuedAt": "2026-03-15T00:00:00Z",
    "gateways": [
      {
        "id": "gw-eu-cz-1",
        "domains": ["gateway.cz"],
        "regions": ["CZ", "SK", "PL"],
        "pubkey": "ed25519:...",
        "scopes": ["serve", "write-limited"],
        "expiresAt": "2026-09-15T00:00:00Z"
      }
    ],
    "signature": "ed25519:..."  // signed by authority key
  }
  ```
  - Hosted on AR; gateway refreshes on interval; expired entries dropped.

## Data flows
1) DNS/CNAME → gateway (e.g., gateway.cz/tenant).
2) Gateway → resolver (-ao) → AR/WeaveDB → cache → user.
3) Write path: `POST /t/{tenant}/write` with tenant signature/trusted token; audit to AR/WeaveDB.

### Reference flow (serve path)
1) Parse tenant from host/path → map to tenant id + AR manifest.
2) Verify signature headers (timestamp/nonce); enforce replay window.
3) Rate limit (tenant+IP buckets).
4) Cache lookup (RAM → disk). If hit, serve.
5) On miss, fetch from resolver (-ao), verify AR manifest hash, store in cache, serve.
6) Emit metrics + structured log.

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
- Serve stale-if-error: if AR/resolver unreachable, serve cached copy up to a max `stale_ttl` and surface warning in metrics/logs.

## Security
- TLS required; per-tenant rate limits/bot guard; replay window + nonce cache; audit hashes optionally pinned to AR/WeaveDB.
- PSP: tokenization only; card numbers never traverse gateway.

## Deployment checklist
- Geo-pin (place gateway physically in target country/region).
- Resource limits (ulimit/conntrack), basic DoS protection.
- Log shipping to S3/Kafka (optional), retention policy.
- Key rotation and trusted list refresh; optional AR mirror for connectivity loss.
- Tenant mapping config (example):
  ```yaml
  tenants:
    - domain: "abc.cz"
      id: "abc"
      ar_manifest: "ar://txid"
      cache_ttl: 300
    - domain: "xyz.cn"
      id: "xyz"
      ar_manifest: "ar://txid2"
      cache_ttl: 120
  ```

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

## Key/manifest rotation (proposal)
- Gateway pulls trusted list from AR every `TRUST_REFRESH_SEC` (e.g., 5 min).
- Keep both **current** and **next** manifests if signatures are valid and `issuedAt` within skew.
- Overlap window: accept keys from either manifest when verifying `X-Signature`.
- Drop expired entries (`expiresAt < now`).
- On fetch failure: continue with last good manifest; log + metric `gateway_trust_refresh_fail`.

## Auth middleware pseudocode
```
verify(req):
  t = req.headers["X-Timestamp"]
  nonce = req.headers["X-Nonce"]
  tenant = req.headers["X-Tenant"]
  sig = req.headers["X-Signature"]
  if abs(now - t) > 300: reject
  if nonce seen for tenant: reject
  manifest = trusted_list()
  key = manifest.key_for(tenant or gateway_id)
  msg = build_signed_message(req, t, nonce, tenant)
  if !hmac_or_ed25519_verify(msg, sig, key): reject
  cache_nonce(tenant, nonce)
  if over_rate_limit(tenant, req.ip, req.path): reject
  return ok
```

## Metrics (Prometheus) suggestions
- `gateway_requests_total{tenant,method,status}`  
- `gateway_request_duration_seconds_bucket{tenant,method}`  
- `gateway_cache_hit_ratio{tenant}`  
- `gateway_auth_fail_total{reason}` (sig, replay, expired, rate_limited)  
- `gateway_purge_total{tenant}`  
- `gateway_stale_served_total{tenant}`  
- `gateway_trust_refresh_fail_total`  
- `gateway_write_denied_total{tenant,reason}`  
- `gateway_rate_limit_dropped_total{tenant}`  

## Sample deployment (systemd)
```ini
[Unit]
Description=gateway
After=network-online.target

[Service]
ExecStart=/usr/local/bin/gateway --config /etc/gateway.yaml
Restart=always
User=gateway
LimitNOFILE=131072

[Install]
WantedBy=multi-user.target
```

## Tests to add
- Signature happy path + tamper/replay/expired timestamp.
- Rate limit: ensure drops after threshold per tenant/IP.
- Cache: hit/miss, purge after publish event, stale-if-error path.
- Trusted list rotation: accept during overlap, reject expired.
