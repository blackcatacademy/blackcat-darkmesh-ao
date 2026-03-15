# Gateway model (notes)

> Shrnutí pro jednotnou gateway vrstvu, která obsluhuje libovolné domény v dané zemi, s cílem snížit latenci, energii a zvýšit odolnost proti cenzuře.

## Role a cíle
- **Univerzální resolver**: každá gateway umí servírovat jakýkoli tenant (abc.cz → gateway.cz/abc), cachuje frontend/AR obsah, dělá jen tenant‑scoped zápisy.
- **Latence**: malé množství gateway uzlů na region/země, lokální odpověď minimalizuje RTT (uživatel v Indii/Číně dostane obsah z lokální gateway).
- **Energie**: konsolidace výpočetních uzlů místo tisíců malých serverů.
- **Cenzurová odolnost**: obsah je na AR (permanentní), gateway je snadno nahraditelná; firewally blokující „pomalé/vzdálené“ zdroje mají menší účinek, pokud je gateway lokální.
- **PSP a PII**: platební údaje jdou přímo k providerovi; resolver drží minimum citlivých dat (WeaveDB, jednorázové tokeny).

## Trust & práva
- **Trusted list** (podepsaný autoritou): určuje, které gateway mají omezený write scope pro dané tenanty.
- **Auto‑flagging**: rate‑limit, replay okno, checksum/anomálie; podezřelé gateway/tenants lze automaticky zablokovat.
- **Scoped zápisy**: pouze akce nutné pro provoz webu/eshopu (orders, telemetry, inventory updates), bez přístupu k tajným klíčům uživatele.

## Toky
1) DNS/CNAME → gateway (např. gateway.cz/tenant).
2) Gateway → resolver (-ao) → AR/WeaveDB → cache → uživatel.
3) Write path: `POST /t/{tenant}/write` s tenantovým podpisem/trusted tokenem; audit do AR/WeaveDB.

## API nástin (v1)
- `GET /t/{tenant}/{path}` — serve obsah; headers: `X-Tenant`, `X-Signature` (časovaný podpis).
- `POST /t/{tenant}/write` — omezené operace (order create, telemetry, inventory); auth: trusted signature + rate‑limit.
- `GET /health`, `GET /metrics`.

## Cache / invalidace
- Per‑tenant cache (HTML/assets); TTL + event‑based purge po publish (hook z -write/-ao).
- Edge headers: Cache-Control/ETag už generuje -ao; gateway může volitelně propagovat CDN purge.

## Bezpečnost
- TLS povinně, per‑tenant rate‑limit/bot guard, replay window, audit log (hash → AR/WeaveDB).
- PSP: tokenizace, čísla karet nikdy neprochází gateway.

## Deployment checklist
- Geo‑pin (umístit gateway fyzicky v cílové zemi/regionu).
- Resource limity, ulimit, conntrack, DoS ochrana.
- Log ship do S3/Kafka (volitelně), retention.
- Rotace trusted listu/klíčů, případný AR mirror pro výpadky konektivity.

## TODO
- Formální specifikace podpisového schématu (header pole, expirace).
- Referenční implementace gateway služby (reverse proxy + auth middleware + cache).
- CI/CD šablona pro nasazení (container, systemd, k8s edge).
