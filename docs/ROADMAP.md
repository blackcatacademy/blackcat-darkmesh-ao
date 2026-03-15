# AO/Write roadmap (high-level)

- 3‑DS/SCA resolver wiring: expose PaymentReturn endpoint on resolver and forward to write AO.
- Notifications: plug real email/SMS provider hooks; move notify_worker to consume queue with ACK.
- Search: typo tolerance done; add locale/category/pricing sort options at resolver layer.
- SEO: sitemap/structured data helper; locale fallback for content/layout.
- Analytics/Fraud: risk events and A/B hooks; integrate IP/device hashing.
- Subscriptions: recurring billing, proration, dunning hooks.
- Media/CDN: image optimization and cache headers.

## Final push to 99%+ coverage (proposed)
### -ao (runtime)
- **PSP production adapters**: Stripe/Adyen/PayPal/Apple/Google Pay via `psp_call` interface; webhook replay protection; token reuse; SCA challenge handling.
- **Search/recos v2**: facet/typo, synonyms/stopwords per locale, popularity signals, related/recent/bestseller endpoints with segment flags.
- **A11y/perf guardrails**: enforce perf budgets (LCP/CLS/TBT), blur/lazyload defaults, ARIA/contrast checks in pipelines, edge cache headers per route.
- **B2B**: company accounts + net terms/credit limits, PO approvals, invoicing/fiscal receipts, rate-limit/bot guard, GA4/Kafka/S3 telemetry export.
- **Resilience**: serve-stale-if-error, cache stampede single-flight, per-tenant rate limits, signed trusted list consumption for gateways.

### -write (authoring)
- **Workflow**: draft→review→approve/publish, scheduled publish/expire, comments, locks/CRDT for concurrent editing.
- **Content types**: registry-driven schemas (blog/landing/FAQ/collection/banner) without code deploy; per-locale content storage.
- **Localization/SEO**: locale routing/prefixes, hreflang/canonical, sitemap/robots generation from content manifest.
- **Media**: upload/transform (webp/avif/thumbs), cache-busting manifests, CDN purge hook.
- **Admin UX**: price lists/promos editor (percent/amount/BOGO/free-ship), tax class/region rules, inventory/backorder settings.
- **Observability**: audit streams, perf/vitals dashboard, publish pipeline logs; export hooks for GA4/Kafka/S3.

### Delivery order (suggested)
1) PSP production adapters + webhook verify (ao).
2) Authoring workflow + content-type registry (write).
3) Search/recos v2 + localization/SEO wiring (ao/write).
4) Media/A11y/perf guardrails and cache policies (ao/write).
5) B2B/net terms + fiscal receipts + telemetry export (ao).
