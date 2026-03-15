# AO/Write roadmap (high-level)

- 3‑DS/SCA resolver wiring: expose PaymentReturn endpoint on resolver and forward to write AO.
- Notifications: plug real email/SMS provider hooks; move notify_worker to consume queue with ACK.
- Search: typo tolerance done; add locale/category/pricing sort options at resolver layer.
- SEO: sitemap/structured data helper; locale fallback for content/layout.
- Analytics/Fraud: risk events and A/B hooks; integrate IP/device hashing.
- Subscriptions: recurring billing, proration, dunning hooks.
- Media/CDN: image optimization and cache headers.
