# Data Model

Primary: document/NoSQL keyed maps (sites, domains, routes, pages, layouts, menus, products, categories, versions, asset_refs, entitlements). Keep hot state normalized and small.

Immutable payloads (large pages, media, catalog exports) live on Arweave; AO state stores only references and hashes.
