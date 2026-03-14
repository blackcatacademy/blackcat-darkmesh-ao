# Process Topology

Initial split: `registry`, `site`, `catalog`, `access`. Each process owns its state, handlers, and tests. Registry is low-churn (domains, versions, roles), Site is main content runtime, Catalog separates commerce load, Access guards protected assets.
