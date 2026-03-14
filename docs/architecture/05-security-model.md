# Security Model

Write path: signed writes only plus role checks. Enforce schema + size validation, idempotence (publishId/requestId/nonce), and append-only publish history. Never store private keys or secrets in AO state or Arweave metadata; only references and hashes are permitted.
