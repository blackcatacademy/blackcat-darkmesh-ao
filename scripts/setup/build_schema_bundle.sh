#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="$ROOT/dev/schema-bundles"
mkdir -p "$OUT_DIR"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE="$OUT_DIR/schema-bundle-$TS.tar.gz"

# Re-generate compact manifest (v3) from WeaveDB collections
python3 "$ROOT/scripts/setup/make_schema_manifest.py"

# Bundle manifest + preset catalog only (no SQL bodies, no view metadata)
tar -czf "$ARCHIVE" -C "$ROOT" schemas/manifest/schema-manifest.json config/table-presets.json
SHA="$(sha256sum "$ARCHIVE" | awk '{print $1}')"

echo "wrote bundle: $ARCHIVE"
echo "sha256: $SHA"
echo "$SHA  $(basename "$ARCHIVE")" > "$ARCHIVE.sha256"
