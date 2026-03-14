#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="$ROOT/dev/schema-bundles"
mkdir -p "$OUT_DIR"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE="$OUT_DIR/schema-bundle-$TS.tar.gz"

tar -czf "$ARCHIVE" -C "$ROOT" schemas/presets config/table-presets.json
SHA="$(sha256sum "$ARCHIVE" | awk '{print $1}')"

echo "wrote bundle: $ARCHIVE"
echo "sha256: $SHA"
echo "$SHA  $(basename "$ARCHIVE")" > "$ARCHIVE.sha256"
