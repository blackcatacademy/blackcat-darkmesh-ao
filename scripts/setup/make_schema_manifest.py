#!/usr/bin/env python3
"""
Generate a compact JSON manifest from canonical YAML schemas.
- Tables: taken from schemas/canonical-db/schema-defs.yaml
- Views:   all files under schemas/presets/canonical/**/*.yaml
Output: schemas/manifest/schema-manifest.json (no whitespace, sha256 included per file)
"""

import hashlib
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
TABLE_DEFS = ROOT / "schemas" / "canonical-db" / "schema-defs.yaml"
PRESETS_DIR = ROOT / "schemas" / "presets" / "canonical"
OUT = ROOT / "schemas" / "manifest" / "schema-manifest.json"


def sha256(path: Path) -> str:
  h = hashlib.sha256()
  h.update(path.read_bytes())
  return h.hexdigest()


def load_tables():
  data = yaml.safe_load(TABLE_DEFS.read_text())
  tables = data.get("Tables", {})
  out = {}
  for name, body in tables.items():
    out[name] = {
      "summary": body.get("Summary"),
      "columns": body.get("Columns", {}),
      "hash": sha256(TABLE_DEFS),
    }
  return out


def load_views():
  out = {}
  for path in PRESETS_DIR.rglob("*.yaml"):
    data = yaml.safe_load(path.read_text()) or {}
    views = data.get("Views", {})
    for vid, body in views.items():
      out[vid] = {
        "owner": body.get("Owner"),
        "tags": body.get("Tags", []),
        "requires": body.get("Requires", []),
        "engine": "postgres",
        "source": str(path.relative_to(ROOT)),
        "hash": sha256(path),
      }
  return out


def main():
  OUT.parent.mkdir(parents=True, exist_ok=True)
  manifest = {
    "format": "ao-schema-manifest",
    "version": 1,
    "tables": load_tables(),
    "views": load_views(),
  }
  OUT.write_text(json.dumps(manifest, separators=(",", ":")))
  print(f"wrote {OUT} ({len(manifest['tables'])} tables, {len(manifest['views'])} views)")


if __name__ == "__main__":
  main()
