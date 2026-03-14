#!/usr/bin/env python3
"""
Generate compact schema manifest for AO:
- Reads tables from schemas/canonical-db/tables/*.yaml (ao-table/v1)
- Reads view metadata from schemas/presets/canonical/views/**/*.yaml (ao-view/v1)
- Emits schemas/manifest/schema-manifest.json (no SQL bodies, hashes per source)
"""

import hashlib
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
TABLE_DIR = ROOT / "schemas" / "canonical-db" / "tables"
VIEW_DIR = ROOT / "schemas" / "presets" / "canonical" / "views"
WEAVEDB_DIR = ROOT / "schemas" / "weavedb" / "collections"
OUT = ROOT / "schemas" / "manifest" / "schema-manifest.json"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def load_tables():
    tables = {}
    for path in sorted(TABLE_DIR.glob("*.yaml")):
        data = yaml.safe_load(path.read_text()) or {}
        name = data.get("name") or path.stem
        tables[name] = {
            "summary": data.get("summary"),
            "columns": data.get("columns", {}),
            "fks": data.get("fks", []),
            "indexes": data.get("indexes", []),
            "format": data.get("format"),
            "hash": sha256(path),
            "source": str(path.relative_to(ROOT)),
        }
    return tables


def load_views():
    # Views metadata is optional; exclude if directory missing or empty
    views = {}
    if VIEW_DIR.exists():
        for path in sorted(VIEW_DIR.rglob("*.yaml")):
            data = yaml.safe_load(path.read_text()) or {}
            vid = data.get("id") or path.stem
            views[vid] = {
                "owner": data.get("owner"),
                "tags": data.get("tags", []),
                "requires": data.get("requires", []),
                "format": data.get("format"),
                "hash": sha256(path),
                "source": str(path.relative_to(ROOT)),
            }
    return views


def load_weavedb():
    cols = {}
    for path in sorted(WEAVEDB_DIR.glob("*.yaml")):
        data = yaml.safe_load(path.read_text()) or {}
        name = data.get("name") or path.stem
        cols[name] = {
            "schema": data.get("schema", {}),
            "indexes": data.get("indexes", []),
            "format": data.get("format"),
            "hash": sha256(path),
            "source": str(path.relative_to(ROOT)),
        }
    return cols


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "format": "ao-schema-manifest",
        "version": 3,
        "tables": load_tables(),
        "views": load_views(),       # optional metadata, not bundled if empty
        "weavedb": load_weavedb(),   # primary runtime schema
    }
    OUT.write_text(json.dumps(manifest, separators=(",", ":")))
    print(f"wrote {OUT} ({len(manifest['tables'])} tables, {len(manifest['views'])} views)")


if __name__ == "__main__":
    main()
