#!/usr/bin/env python3
"""
Schema helper for WeaveDB bundles.
Commands:
  list                         - show available collections with preset tags
  pick --presets core,commerce - generate subset manifest to stdout
  export --presets core,ebook --out bundle.tar.gz
  explain <collection>         - print JSON Schema + indexes
"""

import argparse
import hashlib
import json
import os
import sys
import tarfile
from pathlib import Path

import yaml

# --- styling ---------------------------------------------------------------
USE_COLOR = sys.stdout.isatty() and os.getenv("NO_COLOR") is None


class C:
    RESET = "\033[0m" if USE_COLOR else ""
    BOLD = "\033[1m" if USE_COLOR else ""
    DIM = "\033[2m" if USE_COLOR else ""
    CYAN = "\033[96m" if USE_COLOR else ""
    MAGENTA = "\033[95m" if USE_COLOR else ""
    YELLOW = "\033[93m" if USE_COLOR else ""
    GREEN = "\033[92m" if USE_COLOR else ""
    BLUE = "\033[94m" if USE_COLOR else ""


def banner():
    if not USE_COLOR:
        print("=== WeaveDB Schema Helper ===")
        return
    art = [
        "╔═══════════════════════════════════════╗",
        "║  WEAVEDB SCHEMA HELPER  ·  v3 bundles ║",
        "╚═══════════════════════════════════════╝",
    ]
    colors = [C.CYAN, C.MAGENTA, C.BLUE]
    for i, line in enumerate(art):
        print(colors[i % len(colors)] + line + C.RESET)

ROOT = Path(__file__).resolve().parents[2]
COLL_DIR = ROOT / "schemas" / "weavedb" / "collections"
MANIFEST_PATH = ROOT / "schemas" / "manifest" / "schema-manifest.json"

# Lightweight preset catalogue (feel free to adjust)
PRESETS = {
    "core": {
        "description": "sites/pages/routes/assets/users/auth basics",
        "collections": [
            "sites", "pages", "routes", "assets", "users",
            "roles", "permissions", "sessions", "audit",
            "entitlements", "rate_limits", "settings", "message_templates",
        ],
    },
    "commerce": {
        "description": "products/orders/payments/logistics/marketing",
        "collections": [
            "products", "product_variants", "product_media", "product_files",
            "product_bundles", "categories", "inventory", "inventory_locations",
            "inventory_reservations", "inventory_adjustments",
            "carts", "cart_events", "orders", "payments", "refunds", "shipments",
            "returns", "return_items", "price_rules", "coupons", "gift_cards",
            "gift_wrapping", "gift_messages",
            "loyalty_points", "wishlists", "reviews", "event_outbox",
            "rate_limits", "entitlements", "segments", "customer_groups",
            "analytics_events", "webhooks", "newsletter_subscribers",
            "support_tickets", "abuse_reports", "risk_events",
            "store_hours", "store_holidays", "store_pickup_slots",
            "shipping_methods", "shipping_rates", "tax_rates", "tax_exemptions",
            "navigation", "seo_metadata", "redirects",
        ],
    },
    "ebook": {
        "description": "authors/publishers/files/licenses/reading progress",
        "collections": [
            "authors", "publishers", "series", "product_series",
            "product_files", "licenses", "downloads", "reading_progress",
            "blog_posts", "comments",
        ],
    },
    "subscriptions": {
        "description": "plans/subscriptions/invoices",
        "collections": ["plans", "subscriptions", "invoices"],
    },
    "content": {
        "description": "CMS fragments/SEO/navigation",
        "collections": ["fragments", "navigation", "seo_metadata", "redirects", "policy_versions"],
    },
    "full": {
        "description": "All collections",
        "collections": [],  # will be filled dynamically
    },
}


def load_collections():
    cols = {}
    for path in sorted(COLL_DIR.glob("*.yaml")):
        data = yaml.safe_load(path.read_text()) or {}
        name = data.get("name") or path.stem
        cols[name] = {
            "schema": data.get("schema", {}),
            "indexes": data.get("indexes", []),
            "format": data.get("format"),
            "source": str(path.relative_to(ROOT)),
        }
    return cols


def calc_sha256(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def build_manifest_subset(collections: dict, chosen: set) -> dict:
    subset = {k: v for k, v in collections.items() if (not chosen or k in chosen)}
    manifest = {
        "format": "ao-schema-manifest",
        "version": 3,
        "tables": {},   # keep reference empty to stay lean
        "views": {},
        "weavedb": subset,
    }
    return manifest


def cmd_list(collections):
    banner()
    all_names = sorted(collections)
    print(f"{C.BOLD}Collections ({len(all_names)} total){C.RESET}")
    for name in all_names:
        print(f"  {C.CYAN}•{C.RESET} {name}")
    print(f"\n{C.BOLD}Presets{C.RESET}")
    for pid, meta in PRESETS.items():
        print(f"  {C.MAGENTA}{pid:12s}{C.RESET} {meta['description']}")


def cmd_explain(collections, name):
    if name not in collections:
        print(f"Collection '{name}' not found.")
        return
    print(json.dumps(collections[name], indent=2))


def cmd_pick(collections, presets):
    chosen = set()
    for p in presets:
        if p == "full":
            chosen = set(collections.keys())
            break
        if p not in PRESETS:
            print(f"Unknown preset: {p}")
            continue
        chosen |= set(PRESETS[p]["collections"])
    manifest = build_manifest_subset(collections, chosen)
    print(json.dumps(manifest, separators=(",", ":")))


def cmd_export(collections, presets, out_path: Path):
    chosen = set()
    for p in presets:
        if p == "full":
            chosen = set(collections.keys())
            break
        if p not in PRESETS:
            print(f"Unknown preset: {p}")
            continue
        chosen |= set(PRESETS[p]["collections"])
    manifest = build_manifest_subset(collections, chosen)

    temp_manifest = ROOT / "schemas" / "manifest" / "schema-manifest.tmp.json"
    temp_manifest.write_text(json.dumps(manifest, separators=(",", ":")))

    with tarfile.open(out_path, "w:gz") as tar:
        tar.add(temp_manifest, arcname="schemas/manifest/schema-manifest.json")

    sha = calc_sha256(out_path)
    temp_manifest.unlink()
    print(f"{C.GREEN}✔{C.RESET} Wrote bundle: {out_path}")
    print(f"{C.GREEN}✔{C.RESET} SHA256: {C.BOLD}{sha}{C.RESET}")


def main():
    collections = load_collections()
    PRESETS["full"]["collections"] = list(collections.keys())

    parser = argparse.ArgumentParser(description="WeaveDB schema helper")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("list", help="List collections and presets")

    p_explain = sub.add_parser("explain", help="Show schema of a collection")
    p_explain.add_argument("name")

    p_pick = sub.add_parser("pick", help="Print manifest subset to stdout")
    p_pick.add_argument("--presets", required=True, help="Comma-separated presets (core,commerce,ebook,full,...)")

    p_export = sub.add_parser("export", help="Write bundle tar.gz with manifest subset")
    p_export.add_argument("--presets", required=True, help="Comma-separated presets")
    p_export.add_argument("--out", required=True, help="Output tar.gz path")

    args = parser.parse_args()

    if args.cmd == "list":
        cmd_list(collections)
    elif args.cmd == "explain":
        cmd_explain(collections, args.name)
    elif args.cmd == "pick":
        presets = [p.strip() for p in args.presets.split(",") if p.strip()]
        cmd_pick(collections, presets)
    elif args.cmd == "export":
        presets = [p.strip() for p in args.presets.split(",") if p.strip()]
        out = Path(args.out)
        cmd_export(collections, presets, out)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
