#!/usr/bin/env python3
"""
User-friendly startup guide.
- Detect OS (Windows vs Linux/WSL) and suggest the right commands.
- Quick actions: list collections, export full bundle.
"""
import platform
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "dev" / "schema-bundles" / "schema-bundle-latest.tar.gz"
MANIFEST_TX = "iygsD6GhCXGI1cXrl2lw6VOpxbjwISZO5pqWmo7y8XM"
MANIFEST_SHA = "b1ee8a00d4d2c989c4d7a88daf1ca45c0ea70fb0037dd8b688d44d05f9f534d5"


def is_windows():
    return platform.system().lower().startswith("win")


def run(cmd):
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        sys.exit(1)


def print_header():
    print("=== Blackcat Darkmesh AO · Quickstart ===")
    print(f"Root: {ROOT}")
    print(f"Deployed schema: tx={MANIFEST_TX} sha={MANIFEST_SHA}")
    print("")


def suggest_paths():
    if is_windows():
        bundle_path = str(ROOT / "dev" / "schema-bundles" / "schema-bundle-*.tar.gz")
        print("Windows shell tips:")
        print(f"  npx arkb deploy \"{bundle_path}\" --content-type application/gzip")
    else:
        print("Linux/WSL shell tips:")
        print("  ./scripts/setup/build_schema_bundle.sh")
        print("  npx arkb deploy ./dev/schema-bundles/schema-bundle-*.tar.gz --content-type application/gzip")
    print("")


def quick_actions():
    print("Quick actions:")
    print(" 1) List collections (schema_helper list)")
    print(" 2) Export full bundle (schema_helper export --presets full)")
    print(" 0) Exit")
    choice = input("> ").strip()
    if choice == "1":
        run([sys.executable, str(ROOT / "scripts" / "setup" / "schema_helper.py"), "list"])
    elif choice == "2":
        out = ROOT / "dev" / "schema-bundles" / "schema-bundle-quick.tar.gz"
        run([sys.executable, str(ROOT / "scripts" / "setup" / "schema_helper.py"), "export", "--presets", "full", "--out", str(out)])
    else:
        print("Bye.")


def main():
    print_header()
    suggest_paths()
    quick_actions()


if __name__ == "__main__":
    main()
