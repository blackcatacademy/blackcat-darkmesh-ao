#!/usr/bin/env python3
"""
Glow-up quickstart for Darkmesh AO schemas.
Features:
  - Detect Windows / WSL / Linux and print ready-to-copy commands.
  - Discover the freshest bundle automatically.
  - One-key actions (list, suggest presets, export bundle, deps check).
  - Pretty gradient UI + inline manifest info.
"""

import glob
import os
import platform
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUNDLES_DIR = ROOT / "dev" / "schema-bundles"
MANIFEST_TX = "iygsD6GhCXGI1cXrl2lw6VOpxbjwISZO5pqWmo7y8XM"
MANIFEST_SHA = "b1ee8a00d4d2c989c4d7a88daf1ca45c0ea70fb0037dd8b688d44d05f9f534d5"
SCHEMA_HELPER = ROOT / "scripts" / "setup" / "schema_helper.py"
DEPS_CHECK = ROOT / "scripts" / "verify" / "deps_check.lua"

# --- styling -----------------------------------------------------------------
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
    ORANGE = "\033[38;5;208m" if USE_COLOR else ""
    PINK = "\033[38;5;213m" if USE_COLOR else ""


def grad(text):
    palette = [C.ORANGE, C.PINK, C.MAGENTA, C.CYAN, C.GREEN]
    if not USE_COLOR:
        return text
    return "".join(palette[i % len(palette)] + ch for i, ch in enumerate(text)) + C.RESET


def banner():
    if not USE_COLOR:
        print("=== Blackcat Darkmesh AO · Quickstart ===")
        return
    art = [
        "╔════════════════════════════════════════════╗",
        "║   ◎  DARKMESH AO · SCHEMA LAUNCHPAD v3    ║",
        "╚════════════════════════════════════════════╝",
    ]
    for i, line in enumerate(art):
        color = [C.CYAN, C.MAGENTA, C.BLUE][i % 3]
        print(color + line + C.RESET)


# --- helpers -----------------------------------------------------------------
def is_windows():
    return platform.system().lower().startswith("win")


def is_wsl():
    rel = platform.release().lower()
    return "microsoft" in rel or "wsl" in rel


def latest_bundle() -> Path | None:
    pattern = str(BUNDLES_DIR / "schema-bundle-*.tar.gz")
    matches = sorted(glob.glob(pattern))
    if not matches:
        return None
    return Path(matches[-1])


def run(cmd):
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        print(f"{C.YELLOW}!{C.RESET} Command not found: {cmd[0]}")
        sys.exit(1)
    except subprocess.CalledProcessError as exc:
        print(f"{C.YELLOW}!{C.RESET} Command failed (exit {exc.returncode})")
        sys.exit(exc.returncode)


def section(title):
    print(f"\n{C.BOLD}{grad(title)}{C.RESET}")


def path_hints(bundle: Path | None):
    if bundle:
        stamp = datetime.fromtimestamp(bundle.stat().st_mtime).isoformat(timespec="seconds")
        print(f"{C.DIM}Latest bundle:{C.RESET} {bundle} ({stamp})")
    else:
        print(f"{C.YELLOW}!{C.RESET} No bundles found in {BUNDLES_DIR}")

    win_tip = r'npx arkb deploy "{path}" --content-type application/gzip'
    nix_tip = "npx arkb deploy {path} --content-type application/gzip"

    if bundle:
        bundle_posix = bundle.as_posix()
        print("Windows / PowerShell:")
        print("  " + win_tip.format(path=bundle_posix))
        if is_wsl():
            # WSL users often need the Windows path form for arkb in PowerShell
            try:
                win_path = subprocess.check_output(["wslpath", "-w", bundle_posix], text=True).strip()
                print(f"  (WSL path) {win_tip.format(path=win_path)}")
            except Exception:
                pass
        print("Linux / WSL:")
        print("  ./scripts/setup/build_schema_bundle.sh")
        print("  " + nix_tip.format(path=bundle_posix))


def manifest_card():
    section("Deployed manifest")
    print(f"  tx  : {C.CYAN}{MANIFEST_TX}{C.RESET}")
    print(f"  sha : {C.CYAN}{MANIFEST_SHA}{C.RESET}")
    print(f"  src : schemas/manifest/schema-manifest.json")


def do_list():
    run([sys.executable, str(SCHEMA_HELPER), "list"])


def do_suggest():
    prompt = input("Describe your project (e.g. 'ebook shop with subscriptions'): ").strip()
    if not prompt:
        print(f"{C.YELLOW}!{C.RESET} Empty prompt, skipping.")
        return
    run([sys.executable, str(SCHEMA_HELPER), "suggest", "--prompt", prompt])


def do_export():
    presets = input("Presets (comma separated, default: core,commerce,content,ebook,subscriptions): ").strip()
    if not presets:
        presets = "core,commerce,content,ebook,subscriptions"
    target = latest_bundle() or (BUNDLES_DIR / "schema-bundle-quick.tar.gz")
    out = input(f"Output path [{target}]: ").strip() or str(target)
    run(
        [
            sys.executable,
            str(SCHEMA_HELPER),
            "export",
            "--presets",
            presets,
            "--out",
            out,
        ]
    )


def do_wizard():
    print(f"{C.BOLD}{grad('Site bundle wizard')}{C.RESET}")
    site = input("Site slug (letters/digits/hyphen): ").strip() or "site"
    default_presets = "core,content,commerce"
    presets = input(f"Presets [{default_presets}]: ").strip() or default_presets
    extra = input("Extra collections (comma, optional): ").strip()
    if extra:
        presets = presets + "," + extra

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out = BUNDLES_DIR / f"schema-bundle-{site}-{ts}.tar.gz"
    print(f"{C.DIM}Exporting to {out}{C.RESET}")
    run(
        [
            sys.executable,
            str(SCHEMA_HELPER),
            "export",
            "--presets",
            presets,
            "--out",
            str(out),
        ]
    )
    print(f"\nDeploy with arkb (PowerShell):")
    print(f'  npx arkb deploy "{out.as_posix()}" --content-type application/gzip')
    print(f"Deploy (Linux/WSL):")
    print(f"  npx arkb deploy {out.as_posix()} --content-type application/gzip")


def do_deps_check():
    if not DEPS_CHECK.exists():
        print(f"{C.YELLOW}!{C.RESET} deps_check.lua not found at {DEPS_CHECK}")
        return
    cmd = ["lua", str(DEPS_CHECK)]
    print(f"{C.DIM}Running:{C.RESET} {' '.join(cmd)}")
    run(cmd)


def menu():
    options = {
        "1": ("List collections", do_list),
        "2": ("Suggest presets from prompt", do_suggest),
        "3": ("Export bundle (choose presets)", do_export),
        "4": ("Wizard: make site bundle", do_wizard),
        "5": ("Run deps check (lua libs)", do_deps_check),
        "0": ("Exit", None),
    }
    print("")
    for key, (label, _) in options.items():
        bullet = "⦿" if key != "0" else "○"
        print(f" {C.ORANGE}{bullet}{C.RESET} [{key}] {label}")
    choice = input("> ").strip()
    if choice not in options:
        print(f"{C.YELLOW}!{C.RESET} Unknown choice.")
        return True
    action = options[choice][1]
    if action:
        print("")
        action()
        return True
    return False


def main():
    banner()
    section("Env")
    os_name = platform.system()
    extra = "WSL" if is_wsl() else ""
    print(f"  OS   : {C.CYAN}{os_name}{' · ' + extra if extra else ''}{C.RESET}")
    print(f"  Root : {ROOT}")

    manifest_card()
    section("Deploy commands")
    path_hints(latest_bundle())

    section("Pick an action")
    while True:
        if not menu():
            break
        print("")  # spacing


if __name__ == "__main__":
    main()
