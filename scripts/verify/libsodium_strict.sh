#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import importlib
mods = ["sodium", "ed25519"]
missing = [m for m in mods if importlib.util.find_spec(m) is None]
if missing:
    raise SystemExit(f"Missing modules: {missing}")
PY
