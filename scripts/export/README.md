# Export scripts

Current tools:
- `audit_dump.lua [N] [process]` — prints last N lines (default 50) from the audit log (`AUDIT_LOG_DIR` or default `arweave/manifests`). Optional `process` reads `audit-<process>.log`.
- `audit_export.lua [process|all] [format] [outfile]` — concatenate audit logs; `format`=`ndjson` (default) or `raw`; optional `outfile` (otherwise stdout).

Usage:
```bash
lua scripts/export/audit_dump.lua 50
# or per process
lua scripts/export/audit_dump.lua 50 site

# export all audit logs to NDJSON file
lua scripts/export/audit_export.lua all ndjson /tmp/audit.ndjson
```
