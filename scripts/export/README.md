# Export scripts

Current tools:
- `audit_dump.lua [N] [process]` — prints last N lines (default 50) from the audit log (`AUDIT_LOG_DIR` or default `arweave/manifests`). Optional `process` reads `audit-<process>.log`.

Usage:
```bash
lua scripts/export/audit_dump.lua 50
# or per process
lua scripts/export/audit_dump.lua 50 site
```
