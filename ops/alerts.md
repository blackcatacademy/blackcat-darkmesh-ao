# Sample Prometheus alerts

```
# Checksum daemon failures
- alert: AOChecksumDaemonDown
  expr: probe_success{job="ao-checksum"} == 0
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "AO checksum daemon not running"

# Rate limit error spikes (example metric name if exported)
- alert: AORateLimitErrors
  expr: increase(ao_rate_limited_total[5m]) > 10
  labels:
    severity: warning
  annotations:
    summary: "AO rate-limit errors high"

# Outbox/WAL checksum drift
- alert: AOChecksumMismatch
  expr: increase(ao_checksum_mismatch_total[5m]) > 0
  labels:
    severity: critical
  annotations:
    summary: "AO checksum mismatch detected"

# Queue lag (if exported)
- alert: AOQueueLagHigh
  expr: ao_outbox_queue_size > 100
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "AO outbox queue backlog high"
```

Adjust metric names to your scrape config; checksum daemon can expose a blackbox probe or use systemd service monitor.
