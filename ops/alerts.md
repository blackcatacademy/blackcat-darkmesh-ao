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
```

Adjust metric names to your scrape config; checksum daemon can expose a blackbox probe or use systemd service monitor.
