# Project Watchdog

The project watchdog is the alerting layer for this mock bank pipeline. It wraps the harness checks, records pass/fail status, and writes a report that can be reviewed locally or uploaded from CI.

## Local Use

```bash
bash scripts/run_watchdog.sh
```

## Alerts

The watchdog exits with a failure when it detects:

- Missing required architecture folders.
- Missing required config keys.
- Missing README coverage for major pipeline areas.
- Missing dbt model layer registrations.
- Failed optional dbt parsing when `DBT_HARNESS_PARSE=1`.

## Reports

By default, the report is written to:

```text
watchdog/reports/latest.md
```

Override the location with:

```bash
WATCHDOG_REPORT_PATH=/tmp/bank-watchdog.md bash scripts/run_watchdog.sh
```
