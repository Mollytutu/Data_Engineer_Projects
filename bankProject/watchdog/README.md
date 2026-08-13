# Watchdog

The watchdog runs the project harness as a monitoring-style check and writes a Markdown report.

Run it from the repository root:

```bash
bash scripts/run_watchdog.sh
```

The default report is written to:

```text
watchdog/reports/latest.md
```

Set `WATCHDOG_REPORT_PATH` to write the report somewhere else.
