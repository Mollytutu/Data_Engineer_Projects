# Documentation

Architecture notes, runbooks, data contracts, and operating procedures for the bank pipeline.

## Index

- `harness-framework.md` - local validation framework, check layers, and extension points.
- `project-watchdog.md` - watchdog behavior, alert conditions, report output, and CI usage.
- `tooling.md` - installed terminal tools, local environment setup, and CLI notes.
- `pipeline1-batch-payments.md` - batch S3/Lambda/Glue implementation and validation.
- `pipeline2-payment-streaming.md` - Kafka/Spark/S3/Snowflake pipeline.
- `pipeline3-core-banking-cdc.md` - database migration/CDC pipeline.
- `pipelines-runbook.md` - concise commands for all three pipelines.

## Current Control Flow

```text
Harness -> validates scaffold, config, docs, and dbt layers
Watchdog -> runs checks and writes alert-style reports
CI -> runs harness and watchdog automatically
```

## Local Commands

```bash
bash scripts/run_harness.sh
bash scripts/run_watchdog.sh
```

The latest watchdog report is written to:

```text
watchdog/reports/latest.md
```
