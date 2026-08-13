# Pipeline 2 — payment lifecycle streaming

```text
Producer -> Kafka payment_events -> Spark Structured Streaming
  -> S3 curated/quarantine + checkpoints
  -> current payment snapshot -> Snowflake -> dbt marts
```

## Verified components

- Producer emits reference-linked lifecycle events and controlled bad records.
- Producer stops after ten minutes and uses Kafka idempotent delivery settings.
- Spark validates, enriches, quarantines, and deduplicates `event_id`.
- S3 contains curated events, quarantined events, and durable checkpoints.
- `build_payments_current.py` creates one latest row per `payment_id`.
- Snowflake RAW sources feed `stg_payments_current`, `fct_payments`, and payment
  status/type aggregates.
- dbt models and all selected tests pass.

The final business-facing table is `BANK_DB.MARTS.FCT_PAYMENTS`, with payment
status and type aggregates in the same MARTS schema.

Run instructions and exact paths are in `streaming/consumer/README.md`.
