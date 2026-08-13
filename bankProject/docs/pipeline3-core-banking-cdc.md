# Pipeline 3 — Oracle/core banking CDC

## Business problem

Migrate historical treasury ledger data and continuously replicate subsequent
inserts, updates, and deletes without repeatedly extracting the entire Oracle
database.

## Architecture

```text
Oracle Exadata
  -> AWS DMS full load + redo-log CDC
  -> S3 Bronze JSON/Parquet
  -> EMR Serverless PySpark
  -> Glue Catalog + Iceberg Silver
  -> Snowflake RAW
  -> dbt staging + fct_account_ledger Gold mart
```

MWAA checks DMS health, submits EMR, triggers the Snowflake merge, runs dbt and
reconciles control totals. IAM roles provide credentials in AWS.

The final business-facing table is `BANK_DB.MARTS.FCT_ACCOUNT_LEDGER`.

## Local proof

PostgreSQL and `mock_dms_to_s3.py` simulate Oracle and DMS without cloud cost.
The verified run produced:

- 100 records in the historical full load;
- four CDC records: one update, two inserts, one delete;
- zero records on an immediate CDC rerun.

The local Spark job reads both immutable Bronze batches, selects the newest
operation per `transaction_id`, removes delete tombstones, validates types and
writes a 101-row current-state Silver snapshot: 100 + 2 inserts - 1 delete.

## Idempotency

- DMS/checkpoint position prevents rereading acknowledged source changes.
- Bronze objects are immutable and uniquely named.
- Spark orders changes by `change_id` and keeps one current row per transaction.
- Iceberg provides atomic snapshots.
- Snowflake MERGE uses `transaction_id` and `cdc_sequence`, including deletes.
- dbt tests require unique, non-null transaction IDs.
