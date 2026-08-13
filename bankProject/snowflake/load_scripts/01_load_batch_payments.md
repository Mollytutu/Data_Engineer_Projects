# Load Pipeline 1 curated payments

The executable Snowflake load is implemented as the dbt macro
`load_batch_payments` so it uses the project's existing authenticated Snowflake
profile and `BANK_DB.EXTERNAL.BANK_S3_STAGE`.

```bash
cd dbt_bank
dbt run-operation load_batch_payments
dbt build --select stg_batch_payments+
```

The macro recreates the RAW snapshot from S3 curated Parquet. Snowflake records
the source object in `source_s3_file`, and dbt enforces unique payment IDs.

