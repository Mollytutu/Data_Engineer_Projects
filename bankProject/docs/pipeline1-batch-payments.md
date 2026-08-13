# Pipeline 1 — batch payment files

```text
Payment CSV -> S3 raw -> S3 event -> Lambda -> AWS Glue
  -> S3 curated Parquet + rejected Parquet
  -> Snowflake RAW.BATCH_PAYMENTS
  -> dbt stg_batch_payments -> fct_batch_payments
```

## Implemented components

- Raw payment/account/client fixtures under `data/raw`.
- Real S3 raw objects under `s3://bank-pipeline-project-molly/raw/`.
- Lambda entry point: `ingestion/batch/lambda_start_payment_glue.py`.
- Glue transformation: `glue/jobs/process_batch_payments.py`.
- Existing real S3 outputs under `curated/` and `rejected/`.
- Snowflake load macro: `dbt_bank/macros/load_batch_payments.sql`.
- Tested dbt staging and business fact models.

The final business-facing table is
`BANK_DB.MARTS.FCT_BATCH_PAYMENTS`; staging models remain in
`BANK_DB.STAGING`.
- Deterministic local contract check: `scripts/check_pipeline1_data.py`.

The batch contains 6,005 rows: 6,000 IDs plus five duplicate rows. It also
contains exactly five missing accounts, five invalid currencies, five negative
amounts, and five invalid dates. Glue joins reference data, validates ownership
and currency, and separates accepted and rejected records.

## Reproduce

```bash
python scripts/check_pipeline1_data.py
aws s3 cp data/raw/payments/payments.csv \
  s3://bank-pipeline-project-molly/raw/payments/payments.csv
cd dbt_bank
dbt run-operation load_batch_payments
dbt build --select stg_batch_payments+
```

In the deployed design, the S3 notification invokes Lambda, which starts Glue.
The deployed AWS trigger and Glue job are active in the project account. The
Snowflake macro loads the curated snapshot through the existing external S3
stage, and dbt publishes the tested business fact.
