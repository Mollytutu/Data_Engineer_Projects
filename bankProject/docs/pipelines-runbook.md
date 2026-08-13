# Three-pipeline runbook

## Pipeline 1 — batch files

```bash
python scripts/check_pipeline1_data.py
aws s3 ls s3://bank-pipeline-project-molly/raw/ --recursive
aws s3 ls s3://bank-pipeline-project-molly/curated/ --recursive
aws s3 ls s3://bank-pipeline-project-molly/rejected/ --recursive
cd dbt_bank
dbt run-operation load_batch_payments
dbt build --select stg_batch_payments+
```

## Pipeline 2 — Kafka streaming

```bash
bash scripts/run_payment_stream_s3.sh
# In another terminal:
python streaming/producer/payment_event_producer.py
```

Then build the Snowflake analytics chain:

```bash
cd dbt_bank
dbt build --select stg_payments_current+
```

## Pipeline 3 — PostgreSQL migration/CDC

The no-cost implementation uses PostgreSQL plus the DMS behavior simulator;
production uses Oracle/PostgreSQL log-based AWS DMS.

```bash
export POSTGRES_DSN='postgresql://localhost/bank_oltp'
python ingestion/database_cdc/mock_dms_to_s3.py full-load
python ingestion/database_cdc/mock_dms_to_s3.py cdc
```

The real S3 Bronze prefix is `s3://bank-pipeline-project-molly/pipeline3/bronze/`.
Build the Snowflake analytics chain with:

```bash
cd dbt_bank
dbt build --select +int_account_ledger_enriched fct_account_ledger
```
