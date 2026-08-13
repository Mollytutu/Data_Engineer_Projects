# Mock Citi-style banking data platform

This portfolio project demonstrates three different ingestion patterns used in
banking data engineering: batch files, real-time events, and operational
database change capture. The pipelines share account and client reference data,
write analytical data to Snowflake, and use dbt to publish tested business
tables.

## Architecture

```text
Pipeline 1 — batch files
Payment CSV -> S3 Raw -> Lambda -> AWS Glue
  -> S3 Curated / Rejected -> Snowflake -> dbt MARTS

Pipeline 2 — streaming
Payment producer -> Kafka -> Spark Structured Streaming
  -> S3 Curated / Quarantine -> Snowflake -> dbt MARTS

Pipeline 3 — database migration and CDC
Oracle/PostgreSQL -> AWS DMS full load + CDC -> S3 Bronze
  -> EMR/Glue Spark -> Iceberg Silver -> Snowflake -> dbt MARTS
```

Pipeline 3's production architecture uses Oracle/PostgreSQL and AWS DMS. Its
no-cost executable demonstration uses local PostgreSQL, a change-log trigger,
and a Python DMS-behavior simulator that captures full-load records plus
INSERT, UPDATE, and DELETE operations.

## Verified results

| Pipeline | Verification | Final business table |
| --- | --- | --- |
| Batch payments | 6,005 input = 5,023 curated + 982 rejected | `BANK_DB.MARTS.FCT_BATCH_PAYMENTS` (5,023 rows) |
| Payment streaming | Valid/rejected S3 outputs and durable Spark checkpoints | `BANK_DB.MARTS.FCT_PAYMENTS` (158 current payments) |
| Ledger CDC | Full load plus update/insert/delete capture | `BANK_DB.MARTS.FCT_ACCOUNT_LEDGER` (102 current transactions) |

The latest full dbt build completed 59/59 resources successfully. The final
test suite passes all 45 data tests, including uniqueness, required fields,
accepted values, reference relationships, and Pipeline 1 RAW-to-fact
reconciliation.

## Snowflake layers

```text
BANK_DB.RAW
  Source-aligned tables loaded from S3 and operational extracts

BANK_DB.STAGING
  STG_* cleaning, typing, and standardization views

BANK_DB.INTERMEDIATE
  Reusable enrichment and join views

BANK_DB.MARTS
  FCT_* and AGG_* business-facing tables
```

Business users query `BANK_DB.MARTS`, not `RAW` or `STAGING`.

## What is deployed and what is mocked

| Component | Project status |
| --- | --- |
| S3, Pipeline 1 Lambda trigger, and Pipeline 1 Glue job | Deployed and executed in AWS |
| Snowflake external stage, RAW tables, and dbt models | Deployed and tested |
| Kafka and Spark Structured Streaming | Executed locally with real S3 output |
| AWS MSK | Represented by local Kafka; not deployed |
| AWS DMS/Oracle | Production design; locally simulated with PostgreSQL |
| EMR Serverless/Iceberg | Deployment-ready Spark design; not deployed as paid infrastructure |
| Airflow/MWAA | Example orchestration DAG included; MWAA not deployed |

## Quick validation

From the repository root:

```bash
source scripts/use_local_env.sh
DBT_HARNESS_PARSE=1 bash scripts/run_harness.sh
dbt test --project-dir dbt_bank
```

Pipeline 1 Snowflake refresh:

```bash
dbt run-operation load_batch_payments --project-dir dbt_bank
dbt build --project-dir dbt_bank --select stg_batch_payments+
```

Detailed commands are in [the three-pipeline runbook](docs/pipelines-runbook.md).

## Repository map

- `data/raw/` — synthetic payments, ledger transactions, clients, and accounts.
- `ingestion/batch/` — S3-triggered Lambda handler for Pipeline 1.
- `glue/jobs/` — Pipeline 1 AWS Glue validation/transformation job.
- `streaming/` — Kafka producer and Spark Structured Streaming consumer.
- `ingestion/database_cdc/` — PostgreSQL schema and DMS behavior simulator.
- `spark/jobs/` — current-state and Bronze-to-Silver Spark jobs.
- `snowflake/` — Snowflake landing and merge definitions.
- `dbt_bank/` — staging, intermediate, marts, tests, and load macros.
- `airflow/` — MWAA/Airflow orchestration example.
- `docs/` — pipeline explanations and operating runbooks.
- `scripts/` and `harness/` — validation and operational helpers.

## Data safety

All included banking records are synthetic. Credentials are supplied through
local profiles, IAM roles, and ignored configuration files; secrets are not
stored in the repository.
