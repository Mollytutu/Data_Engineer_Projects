# Pipeline 3: Oracle/core banking CDC

## Production architecture

```text
Oracle Exadata / core banking OLTP
  -> AWS DMS full load + log-based CDC (INSERT, UPDATE, DELETE)
  -> S3 Bronze
  -> EMR Serverless PySpark validation, standardization, deduplication
  -> Iceberg Silver
  -> Snowflake
  -> dbt Gold/marts
```

MWAA/Airflow monitors DMS, starts the EMR transformation, loads Snowflake,
runs dbt and quality tests, and performs reconciliation. Airflow does not poll
Oracle for changes; AWS DMS reads Oracle redo/archive logs continuously.

## Why there is a local simulator

The portfolio architecture remains AWS DMS. `mock_dms_to_s3.py` makes the DMS
behavior observable without paying for DMS or Oracle: PostgreSQL stands in for
Oracle, a trigger stands in for redo/WAL capture, and the script emits full-load
and CDC records in an immutable S3 Bronze layout. It includes `Op` values:

- `L`: historical full-load row
- `I`: inserted row
- `U`: updated row image
- `D`: deleted row image

Unlike an `updated_at` query, this design captures deletes.

## Lesson 1: full load and CDC

The visible 100-row source fixture is
`data/raw/core_banking/account_ledger_transactions.csv`. It is generated from
active account/client pairs in `data/raw/reference`, so every transaction uses
an existing account, the account's actual owner, and the account's currency.
Regenerate it with:

```bash
python ingestion/database_cdc/generate_ledger_fixture.py
```

```bash
createdb bank_oltp
psql bank_oltp -f ingestion/database_cdc/schema.sql
psql bank_oltp -f ingestion/database_cdc/seed_initial.sql
export POSTGRES_DSN='postgresql://localhost/bank_oltp'
python ingestion/database_cdc/mock_dms_to_s3.py full-load
```

The full load writes 100 historical rows, L001-L100, and records the change-log
position. Simulate
ongoing banking activity and capture only its changes:

```bash
psql bank_oltp -f ingestion/database_cdc/simulate_changes.sql
python ingestion/database_cdc/mock_dms_to_s3.py cdc
python ingestion/database_cdc/mock_dms_to_s3.py cdc
```

The first CDC run writes one update, two inserts (L101-L102), and one delete.
The second
writes nothing because its ordered change position is already checkpointed.

Set `CDC_OUTPUT_MODE=s3` and `CDC_S3_BUCKET` to write the same Bronze keys to
AWS. The next implementation layer is the EMR Serverless PySpark job that
merges these operation records into an Iceberg Silver table.
