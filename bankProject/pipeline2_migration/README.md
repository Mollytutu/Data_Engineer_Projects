# PostgreSQL-to-S3 migration extract

`extract_postgres_to_s3.py` reads the current
`account_ledger_transactions` table from PostgreSQL, converts its rows to
Parquet, and uploads the file to S3.

```text
PostgreSQL -> Python extractor -> S3 raw
```

Run from the `bankProject` root:

```bash
source .venv/bin/activate
python -m pip install -r requirements-database-cdc.txt
export BANK_DB_URL='postgresql+psycopg://localhost:5432/bank_oltp'
python pipeline2_migration/extract_postgres_to_s3.py
```

The default destination is:

```text
s3://bank-pipeline-project-molly/
  raw/account_ledger_transactions/account_ledger_transactions.parquet
```

This script performs a full snapshot, not CDC. It does not capture deletes.
Production log-based CDC should use AWS DMS reading PostgreSQL WAL or Oracle
redo logs.
