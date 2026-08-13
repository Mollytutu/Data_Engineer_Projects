"""Extract PostgreSQL account ledger transactions directly to Amazon S3."""

import os
import boto3
import pandas as pd
from sqlalchemy import create_engine

DB_URL = os.getenv(
    "BANK_DB_URL",
    "postgresql+psycopg://localhost:5432/bank_oltp"
)

# A plain PostgreSQL URL makes SQLAlchemy look for the older psycopg2 package.
# This project uses Psycopg 3, whose SQLAlchemy driver name is `psycopg`.
if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)

S3_BUCKET = "bank-pipeline-project-molly"
S3_KEY = "raw/account_ledger_transactions/account_ledger_transactions.parquet"

engine = create_engine(DB_URL)

query = """
SELECT *
FROM account_ledger_transactions
"""

df = pd.read_sql(query, engine)

local_file = "/tmp/account_ledger_transactions.parquet"

df.to_parquet(local_file, index=False)

s3 = boto3.client("s3")

s3.upload_file(
    local_file,
    S3_BUCKET,
    S3_KEY
)

print(f"Extracted {len(df)} rows")
print(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")


