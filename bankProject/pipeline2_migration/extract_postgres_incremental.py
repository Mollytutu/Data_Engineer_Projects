from pathlib import Path
import os
import boto3
import pandas as pd
from sqlalchemy import create_engine

DB_URL = os.getenv(
    "BANK_DB_URL",
    "postgresql://localhost:5432/bank_oltp"
)

S3_BUCKET = "bank-pipeline-project-molly"
S3_PREFIX = "raw/account_ledger_transactions"

WATERMARK_FILE = Path(
    "pipeline2_migration/state/ledger_watermark.txt"
)

WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(DB_URL)

last_watermark = ""

if WATERMARK_FILE.exists():
    last_watermark = WATERMARK_FILE.read_text().strip()

if last_watermark:
    query = """
    SELECT *
    FROM account_ledger_transactions
    WHERE updated_at > %(watermark)s
    ORDER BY updated_at
    """

    df = pd.read_sql(
        query,
        engine,
        params={"watermark": last_watermark}
    )
else:
    query = """
    SELECT *
    FROM account_ledger_transactions
    ORDER BY updated_at
    """

    df = pd.read_sql(query, engine)

if df.empty:
    print("No new or changed rows found.")
    raise SystemExit(0)

new_watermark = df["updated_at"].max()

run_timestamp = pd.Timestamp.now(tz="UTC").strftime(
    "%Y%m%dT%H%M%SZ"
)

local_file = (
    f"/tmp/account_ledger_transactions_{run_timestamp}.parquet"
)

df.to_parquet(local_file, index=False)

s3_key = (
    f"{S3_PREFIX}/"
    f"account_ledger_transactions_{run_timestamp}.parquet"
)

s3 = boto3.client("s3")

s3.upload_file(
    local_file,
    S3_BUCKET,
    s3_key
)

WATERMARK_FILE.write_text(str(new_watermark))

print(f"Extracted {len(df)} rows")
print(f"New watermark: {new_watermark}")
print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")

audit_row = {
    "source_table": "account_ledger_transactions",
    "rows_extracted": len(df),
    "watermark": str(new_watermark),
    "s3_key": s3_key,
    "run_timestamp": run_timestamp
}

audit_df = pd.DataFrame([audit_row])

audit_file = (
    f"/tmp/account_ledger_transactions_audit_{run_timestamp}.parquet"
)

audit_df.to_parquet(audit_file, index=False)

audit_s3_key = (
    f"audit/account_ledger_transactions/"
    f"audit_{run_timestamp}.parquet"
)

s3.upload_file(
    audit_file,
    S3_BUCKET,
    audit_s3_key
)

print(f"Audit uploaded to s3://{S3_BUCKET}/{audit_s3_key}")