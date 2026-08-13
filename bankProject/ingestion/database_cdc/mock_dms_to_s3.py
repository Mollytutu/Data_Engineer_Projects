"""Locally simulate AWS DMS full-load and CDC files written to S3 Bronze."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATE_PATH = PROJECT_ROOT / "data/checkpoints/pipeline3_mock_dms.json"
LOCAL_BRONZE = PROJECT_ROOT / "data/raw/pipeline3_s3_bronze"


def json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def load_last_change_id() -> int:
    if not STATE_PATH.exists():
        return 0
    return int(json.loads(STATE_PATH.read_text())["last_change_id"])


def save_last_change_id(change_id: int) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=STATE_PATH.parent, delete=False, encoding="utf-8"
    ) as handle:
        json.dump({"last_change_id": change_id}, handle, indent=2)
        handle.write("\n")
        temporary = handle.name
    os.replace(temporary, STATE_PATH)


def query(dsn: str, sql: str, parameters: tuple = ()) -> list[dict[str, Any]]:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())


def bronze_key(load_type: str) -> str:
    now = datetime.now(timezone.utc)
    return (
        "pipeline3/bronze/core_banking/account_ledger_transactions/"
        f"load_type={load_type}/extract_date={now:%Y-%m-%d}/"
        f"{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.jsonl"
    )


def write_bronze(records: list[dict[str, Any]], key: str) -> str:
    content = "".join(
        json.dumps({k: json_value(v) for k, v in row.items()}, separators=(",", ":"))
        + "\n"
        for row in records
    ).encode()
    if os.getenv("CDC_OUTPUT_MODE", "local").lower() == "s3":
        import boto3

        bucket = os.environ["CDC_S3_BUCKET"]
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=content)
        return f"s3://{bucket}/{key}"
    destination = LOCAL_BRONZE / key
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
    return str(destination)


def full_load(dsn: str) -> None:
    rows = query(
        dsn,
        "select * from account_ledger_transactions order by transaction_id",
    )
    records = [{"Op": "L", **row} for row in rows]
    destination = write_bronze(records, bronze_key("full"))
    baseline = query(dsn, "select coalesce(max(change_id), 0) as value from mock_dms_change_log")
    save_last_change_id(int(baseline[0]["value"]))
    print(f"DMS full load: {len(records)} rows -> {destination}")


def cdc_load(dsn: str) -> None:
    previous = load_last_change_id()
    changes = query(
        dsn,
        """
        select change_id, operation as "Op", changed_at, row_data
        from mock_dms_change_log
        where change_id > %s
        order by change_id
        """,
        (previous,),
    )
    if not changes:
        print(f"DMS CDC: no changes after change_id={previous}")
        return
    records = []
    for change in changes:
        row = dict(change.pop("row_data"))
        records.append({**change, **row})
    destination = write_bronze(records, bronze_key("cdc"))
    # Advance only after the entire Bronze object is durable.
    save_last_change_id(int(changes[-1]["change_id"]))
    print(f"DMS CDC: {len(records)} changes -> {destination}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("full-load", "cdc"))
    args = parser.parse_args()
    dsn = os.environ["POSTGRES_DSN"]
    full_load(dsn) if args.mode == "full-load" else cdc_load(dsn)


if __name__ == "__main__":
    main()

