"""Local orchestration demo for Pipeline 3 database CDC."""

from datetime import datetime
import os
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = Path(os.getenv("BANK_PROJECT_ROOT", Path(__file__).resolve().parents[2]))

with DAG(
    dag_id="pipeline3_core_banking_cdc",
    description="Capture mock CDC, check Bronze, build ledger marts",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["pipeline3", "cdc", "ledger"],
) as dag:
    capture_cdc = BashOperator(
        task_id="capture_postgres_cdc",
        bash_command=(
            f"cd {PROJECT_ROOT} && python3 "
            "ingestion/database_cdc/mock_dms_to_s3.py cdc"
        ),
        env={
            "POSTGRES_DSN": "{{ var.value.get('pipeline3_postgres_dsn', '') }}",
            "CDC_OUTPUT_MODE": "s3",
            "CDC_S3_BUCKET": "bank-pipeline-project-molly",
        },
        append_env=True,
    )

    verify_bronze = BashOperator(
        task_id="verify_s3_bronze",
        bash_command=(
            f"cd {PROJECT_ROOT} && python3 scripts/verify_aws_pipeline_state.py "
            "s3 --bucket bank-pipeline-project-molly "
            "--prefix pipeline3/bronze/ --suffix .jsonl"
        ),
    )

    build_ledger_mart = BashOperator(
        task_id="build_and_test_ledger_mart",
        bash_command=(
            f"cd {PROJECT_ROOT} && dbt build --project-dir dbt_bank "
            "--profiles-dir dbt_bank "
            "--select +int_account_ledger_enriched fct_account_ledger"
        ),
    )

    capture_cdc >> verify_bronze >> build_ledger_mart
