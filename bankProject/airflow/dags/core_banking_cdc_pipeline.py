"""Local orchestration demo for Pipeline 3 database CDC."""

from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parents[2]

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
            f"cd {PROJECT_ROOT} && .venv/bin/python "
            "ingestion/database_cdc/mock_dms_to_s3.py cdc"
        ),
        env={"POSTGRES_DSN": "{{ var.value.get('pipeline3_postgres_dsn', '') }}"},
        append_env=True,
    )

    verify_bronze = BashOperator(
        task_id="verify_s3_bronze",
        bash_command=(
            "aws s3 ls s3://bank-pipeline-project-molly/pipeline3/bronze/ "
            "--recursive | grep jsonl"
        ),
    )

    build_ledger_mart = BashOperator(
        task_id="build_and_test_ledger_mart",
        bash_command=(
            f"cd {PROJECT_ROOT} && .venv/bin/dbt build --project-dir dbt_bank "
            "--select +int_account_ledger_enriched fct_account_ledger"
        ),
    )

    capture_cdc >> verify_bronze >> build_ledger_mart

