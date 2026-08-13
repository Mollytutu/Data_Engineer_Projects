"""Manual/local orchestration for Pipeline 1 batch payments."""

from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parents[2]

with DAG(
    dag_id="pipeline1_batch_payments",
    description="Validate batch input, check Glue, load Snowflake, run dbt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["pipeline1", "batch", "glue", "snowflake"],
) as dag:
    validate_source = BashOperator(
        task_id="validate_source_fixture",
        bash_command=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/check_pipeline1_data.py",
    )

    verify_glue = BashOperator(
        task_id="verify_latest_glue_run",
        bash_command=(
            "aws glue get-job-runs --job-name bank-batch-etl --max-results 1 "
            "--query 'JobRuns[0].JobRunState' --output text | grep SUCCEEDED"
        ),
    )

    load_snowflake = BashOperator(
        task_id="load_snowflake_raw",
        bash_command=(
            f"cd {PROJECT_ROOT} && .venv/bin/dbt run-operation "
            "load_batch_payments --project-dir dbt_bank"
        ),
    )

    build_mart = BashOperator(
        task_id="build_and_test_batch_mart",
        bash_command=(
            f"cd {PROJECT_ROOT} && .venv/bin/dbt build --project-dir dbt_bank "
            "--select stg_batch_payments+"
        ),
    )

    validate_source >> verify_glue >> load_snowflake >> build_mart

