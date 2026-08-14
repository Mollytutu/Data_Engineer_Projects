"""Manual/local orchestration for Pipeline 1 batch payments."""

from datetime import datetime
import os
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = Path(os.getenv("BANK_PROJECT_ROOT", Path(__file__).resolve().parents[2]))

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
        bash_command=f"cd {PROJECT_ROOT} && python3 scripts/check_pipeline1_data.py",
    )

    verify_glue = BashOperator(
        task_id="verify_latest_glue_run",
        bash_command=(
            f"cd {PROJECT_ROOT} && python3 scripts/verify_aws_pipeline_state.py "
            "glue --job-name bank-batch-etl"
        ),
    )

    load_snowflake = BashOperator(
        task_id="load_snowflake_raw",
        bash_command=(
            f"cd {PROJECT_ROOT} && dbt run-operation load_batch_payments "
            "--project-dir dbt_bank --profiles-dir dbt_bank"
        ),
    )

    build_mart = BashOperator(
        task_id="build_and_test_batch_mart",
        bash_command=(
            f"cd {PROJECT_ROOT} && dbt build --project-dir dbt_bank "
            "--profiles-dir dbt_bank --select stg_batch_payments+"
        ),
    )

    validate_source >> verify_glue >> load_snowflake >> build_mart
