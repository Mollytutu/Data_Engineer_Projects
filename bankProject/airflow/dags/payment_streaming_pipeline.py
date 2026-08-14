"""Downstream orchestration for the continuously running Pipeline 2 stream."""

from datetime import datetime
import os
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = Path(os.getenv("BANK_PROJECT_ROOT", Path(__file__).resolve().parents[2]))

with DAG(
    dag_id="pipeline2_payment_streaming_downstream",
    description="Check streaming S3 output and refresh Snowflake/dbt marts",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["pipeline2", "kafka", "spark", "streaming"],
) as dag:
    verify_s3_output = BashOperator(
        task_id="verify_streaming_s3_output",
        bash_command=(
            f"cd {PROJECT_ROOT} && python3 scripts/verify_aws_pipeline_state.py "
            "s3 --bucket bank-pipeline-project-molly "
            "--prefix streaming/curated/payment_events/ --suffix .parquet"
        ),
    )

    build_payment_marts = BashOperator(
        task_id="build_and_test_payment_marts",
        bash_command=(
            f"cd {PROJECT_ROOT} && dbt build --project-dir dbt_bank "
            "--profiles-dir dbt_bank --select stg_payments_current+"
        ),
    )

    verify_s3_output >> build_payment_marts
