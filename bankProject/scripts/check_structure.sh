#!/usr/bin/env bash
set -euo pipefail

required_paths=(
  "configs/config.yaml"
  "data/raw"
  "data/curated"
  "ingestion/batch"
  "ingestion/streaming"
  "ingestion/database_cdc"
  "glue/jobs"
  "spark/jobs"
  "streaming/kafka"
  "snowflake/raw"
  "snowflake/stages"
  "snowflake/load_scripts"
  "snowflake/warehouse"
  "dbt_bank/models/staging"
  "dbt_bank/models/intermediate"
  "dbt_bank/models/core"
  "dbt_bank/models/marts"
  "reporting"
  "fraud"
  "reconciliation"
  "tests"
  "docs"
  "harness"
  "watchdog"
  "airflow/dags"
)

for path in "${required_paths[@]}"; do
  if [[ ! -e "$path" ]]; then
    echo "Missing required path: $path"
    exit 1
  fi
done

echo "Project structure is valid."
