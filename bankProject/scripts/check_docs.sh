#!/usr/bin/env bash
set -euo pipefail

documented_dirs=(
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
  "dbt_bank"
  "dbt_bank/models/staging"
  "dbt_bank/models/intermediate"
  "dbt_bank/models/core"
  "dbt_bank/models/marts"
  "reporting"
  "fraud"
  "reconciliation"
  "tests"
  "scripts"
  "docs"
  "harness"
  "watchdog"
  "airflow"
)

for dir in "${documented_dirs[@]}"; do
  readme="$dir/README.md"
  if [[ ! -s "$readme" ]]; then
    echo "Missing or empty README: $readme"
    exit 1
  fi
done

echo "Documentation coverage is valid."
