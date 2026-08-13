#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f "dbt_bank/dbt_project.yml" ]]; then
  echo "Missing dbt project file: dbt_bank/dbt_project.yml"
  exit 1
fi

required_model_paths=(
  "staging"
  "intermediate"
  "core"
  "marts"
)

for path in "${required_model_paths[@]}"; do
  if [[ ! -d "dbt_bank/models/$path" ]]; then
    echo "dbt project is missing model layer: models/$path"
    exit 1
  fi
done

if [[ "${DBT_HARNESS_PARSE:-0}" == "1" ]] && command -v dbt >/dev/null 2>&1; then
  (
    cd dbt_bank
    dbt parse
  )
elif [[ "${DBT_HARNESS_PARSE:-0}" == "1" ]]; then
  echo "dbt executable not found; skipped dbt parse."
else
  echo "Set DBT_HARNESS_PARSE=1 to run dbt parse."
fi

echo "dbt harness checks passed."
