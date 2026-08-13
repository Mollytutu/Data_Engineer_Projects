#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

export AIRFLOW_HOME="$project_root/.airflow-home"
export AIRFLOW__CORE__DAGS_FOLDER="$project_root/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

exec .airflow-venv/bin/airflow standalone

