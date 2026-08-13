#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
airflow_version="3.3.0"
python_version="3.11"

cd "$project_root"
python3.11 -m venv .airflow-venv
.airflow-venv/bin/python -m pip install --upgrade pip
.airflow-venv/bin/python -m pip install \
  "apache-airflow==$airflow_version" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$airflow_version/constraints-$python_version.txt"

export AIRFLOW_HOME="$project_root/.airflow-home"
export AIRFLOW__CORE__DAGS_FOLDER="$project_root/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

.airflow-venv/bin/airflow db migrate
echo "Airflow $airflow_version installed and initialized."

