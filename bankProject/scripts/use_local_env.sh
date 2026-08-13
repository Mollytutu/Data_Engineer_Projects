#!/usr/bin/env bash

# Source this file from the repository root, as documented.
PROJECT_ENV_ROOT="$PWD"

export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PROJECT_ENV_ROOT/.venv/bin:$PATH"
export PYSPARK_PYTHON="$PROJECT_ENV_ROOT/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PROJECT_ENV_ROOT/.venv/bin/python"
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$PROJECT_ENV_ROOT/dbt_bank}"

echo "bankProject local environment enabled."
echo "JAVA_HOME=$JAVA_HOME"
echo "PYSPARK_PYTHON=$PYSPARK_PYTHON"
echo "dbt=$(command -v dbt)"
