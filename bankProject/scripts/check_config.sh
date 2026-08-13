#!/usr/bin/env bash
set -euo pipefail

config_file="${1:-configs/config.yaml}"

required_patterns=(
  "^ingestion:"
  "^[[:space:]]+batch_schedule:"
  "^[[:space:]]+kafka_bootstrap:"
  "^snowflake:"
  "^[[:space:]]+account:"
  "^[[:space:]]+user:"
  "^[[:space:]]+role:"
)

if [[ ! -f "$config_file" ]]; then
  echo "Missing config file: $config_file"
  exit 1
fi

for pattern in "${required_patterns[@]}"; do
  if ! grep -Eq "$pattern" "$config_file"; then
    echo "Config validation failed. Missing pattern: $pattern"
    exit 1
  fi
done

echo "Config template is valid."
