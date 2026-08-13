#!/usr/bin/env bash
set -euo pipefail

checks=(
  "scripts/check_structure.sh"
  "scripts/check_config.sh"
  "scripts/check_docs.sh"
  "scripts/check_dbt.sh"
  "scripts/check_pipeline1_data.sh"
)

echo "Running bank pipeline harness..."

for check in "${checks[@]}"; do
  echo
  echo "==> $check"
  bash "$check"
done

echo
echo "Harness completed successfully."
