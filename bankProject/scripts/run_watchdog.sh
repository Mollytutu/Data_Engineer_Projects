#!/usr/bin/env bash
set -u

report_path="${WATCHDOG_REPORT_PATH:-watchdog/reports/latest.md}"
mkdir -p "$(dirname "$report_path")"

checks=(
  "structure|Architecture folders|scripts/check_structure.sh"
  "config|Configuration template|scripts/check_config.sh"
  "docs|Documentation coverage|scripts/check_docs.sh"
  "dbt|dbt scaffold|scripts/check_dbt.sh"
)

started_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
overall_status="PASS"
summary_rows=()
detail_blocks=()

echo "Running bank pipeline watchdog..."

for check_def in "${checks[@]}"; do
  IFS="|" read -r check_id check_name check_cmd <<< "$check_def"
  echo
  echo "==> $check_name"

  output_file="$(mktemp)"
  if bash "$check_cmd" >"$output_file" 2>&1; then
    status="PASS"
    echo "PASS: $check_name"
  else
    status="FAIL"
    overall_status="FAIL"
    echo "FAIL: $check_name"
  fi

  output="$(cat "$output_file")"
  rm -f "$output_file"

  summary_rows+=("| $check_id | $check_name | $status |")
  detail_blocks+=("## $check_name

Status: $status

\`\`\`text
$output
\`\`\`")
done

finished_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

{
  echo "# Bank Pipeline Watchdog Report"
  echo
  echo "- Status: $overall_status"
  echo "- Started: $started_at"
  echo "- Finished: $finished_at"
  echo
  echo "## Summary"
  echo
  echo "| Check | Area | Status |"
  echo "| --- | --- | --- |"
  for row in "${summary_rows[@]}"; do
    echo "$row"
  done
  echo
  for block in "${detail_blocks[@]}"; do
    echo "$block"
    echo
  done
} > "$report_path"

echo
echo "Watchdog report written to $report_path"

if [[ "$overall_status" != "PASS" ]]; then
  echo "Watchdog found failures."
  exit 1
fi

echo "Watchdog completed successfully."
