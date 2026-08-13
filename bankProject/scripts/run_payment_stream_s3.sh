#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"
source scripts/use_local_env.sh

for command_name in aws spark-submit; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command is not installed: $command_name" >&2
    exit 1
  fi
done

export PAYMENT_STORAGE_MODE="s3"
export PAYMENT_S3_BUCKET="${PAYMENT_S3_BUCKET:-bank-pipeline-project-molly}"
export PAYMENT_S3_PREFIX="${PAYMENT_S3_PREFIX:-streaming}"

# Load credentials from the active AWS CLI profile into this process. No
# credentials are printed or stored in the repository.
eval "$(aws configure export-credentials --format env)"

echo "Checking S3 bucket: s3://$PAYMENT_S3_BUCKET"
aws s3api head-bucket --bucket "$PAYMENT_S3_BUCKET" >/dev/null

echo "Starting payment stream consumer"
echo "Storage: s3://$PAYMENT_S3_BUCKET/$PAYMENT_S3_PREFIX/"

exec spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.hadoop:hadoop-aws:3.4.1 \
  streaming/consumer/payment_stream_processor.py
