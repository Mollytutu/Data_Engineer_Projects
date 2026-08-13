# Scripts

Local development, validation, deployment, and operational helper scripts.

## Commands

- `bash scripts/check_structure.sh` validates that required architecture folders exist.
- `bash scripts/check_config.sh` validates the shared config template has required sections.
- `bash scripts/check_docs.sh` validates each major folder has a README.
- `bash scripts/run_harness.sh` runs the full local harness.
- `bash scripts/run_watchdog.sh` runs the watchdog and writes a Markdown report.
- `bash scripts/run_payment_stream_s3.sh` runs the continuous Spark payment consumer with direct S3 output using the active AWS CLI profile.
