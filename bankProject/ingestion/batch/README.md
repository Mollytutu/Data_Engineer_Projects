# Pipeline 1 batch ingestion

```text
payment CSV -> S3 raw/payments -> Lambda -> AWS Glue
  -> S3 curated/payments + rejected/payments
```

`lambda_start_payment_glue.py` handles S3 object-created events and starts the
Glue job only for CSV objects under `raw/payments/`. The Glue script is
`glue/jobs/process_batch_payments.py`.

The Lambda execution role needs `glue:StartJobRun`. The Glue role needs read
access to `raw/` and write access to `curated/` and `rejected/`. Configure the
Glue job with maximum concurrency 1 because the current job overwrites its
outputs. Production ingestion should use per-batch prefixes and a load-control
table for concurrent arrivals.
