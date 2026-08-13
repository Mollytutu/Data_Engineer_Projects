# AWS Glue jobs

- `process_batch_payments.py` implements Pipeline 1: it validates batch payment
  CSVs against account/client references and writes curated and rejected
  Parquet datasets.
- Pipeline 3's Glue-compatible ledger job is under
  `pipeline2_migration/glue/transform_ledger.py` for historical naming reasons.
