# Payment stream consumer

`payment_stream_processor.py` reads `payment_events` from local Kafka, validates each event against the client and account reference data, and writes Parquet output either locally or directly to Amazon S3.

## What the processor does

`payment_stream_processor.py` is a PySpark Structured Streaming job. Spark is the processing engine inside this script. Spark processes events only while the consumer command is running; Kafka continues retaining messages when the consumer is stopped.

```text
payment_event_producer.py
    │ sends JSON payment lifecycle events
    ▼
Kafka topic: payment_events
    │ Spark reads retained and new events
    ▼
payment_stream_processor.py
    ├── parse and normalize JSON
    ├── join client and account references
    ├── validate banking and data-quality rules
    ├── deduplicate valid event_id values
    │
    ├── valid ─────> data/curated/payment_events/
    └── rejected ──> data/quarantine/payment_events/
```

For each event, Spark:

1. Reads the Kafka key, JSON payload, topic, partition, offset, and Kafka timestamp.
2. Converts the JSON payload into typed columns.
3. Normalizes controlled values such as currency, payment type, direction, and status.
4. Joins `clients.csv` and `accounts.csv` to enrich and verify the event.
5. Confirms that the client and account exist and are active.
6. Confirms that the account belongs to the stated client and that its currency matches.
7. Validates required identifiers, payment type, direction, amount, country, lifecycle status, and timestamps.
8. Rejects events arriving more than ten minutes after their business event time.
9. Removes repeated valid events with the same `event_id` for the lifetime of the checkpoint.
10. Writes valid and rejected events to separate Parquet datasets.

For example:

```text
CLIENT-0001 + ACC-000001 + AED + positive amount
    → valid → data/curated/payment_events/

CLIENT-0001 + missing account_id
    → rejected → data/quarantine/payment_events/
                 error_code=MISSING_ACCOUNT_ID
```

Kafka and Spark have different responsibilities. Kafka stores and transports the event stream. Spark reads that stream, applies the processing rules, and writes analytical datasets. When Spark restarts, its checkpoint makes it continue from the last completed Kafka offsets and preserves the set of `event_id` values already accepted.

## Outputs

Local mode is the default:

```text
data/curated/payment_events/       valid, deduplicated events
data/quarantine/payment_events/    invalid or late events
data/checkpoints/payment_events/   Kafka offsets and Spark query state
```

S3 mode writes directly from Spark on every completed micro-batch:

```text
s3a://bank-pipeline-project-molly/streaming/curated/payment_events/
s3a://bank-pipeline-project-molly/streaming/quarantine/payment_events/
s3a://bank-pipeline-project-molly/streaming/checkpoints/payment_events/
```

Kafka's broker files are never copied. Spark consumes records through the Kafka API and writes the processed results to S3.

These runtime directories are ignored by Git. Checkpoints make later runs resume from the last processed Kafka offsets and preserve deduplication state. Do not delete a checkpoint while keeping its output dataset: replaying without that state can write duplicates. For an intentional full rebuild, replace both the output and its checkpoint together.

## Setup

From the repository root:

```bash
source scripts/use_local_env.sh
python -m pip install -r requirements-streaming.txt
```

The project uses Java 17. PySpark 4.0.1 uses the Scala 2.13 Kafka connector with the same Spark version.

## Run locally

Start Kafka first, then submit the consumer with its Kafka connector:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  streaming/consumer/payment_stream_processor.py
```

On the first run, `startingOffsets=earliest` processes events already retained in the topic. Once checkpoints exist, Spark resumes from those checkpointed offsets.

Run the producer in a second terminal:

```bash
source scripts/use_local_env.sh
python streaming/producer/payment_event_producer.py
```

Stop the consumer with `Ctrl+C`. The producer stops automatically after ten minutes.

## Run with S3 storage

The bucket must exist and the active AWS identity must have permission to list the bucket and read, write, and delete objects under the configured prefix. Never place access keys in this repository.

Use the reusable launcher from the repository root:

```bash
bash scripts/run_payment_stream_s3.sh
```

The launcher selects the project Java and Python environments, loads credentials from the active AWS CLI profile without saving them, validates the bucket, sets S3 mode, and starts Spark with both required connectors. It runs continuously until stopped with `Ctrl+C`.

You may override the destination without editing code:

```bash
PAYMENT_S3_BUCKET=my-bucket \
PAYMENT_S3_PREFIX=my-prefix \
bash scripts/run_payment_stream_s3.sh
```

The equivalent manual commands are shown below for reference.

Export credentials from the active AWS CLI profile into the consumer process, select S3 mode, and include the Hadoop AWS connector that matches Spark's Hadoop 3.4.1 runtime:

```bash
source scripts/use_local_env.sh
eval "$(aws configure export-credentials --format env)"

export PAYMENT_STORAGE_MODE=s3
export PAYMENT_S3_BUCKET=bank-pipeline-project-molly
export PAYMENT_S3_PREFIX=streaming

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.hadoop:hadoop-aws:3.4.1 \
  streaming/consumer/payment_stream_processor.py
```

The S3 checkpoint is separate from the local checkpoint. On the first S3 run, Spark reads the Kafka topic from `earliest`; later S3 runs resume from the offsets saved under the S3 checkpoint prefix.

Verify the output without downloading it:

```bash
aws s3 ls s3://bank-pipeline-project-molly/streaming/ --recursive --summarize
```

## Validation behavior

The consumer validates JSON parsing, required identifiers, the Kafka key, timestamps, late arrival, active clients and accounts, account ownership and currency, payment type, direction, amount, country code, and lifecycle status. Valid duplicate `event_id` values are removed using checkpointed, unbounded state. This simple approach prevents old duplicates during the project's short local runs, although its state would grow indefinitely on a permanent production stream. Invalid events retain their original Kafka payload and metadata in quarantine.

## Idempotency

- The producer enables Kafka idempotence with `acks=all`, preventing Kafka retries for one send operation from creating duplicate records.
- An intentionally duplicated fixture sends the same `event_id` twice so consumer deduplication can be tested.
- The valid Spark stream keeps accepted `event_id` values in checkpointed state and writes only the first occurrence.
- Spark file-sink commit logs prevent a completed micro-batch from being appended again during normal checkpoint recovery.
- `payment_id` is not a deduplication key because one payment legitimately produces multiple lifecycle events.

This guarantee depends on keeping the output and checkpoint together. A production stream with unlimited retention should use a transactional sink such as Delta Lake or Iceberg and enforce `event_id` uniqueness with a merge operation rather than keeping unbounded Spark state forever.
