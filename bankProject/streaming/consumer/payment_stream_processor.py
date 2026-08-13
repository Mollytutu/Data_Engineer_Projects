"""Validate Kafka payment events and route them to local Parquet datasets."""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "payment_events"
LATE_EVENT_THRESHOLD = "10 minutes"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = PROJECT_ROOT / "data"

STORAGE_MODE = os.getenv("PAYMENT_STORAGE_MODE", "local").strip().lower()
S3_BUCKET = os.getenv(
    "PAYMENT_S3_BUCKET", "bank-pipeline-project-molly"
).strip()
S3_PREFIX = os.getenv("PAYMENT_S3_PREFIX", "streaming").strip().strip("/")

if STORAGE_MODE == "s3":
    if not S3_BUCKET:
        raise ValueError("PAYMENT_S3_BUCKET is required when storage mode is s3")
    STORAGE_ROOT = f"s3a://{S3_BUCKET}/{S3_PREFIX}"
    VALID_OUTPUT = f"{STORAGE_ROOT}/curated/payment_events"
    REJECTED_OUTPUT = f"{STORAGE_ROOT}/quarantine/payment_events"
    VALID_CHECKPOINT = f"{STORAGE_ROOT}/checkpoints/payment_events/valid"
    REJECTED_CHECKPOINT = f"{STORAGE_ROOT}/checkpoints/payment_events/rejected"
elif STORAGE_MODE == "local":
    VALID_OUTPUT = str(DATA_ROOT / "curated/payment_events")
    REJECTED_OUTPUT = str(DATA_ROOT / "quarantine/payment_events")
    VALID_CHECKPOINT = str(DATA_ROOT / "checkpoints/payment_events/valid")
    REJECTED_CHECKPOINT = str(DATA_ROOT / "checkpoints/payment_events/rejected")
else:
    raise ValueError("PAYMENT_STORAGE_MODE must be 'local' or 's3'")

ACCOUNTS_PATH = PROJECT_ROOT / "data/raw/reference/accounts.csv"
CLIENTS_PATH = PROJECT_ROOT / "data/raw/reference/clients.csv"

VALID_PAYMENT_TYPES = [
    "ACH",
    "WIRE",
    "SWIFT",
    "SEPA",
    "CARD",
    "BOOK_TRANSFER",
    "FX_TRANSFER",
    "REAL_TIME_PAYMENT",
]

VALID_DIRECTIONS = ["INBOUND", "OUTBOUND"]

VALID_STATUSES = [
    "RECEIVED",
    "VALIDATED",
    "APPROVED",
    "AUTHORIZED",
    "CAPTURED",
    "SANCTIONS_SCREENING",
    "SUBMITTED_TO_ACH",
    "SUBMITTED_TO_SEPA",
    "SENT_TO_NETWORK",
    "SENT_TO_SWIFT",
    "POSTED_TO_LEDGER",
    "FX_RATE_APPLIED",
    "SETTLED",
    "REJECTED",
    "RETURNED",
]

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_time", StringType()),
        StructField("ingested_at", StringType()),
        StructField("payment_id", StringType()),
        StructField("client_id", StringType()),
        StructField("account_id", StringType()),
        StructField("payment_type", StringType()),
        StructField("direction", StringType()),
        StructField("currency", StringType()),
        StructField("amount", DecimalType(18, 2)),
        StructField("counterparty_country", StringType()),
        StructField("previous_status", StringType()),
        StructField("new_status", StringType()),
        StructField("reason_code", StringType()),
        StructField("mock_problem_type", StringType()),
    ]
)


def build_spark_session():
    builder = (
        SparkSession.builder.appName("PaymentStreamingProcessor")
        # Keep the local mock from creating hundreds of tiny Parquet files.
        .config("spark.sql.shuffle.partitions", "4")
    )
    if STORAGE_MODE == "s3":
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
            .config("spark.hadoop.fs.s3a.path.style.access", "false")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        )
    return builder.getOrCreate()


def load_reference_data(spark):
    clients = (
        spark.read.option("header", True)
        .schema(
            "client_id string, client_type string, country_code string, "
            "risk_rating string, onboarding_date string, status string"
        )
        .csv(str(CLIENTS_PATH))
        .select(
            F.trim("client_id").alias("reference_client_id"),
            F.upper(F.trim("country_code")).alias("client_country_code"),
            F.upper(F.trim("status")).alias("client_status"),
        )
    )

    accounts = (
        spark.read.option("header", True)
        .schema(
            "account_id string, client_id string, account_type string, "
            "currency string, country_code string, opened_date string, "
            "status string, available_balance decimal(18,2)"
        )
        .csv(str(ACCOUNTS_PATH))
        .select(
            F.trim("account_id").alias("reference_account_id"),
            F.trim("client_id").alias("account_owner_client_id"),
            F.upper(F.trim("currency")).alias("account_currency"),
            F.upper(F.trim("status")).alias("account_status"),
        )
    )

    return F.broadcast(clients), F.broadcast(accounts)


def read_events(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        # Use earliest for the first local run. Checkpoints preserve progress later.
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("original_payload"),
        )
        .withColumn("processor_ingested_at", F.current_timestamp())
    )


def parse_and_normalize(raw_events):
    parsed = raw_events.withColumn(
        "event", F.from_json(F.col("original_payload"), EVENT_SCHEMA)
    ).withColumn("json_parse_failed", F.col("event").isNull())

    return (
        parsed.select(
            "kafka_key",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "processor_ingested_at",
            "original_payload",
            "json_parse_failed",
            "event.*",
        )
        .withColumn("event_type", F.upper(F.trim("event_type")))
        .withColumn("payment_type", F.upper(F.trim("payment_type")))
        .withColumn("direction", F.upper(F.trim("direction")))
        .withColumn("currency", F.upper(F.trim("currency")))
        .withColumn(
            "counterparty_country", F.upper(F.trim("counterparty_country"))
        )
        .withColumn("previous_status", F.upper(F.trim("previous_status")))
        .withColumn("new_status", F.upper(F.trim("new_status")))
        # try_to_timestamp returns null for malformed fixtures instead of
        # terminating the streaming query under Spark ANSI mode.
        .withColumn("event_timestamp", F.try_to_timestamp(F.col("event_time")))
        .withColumn(
            "source_ingested_timestamp",
            F.try_to_timestamp(F.col("ingested_at")),
        )
    )


def enrich_with_references(events, clients, accounts):
    return (
        events.join(
            clients,
            events.client_id == clients.reference_client_id,
            "left",
        )
        .join(
            accounts,
            events.account_id == accounts.reference_account_id,
            "left",
        )
        .drop("reference_client_id", "reference_account_id")
    )


def validate_events(events):
    error_code = (
        F.when(F.col("json_parse_failed"), "INVALID_JSON")
        .when(F.col("event_id").isNull(), "MISSING_EVENT_ID")
        .when(F.col("event_type").isNull(), "MISSING_EVENT_TYPE")
        .when(F.col("event_type") != "PAYMENT_STATUS_CHANGED", "INVALID_EVENT_TYPE")
        .when(F.col("payment_id").isNull(), "MISSING_PAYMENT_ID")
        .when(F.col("client_id").isNull(), "MISSING_CLIENT_ID")
        .when(F.col("account_id").isNull(), "MISSING_ACCOUNT_ID")
        .when(F.col("event_timestamp").isNull(), "INVALID_EVENT_TIME")
        .when(F.col("source_ingested_timestamp").isNull(), "INVALID_INGESTED_AT")
        .when(F.col("kafka_key") != F.col("payment_id"), "KAFKA_KEY_MISMATCH")
        .when(F.col("client_status").isNull(), "CLIENT_NOT_FOUND")
        .when(F.col("account_owner_client_id").isNull(), "ACCOUNT_NOT_FOUND")
        .when(
            F.col("account_owner_client_id") != F.col("client_id"),
            "ACCOUNT_CLIENT_MISMATCH",
        )
        .when(F.col("client_status") != "ACTIVE", "CLIENT_NOT_ACTIVE")
        .when(F.col("account_status") != "ACTIVE", "ACCOUNT_NOT_ACTIVE")
        .when(~F.col("payment_type").isin(VALID_PAYMENT_TYPES), "INVALID_PAYMENT_TYPE")
        .when(~F.col("direction").isin(VALID_DIRECTIONS), "INVALID_DIRECTION")
        .when(~F.col("currency").rlike("^[A-Z]{3}$"), "INVALID_CURRENCY")
        .when(F.col("currency") != F.col("account_currency"), "ACCOUNT_CURRENCY_MISMATCH")
        .when(F.col("amount").isNull(), "INVALID_AMOUNT")
        .when(F.col("amount") <= 0, "NON_POSITIVE_AMOUNT")
        .when(
            ~F.col("counterparty_country").rlike("^[A-Z]{2}$"),
            "INVALID_COUNTERPARTY_COUNTRY",
        )
        .when(F.col("new_status").isNull(), "MISSING_STATUS")
        .when(~F.col("new_status").isin(VALID_STATUSES), "INVALID_STATUS")
        .when(
            F.col("event_timestamp")
            < F.col("source_ingested_timestamp") - F.expr("INTERVAL 10 MINUTES"),
            "LATE_EVENT",
        )
    )

    error_detail = (
        F.when(error_code == "INVALID_JSON", "Payload is not valid payment-event JSON")
        .when(error_code == "MISSING_EVENT_ID", "event_id is required")
        .when(error_code == "MISSING_EVENT_TYPE", "event_type is required")
        .when(error_code == "INVALID_EVENT_TYPE", "event_type is not supported")
        .when(error_code == "MISSING_PAYMENT_ID", "payment_id is required")
        .when(error_code == "MISSING_CLIENT_ID", "client_id is required")
        .when(error_code == "MISSING_ACCOUNT_ID", "account_id is required")
        .when(error_code == "INVALID_EVENT_TIME", "event_time cannot be parsed")
        .when(error_code == "INVALID_INGESTED_AT", "ingested_at cannot be parsed")
        .when(error_code == "KAFKA_KEY_MISMATCH", "Kafka key must equal payment_id")
        .when(error_code == "CLIENT_NOT_FOUND", "client_id is not in client reference data")
        .when(error_code == "ACCOUNT_NOT_FOUND", "account_id is not in account reference data")
        .when(error_code == "ACCOUNT_CLIENT_MISMATCH", "account is owned by another client")
        .when(error_code == "CLIENT_NOT_ACTIVE", "client is not ACTIVE")
        .when(error_code == "ACCOUNT_NOT_ACTIVE", "account is not ACTIVE")
        .when(error_code == "INVALID_PAYMENT_TYPE", "payment_type is not supported")
        .when(error_code == "INVALID_DIRECTION", "direction must be INBOUND or OUTBOUND")
        .when(error_code == "INVALID_CURRENCY", "currency must be a three-letter uppercase code")
        .when(error_code == "ACCOUNT_CURRENCY_MISMATCH", "currency differs from account currency")
        .when(error_code == "INVALID_AMOUNT", "amount is missing or cannot be parsed")
        .when(error_code == "NON_POSITIVE_AMOUNT", "amount must be greater than zero")
        .when(
            error_code == "INVALID_COUNTERPARTY_COUNTRY",
            "counterparty_country must be a two-letter uppercase code",
        )
        .when(error_code == "MISSING_STATUS", "new_status is required")
        .when(error_code == "INVALID_STATUS", "new_status is not supported")
        .when(
            error_code == "LATE_EVENT",
            f"event_time is more than {LATE_EVENT_THRESHOLD} before ingested_at",
        )
    )

    return events.withColumn("error_code", error_code).withColumn(
        "error_detail", error_detail
    )


def start_queries(validated):
    valid_events = (
        validated.filter(F.col("error_code").isNull())
        # Keep every event_id in checkpointed state for this local mock. Unlike
        # watermark-bounded deduplication, an old duplicate cannot pass after
        # state eviction. This is intentionally unbounded and is appropriate
        # for the project's short test runs, not an unlimited production feed.
        .dropDuplicates(["event_id"])
        .drop(
            "error_code",
            "error_detail",
            "mock_problem_type",
            "json_parse_failed",
            "client_status",
            "account_status",
            "account_owner_client_id",
            "account_currency",
            "client_country_code",
        )
        .withColumn("event_date", F.to_date("event_timestamp"))
    )

    rejected_events = (
        validated.filter(F.col("error_code").isNotNull())
        .withColumn("rejected_at", F.current_timestamp())
        .withColumn("rejection_date", F.to_date("rejected_at"))
    )

    valid_query = (
        valid_events.writeStream.queryName("valid_payment_events")
        .format("parquet")
        .outputMode("append")
        .option("path", VALID_OUTPUT)
        .option("checkpointLocation", VALID_CHECKPOINT)
        .partitionBy("event_date")
        .trigger(processingTime="10 seconds")
        .start()
    )

    rejected_query = (
        rejected_events.writeStream.queryName("rejected_payment_events")
        .format("parquet")
        .outputMode("append")
        .option("path", REJECTED_OUTPUT)
        .option("checkpointLocation", REJECTED_CHECKPOINT)
        .partitionBy("rejection_date", "error_code")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return valid_query, rejected_query


def run():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    clients, accounts = load_reference_data(spark)
    raw_events = read_events(spark)
    events = parse_and_normalize(raw_events)
    enriched = enrich_with_references(events, clients, accounts)
    validated = validate_events(enriched)
    queries = start_queries(validated)

    print(f"Reading Kafka topic: {KAFKA_TOPIC}")
    print(f"Storage mode: {STORAGE_MODE}")
    print(f"Valid output: {VALID_OUTPUT}")
    print(f"Rejected output: {REJECTED_OUTPUT}")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping payment stream processor")
    finally:
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    run()
