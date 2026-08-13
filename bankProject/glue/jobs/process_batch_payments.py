"""AWS Glue job for Pipeline 1 batch payment files."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "PAYMENTS_PATH",
        "ACCOUNTS_PATH",
        "CLIENTS_PATH",
        "CURATED_PATH",
        "REJECTED_PATH",
    ],
)

context = GlueContext(SparkContext.getOrCreate())
spark = context.spark_session
job = Job(context)
job.init(args["JOB_NAME"], args)

payments = spark.read.option("header", True).csv(args["PAYMENTS_PATH"])
accounts = (
    spark.read.option("header", True).csv(args["ACCOUNTS_PATH"])
    .select(
        F.trim("account_id").alias("reference_account_id"),
        F.trim("client_id").alias("account_owner_client_id"),
        F.upper(F.trim("currency")).alias("account_currency"),
        F.upper(F.trim("status")).alias("account_status"),
    )
)
clients = (
    spark.read.option("header", True).csv(args["CLIENTS_PATH"])
    .select(
        F.trim("client_id").alias("reference_client_id"),
        F.upper(F.trim("status")).alias("client_status"),
    )
)

normalized = (
    payments
    .withColumn("raw_payment_date", F.trim("payment_date"))
    .withColumn("raw_currency", F.trim("currency"))
    .withColumn("payment_id", F.trim("payment_id"))
    .withColumn("client_id", F.trim("client_id"))
    .withColumn("account_id", F.trim("account_id"))
    .withColumn(
        "payment_date",
        F.when(
            F.col("raw_payment_date").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
            F.to_date("raw_payment_date"),
        ),
    )
    .withColumn("direction", F.upper(F.trim("direction")))
    .withColumn("payment_type", F.upper(F.trim("payment_type")))
    .withColumn("currency", F.upper(F.trim("currency")))
    .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    .withColumn("counterparty_country", F.upper(F.trim("counterparty_country")))
    .withColumn("status", F.upper(F.trim("status")))
    .withColumn("source_file", F.input_file_name())
    .withColumn("ingested_at", F.current_timestamp())
    .join(F.broadcast(accounts), F.col("account_id") == F.col("reference_account_id"), "left")
    .join(F.broadcast(clients), F.col("client_id") == F.col("reference_client_id"), "left")
)

duplicate_window = Window.partitionBy("payment_id").orderBy(
    F.col("source_file"), F.monotonically_increasing_id()
)

validated = (
    normalized.withColumn("duplicate_rank", F.row_number().over(duplicate_window))
    .withColumn(
        "rejection_reason",
        F.when(F.col("duplicate_rank") > 1, "DUPLICATE_PAYMENT_ID")
        .when(F.col("payment_id").isNull(), "MISSING_PAYMENT_ID")
        .when(F.col("account_id").isNull(), "MISSING_ACCOUNT_ID")
        .when(F.col("reference_account_id").isNull(), "ACCOUNT_NOT_FOUND")
        .when(F.col("reference_client_id").isNull(), "CLIENT_NOT_FOUND")
        .when(F.col("account_owner_client_id") != F.col("client_id"), "ACCOUNT_CLIENT_MISMATCH")
        .when(F.col("account_status") != "ACTIVE", "ACCOUNT_NOT_ACTIVE")
        .when(F.col("client_status") != "ACTIVE", "CLIENT_NOT_ACTIVE")
        .when(F.col("payment_date").isNull(), "INVALID_PAYMENT_DATE")
        .when(
            ~F.col("raw_currency").isin(
                "AED", "AUD", "BRL", "CAD", "CHF", "EUR", "GBP",
                "INR", "JPY", "MXN", "SGD", "USD",
            ),
            "INVALID_CURRENCY",
        )
        .when(F.col("currency") != F.col("account_currency"), "ACCOUNT_CURRENCY_MISMATCH")
        .when(F.col("amount").isNull() | (F.col("amount") <= 0), "INVALID_AMOUNT")
    )
)

technical_columns = [
    "reference_account_id", "account_owner_client_id", "account_currency",
    "account_status", "reference_client_id", "client_status", "duplicate_rank",
    "raw_currency", "raw_payment_date",
]
valid = validated.filter(F.col("rejection_reason").isNull()).drop(*technical_columns)
rejected = validated.filter(F.col("rejection_reason").isNotNull()).drop(*technical_columns)

valid.write.mode("overwrite").partitionBy("payment_date").parquet(args["CURATED_PATH"])
rejected.write.mode("overwrite").partitionBy("rejection_reason").parquet(args["REJECTED_PATH"])

print(f"Pipeline 1 input={payments.count()} valid={valid.count()} rejected={rejected.count()}")
job.commit()
