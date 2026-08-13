"""EMR-compatible Bronze CDC to current-state Iceberg Silver transformation."""

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


BRONZE_PATH = os.environ["PIPELINE3_BRONZE_PATH"]
SILVER_MODE = os.getenv("PIPELINE3_SILVER_MODE", "parquet")
SILVER_PATH = os.environ["PIPELINE3_SILVER_PATH"]
ICEBERG_TABLE = os.getenv(
    "PIPELINE3_ICEBERG_TABLE", "glue_catalog.bank_silver.account_ledger_transactions"
)


def session() -> SparkSession:
    builder = SparkSession.builder.appName("CoreBankingBronzeToSilver")
    if SILVER_MODE == "iceberg":
        builder = (
            builder.config(
                "spark.sql.catalog.glue_catalog",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(
                "spark.sql.catalog.glue_catalog.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config(
                "spark.sql.catalog.glue_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
        )
    return builder.getOrCreate()


def current_snapshot(spark: SparkSession):
    bronze = (
        spark.read.option("recursiveFileLookup", "true").json(BRONZE_PATH)
        .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
        .withColumn(
            "balance_after_transaction",
            F.col("balance_after_transaction").cast("decimal(18,2)"),
        )
        .withColumn("available_balance", F.col("available_balance").cast("decimal(18,2)"))
        .withColumn("ledger_balance", F.col("ledger_balance").cast("decimal(18,2)"))
        .withColumn("transaction_timestamp", F.to_timestamp("transaction_timestamp"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("cdc_sequence", F.coalesce(F.col("change_id"), F.lit(0)))
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("account_id").isNotNull())
        .filter(F.col("client_id").isNotNull())
        .filter(F.col("amount") > 0)
        .filter(F.col("currency").rlike("^[A-Z]{3}$"))
    )
    latest = Window.partitionBy("transaction_id").orderBy(
        F.col("cdc_sequence").desc(), F.col("updated_at").desc()
    )
    return (
        bronze.withColumn("row_number", F.row_number().over(latest))
        .filter(F.col("row_number") == 1)
        .filter(F.col("Op") != "D")
        .drop("row_number", "changed_at")
        .withColumn("silver_processed_at", F.current_timestamp())
    )


def run() -> None:
    spark = session()
    silver = current_snapshot(spark)
    if SILVER_MODE == "iceberg":
        silver.writeTo(ICEBERG_TABLE).using("iceberg").createOrReplace()
    elif SILVER_MODE == "parquet":
        silver.write.mode("overwrite").parquet(SILVER_PATH)
    else:
        raise ValueError("PIPELINE3_SILVER_MODE must be parquet or iceberg")
    print(f"Silver current-state rows: {silver.count()}")
    spark.stop()


if __name__ == "__main__":
    run()

