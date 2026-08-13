from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("BuildPaymentsCurrent")
    .getOrCreate()
)

source = (
    "s3a://bank-pipeline-project-molly/"
    "streaming/curated/payment_events/"
)

target = (
    "s3a://bank-pipeline-project-molly/"
    "streaming/curated/payments_current/"
)

events = spark.read.parquet(source)

latest_window = (
    Window
    .partitionBy("payment_id")
    .orderBy(
        F.col("event_timestamp").desc(),
        F.col("kafka_timestamp").desc(),
        F.col("partition").desc(),
        F.col("offset").desc(),
    )
)

payments_current = (
    events
    .withColumn(
        "latest_row_number",
        F.row_number().over(latest_window),
    )
    .filter(F.col("latest_row_number") == 1)
    .drop("latest_row_number")
)

payments_current.write.mode("overwrite").parquet(target)

spark.stop()