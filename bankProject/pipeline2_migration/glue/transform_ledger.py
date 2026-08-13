import sys


from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "INPUT_PATH",
        "CURATED_PATH",
        "REJECTED_PATH",
    ],
)

sc = SparkContext()

glue_context = GlueContext(sc)

spark = glue_context.spark_session

job = Job(glue_context)

job.init(
    args["JOB_NAME"],
    args
)

df = spark.read.parquet(
    args["INPUT_PATH"]
)
print(f"Input row count: {df.count()}")

df = (
    df
    .withColumn("transaction_id", F.trim(F.col("transaction_id")))
    .withColumn("account_id", F.trim(F.col("account_id")))
    .withColumn("client_id", F.trim(F.col("client_id")))
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .withColumn("debit_credit", F.upper(F.trim(F.col("debit_credit"))))
    .withColumn("status", F.upper(F.trim(F.col("status"))))
)

df = df.withColumn(
    "rejection_reason",
    F.when(F.col("transaction_id").isNull(), "MISSING_TRANSACTION_ID")
     .when(F.col("account_id").isNull(), "MISSING_ACCOUNT_ID")
     .when(F.col("client_id").isNull(), "MISSING_CLIENT_ID")
     .when(F.col("amount") <= 0, "INVALID_AMOUNT")
     .when(~F.col("debit_credit").isin("D", "C"), "INVALID_DEBIT_CREDIT")
     .when(~F.col("status").isin("PENDING", "POSTED", "REVERSED"), "INVALID_STATUS")
     .otherwise(None)
)

valid_df = df.filter(
    F.col("rejection_reason").isNull()
)

rejected_df = df.filter(
    F.col("rejection_reason").isNotNull()
)

window_spec = Window.partitionBy(
    "transaction_id"
).orderBy(
    F.col("updated_at").desc()
)

valid_df = (
    valid_df
    .withColumn(
        "row_number",
        F.row_number().over(window_spec)
    )
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

valid_df.write \
    .mode("overwrite") \
    .parquet(args["CURATED_PATH"])

rejected_df.write \
    .mode("overwrite") \
    .parquet(args["REJECTED_PATH"])

print(f"Valid rows: {valid_df.count()}")
print(f"Rejected rows: {rejected_df.count()}")

job.commit()