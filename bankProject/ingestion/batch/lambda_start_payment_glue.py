"""S3-triggered Lambda entry point that starts the Pipeline 1 Glue job."""

import os
from urllib.parse import unquote_plus

import boto3


GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
glue = boto3.client("glue")


def lambda_handler(event, _context):
    started = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        if not key.startswith("raw/payments/") or not key.endswith(".csv"):
            continue
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--PAYMENTS_PATH": f"s3://{bucket}/{key}",
                "--ACCOUNTS_PATH": f"s3://{bucket}/raw/reference/accounts.csv",
                "--CLIENTS_PATH": f"s3://{bucket}/raw/reference/clients.csv",
                "--CURATED_PATH": f"s3://{bucket}/curated/payments/",
                "--REJECTED_PATH": f"s3://{bucket}/rejected/payments/",
            },
        )
        started.append(response["JobRunId"])
    return {"started_job_runs": started}

