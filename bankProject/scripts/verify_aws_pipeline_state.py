"""Small Airflow-friendly AWS checks that avoid requiring the AWS CLI."""

from __future__ import annotations

import argparse

import boto3


def verify_glue(job_name: str) -> None:
    response = boto3.client("glue").get_job_runs(JobName=job_name, MaxResults=1)
    runs = response.get("JobRuns", [])
    if not runs:
        raise RuntimeError(f"Glue job {job_name!r} has no runs")
    state = runs[0]["JobRunState"]
    print(f"Latest Glue run for {job_name}: {state}")
    if state != "SUCCEEDED":
        raise RuntimeError(f"latest Glue run is {state}, expected SUCCEEDED")


def verify_s3(bucket: str, prefix: str, suffix: str) -> None:
    paginator = boto3.client("s3").get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"].endswith(suffix):
                print(f"Found s3://{bucket}/{item['Key']}")
                return
    raise RuntimeError(f"no {suffix} object found under s3://{bucket}/{prefix}")


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="check", required=True)
    glue = subparsers.add_parser("glue")
    glue.add_argument("--job-name", required=True)
    s3 = subparsers.add_parser("s3")
    s3.add_argument("--bucket", required=True)
    s3.add_argument("--prefix", required=True)
    s3.add_argument("--suffix", required=True)
    args = parser.parse_args()
    if args.check == "glue":
        verify_glue(args.job_name)
    else:
        verify_s3(args.bucket, args.prefix, args.suffix)


if __name__ == "__main__":
    main()
