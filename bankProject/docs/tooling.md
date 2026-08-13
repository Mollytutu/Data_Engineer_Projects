# Terminal Tooling

This project uses local terminal tools for the mock bank pipeline.

## Installed Tooling

- Kafka CLI: `kafka-topics`, `kafka-console-producer`, `kafka-console-consumer`
- Scala: `scala`, `scala-cli`
- Spark: `spark-submit`, `pyspark`
- Snowflake CLI: `snow`
- SQLFluff: `sqlfluff`
- Local dbt: `.venv/bin/dbt` with `dbt-core` and `dbt-snowflake`

## Project Environment

From the repository root, load the project environment before Kafka/dbt work:

```bash
source scripts/use_local_env.sh
```

This sets:

- `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`
- project `.venv/bin` before the global PATH
- `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` set to the project `.venv` interpreter
- `DBT_PROFILES_DIR` to the working `dbt_bank/` project if not already set

## Notes

- The global `dbt` command is the dbt Cloud CLI. Use `.venv/bin/dbt` or source `scripts/use_local_env.sh` for local dbt-core.
- Legacy `snowsql` requires a macOS installer with `sudo`; use the modern `snow` CLI unless you specifically need SnowSQL.
- AWS CLI is intentionally not installed yet.
