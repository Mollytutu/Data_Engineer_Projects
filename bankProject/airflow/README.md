# Local Airflow orchestration

The project uses a separate `.airflow-venv` and project-local `.airflow-home`.
This setup is for portfolio demonstration and local development, not a
production scheduler.

## Install and initialize

```bash
bash scripts/install_airflow_local.sh
```

## Start

```bash
bash scripts/run_airflow_local.sh
```

Open `http://localhost:8080`. Airflow 3 stores the generated local admin
password in `.airflow-home/simple_auth_manager_passwords.json.generated`.

The three DAGs are manual by default, preventing an installation test from
starting Glue, querying PostgreSQL, or rebuilding Snowflake unexpectedly:

- `pipeline1_batch_payments`
- `pipeline2_payment_streaming_downstream`
- `pipeline3_core_banking_cdc`

Set the Pipeline 3 local connection before triggering its DAG:

```bash
export AIRFLOW_HOME="$PWD/.airflow-home"
.airflow-venv/bin/airflow variables set \
  pipeline3_postgres_dsn 'postgresql://localhost/bank_oltp'
```

Stop standalone Airflow with `Ctrl+C`. A production deployment would use MWAA
roles, Connections/Secrets Manager, remote logging, alerting, and a production
metadata database. Airflow coordinates DMS/Kafka/Glue/dbt; it does not replace
those processing or ingestion systems.
