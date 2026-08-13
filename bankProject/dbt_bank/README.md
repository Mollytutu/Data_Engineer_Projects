# dbt banking transformations

This is the single working dbt project for the mock banking pipeline. It transforms Snowflake source tables into standardized staging views, canonical banking models, and business marts.

## Model layers

- `models/staging/` performs light renaming, typing, and source standardization
  in `BANK_DB.STAGING`.
- `models/intermediate/` contains reusable joins and transformation steps in
  `BANK_DB.INTERMEDIATE`.
- `models/core/` is reserved for future canonical entities.
- `models/marts/` builds business-facing tables in `BANK_DB.MARTS`.

Current business-ready tables:

```text
BANK_DB.MARTS.FCT_BATCH_PAYMENTS
BANK_DB.MARTS.FCT_PAYMENTS
BANK_DB.MARTS.FCT_ACCOUNT_LEDGER
BANK_DB.MARTS.AGG_PAYMENT_STATUS
BANK_DB.MARTS.AGG_PAYMENT_TYPE
```

The local `profiles.yml` contains connection configuration and is ignored by Git. From the repository root, load the environment and run dbt:

```bash
source scripts/use_local_env.sh
dbt ls --project-dir dbt_bank
dbt run --project-dir dbt_bank
dbt test --project-dir dbt_bank
```

From inside `dbt_bank/`, the equivalent commands are simply `dbt run` and `dbt test` after the environment has been loaded from the repository root.
