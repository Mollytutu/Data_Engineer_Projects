# Harness

The harness is the local validation framework for the bank pipeline scaffold.

Run it from the repository root:

```bash
bash scripts/run_harness.sh
```

It validates:

- Required pipeline folders exist.
- Shared config keys are present.
- Major folders are documented.
- dbt model layers are registered.
- Optional dbt parsing runs when `DBT_HARNESS_PARSE=1` is set and dbt is installed.
