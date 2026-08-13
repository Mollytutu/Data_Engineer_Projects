# Harness Framework

The harness gives this mock bank pipeline a repeatable validation entry point while implementation code is still being added.

## Local Command

```bash
bash scripts/run_harness.sh
```

## Check Layers

- Structure check: confirms the architecture folders exist.
- Config check: confirms required top-level config keys are present.
- Documentation check: confirms major folders have non-empty README files.
- dbt check: confirms dbt model layers are registered. Set `DBT_HARNESS_PARSE=1` to also run `dbt parse` when dbt is available.

## Extension Points

Add new checks as separate scripts under `scripts/`, then register them in:

- `scripts/run_harness.sh`
- `harness/harness.yaml`
- `.github/workflows/ci.yml` if the check should run in CI
