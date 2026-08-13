{% macro cleanup_legacy_schemas() %}
  {# Run only after the replacement INTERMEDIATE/MARTS objects are verified. #}
  {% set legacy_objects = [
    'BANK_DB.STAGING.INT_ACCOUNT_LEDGER_ENRICHED',
    'BANK_DB.STAGING.FCT_BATCH_PAYMENTS',
    'BANK_DB.STAGING.FCT_PAYMENTS',
    'BANK_DB.STAGING.FCT_ACCOUNT_LEDGER',
    'BANK_DB.STAGING.AGG_PAYMENT_STATUS',
    'BANK_DB.STAGING.AGG_PAYMENT_TYPE'
  ] %}
  {% for object_name in legacy_objects %}
    {% do run_query('drop view if exists ' ~ object_name) %}
  {% endfor %}
  {{ log('Removed obsolete intermediate and mart views from STAGING', info=True) }}
{% endmacro %}
