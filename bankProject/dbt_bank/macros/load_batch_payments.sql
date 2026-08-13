{% macro load_batch_payments() %}
  {% set create_table_sql %}
    create table if not exists BANK_DB.RAW.BATCH_PAYMENTS (
      payment_id varchar,
      client_id varchar,
      account_id varchar,
      payment_date date,
      direction varchar,
      payment_type varchar,
      currency varchar(3),
      amount number(18,2),
      counterparty_country varchar(2),
      status varchar,
      source_file varchar,
      ingested_at timestamp_ntz,
      source_s3_file varchar,
      snowflake_loaded_at timestamp_ntz default current_timestamp()
    )
  {% endset %}

  {% set truncate_sql %}
    truncate table BANK_DB.RAW.BATCH_PAYMENTS
  {% endset %}

  {% set copy_sql %}
    copy into BANK_DB.RAW.BATCH_PAYMENTS (
      payment_id, client_id, account_id, payment_date, direction, payment_type,
      currency, amount, counterparty_country, status, source_file, ingested_at,
      source_s3_file
    )
    from (
      select
        $1:payment_id::varchar,
        $1:client_id::varchar,
        $1:account_id::varchar,
        to_date(regexp_substr(metadata$filename, 'payment_date=([0-9-]+)', 1, 1, 'e', 1)),
        $1:direction::varchar,
        $1:payment_type::varchar,
        $1:currency::varchar,
        $1:amount::number(18,2),
        $1:counterparty_country::varchar,
        $1:status::varchar,
        $1:source_file::varchar,
        $1:ingested_at::timestamp_ntz,
        metadata$filename
      from @BANK_DB.EXTERNAL.BANK_S3_STAGE/curated/payments/
    )
    file_format = (type = parquet)
    pattern = '.*[.]parquet'
    on_error = abort_statement
  {% endset %}

  {% do run_query(create_table_sql) %}
  {% do run_query(truncate_sql) %}
  {% set copy_result = run_query(copy_sql) %}
  {% if execute %}
    {{ log('Loaded BANK_DB.RAW.BATCH_PAYMENTS from S3 curated Parquet', info=True) }}
  {% endif %}
{% endmacro %}

