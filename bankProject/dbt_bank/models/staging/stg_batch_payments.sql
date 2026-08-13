select
    payment_id,
    client_id,
    account_id,
    payment_date,
    upper(direction) as direction,
    upper(payment_type) as payment_type,
    upper(currency) as currency,
    amount,
    upper(counterparty_country) as counterparty_country,
    upper(status) as status,
    source_file,
    ingested_at,
    source_s3_file,
    snowflake_loaded_at
from {{ source('bank_raw', 'batch_payments') }}
where payment_id is not null

