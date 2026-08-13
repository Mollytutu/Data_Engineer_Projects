select
    payment_id,
    event_id,
    client_id,
    account_id,

    upper(payment_type) as payment_type,
    upper(direction) as direction,
    upper(currency) as currency,

    amount,
    counterparty_country,

    previous_status,
    new_status,

    event_timestamp,
    kafka_timestamp,
    partition,
    offset

from {{ source('bank_raw', 'payment_events') }}

where payment_id is not null