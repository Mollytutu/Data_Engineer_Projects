select
    payment_id,
    client_id,
    account_id,
    upper(payment_type) as payment_type,
    upper(direction) as direction,
    upper(currency) as currency,
    amount,
    counterparty_country,
    previous_status,
    new_status,
    reason_code,
    event_timestamp

from {{ source('bank_raw', 'payments_current') }}
where payment_id is not null
