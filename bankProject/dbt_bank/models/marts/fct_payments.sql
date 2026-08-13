select
    payment_id,
    client_id,
    account_id,
    payment_type,
    direction,
    currency,
    amount,
    counterparty_country,
    new_status as current_status,
    event_timestamp
from {{ ref('stg_payments_current') }}
