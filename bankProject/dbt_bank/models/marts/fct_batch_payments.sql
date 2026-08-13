select
    payment_id,
    client_id,
    account_id,
    payment_date,
    direction,
    payment_type,
    currency,
    amount,
    case when direction = 'OUTBOUND' then -amount else amount end as signed_amount,
    counterparty_country,
    status,
    ingested_at
from {{ ref('stg_batch_payments') }}

