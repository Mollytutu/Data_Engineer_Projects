select
    payment_type,
    count(*) as payment_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount
from {{ ref('fct_payments') }}
group by payment_type
