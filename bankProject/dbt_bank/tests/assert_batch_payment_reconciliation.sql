with counts as (
    select
        (select count(*) from {{ source('bank_raw', 'batch_payments') }}) as raw_count,
        (select count(*) from {{ ref('fct_batch_payments') }}) as fact_count
)
select *
from counts
where raw_count != fact_count

