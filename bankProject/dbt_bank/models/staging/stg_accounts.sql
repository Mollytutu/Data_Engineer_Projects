select
    account_id,
    client_id,
    upper(account_type) as account_type,
    upper(currency) as currency,
    upper(country_code) as country_code,
    opened_date,
    upper(status) as account_status,
    available_balance
from {{ source('bank_raw', 'accounts') }}
where account_id is not null

