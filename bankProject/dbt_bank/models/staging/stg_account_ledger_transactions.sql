with source as (

    select *
    from {{ source('bank_raw', 'account_ledger_transactions') }}

),

cleaned as (

    select
        transaction_id,
        account_id,
        client_id,
        transaction_timestamp,
        transaction_type,
        debit_credit,
        amount,
        currency,
        balance_after_transaction,
        available_balance,
        ledger_balance,
        status,
        updated_at
    from source

)

select *
from cleaned
