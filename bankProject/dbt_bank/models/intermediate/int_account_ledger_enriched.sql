with ledger as (

    select *
    from {{ ref('stg_account_ledger_transactions') }}

),

accounts as (

    select *
    from {{ ref('stg_accounts') }}

),

clients as (

    select *
    from {{ ref('stg_clients') }}

)

select
    l.transaction_id,
    l.account_id,
    l.client_id,
    l.transaction_timestamp,
    l.transaction_type,
    l.debit_credit,
    l.amount,
    l.currency,
    l.balance_after_transaction,
    l.available_balance,
    l.ledger_balance,
    l.status,
    l.updated_at,

    a.account_type,
    a.account_status,

    c.client_type,
    c.risk_level,
    c.country

from ledger l

left join accounts a
    on l.account_id = a.account_id

left join clients c
    on l.client_id = c.client_id