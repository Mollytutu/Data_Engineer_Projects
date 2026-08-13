-- The EMR/Iceberg export lands in this transient change table before MERGE.
-- MERGE makes Snowflake reruns idempotent by transaction_id and CDC sequence.
use database BANK_DB;
use schema RAW;

merge into ACCOUNT_LEDGER_TRANSACTIONS target
using ACCOUNT_LEDGER_TRANSACTIONS_INCREMENTAL source
on target.transaction_id = source.transaction_id
when matched and source.Op = 'D' then delete
when matched and source.cdc_sequence >= target.cdc_sequence then update set
    account_id = source.account_id,
    client_id = source.client_id,
    transaction_timestamp = source.transaction_timestamp,
    transaction_type = source.transaction_type,
    debit_credit = source.debit_credit,
    amount = source.amount,
    currency = source.currency,
    balance_after_transaction = source.balance_after_transaction,
    available_balance = source.available_balance,
    ledger_balance = source.ledger_balance,
    status = source.status,
    updated_at = source.updated_at,
    cdc_sequence = source.cdc_sequence,
    silver_processed_at = source.silver_processed_at
when not matched and source.Op != 'D' then insert (
    transaction_id, account_id, client_id, transaction_timestamp,
    transaction_type, debit_credit, amount, currency,
    balance_after_transaction, available_balance, ledger_balance, status,
    updated_at, cdc_sequence, silver_processed_at
) values (
    source.transaction_id, source.account_id, source.client_id,
    source.transaction_timestamp, source.transaction_type,
    source.debit_credit, source.amount, source.currency,
    source.balance_after_transaction, source.available_balance,
    source.ledger_balance, source.status, source.updated_at,
    source.cdc_sequence, source.silver_processed_at
);
