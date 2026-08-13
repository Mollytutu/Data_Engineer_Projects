use database BANK_DB;
create schema if not exists RAW;
use schema RAW;

create table if not exists ACCOUNT_LEDGER_TRANSACTIONS (
    transaction_id varchar primary key,
    account_id varchar,
    client_id varchar,
    transaction_timestamp timestamp_tz,
    transaction_type varchar,
    debit_credit varchar(1),
    amount number(18,2),
    currency varchar(3),
    balance_after_transaction number(18,2),
    available_balance number(18,2),
    ledger_balance number(18,2),
    status varchar,
    updated_at timestamp_tz,
    cdc_sequence number,
    silver_processed_at timestamp_tz
);

