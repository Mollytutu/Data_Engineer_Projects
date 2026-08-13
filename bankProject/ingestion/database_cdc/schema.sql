create table if not exists account_ledger_transactions (
    transaction_id varchar(40) primary key,
    account_id varchar(40) not null,
    client_id varchar(40) not null,
    transaction_timestamp timestamp with time zone not null,
    transaction_type varchar(30) not null,
    debit_credit char(1) not null check (debit_credit in ('D', 'C')),
    amount numeric(18, 2) not null check (amount > 0),
    currency char(3) not null check (currency = upper(currency)),
    balance_after_transaction numeric(18, 2) not null,
    available_balance numeric(18, 2) not null,
    ledger_balance numeric(18, 2) not null,
    status varchar(20) not null,
    updated_at timestamp with time zone not null default current_timestamp
);

-- This change table is only for the local DMS simulator. In production,
-- Oracle redo/archive logs or PostgreSQL WAL are read directly by AWS DMS.
create table if not exists mock_dms_change_log (
    change_id bigint generated always as identity primary key,
    operation char(1) not null check (operation in ('I', 'U', 'D')),
    transaction_id varchar(40) not null,
    changed_at timestamp with time zone not null default current_timestamp,
    row_data jsonb
);

create or replace function capture_ledger_change()
returns trigger
language plpgsql
as $$
begin
    if tg_op = 'DELETE' then
        insert into mock_dms_change_log (operation, transaction_id, row_data)
        values ('D', old.transaction_id, to_jsonb(old));
        return old;
    elsif tg_op = 'UPDATE' then
        insert into mock_dms_change_log (operation, transaction_id, row_data)
        values ('U', new.transaction_id, to_jsonb(new));
        return new;
    else
        insert into mock_dms_change_log (operation, transaction_id, row_data)
        values ('I', new.transaction_id, to_jsonb(new));
        return new;
    end if;
end;
$$;

drop trigger if exists account_ledger_mock_dms on account_ledger_transactions;
create trigger account_ledger_mock_dms
after insert or update or delete on account_ledger_transactions
for each row execute function capture_ledger_change();

