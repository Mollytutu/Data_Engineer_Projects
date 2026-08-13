-- Run from the bankProject root. This loads the visible 100-row CSV fixture.
truncate table account_ledger_transactions, mock_dms_change_log restart identity;

\copy account_ledger_transactions (transaction_id, account_id, client_id, transaction_timestamp, transaction_type, debit_credit, amount, currency, balance_after_transaction, available_balance, ledger_balance, status, updated_at) from 'data/raw/core_banking/account_ledger_transactions.csv' with (format csv, header true)
