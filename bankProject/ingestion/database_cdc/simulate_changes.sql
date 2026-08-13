-- UPDATE: pending wire posts and both balances become final.
update account_ledger_transactions
set status = 'POSTED', available_balance = 8750.00,
    updated_at = '2026-08-10 09:10:01+00'
where transaction_id = 'L002';

-- INSERT: two new ledger transactions.
insert into account_ledger_transactions values
    ('L101', 'ACC-000001', 'CLIENT-0001', '2026-08-10 09:10:00+00',
     'ACH_DEBIT', 'D', 45.25, 'USD', 6454.75, 6454.75, 6454.75,
     'POSTED', '2026-08-10 09:10:02+00'),
    ('L102', 'ACC-000002', 'CLIENT-0002', '2026-08-10 09:10:00+00',
     'INTEREST', 'C', 12.50, 'USD', 8762.50, 8762.50, 8762.50,
     'POSTED', '2026-08-10 09:10:03+00')
on conflict (transaction_id) do nothing;

-- DELETE: demonstrates that CDC preserves deletion information. A normal
-- updated_at extractor cannot capture this operation after the row disappears.
delete from account_ledger_transactions where transaction_id = 'L003';
