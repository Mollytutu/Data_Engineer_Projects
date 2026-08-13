select *
from {{ source('bank_raw', 'rejected_payment_events') }}