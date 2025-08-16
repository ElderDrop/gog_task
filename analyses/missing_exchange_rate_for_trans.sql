-- For some transactions, there is no exchange rate for the transaction date.
-- This is because the exchange rate for the transaction date is not available.


with missing_exchange_rate_for_transactions as (
    select * from {{ ref('stg_transactions') }}
    left join {{ ref('stg_exchange_rates') }} on stg_transactions.transaction_date = stg_exchange_rates.date and stg_transactions.currency = stg_exchange_rates.currency_from
)

select * from missing_exchange_rate_for_transactions limit 10;