-- For some transactions, there is no exchange rate for the transaction date.
-- This is because the exchange rate for the transaction date is not available.


with missing_exchange_rate_for_transactions as (
    select transaction_id, transaction_date, currency from {{ ref('stg_transactions') }}
    left join {{ ref('stg_excahnge_rates') }} 
        on stg_transactions.transaction_date = stg_excahnge_rates.exchange_rate_date 
        and stg_transactions.currency = stg_excahnge_rates.currency_from
    where stg_excahnge_rates.id is null
)

select * from missing_exchange_rate_for_transactions;