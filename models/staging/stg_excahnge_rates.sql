{{ config(
    materialized='table',
    tags=['stage', 'stg_exchange_rates']
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['currency_from', 'date']) }} as id,
    CAST(TRIM(date) as date) as exchange_rate_date,
    TRIM(currency_from) as currency_from,
    TRIM(currency_to) as currency_to,
    CAST(TRIM(rate) as decimal(10, 4)) as rate,
FROM {{ ref('exchange_rates') }}