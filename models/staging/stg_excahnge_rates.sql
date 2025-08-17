{{ config(
    materialized='table',
    tags=['stage', 'stg_exchange_rates']
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['currency_from', 'date']) }} AS id,
    CAST(date AS date) AS exchange_rate_date,
    TRIM(currency_from) AS currency_from,
    TRIM(currency_to) AS currency_to,
    CAST(rate AS decimal(10, 4)) AS rate
FROM {{ ref('exchange_rates') }}