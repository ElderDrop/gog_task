{{ config(
    materialized='view',
    tags=['mart', 'fact_daily_revenue']
) }}

with converted_transactions as (
    SELECT 
        t.id,
        t.game_id,
        t.product_type,
        t.payment_method,
        t.transaction_date,
        {{ convert_currency('t.amount', 't.currency', 't.transaction_date') }} as converted_amount,
        'PLN' as converted_currency
    FROM {{ ref('stg_transactions')}} t
)

SELECT 
    t.transaction_date,
    t.product_type,
    g.genre,
    t.payment_method,
    SUM(converted_amount) as daily_revenue,
    t.converted_currency as currency
FROM converted_transactions t
JOIN {{ ref('dim_games')}} g ON g.id = {{ dbt_utils.generate_surrogate_key(['t.game_id']) }}
WHERE t.converted_amount is not null
GROUP BY t.transaction_date, t.product_type, g.genre, t.payment_method, t.converted_currency
ORDER BY t.transaction_date DESC, g.genre, t.payment_method, t.product_type

