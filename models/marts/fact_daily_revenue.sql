{{ config(
    materialized='view',
    tags=['mart', 'fact_daily_revenue']
) }}



SELECT 
    t.transaction_date,
    t.product_type,
    g.genre,
    t.payment_method,
    SUM(converted_amount) as daily_revenue,
    t.converted_currency as currency
FROM {{ ref('reconsiled_transations')}} t
JOIN {{ ref('dim_games')}} g ON g.game_id = t.game_id
WHERE reconciliation_status = 'RECONCILED'
GROUP BY t.transaction_date, t.product_type, g.genre, t.payment_method, t.converted_currency
ORDER BY t.transaction_date DESC, g.genre, t.payment_method, t.product_type

