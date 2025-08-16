{{ config(
    materialized='table',
    tags=['stage', 'stg_transactions']
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['transaction_id', 'user_id', 'game_id']) }} as id,
    TRIM(transaction_id) as transaction_id,
    TRIM(user_id) as user_id,
    TRIM(game_id) as game_id,
    TRIM(amount) as amount,
    TRIM(currency) as currency,
    CAST(TRIM(transaction_date) as date) as transaction_date,
    TRIM(payment_method) as payment_method,
    TRIM(product_type) as product_type
FROM {{ ref('raw_transactions') }}
WHERE game_id LIKE 'G%'

