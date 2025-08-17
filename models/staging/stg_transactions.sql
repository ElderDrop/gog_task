{{ config(
    materialized='table',
    tags=['stage', 'stg_transactions']
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['transaction_id', 'user_id', 'game_id']) }} AS id,
    TRIM(transaction_id) AS transaction_id,
    TRIM(user_id) AS user_id,
    TRIM(game_id) AS game_id,
    amount AS amount,
    TRIM(currency) AS currency,
    CAST(transaction_date AS date) AS transaction_date,
    TRIM(payment_method) AS payment_method,
    TRIM(product_type) AS product_type
FROM {{ ref('raw_transactions') }}
WHERE game_id LIKE 'G%'

