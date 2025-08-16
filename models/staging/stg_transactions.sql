{{ config(
    materialized='table',
    tags=['stage', 'stg_transactions']
) }}

SELECT 
{{ dbt_utils.generate_surrogate_key(['transaction_id', 'user_id', 'game_id']) }} as id,
* 
FROM {{ ref('raw_transactions') }}
WHERE game_id LIKE 'G%'

