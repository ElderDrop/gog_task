{{ config(
    materialized='table',
    tags=['stage', 'stg_psp_transactions']
) }}

SELECT 
{{ dbt_utils.generate_surrogate_key(['psp_transaction_id', 'original_transaction_id']) }} as id,
* 
FROM {{ ref('psp_transactions') }}
WHERE original_transaction_id IS NOT NULL

