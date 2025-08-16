{{ config(
    materialized='table',
    tags=['stage', 'stg_psp_transactions']
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['psp_transaction_id', 'original_transaction_id']) }} as id,
    TRIM(psp_transaction_id) as psp_transaction_id,
    TRIM(original_transaction_id) as original_transaction_id,
    TRIM(psp_amount) as psp_amount,
    TRIM(psp_currency) as psp_currency,
    CAST(TRIM(psp_timestamp) as timestamp) as psp_timestamp,
    TRIM(status) as psp_status
FROM {{ ref('psp_transactions') }}
WHERE original_transaction_id IS NOT NULL

