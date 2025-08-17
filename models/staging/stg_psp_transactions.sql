{{ config(
    materialized='table',
    tags=['stage', 'stg_psp_transactions']
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['psp_transaction_id', 'original_transaction_id']) }} AS id,
    TRIM(psp_transaction_id) AS psp_transaction_id,
    TRIM(original_transaction_id) AS original_transaction_id,
    psp_amount AS psp_amount,
    TRIM(psp_currency) AS psp_currency,
    CAST(psp_timestamp AS timestamp) AS psp_timestamp,
    TRIM(status) AS psp_status
FROM {{ ref('psp_transactions') }}
WHERE original_transaction_id IS NOT NULL

