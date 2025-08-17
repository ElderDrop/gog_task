{{ config(
    materialized='view',
    tags=['mart', 'fact_reconciled_transactions']
) }}

SELECT 
    t.id AS transaction_key,
    p.id AS psp_transaction_key,
    p.psp_transaction_id as source_psp_transaction_id,
    p.original_transaction_id as source_original_transaction_id,
    CASE 
        WHEN t.transaction_id IS NOT NULL AND p.psp_transaction_id IS NOT NULL THEN 'MATCH'
        WHEN t.transaction_id IS NOT NULL AND p.psp_transaction_id IS NULL THEN 'ONLY IN GOG'
        WHEN t.transaction_id IS NULL AND p.psp_transaction_id IS NOT NULL THEN 'ONLY IN PSP'
        ELSE 'UNKNOWN'
    END as reconciliation_status
FROM {{ ref('stg_transactions') }} t
FULL OUTER JOIN {{ ref('stg_psp_transactions') }} p 
    ON t.transaction_id = p.original_transaction_id
    AND t.transaction_date = CAST(p.psp_timestamp AS DATE)

