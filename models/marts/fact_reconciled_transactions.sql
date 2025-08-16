{{ config(
    materialized='view',
    tags=['mart', 'fact_reconciled_transactions']
) }}

SELECT 
    t.transaction_id,
    t.user_id,
    t.game_id,
    t.amount as original_amount,
    t.currency as original_currency,
    t.transaction_date,
    t.payment_method,
    t.product_type,
    t.id as transaction_key,
    p.psp_transaction_id,
    p.original_transaction_id,
    p.id as psp_transaction_key,
    CASE 
        WHEN t.transaction_id IS NOT NULL AND p.psp_transaction_id IS NOT NULL THEN 
            {{ convert_currency('t.amount', 't.currency', 't.transaction_date') }}
        WHEN t.transaction_id IS NOT NULL AND p.psp_transaction_id IS NULL THEN 
            {{ convert_currency('t.amount', 't.currency', 't.transaction_date') }}
        WHEN t.transaction_id IS NULL AND p.psp_transaction_id IS NOT NULL THEN 
            {{ convert_currency('p.psp_amount', 'p.psp_currency', ' CAST(p.psp_timestamp AS DATE)') }}
        ELSE NULL
    END as converted_amount,
    'PLN' as converted_currency,
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

