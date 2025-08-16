{{ config(
    materialized='table',
    tags=['stage', 'stg_exchange_rates']
) }}

SELECT 
{{ dbt_utils.generate_surrogate_key(['currency_from', 'date']) }} as id,
* 
FROM {{ ref('exchange_rates') }}