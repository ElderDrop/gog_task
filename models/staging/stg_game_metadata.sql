{{ config(
    materialized='table',
    tags=['stage', 'stg_game_metadata']
) }}

SELECT 
{{ dbt_utils.generate_surrogate_key(['game_id']) }} as id,
* 
FROM {{ ref('game_metadata') }}