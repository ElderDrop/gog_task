{{ config(
    materialized='table',
    tags=['stage', 'stg_game_metadata']
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['game_id']) }} as id,
    TRIM(game_id) as game_id,
    TRIM(game_title) as game_title,
    TRIM(genre) as genre,
    TRIM(developer) as developer,
    CAST(TRIM(release_date) as date) as release_date
FROM {{ ref('game_metadata') }}