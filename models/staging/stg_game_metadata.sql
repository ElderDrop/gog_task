{{ config(
    materialized='table',
    tags=['stage', 'stg_game_metadata']
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['game_id']) }} AS id,
    TRIM(game_id) AS game_id,
    TRIM(game_title) AS game_title,
    TRIM(genre) AS genre,
    TRIM(developer) AS developer,
    CAST(releASe_date AS date) AS releASe_date
FROM {{ ref('game_metadata') }}