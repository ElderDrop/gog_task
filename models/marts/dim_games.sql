{{ config(
    materialized='view',
    tags=['mart', 'dim_games']
) }}

SELECT
    id,
    game_id as source_game_id,
    game_title,
    genre,
    developer,
    release_date
FROM {{ ref('stg_game_metadata') }}