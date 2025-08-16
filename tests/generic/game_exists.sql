{% test game_exists(model, column_name) %}

with validation as (

    select
        {{ column_name }} as game_id

    from {{ model }}

),

validation_errors as (

    select
        game_id

    from validation
    where game_id not like 'G%'

)

select *
from validation_errors

{% endtest %}