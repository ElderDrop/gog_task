{% test not_negative(model, column_name) %}

with validation as (

    select
        {{ column_name }} as value

    from {{ model }}

),

validation_errors as (

    select
        value

    from validation
    where value < 0

)

select *
from validation_errors

{% endtest %}