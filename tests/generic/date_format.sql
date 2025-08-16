{% test date_format(model, column_name, format) %}

with validation as (

    select
        {{ column_name }} as value

    from {{ model }}

),

validation_errors as (

    select
        value

    from validation
    where value is not null
      and case 
            when '{{ format }}' = '%Y-%m-%d' then
              to_date(value::text, 'YYYY-MM-DD') is null
            when '{{ format }}' = '%Y-%m-%d %H:%M:%S' then
              to_timestamp(value::text, 'YYYY-MM-DD HH24:MI:SS') is null
            when '{{ format }}' = '%Y-%m-%d %H:%M' then
              to_timestamp(value::text, 'YYYY-MM-DD HH24:MI') is null
            else
              to_date(value::text, '{{ format }}') is null
          end

)

select *
from validation_errors

{% endtest %}