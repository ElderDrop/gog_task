{% macro convert_currency(amount, currency_from, transaction_date, currency_to='PLN') %}

    CASE
        WHEN {{ currency_from }} = 'PLN' THEN {{ amount }}
        ELSE {{ amount }} * (
            SELECT rate
            FROM {{ ref('stg_excahnge_rates') }} er
            WHERE er.currency_from = {{ currency_from }}
            AND er.currency_to = '{{ currency_to }}'
            AND er.exchange_rate_date = {{ transaction_date }}
        )
    END

{% endmacro %}