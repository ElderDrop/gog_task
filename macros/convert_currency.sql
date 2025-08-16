{% macro convert_currency(amount, currency_from, transaction_date, currency_to='PLN') %}

    CASE
        WHEN {{ currency_from }} = 'PLN' THEN {{ amount }}
        ELSE {{ amount }} * (
            SELECT rate
            FROM {{ ref('exchange_rates') }}
            WHERE currency_from = {{ currency_from }}
            AND currency_to = '{{ currency_to }}'
            AND exchange_rate_date = {{ transaction_date }}
        )
    END

{% endmacro %}