{% macro convert_currency_to_pln(amount, currency_from, transaction_date) %}

    {% if currency_from == 'PLN' %}
        {{ amount }}
    {% else %}
        ({{ amount }} * (
            SELECT rate
            FROM {{ ref('exchange_rates') }}
            WHERE currency_from = {{ currency_from }}
            AND date = {{ transaction_date }}
        ))
    {% endif %}

{% endmacro %}