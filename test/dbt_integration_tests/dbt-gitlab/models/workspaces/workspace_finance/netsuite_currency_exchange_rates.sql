{{ simple_cte([
    ('currency_exchange_rates','netsuite_currency_exchange_rates_source'),
    ('base_currency','netsuite_currencies_source'),
    ('transaction_currency','netsuite_currencies_source')
]) }}

, final AS (

  SELECT 

    base_currency.currency_symbol AS base_currency,
    transaction_currency.currency_symbol AS transaction_currency,
    currency_exchange_rates.exchange_rate,
    currency_exchange_rates.date_effective AS effective_date

  FROM currency_exchange_rates
  LEFT JOIN base_currency
    ON currency_exchange_rates.base_currency_id = base_currency.currency_id
  LEFT JOIN transaction_currency
    ON currency_exchange_rates.currency_id = transaction_currency.currency_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-07-06",
    updated_date="2022-07-06"
) }}