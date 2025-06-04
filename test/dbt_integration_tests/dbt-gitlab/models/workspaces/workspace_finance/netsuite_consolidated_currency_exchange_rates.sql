{{ config(
    materialized = "table"
) }}

{{ simple_cte([
    ('consolidated_exchange_rates','netsuite_consolidated_exchange_rates_source'),
    ('accounting_period','netsuite_accounting_periods_source'),
    ('subsidiaries','netsuite_subsidiaries_source'),
    ('transaction_currency','netsuite_currencies_source')
]) }}

, final AS (

  SELECT DISTINCT

    accounting_period.accounting_period_name,
    accounting_period.accounting_period_end_date::DATE  AS accounting_period_end_date,
    transaction_currency.currency_symbol                AS base_currency,
    base_currency.currency_symbol                       AS transaction_currency,
    1 / consolidated_exchange_rates.current_rate        AS consolidated_exchange_rate

  FROM consolidated_exchange_rates
  INNER JOIN accounting_period
    ON consolidated_exchange_rates.accounting_period_id = accounting_period.accounting_period_id
  INNER JOIN subsidiaries AS from_subsidiary
    ON consolidated_exchange_rates.from_subsidiary_id = from_subsidiary.subsidiary_id
  INNER JOIN subsidiaries AS to_subsidiary
    ON consolidated_exchange_rates.to_subsidiary_id = to_subsidiary.subsidiary_id
  INNER JOIN transaction_currency AS base_currency
    ON from_subsidiary.base_currency_id = base_currency.currency_id
  INNER JOIN transaction_currency
    ON to_subsidiary.base_currency_id = transaction_currency.currency_id
  WHERE accounting_period.accounting_period_end_date < CURRENT_DATE()

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2023-03-01",
    updated_date="2023-03-01"
) }}
