
  
    

create or replace transient table EMBUCKET.workspace_finance.netsuite_currency_exchange_rates
    

    
    as (

WITH currency_exchange_rates AS (

    SELECT * 
    FROM EMBUCKET.netsuite.netsuite_currency_exchange_rates_source

), base_currency AS (

    SELECT * 
    FROM EMBUCKET.netsuite.netsuite_currencies_source

), transaction_currency AS (

    SELECT * 
    FROM EMBUCKET.netsuite.netsuite_currencies_source

)

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

SELECT
      *,
      '@michellecooper'::VARCHAR       AS created_by,
      '@michellecooper'::VARCHAR       AS updated_by,
      '2022-07-06'::DATE        AS model_created_date,
      '2022-07-06'::DATE        AS model_updated_date,
      CURRENT_TIMESTAMP()               AS dbt_updated_at,

    

        

            CURRENT_TIMESTAMP()               AS dbt_created_at

        
    

    FROM final
    )
;


  