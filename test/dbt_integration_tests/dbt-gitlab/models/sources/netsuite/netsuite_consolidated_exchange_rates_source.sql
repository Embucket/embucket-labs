WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'consolidated_exchange_rates') }}

), renamed AS (

    SELECT
      --Primary Key
      consolidated_exchange_rate_id::FLOAT   AS consolidated_exchange_rate_id,

      --Foreign Keys
      accounting_period_id::FLOAT            AS accounting_period_id,
      from_subsidiary_id::FLOAT              AS from_subsidiary_id,
      to_subsidiary_id::FLOAT                AS to_subsidiary_id,

      --Info
      accounting_book_id::FLOAT              AS accounting_book_id,
      average_budget_rate::FLOAT             AS average_budget_rate,
      current_budget_rate::FLOAT             AS current_budget_rate,
      average_rate::FLOAT                    AS average_rate,
      current_rate::FLOAT                    AS current_rate,
      historical_budget_rate::FLOAT          AS historical_budget_rate,
      historical_rate::FLOAT                 AS historical_rate,
      _fivetran_deleted::BOOLEAN             AS is_fivetran_deleted


    FROM source


)

SELECT *
FROM renamed
