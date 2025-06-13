WITH source AS (

  SELECT *
  FROM {{ ref('snowflake_contract_rates') }}

), 

renamed AS (

  SELECT
    effective_date::DATE                    AS contract_rate_effective_date,
    rate::FLOAT                            AS contract_rate
  FROM source

)

SELECT *
FROM renamed
