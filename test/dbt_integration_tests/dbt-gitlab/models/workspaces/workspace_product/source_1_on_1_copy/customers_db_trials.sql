WITH source AS (

  SELECT *
  FROM {{ ref('customers_db_trials_source') }}

)

SELECT *
FROM source
