WITH source AS (

  SELECT *
  FROM {{ ref('customers_db_provisions_source') }}

)

SELECT *
FROM source
