WITH source AS (

  SELECT *
  FROM {{ ref('customers_db_trial_accounts_source') }}

)

SELECT *
FROM source
