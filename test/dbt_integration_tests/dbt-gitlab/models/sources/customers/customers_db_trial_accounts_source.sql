WITH source AS (

  SELECT *
  FROM {{ source('customers', 'customers_db_trial_accounts') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

),

renamed AS (

  SELECT
    id::NUMBER                 AS id,
    billing_account_id::NUMBER AS billing_account_id,
    company::VARCHAR           AS company,
    name::VARCHAR              AS name,
    email::VARCHAR             AS email,
    created_at::TIMESTAMP      AS created_at,
    updated_at::TIMESTAMP      AS updated_at,
  FROM source

)

SELECT *
FROM renamed
