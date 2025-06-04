WITH source AS (

  SELECT *
  FROM {{ source('customers', 'customers_db_provisions') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY updated_at DESC) = 1

),

renamed AS (

  SELECT DISTINCT
    subscription_name::VARCHAR    AS subscription_name,
    subscription_id::VARCHAR      AS subscription_id,
    subscription_version::NUMBER  AS subscription_version,
    state::VARCHAR                AS state,
    created_at::TIMESTAMP         AS created_at,
    updated_at::TIMESTAMP         AS updated_at,
    state_reason::NUMBER          AS state_reason,
    contract_effective_date::DATE AS contract_effective_date
  FROM source

)

SELECT *
FROM renamed
