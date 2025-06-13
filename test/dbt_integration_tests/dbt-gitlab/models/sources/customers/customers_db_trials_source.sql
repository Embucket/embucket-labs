WITH source AS (

  SELECT *
  FROM {{ source('customers', 'customers_db_trials') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

),

renamed AS (

  SELECT
    id::NUMBER                 AS id,
    trial_account_id::NUMBER   AS trial_account_id,
    quantity::NUMBER           AS quantity,
    product_rate_plan_id,
    subscription_name::VARCHAR AS subscription_name,
    start_date::DATE           AS start_date,
    end_date::DATE             AS end_date,
    created_at::TIMESTAMP      AS created_at,
    updated_at::TIMESTAMP      AS updated_at
  FROM source

)

SELECT *
FROM renamed
