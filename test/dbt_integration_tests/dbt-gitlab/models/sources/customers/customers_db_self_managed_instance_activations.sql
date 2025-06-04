WITH source AS (

  SELECT *
  FROM {{ source('customers', 'customers_db_self_managed_instance_activations') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

),

renamed AS (

  SELECT
    id::NUMBER AS id,
    created_at::TIMESTAMP            AS created_at,
    updated_at::TIMESTAMP            AS updated_at,
    activated_at::TIMESTAMP          AS activated_at,
    self_managed_instance_id::NUMBER AS self_managed_instance_id,
    cloud_activation_id::NUMBER      AS cloud_activation_id,
    subscription_id::VARCHAR         AS subscription_id,
  FROM source

)

SELECT *
FROM renamed
