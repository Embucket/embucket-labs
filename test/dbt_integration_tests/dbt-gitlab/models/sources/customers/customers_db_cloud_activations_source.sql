WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_cloud_activations') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) = 1

), renamed AS (

    SELECT DISTINCT
      id::NUMBER                                              AS cloud_activation_id,
      customer_id::NUMBER                                     AS customer_id,
      billing_account_id::NUMBER                              AS billing_account_id,
      activation_code::VARCHAR                                AS cloud_activation_code,
      subscription_name::VARCHAR                              AS subscription_name,
      super_sonics_aware::BOOLEAN                             AS is_super_sonics_aware_subscription,
      seat_utilization_reminder_sent_at::TIMESTAMP            AS seat_utilization_reminder_sent_at,
      created_at::TIMESTAMP                                   AS cloud_activation_created_at,
      updated_at::TIMESTAMP                                   AS cloud_activation_updated_at
    FROM source



)

SELECT
  *
FROM renamed
