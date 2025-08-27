{{ config({
    "alias": "customers_db_cloud_activations_snapshots"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'customers_db_cloud_activations_snapshots') }}

), renamed AS (

    SELECT 
      dbt_scd_id::VARCHAR                                     AS cloud_activation_snapshot_id,
      id::NUMBER                                              AS cloud_activation_id,
      billing_account_id::VARCHAR                             AS billing_account_id,
      customer_id::VARCHAR                                    AS customer_id,
      subscription_name::VARCHAR                              AS subscription_name,
      super_sonics_aware::BOOLEAN                             AS is_super_sonics_aware_subscription,
      seat_utilization_reminder_sent_at                       AS seat_utilization_reminder_sent_at,
      created_at::TIMESTAMP                                   AS cloud_activation_created_at,
      updated_at::TIMESTAMP                                   AS cloud_activation_updated_at,
      dbt_valid_from::TIMESTAMP                             AS valid_from,
      dbt_valid_to::TIMESTAMP                               AS valid_to
    FROM source

)

SELECT *
FROM renamed
