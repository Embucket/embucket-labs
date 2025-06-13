WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_license_seat_links') }}

), renamed AS (

    SELECT
      zuora_subscription_id::VARCHAR     AS zuora_subscription_id,
      zuora_subscription_name::VARCHAR   AS zuora_subscription_name,
      hostname::VARCHAR                  AS hostname,
      instance_identifier::VARCHAR       AS uuid,
      order_id::NUMBER                   AS order_id,
      report_timestamp::TIMESTAMP        AS report_timestamp,
      report_timestamp::DATE             AS report_date,
      license_starts_on::DATE            AS license_starts_on,
      created_at::TIMESTAMP              AS created_at,
      updated_at::TIMESTAMP              AS updated_at,
      active_user_count::NUMBER          AS active_user_count,
      license_user_count::NUMBER         AS license_user_count,
      max_historical_user_count::NUMBER  AS max_historical_user_count,
      add_on_metrics::VARIANT            AS add_on_metrics_user_count
    FROM source  
    QUALIFY ROW_NUMBER() OVER (PARTITION BY zuora_subscription_id, hostname, uuid, report_date ORDER BY updated_at DESC) = 1

)

SELECT *
FROM renamed
