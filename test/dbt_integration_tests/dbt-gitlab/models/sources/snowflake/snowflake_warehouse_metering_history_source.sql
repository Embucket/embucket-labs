WITH source AS (

  SELECT *
  FROM {{ source('snowflake_account_usage','warehouse_metering_history') }}

),

renamed AS (

  SELECT
    start_time::TIMESTAMP                     AS warehouse_metering_start_at,
    end_time::TIMESTAMP                       AS warehouse_metering_end_at,
    warehouse_id::INT                         AS warehouse_id,
    warehouse_name::VARCHAR                   AS warehouse_name,
    credits_used::NUMBER(38,9)                AS credits_used_total,
    credits_used_compute::NUMBER(38,9)        AS credits_used_compute,
    credits_used_cloud_services::NUMBER(38,9) AS credits_used_cloud_services
  FROM source

)

SELECT *
FROM renamed
