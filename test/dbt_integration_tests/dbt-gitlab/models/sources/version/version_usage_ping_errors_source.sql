{{ config({
    "materialized": "incremental",
    "unique_key": "id",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}


WITH source AS (

  SELECT *
  FROM {{ source('version', 'usage_ping_errors') }}
  {% if is_incremental() %}
    WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})
  {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1

),

renamed AS (

  SELECT
    id::NUMBER AS id,
    version_id::NUMBER AS version_id,
    host_id::NUMBER AS host_id,
    uuid::VARCHAR AS uuid,
    elapsed::FLOAT AS elapsed,
    message::VARCHAR AS message,
    created_at::TIMESTAMP AS created_at
  FROM source

)

SELECT *
FROM renamed
ORDER BY created_at
