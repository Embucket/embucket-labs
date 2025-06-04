{{ config({
    "materialized": "incremental",
    "unique_key": "id",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}


WITH source AS (

  SELECT *
  FROM {{ source('version', 'usage_ping_metadata') }}
  {% if is_incremental() %}
    WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})
  {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1

),

renamed AS (

  SELECT
    id::NUMBER AS id,
    created_at::TIMESTAMP AS created_at,
    metrics::VARCHAR as metrics,
    uuid::VARCHAR AS uuid
  FROM source

)

SELECT *
FROM renamed
ORDER BY created_at
