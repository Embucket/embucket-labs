{{ config({
    "materialized": "incremental",
    "unique_key": "host_id",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('version', 'hosts') }}
    {% if is_incremental() %}
    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                AS host_id,
      url::VARCHAR              AS host_url,
      created_at::TIMESTAMP     AS created_at,
      updated_at::TIMESTAMP     AS updated_at,
      true::BOOLEAN             AS has_star,
      fortune_rank::NUMBER      AS fortune_rank
    FROM source

)

SELECT *
FROM renamed
