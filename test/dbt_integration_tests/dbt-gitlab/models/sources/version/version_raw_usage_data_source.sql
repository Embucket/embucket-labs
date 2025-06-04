{{ config({
    "materialized": "incremental",
    "unique_key": "raw_usage_data_id",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('version', 'raw_usage_data') }}
    {% if is_incremental() %}
    WHERE _uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      id::VARCHAR             AS raw_usage_data_id,
      PARSE_JSON(payload)     AS raw_usage_data_payload,
      created_at::TIMESTAMP   AS created_at,
      recorded_at::TIMESTAMP  AS recorded_at,
      _uploaded_at::TIMESTAMP AS uploaded_at
    FROM source

)

SELECT *
FROM renamed
