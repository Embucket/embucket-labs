{{config({
    "materialized": "view",
    "unique_key":"event_id",
    "snowflake_warehouse": generate_warehouse_name('XL')
  })
}}

WITH source AS (

    SELECT *
    FROM {{ ref("snowplow_duplicate_events_source") }}

)

SELECT *
FROM source
