{{config({
    "unique_key":"bad_event_surrogate",
    "snowflake_warehouse": generate_warehouse_name('XL')
  })
}}

WITH gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_bad_events') }}

)

SELECT *
FROM gitlab
