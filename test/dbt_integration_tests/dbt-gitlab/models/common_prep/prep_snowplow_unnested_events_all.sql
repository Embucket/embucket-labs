{{config({
    "materialized":"view",
    "tags":"product",
    "snowflake_warehouse": generate_warehouse_name('XL')
  })
}}

-- depends_on: {{ ref('snowplow_unnested_events') }}

WITH unioned_view AS (

{{ schema_union_limit('snowplow_', 'snowplow_unnested_events', 'derived_tstamp', 800, database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}

)

{{ macro_prep_snowplow_unnested_events_all(unioned_view) }}
