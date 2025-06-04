{{config({
    "materialized":"view",
    "tags":"product",
    "snowflake_warehouse": generate_warehouse_name('XL')
  })
}}

-- depends_on: {{ ref('snowplow_sessions') }}

{{ schema_union_all('snowplow_', 'snowplow_sessions', database_name=env_var('SNOWFLAKE_PREP_DATABASE'), excluded_col = ['geo_zipcode', 'geo_latitude', 'geo_longitude', 'ip_address']) }}
