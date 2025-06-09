{{ config({
    "alias": "snowplow_gitlab_good_events_source",
    "snowflake_warehouse": generate_warehouse_name('XL')
}) }}

WITH source as (

  SELECT
    {{ dbt_utils.star(from=source('gitlab_snowplow', 'events'), except=['geo_zipcode', 'geo_latitude', 'geo_longitude', 'user_ipaddress']) }}
  FROM {{ source('gitlab_snowplow', 'events') }}

)

SELECT *
FROM source
