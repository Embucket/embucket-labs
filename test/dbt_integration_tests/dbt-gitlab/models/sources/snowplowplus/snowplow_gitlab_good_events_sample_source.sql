

WITH source as (

  SELECT
    {{ dbt_utils.star(from=source('gitlab_snowplow', 'events_sample'), except=['geo_zipcode', 'geo_latitude', 'geo_longitude', 'user_ipaddress']) }}
  FROM {{ source('gitlab_snowplow', 'events_sample') }}

)

SELECT *
FROM source
