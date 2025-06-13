

WITH source as (

    SELECT *
    FROM {{ source('fishtown_snowplow', 'events_sample') }}

)

SELECT *
FROM source
