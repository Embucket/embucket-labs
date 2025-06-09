

WITH source as (

    SELECT *
    FROM {{ source('fishtown_snowplow', 'bad_events') }}

)

SELECT *
FROM source
