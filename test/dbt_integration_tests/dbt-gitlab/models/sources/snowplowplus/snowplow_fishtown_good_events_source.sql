

WITH source as (

    SELECT *
    FROM {{ source('fishtown_snowplow', 'events') }}

)

SELECT *
FROM source
