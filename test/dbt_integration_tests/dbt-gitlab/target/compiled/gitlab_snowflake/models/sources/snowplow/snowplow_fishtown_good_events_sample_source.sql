WITH source as (

    SELECT *
    FROM EMBUCKET.snowplow.fishtown_events_sample

)

SELECT *
FROM source