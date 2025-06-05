WITH source as (

    SELECT *
    FROM EMBUCKET.snowplow.bad_events

)

SELECT *
FROM source