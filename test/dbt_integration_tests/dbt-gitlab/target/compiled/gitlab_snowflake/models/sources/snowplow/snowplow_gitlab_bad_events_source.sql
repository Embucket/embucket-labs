WITH source as (

    SELECT *
    FROM EMBUCKET.snowplow.gitlab_bad_events

)

SELECT *
FROM source