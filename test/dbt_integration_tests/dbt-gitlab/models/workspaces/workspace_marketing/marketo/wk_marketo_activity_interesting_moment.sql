WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_interesting_moment_source') }}

)

SELECT *
FROM source