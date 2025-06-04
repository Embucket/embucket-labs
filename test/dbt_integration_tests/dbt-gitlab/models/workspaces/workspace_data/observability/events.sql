WITH source AS (

    SELECT *
    FROM {{ ref('events_source') }}

)

SELECT *
FROM source
