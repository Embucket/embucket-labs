WITH source AS (

    SELECT *
    FROM {{ref('xactly_participant_source')}}

)

SELECT *
FROM source