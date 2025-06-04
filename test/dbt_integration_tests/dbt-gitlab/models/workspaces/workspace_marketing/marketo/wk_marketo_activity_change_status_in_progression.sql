WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_change_status_in_progression_source') }}

)

SELECT *
FROM source