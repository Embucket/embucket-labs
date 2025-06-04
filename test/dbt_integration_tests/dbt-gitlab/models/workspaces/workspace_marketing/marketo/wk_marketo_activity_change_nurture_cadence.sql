WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_change_nurture_cadence_source') }}

)

SELECT *
FROM source