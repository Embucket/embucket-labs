WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_change_segment_source') }}

)

SELECT *
FROM source