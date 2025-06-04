WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_event_source') }}
    WHERE is_deleted = FALSE

)

SELECT *
FROM source
