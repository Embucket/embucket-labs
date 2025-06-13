WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_new_lead_source_pii') }}

)

SELECT *
FROM source