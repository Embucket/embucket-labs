WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_push_lead_to_marketo_source_pii') }}

)

SELECT *
FROM source